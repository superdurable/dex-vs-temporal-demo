"""Conversation state and the sequentially chunked transcript with a retention window."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from dex import (
    Attribute,
    AttributeMap,
    AttributeMapLoad,
    Channel,
    Context,
    StepOptions,
)

CURRENT_CHUNK_TOKEN = "current"
TRANSCRIPT_CHUNK_SIZE = 100
RETAINED_ARCHIVED_CHUNKS = 100
CHUNK_TOKEN_WIDTH = 20

ROLE_CUSTOMER = "customer"
ROLE_ASSISTANT = "assistant"

STATUS_WAITING = "waiting"
STATUS_REPLYING = "replying"
STATUS_NUDGED = "nudged"
STATUS_CLOSED = "closed"


@dataclass(frozen=True)
class ConversationStart:
    idle_seconds: int = 600
    close_after_seconds: int = 3600


@dataclass(frozen=True)
class ReplyRequest:
    conversation: ConversationStart
    customer_sequence: int
    customer_text: str


@dataclass
class TranscriptRecord:
    sequence: int
    role: str
    text: str
    tool_calls: list[str] = field(default_factory=list)


@dataclass
class TranscriptChunk:
    records: list[TranscriptRecord] = field(default_factory=list)


@dataclass
class ConversationState:
    next_sequence: int = 1
    newest_archived_chunk_token: str = ""
    oldest_archived_chunk_token: str = ""
    status: str = STATUS_WAITING
    customer_message_count: int = 0
    assistant_reply_count: int = 0

    @property
    def record_count(self) -> int:
        return self.next_sequence - 1


@dataclass
class TranscriptSummary:
    summarized_chunk_count: int = 0
    summarized_record_count: int = 0
    summarized_customer_count: int = 0
    summarized_assistant_count: int = 0
    first_sequence: int = 0
    last_sequence: int = 0
    first_text: str = ""
    last_text: str = ""


@dataclass
class TranscriptPage:
    page_token: str
    records: list[TranscriptRecord]
    next_older_page_token: str = ""
    is_beyond_retention: bool = False
    oldest_retained_page_token: str = ""


CUSTOMER_MESSAGES = Channel("CustomerMessages", str)
CONVERSATION_STATE = Attribute("ConversationState", ConversationState)
ASSIGNED_AGENT = Attribute("AssignedAgent", str)
SUMMARY = Attribute("Summary", TranscriptSummary)
TRANSCRIPT_CHUNKS = AttributeMap("TranscriptChunks", TranscriptChunk)


class TranscriptSummarizer(Protocol):
    def fold_chunk_into_summary(
        self, summary: TranscriptSummary | None, chunk: TranscriptChunk
    ) -> TranscriptSummary: ...


class LocalTranscriptSummarizer:
    """Deterministic stand-in for a model: counts, sequence range, first and last text."""

    TEXT_PREVIEW_CHARACTERS = 120

    def fold_chunk_into_summary(
        self, summary: TranscriptSummary | None, chunk: TranscriptChunk
    ) -> TranscriptSummary:
        if not chunk.records:
            return summary or TranscriptSummary()
        first_record = chunk.records[0]
        last_record = chunk.records[-1]
        if summary is None or summary.summarized_chunk_count == 0:
            summary = TranscriptSummary(
                first_sequence=first_record.sequence,
                first_text=self.preview(first_record),
            )
        customer_count = sum(1 for r in chunk.records if r.role == ROLE_CUSTOMER)
        return TranscriptSummary(
            summarized_chunk_count=summary.summarized_chunk_count + 1,
            summarized_record_count=summary.summarized_record_count
            + len(chunk.records),
            summarized_customer_count=summary.summarized_customer_count
            + customer_count,
            summarized_assistant_count=summary.summarized_assistant_count
            + len(chunk.records)
            - customer_count,
            first_sequence=summary.first_sequence,
            last_sequence=last_record.sequence,
            first_text=summary.first_text,
            last_text=self.preview(last_record),
        )

    def preview(self, record: TranscriptRecord) -> str:
        return f"{record.role}: {record.text}"[: self.TEXT_PREVIEW_CHARACTERS]


def archived_chunk_token(first_sequence: int) -> str:
    return f"{first_sequence:0{CHUNK_TOKEN_WIDTH}d}"


def count_retained_archived_chunks(state: ConversationState) -> int:
    if not state.newest_archived_chunk_token:
        return 0
    newest = int(state.newest_archived_chunk_token)
    oldest = int(state.oldest_archived_chunk_token)
    return (newest - oldest) // TRANSCRIPT_CHUNK_SIZE + 1


def next_evicted_chunk_token(state: ConversationState) -> str:
    """Return the chunk the next archive would evict, or "" when there is room."""
    if count_retained_archived_chunks(state) < RETAINED_ARCHIVED_CHUNKS:
        return ""
    return state.oldest_archived_chunk_token


def transcript_writer_step_options(state: ConversationState | None) -> StepOptions:
    """Options of a Step that appends one record: load "current" and the chunk it may evict.

    One appended record archives at most one chunk and evicts at most one chunk.
    """
    loads: list[AttributeMapLoad] = [TRANSCRIPT_CHUNKS.load(CURRENT_CHUNK_TOKEN)]
    if state is not None:
        evicted_token = next_evicted_chunk_token(state)
        if evicted_token:
            loads.append(TRANSCRIPT_CHUNKS.load(evicted_token))
    return StepOptions(execute_load_attribute_map_instances=tuple(loads))


def append_record_to_transcript(
    context: Context,
    role: str,
    text: str,
    summarizer: TranscriptSummarizer,
    tool_calls: list[str] | None = None,
) -> tuple[ConversationState, TranscriptRecord]:
    """Append one record; archive a full chunk; evict the oldest beyond the window."""
    state = CONVERSATION_STATE.get(context)
    current_chunk = TRANSCRIPT_CHUNKS.get(context, CURRENT_CHUNK_TOKEN)
    record = TranscriptRecord(state.next_sequence, role, text, list(tool_calls or []))
    current_chunk.records.append(record)
    state.next_sequence += 1
    if role == ROLE_CUSTOMER:
        state.customer_message_count += 1
    else:
        state.assistant_reply_count += 1
    if len(current_chunk.records) >= TRANSCRIPT_CHUNK_SIZE:
        token = archived_chunk_token(current_chunk.records[0].sequence)
        TRANSCRIPT_CHUNKS.set(context, token, current_chunk)
        state.newest_archived_chunk_token = token
        if not state.oldest_archived_chunk_token:
            state.oldest_archived_chunk_token = token
        current_chunk = TranscriptChunk()
        if count_retained_archived_chunks(state) > RETAINED_ARCHIVED_CHUNKS:
            evict_oldest_archived_chunk(context, state, summarizer)
    TRANSCRIPT_CHUNKS.set(context, CURRENT_CHUNK_TOKEN, current_chunk)
    CONVERSATION_STATE.set(context, state)
    return state, record


def evict_oldest_archived_chunk(
    context: Context,
    state: ConversationState,
    summarizer: TranscriptSummarizer,
) -> None:
    oldest_token = state.oldest_archived_chunk_token
    # Loaded through the per-movement options built by transcript_writer_step_options.
    evicted_chunk = TRANSCRIPT_CHUNKS.get(context, oldest_token)
    SUMMARY.set(
        context,
        summarizer.fold_chunk_into_summary(SUMMARY.get(context), evicted_chunk),
    )
    TRANSCRIPT_CHUNKS.delete(context, oldest_token)
    state.oldest_archived_chunk_token = archived_chunk_token(
        int(oldest_token) + TRANSCRIPT_CHUNK_SIZE
    )


def parse_transcript_page_token(page_token: str) -> str:
    """Return the canonical token; raise ValueError for a malformed token."""
    if not page_token or page_token == CURRENT_CHUNK_TOKEN:
        return CURRENT_CHUNK_TOKEN
    if (
        len(page_token) != CHUNK_TOKEN_WIDTH
        or not page_token.isascii()
        or not page_token.isdigit()
    ):
        raise ValueError(f'invalid transcript page token "{page_token}"')
    first_sequence = int(page_token)
    if first_sequence == 0 or (first_sequence - 1) % TRANSCRIPT_CHUNK_SIZE != 0:
        raise ValueError(f'invalid transcript page token "{page_token}"')
    return page_token
