"""Data models of the conversation and the chunked transcript; no Dex resources here."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

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
class ConversationSettings:
    idle_seconds: int = 600
    close_after_seconds: int = 3600


@dataclass(frozen=True)
class ReplyRequest:
    """The Flow start input and the GenerateReplyStep input: messages to answer."""

    settings: ConversationSettings
    customer_messages: list[str]


@dataclass
class TranscriptRecord:
    sequence: int
    role: str
    text: str
    tool_calls: list[str] = field(default_factory=list)
    recorded_at: str = ""


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


@dataclass
class TranscriptAppend:
    """What the transcript writer must store after appending records."""

    current_chunk: TranscriptChunk
    archived_chunk_token: str = ""
    archived_chunk: TranscriptChunk | None = None
    evicted_chunk_token: str = ""


def append_records_to_chunk(
    state: ConversationState,
    current_chunk: TranscriptChunk,
    records: list[TranscriptRecord],
) -> TranscriptAppend:
    """Assign sequences and append records; archive a full chunk; pick the chunk to evict.

    Updates ``state`` in place. Takes at most TRANSCRIPT_CHUNK_SIZE records, so
    it archives at most one chunk and evicts at most one chunk.
    """
    if len(records) > TRANSCRIPT_CHUNK_SIZE:
        raise ValueError(f"at most {TRANSCRIPT_CHUNK_SIZE} records per append")
    result = TranscriptAppend(current_chunk)
    for record in records:
        record.sequence = state.next_sequence
        result.current_chunk.records.append(record)
        state.next_sequence += 1
        if record.role == ROLE_CUSTOMER:
            state.customer_message_count += 1
        else:
            state.assistant_reply_count += 1
        if len(result.current_chunk.records) < TRANSCRIPT_CHUNK_SIZE:
            continue
        token = archived_chunk_token(result.current_chunk.records[0].sequence)
        result.archived_chunk_token = token
        result.archived_chunk = result.current_chunk
        result.current_chunk = TranscriptChunk()
        state.newest_archived_chunk_token = token
        if not state.oldest_archived_chunk_token:
            state.oldest_archived_chunk_token = token
        if count_retained_archived_chunks(state) > RETAINED_ARCHIVED_CHUNKS:
            result.evicted_chunk_token = state.oldest_archived_chunk_token
            state.oldest_archived_chunk_token = archived_chunk_token(
                int(result.evicted_chunk_token) + TRANSCRIPT_CHUNK_SIZE
            )
    return result


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
