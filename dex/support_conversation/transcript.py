"""Data models and pure helpers of the conversation and its chunked transcript.

No Dex resources here: the Flow reads and writes them, these helpers compute values.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Protocol

from dex import CodecRegistry

CURRENT_CHUNK_TOKEN = "current"
TRANSCRIPT_CHUNK_SIZE = 100
CHUNK_TOKEN_WIDTH = 20

# Retention by size: archived chunks plus "current" stay within this many
# serialized bytes. A run's active state budget is 64 MiB by default.
TRANSCRIPT_BUDGET_BYTES = 32 * 1024 * 1024
# The server rejects a single value larger than this (values.maxValueBytes).
MAX_VALUE_BYTES = 16 * 1024 * 1024
# Bounds the chunks one Step execution loads to drop them.
MAX_DROPPED_CHUNKS_PER_EXECUTION = 2

MAX_MESSAGE_CHARACTERS = 8192
# DEMO_REPLY_BYTES: mock reply size of the memory benchmark; unset keeps 200.
DEMO_REPLY_BYTES = int(os.environ.get("DEMO_REPLY_BYTES", "") or 0)
# Raised to the benchmark's reply size so long mock replies are stored whole.
MAX_REPLY_CHARACTERS = max(2000, DEMO_REPLY_BYTES)
assert MAX_REPLY_CHARACTERS <= MAX_MESSAGE_CHARACTERS
MAX_TOOL_CALLS = 8
MAX_TOOL_CALL_CHARACTERS = 64
MAX_AGENT_NAME_CHARACTERS = 200

ROLE_CUSTOMER = "customer"
ROLE_ASSISTANT = "assistant"

STATUS_WAITING = "waiting"
STATUS_NUDGED = "nudged"
# Reported by readers when the Flow run completed; no Step writes it.
STATUS_CLOSED = "closed"


@dataclass(frozen=True)
class ConversationSettings:
    idle_seconds: int = 600
    close_after_seconds: int = 3600


@dataclass(frozen=True)
class ReplyRequest:
    """The Flow start input and the GenerateReplyStep input: one message to answer.

    customer_messages holds exactly one message; the list shape is kept so
    open runs keep decoding.
    """

    settings: ConversationSettings
    customer_messages: list[str]
    # Set by AnalyzeSentimentStep (Flow code v2); empty when no analysis ran.
    sentiment: str = ""


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


@dataclass(frozen=True)
class PendingExchange:
    """TrimTranscriptStep input: a generated exchange that waits for room."""

    settings: ConversationSettings
    records: list[TranscriptRecord]


_TRANSCRIPT_RECORD_CODEC = CodecRegistry().resolve(TranscriptRecord)
_TRANSCRIPT_CHUNK_CODEC = CodecRegistry().resolve(TranscriptChunk)


def encoded_chunk_bytes(chunk: TranscriptChunk) -> int:
    """Return the UTF-8 bytes of the chunk's JSON as the SDK stores it."""
    return len(_TRANSCRIPT_CHUNK_CODEC.encode(chunk).data.encode("utf-8"))


def encoded_record_bytes(record: TranscriptRecord) -> int:
    """Return the UTF-8 bytes of the record's JSON as the SDK encodes it."""
    return len(_TRANSCRIPT_RECORD_CODEC.encode(record).data.encode("utf-8"))


EMPTY_CHUNK_BYTES = encoded_chunk_bytes(TranscriptChunk())


@dataclass
class ConversationState:
    next_sequence: int = 1
    newest_archived_chunk_token: str = ""
    oldest_archived_chunk_token: str = ""
    status: str = STATUS_WAITING
    customer_message_count: int = 0
    assistant_reply_count: int = 0
    # Serialized bytes of each retained archived chunk, oldest first.
    archived_chunk_bytes: list[int] = field(default_factory=list)
    current_chunk_bytes: int = EMPTY_CHUNK_BYTES
    # All retained archived chunks plus "current".
    transcript_bytes: int = EMPTY_CHUNK_BYTES

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


# --- Validation shared by the RPC, the CLI, and the first Step ---


def customer_message_error(text: object) -> str:
    """Return why the customer message is invalid, or "" when it is valid."""
    if not isinstance(text, str) or not text.strip():
        return "message text is empty"
    if len(text) > MAX_MESSAGE_CHARACTERS:
        return f"message text exceeds {MAX_MESSAGE_CHARACTERS} characters"
    return ""


def reply_request_error(request: ReplyRequest) -> str:
    """Return why the request is invalid, or "" for exactly one valid message."""
    if len(request.customer_messages) != 1:
        return (
            "a reply request carries exactly one customer message, "
            f"got {len(request.customer_messages)}"
        )
    return customer_message_error(request.customer_messages[0])


def validate_customer_message(text: object) -> None:
    """Raise ValueError when the customer message is invalid."""
    message_error = customer_message_error(text)
    if message_error:
        raise ValueError(message_error)


def validated_agent_name(agent_name: str) -> str:
    """Return the stripped agent name; raise ValueError when it is empty or too long."""
    agent_name = agent_name.strip()
    if not agent_name or len(agent_name) > MAX_AGENT_NAME_CHARACTERS:
        raise ValueError("agent name must be 1 to 200 characters")
    return agent_name


def bounded_reply_text(text: str) -> str:
    return text[:MAX_REPLY_CHARACTERS]


def bounded_tool_calls(tool_calls: list[str]) -> list[str]:
    return [call[:MAX_TOOL_CALL_CHARACTERS] for call in tool_calls[:MAX_TOOL_CALLS]]


def utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def exchange_records(
    customer_message: str, reply_text: str, tool_calls: list[str]
) -> list[TranscriptRecord]:
    """The customer message and its bounded reply; the append assigns sequences."""
    recorded_at = utc_timestamp()
    return [
        TranscriptRecord(0, ROLE_CUSTOMER, customer_message, recorded_at=recorded_at),
        TranscriptRecord(
            0,
            ROLE_ASSISTANT,
            bounded_reply_text(reply_text),
            bounded_tool_calls(tool_calls),
            recorded_at,
        ),
    ]


# --- Size bounds, checked once at import ---


def largest_exchange_bytes() -> int:
    """Bytes one validated exchange adds to a chunk at most: two records and two commas."""
    widest_character = "\U0001f600"  # 12 JSON bytes: an escaped surrogate pair
    largest_sequence = 10**CHUNK_TOKEN_WIDTH - 1
    customer = TranscriptRecord(
        largest_sequence,
        ROLE_CUSTOMER,
        widest_character * MAX_MESSAGE_CHARACTERS,
        recorded_at="2026-01-01T00:00:00.000Z",
    )
    assistant = TranscriptRecord(
        largest_sequence,
        ROLE_ASSISTANT,
        widest_character * MAX_REPLY_CHARACTERS,
        [widest_character * MAX_TOOL_CALL_CHARACTERS] * MAX_TOOL_CALLS,
        "2026-01-01T00:00:00.000Z",
    )
    return encoded_record_bytes(customer) + encoded_record_bytes(assistant) + 2


LARGEST_EXCHANGE_BYTES = largest_exchange_bytes()
LARGEST_CHUNK_BYTES = EMPTY_CHUNK_BYTES + (
    TRANSCRIPT_CHUNK_SIZE // 2
) * LARGEST_EXCHANGE_BYTES
# Every chunk fits one value. After every droppable chunk is gone, the newest
# archived chunk, "current", and one exchange still fit the budget, so a
# validated exchange always fits.
assert TRANSCRIPT_CHUNK_SIZE % 2 == 0
assert LARGEST_CHUNK_BYTES <= MAX_VALUE_BYTES
assert 2 * LARGEST_CHUNK_BYTES + EMPTY_CHUNK_BYTES <= TRANSCRIPT_BUDGET_BYTES


# --- Chunk tokens and retention bookkeeping ---


def archived_chunk_token(first_sequence: int) -> str:
    return f"{first_sequence:0{CHUNK_TOKEN_WIDTH}d}"


def count_retained_archived_chunks(state: ConversationState) -> int:
    return len(state.archived_chunk_bytes)


def droppable_archived_chunk_count(state: ConversationState) -> int:
    """Archived chunks that may be dropped: all but the newest archived chunk."""
    return max(len(state.archived_chunk_bytes) - 1, 0)


def chunk_tokens_to_drop(
    state: ConversationState, bytes_to_free: int
) -> list[str]:
    """Return the oldest droppable chunks, at most two, that free bytes_to_free.

    Returns fewer chunks when two are not enough; the caller then trims again
    in a later Step execution. The newest archived chunk is never returned.
    """
    tokens: list[str] = []
    for index, size in enumerate(state.archived_chunk_bytes[:-1]):
        if bytes_to_free <= 0 or len(tokens) == MAX_DROPPED_CHUNKS_PER_EXECUTION:
            break
        first_sequence = (
            int(state.oldest_archived_chunk_token) + index * TRANSCRIPT_CHUNK_SIZE
        )
        tokens.append(archived_chunk_token(first_sequence))
        bytes_to_free -= size
    return tokens


def drop_oldest_archived_chunks(state: ConversationState, tokens: list[str]) -> None:
    """Remove the dropped chunks, oldest first, from the retention bookkeeping."""
    for token in tokens:
        if token != state.oldest_archived_chunk_token:
            raise ValueError(f'chunk "{token}" is not the oldest retained chunk')
        if len(state.archived_chunk_bytes) < 2:
            raise ValueError("the newest archived chunk is never dropped")
        state.archived_chunk_bytes.pop(0)
        state.oldest_archived_chunk_token = archived_chunk_token(
            int(token) + TRANSCRIPT_CHUNK_SIZE
        )
    state.transcript_bytes = sum(state.archived_chunk_bytes) + state.current_chunk_bytes


@dataclass
class TranscriptAppendPlan:
    """The chunks after an append, their serialized sizes, and the bytes to free first."""

    current_chunk: TranscriptChunk
    current_chunk_bytes: int
    archived_chunk_token: str = ""
    archived_chunk: TranscriptChunk | None = None
    archived_chunk_bytes: int = 0
    # Bytes to drop before the append fits TRANSCRIPT_BUDGET_BYTES; 0 when it fits.
    over_budget_bytes: int = 0
    # chunk_tokens_to_drop for over_budget_bytes; empty when the append fits.
    chunk_tokens_to_drop: list[str] = field(default_factory=list)


def plan_transcript_append(
    state: ConversationState,
    current_chunk: TranscriptChunk | None,
    records: list[TranscriptRecord],
) -> TranscriptAppendPlan:
    """Assign sequences, build the chunks after the append, and measure them.

    Pure: changes neither ``state`` nor ``current_chunk``. Sizes come from the
    serialized chunks the transcript writer stores, not from estimates.
    A ``current_chunk`` of None is the empty chunk of a new conversation.
    """
    if len(records) > TRANSCRIPT_CHUNK_SIZE:
        raise ValueError(f"at most {TRANSCRIPT_CHUNK_SIZE} records per append")
    numbered = [
        replace(record, sequence=state.next_sequence + offset)
        for offset, record in enumerate(records)
    ]
    current_records = current_chunk.records if current_chunk is not None else []
    combined = [*current_records, *numbered]
    archived: TranscriptChunk | None = None
    if len(combined) >= TRANSCRIPT_CHUNK_SIZE:
        archived = TranscriptChunk(combined[:TRANSCRIPT_CHUNK_SIZE])
        combined = combined[TRANSCRIPT_CHUNK_SIZE:]
    plan = TranscriptAppendPlan(TranscriptChunk(combined), 0)
    plan.current_chunk_bytes = encoded_chunk_bytes(plan.current_chunk)
    if archived is not None:
        plan.archived_chunk = archived
        plan.archived_chunk_token = archived_chunk_token(archived.records[0].sequence)
        plan.archived_chunk_bytes = encoded_chunk_bytes(archived)
    total_bytes = (
        sum(state.archived_chunk_bytes)
        + plan.archived_chunk_bytes
        + plan.current_chunk_bytes
    )
    plan.over_budget_bytes = max(total_bytes - TRANSCRIPT_BUDGET_BYTES, 0)
    plan.chunk_tokens_to_drop = chunk_tokens_to_drop(state, plan.over_budget_bytes)
    largest_value = max(plan.current_chunk_bytes, plan.archived_chunk_bytes)
    if largest_value > MAX_VALUE_BYTES:
        raise ValueError(
            f"a transcript chunk of {largest_value} bytes exceeds {MAX_VALUE_BYTES}"
        )
    return plan


def apply_transcript_append(
    state: ConversationState,
    plan: TranscriptAppendPlan,
    records: list[TranscriptRecord],
) -> None:
    """Record a fitting plan in ``state``: sequences, counts, sizes, and tokens."""
    if plan.over_budget_bytes > 0:
        raise ValueError("the append does not fit the transcript budget yet")
    state.next_sequence += len(records)
    for record in records:
        if record.role == ROLE_CUSTOMER:
            state.customer_message_count += 1
        else:
            state.assistant_reply_count += 1
    if plan.archived_chunk is not None:
        state.archived_chunk_bytes.append(plan.archived_chunk_bytes)
        state.newest_archived_chunk_token = plan.archived_chunk_token
        if not state.oldest_archived_chunk_token:
            state.oldest_archived_chunk_token = plan.archived_chunk_token
    state.current_chunk_bytes = plan.current_chunk_bytes
    state.transcript_bytes = sum(state.archived_chunk_bytes) + state.current_chunk_bytes
    if state.transcript_bytes > TRANSCRIPT_BUDGET_BYTES:
        raise ValueError(
            f"transcript of {state.transcript_bytes} bytes exceeds the budget"
        )


def unfittable_exchange_error(plan: TranscriptAppendPlan) -> str:
    return (
        f"one exchange cannot fit the transcript budget: "
        f"{plan.over_budget_bytes} bytes over with no chunk left to drop"
    )


def record_exchange(
    state: ConversationState,
    plan: TranscriptAppendPlan,
    records: list[TranscriptRecord],
) -> list[tuple[str, TranscriptChunk]]:
    """Apply a fitting plan to ``state``; return the chunk writes, archived chunk first."""
    apply_transcript_append(state, plan, records)
    writes: list[tuple[str, TranscriptChunk]] = []
    if plan.archived_chunk is not None:
        writes.append((plan.archived_chunk_token, plan.archived_chunk))
    writes.append((CURRENT_CHUNK_TOKEN, plan.current_chunk))
    return writes


# --- Transcript pages, newest record first ---


def require_archived_transcript_page(state: ConversationState, page_token: str) -> None:
    """Raise ValueError when the archived page token is newer than every archived chunk."""
    newest = state.newest_archived_chunk_token
    if not newest or int(page_token) > int(newest):
        raise ValueError(f'transcript page "{page_token}" is not archived yet')


def beyond_retention_page(page_token: str, oldest: str) -> TranscriptPage:
    """The empty page of a token older than the retention window."""
    return TranscriptPage(
        page_token, [], is_beyond_retention=True, oldest_retained_page_token=oldest
    )


def transcript_page(
    state: ConversationState, page_token: str, chunk: TranscriptChunk
) -> TranscriptPage:
    """The chunk's records newest first, with the token of the next older page."""
    oldest = state.oldest_archived_chunk_token
    if page_token == CURRENT_CHUNK_TOKEN:
        next_older = state.newest_archived_chunk_token
    else:
        older_sequence = int(page_token) - TRANSCRIPT_CHUNK_SIZE
        next_older = (
            archived_chunk_token(older_sequence) if older_sequence >= int(oldest) else ""
        )
    return TranscriptPage(
        page_token,
        list(reversed(chunk.records)),
        next_older_page_token=next_older,
        oldest_retained_page_token=oldest,
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
