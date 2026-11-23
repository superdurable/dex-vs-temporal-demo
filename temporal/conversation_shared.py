"""Types and pure helpers shared by both conversation workflow variants.

This module has no side effects, so it is safe to import inside the
Temporal workflow sandbox.
"""

from __future__ import annotations

from dataclasses import dataclass, field

TASK_QUEUE = "conversation-demo"
NAIVE_WORKFLOW_TYPE = "NaiveConversationWorkflow"
CORRECT_WORKFLOW_TYPE = "ConversationWorkflow"
CONTINUE_AS_NEW_WORKFLOW_TYPE = "ConversationWorkflowWithContinueAsNew"
SENTIMENT_PATCH_ID = "analyze-sentiment-before-reply"

DEFAULT_IDLE_SECONDS = 600
DEFAULT_CLOSE_AFTER_SECONDS = 3600
TRANSCRIPT_PAGE_SIZE = 100

ROLE_CUSTOMER = "customer"
ROLE_ASSISTANT = "assistant"
CURRENT_PAGE_TOKEN = "current"
PAGE_TOKEN_WIDTH = 20

STATUS_OPEN = "open"
STATUS_NUDGED = "nudged"
STATUS_CLOSED = "closed"


@dataclass
class ConversationInput:
    conversation_id: str
    first_message: str
    idle_seconds: int = DEFAULT_IDLE_SECONDS
    close_after_seconds: int = DEFAULT_CLOSE_AFTER_SECONDS


@dataclass
class TranscriptMessage:
    sequence: int
    role: str
    text: str
    time: str
    tool_calls: list[str] = field(default_factory=list)


@dataclass
class TranscriptPage:
    page_token: str
    messages: list[TranscriptMessage] = field(default_factory=list)
    next_page_token: str = ""


@dataclass
class ConversationSummary:
    conversation_id: str
    status: str
    agent: str
    message_count: int
    customer_message_count: int
    assistant_message_count: int
    pending_reply_count: int


@dataclass
class OutboxRequest:
    conversation_id: str
    message_count: int


@dataclass
class ReplyRequest:
    conversation_id: str
    customer_sequence: int
    customer_text: str
    # Empty when the workflow code did not run sentiment analysis.
    sentiment: str = ""


@dataclass
class GeneratedReply:
    text: str
    tool_calls: list[str] = field(default_factory=list)


def summarize_conversation(
    conversation_id: str,
    status: str,
    agent: str,
    transcript: list[TranscriptMessage],
    pending_reply_count: int,
) -> ConversationSummary:
    customer_message_count = sum(
        1 for message in transcript if message.role == ROLE_CUSTOMER
    )
    return ConversationSummary(
        conversation_id,
        status,
        agent,
        len(transcript),
        customer_message_count,
        len(transcript) - customer_message_count,
        pending_reply_count,
    )


def archived_page_token(first_sequence: int) -> str:
    return f"{first_sequence:0{PAGE_TOKEN_WIDTH}d}"


def read_transcript_page(
    transcript: list[TranscriptMessage], page_token: str
) -> TranscriptPage:
    """Returns one page of the transcript, newest message first.

    Pages hold 100 messages. Sequences start at 1. The "current" page holds
    the newest 1..100 messages; every older page is full and its token is the
    zero-padded sequence of its first message. next_page_token names the next
    older page, or is empty when there is none. An unknown or malformed token
    raises ValueError.
    """
    if not page_token:
        page_token = CURRENT_PAGE_TOKEN
    message_count = len(transcript)
    if message_count == 0:
        if page_token == CURRENT_PAGE_TOKEN:
            return TranscriptPage(page_token)
        raise ValueError(f'transcript page "{page_token}" not found')

    current_first_sequence = (
        (message_count - 1) // TRANSCRIPT_PAGE_SIZE
    ) * TRANSCRIPT_PAGE_SIZE + 1
    if page_token == CURRENT_PAGE_TOKEN:
        first_sequence = current_first_sequence
        last_sequence = message_count
    else:
        first_sequence = parse_archived_page_token(page_token)
        if first_sequence >= current_first_sequence:
            raise ValueError(f'transcript page "{page_token}" not found')
        last_sequence = first_sequence + TRANSCRIPT_PAGE_SIZE - 1

    page = TranscriptPage(
        page_token,
        list(reversed(transcript[first_sequence - 1 : last_sequence])),
    )
    if first_sequence > 1:
        page.next_page_token = archived_page_token(
            first_sequence - TRANSCRIPT_PAGE_SIZE
        )
    return page


def parse_archived_page_token(page_token: str) -> int:
    if (
        len(page_token) != PAGE_TOKEN_WIDTH
        or not page_token.isascii()
        or not page_token.isdigit()
    ):
        raise ValueError(f'invalid page token "{page_token}"')
    first_sequence = int(page_token)
    if first_sequence == 0 or (first_sequence - 1) % TRANSCRIPT_PAGE_SIZE != 0:
        raise ValueError(f'invalid page token "{page_token}"')
    return first_sequence
