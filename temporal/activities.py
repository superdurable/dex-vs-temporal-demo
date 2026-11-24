"""Activities that stand in for an LLM, an email provider, and a summary channel.

generate_reply is a deterministic local mock. The outbox activities append a
single JSON line to the shared demo outbox file instead of calling a real
provider. The outbox is a demo file: a retried activity can append a
duplicate line, where a real provider call would use an idempotency key.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from temporalio import activity

from conversation_shared import GeneratedReply, OutboxRequest, ReplyRequest

# DEMO_REPLY_BYTES sets the mock reply size of the memory benchmark; unset keeps 200.
REPLY_BYTES = int(os.environ.get("DEMO_REPLY_BYTES", "") or 200)
TOOL_CALLS_BY_KEYWORD = (
    ("order", "lookup_order"),
    ("refund", "check_refund_policy"),
    ("deliver", "track_shipment"),
)
REPLY_OPENING_BY_SENTIMENT = {
    "negative": "I am sorry for the trouble. ",
    "neutral": "Thanks for reaching out. ",
}

OUTBOX_DIRECTORY = Path(
    os.environ.get("DEMO_OUTBOX_DIR", "/tmp/dex-vs-temporal-demo/outbox")
)
OUTBOX_FILE = OUTBOX_DIRECTORY / "temporal.jsonl"


def append_outbox_record(kind: str, request: OutboxRequest) -> None:
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "conversation": request.conversation_id,
        "kind": kind,
        "messages": request.message_count,
    }
    OUTBOX_DIRECTORY.mkdir(parents=True, exist_ok=True)
    with OUTBOX_FILE.open("a", encoding="utf-8") as outbox:
        outbox.write(json.dumps(record) + "\n")
    activity.logger.info("outbox %s for %s", kind, request.conversation_id)


def build_mock_reply(
    customer_sequence: int, customer_text: str, sentiment: str = ""
) -> GeneratedReply:
    lowered_text = customer_text.lower()
    tool_calls = [
        tool for keyword, tool in TOOL_CALLS_BY_KEYWORD if keyword in lowered_text
    ] or ["lookup_customer"]
    # Empty when the workflow did not analyze sentiment; replies then match v1.
    opening = REPLY_OPENING_BY_SENTIMENT.get(sentiment, "")
    reply = (
        f"Reply to message {customer_sequence}: {opening}I checked "
        f"{', '.join(tool_calls)} for you. Your request is on file and a "
        "support specialist will follow up shortly. Is there anything else "
        "I can help you with today? "
    )
    repeat_count = REPLY_BYTES // len(reply) + 2
    return GeneratedReply((reply * repeat_count)[:REPLY_BYTES], tool_calls)


@activity.defn
async def generate_reply(request: ReplyRequest) -> GeneratedReply:
    """Mock of an LLM call that uses internal tools: deterministic, local, fast."""
    return build_mock_reply(
        request.customer_sequence, request.customer_text, request.sentiment
    )


NEGATIVE_WORDS = ("angry", "broken", "late", "never", "not", "refund", "wrong")


@activity.defn
async def analyze_sentiment(customer_text: str) -> str:
    """Mock of a sentiment model: deterministic, local, fast."""
    lowered_text = customer_text.lower()
    if any(word in lowered_text for word in NEGATIVE_WORDS):
        return "negative"
    return "neutral"


@activity.defn
async def send_nudge_email(request: OutboxRequest) -> None:
    append_outbox_record("nudge", request)


@activity.defn
async def post_close_summary(request: OutboxRequest) -> None:
    append_outbox_record("close-summary", request)
