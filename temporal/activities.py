"""Activities that stand in for an email provider and a summary channel.

Each one appends a single JSON line to the shared demo outbox file instead
of calling a real provider.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from temporalio import activity

from conversation_shared import OutboxRequest

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


@activity.defn
async def send_nudge_email(request: OutboxRequest) -> None:
    append_outbox_record("nudge", request)


@activity.defn
async def post_close_summary(request: OutboxRequest) -> None:
    append_outbox_record("close-summary", request)
