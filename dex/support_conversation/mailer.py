"""Outbound notifications of the conversation, written to a local JSONL outbox."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

DEFAULT_OUTBOX_PATH = "/tmp/dex-vs-temporal-demo/outbox/dex.jsonl"


class ConversationMailer(Protocol):
    def send_conversation_notice(
        self, conversation_id: str, kind: str, message_count: int
    ) -> None: ...


class JsonlOutboxMailer:
    """Stands in for an email provider: appends one JSON line per notice."""

    def __init__(self, outbox_path: Path) -> None:
        self.outbox_path = outbox_path

    def send_conversation_notice(
        self, conversation_id: str, kind: str, message_count: int
    ) -> None:
        line = json.dumps(
            {
                "time": datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "conversation": conversation_id,
                "kind": kind,
                "messages": message_count,
            }
        )
        self.outbox_path.parent.mkdir(parents=True, exist_ok=True)
        with self.outbox_path.open("a", encoding="utf-8") as outbox:
            outbox.write(line + "\n")


def outbox_path_from_environment() -> Path:
    return Path(os.environ.get("DEMO_OUTBOX_PATH", DEFAULT_OUTBOX_PATH))
