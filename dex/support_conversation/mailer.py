"""Outbound notifications of the conversation, written to a local JSONL outbox."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from dex import Context

OUTBOX_FILE_NAME = "outbox-dex.jsonl"


def conversation_notice_key(context: Context, kind: str) -> str:
    """Stable across retries of one Step execution; distinct per execution and run."""
    return f"{context.run_id}/{kind}/{context.step_execution_id}"


class ConversationMailer(Protocol):
    def send_conversation_notice(
        self, notice_key: str, conversation_id: str, kind: str, message_count: int
    ) -> None: ...


class JsonlOutboxMailer:
    """Stands in for an email provider: appends one JSON line per notice.

    The outbox is a demo file, not a deduplicating store. A Step attempt that
    is retried after the append, for example because the Worker stopped before
    Dex committed the Step, appends another line with the same notice_key.
    Readers count notices by distinct notice_key. A "close-summary" line is
    written before the Flow decides to close; it counts as a close only when
    the Flow run completed, because a message that arrived in the meantime
    sends the conversation back to waiting.
    """

    def __init__(self, outbox_path: Path) -> None:
        self.outbox_path = outbox_path

    def send_conversation_notice(
        self, notice_key: str, conversation_id: str, kind: str, message_count: int
    ) -> None:
        line = json.dumps(
            {
                "time": datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "key": notice_key,
                "conversation": conversation_id,
                "kind": kind,
                "messages": message_count,
            }
        )
        self.outbox_path.parent.mkdir(parents=True, exist_ok=True)
        with self.outbox_path.open("a", encoding="utf-8") as outbox:
            outbox.write(line + "\n")


def outbox_path_from_environment(run_directory: Path) -> Path:
    """DEMO_OUTBOX_PATH, or outbox-dex.jsonl in the run directory."""
    explicit = os.environ.get("DEMO_OUTBOX_PATH")
    return Path(explicit) if explicit else run_directory / OUTBOX_FILE_NAME
