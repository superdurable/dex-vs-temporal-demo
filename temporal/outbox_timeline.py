"""Prints a timeline of one conversation relative to its start time.

Usage:
  uv run outbox_timeline.py <conversation-id> <start-epoch> [label=epoch ...]

Each label=epoch pair is a driver action (for example post=1700000020.1).
Outbox lines for the conversation are read from the demo outbox file.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime

from activities import OUTBOX_FILE


def read_outbox_events(conversation_id: str) -> list[tuple[float, str]]:
    events: list[tuple[float, str]] = []
    if not OUTBOX_FILE.exists():
        return events
    for line in OUTBOX_FILE.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record["conversation"] != conversation_id:
            continue
        epoch = datetime.fromisoformat(record["time"]).timestamp()
        events.append(
            (epoch, f"outbox {record['kind']} written (messages={record['messages']})")
        )
    return events


def main() -> None:
    conversation_id = sys.argv[1]
    start_epoch = float(sys.argv[2])
    events: list[tuple[float, str]] = [(start_epoch, "start conversation")]
    for mark in sys.argv[3:]:
        label, epoch = mark.split("=", 1)
        events.append((float(epoch), label))
    events.extend(read_outbox_events(conversation_id))
    print(f"timeline for {conversation_id}")
    for epoch, label in sorted(events):
        print(f"  t={epoch - start_epoch:6.1f}s  {label}")


if __name__ == "__main__":
    main()
