#!/usr/bin/env python3
"""Summarize `dexcli flow history -all -no-hydrate` output read from stdin.

  history_steps.py steps [--since-event N]   Step events with the Worker that ran each
  history_steps.py timing [--last N]         where the time goes per reply cycle
  history_steps.py count                     events kept and the highest event ID
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime


def read_events(text: str) -> list[dict]:
    events: list[dict] = []
    decoder = json.JSONDecoder()
    position = 0
    while position < len(text):
        while position < len(text) and text[position].isspace():
            position += 1
        if position >= len(text):
            break
        page, position = decoder.raw_decode(text, position)
        events.extend(page.get("events", []))
    return events


def parse_time(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def print_steps(events: list[dict], since_event: int) -> None:
    for event in events:
        if int(event["eventId"]) < since_event:
            continue
        payload = event.get("payload", {})
        context = payload.get("context")
        if event["type"] == "FlowStartedOrContinued":
            start = payload.get("initialStart", {})
            print(f'{event["eventId"]:>6} {event["eventTime"]} FlowStarted start Step {start.get("startStepType")}')
        elif context:
            next_steps = [
                step["stepType"]
                for step in payload.get("output", {}).get("stepDecision", {}).get("nextSteps", [])
            ]
            wait = payload.get("output", {}).get("waitForCondition")
            detail = f"-> {', '.join(next_steps)}" if next_steps else ""
            if wait:
                timers = [t.get("durationSeconds") for t in wait.get("timerConditions", [])]
                detail = f"waits for {[c['channelName'] for c in wait.get('channelConditions', [])]} timer {timers}s"
            worker = context.get("pullRouting", {}).get("servedByAppWorkerId", "")
            print(f'{event["eventId"]:>6} {event["eventTime"]} {event["type"]:<24} {context["stepType"]:<22} worker={worker} {detail}')
        elif event["type"] == "RpcExecutionCompleted":
            print(f'{event["eventId"]:>6} {event["eventTime"]} RPC {payload.get("rpcName")} worker={payload.get("servedByAppWorkerId", "")}')
        else:
            print(f'{event["eventId"]:>6} {event["eventTime"]} {event["type"]}')


def print_timing(events: list[dict], last: int) -> None:
    """Per Step method: queue wait (scheduled to started), handler, and commit after the handler."""
    rows: dict[str, list[tuple[float, float, float]]] = {}
    step_events = [e for e in events if e.get("payload", {}).get("context")]
    for event in step_events[-last:]:
        context = event["payload"]["context"]
        key = f'{context["stepType"]} {event["type"]}'
        scheduled = parse_time(context["scheduledTime"])
        started = parse_time(context["startedTime"])
        handler = float(context["duration"].rstrip("s"))
        committed = parse_time(event["eventTime"])
        rows.setdefault(key, []).append(
            (started - scheduled, handler, committed - started - handler)
        )
    replies = [
        parse_time(e["eventTime"])
        for e in step_events[-last:]
        if e["payload"]["context"]["stepType"] == "GenerateReplyStep"
        and e["type"] == "StepExecuteCompleted"
    ]
    print(f"Step method events analyzed: {min(last, len(step_events))}")
    print(f'{"Step method":<46} {"count":>5} {"queue ms":>9} {"handler ms":>11} {"commit ms":>10}  (medians)')
    for key, values in sorted(rows.items()):
        print(
            f"{key:<46} {len(values):>5} "
            f"{statistics.median(v[0] for v in values) * 1000:>9.1f} "
            f"{statistics.median(v[1] for v in values) * 1000:>11.1f} "
            f"{statistics.median(v[2] for v in values) * 1000:>10.1f}"
        )
    if len(replies) > 1:
        gaps = [b - a for a, b in zip(replies, replies[1:])]
        print(
            f"replies: {len(replies)}, median gap {statistics.median(gaps) * 1000:.1f} ms, "
            f"rate {(len(replies) - 1) / (replies[-1] - replies[0]):.1f} replies/s"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    steps = commands.add_parser("steps")
    steps.add_argument("--since-event", type=int, default=0)
    timing = commands.add_parser("timing")
    timing.add_argument("--last", type=int, default=600)
    commands.add_parser("count")
    arguments = parser.parse_args()
    events = read_events(sys.stdin.read())
    if arguments.command == "steps":
        print_steps(events, arguments.since_event)
    elif arguments.command == "timing":
        print_timing(events, arguments.last)
    else:
        ids = [int(e["eventId"]) for e in events]
        print(json.dumps({"eventsKept": len(events), "lowestEventId": min(ids, default=None), "highestEventId": max(ids, default=None)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
