"""Summarize a saved `dexcli flow history -all -no-hydrate` file.

Usage: history_summary.py <history.jsonl>           one line per event
       history_summary.py --steps <history.jsonl>   completed Step executions, in order
Event lines show the event ID, time, type, Step, the Worker that ran it, and
the Channels a WaitFor waits on.
"""

import json
import sys


def read_events(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as history_file:
        text = history_file.read()
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


def describe(event: dict) -> str:
    payload = event.get("payload", {})
    context = payload.get("context")
    if context:
        output = payload.get("output", {})
        worker = context.get("pullRouting", {}).get("servedByAppWorkerId", "")
        detail = ""
        wait = output.get("waitForCondition")
        if wait:
            channels = [c["channelName"] for c in wait.get("channelConditions", [])]
            detail = f"waits for channels {channels}"
        next_steps = [
            step["stepType"]
            for step in output.get("stepDecision", {}).get("nextSteps", [])
        ]
        if next_steps:
            detail = f"-> {', '.join(next_steps)}"
        return f'{context["stepType"]} worker={worker} {detail}'
    if event["type"] == "RpcExecutionCompleted":
        return f'{payload.get("rpcName")} worker={payload.get("servedByAppWorkerId", "")}'
    return json.dumps(payload, sort_keys=True)[:160]


def print_events(path: str) -> None:
    for event in read_events(path):
        print(f'{event["eventId"]:>4} {event["eventTime"]} {event["type"]:<28} {describe(event)}')


def print_steps(path: str) -> None:
    steps = [
        event["payload"]["context"]["stepType"]
        for event in read_events(path)
        if event["type"] == "StepExecuteCompleted"
    ]
    print(" ".join(steps))


if __name__ == "__main__":
    if sys.argv[1] == "--steps":
        print_steps(sys.argv[2])
    else:
        print_events(sys.argv[1])
