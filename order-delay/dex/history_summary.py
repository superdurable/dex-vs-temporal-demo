"""Print one line per event of a saved `dexcli flow history -all -no-hydrate` file.

Usage: history_summary.py <history.jsonl>
Each line shows the event ID, time, type, Step, the Worker that ran it, and
what a WaitFor waits on.
"""

import json
import sys


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


def describe(event: dict) -> str:
    payload = event.get("payload", {})
    context = payload.get("context")
    if context:
        output = payload.get("output", {})
        worker = context.get("pullRouting", {}).get("servedByAppWorkerId", "")
        detail = ""
        wait = output.get("waitForCondition")
        if wait:
            timers = [t.get("durationSeconds") for t in wait.get("timerConditions", [])]
            channels = [c["channelName"] for c in wait.get("channelConditions", [])]
            detail = f"waits for timers {timers} s, channels {channels}"
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


def main(path: str) -> None:
    with open(path, encoding="utf-8") as history_file:
        events = read_events(history_file.read())
    for event in events:
        print(f'{event["eventId"]:>4} {event["eventTime"]} {event["type"]:<28} {describe(event)}')


if __name__ == "__main__":
    main(sys.argv[1])
