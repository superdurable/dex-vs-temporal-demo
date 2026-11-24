"""Summarize a saved `temporal workflow show -o json` file.

Usage: history_summary.py <history.json>               one line per event
       history_summary.py --activities <history.json>  scheduled activity types, in order
Event lines show the event ID, time, type, the worker identity, the activity
type of a scheduled activity, and the cause and message of a failed workflow task.
"""

import json
import sys


def read_events(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as history_file:
        return json.load(history_file)["events"]


def attributes_of(event: dict) -> dict:
    return next(
        (value for key, value in event.items() if key.endswith("Attributes")), {}
    )


def print_events(path: str) -> None:
    for event in read_events(path):
        attributes = attributes_of(event)
        detail = attributes.get("identity", "")
        if "activityType" in attributes:
            detail = attributes["activityType"]["name"]
        if "failure" in attributes:
            detail += f" cause={attributes.get('cause')} {attributes['failure'].get('message')}"
        event_type = event["eventType"].removeprefix("EVENT_TYPE_")
        print(f"{event['eventId']:>4} {event['eventTime']} {event_type:<34} {detail}")


def print_activities(path: str) -> None:
    names = [
        attributes_of(event)["activityType"]["name"]
        for event in read_events(path)
        if event["eventType"] == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"
    ]
    print(" ".join(names))


if __name__ == "__main__":
    if sys.argv[1] == "--activities":
        print_activities(sys.argv[2])
    else:
        print_events(sys.argv[1])
