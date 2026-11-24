"""Print one line per event of a saved `temporal workflow show -o json` file.

Usage: history_summary.py <history.json>
Each line shows the event ID, time, type, the worker identity, and, for a
failed workflow task, its cause and message.
"""

import json
import sys


def main(path: str) -> None:
    with open(path, encoding="utf-8") as history_file:
        history = json.load(history_file)
    for event in history["events"]:
        attributes = next(
            (value for key, value in event.items() if key.endswith("Attributes")), {}
        )
        detail = attributes.get("identity", "")
        if "startToFireTimeout" in attributes:
            detail = f"timer {attributes['startToFireTimeout']}"
        if "failure" in attributes:
            detail += f" cause={attributes.get('cause')} {attributes['failure'].get('message')}"
        event_type = event["eventType"].removeprefix("EVENT_TYPE_")
        print(f"{event['eventId']:>4} {event['eventTime']} {event_type:<34} {detail}")


if __name__ == "__main__":
    main(sys.argv[1])
