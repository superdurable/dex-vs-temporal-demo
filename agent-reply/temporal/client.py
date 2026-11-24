"""Start, approve, and inspect ReplyWorkflow runs.

  start <workflow-id> <ticket>      start a workflow
  approve <workflow-id>             send the approve_reply signal
  signal <workflow-id> <name>       send a signal by name (for experiments)
  result <workflow-id> [seconds]    wait for the result (default 120 s) and print it
  status <workflow-id>              print the status and the last history events
"""

import asyncio
import os
import sys

from temporalio.client import Client

from settings import TASK_QUEUE


async def print_status(client: Client, workflow_id: str) -> None:
    handle = client.get_workflow_handle(workflow_id)
    description = await handle.describe()
    events = [event async for event in handle.fetch_history_events()]
    last = ", ".join(
        f"{event.event_id} {event.WhichOneof('attributes')}" for event in events[-3:]
    )
    print(f"{workflow_id}: {description.status.name}; last events: {last}")


async def main(command: str, workflow_id: str, *args: str) -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address, identity="reply-client")
    handle = client.get_workflow_handle(workflow_id)
    if command == "start":
        await client.start_workflow(
            "ReplyWorkflow", args[0], id=workflow_id, task_queue=TASK_QUEUE
        )
        print(f"started {workflow_id}")
    elif command == "approve":
        await handle.signal("approve_reply")
        print(f"sent approve_reply to {workflow_id}")
    elif command == "signal":
        await handle.signal(args[0])
        print(f"sent {args[0]} to {workflow_id}")
    elif command == "result":
        seconds = float(args[0]) if args else 120
        try:
            result = await asyncio.wait_for(handle.result(), seconds)
            print(f"result of {workflow_id}: {result}")
        except TimeoutError:
            print(f"{workflow_id}: no result within {seconds:g} s")
    elif command == "status":
        await print_status(client, workflow_id)
    else:
        sys.exit(f"unknown command {command}")


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
