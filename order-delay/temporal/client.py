"""Start, trigger, and wait for OrderWorkflow runs.

  start <workflow-id> <order-id>   start a workflow and print its run ID
  send-now <workflow-id>           send the send_order_now signal
  result <workflow-id>             wait for the result and print it
"""

import asyncio
import os
import sys

from temporalio.client import Client

from settings import TASK_QUEUE


async def main(command: str, workflow_id: str, *args: str) -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address, identity="order-client")
    if command == "start":
        handle = await client.start_workflow(
            "OrderWorkflow", args[0], id=workflow_id, task_queue=TASK_QUEUE
        )
        print(f"started {workflow_id} run {handle.result_run_id}")
    elif command == "send-now":
        await client.get_workflow_handle(workflow_id).signal("send_order_now")
        print(f"sent send_order_now to {workflow_id}")
    elif command == "result":
        result = await client.get_workflow_handle(workflow_id).result()
        print(f"result of {workflow_id}: {result}")
    else:
        sys.exit(f"unknown command {command}")


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
