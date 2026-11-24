"""Run the Temporal worker for OrderWorkflow until SIGINT or SIGTERM.

ORDER_CODE selects the workflow code: v1 (flow_v1.py, default) or v2
(flow_v2.py). The worker identity names the code, so the workflow history
shows which code completed each workflow task.
"""

import asyncio
import importlib
import os
import signal

from temporalio.client import Client
from temporalio.worker import Worker

from settings import TASK_QUEUE


async def run_worker() -> None:
    code = os.environ.get("ORDER_CODE", "v1")
    flow = importlib.import_module(f"flow_{code}")
    identity = f"order-worker-{code}"
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address, identity=identity)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        identity=identity,
        workflows=[flow.OrderWorkflow],
        activities=[flow.confirm_order, flow.send_to_kitchen],
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(stop_signal, stop_requested.set)
    async with worker:
        print(f"worker polling with code {code} as {identity}", flush=True)
        await stop_requested.wait()
    print("worker stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(run_worker())
