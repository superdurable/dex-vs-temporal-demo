"""Run the Temporal worker for ReplyWorkflow until SIGINT or SIGTERM.

AGENT_REPLY_CODE selects the workflow code module: v1 (flow_v1.py, default),
v2 (flow_v2.py), or v2_no_patch (flow_v2_no_patch.py). The worker identity
names the code, so the workflow history shows which code completed each
workflow task.
"""

import asyncio
import importlib
import logging
import os
import signal

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from settings import SEND_REPLY_FAILED_ATTEMPTS, TASK_QUEUE


def build_send_reply(flow):
    if not SEND_REPLY_FAILED_ATTEMPTS:
        return flow.send_reply

    # Same activity name; only the first attempts fail, for upgrade tests.
    @activity.defn(name="send_reply")
    async def test_send_reply(draft: str) -> str:
        info = activity.info()
        print(f"send_reply {info.workflow_id} attempt {info.attempt} started", flush=True)
        if info.attempt <= SEND_REPLY_FAILED_ATTEMPTS:
            raise RuntimeError(f"test failure of send_reply attempt {info.attempt}")
        return await flow.send_reply(draft)

    return test_send_reply


async def run_worker() -> None:
    code = os.environ.get("AGENT_REPLY_CODE", "v1")
    flow = importlib.import_module(f"flow_{code}")
    identity = f"reply-worker-{code}"
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address, identity=identity)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        identity=identity,
        workflows=[flow.ReplyWorkflow],
        activities=[flow.draft_reply, build_send_reply(flow)]
        + ([flow.notify_reviewer] if hasattr(flow, "notify_reviewer") else []),
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
    # Shows workflow task failures, such as non-determinism errors, in the worker log.
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run_worker())
