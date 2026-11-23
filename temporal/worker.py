"""Runs the Temporal worker for both conversation workflow variants.

Usage: uv run worker.py
Set TEMPORAL_ADDRESS to reach a server other than localhost:7233.
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import post_close_summary, send_nudge_email
from conversation_shared import TASK_QUEUE
from conversation_workflow import ConversationWorkflow
from naive_conversation_workflow import NaiveConversationWorkflow


async def run_worker() -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[NaiveConversationWorkflow, ConversationWorkflow],
        activities=[send_nudge_email, post_close_summary],
    )
    logging.info("worker polling task queue %s on %s", TASK_QUEUE, address)
    await worker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
