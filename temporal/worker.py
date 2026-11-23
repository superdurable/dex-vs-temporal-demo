"""Runs the Temporal worker for the conversation workflow variants.

Usage: uv run worker.py
Set TEMPORAL_ADDRESS to reach a server other than localhost:7233.
Set DEMO_CONVERSATION_CODE to v1 (default), v2, or v3 to choose which code
runs the ConversationWorkflow type:
  v1  conversation_workflow.py     the original code
  v2  conversation_workflow_v2.py  adds sentiment analysis, unguarded
  v3  conversation_workflow_v3.py  adds sentiment analysis behind a patch
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    analyze_sentiment,
    generate_reply,
    post_close_summary,
    send_nudge_email,
)
from conversation_shared import TASK_QUEUE
from conversation_workflow_continue_as_new import ConversationWorkflowWithContinueAsNew
from local_file_storage import build_data_converter
from naive_conversation_workflow import NaiveConversationWorkflow

CONVERSATION_MODULE_BY_CODE = {
    "v1": "conversation_workflow",
    "v2": "conversation_workflow_v2",
    "v3": "conversation_workflow_v3",
}


async def run_worker() -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    code_version = os.environ.get("DEMO_CONVERSATION_CODE", "v1")
    conversation_module = importlib.import_module(
        CONVERSATION_MODULE_BY_CODE[code_version]
    )
    # A fixed identity keeps the host name out of recorded histories.
    identity = os.environ.get(
        "DEMO_WORKER_IDENTITY", f"conversation-demo-worker-{code_version}"
    )
    client = await Client.connect(
        address, identity=identity, data_converter=build_data_converter()
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        identity=identity,
        workflows=[
            NaiveConversationWorkflow,
            conversation_module.ConversationWorkflow,
            ConversationWorkflowWithContinueAsNew,
        ],
        activities=[
            generate_reply,
            analyze_sentiment,
            send_nudge_email,
            post_close_summary,
        ],
    )
    logging.info(
        "worker polling task queue %s on %s with ConversationWorkflow code %s (%s.py)",
        TASK_QUEUE,
        address,
        code_version,
        conversation_module.__name__,
    )
    await worker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
