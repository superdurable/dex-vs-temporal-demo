"""Runs the Temporal worker for the conversation workflow variants.

Usage: uv run --locked worker.py
Set TEMPORAL_ADDRESS to reach a server other than localhost:7233.
Set DEMO_CONVERSATION_CODE to v1 (default), v2, or v3 to choose which code
runs the ConversationWorkflow type:
  v1  conversation_workflow.py     the original code
  v2  conversation_workflow_v2.py  adds sentiment analysis, unguarded
  v3  conversation_workflow_v3.py  adds sentiment analysis behind a patch
The naive, Continue-As-New inline transcript, and context-in-activity
(ConversationWithContextWorkflow) variants are always registered.
Set DEMO_WORKER_PID_FILE to have the worker write its process ID there, so
scripts can stop exactly this process. SIGINT or SIGTERM stops it.

Optional memory benchmark switches (unset means unchanged behavior):
  DEMO_WORKER_IDENTITY                 worker identity (default conversation-demo-worker-<code>)
  DEMO_MAX_CACHED_WORKFLOWS            sticky cache size in workflows (SDK default 1000)
  DEMO_MAX_CONCURRENT_WORKFLOW_TASKS   workflow task slots
  DEMO_MAX_CONCURRENT_ACTIVITIES       activity task slots
  DEMO_METRICS_BIND                    Prometheus bind address, for example 0.0.0.0:9464
  DEMO_REPLY_BYTES                     mock reply size (default 200)
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import signal

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from activities import (
    analyze_sentiment,
    generate_reply,
    post_close_summary,
    send_nudge_email,
)
from context_reply_activity import generate_reply_from_context
from conversation_shared import TASK_QUEUE
from conversation_workflow_inline_transcript import (
    ConversationWorkflowContinueAsNewInlineTranscript,
)
from conversation_workflow_with_context import ConversationWithContextWorkflow
from local_file_storage import build_data_converter
from naive_conversation_workflow import NaiveConversationWorkflow

CONVERSATION_MODULE_BY_CODE = {
    "v1": "conversation_workflow",
    "v2": "conversation_workflow_v2",
    "v3": "conversation_workflow_v3",
}


def optional_int_from_environment(name: str) -> int | None:
    value = os.environ.get(name, "")
    return int(value) if value else None


def build_worker_options() -> dict[str, int]:
    options: dict[str, int] = {}
    for option, variable in (
        ("max_cached_workflows", "DEMO_MAX_CACHED_WORKFLOWS"),
        ("max_concurrent_workflow_tasks", "DEMO_MAX_CONCURRENT_WORKFLOW_TASKS"),
        ("max_concurrent_activities", "DEMO_MAX_CONCURRENT_ACTIVITIES"),
    ):
        value = optional_int_from_environment(variable)
        if value is not None:
            options[option] = value
    return options


def build_runtime(metrics_bind: str) -> Runtime | None:
    if not metrics_bind:
        return None
    return Runtime(
        telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=metrics_bind))
    )


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
    metrics_bind = os.environ.get("DEMO_METRICS_BIND", "")
    client = await Client.connect(
        address,
        identity=identity,
        data_converter=build_data_converter(),
        runtime=build_runtime(metrics_bind),
    )
    worker_options = build_worker_options()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        identity=identity,
        workflows=[
            NaiveConversationWorkflow,
            conversation_module.ConversationWorkflow,
            ConversationWorkflowContinueAsNewInlineTranscript,
            ConversationWithContextWorkflow,
        ],
        activities=[
            generate_reply,
            generate_reply_from_context,
            analyze_sentiment,
            send_nudge_email,
            post_close_summary,
        ],
        **worker_options,
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(stop_signal, stop_requested.set)
    async with worker:
        logging.info(
            "worker polling task queue %s on %s with ConversationWorkflow code %s (%s.py)",
            TASK_QUEUE,
            address,
            code_version,
            conversation_module.__name__,
        )
        if worker_options or metrics_bind:
            logging.info(
                "worker options %s metrics %s", worker_options, metrics_bind or "off"
            )
        await stop_requested.wait()
    logging.info("worker stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    pid_file = os.environ.get("DEMO_WORKER_PID_FILE", "")
    if pid_file:
        with open(pid_file, "w", encoding="utf-8") as pid_output:
            pid_output.write(f"{os.getpid()}\n")
    asyncio.run(run_worker())
