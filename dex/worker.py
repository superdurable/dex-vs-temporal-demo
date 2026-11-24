"""Run the Dex Worker for SupportConversationFlow until SIGINT or SIGTERM.

Optional memory benchmark switches (unset means unchanged behavior):
  DEMO_MAX_CONCURRENT_TASKS   task slots (default 32)
  DEMO_WORKER_ID_PER_START    1 appends a per-process UUID to the Worker ID
  DEMO_BLOB_CACHE_BYTES       on-disk BlobCache limit (default 1 GiB)
  DEMO_METRICS_BIND           host:port of the worker_metrics.py endpoint
  DEMO_REPLY_BYTES            mock reply size (default 200)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import uuid
from pathlib import Path

from dex import AsyncWorker, PullWorkerOptions, WorkerOptions

from support_conversation.app import (
    build_registry,
    build_support_conversation_flow,
    flow_version_from_environment,
    open_process_blob_cache,
    server_address_from_environment,
)

FLOW_DEFINITIONS_DIRECTORY = Path(__file__).resolve().parent / "flow-definitions"


def worker_id_from_environment(flow_version: str) -> str:
    worker_id = os.environ.get("DEMO_WORKER_ID", f"demo-worker-{flow_version}")
    # A Worker ID must change after a restart, such as a container restart after an OOM kill.
    if os.environ.get("DEMO_WORKER_ID_PER_START") == "1":
        worker_id = f"{worker_id}-{uuid.uuid4().hex}"
    return worker_id


async def run_worker() -> None:
    server_address = server_address_from_environment()
    flow_version = flow_version_from_environment()
    cache, cache_directory = open_process_blob_cache("worker")
    metrics_bind = os.environ.get("DEMO_METRICS_BIND", "")
    if metrics_bind:
        # Imported only here: a Worker without DEMO_METRICS_BIND loads no metrics code.
        from worker_metrics import CountingBlobCache, serve_metrics

        cache = CountingBlobCache(cache, cache_directory)
        serve_metrics(metrics_bind, cache)
    registry = build_registry(build_support_conversation_flow(flow_version))
    worker_id = worker_id_from_environment(flow_version)
    max_concurrent_tasks = int(os.environ.get("DEMO_MAX_CONCURRENT_TASKS", "") or 32)
    if metrics_bind or os.environ.get("DEMO_MAX_CONCURRENT_TASKS"):
        print(
            f"worker {worker_id} max_concurrent_tasks={max_concurrent_tasks} "
            f"max_bytes={cache.config.max_bytes} metrics={metrics_bind or 'off'}",
            flush=True,
        )
    worker = AsyncWorker(
        registry,
        cache,
        WorkerOptions(
            server_address=server_address,
            pull=PullWorkerOptions(
                max_concurrent_tasks=max_concurrent_tasks,
                # The default identity includes the machine host name, which Dex Web shows.
                # The Flow code version in the ID shows in history which code ran a Step.
                app_worker_id=worker_id,
            ),
            # Regenerate after Flow changes: dexcli visualize support_conversation/flow.py
            # --schema-version 2.0 --json --out flow-definitions/v1/SupportConversationFlow
            # Generate later versions after earlier ones: the Server keeps the newer generatedAt.
            flow_definition_paths=(str(FLOW_DEFINITIONS_DIRECTORY / flow_version),),
        ),
    )
    shutdown_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, shutdown_requested.set)
    worker_task = asyncio.create_task(worker.start())
    try:
        await worker.wait_pull_registered(timeout=30)
        print(f"worker ready on {server_address} with Flow code {flow_version}", flush=True)
        await shutdown_requested.wait()
    finally:
        await worker.close()
        await worker_task
        cache.close()
        print("worker stopped", flush=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    # Shows the Flow definition upload result at start.
    logging.getLogger("dex").setLevel(logging.INFO)
    # Also logs an unchanged Flow definition, so d-design.sh can confirm every upload.
    logging.getLogger("dex._flow_definition_upload").setLevel(logging.DEBUG)
    asyncio.run(run_worker())
