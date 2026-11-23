"""Run the Dex Worker for SupportConversationFlow until SIGINT or SIGTERM."""

from __future__ import annotations

import asyncio
import signal
from pathlib import Path

from dex import AsyncWorker, PullWorkerOptions, WorkerOptions

from support_conversation.app import (
    build_registry,
    build_support_conversation_flow,
    open_process_blob_cache,
    server_address_from_environment,
)

FLOW_DEFINITIONS_DIRECTORY = Path(__file__).resolve().parent / "flow-definitions"


async def run_worker() -> None:
    server_address = server_address_from_environment()
    cache, _ = open_process_blob_cache("worker")
    registry = build_registry(build_support_conversation_flow())
    worker = AsyncWorker(
        registry,
        cache,
        WorkerOptions(
            server_address=server_address,
            pull=PullWorkerOptions(max_concurrent_tasks=32),
            # Regenerate after Flow changes: dexcli visualize support_conversation/flow.py
            # --schema-version 2.0 --json --out flow-definitions/SupportConversationFlow
            flow_definition_paths=(str(FLOW_DEFINITIONS_DIRECTORY),),
        ),
    )
    shutdown_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, shutdown_requested.set)
    worker_task = asyncio.create_task(worker.start())
    try:
        await worker.wait_pull_registered(timeout=30)
        print(f"worker ready on {server_address}", flush=True)
        await shutdown_requested.wait()
    finally:
        await worker.close()
        await worker_task
        cache.close()
        print("worker stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(run_worker())
