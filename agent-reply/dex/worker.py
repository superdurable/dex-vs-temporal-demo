"""Run the Dex Worker for ReplyFlow until SIGINT or SIGTERM.

The Worker ID names the Flow code (reply-worker-v1 or reply-worker-v2), so
the Flow history shows which code ran each Step.
"""

import asyncio
import signal

from dex import AsyncWorker, PullWorkerOptions, WorkerOptions

from app import CODE, REGISTRY, SERVER_ADDRESS, open_process_blob_cache


async def run_worker() -> None:
    cache = open_process_blob_cache("worker")
    worker_id = f"reply-worker-{CODE}"
    worker = AsyncWorker(
        REGISTRY,
        cache,
        WorkerOptions(
            server_address=SERVER_ADDRESS,
            pull=PullWorkerOptions(app_worker_id=worker_id),
        ),
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(stop_signal, stop_requested.set)
    worker_task = asyncio.create_task(worker.start())
    try:
        await worker.wait_pull_registered(timeout=30)
        print(f"worker ready with code {CODE} as {worker_id}", flush=True)
        await stop_requested.wait()
    finally:
        await worker.close()
        await worker_task
        cache.close()
        print("worker stopped", flush=True)


if __name__ == "__main__":
    asyncio.run(run_worker())
