"""Wiring shared by worker.py and the client scripts.

AGENT_REPLY_CODE selects the Flow code: v1 (flow_v1.py, default) or v2 (flow_v2.py).
DEX_SERVER_ADDRESS is the Dex Server gRPC address (required).
AGENT_REPLY_RUN_DIR holds the BlobCache directories (default /tmp/dex-video/two-step).
SEND_REPLY_FAILED_ATTEMPTS (test only) fails the first attempts of SendReplyStep.execute.
"""

import importlib
import os
from pathlib import Path

from dex import (
    AsyncContext,
    BlobCache,
    BlobCacheConfig,
    Registry,
    StepDecision,
    open_blob_cache,
)

from settings import SEND_REPLY_FAILED_ATTEMPTS

CODE = os.environ.get("AGENT_REPLY_CODE", "v1")
SERVER_ADDRESS = os.environ["DEX_SERVER_ADDRESS"]
FLOW = importlib.import_module(f"flow_{CODE}").ReplyFlow()


def wrap_send_reply_for_tests() -> None:
    send_reply_execute = FLOW.send_reply.execute

    # Same Step type and handler; only the first attempts fail, for upgrade tests.
    async def test_send_reply_execute(context: AsyncContext, input: str) -> StepDecision:
        print(f"SendReplyStep.execute {context.flow_id} attempt {context.attempt} started", flush=True)
        if context.attempt <= SEND_REPLY_FAILED_ATTEMPTS:
            raise RuntimeError(f"test failure of SendReplyStep attempt {context.attempt}")
        return send_reply_execute(context, input)

    FLOW.send_reply.execute = test_send_reply_execute


if SEND_REPLY_FAILED_ATTEMPTS:
    wrap_send_reply_for_tests()
REGISTRY = Registry([FLOW], allow_async_handlers=True)


def open_process_blob_cache(role: str) -> BlobCache:
    run_dir = Path(os.environ.get("AGENT_REPLY_RUN_DIR", "/tmp/dex-video/two-step"))
    directory = run_dir / "blob-cache" / f"{role}-{os.getpid()}"
    return open_blob_cache(BlobCacheConfig(str(directory), 64 << 20))
