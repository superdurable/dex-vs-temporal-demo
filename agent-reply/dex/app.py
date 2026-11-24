"""Wiring shared by worker.py and the client scripts.

AGENT_REPLY_CODE selects the Flow code: v1 (flow_v1.py, default) or v2 (flow_v2.py).
DEX_SERVER_ADDRESS is the Dex Server gRPC address (required).
AGENT_REPLY_RUN_DIR holds the BlobCache directories (default /tmp/dex-video/two-step).
"""

import importlib
import os
from pathlib import Path

from dex import BlobCache, BlobCacheConfig, Registry, open_blob_cache

CODE = os.environ.get("AGENT_REPLY_CODE", "v1")
SERVER_ADDRESS = os.environ["DEX_SERVER_ADDRESS"]
FLOW = importlib.import_module(f"flow_{CODE}").ReplyFlow()
REGISTRY = Registry([FLOW])


def open_process_blob_cache(role: str) -> BlobCache:
    run_dir = Path(os.environ.get("AGENT_REPLY_RUN_DIR", "/tmp/dex-video/two-step"))
    directory = run_dir / "blob-cache" / f"{role}-{os.getpid()}"
    return open_blob_cache(BlobCacheConfig(str(directory), 64 << 20))
