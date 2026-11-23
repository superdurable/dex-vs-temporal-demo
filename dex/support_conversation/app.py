"""Composition shared by the Worker and the driver: one Flow instance, one Registry."""

from __future__ import annotations

import os
from pathlib import Path

from dex import BlobCache, BlobCacheConfig, Registry, open_blob_cache

from support_conversation.flow import SupportConversationFlow
from support_conversation.mailer import (
    JsonlOutboxMailer,
    outbox_path_from_environment,
)
from support_conversation.transcript import LocalTranscriptSummarizer

DEFAULT_SERVER_ADDRESS = "127.0.0.1:18801"
DEFAULT_RUN_DIRECTORY = "/tmp/dex-video/dex-run"
BLOB_CACHE_BYTES = 1 << 30


def server_address_from_environment() -> str:
    return os.environ.get("DEX_SERVER_ADDRESS", DEFAULT_SERVER_ADDRESS)


def build_support_conversation_flow() -> SupportConversationFlow:
    return SupportConversationFlow(
        JsonlOutboxMailer(outbox_path_from_environment()),
        LocalTranscriptSummarizer(),
    )


def build_registry(flow: SupportConversationFlow) -> Registry:
    return Registry([flow], allow_async_handlers=True)


def open_process_blob_cache(role: str) -> tuple[BlobCache, Path]:
    """Open a disposable BlobCache directory owned by this process."""
    run_directory = Path(os.environ.get("DEX_DEMO_RUN_DIR", DEFAULT_RUN_DIRECTORY))
    cache_directory = run_directory / "blob-cache" / f"{role}-{os.getpid()}"
    cache_directory.mkdir(parents=True, exist_ok=True)
    cache = open_blob_cache(BlobCacheConfig(str(cache_directory), BLOB_CACHE_BYTES))
    return cache, cache_directory
