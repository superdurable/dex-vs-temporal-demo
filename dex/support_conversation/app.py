"""Composition shared by the Worker and the driver: one Flow instance, one Registry."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from types import ModuleType

from dex import BlobCache, BlobCacheConfig, Flow, Registry, open_blob_cache

from support_conversation.mailer import (
    JsonlOutboxMailer,
    outbox_path_from_environment,
)
from support_conversation.reply_generator import MockReplyGenerator
from support_conversation.sentiment import MockSentimentAnalyzer
from support_conversation.transcript import LocalTranscriptSummarizer, ReplyRequest

DEFAULT_SERVER_ADDRESS = "127.0.0.1:18801"
DEFAULT_RUN_DIRECTORY = "/tmp/dex-video/dex-run"
BLOB_CACHE_BYTES = 1 << 30

# SUPPORT_FLOW_VERSION selects which Flow code this process runs; v1 is the default.
FLOW_VERSION_ENVIRONMENT_VARIABLE = "SUPPORT_FLOW_VERSION"
FLOW_MODULES_BY_VERSION = {
    "v1": "support_conversation.flow",
    "v2": "support_conversation.flow_v2",
    "v2-short-idle": "support_conversation.flow_v2_short_idle",
}


def server_address_from_environment() -> str:
    return os.environ.get("DEX_SERVER_ADDRESS", DEFAULT_SERVER_ADDRESS)


def flow_version_from_environment() -> str:
    version = os.environ.get(FLOW_VERSION_ENVIRONMENT_VARIABLE, "v1")
    if version not in FLOW_MODULES_BY_VERSION:
        raise ValueError(
            f"{FLOW_VERSION_ENVIRONMENT_VARIABLE}={version!r}; "
            f"expected one of {sorted(FLOW_MODULES_BY_VERSION)}"
        )
    return version


def import_flow_module(version: str) -> ModuleType:
    return importlib.import_module(FLOW_MODULES_BY_VERSION[version])


def build_support_conversation_flow(version: str) -> Flow[ReplyRequest]:
    flow_module = import_flow_module(version)
    mailer = JsonlOutboxMailer(outbox_path_from_environment())
    if version == "v1":
        return flow_module.SupportConversationFlow(
            mailer, LocalTranscriptSummarizer(), MockReplyGenerator()
        )
    return flow_module.SupportConversationFlow(
        mailer,
        LocalTranscriptSummarizer(),
        MockReplyGenerator(),
        MockSentimentAnalyzer(),
    )


def build_registry(flow: Flow[ReplyRequest]) -> Registry:
    return Registry([flow], allow_async_handlers=True)


def open_process_blob_cache(role: str) -> tuple[BlobCache, Path]:
    """Open a disposable BlobCache directory owned by this process."""
    run_directory = Path(os.environ.get("DEX_DEMO_RUN_DIR", DEFAULT_RUN_DIRECTORY))
    cache_directory = run_directory / "blob-cache" / f"{role}-{os.getpid()}"
    cache_directory.mkdir(parents=True, exist_ok=True)
    cache = open_blob_cache(BlobCacheConfig(str(cache_directory), BLOB_CACHE_BYTES))
    return cache, cache_directory
