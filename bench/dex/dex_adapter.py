"""Dex implementation of bench/shared/conversation_adapter.py.

Each conversation is one SupportConversationFlow (Flow code v1). The n-th
customer message of a conversation is answered by execution n of
GenerateReplyStep, which writes the message and the reply to the transcript
in the same commit. A reply is visible to a reader once that Step execution
has completed. The adapter waits for it with the Client's Step-completion
wait, a long poll that Dex Server answers, and then confirms it with one
Client Attribute read of ConversationState. Neither call runs on the Worker,
so observing a reply never makes the Worker load the conversation.

Posting a message is the Flow's post_message RPC, which runs on the Worker.

Environment:
  DEX_SERVER_ADDRESS     Dex Server address (default 127.0.0.1:18801)
  DEX_DEMO_RUN_DIR       directory for the driver's BlobCache and outbox file
  DEMO_IDLE_SECONDS      idle window and close-after window (default 86400)
  DEMO_POST_DEADLINE_SECONDS  give up posting one message after this (default 300)
Requires the repository's dex/ directory on PYTHONPATH.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from datetime import timedelta

import grpc
from dex import (
    AsyncClient,
    ClientOptions,
    DexServiceError,
    RequestTimeoutError,
    RPCInvokeOptions,
    StartFlowOptions,
    StepExecutionId,
    WaitForStepCompletionOptions,
)

from support_conversation.app import (
    build_registry,
    build_support_conversation_flow,
    open_process_blob_cache,
    server_address_from_environment,
)
from support_conversation.flow import CONVERSATION_STATE, TRANSCRIPT_CHUNKS
from support_conversation.transcript import (
    CURRENT_CHUNK_TOKEN,
    ConversationSettings,
    ConversationState,
    ReplyRequest,
    TranscriptChunk,
)

REPLY_STEP_TYPE = "GenerateReplyStep"
# One RPC attempt; a timed-out attempt has an unknown outcome and is retried
# with the same request ID, so the message is queued at most once.
RPC_ATTEMPT_SECONDS = 15
RETRYABLE_CODES = (
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
)
BACKOFF_INITIAL_SECONDS = 0.05
BACKOFF_MAX_SECONDS = 2.0


def is_retryable(error: BaseException) -> bool:
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return True
    return isinstance(error, DexServiceError) and error.code in RETRYABLE_CODES


class DexConversationAdapter:
    engine_name = "dex"

    def __init__(self) -> None:
        self._server_address = server_address_from_environment()
        self._idle_seconds = int(os.environ.get("DEMO_IDLE_SECONDS", "86400"))
        self._post_deadline_seconds = float(
            os.environ.get("DEMO_POST_DEADLINE_SECONDS", "300")
        )
        self._flow = build_support_conversation_flow("v1")
        self._client: AsyncClient | None = None
        self._cache = None
        # Execution number of GenerateReplyStep that answers the latest posted message.
        self._expected_reply: dict[str, int] = {}

    @property
    def client(self) -> AsyncClient:
        if self._client is None:
            raise RuntimeError("connect() was not called")
        return self._client

    async def connect(self) -> None:
        self._cache, _ = open_process_blob_cache("bench-driver")
        self._client = AsyncClient(
            build_registry(self._flow),
            self._cache,
            ClientOptions(server_address=self._server_address),
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._cache is not None:
            self._cache.close()
            self._cache = None

    async def start_conversation(self, conversation_id: str, first_message: str) -> None:
        options = (
            StartFlowOptions(
                request_id=f"start-{conversation_id}", ignore_already_started=True
            )
            .with_attribute(CONVERSATION_STATE, ConversationState())
            .with_attribute(TRANSCRIPT_CHUNKS, CURRENT_CHUNK_TOKEN, TranscriptChunk())
        )
        request = ReplyRequest(
            ConversationSettings(self._idle_seconds, self._idle_seconds), [first_message]
        )
        self._expected_reply[conversation_id] = 1
        await self._call_with_retries(
            lambda: self.client.start_flow(self._flow, conversation_id, request, options)
        )

    async def observe_conversation(self, conversation_id: str) -> None:
        state = await self._read_state(conversation_id)
        self._expected_reply[conversation_id] = state.assistant_reply_count

    async def post_message(self, conversation_id: str, text: str) -> None:
        if conversation_id not in self._expected_reply:
            await self.observe_conversation(conversation_id)
        message_number = self._expected_reply[conversation_id] + 1
        self._expected_reply[conversation_id] = message_number
        options = RPCInvokeOptions(request_id=f"{conversation_id}/message/{message_number}")
        await self._call_with_retries(
            lambda: self.client.invoke_rpc(
                self._flow.post_message, conversation_id, text, options=options
            )
        )

    async def wait_for_reply(self, conversation_id: str, timeout_seconds: float) -> None:
        message_number = self._expected_reply[conversation_id]
        try:
            await self.client.wait_for_step_completion(
                conversation_id,
                StepExecutionId(REPLY_STEP_TYPE, message_number),
                WaitForStepCompletionOptions(
                    request_timeout=timedelta(seconds=timeout_seconds)
                ),
            )
        except RequestTimeoutError as error:
            raise TimeoutError(
                f"{REPLY_STEP_TYPE}-{message_number} not completed in {timeout_seconds}s"
            ) from error
        state = await self._read_state(conversation_id)
        if state.assistant_reply_count < message_number:
            raise RuntimeError(
                f"{REPLY_STEP_TYPE}-{message_number} completed but the transcript has "
                f"{state.assistant_reply_count} replies"
            )

    async def _read_state(self, conversation_id: str) -> ConversationState:
        values = await self._call_with_retries(
            lambda: self.client.get_attributes(conversation_id, CONVERSATION_STATE)
        )
        return values.get(CONVERSATION_STATE) or ConversationState()

    async def _call_with_retries(self, make_call):
        deadline = time.monotonic() + self._post_deadline_seconds
        delay_seconds = BACKOFF_INITIAL_SECONDS
        while True:
            try:
                return await asyncio.wait_for(make_call(), RPC_ATTEMPT_SECONDS)
            except Exception as error:
                if not is_retryable(error) or time.monotonic() >= deadline:
                    raise
            await asyncio.sleep(delay_seconds * (0.5 + random.random()))
            delay_seconds = min(delay_seconds * 2, BACKOFF_MAX_SECONDS)
