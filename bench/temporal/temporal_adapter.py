"""Temporal implementation of bench/shared/conversation_adapter.py.

Each conversation is one ConversationWorkflow. A reply is visible when the
generate_reply activity has completed and the workflow task that appends the
reply to the transcript has completed. The adapter sees this by long-polling
the workflow history (wait_new_event), so observing a reply never sends a
query and never makes the worker load the workflow.

Environment:
  TEMPORAL_ADDRESS          server address (default localhost:7233)
  DEMO_CLIENT_IDENTITY      fixed client identity (default memory-bench-driver)
  DEMO_IDLE_SECONDS         idle window and close-after window (default 86400)
Requires the repository's temporal/ directory on PYTHONPATH.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TypeVar

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryReverseRequest,
    ResetStickyTaskQueueRequest,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.client import Client

from conversation_shared import CORRECT_WORKFLOW_TYPE, TASK_QUEUE, ConversationInput

HISTORY_PAGE_SIZE = 1000
# Server-side long polls end after about 20 s; keep each RPC deadline above it.
LONG_POLL_RPC_TIMEOUT = timedelta(seconds=30)
RETRYABLE_STATUSES = (
    RPCStatusCode.RESOURCE_EXHAUSTED,
    RPCStatusCode.DEADLINE_EXCEEDED,
    RPCStatusCode.UNAVAILABLE,
)
BACKOFF_INITIAL_SECONDS = 0.05
BACKOFF_MAX_SECONDS = 2.0
T = TypeVar("T")


@dataclass
class ConversationHistoryCursor:
    next_page_token: bytes = b""
    activity_completed_since_post: bool = False


class TemporalConversationAdapter:
    engine_name = "temporal"

    def __init__(self) -> None:
        self._address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
        self._identity = os.environ.get("DEMO_CLIENT_IDENTITY", "memory-bench-driver")
        self._idle_seconds = int(os.environ.get("DEMO_IDLE_SECONDS", "86400"))
        self._client: Client | None = None
        self._cursors: dict[str, ConversationHistoryCursor] = {}

    @property
    def client(self) -> Client:
        if self._client is None:
            raise RuntimeError("connect() was not called")
        return self._client

    async def connect(self) -> None:
        self._client = await Client.connect(self._address, identity=self._identity)

    async def close(self) -> None:
        self._client = None

    async def start_conversation(self, conversation_id: str, first_message: str) -> None:
        self._cursors[conversation_id] = ConversationHistoryCursor()
        await call_with_backoff(
            lambda: self.client.start_workflow(
                CORRECT_WORKFLOW_TYPE,
                ConversationInput(
                    conversation_id, first_message, self._idle_seconds, self._idle_seconds
                ),
                id=conversation_id,
                task_queue=TASK_QUEUE,
            )
        )

    async def observe_conversation(self, conversation_id: str) -> None:
        cursor = ConversationHistoryCursor()
        # A worker that died earlier may still own the sticky queue; without
        # this, the first task waits for the sticky schedule-to-start timeout.
        await call_with_backoff(
            lambda: self.client.workflow_service.reset_sticky_task_queue(
                ResetStickyTaskQueueRequest(
                    namespace=self.client.namespace,
                    execution=WorkflowExecution(workflow_id=conversation_id),
                )
            )
        )
        last_event_id = await self._read_last_event_id(conversation_id)
        # Waiting pages always carry a token, so the cursor can keep following.
        seen_event_id = 0
        while seen_event_id < last_event_id:
            response = await self._read_history_page(conversation_id, cursor, wait_new_event=True)
            if response.next_page_token:
                cursor.next_page_token = response.next_page_token
            if response.history.events:
                seen_event_id = response.history.events[-1].event_id
        self._cursors[conversation_id] = cursor

    async def post_message(self, conversation_id: str, text: str) -> None:
        cursor = self._cursors.get(conversation_id)
        if cursor is None:
            await self.observe_conversation(conversation_id)
            cursor = self._cursors[conversation_id]
        cursor.activity_completed_since_post = False
        handle = self.client.get_workflow_handle(conversation_id)
        await call_with_backoff(lambda: handle.signal("post_message", text))

    async def wait_for_reply(self, conversation_id: str, timeout_seconds: float) -> None:
        await asyncio.wait_for(self._follow_history_until_reply(conversation_id), timeout_seconds)

    async def _follow_history_until_reply(self, conversation_id: str) -> None:
        cursor = self._cursors[conversation_id]
        while True:
            response = await self._read_history_page(conversation_id, cursor, wait_new_event=True)
            if response.next_page_token:
                cursor.next_page_token = response.next_page_token
            for event in response.history.events:
                if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
                    cursor.activity_completed_since_post = True
                elif (
                    event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
                    and cursor.activity_completed_since_post
                ):
                    cursor.activity_completed_since_post = False
                    return

    async def _read_last_event_id(self, conversation_id: str) -> int:
        response = await call_with_backoff(
            lambda: self.client.workflow_service.get_workflow_execution_history_reverse(
                GetWorkflowExecutionHistoryReverseRequest(
                    namespace=self.client.namespace,
                    execution=WorkflowExecution(workflow_id=conversation_id),
                    maximum_page_size=1,
                )
            )
        )
        return response.history.events[0].event_id if response.history.events else 0

    async def _read_history_page(
        self, conversation_id: str, cursor: ConversationHistoryCursor, wait_new_event: bool
    ):
        return await call_with_backoff(
            lambda: self.client.workflow_service.get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest(
                    namespace=self.client.namespace,
                    execution=WorkflowExecution(workflow_id=conversation_id),
                    maximum_page_size=HISTORY_PAGE_SIZE,
                    next_page_token=cursor.next_page_token,
                    wait_new_event=wait_new_event,
                ),
                timeout=LONG_POLL_RPC_TIMEOUT,
            )
        )


async def call_with_backoff(make_call: Callable[[], Awaitable[T]]) -> T:
    """Retries calls the server rejected for load, such as its default namespace rate limit."""
    delay_seconds = BACKOFF_INITIAL_SECONDS
    while True:
        try:
            return await make_call()
        except RPCError as error:
            if error.status not in RETRYABLE_STATUSES:
                raise
        await asyncio.sleep(delay_seconds * (0.5 + random.random()))
        delay_seconds = min(delay_seconds * 2, BACKOFF_MAX_SECONDS)
