"""AI support conversation workflow with a correct idle timer.

The idle timer is re-armed after every AI reply, following the updatable
timer technique of the official sample:
https://github.com/temporalio/samples-python/blob/7577bd6f52652cb82ad9818074931a794d568a39/updatable_timer/updatable_timer_lib.py
It deliberately has no Continue-As-New, so its history grows with every
message.
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import generate_reply, post_close_summary, send_nudge_email
    from conversation_shared import (
        CORRECT_WORKFLOW_TYPE,
        ROLE_ASSISTANT,
        ROLE_CUSTOMER,
        STATUS_CLOSED,
        STATUS_NUDGED,
        STATUS_OPEN,
        ConversationInput,
        ConversationSummary,
        OutboxRequest,
        ReplyRequest,
        TranscriptMessage,
        TranscriptPage,
        read_transcript_page,
        summarize_conversation,
    )

ACTIVITY_TIMEOUT = timedelta(seconds=30)


@workflow.defn(name=CORRECT_WORKFLOW_TYPE)
class ConversationWorkflow:
    @workflow.init
    def __init__(self, input: ConversationInput) -> None:
        self._conversation_id = input.conversation_id
        self._idle_window = timedelta(seconds=input.idle_seconds)
        self._close_after = timedelta(seconds=input.close_after_seconds)
        self._transcript: list[TranscriptMessage] = []
        self._unanswered_messages: deque[TranscriptMessage] = deque()
        self._agent = ""
        self._status = STATUS_OPEN

    @workflow.run
    async def run(self, input: ConversationInput) -> ConversationSummary:
        while True:
            await self._reply_until_idle()
            self._status = STATUS_NUDGED
            await workflow.execute_activity(
                send_nudge_email,
                OutboxRequest(self._conversation_id, len(self._transcript)),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )

            # Any customer message before the close reopens the conversation.
            try:
                await workflow.wait_condition(
                    lambda: bool(self._unanswered_messages),
                    timeout=self._close_after,
                )
            except asyncio.TimeoutError:
                break
            self._status = STATUS_OPEN

        self._status = STATUS_CLOSED
        await workflow.execute_activity(
            post_close_summary,
            OutboxRequest(self._conversation_id, len(self._transcript)),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        return self.get_summary()

    async def _reply_until_idle(self) -> None:
        while True:
            await self._reply_to_unanswered_messages()
            # Re-arm the idle timer after the reply; a new message wakes it early.
            try:
                await workflow.wait_condition(
                    lambda: bool(self._unanswered_messages),
                    timeout=self._idle_window,
                )
            except asyncio.TimeoutError:
                return

    async def _reply_to_unanswered_messages(self) -> None:
        # One reply per customer message, in arrival order.
        while self._unanswered_messages:
            customer_message = self._unanswered_messages.popleft()
            reply = await workflow.execute_activity(
                generate_reply,
                ReplyRequest(
                    self._conversation_id,
                    customer_message.sequence,
                    customer_message.text,
                ),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            self._append_record(ROLE_ASSISTANT, reply.text, reply.tool_calls)

    def _append_record(
        self, role: str, text: str, tool_calls: list[str] | None = None
    ) -> TranscriptMessage:
        record = TranscriptMessage(
            len(self._transcript) + 1,
            role,
            text,
            workflow.now().isoformat(),
            tool_calls or [],
        )
        self._transcript.append(record)
        return record

    @workflow.signal
    def post_message(self, text: str) -> None:
        self._unanswered_messages.append(self._append_record(ROLE_CUSTOMER, text))

    @workflow.update
    def assign_agent(self, name: str) -> str:
        previous_agent = self._agent
        self._agent = name
        return previous_agent

    @workflow.query
    def get_transcript_page(self, page_token: str) -> TranscriptPage:
        return read_transcript_page(self._transcript, page_token)

    @workflow.query
    def get_summary(self) -> ConversationSummary:
        return summarize_conversation(
            self._conversation_id,
            self._status,
            self._agent,
            self._transcript,
            len(self._unanswered_messages),
        )
