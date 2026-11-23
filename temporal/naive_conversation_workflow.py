"""AI support conversation workflow with the idle timer written as a plausible
first implementation: the message handler replies and sets a nudge_at
field, and run sleeps once until nudge_at.

Only the idle timer is naive on purpose: the sleep is computed once, so a
reply that moves nudge_at does not move a sleep that already started. A
message accepted while the nudge or the close summary is sent is still
answered and keeps the conversation open.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import generate_reply, post_close_summary, send_nudge_email
    from conversation_shared import (
        NAIVE_WORKFLOW_TYPE,
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


@workflow.defn(name=NAIVE_WORKFLOW_TYPE)
class NaiveConversationWorkflow:
    @workflow.init
    def __init__(self, input: ConversationInput) -> None:
        self._conversation_id = input.conversation_id
        self._idle_window = timedelta(seconds=input.idle_seconds)
        self._close_after = timedelta(seconds=input.close_after_seconds)
        self._transcript: list[TranscriptMessage] = []
        self._agent = ""
        self._status = STATUS_OPEN
        self._nudge_at: datetime | None = None
        # Replies are generated one at a time, in message order.
        self._reply_lock = asyncio.Lock()
        self._pending_reply_count = 0

    @workflow.run
    async def run(self, input: ConversationInput) -> ConversationSummary:
        if input.first_message:
            await self._reply_to_customer(input.first_message)
        while True:
            await workflow.wait_condition(lambda: self._nudge_at is not None)
            # Sleep until the nudge time set after the latest reply.
            await asyncio.sleep(self._seconds_until(self._nudge_at))
            self._status = STATUS_NUDGED
            # Counted before the nudge, so a message sent during it keeps the conversation open.
            messages_at_nudge = len(self._transcript)
            await workflow.execute_activity(
                send_nudge_email,
                OutboxRequest(self._conversation_id, len(self._transcript)),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )

            # Close the conversation if the customer stays silent.
            await asyncio.sleep(self._close_after.total_seconds())
            if len(self._transcript) != messages_at_nudge:
                # Its reply sets the next nudge_at.
                await workflow.wait_condition(workflow.all_handlers_finished)
                self._status = STATUS_OPEN
                continue

            await workflow.wait_condition(workflow.all_handlers_finished)
            self._status = STATUS_CLOSED
            messages_at_close = len(self._transcript)
            await workflow.execute_activity(
                post_close_summary,
                OutboxRequest(self._conversation_id, len(self._transcript)),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            # A message accepted while the summary was posted reopens the conversation.
            await workflow.wait_condition(workflow.all_handlers_finished)
            if len(self._transcript) == messages_at_close:
                return self.get_summary()
            self._status = STATUS_OPEN

    @workflow.signal
    async def post_message(self, text: str) -> None:
        await self._reply_to_customer(text)

    async def _reply_to_customer(self, text: str) -> None:
        customer_message = self._append_record(ROLE_CUSTOMER, text)
        self._pending_reply_count += 1
        async with self._reply_lock:
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
            self._pending_reply_count -= 1
            # The idle window restarts once the customer has the reply.
            self._nudge_at = workflow.now() + self._idle_window

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

    def _seconds_until(self, moment: datetime) -> float:
        return max(0.0, (moment - workflow.now()).total_seconds())

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
            self._pending_reply_count,
        )
