"""ConversationWorkflow, with the conversation context passed to each reply.

Identical to conversation_workflow.py, except that every reply activity
receives the whole transcript appended so far, as a real model call needs
it. Temporal records activity input in the ActivityTaskScheduled event,
so each reply copies the transcript into history again. No
Continue-As-New and no External Storage.
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        generate_reply_from_context,
        post_close_summary,
        send_nudge_email,
    )
    from conversation_shared import (
        CONTEXT_IN_ACTIVITY_WORKFLOW_TYPE,
        ROLE_ASSISTANT,
        ROLE_CUSTOMER,
        STATUS_CLOSED,
        STATUS_NUDGED,
        STATUS_OPEN,
        ConversationInput,
        ContextReplyRequest,
        ConversationSummary,
        OutboxRequest,
        TranscriptMessage,
        TranscriptPage,
        read_transcript_page,
        summarize_conversation,
    )

ACTIVITY_TIMEOUT = timedelta(seconds=30)


@workflow.defn(name=CONTEXT_IN_ACTIVITY_WORKFLOW_TYPE)
class ConversationWithContextWorkflow:
    @workflow.init
    def __init__(self, input: ConversationInput) -> None:
        self._conversation_id = input.conversation_id
        self._idle_window = timedelta(seconds=input.idle_seconds)
        self._close_after = timedelta(seconds=input.close_after_seconds)
        self._transcript: list[TranscriptMessage] = []
        self._inbox: deque[TranscriptMessage] = deque()
        self._agent = ""
        self._status = STATUS_OPEN
        if input.first_message:
            self._inbox.append(self._append_record(ROLE_CUSTOMER, input.first_message))

    @workflow.run
    async def run(self, input: ConversationInput) -> ConversationSummary:
        # No idle timer before the first reply.
        await workflow.wait_condition(lambda: bool(self._inbox))
        while True:
            await self._answer_inbox()

            # Re-armed after every reply; a new message wakes it early.
            if await self._message_arrives_within(self._idle_window):
                continue

            self._status = STATUS_NUDGED
            await workflow.execute_activity(
                send_nudge_email,
                OutboxRequest(self._conversation_id, len(self._transcript)),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            if await self._message_arrives_within(self._close_after):
                self._status = STATUS_OPEN
                continue

            self._status = STATUS_CLOSED
            await workflow.execute_activity(
                post_close_summary,
                OutboxRequest(self._conversation_id, len(self._transcript)),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            # A message accepted while the summary was posted reopens the conversation.
            if self._inbox:
                self._status = STATUS_OPEN
                continue
            return self.get_summary()

    async def _message_arrives_within(self, window: timedelta) -> bool:
        """Returns True when the inbox holds a message, False when window ends first.

        A message that arrives in the same workflow task as the timer wins.
        """
        try:
            await workflow.wait_condition(lambda: bool(self._inbox), timeout=window)
        except asyncio.TimeoutError:
            pass
        return bool(self._inbox)

    async def _answer_inbox(self) -> None:
        # One reply per customer message, in arrival order.
        while self._inbox:
            customer_message = self._inbox.popleft()
            reply = await workflow.execute_activity(
                generate_reply_from_context,
                ContextReplyRequest(
                    self._conversation_id,
                    customer_message.sequence,
                    list(self._transcript),
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
        self._inbox.append(self._append_record(ROLE_CUSTOMER, text))

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
            len(self._inbox),
        )
