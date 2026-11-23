"""Conversation workflow with the idle timer written the way most people first
write it: compute the delay, sleep once, and let the signal handler move a
deadline field.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import post_close_summary, send_nudge_email
    from conversation_shared import (
        NAIVE_WORKFLOW_TYPE,
        STATUS_CLOSED,
        STATUS_NUDGED,
        STATUS_OPEN,
        ConversationInput,
        ConversationSummary,
        OutboxRequest,
        TranscriptMessage,
        TranscriptPage,
        read_transcript_page,
    )

ACTIVITY_TIMEOUT = timedelta(seconds=30)


@workflow.defn(name=NAIVE_WORKFLOW_TYPE)
class NaiveConversationWorkflow:
    @workflow.init
    def __init__(self, input: ConversationInput) -> None:
        self._conversation_id = input.conversation_id
        self._idle_window = timedelta(seconds=input.idle_seconds)
        self._transcript: list[TranscriptMessage] = []
        self._agent = ""
        self._status = STATUS_OPEN
        self._idle_deadline = workflow.now() + self._idle_window

    @workflow.run
    async def run(self, input: ConversationInput) -> ConversationSummary:
        while True:
            # Wait until the conversation has been idle for the idle window.
            await asyncio.sleep(self._seconds_until_idle_deadline())
            self._status = STATUS_NUDGED
            messages_at_nudge = len(self._transcript)
            await workflow.execute_activity(
                send_nudge_email,
                OutboxRequest(self._conversation_id, messages_at_nudge),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )

            # Give the customer one more idle window before closing.
            self._idle_deadline = workflow.now() + self._idle_window
            await asyncio.sleep(self._seconds_until_idle_deadline())
            if len(self._transcript) == messages_at_nudge:
                break
            self._status = STATUS_OPEN

        self._status = STATUS_CLOSED
        await workflow.execute_activity(
            post_close_summary,
            OutboxRequest(self._conversation_id, len(self._transcript)),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
        )
        return self._summary()

    @workflow.signal
    def post_message(self, text: str) -> None:
        now = workflow.now()
        self._transcript.append(
            TranscriptMessage(len(self._transcript) + 1, text, now.isoformat())
        )
        # A new message pushes the idle deadline out by one idle window.
        self._idle_deadline = now + self._idle_window

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
        return self._summary()

    def _seconds_until_idle_deadline(self) -> float:
        return (self._idle_deadline - workflow.now()).total_seconds()

    def _summary(self) -> ConversationSummary:
        return ConversationSummary(
            self._conversation_id, self._status, self._agent, len(self._transcript)
        )
