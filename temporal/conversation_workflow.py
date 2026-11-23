"""Conversation workflow with a correct, updatable idle timer.

The idle wait follows the official updatable timer sample:
https://github.com/temporalio/samples-python/blob/7577bd6f52652cb82ad9818074931a794d568a39/updatable_timer/updatable_timer_lib.py
It deliberately has no Continue-As-New, so its history grows with every
message.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import post_close_summary, send_nudge_email
    from conversation_shared import (
        CORRECT_WORKFLOW_TYPE,
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


@workflow.defn(name=CORRECT_WORKFLOW_TYPE)
class ConversationWorkflow:
    @workflow.init
    def __init__(self, input: ConversationInput) -> None:
        self._conversation_id = input.conversation_id
        self._idle_window = timedelta(seconds=input.idle_seconds)
        self._transcript: list[TranscriptMessage] = []
        self._agent = ""
        self._status = STATUS_OPEN
        self._idle_deadline = workflow.now() + self._idle_window
        self._idle_deadline_updated = False

    @workflow.run
    async def run(self, input: ConversationInput) -> ConversationSummary:
        while True:
            await self._sleep_until_idle_deadline()
            self._status = STATUS_NUDGED
            messages_at_nudge = len(self._transcript)
            await workflow.execute_activity(
                send_nudge_email,
                OutboxRequest(self._conversation_id, messages_at_nudge),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )

            # Any message during the second idle window reopens the conversation.
            try:
                await workflow.wait_condition(
                    lambda: len(self._transcript) > messages_at_nudge,
                    timeout=self._idle_window,
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
        return self._summary()

    async def _sleep_until_idle_deadline(self) -> None:
        while True:
            remaining = self._idle_deadline - workflow.now()
            if remaining <= timedelta(0):
                return
            try:
                self._idle_deadline_updated = False
                # Wakes early when a message moves the deadline; then re-arm.
                await workflow.wait_condition(
                    lambda: self._idle_deadline_updated, timeout=remaining
                )
            except asyncio.TimeoutError:
                continue

    @workflow.signal
    def post_message(self, text: str) -> None:
        now = workflow.now()
        self._transcript.append(
            TranscriptMessage(len(self._transcript) + 1, text, now.isoformat())
        )
        self._idle_deadline = now + self._idle_window
        self._idle_deadline_updated = True

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

    def _summary(self) -> ConversationSummary:
        return ConversationSummary(
            self._conversation_id, self._status, self._agent, len(self._transcript)
        )
