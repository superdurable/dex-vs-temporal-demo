"""AI support conversation workflow that continues as new and carries its
whole transcript inline to the next run.

Same behavior as ConversationWorkflow, plus the Continue-As-New steps from
the official guidance:
https://docs.temporal.io/develop/python/continue-as-new
https://docs.temporal.io/develop/python/message-passing

Scope: this variant keeps the complete transcript in workflow state, so the
input of every new run carries all of it and grows with every exchange. It
bounds the event count of each run, not the size of the carried input. A
design that stores the transcript outside the workflow and carries only a
cursor and the unanswered messages is bounded in both; External Storage
(local_file_storage.py) moves the large input out of history without
changing this code.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import generate_reply, post_close_summary, send_nudge_email
    from conversation_shared import (
        DEFAULT_CLOSE_AFTER_SECONDS,
        DEFAULT_IDLE_SECONDS,
        INLINE_TRANSCRIPT_WORKFLOW_TYPE,
        ROLE_ASSISTANT,
        ROLE_CUSTOMER,
        STATUS_CLOSED,
        STATUS_NUDGED,
        STATUS_OPEN,
        ConversationSummary,
        OutboxRequest,
        ReplyRequest,
        TranscriptMessage,
        TranscriptPage,
        read_transcript_page,
        summarize_conversation,
    )

ACTIVITY_TIMEOUT = timedelta(seconds=30)
MINIMUM_TIMER = timedelta(milliseconds=1)


@dataclass
class CarriedConversationState:
    """Everything the next run needs to continue the conversation."""

    # The whole transcript, carried inline in the next run's input.
    transcript: list[TranscriptMessage] = field(default_factory=list)
    # Customer messages accepted but not answered yet.
    pending_sequences: list[int] = field(default_factory=list)
    agent: str = ""
    status: str = STATUS_OPEN
    # Absolute end of the idle or close window; empty before the first reply.
    deadline: str = ""
    run_number: int = 1


@dataclass
class ConversationInlineTranscriptInput:
    conversation_id: str
    first_message: str
    idle_seconds: int = DEFAULT_IDLE_SECONDS
    close_after_seconds: int = DEFAULT_CLOSE_AFTER_SECONDS
    carried: CarriedConversationState | None = None


@workflow.defn(name=INLINE_TRANSCRIPT_WORKFLOW_TYPE)
class ConversationWorkflowContinueAsNewInlineTranscript:
    @workflow.init
    def __init__(self, input: ConversationInlineTranscriptInput) -> None:
        self._conversation_id = input.conversation_id
        self._idle_window = timedelta(seconds=input.idle_seconds)
        self._close_after = timedelta(seconds=input.close_after_seconds)
        self._transcript: list[TranscriptMessage] = []
        self._inbox: deque[TranscriptMessage] = deque()
        self._agent = ""
        self._status = STATUS_OPEN
        self._deadline: datetime | None = None
        self._run_number = 1
        if input.carried is not None:
            # A continued run rebuilds its state; the first message is already in it.
            carried = input.carried
            self._transcript = list(carried.transcript)
            self._inbox = deque(
                self._transcript[sequence - 1] for sequence in carried.pending_sequences
            )
            self._agent = carried.agent
            self._status = carried.status
            if carried.deadline:
                self._deadline = datetime.fromisoformat(carried.deadline)
            self._run_number = carried.run_number + 1
        elif input.first_message:
            self._inbox.append(self._append_record(ROLE_CUSTOMER, input.first_message))

    @workflow.run
    async def run(self, input: ConversationInlineTranscriptInput) -> ConversationSummary:
        if self._deadline is None:
            # No idle timer before the first reply.
            await workflow.wait_condition(
                lambda: bool(self._inbox) or self._should_continue_as_new()
            )
        while True:
            await self._answer_inbox()
            if self._should_continue_as_new():
                await self._continue_as_new(input)
            if self._inbox:
                continue

            # Waits until the carried deadline; a message or a suggestion wakes it early.
            await self._wait_until_deadline()
            if self._should_continue_as_new():
                await self._continue_as_new(input)
            # A message that arrives in the same workflow task as the timer wins.
            if self._inbox:
                self._status = STATUS_OPEN
                continue

            if self._status != STATUS_NUDGED:
                self._status = STATUS_NUDGED
                await workflow.execute_activity(
                    send_nudge_email,
                    OutboxRequest(self._conversation_id, len(self._transcript)),
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                )
                self._deadline = workflow.now() + self._close_after
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

    async def _answer_inbox(self) -> None:
        # One reply per customer message, in arrival order.
        while self._inbox:
            # A flood may never empty the inbox, so check between replies.
            if self._should_continue_as_new():
                return
            customer_message = self._inbox.popleft()
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
            self._status = STATUS_OPEN
            self._deadline = workflow.now() + self._idle_window

    async def _wait_until_deadline(self) -> None:
        try:
            await workflow.wait_condition(
                lambda: bool(self._inbox) or self._should_continue_as_new(),
                timeout=self._time_until_deadline(),
            )
        except asyncio.TimeoutError:
            pass

    def _should_continue_as_new(self) -> bool:
        return workflow.info().is_continue_as_new_suggested()

    async def _continue_as_new(self, input: ConversationInlineTranscriptInput) -> None:
        # Handlers must finish first; messages they accept stay in the inbox.
        await workflow.wait_condition(workflow.all_handlers_finished)
        workflow.logger.info(
            "continuing as new after run %d with %d messages, %d pending",
            self._run_number,
            len(self._transcript),
            len(self._inbox),
        )
        workflow.continue_as_new(
            ConversationInlineTranscriptInput(
                self._conversation_id,
                input.first_message,
                input.idle_seconds,
                input.close_after_seconds,
                CarriedConversationState(
                    self._transcript,
                    [message.sequence for message in self._inbox],
                    self._agent,
                    self._status,
                    self._deadline.isoformat() if self._deadline else "",
                    self._run_number,
                ),
            )
        )

    def _time_until_deadline(self) -> timedelta:
        assert self._deadline is not None
        return max(self._deadline - workflow.now(), MINIMUM_TIMER)

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
    def get_run_number(self) -> int:
        return self._run_number

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
