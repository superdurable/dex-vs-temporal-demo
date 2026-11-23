"""SupportConversationFlow: one long-lived Flow per customer support conversation."""

from __future__ import annotations

from datetime import timedelta

from dex import (
    Context,
    Flow,
    PersistenceSchema,
    RPCResult,
    Step,
    StepDecision,
    StepList,
    StepMovement,
    StepOptions,
    Timer,
    Wait,
    force_complete_if_channels_empty,
    go_to,
    go_to_many,
    rpc,
)

from support_conversation.mailer import ConversationMailer
from support_conversation.transcript import (
    ASSIGNED_AGENT,
    CONVERSATION_STATE,
    CURRENT_CHUNK_TOKEN,
    CUSTOMER_MESSAGES,
    STATUS_CLOSED,
    STATUS_NUDGED,
    STATUS_WAITING,
    SUMMARY,
    TRANSCRIPT_CHUNK_SIZE,
    TRANSCRIPT_CHUNKS,
    ConversationStart,
    TranscriptPage,
    TranscriptSummarizer,
    append_messages_to_transcript,
    archived_chunk_token,
    parse_transcript_page_token,
    waiting_step_options,
)

MAX_MESSAGE_CHARACTERS = 4000
MAX_AGENT_NAME_CHARACTERS = 200

# Movements into a waiting Step carry options that also load the chunk the
# next archive may evict; see waiting_step_options.


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Wait for customer messages, record them, and restart the idle timer after each batch."
class WaitForCustomerStep(Step[ConversationStart]):
    def __init__(self, summarizer: TranscriptSummarizer) -> None:
        self.summarizer = summarizer

    def get_step_options(self) -> StepOptions:
        return waiting_step_options(None)

    def wait_for(self, context: Context, input: ConversationStart) -> Wait:
        # The Channel is declared first, so queued messages win over a due Timer.
        return Wait.any_of(
            CUSTOMER_MESSAGES.for_range(at_least=1, at_most=TRANSCRIPT_CHUNK_SIZE),
            Timer.by_duration(timedelta(seconds=input.idle_seconds)),
        )

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        if context.has_timer_fired():
            return go_to(NudgeCustomerStep, input)
        state = append_messages_to_transcript(
            context, list(CUSTOMER_MESSAGES.results(context)), self.summarizer
        )
        return go_to_many(
            StepMovement.of(
                WaitForCustomerStep, input, options=waiting_step_options(state)
            )
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Send the customer a nudge email after the first idle window."
class NudgeCustomerStep(Step[ConversationStart]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        state = CONVERSATION_STATE.get(context)
        self.mailer.send_conversation_notice(
            context.flow_id, "nudge", state.message_count
        )
        state.status = STATUS_NUDGED
        CONVERSATION_STATE.set(context, state)
        return go_to_many(
            StepMovement.of(
                WaitAfterNudgeStep, input, options=waiting_step_options(state)
            )
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Wait one more idle window; a message returns to the first wait, the timer closes the conversation."
class WaitAfterNudgeStep(Step[ConversationStart]):
    def __init__(self, summarizer: TranscriptSummarizer) -> None:
        self.summarizer = summarizer

    def get_step_options(self) -> StepOptions:
        return waiting_step_options(None)

    def wait_for(self, context: Context, input: ConversationStart) -> Wait:
        return Wait.any_of(
            CUSTOMER_MESSAGES.for_range(at_least=1, at_most=TRANSCRIPT_CHUNK_SIZE),
            Timer.by_duration(timedelta(seconds=input.idle_seconds)),
        )

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        if context.has_timer_fired():
            return go_to(CloseConversationStep, input)
        state = append_messages_to_transcript(
            context, list(CUSTOMER_MESSAGES.results(context)), self.summarizer
        )
        state.status = STATUS_WAITING
        CONVERSATION_STATE.set(context, state)
        return go_to_many(
            StepMovement.of(
                WaitForCustomerStep, input, options=waiting_step_options(state)
            )
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Post the close summary and complete, unless a customer message is still queued."
class CloseConversationStep(Step[ConversationStart]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        state = CONVERSATION_STATE.get(context)
        if CUSTOMER_MESSAGES.size(context) > 0:
            return go_to_many(
                StepMovement.of(
                    WaitForCustomerStep, input, options=waiting_step_options(state)
                )
            )
        self.mailer.send_conversation_notice(
            context.flow_id, "close-summary", state.message_count
        )
        state.status = STATUS_CLOSED
        CONVERSATION_STATE.set(context, state)
        # A message that arrives during this Step sends the Flow back to waiting.
        return force_complete_if_channels_empty(
            state.message_count,
            StepMovement.of(
                WaitForCustomerStep, input, options=waiting_step_options(state)
            ),
            CUSTOMER_MESSAGES,
        )


class SupportConversationFlow(Flow[ConversationStart]):
    def __init__(
        self, mailer: ConversationMailer, summarizer: TranscriptSummarizer
    ) -> None:
        self.wait_for_customer = WaitForCustomerStep(summarizer)
        self.nudge_customer = NudgeCustomerStep(mailer)
        self.wait_after_nudge = WaitAfterNudgeStep(summarizer)
        self.close_conversation = CloseConversationStep(mailer)

    def get_steps(self) -> StepList[ConversationStart]:
        return StepList.start_step(self.wait_for_customer).other_steps(
            self.nudge_customer, self.wait_after_nudge, self.close_conversation
        )

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(
            CUSTOMER_MESSAGES,
            CONVERSATION_STATE,
            ASSIGNED_AGENT,
            SUMMARY,
            TRANSCRIPT_CHUNKS,
        )

    @rpc(timeout=timedelta(seconds=10))
    def post_message(self, context: Context, text: str) -> RPCResult[bool]:
        """Validate one customer message and queue it; the waiting Step records it."""
        if not text.strip():
            raise ValueError("message text is empty")
        if len(text) > MAX_MESSAGE_CHARACTERS:
            raise ValueError(f"message text exceeds {MAX_MESSAGE_CHARACTERS} characters")
        CUSTOMER_MESSAGES.publish(context, text)
        return RPCResult(True)

    @rpc(timeout=timedelta(seconds=10))
    def assign_agent(self, context: Context, agent_name: str) -> RPCResult[str]:
        """Write AssignedAgent, which no waiting Step reads or locks."""
        agent_name = agent_name.strip()
        if not agent_name or len(agent_name) > MAX_AGENT_NAME_CHARACTERS:
            raise ValueError("agent name must be 1 to 200 characters")
        ASSIGNED_AGENT.set(context, agent_name)
        return RPCResult(agent_name)

    @rpc(timeout=timedelta(seconds=10))
    def get_transcript_page(
        self, context: Context, page_token: str
    ) -> RPCResult[TranscriptPage]:
        """Return one chunk newest first; the caller loads only that chunk.

        A token older than the retention window returns an empty page with
        is_beyond_retention set. A malformed or not yet archived token raises.
        """
        page_token = parse_transcript_page_token(page_token)
        state = CONVERSATION_STATE.get(context)
        oldest = state.oldest_archived_chunk_token
        if page_token != CURRENT_CHUNK_TOKEN:
            if not state.newest_archived_chunk_token or int(page_token) > int(
                state.newest_archived_chunk_token
            ):
                raise ValueError(f'transcript page "{page_token}" is not archived yet')
            if int(page_token) < int(oldest):
                return RPCResult(
                    TranscriptPage(
                        page_token,
                        [],
                        is_beyond_retention=True,
                        oldest_retained_page_token=oldest,
                    )
                )
        chunk = TRANSCRIPT_CHUNKS.get(context, page_token)
        if page_token == CURRENT_CHUNK_TOKEN:
            next_older = state.newest_archived_chunk_token
        else:
            older_sequence = int(page_token) - TRANSCRIPT_CHUNK_SIZE
            next_older = (
                archived_chunk_token(older_sequence)
                if older_sequence >= int(oldest)
                else ""
            )
        return RPCResult(
            TranscriptPage(
                page_token,
                list(reversed(chunk.records)),
                next_older_page_token=next_older,
                oldest_retained_page_token=oldest,
            )
        )
