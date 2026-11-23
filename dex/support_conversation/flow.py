"""SupportConversationFlow: one long-lived Flow per AI customer support conversation."""

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
from support_conversation.reply_generator import ReplyGenerator
from support_conversation.transcript import (
    ASSIGNED_AGENT,
    CONVERSATION_STATE,
    CURRENT_CHUNK_TOKEN,
    CUSTOMER_MESSAGES,
    ROLE_ASSISTANT,
    ROLE_CUSTOMER,
    STATUS_CLOSED,
    STATUS_NUDGED,
    STATUS_REPLYING,
    STATUS_WAITING,
    SUMMARY,
    TRANSCRIPT_CHUNK_SIZE,
    TRANSCRIPT_CHUNKS,
    ConversationStart,
    ReplyRequest,
    TranscriptPage,
    TranscriptSummarizer,
    append_record_to_transcript,
    archived_chunk_token,
    parse_transcript_page_token,
    transcript_writer_step_options,
)

MAX_MESSAGE_CHARACTERS = 4000
MAX_AGENT_NAME_CHARACTERS = 200

# Steps run one at a time, so each transcript chunk has a single writer and
# needs no lock. Every movement into a Step that appends a record carries
# options that also load the chunk the next archive may evict.


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Wait for the next customer message and record it; the idle timer starts after each reply."
class WaitForCustomerStep(Step[ConversationStart]):
    def __init__(self, summarizer: TranscriptSummarizer) -> None:
        self.summarizer = summarizer

    def get_step_options(self) -> StepOptions:
        return transcript_writer_step_options(None)

    def wait_for(self, context: Context, input: ConversationStart) -> Wait:
        # The Channel is declared first, so a queued message wins over a due Timer.
        return Wait.any_of(
            CUSTOMER_MESSAGES.for_one(),
            Timer.by_duration(timedelta(seconds=input.idle_seconds)),
        )

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        if context.has_timer_fired():
            return go_to(NudgeCustomerStep, input)
        text = CUSTOMER_MESSAGES.results(context)[0]
        state, record = append_record_to_transcript(
            context, ROLE_CUSTOMER, text, self.summarizer
        )
        state.status = STATUS_REPLYING
        CONVERSATION_STATE.set(context, state)
        return go_to_many(
            StepMovement.of(
                GenerateReplyStep,
                ReplyRequest(input, record.sequence, text),
                options=transcript_writer_step_options(state),
            )
        )


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Generate the assistant reply with internal tools and record it."
class GenerateReplyStep(Step[ReplyRequest]):
    def __init__(
        self, reply_generator: ReplyGenerator, summarizer: TranscriptSummarizer
    ) -> None:
        self.reply_generator = reply_generator
        self.summarizer = summarizer

    def get_step_options(self) -> StepOptions:
        return transcript_writer_step_options(None)

    def execute(self, context: Context, input: ReplyRequest) -> StepDecision:
        reply = self.reply_generator.generate_reply(
            context.flow_id, input.customer_sequence, input.customer_text
        )
        state, _ = append_record_to_transcript(
            context, ROLE_ASSISTANT, reply.text, self.summarizer, reply.tool_calls
        )
        state.status = STATUS_WAITING
        CONVERSATION_STATE.set(context, state)
        return go_to_many(
            StepMovement.of(
                WaitForCustomerStep,
                input.conversation,
                options=transcript_writer_step_options(state),
            )
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Send the customer a nudge email after the idle window."
class NudgeCustomerStep(Step[ConversationStart]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        state = CONVERSATION_STATE.get(context)
        self.mailer.send_conversation_notice(
            context.flow_id, "nudge", state.record_count
        )
        state.status = STATUS_NUDGED
        CONVERSATION_STATE.set(context, state)
        return go_to_many(
            StepMovement.of(
                WaitAfterNudgeStep,
                input,
                options=transcript_writer_step_options(state),
            )
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Wait up to the close-after window; a message is recorded and answered, the timer closes the conversation."
class WaitAfterNudgeStep(Step[ConversationStart]):
    def __init__(self, summarizer: TranscriptSummarizer) -> None:
        self.summarizer = summarizer

    def get_step_options(self) -> StepOptions:
        return transcript_writer_step_options(None)

    def wait_for(self, context: Context, input: ConversationStart) -> Wait:
        return Wait.any_of(
            CUSTOMER_MESSAGES.for_one(),
            Timer.by_duration(timedelta(seconds=input.close_after_seconds)),
        )

    def execute(self, context: Context, input: ConversationStart) -> StepDecision:
        if context.has_timer_fired():
            return go_to(CloseConversationStep, input)
        text = CUSTOMER_MESSAGES.results(context)[0]
        state, record = append_record_to_transcript(
            context, ROLE_CUSTOMER, text, self.summarizer
        )
        state.status = STATUS_REPLYING
        CONVERSATION_STATE.set(context, state)
        return go_to_many(
            StepMovement.of(
                GenerateReplyStep,
                ReplyRequest(input, record.sequence, text),
                options=transcript_writer_step_options(state),
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
                    WaitForCustomerStep,
                    input,
                    options=transcript_writer_step_options(state),
                )
            )
        self.mailer.send_conversation_notice(
            context.flow_id, "close-summary", state.record_count
        )
        state.status = STATUS_CLOSED
        CONVERSATION_STATE.set(context, state)
        # A message that arrives during this Step sends the Flow back to waiting.
        return force_complete_if_channels_empty(
            state.record_count,
            StepMovement.of(
                WaitForCustomerStep,
                input,
                options=transcript_writer_step_options(state),
            ),
            CUSTOMER_MESSAGES,
        )


class SupportConversationFlow(Flow[ConversationStart]):
    def __init__(
        self,
        mailer: ConversationMailer,
        summarizer: TranscriptSummarizer,
        reply_generator: ReplyGenerator,
    ) -> None:
        self.wait_for_customer = WaitForCustomerStep(summarizer)
        self.generate_reply = GenerateReplyStep(reply_generator, summarizer)
        self.nudge_customer = NudgeCustomerStep(mailer)
        self.wait_after_nudge = WaitAfterNudgeStep(summarizer)
        self.close_conversation = CloseConversationStep(mailer)

    def get_steps(self) -> StepList[ConversationStart]:
        return StepList.start_step(self.wait_for_customer).other_steps(
            self.generate_reply,
            self.nudge_customer,
            self.wait_after_nudge,
            self.close_conversation,
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
