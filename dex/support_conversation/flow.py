"""SupportConversationFlow: one long-lived Flow per AI customer support conversation."""

from __future__ import annotations

from datetime import timedelta

from dex import (
    Attribute,
    AttributeMap,
    Channel,
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
    force_fail,
    go_to,
    go_to_many,
    rpc,
)

from support_conversation.mailer import ConversationMailer, conversation_notice_key
from support_conversation.reply_generator import ReplyGenerator
from support_conversation.transcript import (
    CURRENT_CHUNK_TOKEN,
    STATUS_NUDGED,
    STATUS_WAITING,
    ConversationSettings,
    ConversationState,
    PendingExchange,
    ReplyRequest,
    TranscriptChunk,
    TranscriptPage,
    TranscriptSummarizer,
    TranscriptSummary,
    beyond_retention_page,
    drop_oldest_archived_chunks,
    exchange_records,
    parse_transcript_page_token,
    plan_transcript_append,
    record_exchange,
    reply_request_error,
    require_archived_transcript_page,
    transcript_page,
    unfittable_exchange_error,
    validate_customer_message,
    validated_agent_name,
)

# --- Schema: Channel, Attributes, AttributeMap ---

CUSTOMER_MESSAGES = Channel("CustomerMessages", str)
CONVERSATION_STATE = Attribute("ConversationState", ConversationState)
ASSIGNED_AGENT = Attribute("AssignedAgent", str)
SUMMARY = Attribute("Summary", TranscriptSummary)
TRANSCRIPT_CHUNKS = AttributeMap("TranscriptChunks", TranscriptChunk)

# GenerateReplyStep and TrimTranscriptStep write the transcript. Steps of one
# conversation run one at a time, so no transcript chunk needs a lock.


def transcript_step_options(dropped_tokens: list[str]) -> StepOptions:
    """Options of a transcript writer: load "current" and the chunks it will drop."""
    tokens = [CURRENT_CHUNK_TOKEN, *dropped_tokens]
    loads = tuple(TRANSCRIPT_CHUNKS.load(token) for token in tokens)
    return StepOptions(execute_load_attribute_map_instances=loads)


# --- Steps ---


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Answer one customer message with the model and its tools, and record the message and the reply."
class GenerateReplyStep(Step[ReplyRequest]):
    def __init__(self, reply_generator: ReplyGenerator) -> None:
        self.reply_generator = reply_generator

    def get_step_options(self) -> StepOptions:
        return transcript_step_options([])

    def execute(self, context: Context, input: ReplyRequest) -> StepDecision:
        # The start input bypasses post_message, so validate it before the model call.
        input_error = reply_request_error(input)
        if input_error:
            return force_fail(f"invalid reply request: {input_error}")
        messages = input.customer_messages
        reply = self.reply_generator.generate_reply(context.flow_id, messages)
        records = exchange_records(messages[0], reply.text, reply.tool_calls)
        state = CONVERSATION_STATE.get(context) or ConversationState()
        current_chunk = TRANSCRIPT_CHUNKS.get(context, CURRENT_CHUNK_TOKEN)
        plan = plan_transcript_append(state, current_chunk, records)
        if plan.over_budget_bytes > 0:
            # Make room first; the input keeps the reply, so the model is not called again.
            exchange = PendingExchange(input.settings, records)
            options = transcript_step_options(plan.chunk_tokens_to_drop)
            return go_to_many(
                StepMovement.of(TrimTranscriptStep, exchange, options=options)
            )
        for token, chunk in record_exchange(state, plan, records):
            TRANSCRIPT_CHUNKS.set(context, token, chunk)
        state.status = STATUS_WAITING
        CONVERSATION_STATE.set(context, state)
        return go_to(WaitForCustomerStep, input.settings)


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Fold the oldest transcript chunks into the Summary until the pending exchange fits the 32 MiB budget, then record it."
class TrimTranscriptStep(Step[PendingExchange]):
    def __init__(self, summarizer: TranscriptSummarizer) -> None:
        self.summarizer = summarizer

    def get_step_options(self) -> StepOptions:
        return transcript_step_options([])

    def execute(self, context: Context, input: PendingExchange) -> StepDecision:
        state = CONVERSATION_STATE.get(context) or ConversationState()
        current_chunk = TRANSCRIPT_CHUNKS.get(context, CURRENT_CHUNK_TOKEN)
        plan = plan_transcript_append(state, current_chunk, input.records)
        # Loaded through the movement options built by transcript_step_options.
        dropped_tokens = plan.chunk_tokens_to_drop
        if plan.over_budget_bytes > 0 and not dropped_tokens:
            # Unreachable for validated messages; transcript.py asserts the bounds.
            return force_fail(unfittable_exchange_error(plan))
        if dropped_tokens:
            summary = SUMMARY.get(context)
            for token in dropped_tokens:
                chunk = TRANSCRIPT_CHUNKS.get(context, token)
                summary = self.summarizer.fold_chunk_into_summary(summary, chunk)
                TRANSCRIPT_CHUNKS.delete(context, token)
            SUMMARY.set(context, summary)
            drop_oldest_archived_chunks(state, dropped_tokens)
            plan = plan_transcript_append(state, current_chunk, input.records)
        if plan.over_budget_bytes > 0:
            CONVERSATION_STATE.set(context, state)
            options = transcript_step_options(plan.chunk_tokens_to_drop)
            return go_to_many(
                StepMovement.of(TrimTranscriptStep, input, options=options)
            )
        for token, chunk in record_exchange(state, plan, input.records):
            TRANSCRIPT_CHUNKS.set(context, token, chunk)
        state.status = STATUS_WAITING
        CONVERSATION_STATE.set(context, state)
        return go_to(WaitForCustomerStep, input.settings)


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"After a reply, wait for follow-up customer messages or the idle timer."
class WaitForCustomerStep(Step[ConversationSettings]):
    def wait_for(self, context: Context, input: ConversationSettings) -> Wait:
        # The Channel is declared first, so queued messages win over a due Timer.
        # One message per reply: each customer message gets its own answer.
        return Wait.any_of(
            CUSTOMER_MESSAGES.for_one(),
            Timer.by_duration(timedelta(seconds=input.idle_seconds)),
        )

    def execute(self, context: Context, input: ConversationSettings) -> StepDecision:
        if context.has_timer_fired():
            return go_to(NudgeCustomerStep, input)
        messages = list(CUSTOMER_MESSAGES.results(context))
        return go_to(GenerateReplyStep, ReplyRequest(input, messages))


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Send the customer a nudge email after the idle window."
class NudgeCustomerStep(Step[ConversationSettings]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationSettings) -> StepDecision:
        state = CONVERSATION_STATE.get(context)
        notice_key = conversation_notice_key(context, "nudge")
        self.mailer.send_conversation_notice(
            notice_key, context.flow_id, "nudge", state.record_count
        )
        state.status = STATUS_NUDGED
        CONVERSATION_STATE.set(context, state)
        return go_to(WaitAfterNudgeStep, input)


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Wait up to the close-after window; a message is answered, the timer closes the conversation."
class WaitAfterNudgeStep(Step[ConversationSettings]):
    def wait_for(self, context: Context, input: ConversationSettings) -> Wait:
        return Wait.any_of(
            CUSTOMER_MESSAGES.for_one(),
            Timer.by_duration(timedelta(seconds=input.close_after_seconds)),
        )

    def execute(self, context: Context, input: ConversationSettings) -> StepDecision:
        if context.has_timer_fired():
            return go_to(CloseConversationStep, input)
        messages = list(CUSTOMER_MESSAGES.results(context))
        return go_to(GenerateReplyStep, ReplyRequest(input, messages))


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Post the close summary and complete, unless a customer message is still queued."
class CloseConversationStep(Step[ConversationSettings]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationSettings) -> StepDecision:
        if CUSTOMER_MESSAGES.size(context) > 0:
            return go_to(WaitForCustomerStep, input)
        state = CONVERSATION_STATE.get(context)
        # Written before the close decision; it counts only if the run completes.
        notice_key = conversation_notice_key(context, "close-summary")
        self.mailer.send_conversation_notice(
            notice_key, context.flow_id, "close-summary", state.record_count
        )
        # No status write here: it would also commit on the fallback path.
        # A message that arrives during this Step sends the Flow back to waiting.
        return force_complete_if_channels_empty(
            state.record_count,
            StepMovement.of(WaitForCustomerStep, input),
            CUSTOMER_MESSAGES,
        )


# --- Flow, persistence schema, and RPCs ---


class SupportConversationFlow(Flow[ReplyRequest]):
    """Start input: the settings and the customer's first message."""

    def __init__(
        self,
        mailer: ConversationMailer,
        summarizer: TranscriptSummarizer,
        reply_generator: ReplyGenerator,
    ) -> None:
        self.generate_reply = GenerateReplyStep(reply_generator)
        self.trim_transcript = TrimTranscriptStep(summarizer)
        self.wait_for_customer = WaitForCustomerStep()
        self.nudge_customer = NudgeCustomerStep(mailer)
        self.wait_after_nudge = WaitAfterNudgeStep()
        self.close_conversation = CloseConversationStep(mailer)

    def get_steps(self) -> StepList[ReplyRequest]:
        return StepList.start_step(self.generate_reply).other_steps(
            self.trim_transcript,
            self.wait_for_customer,
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
        """Validate one customer message and queue it; the waiting Step consumes it."""
        validate_customer_message(text)
        CUSTOMER_MESSAGES.publish(context, text)
        return RPCResult(True)

    @rpc(timeout=timedelta(seconds=10))
    def assign_agent(self, context: Context, agent_name: str) -> RPCResult[str]:
        """Write AssignedAgent, which no waiting Step reads or locks."""
        agent_name = validated_agent_name(agent_name)
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
            require_archived_transcript_page(state, page_token)
            if int(page_token) < int(oldest):
                return RPCResult(beyond_retention_page(page_token, oldest))
        chunk = TRANSCRIPT_CHUNKS.get(context, page_token)
        return RPCResult(transcript_page(state, page_token, chunk))
