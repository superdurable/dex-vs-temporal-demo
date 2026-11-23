"""SupportConversationFlow v2: analyze sentiment before each reply."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from dex import (
    Attribute,
    AttributeMap,
    AttributeMapLoad,
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
    go_to,
    go_to_many,
    rpc,
)

from support_conversation.mailer import ConversationMailer
from support_conversation.reply_generator import ReplyGenerator
from support_conversation.sentiment import SentimentAnalyzer
from support_conversation.transcript import (
    CURRENT_CHUNK_TOKEN,
    ROLE_ASSISTANT,
    ROLE_CUSTOMER,
    STATUS_CLOSED,
    STATUS_NUDGED,
    STATUS_WAITING,
    TRANSCRIPT_CHUNK_SIZE,
    ConversationSettings,
    ConversationState,
    ReplyRequest,
    TranscriptChunk,
    TranscriptPage,
    TranscriptRecord,
    TranscriptSummarizer,
    TranscriptSummary,
    append_records_to_chunk,
    archived_chunk_token,
    chunk_tokens_to_drop,
    drop_oldest_archived_chunks,
    parse_transcript_page_token,
)

# --- Schema: Channel, Attributes, AttributeMap ---

CUSTOMER_MESSAGES = Channel("CustomerMessages", str)
CONVERSATION_STATE = Attribute("ConversationState", ConversationState)
ASSIGNED_AGENT = Attribute("AssignedAgent", str)
SUMMARY = Attribute("Summary", TranscriptSummary)
TRANSCRIPT_CHUNKS = AttributeMap("TranscriptChunks", TranscriptChunk)

MAX_MESSAGE_CHARACTERS = 8192
MAX_AGENT_NAME_CHARACTERS = 200

# --- Transcript chunk helpers ---
# GenerateReplyStep is the only transcript writer, and Steps of one conversation
# run one at a time, so no transcript chunk needs a lock. Every movement into
# GenerateReplyStep carries options that also load the chunks it will drop.


def transcript_writer_step_options(state: ConversationState | None) -> StepOptions:
    """Options of the transcript writer: load "current" and the chunks it will drop.

    The chunks to drop follow from ConversationState alone, so the movement
    and the execution choose the same ones.
    """
    loads: list[AttributeMapLoad] = [TRANSCRIPT_CHUNKS.load(CURRENT_CHUNK_TOKEN)]
    if state is not None:
        loads.extend(TRANSCRIPT_CHUNKS.load(token) for token in chunk_tokens_to_drop(state))
    return StepOptions(execute_load_attribute_map_instances=tuple(loads))


def utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


# --- Steps ---


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Analyze the sentiment of one customer message before the reply."
class AnalyzeSentimentStep(Step[ReplyRequest]):
    def __init__(self, sentiment_analyzer: SentimentAnalyzer) -> None:
        self.sentiment_analyzer = sentiment_analyzer

    def execute(self, context: Context, input: ReplyRequest) -> StepDecision:
        sentiment = self.sentiment_analyzer.analyze_sentiment(
            "\n".join(input.customer_messages)
        )
        return go_to_many(
            StepMovement.of(
                GenerateReplyStep,
                replace(input, sentiment=sentiment),
                options=transcript_writer_step_options(CONVERSATION_STATE.get(context)),
            )
        )


# dex:group group-id:conversation group-label:"Conversation"
# dex:explanation text:"Answer one customer message with the model and its tools, and record the message and the reply."
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
            context.flow_id, input.customer_messages
        )
        recorded_at = utc_timestamp()
        records = [
            TranscriptRecord(0, ROLE_CUSTOMER, text, recorded_at=recorded_at)
            for text in input.customer_messages
        ]
        tool_calls = [*reply.tool_calls, f"analyze_sentiment:{input.sentiment}"]
        records.append(
            TranscriptRecord(
                0, ROLE_ASSISTANT, reply.text, tool_calls, recorded_at
            )
        )
        state = CONVERSATION_STATE.get(context)
        # Loaded through the movement options built by transcript_writer_step_options.
        dropped_tokens = chunk_tokens_to_drop(state)
        appended = append_records_to_chunk(
            state, TRANSCRIPT_CHUNKS.get(context, CURRENT_CHUNK_TOKEN), records
        )
        if appended.archived_chunk is not None:
            TRANSCRIPT_CHUNKS.set(
                context, appended.archived_chunk_token, appended.archived_chunk
            )
        if dropped_tokens:
            summary = SUMMARY.get(context)
            for token in dropped_tokens:
                summary = self.summarizer.fold_chunk_into_summary(
                    summary, TRANSCRIPT_CHUNKS.get(context, token)
                )
                TRANSCRIPT_CHUNKS.delete(context, token)
            SUMMARY.set(context, summary)
            drop_oldest_archived_chunks(state, dropped_tokens)
        TRANSCRIPT_CHUNKS.set(context, CURRENT_CHUNK_TOKEN, appended.current_chunk)
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
        return go_to(
            AnalyzeSentimentStep,
            ReplyRequest(input, list(CUSTOMER_MESSAGES.results(context))),
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Send the customer a nudge email after the idle window."
class NudgeCustomerStep(Step[ConversationSettings]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationSettings) -> StepDecision:
        state = CONVERSATION_STATE.get(context)
        self.mailer.send_conversation_notice(
            context.flow_id, "nudge", state.record_count
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
        return go_to(
            AnalyzeSentimentStep,
            ReplyRequest(input, list(CUSTOMER_MESSAGES.results(context))),
        )


# dex:group group-id:idle-handling group-label:"Idle handling"
# dex:explanation text:"Post the close summary and complete, unless a customer message is still queued."
class CloseConversationStep(Step[ConversationSettings]):
    def __init__(self, mailer: ConversationMailer) -> None:
        self.mailer = mailer

    def execute(self, context: Context, input: ConversationSettings) -> StepDecision:
        if CUSTOMER_MESSAGES.size(context) > 0:
            return go_to(WaitForCustomerStep, input)
        state = CONVERSATION_STATE.get(context)
        self.mailer.send_conversation_notice(
            context.flow_id, "close-summary", state.record_count
        )
        state.status = STATUS_CLOSED
        CONVERSATION_STATE.set(context, state)
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
        sentiment_analyzer: SentimentAnalyzer,
    ) -> None:
        self.analyze_sentiment = AnalyzeSentimentStep(sentiment_analyzer)
        self.generate_reply = GenerateReplyStep(reply_generator, summarizer)
        self.wait_for_customer = WaitForCustomerStep()
        self.nudge_customer = NudgeCustomerStep(mailer)
        self.wait_after_nudge = WaitAfterNudgeStep()
        self.close_conversation = CloseConversationStep(mailer)

    def get_steps(self) -> StepList[ReplyRequest]:
        return StepList.start_step(self.analyze_sentiment).other_steps(
            self.generate_reply,
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
