"""Offline regression checks of the transcript budget with the SDK's real JSON codec.

Runs the same plan, drop, and append functions the transcript writers use,
one Step execution at a time, and checks after every execution:

- the serialized chunks the writer would store sum to state.transcript_bytes;
- that sum never exceeds TRANSCRIPT_BUDGET_BYTES;
- one execution drops at most two chunks and never the newest archived chunk;
- every message is recorded once, with its reply right after it.

Scenarios: small messages followed by large ones, an append that archives a
chunk while it must also drop, and non-BMP Unicode (12 JSON bytes per
character). Exit status 0 means every check passed.
"""

from __future__ import annotations

import json
import random
import sys
from dataclasses import dataclass, field
from typing import Callable

from support_conversation.transcript import (
    CURRENT_CHUNK_TOKEN,
    LARGEST_EXCHANGE_BYTES,
    TRANSCRIPT_CHUNK_SIZE,
    MAX_DROPPED_CHUNKS_PER_EXECUTION,
    MAX_MESSAGE_CHARACTERS,
    ROLE_ASSISTANT,
    ROLE_CUSTOMER,
    TRANSCRIPT_BUDGET_BYTES,
    ConversationState,
    LocalTranscriptSummarizer,
    TranscriptChunk,
    TranscriptRecord,
    TranscriptSummary,
    apply_transcript_append,
    chunk_tokens_to_drop,
    customer_message_error,
    drop_oldest_archived_chunks,
    encoded_chunk_bytes,
    plan_transcript_append,
)

NON_BMP_CHARACTER = "\U0001f600"
REPLY_TEXT = "Thanks for your message. Your request is covered."


@dataclass
class SimulatedTranscript:
    """Holds what the Flow stores: ConversationState, the chunks, and the Summary."""

    state: ConversationState = field(default_factory=ConversationState)
    chunks: dict[str, TranscriptChunk] = field(
        default_factory=lambda: {CURRENT_CHUNK_TOKEN: TranscriptChunk()}
    )
    summary: TranscriptSummary | None = None
    executions: int = 0
    trim_executions: int = 0
    dropped_chunks: int = 0
    peak_bytes: int = 0
    archived_sizes: dict[str, int] = field(default_factory=dict)
    most_trims_per_exchange: int = 0
    # Appends that archived a chunk right after trimming for the same exchange.
    archives_after_trim: int = 0

    def record_exchange(self, message: str) -> None:
        if customer_message_error(message):
            raise AssertionError(f"invalid test message: {customer_message_error(message)}")
        records = [
            TranscriptRecord(0, ROLE_CUSTOMER, message, recorded_at="2026-11-23T00:00:00.000Z"),
            TranscriptRecord(
                0, ROLE_ASSISTANT, REPLY_TEXT, ["lookup_order"], "2026-11-23T00:00:00.000Z"
            ),
        ]
        # GenerateReplyStep: append when it fits, otherwise hand over to trimming.
        self.executions += 1
        plan = plan_transcript_append(self.state, self.chunks[CURRENT_CHUNK_TOKEN], records)
        trims_for_this_exchange = 0
        while plan.over_budget_bytes > 0:
            trims_for_this_exchange += 1
            self.most_trims_per_exchange = max(
                self.most_trims_per_exchange, trims_for_this_exchange
            )
            # TrimTranscriptStep: one execution drops at most two chunks.
            self.executions += 1
            self.trim_executions += 1
            tokens = chunk_tokens_to_drop(self.state, plan.over_budget_bytes)
            if not tokens:
                raise AssertionError("an exchange cannot fit and nothing is droppable")
            if len(tokens) > MAX_DROPPED_CHUNKS_PER_EXECUTION:
                raise AssertionError(f"dropped {len(tokens)} chunks in one execution")
            if self.state.newest_archived_chunk_token in tokens:
                raise AssertionError("dropped the newest archived chunk")
            for token in tokens:
                self.summary = LocalTranscriptSummarizer().fold_chunk_into_summary(
                    self.summary, self.chunks.pop(token)
                )
            drop_oldest_archived_chunks(self.state, tokens)
            self.dropped_chunks += len(tokens)
            self.check_stored_bytes()
            plan = plan_transcript_append(
                self.state, self.chunks[CURRENT_CHUNK_TOKEN], records
            )
        apply_transcript_append(self.state, plan, records)
        if plan.archived_chunk is not None:
            self.chunks[plan.archived_chunk_token] = plan.archived_chunk
            if trims_for_this_exchange:
                self.archives_after_trim += 1
        self.chunks[CURRENT_CHUNK_TOKEN] = plan.current_chunk
        self.check_stored_bytes()

    def check_stored_bytes(self) -> None:
        # Archived chunks never change, so each is measured once.
        for token, chunk in self.chunks.items():
            if token == CURRENT_CHUNK_TOKEN or token not in self.archived_sizes:
                self.archived_sizes[token] = encoded_chunk_bytes(chunk)
        for token in list(self.archived_sizes):
            if token not in self.chunks:
                del self.archived_sizes[token]
        stored = sum(self.archived_sizes.values())
        if stored != self.state.transcript_bytes:
            raise AssertionError(
                f"bookkeeping {self.state.transcript_bytes} != serialized {stored}"
            )
        if stored > TRANSCRIPT_BUDGET_BYTES:
            raise AssertionError(f"transcript {stored} bytes exceeds the budget")
        self.peak_bytes = max(self.peak_bytes, stored)

    def check_one_to_one(self, messages: list[str]) -> None:
        records = [
            record
            for token in sorted(t for t in self.chunks if t != CURRENT_CHUNK_TOKEN)
            for record in self.chunks[token].records
        ] + self.chunks[CURRENT_CHUNK_TOKEN].records
        summarized = self.summary.summarized_record_count if self.summary else 0
        if records and records[0].sequence != summarized + 1:
            raise AssertionError("retained records do not start after the Summary")
        for index, record in enumerate(records):
            if record.sequence != summarized + index + 1:
                raise AssertionError(f"sequence gap at {record.sequence}")
            expected_role = ROLE_CUSTOMER if record.sequence % 2 == 1 else ROLE_ASSISTANT
            if record.role != expected_role:
                raise AssertionError(f"record {record.sequence} is {record.role}")
            if record.role == ROLE_CUSTOMER and record.text != messages[(record.sequence - 1) // 2]:
                raise AssertionError(f"record {record.sequence} holds another message")
        if self.state.customer_message_count != len(messages):
            raise AssertionError("customer count differs from messages sent")
        if self.state.assistant_reply_count != len(messages):
            raise AssertionError("reply count differs from messages sent")

    def report(self, scenario: str, messages: int) -> dict[str, object]:
        return {
            "scenario": scenario,
            "messages": messages,
            "stepExecutions": self.executions,
            "trimExecutions": self.trim_executions,
            "mostTrimExecutionsBeforeOneAppend": self.most_trims_per_exchange,
            "archivesAfterTrim": self.archives_after_trim,
            "droppedChunks": self.dropped_chunks,
            "retainedArchivedChunkCount": len(self.state.archived_chunk_bytes),
            "transcriptBytes": self.state.transcript_bytes,
            "peakTranscriptBytes": self.peak_bytes,
            "budgetBytes": TRANSCRIPT_BUDGET_BYTES,
            "summarizedRecords": self.summary.summarized_record_count if self.summary else 0,
        }


MessageSource = Callable[[SimulatedTranscript, int], "str | None"]


def run_scenario(
    scenario: str, next_message: MessageSource, needs_repeated_trim: bool
) -> dict[str, object]:
    transcript = SimulatedTranscript()
    messages: list[str] = []
    while (message := next_message(transcript, len(messages))) is not None:
        messages.append(message)
        transcript.record_exchange(message)
    transcript.check_one_to_one(messages)
    if transcript.dropped_chunks == 0:
        raise AssertionError(f"{scenario}: the budget was never reached")
    if needs_repeated_trim and transcript.most_trims_per_exchange < 2:
        raise AssertionError(f"{scenario}: two chunks were always enough")
    if scenario == "archive-boundary" and transcript.archives_after_trim == 0:
        raise AssertionError(f"{scenario}: no append archived a chunk after trimming")
    return transcript.report(scenario, len(messages))


LARGEST_MESSAGE = NON_BMP_CHARACTER * MAX_MESSAGE_CHARACTERS


def small_then_large_messages(transcript: SimulatedTranscript, index: int) -> str | None:
    """The reviewed counterexample: 500 one-character messages, then 8,192 emoji each."""
    if index >= 1200:
        return None
    return "a" if index < 500 else LARGEST_MESSAGE


def archive_boundary_messages(transcript: SimulatedTranscript, index: int) -> str | None:
    """Makes the append that archives a full chunk also the one that needs trimming."""
    if index >= 3000 or transcript.archives_after_trim >= 5:
        return None
    current_records = len(transcript.chunks[CURRENT_CHUNK_TOKEN].records)
    room = TRANSCRIPT_BUDGET_BYTES - transcript.state.transcript_bytes
    near_budget = room < 3 * LARGEST_EXCHANGE_BYTES
    if current_records != TRANSCRIPT_CHUNK_SIZE - 2:
        # Near the budget, small messages fill the chunk without using the room.
        return "b" if near_budget else LARGEST_MESSAGE
    if not near_budget:
        return LARGEST_MESSAGE
    # Each emoji is 12 JSON bytes, so this archiving append overflows the room.
    return NON_BMP_CHARACTER * max(1, min(MAX_MESSAGE_CHARACTERS, room // 12 + 1))


def mixed_unicode_messages(transcript: SimulatedTranscript, index: int) -> str | None:
    if index >= 5000:
        return None
    generator = random.Random(20261123 + index)
    alphabet = ["x", "é", "中", NON_BMP_CHARACTER, '"', "\\", "\n"]
    length = generator.choice([1, 10, 200, 4000, MAX_MESSAGE_CHARACTERS])
    return "m" + "".join(generator.choice(alphabet) for _ in range(length - 1))


def main() -> int:
    # Scenario name: (messages, whether one append must need several trim executions).
    scenarios = {
        "small-then-large-non-bmp": (small_then_large_messages, True),
        "archive-boundary": (archive_boundary_messages, False),
        "mixed-unicode": (mixed_unicode_messages, False),
    }
    failed = False
    for name, (next_message, needs_repeated_trim) in scenarios.items():
        try:
            print(
                json.dumps(run_scenario(name, next_message, needs_repeated_trim)),
                flush=True,
            )
        except AssertionError as error:
            failed = True
            print(json.dumps({"scenario": name, "failed": str(error)}), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
