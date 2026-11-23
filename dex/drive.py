"""Command-line driver for SupportConversationFlow (same contract as the Temporal side).

  start <conversation-id> --first-message <text> [--idle-seconds N] [--close-after-seconds N]
  post <conversation-id> <text> [--message-id ID]
  assign <conversation-id> <agent-name>
  page <conversation-id> [token]
  pages-summary <conversation-id>
  status <conversation-id>
  wait-replies <conversation-id> <reply-count> [--timeout-seconds S]
  flood <conversation-id> --count N [--rate R] [--concurrency C] [--max-seconds S]
  paced <conversation-id> --rate R --duration-seconds S [--snapshot-at N,M] [--drain-seconds S]
  verify-transcript <conversation-id>

flood and paced share the load options: --message-characters, --message-kind,
--message-id-prefix, --snapshot-at, --max-unreplied-messages, and
--max-unreplied-bytes. SUPPORT_FLOW_VERSION (v1, v2, v2-short-idle) selects
the Flow code; v1 is the default.
"""

from __future__ import annotations

import os

# The status command runs dexcli as a subprocess; keep gRPC fork handlers quiet.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "false")

import argparse
import asyncio
import json
import math
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any

import grpc
from dex import (
    AsyncClient,
    ClientOptions,
    DexServiceError,
    FlowAlreadyStartedError,
    FlowStatus,
    RPCInvokeOptions,
    StartFlowOptions,
)

from support_conversation.app import (
    build_registry,
    build_support_conversation_flow,
    flow_version_from_environment,
    import_flow_module,
    open_process_blob_cache,
    run_directory_from_environment,
    server_address_from_environment,
)
from support_conversation.transcript import (
    CURRENT_CHUNK_TOKEN,
    MAX_MESSAGE_CHARACTERS,
    ROLE_ASSISTANT,
    ROLE_CUSTOMER,
    STATUS_CLOSED,
    TRANSCRIPT_BUDGET_BYTES,
    ConversationSettings,
    ConversationState,
    ReplyRequest,
    TranscriptChunk,
    TranscriptRecord,
    count_retained_archived_chunks,
    customer_message_error,
    encoded_chunk_bytes,
    parse_transcript_page_token,
)

FLOW_VERSION = flow_version_from_environment()
FLOW_MODULE = import_flow_module(FLOW_VERSION)
ASSIGNED_AGENT = FLOW_MODULE.ASSIGNED_AGENT
CONVERSATION_STATE = FLOW_MODULE.CONVERSATION_STATE
SUMMARY = FLOW_MODULE.SUMMARY
TRANSCRIPT_CHUNKS = FLOW_MODULE.TRANSCRIPT_CHUNKS

LOAD_MESSAGE_CHARACTERS = 200
LOAD_MESSAGE_FILLER = "customer asks about order status and delivery window "
NON_BMP_FILLER_CHARACTER = "\U0001f4e6"
LOAD_MESSAGE_ID_PATTERN = re.compile(r"^load message ([^:]+): ")
PROGRESS_SECONDS = 30
STATE_POLL_SECONDS = 0.5
FIRST_REPLY_TIMEOUT_SECONDS = 60
# One RPC attempt; a timed-out attempt has an unknown outcome and is retried.
RPC_ATTEMPT_SECONDS = 15
RPC_ATTEMPTS = 3
UNKNOWN_OUTCOME_CODES = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)

# Producer back-pressure (see LoadRun). A run's active state budget is 64 MiB;
# the transcript keeps 32 MiB; 16 MiB stays free for Step inputs, request IDs,
# and the other Attributes.
DEFAULT_MAX_UNREPLIED_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_UNREPLIED_MESSAGES = 1000


def print_json(value: object) -> None:
    print(json.dumps(value, indent=2, default=str), flush=True)


def describe_error(error: BaseException) -> dict[str, Any]:
    described: dict[str, Any] = {"type": type(error).__name__, "message": str(error)}
    if isinstance(error, DexServiceError):
        described.update(
            code=str(error.code),
            sub_status=str(error.sub_status),
            detail=error.detail,
            operation=error.operation,
        )
    return described


def is_unknown_outcome(error: BaseException) -> bool:
    """A caller-side timeout or an unavailable server: the RPC may still commit."""
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return True
    return isinstance(error, DexServiceError) and error.code in UNKNOWN_OUTCOME_CODES


def message_request_id(conversation_id: str, message_id: str) -> str:
    """The same request ID on every attempt of one logical message."""
    return f"{conversation_id}/message/{message_id}"


def load_message_text(message_id: str, characters: int, kind: str) -> str:
    prefix = f"load message {message_id}: "
    if kind == "non-bmp":
        return prefix + NON_BMP_FILLER_CHARACTER * (characters - len(prefix))
    repeats = characters // len(LOAD_MESSAGE_FILLER) + 1
    return (prefix + LOAD_MESSAGE_FILLER * repeats)[:characters]


def channel_value_bytes(text: str) -> int:
    """Bytes of the message as a Channel value: its JSON string."""
    return len(json.dumps(text).encode("utf-8"))


def recorded_process_id(role: str) -> int | None:
    """PID from the scripts' process record in the run directory, if any."""
    record = run_directory_from_environment() / "processes" / f"{role}.record"
    try:
        return int(record.read_text(encoding="utf-8").splitlines()[0])
    except (OSError, ValueError, IndexError):
        return None


def host_load_sample() -> dict[str, Any]:
    """uptime, plus the CPU % of this run's Worker and Dex Server process trees."""
    uptime = subprocess.run(["uptime"], capture_output=True, text=True, check=False)
    table = subprocess.run(
        ["ps", "-A", "-o", "pid=,ppid=,pcpu="], capture_output=True, text=True, check=False
    )
    children: dict[int, list[int]] = {}
    cpu: dict[int, float] = {}
    for line in table.stdout.splitlines():
        fields = line.split()
        if len(fields) == 3:
            pid, parent = int(fields[0]), int(fields[1])
            children.setdefault(parent, []).append(pid)
            cpu[pid] = float(fields[2])

    def tree_cpu(root: int | None) -> float | None:
        if root is None or root not in cpu:
            return None
        total, pending = 0.0, [root]
        while pending:
            pid = pending.pop()
            total += cpu.get(pid, 0.0)
            pending.extend(children.get(pid, []))
        return round(total, 1)

    return {
        "uptime": uptime.stdout.strip(),
        "workerCpuPercent": tree_cpu(recorded_process_id("worker")),
        "dexServerCpuPercent": tree_cpu(recorded_process_id("dexcli-dev")),
    }


def read_history_event_summary(conversation_id: str) -> dict[str, int | None]:
    """Return the size of the retained history window and the highest event ID.

    Dex keeps a recent window of history, so historyEventsRetained is not a
    running total; highestEventId counts every event the run wrote. Neither
    counts Step method attempts. Both are None if unavailable.
    """
    summary: dict[str, int | None] = {"historyEventsRetained": None, "highestEventId": None}
    dexcli = shutil.which("dexcli")
    if dexcli is None:
        return summary
    completed = subprocess.run(
        [
            dexcli,
            "flow",
            "history",
            conversation_id,
            "-all",
            "-no-hydrate",
            "-page-size",
            "1000",
            "-server",
            server_address_from_environment(),
            "-timeout",
            "120s",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return summary
    try:
        event_ids = [
            int(event["eventId"])
            for line in completed.stdout.splitlines()
            if line.strip()
            for event in json.loads(line).get("events", [])
        ]
    except (json.JSONDecodeError, KeyError, ValueError):
        return summary
    summary["historyEventsRetained"] = len(event_ids)
    summary["highestEventId"] = max(event_ids, default=None)
    return summary


def summarize_page(page: dict[str, Any]) -> dict[str, Any]:
    records = page["records"]
    return {
        "label": "beyond retention" if page["is_beyond_retention"] else "retained",
        "page_token": page["page_token"],
        "records": len(records),
        "customer_records": sum(1 for r in records if r["role"] == ROLE_CUSTOMER),
        "assistant_records": sum(1 for r in records if r["role"] == ROLE_ASSISTANT),
        "sequences_newest_to_oldest": [records[0]["sequence"], records[-1]["sequence"]]
        if records
        else None,
        "next_older_page_token": page["next_older_page_token"],
        "oldest_retained_page_token": page["oldest_retained_page_token"],
        "is_beyond_retention": page["is_beyond_retention"],
        "newest_two": [
            {**record, "text": record["text"][:120]} for record in records[:2]
        ],
    }


@dataclass
class LoadSettings:
    count: int
    # Messages per second; 0 posts as fast as the limits allow.
    rate: float
    posting_seconds: float
    drain_seconds: float
    concurrency: int
    message_characters: int
    message_kind: str
    message_id_prefix: str
    snapshot_thresholds: list[int]
    max_unreplied_messages: int
    max_unreplied_bytes: int

    def validate(self) -> None:
        def require(condition: bool, message: str) -> None:
            if not condition:
                raise ValueError(message)

        require(self.count >= 1, "the message count must be at least 1")
        require(math.isfinite(self.rate) and self.rate >= 0, "the rate must be >= 0")
        require(
            math.isfinite(self.posting_seconds) and self.posting_seconds > 0,
            "the posting time must be > 0 seconds",
        )
        require(
            math.isfinite(self.drain_seconds) and self.drain_seconds >= 0,
            "the drain time must be >= 0 seconds",
        )
        require(self.concurrency >= 1, "the concurrency must be at least 1")
        require(self.message_kind in ("ascii", "non-bmp"), "kind is ascii or non-bmp")
        require(
            re.fullmatch(r"[A-Za-z0-9_-]*", self.message_id_prefix) is not None,
            "the message ID prefix is letters, digits, '_' and '-'",
        )
        longest_prefix = len(f"load message {self.message_id_prefix}{self.count:06d}: ")
        require(
            longest_prefix < self.message_characters <= MAX_MESSAGE_CHARACTERS,
            f"message characters must be {longest_prefix + 1} to {MAX_MESSAGE_CHARACTERS}",
        )
        require(self.max_unreplied_messages >= 1, "max unreplied messages must be >= 1")
        require(self.max_unreplied_bytes >= 1, "max unreplied bytes must be >= 1")
        require(
            all(1 <= threshold <= self.count for threshold in self.snapshot_thresholds),
            "every snapshot threshold must be between 1 and the message count",
        )


@dataclass
class LoadCounters:
    offered: int = 0
    sent: int = 0
    acknowledged: int = 0
    unknown: int = 0
    rejected: int = 0
    dropped_slots: int = 0
    throttled_slots: int = 0
    throttled_seconds: float = 0.0
    retried_attempts: int = 0
    acknowledged_ids: set[str] = field(default_factory=set)
    unknown_ids: set[str] = field(default_factory=set)


class LoadRun:
    """Posts one conversation's load and checks that every message got one reply.

    Producer policy (mirror it on the Temporal side):
    - With a rate, message i is due at start + i / rate.
    - Back-pressure: a message is sent only while the messages sent and not
      yet recorded with their reply stay below max_unreplied_messages and
      their Channel bytes below max_unreplied_bytes. A due message waits.
    - A message more than one interval (1 / rate) late is dropped and counted.
    - At most concurrency RPCs are in flight.
    - Every attempt of one message uses the same request ID. A timed-out or
      UNAVAILABLE attempt is retried up to RPC_ATTEMPTS times, then counted
      as unknown. Any other error is a rejection and stops posting.

    The application itself does no admission control: post_message validates
    and queues each message. This driver keeps the queued messages, which
    count toward the run's 64 MiB active state, within the stated bounds.
    """

    def __init__(
        self, client: AsyncClient, flow: Any, conversation_id: str, settings: LoadSettings
    ) -> None:
        self.client = client
        self.flow = flow
        self.conversation_id = conversation_id
        self.settings = settings
        self.counters = LoadCounters()
        self.started = 0.0
        self.baseline_customers = 0
        self.committed = 0
        self.state = ConversationState()
        self.state_updated = asyncio.Event()
        self.stop_posting = asyncio.Event()
        self.first_error: BaseException | None = None
        self.snapshots: list[dict[str, Any]] = []
        self.snapshot_tasks: list[asyncio.Task[None]] = []
        self.pending_thresholds = sorted(set(settings.snapshot_thresholds))
        self.message_bytes = channel_value_bytes(
            load_message_text(
                self.message_id(settings.count - 1),
                settings.message_characters,
                settings.message_kind,
            )
        )

    def message_id(self, index: int) -> str:
        return f"{self.settings.message_id_prefix}{index + 1:06d}"

    def elapsed(self) -> float:
        return round(time.monotonic() - self.started, 1)

    async def read_state(self) -> ConversationState:
        values = await self.client.get_attributes(self.conversation_id, CONVERSATION_STATE)
        return values.get(CONVERSATION_STATE) or ConversationState()

    async def wait_for_first_reply(self) -> float | None:
        """Wait until the first message is answered; return the wait or None on timeout."""
        waited_from = time.monotonic()
        while time.monotonic() - waited_from <= FIRST_REPLY_TIMEOUT_SECONDS:
            state = await self.read_state()
            if (
                state.customer_message_count >= 1
                and state.assistant_reply_count == state.customer_message_count
            ):
                self.baseline_customers = state.customer_message_count
                self.state = state
                return round(time.monotonic() - waited_from, 1)
            await asyncio.sleep(0.2)
        return None

    async def follow_state(self) -> None:
        while True:
            self.state = await self.read_state()
            self.committed = self.state.customer_message_count - self.baseline_customers
            self.state_updated.set()
            await asyncio.sleep(STATE_POLL_SECONDS)

    def unreplied_messages(self) -> int:
        """Sent messages, in flight or acknowledged, whose reply is not recorded yet."""
        return max(self.counters.sent - self.committed, 0)

    def is_over_limits(self) -> bool:
        unreplied = self.unreplied_messages()
        return (
            unreplied >= self.settings.max_unreplied_messages
            or (unreplied + 1) * self.message_bytes > self.settings.max_unreplied_bytes
        )

    def progress(self) -> dict[str, Any]:
        counters = self.counters
        return {
            "elapsedSeconds": self.elapsed(),
            "offered": counters.offered,
            "sent": counters.sent,
            "acknowledged": counters.acknowledged,
            "unknown": counters.unknown,
            "rejected": counters.rejected,
            "committedReplies": self.committed,
            # Negative while a committed message's RPC has not returned yet.
            "backlog": counters.acknowledged - self.committed,
            "unrepliedSent": self.unreplied_messages(),
            "throttledSlots": counters.throttled_slots,
            "throttledSeconds": round(counters.throttled_seconds, 1),
            "droppedSlots": counters.dropped_slots,
            "transcriptBytes": self.state.transcript_bytes,
            "retainedArchivedChunkCount": count_retained_archived_chunks(self.state),
        }

    async def post_one(self, index: int, in_flight: asyncio.Semaphore) -> None:
        message_id = self.message_id(index)
        text = load_message_text(
            message_id, self.settings.message_characters, self.settings.message_kind
        )
        options = RPCInvokeOptions(
            request_id=message_request_id(self.conversation_id, message_id)
        )
        try:
            for attempt in range(1, RPC_ATTEMPTS + 1):
                try:
                    await asyncio.wait_for(
                        self.client.invoke_rpc(
                            self.flow.post_message,
                            self.conversation_id,
                            text,
                            options=options,
                        ),
                        RPC_ATTEMPT_SECONDS,
                    )
                except Exception as error:  # classify the exact engine error
                    if is_unknown_outcome(error) and attempt < RPC_ATTEMPTS:
                        self.counters.retried_attempts += 1
                        continue
                    if is_unknown_outcome(error):
                        self.counters.unknown += 1
                        self.counters.unknown_ids.add(message_id)
                    else:
                        self.counters.rejected += 1
                        self.stop_posting.set()
                    if self.first_error is None:
                        self.first_error = error
                    return
                self.counters.acknowledged += 1
                self.counters.acknowledged_ids.add(message_id)
                self.take_due_snapshots()
                return
        finally:
            in_flight.release()

    def take_due_snapshots(self) -> None:
        while (
            self.pending_thresholds
            and self.counters.acknowledged >= self.pending_thresholds[0]
        ):
            threshold = self.pending_thresholds.pop(0)
            self.snapshot_tasks.append(
                asyncio.create_task(self.record_snapshot(threshold))
            )

    async def record_snapshot(self, threshold: int) -> None:
        triggered = {
            "threshold": threshold,
            "acknowledgedAtTrigger": self.counters.acknowledged,
            "triggerSeconds": self.elapsed(),
        }
        state = await self.read_state()
        committed = state.customer_message_count - self.baseline_customers
        snapshot = {
            **triggered,
            "stateReadSeconds": self.elapsed(),
            "acknowledgedAtStateRead": self.counters.acknowledged,
            "committedReplies": committed,
            "backlogAtStateRead": self.counters.acknowledged - committed,
            "transcriptBytes": state.transcript_bytes,
            "retainedArchivedChunkCount": count_retained_archived_chunks(state),
            "flowStatus": (await self.client.describe_flow(self.conversation_id)).status.name,
        }
        snapshot["history"] = await asyncio.to_thread(
            read_history_event_summary, self.conversation_id
        )
        snapshot["historyReadSeconds"] = self.elapsed()
        self.snapshots.append(snapshot)
        print_json({"snapshot": snapshot})

    async def produce(self) -> None:
        settings = self.settings
        in_flight = asyncio.Semaphore(settings.concurrency)
        posts: list[asyncio.Task[None]] = []
        interval = 1 / settings.rate if settings.rate > 0 else 0.0
        posting_deadline = self.started + settings.posting_seconds + interval
        for index in range(settings.count):
            due = self.started + index * interval
            if due > posting_deadline or self.stop_posting.is_set():
                break
            delay = due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            if time.monotonic() > posting_deadline:
                break
            self.counters.offered += 1
            await in_flight.acquire()
            if self.is_over_limits():
                self.counters.throttled_slots += 1
                throttled_from = time.monotonic()
                while self.is_over_limits() and time.monotonic() < posting_deadline:
                    self.state_updated.clear()
                    try:
                        await asyncio.wait_for(self.state_updated.wait(), 1)
                    except asyncio.TimeoutError:
                        pass
                self.counters.throttled_seconds += time.monotonic() - throttled_from
            late = time.monotonic() - due
            if (interval and late > interval) or self.is_over_limits():
                self.counters.dropped_slots += 1
                in_flight.release()
                continue
            self.counters.sent += 1
            posts.append(asyncio.create_task(self.post_one(index, in_flight)))
        await asyncio.gather(*posts)

    async def report_progress(self) -> None:
        while True:
            await asyncio.sleep(PROGRESS_SECONDS)
            print_json(
                {
                    "progress": self.progress(),
                    "hostLoad": await asyncio.to_thread(host_load_sample),
                }
            )

    async def drain(self) -> float | None:
        """Wait until every acknowledged message is recorded with its reply."""
        drain_started = time.monotonic()
        while time.monotonic() - drain_started <= self.settings.drain_seconds:
            self.state = await self.read_state()
            self.committed = self.state.customer_message_count - self.baseline_customers
            if (
                self.committed >= self.counters.acknowledged
                and self.state.assistant_reply_count == self.state.customer_message_count
            ):
                return round(time.monotonic() - drain_started, 1)
            await asyncio.sleep(1)
        return None

    async def run(self) -> int:
        self.settings.validate()
        first_reply_seconds = await self.wait_for_first_reply()
        if first_reply_seconds is None:
            print_json(
                {"load": {"error": f"first reply not recorded in {FIRST_REPLY_TIMEOUT_SECONDS}s"}}
            )
            return 1
        print_json(
            {
                "firstMessage": {
                    "recordedWithReply": True,
                    "waitedSeconds": first_reply_seconds,
                    "baselineCustomerMessages": self.baseline_customers,
                },
                "producerPolicy": {
                    "rate": self.settings.rate,
                    "count": self.settings.count,
                    "postingSeconds": self.settings.posting_seconds,
                    "drainSeconds": self.settings.drain_seconds,
                    "maxInFlight": self.settings.concurrency,
                    "maxUnrepliedMessages": self.settings.max_unreplied_messages,
                    "maxUnrepliedBytes": self.settings.max_unreplied_bytes,
                    "messageCharacters": self.settings.message_characters,
                    "messageKind": self.settings.message_kind,
                    "messageChannelBytes": self.message_bytes,
                    "lateSlotDropAfterSeconds": 1 / self.settings.rate
                    if self.settings.rate
                    else None,
                    "rpcAttemptSeconds": RPC_ATTEMPT_SECONDS,
                    "rpcAttempts": RPC_ATTEMPTS,
                },
            }
        )
        self.started = time.monotonic()
        follower = asyncio.create_task(self.follow_state())
        reporter = asyncio.create_task(self.report_progress())
        try:
            await self.produce()
            posting_seconds = self.elapsed()
            drain_seconds = await self.drain()
        finally:
            follower.cancel()
            reporter.cancel()
            await asyncio.gather(follower, reporter, return_exceptions=True)
        await asyncio.gather(*self.snapshot_tasks)
        flow_status = (await self.client.describe_flow(self.conversation_id)).status
        if drain_seconds is None:
            # The Worker still records messages, so page reads would mix snapshots.
            verification = {
                "result": "not run: the Flow is still recording; backlog "
                f"{self.counters.acknowledged - self.committed}",
                "errors": [],
            }
        else:
            verification = await verify_transcript(
                self.client,
                self.flow,
                self.conversation_id,
                self.settings.message_id_prefix,
                self.counters.acknowledged_ids,
                self.counters.unknown_ids,
            )
        errors: list[str] = []
        if self.first_error is not None and self.counters.rejected:
            errors.append(f"a message was rejected: {self.first_error}")
        if drain_seconds is None:
            errors.append(
                f"backlog {self.counters.acknowledged - self.committed} after "
                f"{self.settings.drain_seconds}s of drain"
            )
        if flow_status != FlowStatus.RUNNING:
            errors.append(f"Flow is {flow_status.name}")
        if self.pending_thresholds:
            errors.append(f"snapshot thresholds not reached: {self.pending_thresholds}")
        if self.counters.offered < self.settings.count:
            errors.append(
                f"only {self.counters.offered} of {self.settings.count} messages offered "
                "before the posting deadline"
            )
        errors.extend(verification["errors"])
        result = {
            "conversation": self.conversation_id,
            "postingSeconds": posting_seconds,
            "drainSeconds": drain_seconds,
            "requested": self.settings.count,
            # requested = sent + droppedSlots + notOffered
            "notOffered": self.settings.count - self.counters.offered,
            "end": self.progress(),
            "retriedAttempts": self.counters.retried_attempts,
            "firstError": describe_error(self.first_error) if self.first_error else None,
            "flowStatus": flow_status.name,
            "snapshots": sorted(self.snapshots, key=lambda s: s["threshold"]),
            "verification": verification,
            "errors": errors,
        }
        print_json({"load": result})
        return 1 if errors else 0


async def read_page(
    client: AsyncClient, flow: Any, conversation_id: str, page_token: str
) -> dict[str, Any]:
    page_token = parse_transcript_page_token(page_token)
    page = await client.invoke_rpc(
        flow.get_transcript_page,
        conversation_id,
        page_token,
        options=RPCInvokeOptions(
            load_attribute_map_instances=(TRANSCRIPT_CHUNKS.load(page_token),)
        ),
    )
    return asdict(page)


async def verify_transcript(
    client: AsyncClient,
    flow: Any,
    conversation_id: str,
    message_id_prefix: str | None = None,
    acknowledged_ids: set[str] | None = None,
    unknown_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Read every retained page and check the transcript against its bookkeeping.

    Retained records are checked one by one: contiguous sequences, a reply
    right after each customer message, serialized bytes equal to
    ConversationState.transcript_bytes and within the budget, and each load
    message ID at most once. Dropped records are checked by the Summary
    counters. When nothing was dropped, the load's message IDs must match
    the acknowledged ones, plus any unknown-outcome ones that committed.
    Call it after the load settled; each page read is its own snapshot.
    """
    errors: list[str] = []
    values = await client.get_attributes(conversation_id, CONVERSATION_STATE, SUMMARY)
    state = values.get(CONVERSATION_STATE) or ConversationState()
    summary = values.get(SUMMARY)
    token = CURRENT_CHUNK_TOKEN
    records: list[dict[str, Any]] = []
    serialized_bytes = 0
    pages = 0
    while token:
        page = await read_page(client, flow, conversation_id, token)
        pages += 1
        page_records = page["records"]
        chunk = TranscriptChunk([TranscriptRecord(**r) for r in reversed(page_records)])
        serialized_bytes += encoded_chunk_bytes(chunk)
        records.extend(page_records)
        token = page["next_older_page_token"]
    records.reverse()
    summarized_customers = summary.summarized_customer_count if summary else 0
    summarized_replies = summary.summarized_assistant_count if summary else 0
    summarized_records = summary.summarized_record_count if summary else 0
    for offset, record in enumerate(records):
        expected_sequence = summarized_records + offset + 1
        if record["sequence"] != expected_sequence:
            errors.append(f"sequence {record['sequence']} where {expected_sequence} belongs")
            break
        expected_role = ROLE_CUSTOMER if expected_sequence % 2 == 1 else ROLE_ASSISTANT
        if record["role"] != expected_role:
            errors.append(f"record {expected_sequence} is {record['role']}, not {expected_role}")
            break
    if records and records[-1]["role"] != ROLE_ASSISTANT:
        errors.append("the newest customer message has no reply")
    retained_customers = sum(1 for r in records if r["role"] == ROLE_CUSTOMER)
    retained_replies = len(records) - retained_customers
    if summarized_customers + retained_customers != state.customer_message_count:
        errors.append("Summary plus retained customer records differ from the count")
    if summarized_replies + retained_replies != state.assistant_reply_count:
        errors.append("Summary plus retained replies differ from the count")
    if state.customer_message_count != state.assistant_reply_count:
        errors.append("customer messages and replies differ")
    if serialized_bytes != state.transcript_bytes:
        errors.append(
            f"serialized chunks {serialized_bytes} B != transcriptBytes {state.transcript_bytes} B"
        )
    if serialized_bytes > TRANSCRIPT_BUDGET_BYTES:
        errors.append(f"serialized chunks {serialized_bytes} B exceed the budget")
    result: dict[str, Any] = {
        "pagesRead": pages,
        "retainedRecords": len(records),
        "summarizedRecords": summarized_records,
        "serializedTranscriptBytes": serialized_bytes,
        "transcriptBudgetBytes": TRANSCRIPT_BUDGET_BYTES,
        "customerMessages": state.customer_message_count,
        "replies": state.assistant_reply_count,
    }
    if message_id_prefix is not None:
        seen: dict[str, int] = {}
        for record in records:
            if record["role"] != ROLE_CUSTOMER:
                continue
            match = LOAD_MESSAGE_ID_PATTERN.match(record["text"])
            if match and match.group(1).startswith(message_id_prefix):
                seen[match.group(1)] = seen.get(match.group(1), 0) + 1
        duplicates = sorted(message_id for message_id, n in seen.items() if n > 1)
        result.update(loadMessageIdsRetained=len(seen), duplicateMessageIds=duplicates[:20])
        if duplicates:
            errors.append(f"{len(duplicates)} message IDs recorded more than once")
        if acknowledged_ids is None:
            # A standalone check knows no sent IDs; it checks uniqueness only.
            result["idCheck"] = "unique IDs only: the sent IDs are not known here"
            result["errors"] = errors
            return result
        acknowledged = acknowledged_ids
        unknown = unknown_ids or set()
        unexpected = sorted(set(seen) - acknowledged - unknown)
        result.update(
            unexpectedMessageIds=unexpected[:20],
            unknownOutcomeIdsCommitted=len(set(seen) & unknown),
        )
        if unexpected:
            errors.append(f"{len(unexpected)} recorded message IDs were never sent")
        if summarized_records == 0:
            missing = sorted(acknowledged - set(seen))
            result["missingAcknowledgedIds"] = missing[:20]
            result["idCheck"] = "complete: nothing was dropped"
            if missing:
                errors.append(f"{len(missing)} acknowledged messages are not recorded")
        else:
            result["idCheck"] = (
                "retained records checked by ID; dropped records by Summary counters"
            )
    result["errors"] = errors
    return result


class SupportConversationDriver:
    def __init__(self, client: AsyncClient, flow: Any) -> None:
        self.client = client
        self.flow = flow

    async def start_conversation(
        self,
        conversation_id: str,
        first_message: str,
        idle_seconds: int,
        close_after_seconds: int,
    ) -> None:
        message_error = customer_message_error(first_message)
        if message_error:
            raise ValueError(f"--first-message: {message_error}")
        if idle_seconds < 1 or close_after_seconds < 1:
            raise ValueError("--idle-seconds and --close-after-seconds must be >= 1")
        options = (
            StartFlowOptions(
                request_id=f"start-{conversation_id}", ignore_already_started=True
            )
            .with_attribute(CONVERSATION_STATE, ConversationState())
            .with_attribute(TRANSCRIPT_CHUNKS, CURRENT_CHUNK_TOKEN, TranscriptChunk())
        )
        run_id = await self.client.start_flow(
            self.flow,
            conversation_id,
            ReplyRequest(
                ConversationSettings(idle_seconds, close_after_seconds),
                [first_message],
            ),
            options,
        )
        print_json(
            {
                "conversation": conversation_id,
                "runId": run_id,
                "firstMessage": first_message,
                "idleSeconds": idle_seconds,
                "closeAfterSeconds": close_after_seconds,
            }
        )

    async def post_message(
        self, conversation_id: str, text: str, message_id: str | None
    ) -> int:
        message_error = customer_message_error(text)
        if message_error:
            raise ValueError(message_error)
        options = (
            RPCInvokeOptions(request_id=message_request_id(conversation_id, message_id))
            if message_id
            else RPCInvokeOptions()
        )
        for attempt in range(1, RPC_ATTEMPTS + 1):
            try:
                await asyncio.wait_for(
                    self.client.invoke_rpc(
                        self.flow.post_message, conversation_id, text, options=options
                    ),
                    RPC_ATTEMPT_SECONDS,
                )
            except Exception as error:  # classify the exact engine error
                if not is_unknown_outcome(error):
                    raise
                if not message_id or attempt == RPC_ATTEMPTS:
                    print_json(
                        {
                            "conversation": conversation_id,
                            "outcome": "unknown",
                            "attempts": attempt,
                            "error": describe_error(error),
                        }
                    )
                    return 2
                continue
            print_json(
                {
                    "conversation": conversation_id,
                    "posted": text,
                    "messageId": message_id,
                    "attempts": attempt,
                }
            )
            return 0
        return 2

    async def assign_agent(self, conversation_id: str, agent_name: str) -> None:
        assigned = await self.client.invoke_rpc(
            self.flow.assign_agent, conversation_id, agent_name
        )
        print_json({"conversation": conversation_id, "assignedAgent": assigned})

    async def newest_records(self, conversation_id: str, count: int) -> list[dict[str, Any]]:
        """The newest records, oldest first; reads the newest archived page when current is short."""
        current = await read_page(self.client, self.flow, conversation_id, CURRENT_CHUNK_TOKEN)
        records = current["records"]
        if len(records) < count and current["next_older_page_token"]:
            older = await read_page(
                self.client, self.flow, conversation_id, current["next_older_page_token"]
            )
            records = records + older["records"]
        return list(reversed(records[:count]))

    async def read_status(self, conversation_id: str) -> dict[str, Any]:
        info = await self.client.describe_flow(conversation_id)
        values = await self.client.get_attributes(
            conversation_id, CONVERSATION_STATE, ASSIGNED_AGENT, SUMMARY
        )
        state = values.get(CONVERSATION_STATE) or ConversationState()
        summary = values.get(SUMMARY)
        closed = info.status == FlowStatus.COMPLETED
        return {
            "conversation": conversation_id,
            "flowStatus": info.status.name,
            # Closed only when the Flow run completed; Steps never write it.
            "conversationStatus": STATUS_CLOSED if closed else state.status,
            "recordCount": state.record_count,
            "customerMessageCount": state.customer_message_count,
            "assistantReplyCount": state.assistant_reply_count,
            "latestRecords": await self.newest_records(conversation_id, 4),
            "retainedArchivedChunkCount": count_retained_archived_chunks(state),
            "transcriptBytes": state.transcript_bytes,
            "transcriptBudgetBytes": TRANSCRIPT_BUDGET_BYTES,
            "oldestArchivedChunkToken": state.oldest_archived_chunk_token,
            "newestArchivedChunkToken": state.newest_archived_chunk_token,
            "assignedAgent": values.get(ASSIGNED_AGENT),
            "summary": asdict(summary) if summary is not None else None,
            "history": await asyncio.to_thread(read_history_event_summary, conversation_id),
        }

    async def wait_for_replies(
        self, conversation_id: str, reply_count: int, timeout_seconds: float
    ) -> int:
        """Wait until the conversation has reply_count replies; print the newest records."""
        started = time.monotonic()
        while True:
            values = await self.client.get_attributes(conversation_id, CONVERSATION_STATE)
            state = values.get(CONVERSATION_STATE) or ConversationState()
            elapsed = round(time.monotonic() - started, 1)
            if state.assistant_reply_count >= reply_count:
                print_json(
                    {
                        "conversation": conversation_id,
                        "replies": state.assistant_reply_count,
                        "customerMessages": state.customer_message_count,
                        "waitedSeconds": elapsed,
                        "newestTwoRecords": await self.newest_records(conversation_id, 2),
                    }
                )
                return 0
            if elapsed > timeout_seconds:
                print_json(
                    {
                        "conversation": conversation_id,
                        "error": f"{state.assistant_reply_count} of {reply_count} replies after {timeout_seconds}s",
                    }
                )
                return 1
            await asyncio.sleep(0.2)

    async def summarize_pages(self, conversation_id: str) -> int:
        """Newest page, oldest retained page, and a page older than retention, each labeled."""
        newest = await read_page(self.client, self.flow, conversation_id, CURRENT_CHUNK_TOKEN)
        print_json({"newestPage": summarize_page(newest)})
        oldest_token = newest["oldest_retained_page_token"]
        if oldest_token:
            oldest = await read_page(self.client, self.flow, conversation_id, oldest_token)
            print_json({"oldestRetainedPage": summarize_page(oldest)})
        first_token = "00000000000000000001"
        if oldest_token:
            first = await read_page(self.client, self.flow, conversation_id, first_token)
            print_json({"firstArchivedPageProbe": summarize_page(first)})
        return 0


def load_settings_from_arguments(arguments: argparse.Namespace) -> LoadSettings:
    if arguments.command == "paced":
        if not (math.isfinite(arguments.rate) and arguments.rate > 0):
            raise ValueError("--rate must be > 0 for paced")
        count = int(arguments.rate * arguments.duration_seconds)
        posting_seconds = arguments.duration_seconds
        drain_seconds = arguments.drain_seconds
    else:
        count = arguments.count
        posting_seconds = arguments.max_seconds
        drain_seconds = arguments.max_seconds
    return LoadSettings(
        count=count,
        rate=arguments.rate,
        posting_seconds=posting_seconds,
        drain_seconds=drain_seconds,
        concurrency=arguments.concurrency,
        message_characters=arguments.message_characters,
        message_kind=arguments.message_kind,
        message_id_prefix=arguments.message_id_prefix,
        snapshot_thresholds=[
            int(value) for value in arguments.snapshot_at.split(",") if value.strip()
        ],
        max_unreplied_messages=arguments.max_unreplied_messages,
        max_unreplied_bytes=arguments.max_unreplied_bytes,
    )


def add_load_arguments(parser: argparse.ArgumentParser, concurrency: int) -> None:
    parser.add_argument("--concurrency", type=int, default=concurrency, help="RPCs in flight")
    parser.add_argument("--message-characters", type=int, default=LOAD_MESSAGE_CHARACTERS)
    parser.add_argument("--message-kind", choices=("ascii", "non-bmp"), default="ascii")
    parser.add_argument("--message-id-prefix", default="")
    parser.add_argument(
        "--snapshot-at",
        default="",
        help="comma-separated acknowledged counts at which to record a snapshot",
    )
    parser.add_argument(
        "--max-unreplied-messages", type=int, default=DEFAULT_MAX_UNREPLIED_MESSAGES
    )
    parser.add_argument("--max-unreplied-bytes", type=int, default=DEFAULT_MAX_UNREPLIED_BYTES)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("conversation_id")
    start.add_argument("--first-message", required=True)
    start.add_argument("--idle-seconds", type=int, default=600)
    start.add_argument("--close-after-seconds", type=int, default=3600)
    post = commands.add_parser("post")
    post.add_argument("conversation_id")
    post.add_argument("text")
    post.add_argument(
        "--message-id", default=None, help="stable ID: every attempt sends the same request ID"
    )
    assign = commands.add_parser("assign")
    assign.add_argument("conversation_id")
    assign.add_argument("agent_name")
    page = commands.add_parser("page")
    page.add_argument("conversation_id")
    page.add_argument("token", nargs="?", default=CURRENT_CHUNK_TOKEN)
    pages_summary = commands.add_parser("pages-summary")
    pages_summary.add_argument("conversation_id")
    status = commands.add_parser("status")
    status.add_argument("conversation_id")
    flood = commands.add_parser("flood")
    flood.add_argument("conversation_id")
    flood.add_argument("--count", type=int, required=True)
    flood.add_argument("--rate", type=float, default=0, help="messages per second; 0 is unpaced")
    flood.add_argument(
        "--max-seconds", type=float, default=1800, help="bounds posting and, separately, the drain"
    )
    add_load_arguments(flood, concurrency=16)
    paced = commands.add_parser("paced")
    paced.add_argument("conversation_id")
    paced.add_argument("--rate", type=float, required=True, help="messages per second")
    paced.add_argument("--duration-seconds", type=float, required=True)
    paced.add_argument("--drain-seconds", type=float, default=60)
    add_load_arguments(paced, concurrency=64)
    wait_replies = commands.add_parser("wait-replies")
    wait_replies.add_argument("conversation_id")
    wait_replies.add_argument("reply_count", type=int)
    wait_replies.add_argument("--timeout-seconds", type=float, default=60)
    verify = commands.add_parser("verify-transcript")
    verify.add_argument("conversation_id")
    verify.add_argument("--message-id-prefix", default=None)
    return parser.parse_args(argv)


async def run_command(arguments: argparse.Namespace) -> int:
    flow = build_support_conversation_flow(FLOW_VERSION)
    cache, cache_directory = open_process_blob_cache("client")
    client = AsyncClient(
        build_registry(flow),
        cache,
        ClientOptions(server_address=server_address_from_environment()),
    )
    driver = SupportConversationDriver(client, flow)
    try:
        if arguments.command == "start":
            await driver.start_conversation(
                arguments.conversation_id,
                arguments.first_message,
                arguments.idle_seconds,
                arguments.close_after_seconds,
            )
        elif arguments.command == "post":
            return await driver.post_message(
                arguments.conversation_id, arguments.text, arguments.message_id
            )
        elif arguments.command == "assign":
            await driver.assign_agent(arguments.conversation_id, arguments.agent_name)
        elif arguments.command == "page":
            print_json(
                await read_page(client, flow, arguments.conversation_id, arguments.token)
            )
        elif arguments.command == "pages-summary":
            return await driver.summarize_pages(arguments.conversation_id)
        elif arguments.command == "status":
            print_json(await driver.read_status(arguments.conversation_id))
        elif arguments.command in ("flood", "paced"):
            return await LoadRun(
                client, flow, arguments.conversation_id, load_settings_from_arguments(arguments)
            ).run()
        elif arguments.command == "wait-replies":
            return await driver.wait_for_replies(
                arguments.conversation_id,
                arguments.reply_count,
                arguments.timeout_seconds,
            )
        elif arguments.command == "verify-transcript":
            verification = await verify_transcript(
                client, flow, arguments.conversation_id, arguments.message_id_prefix
            )
            print_json({"verification": verification})
            return 1 if verification["errors"] else 0
        return 0
    except (DexServiceError, FlowAlreadyStartedError, ValueError) as error:
        print_json({"error": describe_error(error)})
        return 1
    finally:
        await client.close()
        cache.close()
        shutil.rmtree(cache_directory, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(asyncio.run(run_command(parse_arguments(sys.argv[1:]))))
