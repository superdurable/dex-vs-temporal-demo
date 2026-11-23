"""Command-line driver for the Temporal conversation demo.

Usage (from this directory):
  uv run --locked drive.py start <conversation-id> --first-message <text> [--variant V] [--idle-seconds N] [--close-after-seconds N]
  uv run --locked drive.py post <conversation-id> <text>
  uv run --locked drive.py assign <conversation-id> <agent-name>
  uv run --locked drive.py page <conversation-id> [token]
  uv run --locked drive.py status <conversation-id>
  uv run --locked drive.py flood <conversation-id> --count N --rate R [--message-bytes B] [--backlog-timeout S] [--result-file F]
  uv run --locked drive.py inspect <conversation-id> [--run-id R] [--dump history.json]
  uv run --locked drive.py wait-replies <conversation-id> --count N [--timeout S] [--expect reply|stall] [--failure-cause C] [--expect-patch-marker]
  uv run --locked drive.py watch-task-retries <conversation-id> --seconds N [--interval S]
  uv run --locked drive.py runs <conversation-id>
  uv run --locked drive.py verify <conversation-id> --flood-result F [--allow-query-failure]
  uv run --locked drive.py verify-history <conversation-id> --flood-result F --expect all-answered|stall [--stall-cause C]
  uv run --locked drive.py history-summary <conversation-id> [--output F]
  uv run --locked drive.py timeline <conversation-id> --expect naive|correct --idle-seconds N --close-after-seconds N

The conversation ID is the workflow ID. Set TEMPORAL_ADDRESS to reach a
server other than localhost:7233.

Exit codes: 0 the observed behavior is the expected one (an expected stall
included); 1 the command or the observation itself failed (an RPC error or
a timeout); 2 an assertion failed (the behavior is not the expected one).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.history.v1 import HistoryEvent
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryReverseRequest,
)
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError, RPCStatusCode

from activities import OUTBOX_FILE
from local_file_storage import build_data_converter

from conversation_shared import (
    CORRECT_WORKFLOW_TYPE,
    DEFAULT_CLOSE_AFTER_SECONDS,
    DEFAULT_IDLE_SECONDS,
    INLINE_TRANSCRIPT_WORKFLOW_TYPE,
    NAIVE_WORKFLOW_TYPE,
    TASK_QUEUE,
    ConversationInput,
)

WORKFLOW_TYPE_BY_VARIANT = {
    "naive": NAIVE_WORKFLOW_TYPE,
    "correct": CORRECT_WORKFLOW_TYPE,
    "continue-as-new-inline-transcript": INLINE_TRANSCRIPT_WORKFLOW_TYPE,
}
FLOOD_MESSAGE_BYTES = 200
FLOOD_INDEX_MINIMUM_WIDTH = 6
FLOOD_PROGRESS_SECONDS = 60
FLOOD_BACKLOG_TIMEOUT_SECONDS = 900
SIGNAL_RPC_TIMEOUT = timedelta(seconds=10)
READ_RPC_TIMEOUT = timedelta(seconds=30)
QUERY_TIMEOUT = timedelta(seconds=10)
EXTERNAL_STORAGE_REFERENCE_MESSAGE_TYPE = "temporal.api.sdk.v1.ExternalStorageReference"

EXIT_OK = 0
EXIT_OBSERVATION_FAILED = 1
EXIT_ASSERTION_FAILED = 2

FLOOD_INDEX_PATTERN = re.compile(r"^flood message (\d+): ")
REPLY_SEQUENCE_PATTERN = re.compile(r"^Reply to message (\d+):")
FIRST_MESSAGE_KEY = "first"


async def connect_client() -> Client:
    # A fixed identity keeps the host name out of recorded histories.
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        identity="conversation-demo-driver",
        data_converter=build_data_converter(),
    )


def print_json(value: Any) -> None:
    print(json.dumps(value, indent=2), flush=True)


def describe_error(error: BaseException) -> str:
    if isinstance(error, RPCError):
        return f"RPCError status={error.status.name} message={error.message}"
    return f"{type(error).__name__}: {error}"


def event_type_name(event: HistoryEvent) -> str:
    return EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")


def event_time(event: HistoryEvent) -> datetime:
    return event.event_time.ToDatetime()


def positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"{text} must be at least 1")
    return value


def non_negative_float(text: str) -> float:
    value = float(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"{text} must not be negative")
    return value


def positive_float(text: str) -> float:
    value = float(text)
    if value <= 0:
        raise argparse.ArgumentTypeError(f"{text} must be greater than 0")
    return value


def non_empty_text(text: str) -> str:
    if not text.strip():
        raise argparse.ArgumentTypeError("the message must not be empty")
    return text


# ---------------------------------------------------------------------------
# Simple commands


async def start_conversation(client: Client, args: argparse.Namespace) -> int:
    workflow_type = WORKFLOW_TYPE_BY_VARIANT[args.variant]
    handle = await client.start_workflow(
        workflow_type,
        ConversationInput(
            args.conversation_id,
            args.first_message,
            args.idle_seconds,
            args.close_after_seconds,
        ),
        id=args.conversation_id,
        task_queue=TASK_QUEUE,
    )
    print_json(
        {
            "conversation": args.conversation_id,
            "workflow_type": workflow_type,
            "run_id": handle.result_run_id,
            "idle_seconds": args.idle_seconds,
            "close_after_seconds": args.close_after_seconds,
        }
    )
    return EXIT_OK


async def post_message(client: Client, args: argparse.Namespace) -> int:
    await client.get_workflow_handle(args.conversation_id).signal(
        "post_message", args.text, rpc_timeout=SIGNAL_RPC_TIMEOUT
    )
    print_json({"conversation": args.conversation_id, "posted": args.text})
    return EXIT_OK


async def assign_agent(client: Client, args: argparse.Namespace) -> int:
    previous_agent = await client.get_workflow_handle(
        args.conversation_id
    ).execute_update("assign_agent", args.agent_name, rpc_timeout=READ_RPC_TIMEOUT)
    print_json(
        {
            "conversation": args.conversation_id,
            "agent": args.agent_name,
            "previous_agent": previous_agent,
        }
    )
    return EXIT_OK


async def print_transcript_page(client: Client, args: argparse.Namespace) -> int:
    page = await client.get_workflow_handle(args.conversation_id).query(
        "get_transcript_page", args.token, rpc_timeout=QUERY_TIMEOUT
    )
    print_json(page)
    return EXIT_OK


async def read_last_history_event(client: Client, conversation_id: str) -> dict[str, Any]:
    response = await client.workflow_service.get_workflow_execution_history_reverse(
        GetWorkflowExecutionHistoryReverseRequest(
            namespace=client.namespace,
            execution=WorkflowExecution(workflow_id=conversation_id),
            maximum_page_size=1,
        ),
        timeout=READ_RPC_TIMEOUT,
    )
    if not response.history.events:
        return {}
    return describe_event_briefly(response.history.events[0])


def describe_event_briefly(event: HistoryEvent) -> dict[str, Any]:
    brief: dict[str, Any] = {
        "event_id": event.event_id,
        "event_type": event_type_name(event),
        "time": event.event_time.ToJsonString(),
    }
    if event.HasField("workflow_execution_terminated_event_attributes"):
        attributes = event.workflow_execution_terminated_event_attributes
        brief["reason"] = attributes.reason
        brief["identity"] = attributes.identity
    return brief


async def read_conversation_status(
    client: Client, conversation_id: str
) -> dict[str, Any]:
    handle: WorkflowHandle = client.get_workflow_handle(conversation_id)
    description = await handle.describe(rpc_timeout=READ_RPC_TIMEOUT)
    status: dict[str, Any] = {
        "conversation": conversation_id,
        "run_id": description.run_id,
        "workflow_type": description.workflow_type,
        "workflow_status": description.status.name if description.status else None,
        "history_event_count": description.history_length,
        "history_size_bytes": description.raw_description.workflow_execution_info.history_size_bytes,
    }
    try:
        summary = await handle.query("get_summary", rpc_timeout=QUERY_TIMEOUT)
        for key in (
            "agent",
            "message_count",
            "customer_message_count",
            "assistant_message_count",
            "pending_reply_count",
        ):
            status[key] = summary[key]
        status["conversation_status"] = summary["status"]
    except Exception as error:
        status["query_error"] = describe_error(error)
    status["last_history_event"] = await read_last_history_event(
        client, conversation_id
    )
    return status


async def print_status(client: Client, args: argparse.Namespace) -> int:
    print_json(await read_conversation_status(client, args.conversation_id))
    return EXIT_OK


# ---------------------------------------------------------------------------
# Flood


def flood_index_width(count: int) -> int:
    return max(FLOOD_INDEX_MINIMUM_WIDTH, len(str(count)))


def flood_message_prefix(index: int, width: int) -> str:
    return f"flood message {index:0{width}d}: "


def build_flood_message(index: int, width: int, size: int) -> str:
    prefix = flood_message_prefix(index, width)
    filler = "the customer repeats the same question about the order status. "
    return (prefix + filler * (size // len(filler) + 1))[:size]


def compress_to_ranges(indexes: list[int]) -> list[list[int]]:
    ranges: list[list[int]] = []
    for index in sorted(indexes):
        if ranges and index == ranges[-1][1] + 1:
            ranges[-1][1] = index
        else:
            ranges.append([index, index])
    return ranges


def expand_ranges(ranges: list[list[int]]) -> set[int]:
    return {index for first, last in ranges for index in range(first, last + 1)}


def is_client_timeout(error: BaseException) -> bool:
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return True
    return isinstance(error, RPCError) and error.status in (
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.CANCELLED,
    )


@dataclass
class FloodProgress:
    started_at: float
    rpc_attempts: int = 0
    acknowledged: list[int] = field(default_factory=list)
    client_timeouts: list[dict[str, Any]] = field(default_factory=list)
    rejection: dict[str, Any] | None = None
    max_schedule_lag_seconds: float = 0.0
    sending_finished: bool = False


async def observe_flood(
    handle: WorkflowHandle, progress: FloodProgress, interval_seconds: float
) -> None:
    """Prints progress on its own schedule, so slow reads never delay sending."""
    while not progress.sending_finished:
        await asyncio.sleep(interval_seconds)
        if progress.sending_finished:
            return
        line = (
            f"progress elapsed={time.monotonic() - progress.started_at:.1f}s "
            f"rpc_attempts={progress.rpc_attempts} acknowledged={len(progress.acknowledged)} "
            f"client_timeouts={len(progress.client_timeouts)}"
        )
        try:
            description = await handle.describe(rpc_timeout=READ_RPC_TIMEOUT)
            line += (
                f" history_events={description.history_length}"
                f" workflow_status={description.status.name if description.status else None}"
            )
        except Exception as error:
            line += f" describe_error=({describe_error(error)})"
        try:
            summary = await handle.query("get_summary", rpc_timeout=QUERY_TIMEOUT)
            line += f" replies_in_transcript={summary['assistant_message_count']}"
        except Exception as error:
            line += f" query_error=({describe_error(error)})"
        print(line, flush=True)


async def wait_for_reply_backlog(
    handle: WorkflowHandle, expected_replies: int, started_at: float, timeout_seconds: float
) -> None:
    """Waits until the transcript holds expected_replies replies or the workflow stops.

    This is a progress hint from a query; verify-history is the authoritative check.
    """
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            description = await handle.describe(rpc_timeout=READ_RPC_TIMEOUT)
        except Exception as error:
            last_error = describe_error(error)
            await asyncio.sleep(1)
            continue
        if description.status is None or description.status.name != "RUNNING":
            print(
                f"workflow stopped: status={description.status.name if description.status else None} "
                f"elapsed={time.monotonic() - started_at:.1f}s history_events={description.history_length}",
                flush=True,
            )
            return
        try:
            summary = await handle.query("get_summary", rpc_timeout=QUERY_TIMEOUT)
        except Exception as error:
            last_error = describe_error(error)
            await asyncio.sleep(1)
            continue
        if (
            summary["assistant_message_count"] >= expected_replies
            and summary["pending_reply_count"] == 0
        ):
            print(
                f"transcript holds {summary['assistant_message_count']} replies for "
                f"{expected_replies} customer messages (hint from a query) "
                f"elapsed={time.monotonic() - started_at:.1f}s history_events={description.history_length}",
                flush=True,
            )
            return
        await asyncio.sleep(1)
    print(
        f"backlog wait ended after {timeout_seconds:.0f}s without all replies in the transcript"
        + (f"; last read error: {last_error}" if last_error else ""),
        flush=True,
    )


async def flood_conversation(client: Client, args: argparse.Namespace) -> int:
    width = flood_index_width(args.count)
    shortest = len(flood_message_prefix(args.count, width)) + 1
    if args.message_bytes < shortest:
        print(
            f"error: --message-bytes {args.message_bytes} cannot hold the message ID; use at least {shortest}",
            file=sys.stderr,
        )
        return EXIT_ASSERTION_FAILED
    handle = client.get_workflow_handle(args.conversation_id)
    progress = FloodProgress(time.monotonic())
    observer = asyncio.create_task(
        observe_flood(handle, progress, args.progress_seconds)
    )
    for index in range(1, args.count + 1):
        if args.rate > 0:
            scheduled_at = progress.started_at + (index - 1) / args.rate
            delay = scheduled_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            else:
                progress.max_schedule_lag_seconds = max(
                    progress.max_schedule_lag_seconds, -delay
                )
        progress.rpc_attempts += 1
        try:
            await handle.signal(
                "post_message",
                build_flood_message(index, width, args.message_bytes),
                rpc_timeout=SIGNAL_RPC_TIMEOUT,
            )
        except Exception as error:
            if is_client_timeout(error):
                # The server may still have accepted it; history decides.
                progress.client_timeouts.append(
                    {"index": index, "error": describe_error(error)}
                )
                continue
            progress.rejection = {"index": index, "error": describe_error(error)}
            break
        progress.acknowledged.append(index)
    progress.sending_finished = True
    observer.cancel()
    await asyncio.gather(observer, return_exceptions=True)
    elapsed = time.monotonic() - progress.started_at

    if progress.rejection:
        print(
            f"REJECTED flood message {progress.rejection['index']}: {progress.rejection['error']}",
            flush=True,
        )
    for timeout in progress.client_timeouts[:5]:
        print(
            f"CLIENT TIMEOUT flood message {timeout['index']} (no acknowledgement; "
            f"the server may or may not have accepted it): {timeout['error']}",
            flush=True,
        )
    result = {
        "conversation": args.conversation_id,
        "target_count": args.count,
        "rate_per_second": args.rate,
        "message_bytes": args.message_bytes,
        "index_width": width,
        "rpc_attempts": progress.rpc_attempts,
        "acknowledged_count": len(progress.acknowledged),
        "acknowledged_ranges": compress_to_ranges(progress.acknowledged),
        "client_timeout_count": len(progress.client_timeouts),
        "client_timeouts": progress.client_timeouts,
        "rejection": progress.rejection,
        "elapsed_seconds": round(elapsed, 3),
        "achieved_rate_per_second": round(progress.rpc_attempts / elapsed, 2) if elapsed else 0,
        "max_schedule_lag_seconds": round(progress.max_schedule_lag_seconds, 3),
        "counts_exclude_the_first_message": True,
    }
    print(
        f"flood finished: target={args.count} rpc_attempts={progress.rpc_attempts} "
        f"acknowledged={len(progress.acknowledged)} client_timeouts={len(progress.client_timeouts)} "
        f"rejected={'1 (message ' + str(progress.rejection['index']) + ')' if progress.rejection else 0} "
        f"elapsed={elapsed:.1f}s achieved_rate={result['achieved_rate_per_second']}/s "
        f"max_schedule_lag={result['max_schedule_lag_seconds']}s "
        "(flood counts exclude the first message, which arrived with the start)",
        flush=True,
    )
    if args.result_file:
        with open(args.result_file, "w", encoding="utf-8") as result_output:
            json.dump(result, result_output, indent=2)
    if args.backlog_timeout > 0:
        # Replies for the first message plus every acknowledged flood message.
        await wait_for_reply_backlog(
            handle, len(progress.acknowledged) + 1, progress.started_at, args.backlog_timeout
        )
    print("final status:")
    print_json(await read_conversation_status(client, args.conversation_id))
    return EXIT_OK


# ---------------------------------------------------------------------------
# History of one run and of the whole run chain


@dataclass
class RunHistory:
    run_id: str
    events: list[HistoryEvent]
    workflow_status: str
    history_size_bytes: int


async def fetch_run_history(client: Client, conversation_id: str, run_id: str = "") -> RunHistory:
    handle = client.get_workflow_handle(conversation_id, run_id=run_id or None)
    description = await handle.describe(rpc_timeout=READ_RPC_TIMEOUT)
    history = await handle.fetch_history(rpc_timeout=READ_RPC_TIMEOUT)
    return RunHistory(
        description.run_id,
        list(history.events),
        description.status.name if description.status else "UNKNOWN",
        description.raw_description.workflow_execution_info.history_size_bytes,
    )


async def fetch_run_chain(client: Client, conversation_id: str) -> list[RunHistory]:
    """Returns every run, first to latest, linked by the run IDs in history.

    Walks back from the latest run through continued_execution_run_id, and
    checks that each earlier run ends with a ContinuedAsNew event that names
    the next run.
    """
    chain = [await fetch_run_history(client, conversation_id)]
    while True:
        started = chain[0].events[0].workflow_execution_started_event_attributes
        previous_run_id = started.continued_execution_run_id
        if not previous_run_id:
            break
        previous = await fetch_run_history(client, conversation_id, previous_run_id)
        last_event = previous.events[-1]
        if (
            not last_event.HasField("workflow_execution_continued_as_new_event_attributes")
            or last_event.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
            != chain[0].run_id
        ):
            raise RuntimeError(
                f"run {previous.run_id} does not continue as new into {chain[0].run_id}"
            )
        chain.insert(0, previous)
    return chain


def workflow_task_failures(events: list[HistoryEvent]) -> list[dict[str, Any]]:
    failures = []
    for event in events:
        if event.HasField("workflow_task_failed_event_attributes"):
            attributes = event.workflow_task_failed_event_attributes
            failures.append(
                {
                    "event_id": event.event_id,
                    "time": event.event_time.ToJsonString(),
                    "cause": WorkflowTaskFailedCause.Name(attributes.cause).removeprefix(
                        "WORKFLOW_TASK_FAILED_CAUSE_"
                    ),
                    "identity": attributes.identity,
                    "message": attributes.failure.message,
                }
            )
    return failures


def count_external_storage_references(events: list[HistoryEvent]) -> int:
    """Counts payloads in history that are External Storage references.

    Each reference payload names its message type in metadata exactly once.
    """
    marker = EXTERNAL_STORAGE_REFERENCE_MESSAGE_TYPE.encode()
    return sum(event.SerializeToString().count(marker) for event in events)


@dataclass
class ActivityRecord:
    activity_type: str
    scheduled_event_id: int
    scheduled_time: datetime
    input_payloads: Any
    outcome: str = "pending"
    outcome_time: datetime | None = None


def activities_by_scheduled_event(events: list[HistoryEvent]) -> dict[int, ActivityRecord]:
    """Matches every activity outcome to its ActivityTaskScheduled event."""
    activities: dict[int, ActivityRecord] = {}
    for event in events:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes = event.activity_task_scheduled_event_attributes
            activities[event.event_id] = ActivityRecord(
                attributes.activity_type.name,
                event.event_id,
                event_time(event),
                attributes.input,
            )
            continue
        for attribute_name, outcome in (
            ("activity_task_completed_event_attributes", "completed"),
            ("activity_task_failed_event_attributes", "failed"),
            ("activity_task_timed_out_event_attributes", "timed_out"),
            ("activity_task_canceled_event_attributes", "canceled"),
        ):
            if event.HasField(attribute_name):
                record = activities[getattr(event, attribute_name).scheduled_event_id]
                record.outcome = outcome
                record.outcome_time = event_time(event)
    return activities


async def inspect_run(
    client: Client,
    conversation_id: str,
    run_id: str = "",
    dump_path: str = "",
    with_query: bool = True,
) -> dict[str, Any]:
    """Reads replies and workflow task failures of one run from history.

    A query needs the worker to replay the workflow, which fails while the
    code is non-deterministic, so history is the reliable source here.
    """
    handle = client.get_workflow_handle(conversation_id, run_id=run_id or None)
    description = await handle.describe(rpc_timeout=READ_RPC_TIMEOUT)
    history = await handle.fetch_history(rpc_timeout=READ_RPC_TIMEOUT)
    if dump_path:
        with open(dump_path, "w", encoding="utf-8") as dump_file:
            dump_file.write(history.to_json())
    events = list(history.events)

    customer_messages = 0
    patch_markers: list[str] = []
    for event in events:
        if event.HasField("workflow_execution_started_event_attributes"):
            # Only the first run's start carries a new customer message.
            if not event.workflow_execution_started_event_attributes.continued_execution_run_id:
                customer_messages += 1
        elif event.HasField("workflow_execution_signaled_event_attributes"):
            if event.workflow_execution_signaled_event_attributes.signal_name == "post_message":
                customer_messages += 1
        elif event.HasField("marker_recorded_event_attributes"):
            marker = event.marker_recorded_event_attributes
            patch_markers.append(f"{marker.marker_name} (event {event.event_id})")
    activities = activities_by_scheduled_event(events)
    completed = [record for record in activities.values() if record.outcome == "completed"]
    completed.sort(key=lambda record: record.outcome_time or record.scheduled_time)
    activity_types_completed = [record.activity_type for record in completed]
    replies = activity_types_completed.count("generate_reply")
    task_failures = workflow_task_failures(events)
    report: dict[str, Any] = {
        "conversation": conversation_id,
        "run_id": description.run_id,
        "workflow_status": description.status.name if description.status else None,
        "history_event_count": len(events),
        "history_size_bytes": description.raw_description.workflow_execution_info.history_size_bytes,
        "customer_messages_in_history": customer_messages,
        "replies_completed_in_history": replies,
        "last_message_answered": replies >= customer_messages,
        "completed_activities_in_order": activity_types_completed
        if len(activity_types_completed) <= 50
        else f"{len(activity_types_completed)} activities, list omitted",
        "patch_markers": patch_markers,
        "workflow_task_failed_events": len(task_failures),
        "workflow_task_failure_causes": sorted({failure["cause"] for failure in task_failures}),
        "first_workflow_task_failure": task_failures[0] if task_failures else None,
        "last_workflow_task_failure": task_failures[-1] if task_failures else None,
        "pending_workflow_task": describe_pending_workflow_task(description),
    }
    if not with_query:
        return report
    try:
        report["query_get_summary"] = await handle.query(
            "get_summary", rpc_timeout=QUERY_TIMEOUT
        )
    except Exception as error:
        report["query_get_summary_error"] = describe_error(error)
    return report


def describe_pending_workflow_task(description: Any) -> dict[str, Any]:
    raw = description.raw_description
    if not raw.HasField("pending_workflow_task"):
        return {}
    task = raw.pending_workflow_task
    return {
        "attempt": task.attempt,
        "original_scheduled_time": task.original_scheduled_time.ToJsonString(),
        "scheduled_time": task.scheduled_time.ToJsonString(),
        "started_time": task.started_time.ToJsonString()
        if task.HasField("started_time")
        else "",
    }


async def print_inspection(client: Client, args: argparse.Namespace) -> int:
    print_json(await inspect_run(client, args.conversation_id, args.run_id, args.dump))
    return EXIT_OK


# ---------------------------------------------------------------------------
# Waiting with an explicit expectation


async def wait_for_replies(client: Client, args: argparse.Namespace) -> int:
    """Waits for --count completed replies, or for the expected stall.

    --expect reply: succeeds when history holds --count completed
    generate_reply activities; a timeout is an assertion failure.
    --expect stall: succeeds when history shows a workflow task failure with
    --failure-cause and fewer than --count replies; reaching --count, or no
    such failure within the timeout, is an assertion failure.
    """
    deadline = time.monotonic() + args.timeout
    while True:
        run = await fetch_run_history(client, args.conversation_id)
        activities = activities_by_scheduled_event(run.events)
        replies = sum(
            1
            for record in activities.values()
            if record.activity_type == "generate_reply" and record.outcome == "completed"
        )
        failures = [
            failure
            for failure in workflow_task_failures(run.events)
            if not args.failure_cause or failure["cause"] == args.failure_cause
        ]
        markers = [
            event.marker_recorded_event_attributes.marker_name
            for event in run.events
            if event.HasField("marker_recorded_event_attributes")
        ]
        if args.expect == "reply" and replies >= args.count:
            if args.expect_patch_marker and not markers:
                print(f"ASSERTION FAILED {args.conversation_id}: {replies} replies but no patch marker")
                return EXIT_ASSERTION_FAILED
            print(
                f"{args.conversation_id}: {replies} replies completed (expected {args.count})"
                + (f", patch markers {markers}" if markers else ""),
                flush=True,
            )
            return EXIT_OK
        if args.expect == "stall":
            if replies >= args.count:
                print(
                    f"ASSERTION FAILED {args.conversation_id}: expected a stall but "
                    f"{replies} replies completed",
                    flush=True,
                )
                return EXIT_ASSERTION_FAILED
            if failures and time.monotonic() >= deadline:
                print(
                    f"{args.conversation_id}: stalled as expected at {replies} of {args.count} replies "
                    f"after {args.timeout:.0f}s; {len(failures)} workflow task failure event(s) "
                    f"with cause {failures[-1]['cause']}: {failures[-1]['message'][:300]}",
                    flush=True,
                )
                return EXIT_OK
        if time.monotonic() >= deadline:
            print(
                f"ASSERTION FAILED {args.conversation_id}: only {replies} of {args.count} replies "
                f"after {args.timeout:.0f}s"
                + (
                    f"; no workflow task failure with cause {args.failure_cause}"
                    if args.expect == "stall"
                    else ""
                ),
                flush=True,
            )
            return EXIT_ASSERTION_FAILED
        await asyncio.sleep(0.5)


async def watch_task_retries(client: Client, args: argparse.Namespace) -> int:
    """Prints a line whenever the sampled pending workflow task attempt changes.

    Sampling shows the retry trend; it is not a complete count of attempts.
    """
    handle = client.get_workflow_handle(args.conversation_id)
    started_at = time.monotonic()
    last_attempt = None
    while time.monotonic() - started_at < args.seconds:
        description = await handle.describe(rpc_timeout=READ_RPC_TIMEOUT)
        pending = describe_pending_workflow_task(description)
        attempt = pending.get("attempt")
        if attempt != last_attempt:
            print(
                f"t={time.monotonic() - started_at:6.1f}s sampled_attempt={attempt} "
                f"scheduled={pending.get('scheduled_time')} "
                f"history_events={description.history_length}",
                flush=True,
            )
            last_attempt = attempt
        await asyncio.sleep(args.interval)
    return EXIT_OK


async def print_runs(client: Client, args: argparse.Namespace) -> int:
    chain = await fetch_run_chain(client, args.conversation_id)
    for number, run in enumerate(chain, 1):
        activities = activities_by_scheduled_event(run.events)
        replies = sum(
            1
            for record in activities.values()
            if record.activity_type == "generate_reply" and record.outcome == "completed"
        )
        signals = sum(
            1
            for event in run.events
            if event.HasField("workflow_execution_signaled_event_attributes")
            and event.workflow_execution_signaled_event_attributes.signal_name == "post_message"
        )
        failures = workflow_task_failures(run.events)
        causes = Counter(failure["cause"] for failure in failures)
        print(
            f"run {number:3d} {run.run_id} status={run.workflow_status} "
            f"events={len(run.events)} bytes={run.history_size_bytes} "
            f"post_message_signals={signals} replies_completed={replies} "
            f"external_storage_references={count_external_storage_references(run.events)} "
            f"workflow_task_failed={len(failures)} "
            f"causes={','.join(f'{cause}x{count}' for cause, count in sorted(causes.items())) or '-'}",
            flush=True,
        )
    print(f"runs: {len(chain)} (linked by run IDs in history)")
    return EXIT_OK


# ---------------------------------------------------------------------------
# Verification of a flood


def load_flood_result(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as result_input:
        return json.load(result_input)


def customer_key(text: str, first_message: str) -> str:
    match = FLOOD_INDEX_PATTERN.match(text)
    if match:
        return f"flood:{int(match.group(1))}"
    if text == first_message:
        return FIRST_MESSAGE_KEY
    return f"other:{text[:60]}"


def flood_index_of(key: str) -> int | None:
    return int(key.split(":", 1)[1]) if key.startswith("flood:") else None


async def verify_history(client: Client, args: argparse.Namespace) -> int:
    """Checks every accepted customer message and every reply across all runs.

    Runs are linked by run IDs. Activity outcomes are matched to their
    ActivityTaskScheduled event. Messages are keyed by their stable flood
    ID; the first message is counted once, from the first run's start.
    Needs no query, so it also works while workflow tasks keep failing.
    """
    flood = load_flood_result(args.flood_result)
    target = flood["target_count"]
    acknowledged = expand_ranges(flood["acknowledged_ranges"])
    chain = await fetch_run_chain(client, args.conversation_id)
    converter = client.data_converter

    async def decode_first(payloads: Any) -> Any:
        return (await converter.decode(list(payloads.payloads)))[0]

    first_input = await decode_first(
        chain[0].events[0].workflow_execution_started_event_attributes.input
    )
    first_message = first_input["first_message"]
    accepted: Counter[str] = Counter({FIRST_MESSAGE_KEY: 1})
    replies_completed: Counter[str] = Counter()
    replies_unfinished: Counter[str] = Counter()
    reply_outcomes: Counter[str] = Counter()
    last_reply: dict[str, Any] | None = None
    per_run = []
    for number, run in enumerate(chain, 1):
        signals = 0
        for event in run.events:
            if event.HasField("workflow_execution_signaled_event_attributes"):
                attributes = event.workflow_execution_signaled_event_attributes
                if attributes.signal_name == "post_message":
                    signals += 1
                    accepted[customer_key(await decode_first(attributes.input), first_message)] += 1
        run_replies = 0
        for record in activities_by_scheduled_event(run.events).values():
            if record.activity_type != "generate_reply":
                continue
            request = await decode_first(record.input_payloads)
            key = customer_key(request["customer_text"], first_message)
            reply_outcomes[record.outcome] += 1
            if record.outcome == "completed":
                replies_completed[key] += 1
                run_replies += 1
                if last_reply is None or record.outcome_time >= last_reply["time"]:
                    last_reply = {
                        "time": record.outcome_time,
                        "run": number,
                        "message": key,
                        "customer_sequence": request["customer_sequence"],
                    }
            else:
                replies_unfinished[key] += 1
        failures = workflow_task_failures(run.events)
        per_run.append(
            {
                "run": number,
                "run_id": run.run_id,
                "status": run.workflow_status,
                "events": len(run.events),
                "post_message_signals": signals,
                "replies_completed": run_replies,
                "workflow_task_failures": dict(Counter(f["cause"] for f in failures)),
            }
        )

    accepted_flood = {flood_index_of(key) for key in accepted if flood_index_of(key) is not None}
    in_range = set(range(1, target + 1))
    missing_signals = sorted(acknowledged - accepted_flood)
    out_of_range = sorted(index for index in accepted_flood if index not in in_range)
    duplicated_messages = sorted(key for key, count in accepted.items() if count > 1)
    duplicated_replies = sorted(key for key, count in replies_completed.items() if count > 1)
    orphan_replies = sorted(key for key in replies_completed if key not in accepted)
    unanswered = sorted(
        (key for key in accepted if key not in replies_completed),
        key=lambda key: (flood_index_of(key) or 0, key),
    )
    in_flight = sorted(key for key in unanswered if key in replies_unfinished)
    answered_flood = sorted(
        index for key in replies_completed if (index := flood_index_of(key)) is not None
    )
    timed_out_but_accepted = sorted(
        timeout["index"] for timeout in flood["client_timeouts"] if timeout["index"] in accepted_flood
    )
    latest = chain[-1]
    latest_failures = workflow_task_failures(latest.events)
    report: dict[str, Any] = {
        "conversation": args.conversation_id,
        "runs": per_run,
        "flood_target_count": target,
        "flood_rpc_attempts": flood["rpc_attempts"],
        "flood_acknowledged_by_client": len(acknowledged),
        "flood_client_timeouts": flood["client_timeout_count"],
        "flood_client_timeouts_found_in_history": timed_out_but_accepted,
        "customer_messages_accepted_in_history": sum(accepted.values()),
        "of_which_first_message": accepted[FIRST_MESSAGE_KEY],
        "of_which_flood_messages": sum(
            count for key, count in accepted.items() if key.startswith("flood:")
        ),
        "of_which_other_messages": sum(
            count for key, count in accepted.items() if key.startswith("other:")
        ),
        "generate_reply_outcomes": dict(reply_outcomes),
        "customer_messages_with_completed_reply": len(replies_completed),
        "unanswered_count": len(unanswered),
        "unanswered_first": unanswered[:10],
        "unanswered_with_reply_in_flight": len(in_flight),
        "answered_flood_ids_form_a_prefix": answered_flood == list(range(1, len(answered_flood) + 1)),
        "last_completed_reply": {
            **last_reply,
            "time": last_reply["time"].isoformat(),
        }
        if last_reply
        else None,
        "anomalies": {
            "acknowledged_but_missing_from_history": missing_signals[:20],
            "acknowledged_but_missing_from_history_count": len(missing_signals),
            "duplicate_customer_messages": duplicated_messages[:20],
            "duplicate_replies": duplicated_replies[:20],
            "orphan_replies": orphan_replies[:20],
            "out_of_range_flood_ids": out_of_range[:20],
        },
        "latest_run_status": latest.workflow_status,
        "latest_run_workflow_task_failures": len(latest_failures),
        "latest_run_first_failure": latest_failures[0] if latest_failures else None,
    }
    print_json(report)

    problems = [
        name
        for name, values in (
            ("acknowledged messages missing from history", missing_signals),
            ("duplicate customer messages", duplicated_messages),
            ("duplicate replies", duplicated_replies),
            ("orphan replies", orphan_replies),
            ("out-of-range flood IDs", out_of_range),
        )
        if values
    ]
    if args.expect == "all-answered":
        if unanswered:
            problems.append(f"{len(unanswered)} accepted messages without a completed reply")
    else:
        if not unanswered:
            problems.append("expected a stall but every accepted message has a reply")
        if latest.workflow_status != "RUNNING":
            problems.append(f"expected the stalled run to be RUNNING, not {latest.workflow_status}")
        if not any(failure["cause"] == args.stall_cause for failure in latest_failures):
            problems.append(f"no workflow task failure with cause {args.stall_cause} in the latest run")
    if problems:
        print("ASSERTION FAILED: " + "; ".join(problems), flush=True)
        return EXIT_ASSERTION_FAILED
    print(f"verify-history: as expected ({args.expect})", flush=True)
    return EXIT_OK


async def verify_transcript(client: Client, args: argparse.Namespace) -> int:
    """Checks what the workflow wrote to its transcript, read by query.

    Reports separately from verify-history, which sees completed activities:
    a reply is in the transcript only after the workflow task that handled
    the completion succeeded.
    """
    flood = load_flood_result(args.flood_result)
    acknowledged = expand_ranges(flood["acknowledged_ranges"])
    handle = client.get_workflow_handle(args.conversation_id)
    messages: list[dict[str, Any]] = []
    page_token = "current"
    try:
        while page_token:
            page = await handle.query(
                "get_transcript_page", page_token, rpc_timeout=QUERY_TIMEOUT
            )
            messages.extend(page["messages"])
            page_token = page["next_page_token"]
    except Exception as error:
        if args.allow_query_failure:
            print(
                f"transcript query unavailable, as expected while workflow tasks fail: {describe_error(error)}",
                flush=True,
            )
            return EXIT_OK
        raise
    messages.sort(key=lambda message: message["sequence"])
    sequences = [message["sequence"] for message in messages]
    customer_by_sequence: dict[int, str] = {}
    flood_ids: Counter[int] = Counter()
    replies_by_sequence: Counter[int] = Counter()
    malformed_replies = []
    for message in messages:
        if message["role"] == "customer":
            customer_by_sequence[message["sequence"]] = message["text"]
            match = FLOOD_INDEX_PATTERN.match(message["text"])
            if match:
                flood_ids[int(match.group(1))] += 1
        else:
            match = REPLY_SEQUENCE_PATTERN.match(message["text"])
            if match:
                replies_by_sequence[int(match.group(1))] += 1
            else:
                malformed_replies.append(message["sequence"])
    missing_flood = sorted(acknowledged - set(flood_ids))
    duplicated_flood = sorted(index for index, count in flood_ids.items() if count > 1)
    out_of_range = sorted(
        index for index in flood_ids if not 1 <= index <= flood["target_count"]
    )
    orphan_replies = sorted(s for s in replies_by_sequence if s not in customer_by_sequence)
    duplicated_replies = sorted(s for s, count in replies_by_sequence.items() if count > 1)
    unanswered = sorted(s for s in customer_by_sequence if s not in replies_by_sequence)
    report = {
        "conversation": args.conversation_id,
        "transcript_messages": len(messages),
        "sequences_contiguous": sequences == list(range(1, len(messages) + 1)),
        "customer_messages_in_transcript": len(customer_by_sequence),
        "replies_in_transcript": sum(replies_by_sequence.values()),
        "unanswered_in_transcript": len(unanswered),
        "anomalies": {
            "acknowledged_flood_missing": missing_flood[:20],
            "acknowledged_flood_missing_count": len(missing_flood),
            "duplicate_flood_messages": duplicated_flood[:20],
            "out_of_range_flood_ids": out_of_range[:20],
            "orphan_replies": orphan_replies[:20],
            "duplicate_replies": duplicated_replies[:20],
            "malformed_replies": malformed_replies[:20],
        },
    }
    print_json(report)
    problems = [name for name, values in report["anomalies"].items() if values]
    if not report["sequences_contiguous"]:
        problems.append("sequences not contiguous")
    if unanswered:
        problems.append(f"{len(unanswered)} customer messages without a reply in the transcript")
    if problems:
        print("ASSERTION FAILED: " + "; ".join(problems), flush=True)
        return EXIT_ASSERTION_FAILED
    print("verify (transcript): as expected", flush=True)
    return EXIT_OK


# ---------------------------------------------------------------------------
# History summary and per-message breakdown


async def summarize_history(client: Client, args: argparse.Namespace) -> int:
    """Summarizes one run: event counts by type, first and last event, and
    the average events per customer message in the steady middle half."""
    run = await fetch_run_history(client, args.conversation_id, args.run_id)
    events = run.events
    counts = Counter(event_type_name(event) for event in events)
    activities = activities_by_scheduled_event(events)
    completed_by_type = Counter(
        record.activity_type for record in activities.values() if record.outcome == "completed"
    )
    signal_event_ids = [
        event.event_id
        for event in events
        if event.HasField("workflow_execution_signaled_event_attributes")
        and event.workflow_execution_signaled_event_attributes.signal_name == "post_message"
    ]
    started = events[0].workflow_execution_started_event_attributes
    summary: dict[str, Any] = {
        "conversation": args.conversation_id,
        "run_id": run.run_id,
        "workflow_status": run.workflow_status,
        "history_event_count": len(events),
        "history_size_bytes": run.history_size_bytes,
        "first_event": describe_event_briefly(events[0]),
        "last_event": describe_event_briefly(events[-1]),
        "start_carries_first_message": not started.continued_execution_run_id,
        "post_message_signals": len(signal_event_ids),
        "completed_activities_by_type": dict(completed_by_type),
        "events_by_type": dict(sorted(counts.items(), key=lambda item: -item[1])),
        "workflow_task_failures": dict(Counter(f["cause"] for f in workflow_task_failures(events))),
    }
    if len(signal_event_ids) >= 8:
        first = signal_event_ids[len(signal_event_ids) // 4]
        last = signal_event_ids[3 * len(signal_event_ids) // 4]
        window_messages = signal_event_ids.index(last) - signal_event_ids.index(first)
        window_counts = Counter(
            event_type_name(event) for event in events if first <= event.event_id < last
        )
        summary["steady_window"] = {
            "from_event_id": first,
            "to_event_id": last,
            "customer_messages": window_messages,
            "events": sum(window_counts.values()),
            "events_per_message": round(sum(window_counts.values()) / window_messages, 3),
            "events_per_message_by_type": {
                name: round(count / window_messages, 3)
                for name, count in sorted(window_counts.items(), key=lambda item: -item[1])
            },
        }
    print_json(summary)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as summary_output:
            json.dump(summary, summary_output, indent=2)
    return EXIT_OK


# ---------------------------------------------------------------------------
# T1 timeline from history


def read_outbox_records(conversation_id: str) -> list[dict[str, Any]]:
    if not OUTBOX_FILE.exists():
        return []
    records = []
    for line in OUTBOX_FILE.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record["conversation"] == conversation_id:
            records.append(record)
    return records


async def print_timeline(client: Client, args: argparse.Namespace) -> int:
    """Prints a conversation timeline from history event times and asserts it.

    naive: the nudge is scheduled one idle window after the FIRST reply.
    correct: the nudge is scheduled one idle window after the LAST reply.
    Both: the close summary is scheduled one close window after the nudge.
    """
    run = await fetch_run_history(client, args.conversation_id)
    events = run.events
    started_at = event_time(events[0])

    def offset(moment: datetime) -> float:
        return (moment - started_at).total_seconds()

    lines: list[tuple[float, str]] = [(0.0, "WorkflowExecutionStarted (first customer message)")]
    timer_durations: dict[int, float] = {}
    for event in events:
        if event.HasField("workflow_execution_signaled_event_attributes"):
            lines.append((offset(event_time(event)), "WorkflowExecutionSignaled post_message"))
        elif event.HasField("timer_started_event_attributes"):
            seconds = event.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta().total_seconds()
            timer_durations[event.event_id] = seconds
            lines.append((offset(event_time(event)), f"TimerStarted {seconds:.1f}s (event {event.event_id})"))
        elif event.HasField("timer_fired_event_attributes"):
            started_id = event.timer_fired_event_attributes.started_event_id
            lines.append((offset(event_time(event)), f"TimerFired (started by event {started_id})"))
        elif event.HasField("timer_canceled_event_attributes"):
            started_id = event.timer_canceled_event_attributes.started_event_id
            lines.append((offset(event_time(event)), f"TimerCanceled (started by event {started_id})"))
        elif event.HasField("workflow_execution_completed_event_attributes"):
            lines.append((offset(event_time(event)), "WorkflowExecutionCompleted"))
    activities = sorted(
        activities_by_scheduled_event(events).values(), key=lambda record: record.scheduled_event_id
    )
    for record in activities:
        if record.outcome_time is not None:
            lines.append(
                (offset(record.outcome_time), f"ActivityTask {record.outcome} {record.activity_type}")
            )
    for record in read_outbox_records(args.conversation_id):
        written_at = datetime.fromisoformat(record["time"]).replace(tzinfo=None)
        lines.append(
            (offset(written_at), f"  outbox line written: {record['kind']} (messages={record['messages']})")
        )
    print(f"timeline of {args.conversation_id} from history event times (t=0 at WorkflowExecutionStarted)")
    for moment, label in sorted(lines, key=lambda line: line[0]):
        print(f"  t={moment:6.1f}s  {label}", flush=True)

    def completed(activity_type: str) -> list[ActivityRecord]:
        return [r for r in activities if r.activity_type == activity_type and r.outcome == "completed"]

    replies = completed("generate_reply")
    nudges = completed("send_nudge_email")
    closes = completed("post_close_summary")
    problems = []
    if run.workflow_status != "COMPLETED":
        problems.append(f"workflow status {run.workflow_status}, expected COMPLETED")
    for name, records, expected in (("replies", replies, 2), ("nudges", nudges, 1), ("close summaries", closes, 1)):
        if len(records) != expected:
            problems.append(f"{len(records)} {name}, expected {expected}")
    if not problems:
        anchor = replies[0] if args.expect == "naive" else replies[-1]
        nudge_delay = (nudges[0].scheduled_time - anchor.outcome_time).total_seconds()
        close_delay = (closes[0].scheduled_time - nudges[0].outcome_time).total_seconds()
        anchor_name = "first" if args.expect == "naive" else "last"
        print(
            f"  nudge scheduled {nudge_delay:.1f}s after the {anchor_name} reply completed "
            f"(expected {args.idle_seconds}s); close summary scheduled {close_delay:.1f}s after "
            f"the nudge completed (expected {args.close_after_seconds}s)",
            flush=True,
        )
        if abs(nudge_delay - args.idle_seconds) > args.tolerance:
            problems.append(f"nudge {nudge_delay:.1f}s after the {anchor_name} reply")
        if abs(close_delay - args.close_after_seconds) > args.tolerance:
            problems.append(f"close summary {close_delay:.1f}s after the nudge")
        if args.expect == "naive":
            early_by = (replies[-1].outcome_time - anchor.outcome_time).total_seconds()
            print(
                f"  the follow-up reply completed {early_by:.1f}s after the first; the nudge "
                "did not move with it",
                flush=True,
            )
    if problems:
        print("ASSERTION FAILED: " + "; ".join(problems), flush=True)
        return EXIT_ASSERTION_FAILED
    print(f"  timeline as expected for the {args.expect} variant", flush=True)
    return EXIT_OK


# ---------------------------------------------------------------------------


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Temporal conversation demo driver")
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start", help="start a conversation workflow")
    start.add_argument("conversation_id")
    start.add_argument(
        "--first-message",
        required=True,
        type=non_empty_text,
        help="the customer message that opens the conversation",
    )
    start.add_argument("--variant", choices=sorted(WORKFLOW_TYPE_BY_VARIANT), default="correct")
    start.add_argument("--idle-seconds", type=positive_int, default=DEFAULT_IDLE_SECONDS)
    start.add_argument(
        "--close-after-seconds", type=positive_int, default=DEFAULT_CLOSE_AFTER_SECONDS
    )
    start.set_defaults(handler=start_conversation)

    post = commands.add_parser("post", help="post one customer message")
    post.add_argument("conversation_id")
    post.add_argument("text", type=non_empty_text)
    post.set_defaults(handler=post_message)

    assign = commands.add_parser("assign", help="assign a support agent")
    assign.add_argument("conversation_id")
    assign.add_argument("agent_name")
    assign.set_defaults(handler=assign_agent)

    page = commands.add_parser("page", help="print one transcript page as JSON")
    page.add_argument("conversation_id")
    page.add_argument("token", nargs="?", default="current")
    page.set_defaults(handler=print_transcript_page)

    status = commands.add_parser("status", help="print workflow status as JSON")
    status.add_argument("conversation_id")
    status.set_defaults(handler=print_status)

    flood = commands.add_parser("flood", help="post many messages until the engine stops them")
    flood.add_argument("conversation_id")
    flood.add_argument("--count", type=positive_int, required=True)
    flood.add_argument(
        "--rate", type=non_negative_float, required=True, help="messages per second; 0 means as fast as possible"
    )
    flood.add_argument(
        "--message-bytes", type=positive_int, default=FLOOD_MESSAGE_BYTES, help="size of each flood message"
    )
    flood.add_argument(
        "--backlog-timeout",
        type=non_negative_float,
        default=FLOOD_BACKLOG_TIMEOUT_SECONDS,
        help="seconds to wait for replies after the last message; 0 skips the wait",
    )
    flood.add_argument(
        "--progress-seconds", type=positive_float, default=FLOOD_PROGRESS_SECONDS
    )
    flood.add_argument("--result-file", default="", help="write the flood counts as JSON here")
    flood.set_defaults(handler=flood_conversation)

    inspect = commands.add_parser(
        "inspect", help="read a run's history: replies, activity types, workflow task failures"
    )
    inspect.add_argument("conversation_id")
    inspect.add_argument("--run-id", default="")
    inspect.add_argument("--dump", default="", help="write the history JSON to this file")
    inspect.set_defaults(handler=print_inspection)

    replies = commands.add_parser(
        "wait-replies", help="wait for N completed replies, or for an expected stall"
    )
    replies.add_argument("conversation_id")
    replies.add_argument("--count", type=positive_int, required=True)
    replies.add_argument("--timeout", type=positive_float, default=30)
    replies.add_argument("--expect", choices=("reply", "stall"), default="reply")
    replies.add_argument(
        "--failure-cause", default="", help="for --expect stall, e.g. NON_DETERMINISTIC_ERROR"
    )
    replies.add_argument("--expect-patch-marker", action="store_true")
    replies.set_defaults(handler=wait_for_replies)

    retries = commands.add_parser(
        "watch-task-retries", help="sample the pending workflow task attempt over time"
    )
    retries.add_argument("conversation_id")
    retries.add_argument("--seconds", type=positive_float, required=True)
    retries.add_argument("--interval", type=positive_float, default=2)
    retries.set_defaults(handler=watch_task_retries)

    runs = commands.add_parser("runs", help="list every run of a conversation, linked by run IDs")
    runs.add_argument("conversation_id")
    runs.set_defaults(handler=print_runs)

    verify = commands.add_parser(
        "verify", help="check the transcript, read by query, against a flood result"
    )
    verify.add_argument("conversation_id")
    verify.add_argument("--flood-result", required=True)
    verify.add_argument("--allow-query-failure", action="store_true")
    verify.set_defaults(handler=verify_transcript)

    verify_from_history = commands.add_parser(
        "verify-history",
        help="check every accepted message and reply across all runs, from history",
    )
    verify_from_history.add_argument("conversation_id")
    verify_from_history.add_argument("--flood-result", required=True)
    verify_from_history.add_argument("--expect", choices=("all-answered", "stall"), required=True)
    verify_from_history.add_argument("--stall-cause", default="PAYLOADS_TOO_LARGE")
    verify_from_history.set_defaults(handler=verify_history)

    summary = commands.add_parser(
        "history-summary", help="event counts by type, first and last event, per-message breakdown"
    )
    summary.add_argument("conversation_id")
    summary.add_argument("--run-id", default="")
    summary.add_argument("--output", default="", help="also write the summary JSON here")
    summary.set_defaults(handler=summarize_history)

    timeline = commands.add_parser(
        "timeline", help="print and assert a T1 conversation timeline from history"
    )
    timeline.add_argument("conversation_id")
    timeline.add_argument("--expect", choices=("naive", "correct"), required=True)
    timeline.add_argument("--idle-seconds", type=positive_int, required=True)
    timeline.add_argument("--close-after-seconds", type=positive_int, required=True)
    timeline.add_argument("--tolerance", type=positive_float, default=3)
    timeline.set_defaults(handler=print_timeline)
    return parser


async def main() -> int:
    args = build_argument_parser().parse_args()
    try:
        client = await connect_client()
        return await args.handler(client, args)
    except Exception as error:
        # For post and assign this is the command's own failure, such as a closed workflow.
        print(f"error ({args.command}): {describe_error(error)}", file=sys.stderr)
        return EXIT_OBSERVATION_FAILED


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
