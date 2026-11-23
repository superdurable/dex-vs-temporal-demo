"""Command-line driver for the Temporal conversation demo.

Usage (from this directory):
  uv run drive.py start <conversation-id> --first-message <text> [--variant naive|correct] [--idle-seconds N] [--close-after-seconds N]
  uv run drive.py post <conversation-id> <text>
  uv run drive.py assign <conversation-id> <agent-name>
  uv run drive.py page <conversation-id> [token]
  uv run drive.py status <conversation-id>
  uv run drive.py flood <conversation-id> --count N [--rate R] [--message-bytes B] [--backlog-timeout S]
  uv run drive.py inspect <conversation-id> [--run-id R] [--dump history.json]
  uv run drive.py wait-replies <conversation-id> --count N [--timeout S]
  uv run drive.py watch-task-retries <conversation-id> --seconds N [--interval S]
  uv run drive.py runs <conversation-id>
  uv run drive.py verify <conversation-id> --flood-count N

The conversation ID is the workflow ID. Set TEMPORAL_ADDRESS to reach a
server other than localhost:7233.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from typing import Any

from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryReverseRequest,
)
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError

from local_file_storage import build_data_converter

from conversation_shared import (
    CONTINUE_AS_NEW_WORKFLOW_TYPE,
    CORRECT_WORKFLOW_TYPE,
    DEFAULT_CLOSE_AFTER_SECONDS,
    DEFAULT_IDLE_SECONDS,
    NAIVE_WORKFLOW_TYPE,
    TASK_QUEUE,
    ConversationInput,
)

WORKFLOW_TYPE_BY_VARIANT = {
    "naive": NAIVE_WORKFLOW_TYPE,
    "correct": CORRECT_WORKFLOW_TYPE,
    "continue-as-new": CONTINUE_AS_NEW_WORKFLOW_TYPE,
}
FLOOD_MESSAGE_BYTES = 200
FLOOD_PROGRESS_INTERVAL = 1000
FLOOD_BACKLOG_TIMEOUT_SECONDS = 900


async def connect_client() -> Client:
    # A fixed identity keeps the host name out of recorded histories.
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        identity=os.environ.get("DEMO_CLIENT_IDENTITY", "conversation-demo-driver"),
        data_converter=build_data_converter(),
    )


def print_json(value: Any) -> None:
    print(json.dumps(value, indent=2))


async def start_conversation(client: Client, args: argparse.Namespace) -> None:
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


async def post_message(client: Client, args: argparse.Namespace) -> None:
    await client.get_workflow_handle(args.conversation_id).signal(
        "post_message", args.text
    )
    print_json({"conversation": args.conversation_id, "posted": args.text})


async def assign_agent(client: Client, args: argparse.Namespace) -> None:
    previous_agent = await client.get_workflow_handle(
        args.conversation_id
    ).execute_update("assign_agent", args.agent_name)
    print_json(
        {
            "conversation": args.conversation_id,
            "agent": args.agent_name,
            "previous_agent": previous_agent,
        }
    )


async def print_transcript_page(client: Client, args: argparse.Namespace) -> None:
    page = await client.get_workflow_handle(args.conversation_id).query(
        "get_transcript_page", args.token
    )
    print_json(page)


async def read_last_history_event(client: Client, conversation_id: str) -> dict[str, Any]:
    response = await client.workflow_service.get_workflow_execution_history_reverse(
        GetWorkflowExecutionHistoryReverseRequest(
            namespace=client.namespace,
            execution=WorkflowExecution(workflow_id=conversation_id),
            maximum_page_size=1,
        )
    )
    if not response.history.events:
        return {}
    event = response.history.events[0]
    last_event: dict[str, Any] = {
        "event_id": event.event_id,
        "event_type": EventType.Name(event.event_type),
    }
    if event.HasField("workflow_execution_terminated_event_attributes"):
        attributes = event.workflow_execution_terminated_event_attributes
        last_event["reason"] = attributes.reason
        last_event["identity"] = attributes.identity
    return last_event


async def read_conversation_status(
    client: Client, conversation_id: str
) -> dict[str, Any]:
    handle: WorkflowHandle = client.get_workflow_handle(conversation_id)
    description = await handle.describe()
    status: dict[str, Any] = {
        "conversation": conversation_id,
        "workflow_type": description.workflow_type,
        "workflow_status": description.status.name if description.status else None,
        "history_event_count": description.history_length,
        "history_size_bytes": description.raw_description.workflow_execution_info.history_size_bytes,
    }
    try:
        summary = await handle.query("get_summary")
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
        status["message_count"] = None
        status["query_error"] = f"{type(error).__name__}: {error}"
    status["last_history_event"] = await read_last_history_event(
        client, conversation_id
    )
    return status


async def print_status(client: Client, args: argparse.Namespace) -> None:
    print_json(await read_conversation_status(client, args.conversation_id))


def build_flood_message(index: int, size: int = FLOOD_MESSAGE_BYTES) -> str:
    prefix = f"flood message {index:06d}: "
    filler = "the customer repeats the same question about the order status. "
    return (prefix + filler * (size // len(filler) + 1))[:size]


def describe_error(error: BaseException) -> str:
    if isinstance(error, RPCError):
        return f"RPCError status={error.status.name} message={error.message}"
    return f"{type(error).__name__}: {error}"


async def wait_for_reply_backlog(
    handle: WorkflowHandle, started_at: float, timeout_seconds: float
) -> None:
    """Waits until every accepted message has a reply or the workflow stops."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        description = await handle.describe()
        if description.status is None or description.status.name != "RUNNING":
            print(
                f"workflow stopped while replying: status={description.status.name if description.status else None} "
                f"elapsed={time.monotonic() - started_at:.1f}s history_events={description.history_length}",
                flush=True,
            )
            return
        try:
            summary = await handle.query("get_summary")
        except Exception:
            # A run that has just continued as new may not answer queries yet.
            await asyncio.sleep(1)
            continue
        if summary["pending_reply_count"] == 0:
            print(
                f"reply backlog drained: replies={summary['assistant_message_count']} "
                f"elapsed={time.monotonic() - started_at:.1f}s history_events={description.history_length}",
                flush=True,
            )
            return
        await asyncio.sleep(1)
    print("reply backlog not drained within the timeout", flush=True)


async def flood_conversation(client: Client, args: argparse.Namespace) -> None:
    handle = client.get_workflow_handle(args.conversation_id)
    started_at = time.monotonic()
    accepted = 0
    stop_error = ""
    for index in range(1, args.count + 1):
        if args.rate > 0:
            delay = started_at + (index - 1) / args.rate - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
        try:
            await handle.signal(
                "post_message", build_flood_message(index, args.message_bytes)
            )
        except Exception as error:
            stop_error = describe_error(error)
            break
        accepted += 1
        if accepted % FLOOD_PROGRESS_INTERVAL == 0:
            description = await handle.describe()
            try:
                replies = (await handle.query("get_summary"))["assistant_message_count"]
            except Exception:
                replies = "unknown"
            print(
                f"progress customer_messages_accepted={accepted} replies={replies} "
                f"elapsed={time.monotonic() - started_at:.1f}s "
                f"history_events={description.history_length} "
                f"workflow_status={description.status.name if description.status else None}",
                flush=True,
            )

    elapsed = time.monotonic() - started_at
    if stop_error:
        print(f"REJECTED customer_message={accepted + 1} error: {stop_error}", flush=True)
    print(
        f"flood finished customer_messages_accepted={accepted} attempted={args.count} "
        f"elapsed={elapsed:.1f}s rate={accepted / elapsed if elapsed else 0:.1f}/s",
        flush=True,
    )
    # Accepted messages may still be waiting for replies; keep watching them.
    await wait_for_reply_backlog(handle, started_at, args.backlog_timeout)
    # Let the worker finish its last workflow task before the final read.
    await asyncio.sleep(2)
    print("final status:")
    print_json(await read_conversation_status(client, args.conversation_id))


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


async def inspect_run(
    client: Client,
    conversation_id: str,
    run_id: str = "",
    dump_path: str = "",
    with_query: bool = True,
) -> dict[str, Any]:
    """Reads replies and workflow task failures from history, not from a query.

    A query needs the worker to replay the workflow, which fails while the
    code is non-deterministic, so history is the reliable source here.
    """
    handle = client.get_workflow_handle(conversation_id, run_id=run_id or None)
    description = await handle.describe()
    history = await handle.fetch_history()
    if dump_path:
        with open(dump_path, "w", encoding="utf-8") as dump_file:
            dump_file.write(history.to_json())

    activity_type_by_scheduled_event: dict[int, str] = {}
    activity_types_completed: list[str] = []
    customer_messages = 0
    patch_markers: list[str] = []
    task_failures: list[dict[str, Any]] = []
    for event in history.events:
        if event.HasField("workflow_execution_started_event_attributes"):
            # The first customer message arrives with the start input.
            customer_messages += 1
        elif event.HasField("workflow_execution_signaled_event_attributes"):
            if event.workflow_execution_signaled_event_attributes.signal_name == "post_message":
                customer_messages += 1
        elif event.HasField("activity_task_scheduled_event_attributes"):
            activity_type_by_scheduled_event[event.event_id] = (
                event.activity_task_scheduled_event_attributes.activity_type.name
            )
        elif event.HasField("activity_task_completed_event_attributes"):
            activity_types_completed.append(
                activity_type_by_scheduled_event[
                    event.activity_task_completed_event_attributes.scheduled_event_id
                ]
            )
        elif event.HasField("marker_recorded_event_attributes"):
            marker = event.marker_recorded_event_attributes
            patch_markers.append(f"{marker.marker_name} (event {event.event_id})")
        elif event.HasField("workflow_task_failed_event_attributes"):
            attributes = event.workflow_task_failed_event_attributes
            task_failures.append(
                {
                    "event_id": event.event_id,
                    "time": event.event_time.ToJsonString(),
                    "cause": WorkflowTaskFailedCause.Name(attributes.cause),
                    "identity": attributes.identity,
                    "message": attributes.failure.message,
                }
            )

    replies = activity_types_completed.count("generate_reply")
    report: dict[str, Any] = {
        "conversation": conversation_id,
        "run_id": description.run_id,
        "workflow_status": description.status.name if description.status else None,
        "history_event_count": description.history_length,
        "history_size_bytes": description.raw_description.workflow_execution_info.history_size_bytes,
        "customer_messages_in_history": customer_messages,
        "replies_in_history": replies,
        "last_message_answered": replies >= customer_messages,
        "completed_activities_in_order": activity_types_completed
        if len(activity_types_completed) <= 50
        else f"{len(activity_types_completed)} activities, list omitted",
        "patch_markers": patch_markers,
        "workflow_task_failed_events": len(task_failures),
        "workflow_task_failure_causes": sorted(
            {failure["cause"] for failure in task_failures}
        ),
        "last_workflow_task_failure": task_failures[-1] if task_failures else None,
        "pending_workflow_task": describe_pending_workflow_task(description),
    }
    if not with_query:
        return report
    try:
        report["query_get_summary"] = await asyncio.wait_for(
            handle.query("get_summary"), timeout=15
        )
    except Exception as error:
        report["query_get_summary_error"] = describe_error(error)
    return report


async def wait_for_replies(client: Client, args: argparse.Namespace) -> None:
    """Waits until history holds at least --count replies; prints the outcome."""
    handle = client.get_workflow_handle(args.conversation_id)
    deadline = time.monotonic() + args.timeout
    replies = 0
    while time.monotonic() < deadline:
        history = await handle.fetch_history()
        replies = sum(
            1
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
            and event.activity_task_scheduled_event_attributes.activity_type.name
            == "generate_reply"
            and any(
                later.HasField("activity_task_completed_event_attributes")
                and later.activity_task_completed_event_attributes.scheduled_event_id
                == event.event_id
                for later in history.events
            )
        )
        if replies >= args.count:
            print(f"{args.conversation_id}: {replies} replies", flush=True)
            return
        await asyncio.sleep(0.5)
    print(
        f"{args.conversation_id}: only {replies} of {args.count} replies after {args.timeout:.0f}s",
        flush=True,
    )


async def print_inspection(client: Client, args: argparse.Namespace) -> None:
    print_json(await inspect_run(client, args.conversation_id, args.run_id, args.dump))


async def watch_task_retries(client: Client, args: argparse.Namespace) -> None:
    """Prints a line whenever the pending workflow task attempt changes."""
    handle = client.get_workflow_handle(args.conversation_id)
    started_at = time.monotonic()
    last_attempt = None
    while time.monotonic() - started_at < args.seconds:
        description = await handle.describe()
        pending = describe_pending_workflow_task(description)
        attempt = pending.get("attempt")
        if attempt != last_attempt:
            print(
                f"t={time.monotonic() - started_at:6.1f}s attempt={attempt} "
                f"scheduled={pending.get('scheduled_time')} "
                f"started={pending.get('started_time')} "
                f"history_events={description.history_length}",
                flush=True,
            )
            last_attempt = attempt
        await asyncio.sleep(args.interval)


async def print_runs(client: Client, args: argparse.Namespace) -> None:
    runs = []
    async for execution in client.list_workflows(
        f"WorkflowId = '{args.conversation_id}'"
    ):
        runs.append(execution)
    runs.sort(key=lambda execution: execution.start_time)
    for number, execution in enumerate(runs, 1):
        report = await inspect_run(
            client, args.conversation_id, execution.run_id, with_query=False
        )
        print(
            f"run {number:3d} {execution.run_id} status={execution.status.name if execution.status else None} "
            f"events={report['history_event_count']} bytes={report['history_size_bytes']} "
            f"customer_signals_or_start={report['customer_messages_in_history']} "
            f"replies={report['replies_in_history']} "
            f"workflow_task_failed={report['workflow_task_failed_events']} "
            f"causes={','.join(report['workflow_task_failure_causes']) or '-'}",
            flush=True,
        )
    print(f"runs: {len(runs)}")


FLOOD_INDEX_PATTERN = re.compile(r"^flood message (\d{6}): ")
REPLY_SEQUENCE_PATTERN = re.compile(r"^Reply to message (\d+):")


async def verify_transcript(client: Client, args: argparse.Namespace) -> None:
    """Checks every flood index and every reply by sequence, not only counts."""
    handle = client.get_workflow_handle(args.conversation_id)
    messages: list[dict[str, Any]] = []
    page_token = "current"
    while page_token:
        page = await handle.query("get_transcript_page", page_token)
        messages.extend(page["messages"])
        page_token = page["next_page_token"]
    messages.sort(key=lambda message: message["sequence"])
    sequences = [message["sequence"] for message in messages]
    flood_index_counts: dict[int, int] = {}
    reply_counts: dict[int, int] = {}
    customer_sequences: list[int] = []
    for message in messages:
        if message["role"] == "customer":
            customer_sequences.append(message["sequence"])
            match = FLOOD_INDEX_PATTERN.match(message["text"])
            if match:
                index = int(match.group(1))
                flood_index_counts[index] = flood_index_counts.get(index, 0) + 1
        else:
            match = REPLY_SEQUENCE_PATTERN.match(message["text"])
            if match:
                sequence = int(match.group(1))
                reply_counts[sequence] = reply_counts.get(sequence, 0) + 1
    expected_indexes = range(1, args.flood_count + 1)
    missing_flood = [i for i in expected_indexes if i not in flood_index_counts]
    duplicated_flood = sorted(i for i, n in flood_index_counts.items() if n > 1)
    unanswered = [s for s in customer_sequences if s not in reply_counts]
    duplicated_replies = sorted(s for s, n in reply_counts.items() if n > 1)
    print_json(
        {
            "conversation": args.conversation_id,
            "transcript_messages": len(messages),
            "sequences_contiguous": sequences == list(range(1, len(messages) + 1)),
            "customer_messages": len(customer_sequences),
            "flood_messages_expected": args.flood_count,
            "flood_messages_found": len(flood_index_counts),
            "flood_missing_count": len(missing_flood),
            "flood_missing_first": missing_flood[:20],
            "flood_duplicated": duplicated_flood[:20],
            "customer_messages_without_reply": len(unanswered),
            "unanswered_first": unanswered[:20],
            "customer_messages_with_two_or_more_replies": duplicated_replies[:20],
        }
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Temporal conversation demo driver")
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start", help="start a conversation workflow")
    start.add_argument("conversation_id")
    start.add_argument(
        "--first-message", required=True, help="the customer message that opens the conversation"
    )
    start.add_argument("--variant", choices=sorted(WORKFLOW_TYPE_BY_VARIANT), default="correct")
    start.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    start.add_argument(
        "--close-after-seconds", type=int, default=DEFAULT_CLOSE_AFTER_SECONDS
    )
    start.set_defaults(handler=start_conversation)

    post = commands.add_parser("post", help="post one customer message")
    post.add_argument("conversation_id")
    post.add_argument("text")
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
    flood.add_argument("--count", type=int, required=True)
    flood.add_argument("--rate", type=float, default=0, help="messages per second; 0 means as fast as possible")
    flood.add_argument(
        "--message-bytes", type=int, default=FLOOD_MESSAGE_BYTES, help="size of each flood message"
    )
    flood.add_argument(
        "--backlog-timeout",
        type=float,
        default=FLOOD_BACKLOG_TIMEOUT_SECONDS,
        help="seconds to wait for replies after the last message",
    )
    flood.set_defaults(handler=flood_conversation)

    inspect = commands.add_parser(
        "inspect", help="read a run's history: replies, activity types, workflow task failures"
    )
    inspect.add_argument("conversation_id")
    inspect.add_argument("--run-id", default="")
    inspect.add_argument("--dump", default="", help="write the history JSON to this file")
    inspect.set_defaults(handler=print_inspection)

    replies = commands.add_parser(
        "wait-replies", help="wait until history holds at least N replies"
    )
    replies.add_argument("conversation_id")
    replies.add_argument("--count", type=int, required=True)
    replies.add_argument("--timeout", type=float, default=30)
    replies.set_defaults(handler=wait_for_replies)

    retries = commands.add_parser(
        "watch-task-retries", help="sample the pending workflow task attempt over time"
    )
    retries.add_argument("conversation_id")
    retries.add_argument("--seconds", type=float, required=True)
    retries.add_argument("--interval", type=float, default=2)
    retries.set_defaults(handler=watch_task_retries)

    runs = commands.add_parser("runs", help="list every run of a conversation")
    runs.add_argument("conversation_id")
    runs.set_defaults(handler=print_runs)

    verify = commands.add_parser(
        "verify", help="check that every flood message has exactly one reply"
    )
    verify.add_argument("conversation_id")
    verify.add_argument("--flood-count", type=int, required=True)
    verify.set_defaults(handler=verify_transcript)
    return parser


async def main() -> int:
    args = build_argument_parser().parse_args()
    client = await connect_client()
    try:
        await args.handler(client, args)
    except Exception as error:
        print(f"error: {describe_error(error)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
