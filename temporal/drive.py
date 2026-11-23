"""Command-line driver for the Temporal conversation demo.

Usage (from this directory):
  uv run drive.py start <conversation-id> --first-message <text> [--variant naive|correct] [--idle-seconds N] [--close-after-seconds N]
  uv run drive.py post <conversation-id> <text>
  uv run drive.py assign <conversation-id> <agent-name>
  uv run drive.py page <conversation-id> [token]
  uv run drive.py status <conversation-id>
  uv run drive.py flood <conversation-id> --count N [--rate R]

The conversation ID is the workflow ID. Set TEMPORAL_ADDRESS to reach a
server other than localhost:7233.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any

from temporalio.api.enums.v1 import EventType
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryReverseRequest,
)
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError

from conversation_shared import (
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
}
FLOOD_MESSAGE_BYTES = 200
FLOOD_PROGRESS_INTERVAL = 1000
FLOOD_BACKLOG_TIMEOUT_SECONDS = 900


async def connect_client() -> Client:
    # A fixed identity keeps the host name out of recorded histories.
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        identity=os.environ.get("DEMO_CLIENT_IDENTITY", "conversation-demo-driver"),
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


def build_flood_message(index: int) -> str:
    prefix = f"flood message {index:06d}: "
    filler = "the customer repeats the same question about the order status. "
    return (prefix + filler * 4)[:FLOOD_MESSAGE_BYTES]


def describe_error(error: BaseException) -> str:
    if isinstance(error, RPCError):
        return f"RPCError status={error.status.name} message={error.message}"
    return f"{type(error).__name__}: {error}"


async def wait_for_reply_backlog(handle: WorkflowHandle, started_at: float) -> None:
    """Waits until every accepted message has a reply or the workflow stops."""
    deadline = time.monotonic() + FLOOD_BACKLOG_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        description = await handle.describe()
        if description.status is None or description.status.name != "RUNNING":
            print(
                f"workflow stopped while replying: status={description.status.name if description.status else None} "
                f"elapsed={time.monotonic() - started_at:.1f}s history_events={description.history_length}",
                flush=True,
            )
            return
        summary = await handle.query("get_summary")
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
            await handle.signal("post_message", build_flood_message(index))
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
    await wait_for_reply_backlog(handle, started_at)
    # Let the worker finish its last workflow task before the final read.
    await asyncio.sleep(2)
    print("final status:")
    print_json(await read_conversation_status(client, args.conversation_id))


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
    flood.set_defaults(handler=flood_conversation)
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
