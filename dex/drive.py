"""Command-line driver for SupportConversationFlow (same contract as the Temporal side).

  start <conversation-id> --first-message <text> [--idle-seconds N] [--close-after-seconds N]
  post <conversation-id> <text>
  assign <conversation-id> <agent-name>
  page <conversation-id> [token]
  status <conversation-id>
  flood <conversation-id> --count N [--rate R] [--concurrency C]
  paced <conversation-id> --rate R --duration-seconds S [--snapshot-at N,M] [--drain-seconds S]
  wait-replies <conversation-id> <reply-count> [--timeout-seconds S]

SUPPORT_FLOW_VERSION (v1, v2, v2-short-idle) selects the Flow code; v1 is the default.
"""

from __future__ import annotations

import os

# The status command runs dexcli as a subprocess; keep gRPC fork handlers quiet.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "false")

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict
from typing import Any

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
    server_address_from_environment,
)
from support_conversation.transcript import (
    CURRENT_CHUNK_TOKEN,
    ConversationSettings,
    ReplyRequest,
    ConversationState,
    TranscriptChunk,
    count_retained_archived_chunks,
    parse_transcript_page_token,
)

FLOW_VERSION = flow_version_from_environment()
FLOW_MODULE = import_flow_module(FLOW_VERSION)
ASSIGNED_AGENT = FLOW_MODULE.ASSIGNED_AGENT
CONVERSATION_STATE = FLOW_MODULE.CONVERSATION_STATE
SUMMARY = FLOW_MODULE.SUMMARY
TRANSCRIPT_CHUNKS = FLOW_MODULE.TRANSCRIPT_CHUNKS

FLOOD_MESSAGE_BYTES = 200
FLOOD_PROGRESS_EVERY = 1000
PACED_PROGRESS_SECONDS = 60
PACED_MAX_IN_FLIGHT = 64


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
        if not first_message.strip():
            raise ValueError("--first-message must not be empty")
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

    async def post_message(self, conversation_id: str, text: str) -> None:
        await self.client.invoke_rpc(self.flow.post_message, conversation_id, text)
        print_json({"conversation": conversation_id, "posted": text})

    async def assign_agent(self, conversation_id: str, agent_name: str) -> None:
        assigned = await self.client.invoke_rpc(
            self.flow.assign_agent, conversation_id, agent_name
        )
        print_json({"conversation": conversation_id, "assignedAgent": assigned})

    async def read_page(self, conversation_id: str, page_token: str) -> dict[str, Any]:
        page_token = parse_transcript_page_token(page_token)
        page = await self.client.invoke_rpc(
            self.flow.get_transcript_page,
            conversation_id,
            page_token,
            options=RPCInvokeOptions(
                load_attribute_map_instances=(TRANSCRIPT_CHUNKS.load(page_token),)
            ),
        )
        return asdict(page)

    async def read_status(self, conversation_id: str) -> dict[str, Any]:
        info = await self.client.describe_flow(conversation_id)
        current_chunk = TRANSCRIPT_CHUNKS.instance(CURRENT_CHUNK_TOKEN)
        values = await self.client.get_attributes(
            conversation_id, CONVERSATION_STATE, ASSIGNED_AGENT, SUMMARY, current_chunk
        )
        state = values.get(CONVERSATION_STATE) or ConversationState()
        summary = values.get(SUMMARY)
        chunk = values.get(current_chunk) or TranscriptChunk()
        return {
            "conversation": conversation_id,
            "flowStatus": info.status.name,
            "conversationStatus": state.status,
            "recordCount": state.record_count,
            "customerMessageCount": state.customer_message_count,
            "assistantReplyCount": state.assistant_reply_count,
            "latestRecords": [asdict(record) for record in chunk.records[-4:]],
            "retainedChunkCount": count_retained_archived_chunks(state),
            "oldestArchivedChunkToken": state.oldest_archived_chunk_token,
            "newestArchivedChunkToken": state.newest_archived_chunk_token,
            "assignedAgent": values.get(ASSIGNED_AGENT),
            "summary": asdict(summary) if summary is not None else None,
            "history": read_history_event_summary(conversation_id),
        }

    async def flood(
        self,
        conversation_id: str,
        count: int,
        rate: float,
        concurrency: int,
        max_seconds: float,
    ) -> int:
        baseline = await self.client.get_attributes(conversation_id, CONVERSATION_STATE)
        baseline_customers = (
            baseline.get(CONVERSATION_STATE) or ConversationState()
        ).customer_message_count
        started = time.monotonic()
        next_index = 0
        accepted = 0
        failure: BaseException | None = None
        stop = asyncio.Event()

        async def post_until_done() -> None:
            nonlocal next_index, accepted, failure
            while not stop.is_set() and next_index < count:
                if time.monotonic() - started > max_seconds:
                    stop.set()
                    return
                index = next_index
                next_index += 1
                if rate > 0:
                    delay = started + index / rate - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                try:
                    await self.client.invoke_rpc(
                        self.flow.post_message,
                        conversation_id,
                        flood_message_text(index),
                    )
                except Exception as error:  # report the exact engine rejection
                    if failure is None:
                        failure = error
                    stop.set()
                    return
                accepted += 1
                if accepted % FLOOD_PROGRESS_EVERY == 0:
                    print(
                        f"progress accepted={accepted} "
                        f"elapsed_seconds={time.monotonic() - started:.1f}",
                        flush=True,
                    )

        await asyncio.gather(*(post_until_done() for _ in range(concurrency)))
        elapsed = time.monotonic() - started
        result: dict[str, Any] = {
            "conversation": conversation_id,
            "requested": count,
            "accepted": accepted,
            "elapsedSeconds": round(elapsed, 1),
            "acceptedPerSecond": round(accepted / elapsed, 1) if elapsed else None,
        }
        if failure is not None:
            result["error"] = describe_error(failure)
        elif accepted < count:
            result["error"] = {"message": f"stopped after --max-seconds {max_seconds}"}
        else:
            info = await self.client.describe_flow(conversation_id)
            if info.status != FlowStatus.RUNNING:
                result["error"] = {"message": f"Flow is {info.status.name}"}
        if "error" not in result:
            result["allAnsweredSeconds"] = await self.wait_until_answered(
                conversation_id, baseline_customers + accepted, started, max_seconds
            )
            if result["allAnsweredSeconds"] is None:
                result["error"] = {"message": f"not all answered after --max-seconds {max_seconds}"}
        print_json({"flood": result})
        print_json({"status": await self.read_status(conversation_id)})
        return 1 if "error" in result else 0


    async def wait_until_answered(
        self,
        conversation_id: str,
        expected_customer_messages: int,
        started: float,
        max_seconds: float,
    ) -> float | None:
        """Wait until every flooded message is answered and recorded; None on timeout.

        A customer message is recorded in the same commit as the reply to it.
        """
        last_report = -30.0
        while time.monotonic() - started <= max_seconds:
            values = await self.client.get_attributes(conversation_id, CONVERSATION_STATE)
            state = values.get(CONVERSATION_STATE) or ConversationState()
            elapsed = time.monotonic() - started
            progress = (
                f"customer_recorded={state.customer_message_count}/"
                f"{expected_customer_messages} replies={state.assistant_reply_count} "
                f"elapsed_seconds={elapsed:.1f}"
            )
            if state.customer_message_count >= expected_customer_messages:
                print(f"answered all {progress}", flush=True)
                return round(elapsed, 1)
            if elapsed - last_report >= 30:
                last_report = elapsed
                print(f"answering {progress}", flush=True)
            await asyncio.sleep(1)
        return None

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
                page = await self.read_page(conversation_id, CURRENT_CHUNK_TOKEN)
                print_json(
                    {
                        "conversation": conversation_id,
                        "replies": state.assistant_reply_count,
                        "waitedSeconds": elapsed,
                        "newestTwoRecords": page["records"][:2],
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

    async def post_at_fixed_rate(
        self,
        conversation_id: str,
        rate: float,
        duration_seconds: float,
        snapshot_thresholds: list[int],
        drain_seconds: float,
    ) -> int:
        """Post rate messages per second for duration_seconds, then drain briefly.

        Each post starts at its own slot, so a slow RPC does not slow the rate.
        A snapshot is taken when the accepted count first reaches each threshold.
        """
        baseline = await self.client.get_attributes(conversation_id, CONVERSATION_STATE)
        baseline_customers = (
            baseline.get(CONVERSATION_STATE) or ConversationState()
        ).customer_message_count
        total = int(rate * duration_seconds)
        started = time.monotonic()
        accepted = 0
        failure: BaseException | None = None
        stop = asyncio.Event()
        in_flight = asyncio.Semaphore(PACED_MAX_IN_FLIGHT)
        snapshots: list[dict[str, Any]] = []

        async def post_one(index: int) -> None:
            nonlocal accepted, failure
            try:
                await self.client.invoke_rpc(
                    self.flow.post_message, conversation_id, flood_message_text(index)
                )
                accepted += 1
            except Exception as error:  # report the exact engine rejection
                if failure is None:
                    failure = error
                stop.set()
            finally:
                in_flight.release()

        async def post_on_schedule() -> None:
            posts: list[asyncio.Task[None]] = []
            for index in range(total):
                delay = started + index / rate - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                if stop.is_set():
                    break
                await in_flight.acquire()
                posts.append(asyncio.create_task(post_one(index)))
            await asyncio.gather(*posts)

        async def read_progress() -> dict[str, Any]:
            values = await self.client.get_attributes(conversation_id, CONVERSATION_STATE)
            state = values.get(CONVERSATION_STATE) or ConversationState()
            return {
                "elapsedSeconds": round(time.monotonic() - started, 1),
                "accepted": accepted,
                "customerRecorded": state.customer_message_count - baseline_customers,
                "replies": state.assistant_reply_count,
                "backlog": baseline_customers + accepted - state.customer_message_count,
            }

        async def report_until_stopped() -> None:
            pending_thresholds = sorted(snapshot_thresholds)
            last_report = 0.0
            while not stop.is_set():
                await asyncio.sleep(1)
                if pending_thresholds and accepted >= pending_thresholds[0]:
                    threshold = pending_thresholds.pop(0)
                    snapshot = {"threshold": threshold, **await read_progress()}
                    snapshot["flowStatus"] = (
                        await self.client.describe_flow(conversation_id)
                    ).status.name
                    snapshot["history"] = await asyncio.to_thread(
                        read_history_event_summary, conversation_id
                    )
                    snapshots.append(snapshot)
                    print_json({"snapshot": snapshot})
                if time.monotonic() - last_report >= PACED_PROGRESS_SECONDS:
                    last_report = time.monotonic()
                    print_json({"progress": await read_progress()})

        reporter = asyncio.create_task(report_until_stopped())
        await post_on_schedule()
        posting_seconds = time.monotonic() - started
        drain_started = time.monotonic()
        while time.monotonic() - drain_started < drain_seconds:
            if (await read_progress())["backlog"] <= 0:
                break
            await asyncio.sleep(1)
        stop.set()
        await reporter
        result: dict[str, Any] = {
            "conversation": conversation_id,
            "rate": rate,
            "durationSeconds": duration_seconds,
            "requested": total,
            "postingSeconds": round(posting_seconds, 1),
            "end": await read_progress(),
            "snapshots": snapshots,
        }
        if failure is not None:
            result["error"] = describe_error(failure)
        print_json({"paced": result})
        print_json({"status": await self.read_status(conversation_id)})
        return 1 if failure is not None else 0


def flood_message_text(index: int) -> str:
    prefix = f"flood message {index + 1:06d}: "
    filler = "customer asks about order status and delivery window " * 5
    return (prefix + filler)[:FLOOD_MESSAGE_BYTES]


def read_history_event_summary(conversation_id: str) -> dict[str, int | None]:
    """Return the number of history events Dex still keeps and the highest event ID.

    Dex keeps a recent window of history, so the kept count is not a running total;
    the highest event ID counts every event written. Both are None if unavailable.
    """
    summary: dict[str, int | None] = {"eventsKept": None, "highestEventId": None}
    dexcli = shutil.which("dexcli")
    if dexcli is None:
        return summary
    dexcli = shutil.which("dexcli")
    if dexcli is None:
        return None
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
    summary["eventsKept"] = len(event_ids)
    summary["highestEventId"] = max(event_ids, default=None)
    return summary


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
    assign = commands.add_parser("assign")
    assign.add_argument("conversation_id")
    assign.add_argument("agent_name")
    page = commands.add_parser("page")
    page.add_argument("conversation_id")
    page.add_argument("token", nargs="?", default=CURRENT_CHUNK_TOKEN)
    status = commands.add_parser("status")
    status.add_argument("conversation_id")
    flood = commands.add_parser("flood")
    flood.add_argument("conversation_id")
    flood.add_argument("--count", type=int, required=True)
    flood.add_argument("--rate", type=float, default=0, help="messages per second; 0 is unlimited")
    flood.add_argument("--concurrency", type=int, default=16)
    flood.add_argument("--max-seconds", type=float, default=1800)
    paced = commands.add_parser("paced")
    paced.add_argument("conversation_id")
    paced.add_argument("--rate", type=float, required=True, help="messages per second")
    paced.add_argument("--duration-seconds", type=float, required=True)
    paced.add_argument(
        "--snapshot-at",
        default="",
        help="comma-separated accepted counts at which to record a snapshot",
    )
    paced.add_argument("--drain-seconds", type=float, default=60)
    wait_replies = commands.add_parser("wait-replies")
    wait_replies.add_argument("conversation_id")
    wait_replies.add_argument("reply_count", type=int)
    wait_replies.add_argument("--timeout-seconds", type=float, default=60)
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
            await driver.post_message(arguments.conversation_id, arguments.text)
        elif arguments.command == "assign":
            await driver.assign_agent(arguments.conversation_id, arguments.agent_name)
        elif arguments.command == "page":
            print_json(await driver.read_page(arguments.conversation_id, arguments.token))
        elif arguments.command == "status":
            print_json(await driver.read_status(arguments.conversation_id))
        elif arguments.command == "flood":
            return await driver.flood(
                arguments.conversation_id,
                arguments.count,
                arguments.rate,
                arguments.concurrency,
                arguments.max_seconds,
            )
        elif arguments.command == "paced":
            return await driver.post_at_fixed_rate(
                arguments.conversation_id,
                arguments.rate,
                arguments.duration_seconds,
                [int(value) for value in arguments.snapshot_at.split(",") if value],
                arguments.drain_seconds,
            )
        elif arguments.command == "wait-replies":
            return await driver.wait_for_replies(
                arguments.conversation_id,
                arguments.reply_count,
                arguments.timeout_seconds,
            )
        return 0
    except (DexServiceError, FlowAlreadyStartedError, ValueError) as error:
        print_json({"error": describe_error(error)})
        return 1
    finally:
        await client.close()
        cache.close()
        shutil.rmtree(cache_directory, ignore_errors=True)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    sys.exit(asyncio.run(run_command(parse_arguments(sys.argv[1:]))))
