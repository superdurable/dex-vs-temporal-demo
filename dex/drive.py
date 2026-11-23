"""Command-line driver for SupportConversationFlow (same contract as the Temporal side).

  start <conversation-id> [--idle-seconds N]
  post <conversation-id> <text>
  assign <conversation-id> <agent-name>
  page <conversation-id> [token]
  status <conversation-id>
  flood <conversation-id> --count N [--rate R] [--concurrency C]
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
    open_process_blob_cache,
    server_address_from_environment,
)
from support_conversation.flow import SupportConversationFlow
from support_conversation.transcript import (
    ASSIGNED_AGENT,
    CONVERSATION_STATE,
    CURRENT_CHUNK_TOKEN,
    SUMMARY,
    TRANSCRIPT_CHUNKS,
    ConversationStart,
    ConversationState,
    TranscriptChunk,
    count_retained_archived_chunks,
    parse_transcript_page_token,
)

FLOOD_MESSAGE_BYTES = 200
FLOOD_PROGRESS_EVERY = 1000


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
    def __init__(self, client: AsyncClient, flow: SupportConversationFlow) -> None:
        self.client = client
        self.flow = flow

    async def start_conversation(self, conversation_id: str, idle_seconds: int) -> None:
        options = (
            StartFlowOptions(
                request_id=f"start-{conversation_id}", ignore_already_started=True
            )
            .with_attribute(CONVERSATION_STATE, ConversationState())
            .with_attribute(TRANSCRIPT_CHUNKS, CURRENT_CHUNK_TOKEN, TranscriptChunk())
        )
        run_id = await self.client.start_flow(
            self.flow, conversation_id, ConversationStart(idle_seconds), options
        )
        print_json(
            {"conversation": conversation_id, "runId": run_id, "idleSeconds": idle_seconds}
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
        values = await self.client.get_attributes(
            conversation_id, CONVERSATION_STATE, ASSIGNED_AGENT, SUMMARY
        )
        state = values.get(CONVERSATION_STATE) or ConversationState()
        summary = values.get(SUMMARY)
        return {
            "conversation": conversation_id,
            "flowStatus": info.status.name,
            "conversationStatus": state.status,
            "messageCount": state.message_count,
            "retainedChunkCount": count_retained_archived_chunks(state),
            "oldestArchivedChunkToken": state.oldest_archived_chunk_token,
            "newestArchivedChunkToken": state.newest_archived_chunk_token,
            "assignedAgent": values.get(ASSIGNED_AGENT),
            "summary": asdict(summary) if summary is not None else None,
            "historyEventCount": read_history_event_count(conversation_id),
        }

    async def flood(
        self,
        conversation_id: str,
        count: int,
        rate: float,
        concurrency: int,
        max_seconds: float,
    ) -> int:
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
        print_json({"flood": result})
        print_json({"status": await self.read_status(conversation_id)})
        return 1 if "error" in result else 0


def flood_message_text(index: int) -> str:
    prefix = f"flood message {index + 1:06d}: "
    filler = "customer asks about order status and delivery window " * 5
    return (prefix + filler)[:FLOOD_MESSAGE_BYTES]


def read_history_event_count(conversation_id: str) -> int | None:
    """Count the history events still kept, through dexcli; None if unavailable."""
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
        return None
    return count_history_events(completed.stdout)


def count_history_events(output: str) -> int | None:
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        lines = [line for line in output.splitlines() if line.strip()]
        try:
            return sum(len(json.loads(line).get("events", [])) for line in lines)
        except (json.JSONDecodeError, AttributeError):
            return None
    if isinstance(parsed, list):
        return len(parsed)
    if isinstance(parsed, dict):
        for key in ("events", "historyEvents", "history_events"):
            if isinstance(parsed.get(key), list):
                return len(parsed[key])
    return None


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("conversation_id")
    start.add_argument("--idle-seconds", type=int, default=600)
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
    return parser.parse_args(argv)


async def run_command(arguments: argparse.Namespace) -> int:
    flow = build_support_conversation_flow()
    cache, cache_directory = open_process_blob_cache("client")
    client = AsyncClient(
        build_registry(flow),
        cache,
        ClientOptions(server_address=server_address_from_environment()),
    )
    driver = SupportConversationDriver(client, flow)
    try:
        if arguments.command == "start":
            await driver.start_conversation(arguments.conversation_id, arguments.idle_seconds)
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
