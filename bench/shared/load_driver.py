"""Engine-neutral load driver for the worker memory experiment.

Phases:
  fill  creates N conversations and fills each with M exchanges (customer
        message + AI reply) of B bytes per customer message. It writes a
        manifest with the conversation IDs.
  rush  the "morning rush": K conversations from the manifest come back at
        R conversations per second. Each sends F follow-ups; each follow-up is
        posted after the previous reply plus T seconds of think time.

Every customer message is one CSV row: phase, conversation, message index,
post time, reply time, latency, status, error. Times are Unix epoch seconds.

Usage:
  python load_driver.py --adapter temporal_adapter:TemporalConversationAdapter fill ...
  python load_driver.py --adapter ... rush ...
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import importlib
import json
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from conversation_adapter import ConversationAdapter

PROGRESS_INTERVAL_SECONDS = 5.0
LATENCY_CSV_HEADER = [
    "phase",
    "conversation_id",
    "message_index",
    "post_epoch",
    "reply_epoch",
    "latency_ms",
    "status",
    "error",
]
MESSAGE_FILLER = (
    "The customer pastes the order details, the delivery notes and the earlier "
    "support answers again so the assistant has the full context. "
)


def load_adapter(spec: str) -> ConversationAdapter:
    module_name, _, class_name = spec.partition(":")
    adapter_class = getattr(importlib.import_module(module_name), class_name)
    return adapter_class()


def build_customer_message(conversation_id: str, message_index: int, message_bytes: int) -> str:
    prefix = f"[{conversation_id} message {message_index}] "
    repeat_count = message_bytes // len(MESSAGE_FILLER) + 1
    return (prefix + MESSAGE_FILLER * repeat_count)[:message_bytes]


def describe_error(error: BaseException) -> str:
    text = f"{type(error).__name__}: {error}".replace("\n", " ")
    return text[:300]


@dataclass
class LatencyRecorder:
    path: Path
    phase: str
    posted: int = 0
    replied: int = 0
    failed: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._file = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        self._writer.writerow(LATENCY_CSV_HEADER)

    def record(
        self,
        conversation_id: str,
        message_index: int,
        post_epoch: float,
        reply_epoch: float | None,
        error: str = "",
    ) -> None:
        latency_ms = (reply_epoch - post_epoch) * 1000 if reply_epoch else None
        if latency_ms is not None:
            self.replied += 1
            self.latencies_ms.append(latency_ms)
        else:
            self.failed += 1
        self._writer.writerow(
            [
                self.phase,
                conversation_id,
                message_index,
                f"{post_epoch:.3f}",
                f"{reply_epoch:.3f}" if reply_epoch else "",
                f"{latency_ms:.1f}" if latency_ms is not None else "",
                "ok" if latency_ms is not None else "error",
                error,
            ]
        )
        self._file.flush()

    def close(self) -> None:
        self._file.close()

    def progress_line(self, started_at: float) -> str:
        recent = sorted(self.latencies_ms[-200:])
        p50 = recent[len(recent) // 2] if recent else 0.0
        return (
            f"{self.phase}: posted={self.posted} replied={self.replied} "
            f"failed={self.failed} elapsed={time.monotonic() - started_at:.0f}s "
            f"recent_p50_ms={p50:.0f}"
        )


async def post_and_wait(
    adapter: ConversationAdapter,
    recorder: LatencyRecorder,
    conversation_id: str,
    message_index: int,
    text: str,
    reply_timeout: float,
    is_first_message: bool,
) -> bool:
    post_epoch = time.time()
    recorder.posted += 1
    try:
        if is_first_message:
            await adapter.start_conversation(conversation_id, text)
        else:
            await adapter.post_message(conversation_id, text)
        await adapter.wait_for_reply(conversation_id, reply_timeout)
    except (Exception, asyncio.TimeoutError) as error:
        recorder.record(conversation_id, message_index, post_epoch, None, describe_error(error))
        return False
    recorder.record(conversation_id, message_index, post_epoch, time.time())
    return True


async def report_progress(recorder: LatencyRecorder, started_at: float) -> None:
    while True:
        await asyncio.sleep(PROGRESS_INTERVAL_SECONDS)
        print(recorder.progress_line(started_at), flush=True)


async def run_fill(adapter: ConversationAdapter, args: argparse.Namespace) -> None:
    first_index = args.first_index
    conversation_ids = [
        f"{args.prefix}-{index:05d}"
        for index in range(first_index, first_index + args.conversations)
    ]
    population = conversation_ids
    manifest_path = Path(args.manifest)
    if first_index > 0 and manifest_path.exists():
        # A later part of a split fill extends the manifest of the earlier parts.
        population = sorted(
            set(json.loads(manifest_path.read_text(encoding="utf-8"))["conversation_ids"])
            | set(conversation_ids)
        )
    manifest = {
        "engine": adapter.engine_name,
        "prefix": args.prefix,
        "conversations": len(population),
        "exchanges": args.exchanges,
        "message_bytes": args.message_bytes,
        "conversation_ids": population,
    }
    Path(args.manifest).write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    recorder = LatencyRecorder(Path(args.latency_csv), "fill")
    queue: asyncio.Queue[str] = asyncio.Queue()
    for conversation_id in conversation_ids:
        queue.put_nowait(conversation_id)
    started_at = time.monotonic()

    async def fill_worker() -> None:
        while not queue.empty():
            conversation_id = queue.get_nowait()
            for message_index in range(1, args.exchanges + 1):
                text = build_customer_message(conversation_id, message_index, args.message_bytes)
                replied = await post_and_wait(
                    adapter, recorder, conversation_id, message_index, text,
                    args.reply_timeout, message_index == 1,
                )
                if not replied:
                    break

    progress = asyncio.create_task(report_progress(recorder, started_at))
    await asyncio.gather(*(fill_worker() for _ in range(args.concurrency)))
    progress.cancel()
    recorder.close()
    elapsed = time.monotonic() - started_at
    print(
        f"fill finished conversations={args.conversations} exchanges={recorder.replied} "
        f"failed={recorder.failed} elapsed={elapsed:.1f}s "
        f"rate={recorder.replied / elapsed if elapsed else 0:.1f} exchanges/s",
        flush=True,
    )


async def observe_all(adapter: ConversationAdapter, conversation_ids: list[str], concurrency: int) -> None:
    semaphore = asyncio.Semaphore(concurrency)

    async def observe_one(conversation_id: str) -> None:
        async with semaphore:
            await adapter.observe_conversation(conversation_id)

    await asyncio.gather(*(observe_one(conversation_id) for conversation_id in conversation_ids))


async def run_rush(adapter: ConversationAdapter, args: argparse.Namespace) -> None:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    population = manifest["conversation_ids"]
    returning = random.Random(args.seed).sample(population, min(args.returning, len(population)))
    message_bytes = args.message_bytes or manifest["message_bytes"]
    next_index = manifest["exchanges"] + 1

    observe_started = time.monotonic()
    await observe_all(adapter, returning, args.observe_concurrency)
    print(
        f"observed {len(returning)} returning conversations in "
        f"{time.monotonic() - observe_started:.1f}s (not timed)",
        flush=True,
    )
    if args.ready_file:
        Path(args.ready_file).write_text(f"{time.time():.3f}\n", encoding="utf-8")
    if args.start_delay > 0:
        await asyncio.sleep(args.start_delay)

    recorder = LatencyRecorder(Path(args.latency_csv), "rush")
    rush_started_epoch = time.time()
    started_at = time.monotonic()
    print(f"rush started epoch={rush_started_epoch:.3f}", flush=True)

    async def returning_customer(arrival_index: int, conversation_id: str) -> None:
        delay = started_at + arrival_index / args.rate - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        for follow_up in range(args.follow_ups):
            if follow_up > 0 and args.think_seconds > 0:
                await asyncio.sleep(args.think_seconds)
            message_index = next_index + follow_up
            text = build_customer_message(conversation_id, message_index, message_bytes)
            replied = await post_and_wait(
                adapter, recorder, conversation_id, message_index, text,
                args.reply_timeout, False,
            )
            if not replied:
                break

    progress = asyncio.create_task(report_progress(recorder, started_at))
    customers = asyncio.gather(
        *(returning_customer(index, conversation_id) for index, conversation_id in enumerate(returning))
    )
    try:
        await asyncio.wait_for(customers, args.max_seconds)
    except asyncio.TimeoutError:
        print(f"rush stopped by --max-seconds {args.max_seconds}", flush=True)
    progress.cancel()
    recorder.close()
    print(
        f"rush finished returning={len(returning)} follow_ups={args.follow_ups} "
        f"replied={recorder.replied} failed={recorder.failed} "
        f"elapsed={time.monotonic() - started_at:.1f}s",
        flush=True,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--adapter", required=True, help="<module>:<ClassName> of a ConversationAdapter")
    phases = parser.add_subparsers(dest="phase", required=True)

    fill = phases.add_parser("fill")
    fill.add_argument("--manifest", required=True)
    fill.add_argument("--latency-csv", required=True)
    fill.add_argument("--prefix", required=True)
    fill.add_argument("--conversations", type=int, required=True)
    fill.add_argument(
        "--first-index", type=int, default=0,
        help="index of the first conversation; a split fill runs several parts",
    )
    fill.add_argument("--exchanges", type=int, required=True)
    fill.add_argument("--message-bytes", type=int, required=True)
    fill.add_argument("--concurrency", type=int, default=100)
    fill.add_argument("--reply-timeout", type=float, default=120.0)

    rush = phases.add_parser("rush")
    rush.add_argument("--manifest", required=True)
    rush.add_argument("--latency-csv", required=True)
    rush.add_argument("--returning", type=int, required=True, help="K returning conversations")
    rush.add_argument("--rate", type=float, required=True, help="R returning conversations per second")
    rush.add_argument("--follow-ups", type=int, default=1, help="F messages per returning conversation")
    rush.add_argument("--think-seconds", type=float, default=0.0)
    rush.add_argument("--message-bytes", type=int, default=0, help="default: the manifest's size")
    rush.add_argument("--seed", type=int, default=1)
    rush.add_argument("--reply-timeout", type=float, default=300.0)
    rush.add_argument("--observe-concurrency", type=int, default=50)
    rush.add_argument("--start-delay", type=float, default=0.0, help="seconds between observing and the rush")
    rush.add_argument("--ready-file", default="", help="written when observing is done")
    rush.add_argument("--max-seconds", type=float, default=1500.0)
    return parser


async def main() -> int:
    args = build_argument_parser().parse_args()
    adapter = load_adapter(args.adapter)
    await adapter.connect()
    try:
        if args.phase == "fill":
            await run_fill(adapter, args)
        else:
            await run_rush(adapter, args)
    finally:
        await adapter.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
