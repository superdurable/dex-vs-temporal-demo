#!/usr/bin/env python3
"""Checks for the Dex demo scripts; each exits non-zero when its check fails.

  outbox            notices of one conversation and run, counted by distinct key
  d1-timeline       D1 timeline and the nudge interval after the second reply
  await-wait        poll history until a Step execution registered its wait
  check-after-deploy  the wait registered before a deploy finished on the new code
  check-start       a run started at the expected Step on the expected Worker
  nudge-intervals   wait-to-nudge intervals, Timer conditions, and Workers
  failures          failed Step events and RESOURCE_EXHAUSTED lines for one run
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from history_steps import parse_time, read_events

# A Timer fires at or after its due time; the nudge then needs two Step methods.
DEFAULT_TOLERANCE_SECONDS = 5.0


def print_json(value: object) -> None:
    print(json.dumps(value, indent=2), flush=True)


def fail(message: str, **details: object) -> int:
    print_json({"check": "failed", "reason": message, **details})
    return 1


# --- Outbox -------------------------------------------------------------------


def read_notices(outbox: Path, conversation: str, run_id: str, kind: str) -> dict:
    """Notices of one conversation (and run), deduplicated by notice key.

    A retried Step can append a line with the same key; it counts once.
    """
    by_key: dict[str, dict] = {}
    lines = duplicates = malformed = 0
    if outbox.exists():
        for line in outbox.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            lines += 1
            try:
                notice = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if notice.get("conversation") != conversation:
                continue
            if run_id and not str(notice.get("key", "")).startswith(run_id + "/"):
                continue
            if kind and notice.get("kind") != kind:
                continue
            key = notice.get("key") or f"no-key:{line}"
            if key in by_key:
                duplicates += 1
                continue
            by_key[key] = notice
    return {
        "outboxExists": outbox.exists(),
        "notices": sorted(by_key.values(), key=lambda n: n["time"]),
        "distinctKeys": len(by_key),
        "duplicateLinesIgnored": duplicates,
        "malformedLines": malformed,
    }


def command_outbox(arguments: argparse.Namespace) -> int:
    deadline = time.monotonic() + arguments.timeout_seconds
    while True:
        result = read_notices(
            Path(arguments.outbox), arguments.conversation, arguments.run_id, arguments.kind
        )
        if result["distinctKeys"] >= arguments.wait_count:
            print_json(result)
            return 0
        if time.monotonic() > deadline:
            return fail(
                f"{result['distinctKeys']} of {arguments.wait_count} notices after "
                f"{arguments.timeout_seconds}s",
                **result,
            )
        time.sleep(0.5)


# --- D1 -----------------------------------------------------------------------


def command_d1_timeline(arguments: argparse.Namespace) -> int:
    started_at = arguments.started_at
    page = json.loads(Path(arguments.page_json).read_text(encoding="utf-8"))
    notices = read_notices(Path(arguments.outbox), arguments.conversation, arguments.run_id, "")

    def offset(iso_time: str) -> float:
        return round(datetime.fromisoformat(iso_time.replace("Z", "+00:00")).timestamp() - started_at, 1)

    events = [
        (0.0, "start: customer first message"),
        (round(arguments.follow_up_at - started_at, 1), "customer follow-up posted"),
    ]
    replies = []
    for record in reversed(page["records"]):
        if record["role"] == "assistant":
            replies.append(offset(record["recorded_at"]))
            events.append(
                (replies[-1], f"AI reply recorded (seq {record['sequence']}, tools {', '.join(record['tool_calls'])})")
            )
    nudges = [n for n in notices["notices"] if n["kind"] == "nudge"]
    for notice in notices["notices"]:
        events.append((offset(notice["time"]), f"outbox {notice['kind']} key={notice['key']} (messages={notice['messages']})"))
    print(f"Timeline of {arguments.conversation} (seconds since start; outbox lines counted once per key)")
    for at, text in sorted(events):
        print(f"  t={at:6.1f}s  {text}")
    if len(replies) < 2 or not nudges:
        return fail("expected two replies and one nudge", replies=len(replies), nudges=len(nudges))
    interval = round(offset(nudges[0]["time"]) - replies[1], 1)
    low, high = arguments.idle_seconds, arguments.idle_seconds + arguments.tolerance_seconds
    result = {
        "secondReplySeconds": replies[1],
        "nudgeSeconds": offset(nudges[0]["time"]),
        "nudgeAfterSecondReplySeconds": interval,
        "expectedRangeSeconds": [low, high],
    }
    if not low <= interval <= high:
        return fail("the nudge is outside the idle window range", **result)
    print_json({"check": "passed", **result})
    return 0


# --- History checks -------------------------------------------------------------


def read_history(path: str) -> list[dict]:
    return read_events(Path(path).read_text(encoding="utf-8"))


def fetch_history(flow_id: str, server: str, out_path: Path) -> list[dict]:
    completed = subprocess.run(
        ["dexcli", "flow", "history", flow_id, "-all", "-no-hydrate", "-page-size", "1000",
         "-server", server, "-timeout", "60s"],
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        return []
    out_path.write_text(completed.stdout, encoding="utf-8")
    return read_events(completed.stdout)


def context_of(event: dict) -> dict:
    return event.get("payload", {}).get("context") or {}


def worker_of(event: dict) -> str:
    return context_of(event).get("pullRouting", {}).get("servedByAppWorkerId", "")


def describe_wait(event: dict) -> dict:
    wait = event["payload"].get("output", {}).get("waitForCondition", {})
    return {
        "eventId": int(event["eventId"]),
        "eventTime": event["eventTime"],
        "stepExecutionId": context_of(event)["stepExecutionId"],
        "workerId": worker_of(event),
        "timerSeconds": [int(t.get("durationSeconds", 0)) for t in wait.get("timerConditions", [])],
        "channels": [c["channelName"] for c in wait.get("channelConditions", [])],
    }


def find_wait(events: list[dict], step_execution_id: str) -> dict | None:
    for event in events:
        if (
            event.get("type") == "StepWaitForCompleted"
            and context_of(event).get("stepExecutionId") == step_execution_id
        ):
            return event
    return None


def command_await_wait(arguments: argparse.Namespace) -> int:
    deadline = time.monotonic() + arguments.timeout_seconds
    while True:
        events = fetch_history(arguments.flow_id, arguments.server, Path(arguments.history_out))
        event = find_wait(events, arguments.step_execution)
        if event is not None:
            print_json({"waitRegistered": describe_wait(event)})
            return 0
        if time.monotonic() > deadline:
            return fail(f"{arguments.step_execution} registered no wait in {arguments.timeout_seconds}s")
        time.sleep(0.5)


def step_events_after(events: list[dict], event_id: int) -> list[dict]:
    return [e for e in events if int(e["eventId"]) > event_id and context_of(e)]


def command_check_after_deploy(arguments: argparse.Namespace) -> int:
    """The old wait finished on the new Worker, which then ran the new Step path."""
    events = read_history(arguments.history)
    wait_event = arguments.wait_event
    later = step_events_after(events, wait_event)
    executes = [e for e in later if e["type"] == "StepExecuteCompleted"]
    observed = [
        {"eventId": int(e["eventId"]), "type": e["type"],
         "stepExecutionId": context_of(e)["stepExecutionId"], "workerId": worker_of(e)}
        for e in later[: len(arguments.expect_steps) + 2]
    ]
    resumed = next(
        (e for e in executes if str(context_of(e).get("waitForCompletedEventId")) == str(wait_event)),
        None,
    )
    if resumed is None:
        return fail(f"no Execute completed the wait of event {wait_event}", observed=observed)
    if worker_of(resumed) != arguments.new_worker:
        return fail("the wait registered before the deploy was not finished by the new Worker", observed=observed)
    after_resume = [e for e in executes if int(e["eventId"]) > int(resumed["eventId"])]
    path = [context_of(e)["stepType"] for e in after_resume[: len(arguments.expect_steps)]]
    workers = {worker_of(e) for e in after_resume[: len(arguments.expect_steps)]}
    if path != arguments.expect_steps or workers != {arguments.new_worker}:
        return fail("unexpected Step path after the deploy", path=path, workers=sorted(workers), observed=observed)
    print_json({"check": "passed", "resumedEventId": int(resumed["eventId"]),
                "resumedBy": worker_of(resumed), "pathAfterResume": path, "observed": observed})
    return 0


def command_check_start(arguments: argparse.Namespace) -> int:
    events = read_history(arguments.history)
    started = next((e for e in events if e.get("type") == "FlowStartedOrContinued"), None)
    if started is None:
        return fail("no FlowStarted event in the retained history")
    start_step = started["payload"].get("initialStart", {}).get("startStepType")
    first = next((e for e in events if e.get("type") == "StepExecuteCompleted"), None)
    result = {
        "startStepType": start_step,
        "firstStepExecuted": context_of(first).get("stepType") if first else None,
        "firstWorker": worker_of(first) if first else None,
    }
    if start_step != arguments.start_step or result["firstStepExecuted"] != arguments.start_step:
        return fail("the run did not start at the expected Step", **result)
    if result["firstWorker"] != arguments.worker:
        return fail("the first Step ran on another Worker", **result)
    print_json({"check": "passed", **result})
    return 0


def command_nudge_intervals(arguments: argparse.Namespace) -> int:
    events = read_history(arguments.history)
    nudges = [
        e for e in events
        if e.get("type") == "StepExecuteCompleted" and context_of(e).get("stepType") == "NudgeCustomerStep"
    ]
    rows = []
    for nudge in nudges:
        source = context_of(nudge).get("fromStepExecutionId", "")
        wait = find_wait(events, source)
        row = {"nudge": context_of(nudge)["stepExecutionId"], "nudgeTime": nudge["eventTime"],
               "nudgeWorker": worker_of(nudge), "fromWait": source}
        if wait is not None:
            described = describe_wait(wait)
            row.update(waitEventId=described["eventId"], waitWorker=described["workerId"],
                       waitTimerSeconds=described["timerSeconds"],
                       intervalSeconds=round(parse_time(nudge["eventTime"]) - parse_time(wait["eventTime"]), 1))
        rows.append(row)
    print_json({"nudges": rows})
    if len(rows) < len(arguments.expect):
        return fail(f"{len(rows)} nudges; expected {len(arguments.expect)}")
    for row, expected, wait_worker in zip(rows, arguments.expect, arguments.wait_workers):
        interval = row.get("intervalSeconds")
        if interval is None or not expected <= interval <= expected + arguments.tolerance_seconds:
            return fail(f"{row['nudge']}: interval {interval}s, expected {expected}s to "
                        f"{expected + arguments.tolerance_seconds}s")
        if row["waitTimerSeconds"] != [expected]:
            return fail(f"{row['nudge']}: Timer condition {row['waitTimerSeconds']}, expected [{expected}]")
        if row["waitWorker"] != wait_worker:
            return fail(f"{row['nudge']}: wait registered by {row['waitWorker']}, expected {wait_worker}")
    print_json({"check": "passed"})
    return 0


# --- Failure observation --------------------------------------------------------


def count_matches(path: Path, needles: tuple[str, ...], flow_id: str) -> dict:
    if not path.exists():
        return {"file": path.name, "result": "log missing"}
    matches = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if any(needle in line for needle in needles) and (not flow_id or flow_id in line):
            matches += 1
    return {"file": path.name, "result": "matched" if matches else "no match", "lines": matches}


def command_failures(arguments: argparse.Namespace) -> int:
    events = read_history(arguments.history)
    failed_types: dict[str, int] = {}
    for event in events:
        event_type = event.get("type", "")
        if "Fail" in event_type or "RESOURCE_EXHAUSTED" in json.dumps(event):
            failed_types[event_type] = failed_types.get(event_type, 0) + 1
    ids = [int(e["eventId"]) for e in events]
    print_json({
        "history": {
            "scope": "retained history window only; a failed attempt that was retried may not appear",
            "historyEventsRetained": len(events),
            "eventIdRange": [min(ids), max(ids)] if ids else None,
            "failedOrResourceExhaustedEvents": failed_types or "no match",
        },
        "serverLog": count_matches(Path(arguments.server_log), ("RESOURCE_EXHAUSTED",), arguments.flow_id),
        "workerLog": count_matches(Path(arguments.worker_log), ("Traceback", " ERROR "), ""),
    })
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    outbox = commands.add_parser("outbox")
    outbox.add_argument("--outbox", required=True)
    outbox.add_argument("--conversation", required=True)
    outbox.add_argument("--run-id", default="")
    outbox.add_argument("--kind", default="")
    outbox.add_argument("--wait-count", type=int, default=0)
    outbox.add_argument("--timeout-seconds", type=float, default=0)

    timeline = commands.add_parser("d1-timeline")
    timeline.add_argument("--outbox", required=True)
    timeline.add_argument("--conversation", required=True)
    timeline.add_argument("--run-id", required=True)
    timeline.add_argument("--started-at", type=float, required=True, help="epoch seconds")
    timeline.add_argument("--follow-up-at", type=float, required=True, help="epoch seconds")
    timeline.add_argument("--page-json", required=True)
    timeline.add_argument("--idle-seconds", type=float, required=True)
    timeline.add_argument("--tolerance-seconds", type=float, default=DEFAULT_TOLERANCE_SECONDS)

    await_wait = commands.add_parser("await-wait")
    await_wait.add_argument("--flow-id", required=True)
    await_wait.add_argument("--step-execution", required=True)
    await_wait.add_argument("--server", required=True)
    await_wait.add_argument("--timeout-seconds", type=float, default=30)
    await_wait.add_argument("--history-out", required=True)

    after_deploy = commands.add_parser("check-after-deploy")
    after_deploy.add_argument("--history", required=True)
    after_deploy.add_argument("--wait-event", type=int, required=True)
    after_deploy.add_argument("--new-worker", required=True)
    after_deploy.add_argument("--expect-steps", type=lambda v: v.split(","), required=True)

    start = commands.add_parser("check-start")
    start.add_argument("--history", required=True)
    start.add_argument("--start-step", required=True)
    start.add_argument("--worker", required=True)

    intervals = commands.add_parser("nudge-intervals")
    intervals.add_argument("--history", required=True)
    intervals.add_argument("--expect", type=lambda v: [float(x) for x in v.split(",")], required=True)
    intervals.add_argument("--wait-workers", type=lambda v: v.split(","), required=True)
    intervals.add_argument("--tolerance-seconds", type=float, default=DEFAULT_TOLERANCE_SECONDS)

    failures = commands.add_parser("failures")
    failures.add_argument("--history", required=True)
    failures.add_argument("--server-log", required=True)
    failures.add_argument("--worker-log", required=True)
    failures.add_argument("--flow-id", default="")

    arguments = parser.parse_args()
    handlers = {
        "outbox": command_outbox,
        "d1-timeline": command_d1_timeline,
        "await-wait": command_await_wait,
        "check-after-deploy": command_check_after_deploy,
        "check-start": command_check_start,
        "nudge-intervals": command_nudge_intervals,
        "failures": command_failures,
    }
    return handlers[arguments.command](arguments)


if __name__ == "__main__":
    sys.exit(main())
