"""Deterministic checks that a customer message accepted before a conversation
completes is answered and reopens it, in every workflow variant.

Usage: uv run --locked check_close_race.py [scenario ...]
Needs a Temporal server at TEMPORAL_ADDRESS (default localhost:7233).

Each check runs its own in-process worker on its own task queue. To put a
timer and a message in the same workflow task, it stops the worker, waits
for the timer to fire on the server, sends the message, and starts the
worker again. Workflow caching is off, so every task also replays history.

  idle-timer      the idle timer and a message share one workflow task:
                  the message is answered first, the nudge comes later.
  close-timer     the close timer and a message share one workflow task
                  after a nudge: the conversation reopens instead of closing.
  close-activity  a message arrives while post_close_summary runs: it is
                  answered after the summary and the conversation reopens.
  idle-suggestion only Updates grow the history of an idle conversation:
                  the inline transcript variant continues as new while idle
                  and keeps its state.

Exit code 0 when every check passes, 2 when one fails.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import activity
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker

import conversation_workflow
import conversation_workflow_v2
import conversation_workflow_v3
from activities import analyze_sentiment, generate_reply
from conversation_shared import ConversationInput, OutboxRequest
from conversation_workflow_inline_transcript import (
    ConversationWorkflowContinueAsNewInlineTranscript,
)
from naive_conversation_workflow import NaiveConversationWorkflow

WORKFLOW_CLASS_BY_VARIANT: dict[str, Any] = {
    "v1": conversation_workflow.ConversationWorkflow,
    "v2": conversation_workflow_v2.ConversationWorkflow,
    "v3": conversation_workflow_v3.ConversationWorkflow,
    "inline-transcript": ConversationWorkflowContinueAsNewInlineTranscript,
    "naive": NaiveConversationWorkflow,
}
VARIANTS_BY_SCENARIO = {
    # The naive variant's idle timer ignores new messages by design (T1).
    "idle-timer": ["v1", "v2", "v3", "inline-transcript"],
    "close-timer": ["v1", "v2", "v3", "inline-transcript", "naive"],
    "close-activity": ["v1", "v2", "v3", "inline-transcript", "naive"],
    "idle-suggestion": ["inline-transcript"],
}
IDLE_SECONDS = 4
CLOSE_AFTER_SECONDS = 4
WAIT_TIMEOUT_SECONDS = 60
SUGGESTION_UPDATE_LIMIT = 3000


@dataclass
class Scenario:
    name: str
    variant: str
    client: Client
    task_queue: str
    workflow_id: str
    close_calls: int = 0

    def handle(self) -> WorkflowHandle:
        return self.client.get_workflow_handle(self.workflow_id)

    def build_worker(self, cache_workflows: bool = False) -> Worker:
        scenario = self

        @activity.defn(name="send_nudge_email")
        async def send_nudge_email(request: OutboxRequest) -> None:
            return None

        @activity.defn(name="post_close_summary")
        async def post_close_summary(request: OutboxRequest) -> None:
            scenario.close_calls += 1
            if scenario.name == "close-activity" and scenario.close_calls == 1:
                # The message is accepted while the summary activity is still running.
                await scenario.handle().signal("post_message", "One more question, please.")
                await wait_for_history(
                    scenario.handle(),
                    lambda events: count_signals(events) >= 1,
                    "the signal sent during the close summary",
                )

        return Worker(
            self.client,
            task_queue=self.task_queue,
            workflows=[WORKFLOW_CLASS_BY_VARIANT[self.variant]],
            activities=[generate_reply, analyze_sentiment, send_nudge_email, post_close_summary],
            max_cached_workflows=1000 if cache_workflows else 0,
        )


def activity_types(events: list[HistoryEvent], outcome_field: str = "") -> list[str]:
    """Returns activity types in schedule order; with outcome_field, only those with that outcome."""
    scheduled = {
        event.event_id: event.activity_task_scheduled_event_attributes.activity_type.name
        for event in events
        if event.HasField("activity_task_scheduled_event_attributes")
    }
    if not outcome_field:
        return list(scheduled.values())
    return [
        scheduled[getattr(event, outcome_field).scheduled_event_id]
        for event in events
        if event.HasField(outcome_field)
    ]


def completed_types(events: list[HistoryEvent]) -> list[str]:
    return activity_types(events, "activity_task_completed_event_attributes")


def count_signals(events: list[HistoryEvent]) -> int:
    return sum(1 for event in events if event.HasField("workflow_execution_signaled_event_attributes"))


async def wait_for_history(
    handle: WorkflowHandle,
    predicate: Callable[[list[HistoryEvent]], bool],
    description: str,
) -> list[HistoryEvent]:
    deadline = asyncio.get_running_loop().time() + WAIT_TIMEOUT_SECONDS
    while True:
        events = list((await handle.fetch_history()).events)
        if predicate(events):
            return events
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"timed out waiting for {description}")
        await asyncio.sleep(0.2)


def last_index(events: list[HistoryEvent], field_name: str) -> int:
    indexes = [i for i, event in enumerate(events) if event.HasField(field_name)]
    return indexes[-1] if indexes else -1


def timer_started_after(events: list[HistoryEvent], activity_type: str, occurrence: int) -> bool:
    """True when a timer started after the given completion of activity_type."""
    completions = [
        i
        for i, event in enumerate(events)
        if event.HasField("activity_task_completed_event_attributes")
        and activity_types(events[: i + 1], "activity_task_completed_event_attributes")[-1]
        == activity_type
    ]
    if len(completions) < occurrence:
        return False
    return any(
        event.HasField("timer_started_event_attributes")
        for event in events[completions[occurrence - 1] :]
    )


def timer_and_signal_share_a_task(events: list[HistoryEvent]) -> bool:
    """True when the last TimerFired and a signal both precede the same WorkflowTaskStarted."""
    fired = last_index(events, "timer_fired_event_attributes")
    for event in events[fired + 1 :]:
        if event.HasField("workflow_execution_signaled_event_attributes"):
            return True
        if event.HasField("workflow_task_started_event_attributes"):
            return False
    return False


async def start_conversation(scenario: Scenario, idle_seconds: int) -> None:
    await scenario.client.start_workflow(
        WORKFLOW_CLASS_BY_VARIANT[scenario.variant].run,
        ConversationInput(
            scenario.workflow_id, "Hi, my order has not arrived.", idle_seconds, CLOSE_AFTER_SECONDS
        ),
        id=scenario.workflow_id,
        task_queue=scenario.task_queue,
    )


async def put_timer_and_message_in_one_task(
    scenario: Scenario, worker: Worker, timer_ready: Callable[[list[HistoryEvent]], bool]
) -> Worker:
    """Stops the worker once timer_ready holds, lets the timer fire, then signals."""
    worker_task = asyncio.create_task(worker.run())
    await wait_for_history(scenario.handle(), timer_ready, "the timer to start")
    await worker.shutdown()
    await worker_task
    await wait_for_history(
        scenario.handle(),
        lambda events: last_index(events, "timer_fired_event_attributes")
        > last_index(events, "timer_started_event_attributes"),
        "the timer to fire without a worker",
    )
    await scenario.handle().signal("post_message", "Any update on the delivery?")
    return scenario.build_worker()


async def run_until_completed(scenario: Scenario, worker: Worker) -> list[HistoryEvent]:
    async with worker:
        await asyncio.wait_for(scenario.handle().result(), WAIT_TIMEOUT_SECONDS)
    return list((await scenario.handle().fetch_history()).events)


async def check_idle_timer(scenario: Scenario) -> list[str]:
    await start_conversation(scenario, IDLE_SECONDS)
    worker = await put_timer_and_message_in_one_task(
        scenario,
        scenario.build_worker(),
        lambda events: timer_started_after(events, "generate_reply", 1),
    )
    shared = timer_and_signal_share_a_task(list((await scenario.handle().fetch_history()).events))
    events = await run_until_completed(scenario, worker)
    problems = []
    if not shared:
        problems.append("setup: the timer and the message did not share a workflow task")
    scheduled = activity_types(events)
    if scheduled[:2] != ["generate_reply", "generate_reply"] and scheduled[:4] != [
        "analyze_sentiment",
        "generate_reply",
        "analyze_sentiment",
        "generate_reply",
    ]:
        problems.append(f"expected the second reply before any nudge, got {scheduled}")
    expect_counts(problems, completed_types(events), replies=2, nudges=1, closes=1)
    return problems


async def check_close_timer(scenario: Scenario) -> list[str]:
    await start_conversation(scenario, IDLE_SECONDS)
    worker = await put_timer_and_message_in_one_task(
        scenario,
        scenario.build_worker(),
        lambda events: timer_started_after(events, "send_nudge_email", 1),
    )
    shared = timer_and_signal_share_a_task(list((await scenario.handle().fetch_history()).events))
    events = await run_until_completed(scenario, worker)
    problems = []
    if not shared:
        problems.append("setup: the timer and the message did not share a workflow task")
    completed = completed_types(events)
    expect_counts(problems, completed, replies=2, nudges=2, closes=1)
    if "post_close_summary" in completed and completed.index("post_close_summary") < len(completed) - 1:
        problems.append(f"the close summary was not the last activity: {completed}")
    return problems


async def check_close_activity(scenario: Scenario) -> list[str]:
    await start_conversation(scenario, IDLE_SECONDS)
    events = await run_until_completed(scenario, scenario.build_worker())
    problems = []
    completed = completed_types(events)
    expect_counts(problems, completed, replies=2, nudges=2, closes=2)
    first_close = completed.index("post_close_summary") if "post_close_summary" in completed else -1
    if first_close < 0 or "generate_reply" not in completed[first_close:]:
        problems.append(f"expected a reply after the first close summary, got {completed}")
    return problems


async def check_idle_suggestion(scenario: Scenario) -> list[str]:
    await start_conversation(scenario, 3600)
    worker = scenario.build_worker(cache_workflows=True)
    async with worker:
        handle = scenario.handle()
        await wait_for_history(
            handle,
            lambda events: timer_started_after(events, "generate_reply", 1),
            "the first reply and the idle timer",
        )
        first_run_id = (await handle.describe()).run_id
        summary_before = await handle.query("get_summary")
        updates = 0
        while (await handle.describe()).run_id == first_run_id:
            if updates >= SUGGESTION_UPDATE_LIMIT:
                return [f"no Continue-As-New after {updates} updates"]
            updates += 1
            await handle.execute_update("assign_agent", f"agent-{updates}")
        await wait_for_history(
            handle,
            lambda events: any(e.HasField("workflow_task_completed_event_attributes") for e in events),
            "the first task of the continued run",
        )
        summary_after = await handle.query("get_summary")
        run_number = await handle.query("get_run_number")
    problems = []
    print(f"    continued as new while idle after {updates} updates; now run {run_number}")
    if run_number != 2:
        problems.append(f"expected run 2, got {run_number}")
    if summary_after["message_count"] != summary_before["message_count"]:
        problems.append(f"transcript changed: {summary_before} -> {summary_after}")
    if summary_after["status"] != summary_before["status"]:
        problems.append(f"status changed: {summary_before['status']} -> {summary_after['status']}")
    if summary_after["agent"] not in (f"agent-{updates}", f"agent-{updates - 1}"):
        problems.append(f"agent not carried: {summary_after['agent']}")
    return problems


def expect_counts(problems: list[str], completed: list[str], replies: int, nudges: int, closes: int) -> None:
    for activity_type, expected in (
        ("generate_reply", replies),
        ("send_nudge_email", nudges),
        ("post_close_summary", closes),
    ):
        actual = completed.count(activity_type)
        if actual != expected:
            problems.append(f"{actual} completed {activity_type}, expected {expected}")


CHECK_BY_SCENARIO = {
    "idle-timer": check_idle_timer,
    "close-timer": check_close_timer,
    "close-activity": check_close_activity,
    "idle-suggestion": check_idle_suggestion,
}


async def main() -> int:
    scenarios = sys.argv[1:] or list(CHECK_BY_SCENARIO)
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"), identity="conversation-demo-check"
    )
    failures = 0
    for scenario_name in scenarios:
        for variant in VARIANTS_BY_SCENARIO[scenario_name]:
            suffix = uuid.uuid4().hex[:8]
            scenario = Scenario(
                scenario_name,
                variant,
                client,
                f"close-race-{scenario_name}-{variant}-{suffix}",
                f"close-race-{scenario_name}-{variant}-{suffix}",
            )
            try:
                problems = await CHECK_BY_SCENARIO[scenario_name](scenario)
            except Exception as error:
                problems = [f"{type(error).__name__}: {error}"]
            verdict = "PASS" if not problems else "FAIL"
            print(f"{verdict} {scenario_name:15s} {variant:17s} {scenario.workflow_id}", flush=True)
            for problem in problems:
                print(f"    {problem}", flush=True)
            failures += bool(problems)
    print(f"{failures} failed" if failures else "all checks passed")
    return 2 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
