# agent-reply

An AI agent reply with human approval, written once on Temporal and once on Dex.

v1:

1. The agent drafts a reply. Here it is a deterministic mock with no network.
2. Wait until a human approves the draft. This wait can last a long time.
3. Send the reply.

v2 changes the wait: if nobody approves within 5 minutes (`AUTO_SEND_AFTER`), send the reply anyway.

## Files

| File | Temporal (`temporal/`) | Dex (`dex/`) |
| --- | --- | --- |
| `flow_v1.py` | `ReplyWorkflow` with activities `draft_reply` and `send_reply`, the `approve_reply` signal, and `workflow.wait_condition(lambda: self.approved)` | `ReplyFlow` with `DraftReplyStep` and `SendReplyStep`, an `approve` Channel that the `approve_reply` RPC publishes to, and `Wait.until(APPROVE.for_one())` |
| `flow_v2.py` | adds `timeout=AUTO_SEND_AFTER` to the wait, behind `workflow.patched("auto-send")` | waits on `Wait.any_of(APPROVE.for_one(), Timer.by_duration(AUTO_SEND_AFTER))` |
| `flow_v2_no_patch.py` | v2 without `workflow.patched`, used only by the upgrade scripts | none |
| `settings.py` | `AUTO_SEND_AFTER` from `AGENT_REPLY_AUTO_SEND_SECONDS` (default 300); test-only `SEND_REPLY_SECONDS` and `SEND_REPLY_FAILED_ATTEMPTS` | same |
| `worker.py`, `client.py` | worker and command-line client; `AGENT_REPLY_CODE` picks the code. With the test-only settings, `worker.py` registers a slow or failing `send_reply` under the same name | same, plus `app.py` for wiring; with the test-only settings, `app.py` makes `SendReplyStep.execute` slow or failing |
| `run.sh` | v1, then v2 approved and v2 left to the timer | same |
| `upgrade.sh` | replies started on v1 continue on a new v2 worker | same |
| `upgrade-in-send.sh` | replies approved under v1 are in `send_reply` when the v2 worker takes over | same, for `SendReplyStep` |
| `query-completed.sh`, `read-completed.sh` | `query-completed.sh`: replies completed under v1 are queried through a v2 worker | `read-completed.sh`: replies completed under v1 are read with the v2 Worker running and with no Worker |
| other | `replay_check.py` replays a saved history against other code, offline | `skip-timer.sh` and `skip_timer.py`: see the last section |

Lines of code, without blank or comment lines:

| | Temporal | Dex |
| --- | --- | --- |
| `flow_v1.py` | 26 | 26 |
| `flow_v2.py` | 36 | 29 |
| v1 to v2 diff, blank lines excluded | 11 lines added, 1 removed | 5 lines added, 2 removed |

## Run it

You need `uv`, the `temporal` CLI, and `dexcli`.

```bash
agent-reply/temporal/run.sh                    # Temporal dev server on port 17233
agent-reply/temporal/upgrade.sh v2_no_patch
agent-reply/temporal/upgrade.sh v2
agent-reply/temporal/upgrade-in-send.sh v2_no_patch slow   # or retry; or v2
agent-reply/temporal/query-completed.sh v2_no_patch        # or v2
agent-reply/dex/run.sh                         # dexcli dev on port 18911
agent-reply/dex/upgrade.sh
agent-reply/dex/upgrade-in-send.sh slow                    # or retry
agent-reply/dex/read-completed.sh
agent-reply/dex/skip-timer.sh
```

Each script starts its own server and workers, and stops them on exit. Logs, histories, and Python environments go to `/tmp/dex-video/two-step/`. The scripts use a 10-second `AUTO_SEND_AFTER`; set `AGENT_REPLY_AUTO_SEND_SECONDS` to change it.

## Results

Measured on 2026-11-24 with Temporal CLI 1.9.1 (Server 1.32.0), `temporalio` 1.36.1, `dexcli` v1.48.0 (Dex Server 0.1.37), `dex-python-sdk` 1.45.17, and Python 3.14.7.

### v1 and v2 (`run.sh`)

| Run | Temporal | Dex |
| --- | --- | --- |
| v1, approved after 2 s | `sent: draft reply for ...` after 3 s | same result after 2 s |
| v2, approved after 2 s | finished after 3 s | finished after 3 s |
| v2, not approved | finished after 10 s, when the timer fired | finished after 10 s, when the Timer fired |

### Replies started on v1, continued on v2 (`upgrade.sh`)

Each script starts replies on the v1 worker and waits until they wait for approval. It stops the v1 worker and starts a new worker process with v2, so the new worker has no cached workflow state.

Temporal, v2 without `workflow.patched` (`upgrade.sh v2_no_patch`):

| Reply | What happened |
| --- | --- |
| two old replies, approved after the switch | completed. No error. |
| old reply with no new event | still running after the timer plus 10 s. Its history has no timer. |
| old reply queried with `temporal workflow stack` | the query answered. The reply is still running. Its history has no timer. |
| old reply sent a signal that has no handler | the v2 worker started a new 10-second timer when it handled that signal (`TimerStarted`, event 15). The reply was sent 10 s later. |
| new replies | one completed on approval, one when its timer fired |

No workflow task failed. The v2 worker logged no error. `replay_check.py` replays every saved old history against `flow_v2_no_patch.py` with no error.

Temporal, v2 with `workflow.patched("auto-send")` (`upgrade.sh v2`):

| Reply | What happened |
| --- | --- |
| two old replies, approved after the switch | completed |
| old reply with no new event, and the queried one | still running. No timer. |
| old reply sent a signal that has no handler | still running. No timer: old replies keep the v1 wait. |
| new replies | one completed on approval, one when its timer fired. The history has `MarkerRecorded` before `TimerStarted`. |

`flow_v2.py` uses `workflow.patched` because the Temporal Python SDK documents this change as nondeterministic. The `workflow.wait_condition` docstring in `temporalio` 1.36.1 says: "changing from `None` to a positive value (or the inverse) constitutes a nondeterministic change to workflow code." The [Python versioning guide](https://docs.temporal.io/develop/python/workflows/versioning) says: "If you make a change to your Workflow code that would cause non-deterministic behavior on Replay, you'll need to use one of our Versioning methods to gracefully update your running Workflows."

Dex (`upgrade.sh`):

| Reply | What happened |
| --- | --- |
| two old replies, approved after the switch | completed on the v2 Worker |
| two old replies with no approval | still running after the Timer plus 10 s. Their wait was registered under v1 and has no Timer. |
| new replies | one completed on approval, one when its Timer fired |

`flow_v2.py` has no versioning code.

### Replies already approved under v1, inside the second step when v2 takes over (`upgrade-in-send.sh`)

Each script starts three replies on the v1 worker and approves them under v1. Each reply then runs the second step: `send_reply` on Temporal, `SendReplyStep.execute` on Dex. Test-only settings change that step in two modes. In `slow`, each attempt takes 8 s. In `retry`, the first 3 attempts fail and the default retry policy retries them. While the step runs (`slow`) or retries (`retry`), the script stops the v1 worker with SIGTERM and starts a new v2 worker process.

Temporal, with `flow_v2_no_patch.py` and with `flow_v2.py`, in both modes:

- All 12 replies completed. No workflow task failed, and no worker logged a non-determinism error.
- The history after the approval is the v1 shape: `ActivityTaskScheduled`, then `ActivityTaskStarted` and `ActivityTaskCompleted` by the v2 worker, then `WorkflowTaskCompleted` by the v2 worker and `WorkflowExecutionCompleted`. There is no timer event and no `WorkflowTaskFailed`.
- `slow`: stopping the v1 worker ended attempt 1 of `send_reply` at once. The v2 worker ran attempt 2 from the start, 1 to 2 s later, and it completed 8 s later.
- `retry`: attempts 1 and 2 failed on the v1 worker. The v2 worker ran attempts 3 and 4, and attempt 4 succeeded.

Dex, in both modes:

- All 6 replies completed.
- `slow`: stopping the v1 Worker took 8 s. The v1 Worker finished the running `SendReplyStep.execute` before it exited, and the history shows `StepExecuteCompleted` by `reply-worker-v1`.
- `retry`: attempts 1 and 2 failed on the v1 Worker. The v2 Worker ran attempts 3 and 4, and attempt 4 succeeded. The history shows `StepExecuteCompleted` by `reply-worker-v2`.

### Replies completed under v1, read after v2 takes over

Temporal (`query-completed.sh`): three replies complete under v1. A new v2 worker process then answers `temporal workflow stack` for each, which runs the whole history on the v2 code. With `flow_v2_no_patch.py` and with `flow_v2.py`, every query returned a stack trace, and the worker logged no error. Reading the result (`handle.result()`) returned `sent: draft reply for ...` for each.

Dex (`read-completed.sh`): three replies complete under v1. With the v2 Worker running, and again with no Worker running, `wait_for_flow` returned `COMPLETED ['sent: draft reply for ...']` and `describe_flow` returned `COMPLETED` for each.

## Skipping the Timer with an operator call

Dex also has an operator call, `skip_timer`, that makes a waiting Timer fire now. `skip-timer.sh` starts a v2 reply with a 5-minute Timer and calls `skip_timer.py`. The reply was sent 1 s after it started.
