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
| `flow_v2_no_patch.py` | v2 without `workflow.patched`, used only by `upgrade.sh v2_no_patch` | none |
| `settings.py` | `AUTO_SEND_AFTER` from `AGENT_REPLY_AUTO_SEND_SECONDS` (default 300) | same |
| `worker.py`, `client.py` | worker and command-line client; `AGENT_REPLY_CODE` picks the code | same, plus `app.py` for wiring |
| `run.sh` | v1, then v2 approved and v2 left to the timer | same |
| `upgrade.sh` | replies started on v1 continue on a new v2 worker | same |
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
agent-reply/dex/run.sh                         # dexcli dev on port 18911
agent-reply/dex/upgrade.sh
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

## Skipping the Timer with an operator call

Dex also has an operator call, `skip_timer`, that makes a waiting Timer fire now. `skip-timer.sh` starts a v2 reply with a 5-minute Timer and calls `skip_timer.py`. The reply was sent 1 s after it started.
