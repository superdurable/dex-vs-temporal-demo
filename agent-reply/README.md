# agent-reply

An AI agent reply with human approval, written once on Temporal and once on Dex.

v1:

1. The agent drafts a reply. Here it is a deterministic mock with no network.
2. Wait until a human approves the draft. This wait can last a long time.
3. Send the reply.

v2 adds one step: after the draft, notify the reviewer. Then wait for approval and send the reply as before.

## Files

| File | Temporal (`temporal/`) | Dex (`dex/`) |
| --- | --- | --- |
| `flow_v1.py` | `ReplyWorkflow` with activities `draft_reply` and `send_reply`, the `approve_reply` signal, and `workflow.wait_condition(lambda: self.approved)` | `ReplyFlow` with `DraftReplyStep` and `SendReplyStep`, an `approve` Channel that the `approve_reply` RPC publishes to, and `Wait.until(APPROVE.for_one())` |
| `flow_v2.py` | adds the `notify_reviewer` activity after `draft_reply`, behind `workflow.patched("notify-reviewer")` | adds `NotifyReviewerStep` between `DraftReplyStep` and `SendReplyStep` |
| `flow_v2_no_patch.py` | v2 without `workflow.patched`, used only by the upgrade scripts | none |
| `settings.py` | test-only `SEND_REPLY_SECONDS` and `SEND_REPLY_FAILED_ATTEMPTS` | same |
| `worker.py`, `client.py` | worker and command-line client; `AGENT_REPLY_CODE` picks the code. With the test-only settings, `worker.py` registers a slow or failing `send_reply` under the same name | same, plus `app.py` for wiring; with the test-only settings, `app.py` makes `SendReplyStep.execute` slow or failing |
| `run.sh` | one v1 reply and one v2 reply | same |
| `upgrade.sh` | replies waiting for approval under v1 continue on a new v2 worker | same |
| `upgrade-in-send.sh` | replies approved under v1 are in `send_reply` when the v2 worker takes over | same, for `SendReplyStep` |
| `query-completed.sh`, `read-completed.sh` | `query-completed.sh`: replies completed under v1 are queried through a v2 worker | `read-completed.sh`: replies completed under v1 are read with the v2 Worker running and with no Worker |
| other | `replay_check.py` replays a saved history against other code, offline | none |

Lines of code, without blank or comment lines:

| | Temporal | Dex |
| --- | --- | --- |
| `flow_v1.py` | 26 | 26 |
| `flow_v2.py` | 34 | 30 |
| v1 to v2 diff, blank lines excluded | 8 lines added, 0 removed | 6 lines added, 2 removed |

## Run it

You need `uv`, the `temporal` CLI, and `dexcli`.

```bash
agent-reply/temporal/run.sh                                 # Temporal dev server on port 17233
agent-reply/temporal/upgrade.sh v2_no_patch                 # or v2
agent-reply/temporal/upgrade-in-send.sh v2_no_patch retry   # or v2; or slow
agent-reply/temporal/query-completed.sh v2_no_patch         # or v2
agent-reply/dex/run.sh                                      # dexcli dev on port 18911
agent-reply/dex/upgrade.sh
agent-reply/dex/upgrade-in-send.sh retry                    # or slow
agent-reply/dex/read-completed.sh
```

Each script starts its own server and workers, and stops them on exit. Logs, histories, and Python environments go to `/tmp/dex-video/two-step/`.

## Results

Measured on 2026-11-24 with Temporal CLI 1.9.1 (Server 1.32.0), `temporalio` 1.36.1, `dexcli` v1.48.0 (Dex Server 0.1.37), `dex-python-sdk` 1.45.17, and Python 3.14.7.

### v1 and v2 (`run.sh`)

Both sides: a v1 reply and a v2 reply, each approved after 2 s, return `sent: draft reply for ...`. The v2 runs include the new step: `notify_reviewer` on Temporal, `NotifyReviewerStep` on Dex.

### Replies started on v1, continued on v2

Each script starts replies on the v1 worker, stops it with SIGTERM, and starts a new v2 worker process, so the new worker has no cached workflow state.

| Reply state at the switch | Temporal, `flow_v2_no_patch.py` | Temporal, `flow_v2.py` (patched) | Dex, `flow_v2.py` |
| --- | --- | --- | --- |
| waiting for approval, approved after the switch (`upgrade.sh`) | completed. `notify_reviewer` ran after the approval, then `send_reply`. | completed without `notify_reviewer` | completed without `NotifyReviewerStep` |
| waiting for approval, never approved (`upgrade.sh`) | still waiting | still waiting | still waiting |
| approved under v1, `send_reply` failing and retrying (`upgrade-in-send.sh retry`) | stuck: see below | completed. Attempts 3 and 4 ran on v2, and attempt 4 succeeded. | completed. Attempts 3 and 4 ran on v2, and attempt 4 succeeded. |
| completed under v1, then queried or read (`query-completed.sh`, `read-completed.sh`) | `temporal workflow stack` fails: see below. Reading the result works. | `temporal workflow stack` returns the stack. Reading the result works. | `wait_for_flow` returns `COMPLETED ['sent: draft reply for ...']` with the v2 Worker running and with no Worker |
| new replies started on v2 | the approved one completed | the approved one completed | the approved one completed |

Temporal with `flow_v2_no_patch.py`, replies approved under v1 with `send_reply` retrying:

- `send_reply` attempts 3 and 4 ran on the v2 worker, and attempt 4 succeeded (`ActivityTaskCompleted`, event 17). The reply was sent.
- The next workflow task failed on the v2 worker. Event 20 is `WorkflowTaskFailed` with cause `WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR`:

  ```text
  [TMPRL1100] Nondeterminism error: Activity type of scheduled event 'send_reply' does not match activity type of activity command 'notify_reviewer'
  ```

  The scheduled event is event 15, the `ActivityTaskScheduled` of `send_reply` that v1 wrote.
- The workflows stayed `RUNNING`. The history ends with `WorkflowTaskFailed`, `WorkflowTaskScheduled`, `WorkflowTaskStarted`, and nothing more in the 30 s the script waited.

Temporal with `flow_v2_no_patch.py`, replies completed under v1: `temporal workflow stack` fails with

```text
Error: querying workflow failed: [TMPRL1100] Nondeterminism error: Activity type of scheduled event 'send_reply' does not match activity type of activity command 'notify_reviewer'
```

Dex needs no versioning code for this change.
