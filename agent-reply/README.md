# agent-reply

An AI agent reply with human approval, written once on Temporal and once on Dex.

v1:

1. The agent drafts a reply.
2. Wait until a human approves the draft. This wait can last a long time.
3. Send the reply.

v2 adds one step: after the draft, notify the reviewer. Then wait for approval and send the reply as before.

Drafting, notifying, and sending are mocks that return strings. Nothing calls a model, a network, or an email provider.

## Files

| File | Temporal (`temporal/`) | Dex (`dex/`) |
| --- | --- | --- |
| `flow_v1.py` | `ReplyWorkflow` with activities `draft_reply` and `send_reply`, the `approve_reply` signal, and `workflow.wait_condition(lambda: self.approved)` | `ReplyFlow` with `DraftReplyStep` and `SendReplyStep`, an `approve` Channel that the `approve_reply` RPC publishes to, and `Wait.until(APPROVE.for_one())` |
| `flow_v2.py` | adds the `notify_reviewer` activity after `draft_reply`, behind `workflow.patched("notify-reviewer")` | adds `NotifyReviewerStep` between `DraftReplyStep` and `SendReplyStep` |
| `flow_v2_no_patch.py` | v2 without `workflow.patched`. `upgrade.sh`, `upgrade-in-send.sh`, and `query-completed.sh` use it to reproduce the errors below | none |
| `settings.py` | test-only `SEND_REPLY_FAILED_ATTEMPTS` | same |
| `worker.py`, `client.py` | worker and command-line client; `AGENT_REPLY_CODE` picks the code. With the test-only setting, `worker.py` registers a `send_reply` that fails its first attempts, under the same name | same, plus `app.py` for wiring; with the test-only setting, `app.py` makes the first attempts of `SendReplyStep.execute` fail |
| `run.sh` | one v1 reply and one v2 reply | same |
| `upgrade.sh` | replies waiting for approval under v1 continue on a new v2 worker | same |
| `upgrade-in-send.sh` | replies approved under v1 are retrying `send_reply` when the v2 worker takes over | same, for `SendReplyStep` |
| `query-completed.sh`, `read-completed.sh` | `query-completed.sh`: replies completed under v1, queried and read through a v2 worker and read with no worker | `read-completed.sh`: replies completed under v1, read with the v2 Worker running and with no Worker |
| other | `replay_check.py` replays a saved history against other code, offline | none |

Lines in the flow definition files, without blank or comment lines:

| | Temporal | Dex |
| --- | --- | --- |
| `flow_v1.py` | 26 | 26 |
| `flow_v2.py` | 34 | 30 |
| v1 to v2 diff, blank lines excluded | 8 lines added, 0 removed | 6 lines added, 2 removed |

Worker and client setup live in separate files. Temporal sets a 10-second timeout per activity. Dex uses its defaults.

## Run it

You need `uv`, the `temporal` CLI, `dexcli`, and `python3`. `uv` installs the Python each side needs: 3.14 or later for Temporal, 3.11 or later for Dex.

```bash
agent-reply/temporal/run.sh                           # Temporal dev server on port 17233
agent-reply/temporal/upgrade.sh v2_no_patch           # or v2
agent-reply/temporal/upgrade-in-send.sh v2_no_patch   # or v2
agent-reply/temporal/query-completed.sh v2_no_patch   # or v2
agent-reply/dex/run.sh                                # dexcli dev on port 18911
agent-reply/dex/upgrade.sh
agent-reply/dex/upgrade-in-send.sh
agent-reply/dex/read-completed.sh
```

Each script starts its own server and workers, stops them on exit, and checks the outcome described below. It exits 1 when a check fails. Logs, histories, and Python environments go to `/tmp/dex-video/two-step/`. `dexcli dev` also keeps its codec discovery state in its own directory.

Scripts of one side share a run directory and ports. To run two of them at the same time, give each its own `RUN_DIR` and ports: `TEMPORAL_GRPC_PORT` and `TEMPORAL_UI_PORT` on Temporal, `DEX_PORT` and `WEB_PORT` on Dex.

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
| approved under v1, `send_reply` failing and retrying (`upgrade-in-send.sh`) | stuck: see below | completed. Attempts 3 and 4 ran on v2, and attempt 4 succeeded. | completed. Attempts 3 and 4 ran on v2, and attempt 4 succeeded. |
| new replies started on v2 | the approved one completed | the approved one completed | the approved one completed |

Temporal with `flow_v2_no_patch.py`, replies approved under v1 with `send_reply` retrying:

- `send_reply` attempts 3 and 4 ran on the v2 worker, and attempt 4 succeeded (`ActivityTaskCompleted`, event 17). The mock send activity completed.
- The next workflow task failed on the v2 worker. Event 20 is `WorkflowTaskFailed` with cause `WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR`:

  ```text
  [TMPRL1100] Nondeterminism error: Activity type of scheduled event 'send_reply' does not match activity type of activity command 'notify_reviewer'
  ```

  The scheduled event is event 15, the `ActivityTaskScheduled` of `send_reply` that v1 wrote.
- The workflows stayed `RUNNING`. The history ends with `WorkflowTaskFailed`, `WorkflowTaskScheduled`, `WorkflowTaskStarted`, and nothing more in the 30 s the script waited.

### Replies completed under v1, read after v2 takes over

These are two different operations on Temporal.

A query runs on a worker, which first runs the whole history on its code. `query-completed.sh` uses the built-in stack trace query:

```bash
temporal workflow stack -w <workflow-id>
```

With `flow_v2_no_patch.py`, it fails for every reply completed under v1:

```text
Error: querying workflow failed: [TMPRL1100] Nondeterminism error: Activity type of scheduled event 'send_reply' does not match activity type of activity command 'notify_reviewer'
```

With `flow_v2.py`, it returns the stack.

Reading the stored result is served by the server. Temporal can still read the stored result, with either v2 worker running and with no worker running:

```bash
python client.py result <workflow-id>     # Temporal: handle.result()
```

Dex reads the stored result the same way, with the v2 Worker running and with no Worker running. It returns `COMPLETED ['sent: draft reply for ...']`:

```bash
python client.py result <flow-id>         # Dex: client.wait_for_flow()
```

Dex needs no versioning code for this change.
