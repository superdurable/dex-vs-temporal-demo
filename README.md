# dex-vs-temporal-demo

The same small application written twice: once on [Temporal](https://temporal.io) and once on [Dex](https://docs.superdurable.io). Every number in this file comes from a script in this repository. Run the scripts to check them.

## Video

[![Dex vs Temporal, Ep. 1: one idle timer, three production problems](https://img.youtube.com/vi/fzl88JWt734/maxresdefault.jpg)](https://youtu.be/fzl88JWt734)

[Dex vs Temporal, Ep. 1: one idle timer, three production problems](https://youtu.be/fzl88JWt734) walks through this repository. Each part of the video uses these demos:

| Video | Demo in this file | Scripts |
| --- | --- | --- |
| [3:55](https://youtu.be/fzl88JWt734?t=235) The natural way in Temporal, a real run, and why | 1. The idle timer | `scripts/temporal/t1-naive-timer.sh` |
| [8:06](https://youtu.be/fzl88JWt734?t=486) The correct way in Temporal | 1. The idle timer | `scripts/temporal/t1-naive-timer.sh` |
| [9:28](https://youtu.be/fzl88JWt734?t=568) The Dex way | 1. The idle timer | `scripts/dex/d1-timer.sh` |
| [10:54](https://youtu.be/fzl88JWt734?t=654) Changing the code | 2. Changing code while conversations are open | `scripts/temporal/t2-change-code.sh`, `scripts/temporal/t2-patch-fix.sh`, `scripts/dex/d2-change-code.sh` |
| [15:02](https://youtu.be/fzl88JWt734?t=902) The history limit and Continue-As-New | 3. A conversation that keeps going | `scripts/temporal/t5-flood.sh`, `scripts/temporal/t6-continue-as-new.sh`, `scripts/temporal/t8-context-in-activity.sh`, `scripts/dex/d5-flood-fair.sh`, `scripts/dex/d7-transcript-budget.sh` |
| [20:24](https://youtu.be/fzl88JWt734?t=1224) The complete code and the Flow design | 4. The Flow design | `scripts/dex/d-design.sh` |

## The application

An AI support conversation. One workflow (Temporal) or one Flow (Dex) per conversation.

- The conversation starts with the customer's first message.
- Each customer message gets one AI reply. The reply stands for a model call that uses internal tools. Here it is a deterministic local mock, with no network and no real model.
- After a reply, a 10-minute idle timer starts. A follow-up message is answered, and the timer starts again after that reply.
- If the customer sends nothing for 10 minutes, the application sends a nudge email. One hour after the nudge with no message, the conversation closes.
- "Sending email" appends a line to a local outbox file. No provider is called.

The demos use 30 seconds in place of 10 minutes.

## Layout

| Path | Content |
| --- | --- |
| `temporal/` | Python workflows, activities, Worker, and the `drive.py` command line |
| `dex/` | Python Flow, Worker, and the `drive.py` command line |
| `scripts/temporal/`, `scripts/dex/` | One script per demo |

Both `drive.py` tools take the same commands: `start`, `post`, `assign`, `page`, `status`, `flood`.

## Versions used for the numbers below

| Component | Version |
| --- | --- |
| Temporal CLI and dev server | CLI 1.9.1, Server 1.32.0, default limits, no dynamic config |
| Temporal Python SDK | `temporalio` 1.36.1 |
| Dex CLI and dev server | `dexcli` v1.47.20 |
| Dex Python SDK | `dex-python-sdk` 1.45.17 |
| Python | 3.14 (Temporal side), 3.13 (Dex side), managed by `uv` |

All runs were made on one laptop on 2026-11-23. Absolute times depend on the machine. Counts that come from engine limits do not.

## Demos

### 1. The idle timer

| Script | What it shows |
| --- | --- |
| `scripts/temporal/t1-naive-timer.sh` | A plausible first implementation, then the correct one |
| `scripts/dex/d1-timer.sh` | The Dex Flow |

Timeline: first message at t = 0, a follow-up at t = 20 s, idle window 30 s. The nudge is due 30 s after the second reply.

| Implementation | Nudge sent at | Errors |
| --- | --- | --- |
| Temporal, `NaiveConversationWorkflow` | 30.0 s | none |
| Temporal, `ConversationWorkflow` | 50.0 s | none |
| Dex, `SupportConversationFlow` | 50.4 s | none |

The naive workflow computes the wait once and sleeps. The signal handler updates a field, and nothing re-arms the timer. It runs without any error. The correct workflow follows the technique of the official [updatable timer sample](https://github.com/temporalio/samples-python/tree/main/updatable_timer): a loop around `workflow.wait_condition` with a timeout.

### 2. Changing code while conversations are open

New requirement: analyze the sentiment of each customer message before replying.

| Script | What it shows |
| --- | --- |
| `scripts/temporal/t2-change-code.sh` | Deploy the change with no versioning |
| `scripts/temporal/t2-patch-fix.sh` | Deploy it again behind `workflow.patched` |
| `scripts/dex/d2-change-code.sh` | Deploy a new Step |
| `scripts/dex/d2-idle-window-variation.sh` | What an already registered wait does after a deploy |

Temporal:

- Conversation A started on v1. After the v2 deploy, its next message is accepted and never answered. The workflow task fails and is retried: `[TMPRL1100] Nondeterminism error: Activity type of scheduled event 'generate_reply' does not match activity type of activity command 'analyze_sentiment'`. Queries fail too.
- A conversation started on v2 works.
- With `workflow.patched` (v3), A recovers without a reset. A never runs the sentiment activity, because its history has no patch marker.
- Conversation B started on the unguarded v2. On v3 it fails with the mirrored non-determinism error. This is the extra incident a team gets when it ships the unguarded change first.

Dex:

- Conversation A was waiting in `WaitForCustomerStep`, registered by the v1 Worker. After the v2 deploy, its next message is handled by the v2 Worker and goes through the new `AnalyzeSentimentStep`, then replies.
- A conversation started on v2 begins at `AnalyzeSentimentStep`.
- A wait that was registered before the deploy keeps its Timer. In the variation, the old 60 s Timer fires at 60.1 s; the next wait uses the new 10 s window.

### 3. A conversation that keeps going

Load: follow-up messages at 8 per second, one AI reply per message, about 200 bytes per message and per reply. `t8-context-in-activity.sh` also runs with 8 KiB messages at 2 per second.

| Script | What it shows |
| --- | --- |
| `scripts/temporal/t5-flood.sh` | The correct workflow, no Continue-As-New |
| `scripts/temporal/t6-continue-as-new.sh` | Continue-As-New that carries the whole transcript |
| `scripts/temporal/t8-context-in-activity.sh` | The correct workflow, with the conversation context passed to each reply activity |
| `scripts/dex/d5-flood-fair.sh` | The Dex Flow for 30 minutes |
| `scripts/dex/d7-transcript-budget.sh` | Dex only: the transcript size budget with large messages |

Temporal, no Continue-As-New (`t5-flood.sh`):

- 4,266 follow-up messages accepted and answered in 533 s. The 4,267th is rejected: `workflow execution already completed`.
- The server terminated the workflow at 51,201 history events: `Workflow history count exceeds limit.` History size was 9 MB, far below the 50 MB limit.
- Each customer message adds 12 history events: 1 signal, 2 workflow tasks (3 events each), 1 activity (3 events), 1 timer canceled, 1 timer started.
- The reply is a regular activity, because it stands for a model call. A local activity would add about 7 events per message. That design would last about 7,300 messages before the same limit. We did not measure it.

Temporal, Continue-As-New with the transcript carried inline (`t6-continue-as-new.sh`):

- The workflow chains runs at the server's suggestion, about 4,100 events per run. Its file has 249 lines; the correct workflow has 148.
- After 3,531 answered follow-up messages, the next run's input was 2,199,782 bytes. The limit is 2,097,152. The workflow task fails: `[TMPRL1103] Attempted to upload payloads with size that exceeded the error limit.` The workflow stays Running, keeps accepting signals, and stops replying.
- The stop point moves with the timing of each Continue-As-New.
- With the SDK's External Storage (Public Preview) and a local directory as the store, 10,000 messages were each answered once. The stored objects stayed after the workflow completed: Temporal does not delete them.

Temporal, the conversation context passed to the reply activity (`t8-context-in-activity.sh`):

- In the workflows above, `generate_reply` receives only the current customer message, because the reply is a mock. A real model call needs the conversation. `ConversationWithContextWorkflow` is `ConversationWorkflow` with one change: each reply activity, `generate_reply_from_context`, receives the transcript as a regular activity input, with the default JSON encoding. It returns the same mock reply. No Continue-As-New, no External Storage.
- The input holds every transcript record appended when the activity is scheduled: customer messages and AI replies. Under a backlog, that includes customer messages queued behind the one being answered. The text of the message being answered appears once. The workflow adds no history events compared with `t5-flood.sh`.
- Temporal records an activity's input in its `ActivityTaskScheduled` event. Each reply copies the transcript into history again, so history grows with the square of the message count.
- 200-byte messages at 8 per second, five runs: 408 follow-up messages accepted, 407 answered in 51 s. The server terminated the workflow at 4,907 events and 52,602,112 to 52,602,325 history bytes: `Workflow history size exceeds limit.` The 409th message is rejected: `workflow execution already completed`. The input of the last scheduled reply activity, which did not complete, was 253,509 bytes, with 817 transcript records.
- 8 KiB messages at 2 per second, three runs: 109 follow-up messages accepted, 108 answered in 54 s. The same termination at 1,319 events and 52,719,073 to 52,719,369 bytes. The input of the last scheduled reply activity, which did not complete, was 938,958 bytes. A run at 8 per second stopped at the same message, at 52,719,405 bytes.
- The 50 MiB history size limit is reached first in both. The 2 MiB payload limit is not reached.
- History passes 10 MiB after 181 follow-up messages (200 bytes) and after 48 (8 KiB). These counts come from the cumulative serialized size of the history events; the server logs `history size exceeds warn limit.` when it passes 10 MiB. With 8 KiB messages, the worker logs the payload warning from follow-up message 61 on: `[TMPRL1103] Attempted to upload payloads with size that exceeded the warning limit. size=525534 limit=524288`. With 200-byte messages, no input reaches 512 KiB.
- No workflow task failed, and the worker logged no error before the termination.
- These numbers hold for a regular activity that receives the full transcript on each call. Other designs were not measured. A local activity records only its result, in a marker event, so the repeated input is gone; its execution semantics differ from a regular activity. Passing only the new messages needs the conversation kept in storage outside the workflow. Temporal's guidance for large inputs is the claim-check pattern, which External Storage implements.

Dex (`d5-flood-fair.sh`):

- 14,138 follow-up messages sent in 30 minutes, all answered, backlog 0 at the end. The Flow is still running.
- 262 of the 14,400 planned slots were dropped by the load driver's back-pressure during a slow period of about three minutes. The cause of that period was not found.
- At 4,266 accepted messages, 4,260 were answered.
- Each follow-up message runs three Step methods and one RPC, about 4 history events. Dex keeps a window of the newest events and prunes older ones: 5,056 retained events at the end, highest event ID 56,555.

Dex transcript budget (`d7-transcript-budget.sh`, Dex only):

- The Flow keeps the transcript in a chunked AttributeMap within 32 MiB of the run's 64 MiB state budget. When an append does not fit, a Step folds the oldest chunks into a summary and deletes them.
- With 8 KiB messages, the transcript reached 33.2 MB and stayed under the budget; 64 chunks went into the summary. The Flow kept running.

### 4. The Flow design

`scripts/dex/d-design.sh` renders the Flow definition from the source with `dexcli visualize` and opens it in Dex Web.

## Load driver rules

Both `flood` commands post message *i* at `start + i / rate`. The Dex driver also applies back-pressure, so the pending Channel cannot fill the run's state budget:

- at most 64 RPCs in flight;
- at most 1,000 acknowledged messages without a reply, or 16 MiB of them;
- a slot that is later than one interval is dropped and counted.

The Temporal runs above never built a backlog at 8 messages per second, so these limits would not have changed their results.

## Scope

- Both servers ran in their local development mode.
- The reply and the sentiment analysis are local mocks, not a real model.

## Run it

Requirements: `uv`, the `temporal` CLI, and `dexcli`.

```bash
scripts/temporal/t1-naive-timer.sh
scripts/dex/d1-timer.sh
```

Each script starts its own server on its own ports, prints its results, and stops what it started. Set `KEEP=1` to leave the server and the Web UI running. Scripts write logs and state under a run directory that they print.
