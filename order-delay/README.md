# order-delay

A small food order, written once on Temporal and once on Dex.

1. Confirm the order to the customer.
2. Wait 1 minute. The customer can still change the order in this window.
3. Send the order to the kitchen.

Then the requirement changes (v2): a "Send now" button ends the window early.

## Files

| File | Temporal (`temporal/`) | Dex (`dex/`) |
| --- | --- | --- |
| `flow_v1.py` | `OrderWorkflow` with activities `confirm_order` and `send_to_kitchen`, and `workflow.sleep` between them | `OrderFlow` with `ConfirmOrderStep` and `SendToKitchenStep`; `SendToKitchenStep` waits on a Timer |
| `flow_v2.py` | adds the `send_order_now` signal and waits with `workflow.wait_condition(..., timeout=DELAY)` | adds a `send-now` Channel, the `send_order_now` RPC that publishes to it, and waits on `Wait.any_of(Timer, Channel)` |
| `settings.py` | `DELAY` from `ORDER_DELAY_SECONDS` (default 60) | same |
| `worker.py`, `client.py` | worker and command-line client; `ORDER_CODE=v1` or `v2` picks the code | same, plus `app.py` for wiring |
| `run.sh` | v1, then v2 without and with the trigger | same |
| `upgrade.sh` | a run started on v1 continues on the v2 worker | same |
| `replay_check.py`, `skip-timer.sh` | replays a saved history against other code, offline | `skip-timer.sh`: see the last section |

Lines of code (no blank or comment lines):

| | Temporal | Dex |
| --- | --- | --- |
| `flow_v1.py` | 22 | 21 |
| `flow_v2.py` | 33 | 27 |
| v1 to v2 diff | 12 lines added, 1 removed | 9 lines added, 3 removed |

## Run it

You need `uv`, the `temporal` CLI, and `dexcli`.

```bash
order-delay/temporal/run.sh       # Temporal dev server on port 17233
order-delay/temporal/upgrade.sh
order-delay/dex/run.sh            # dexcli dev on port 18911
order-delay/dex/upgrade.sh
order-delay/dex/skip-timer.sh
```

Each script starts its own server and workers, and stops them on exit. Logs, histories, and Python environments go to `/tmp/dex-video/two-step/`. `run.sh` uses a 10-second window and the other scripts use 30 seconds. Set `ORDER_DELAY_SECONDS` to change it.

## Results

Measured on 2026-11-24 with Temporal CLI 1.9.1 (Server 1.32.0), `temporalio` 1.36.1 on Python 3.14.7, `dexcli` v1.48.0 (Dex Server 0.1.37), and `dex-python-sdk` 1.45.17 on Python 3.13.14.

### v1 and v2 (`run.sh`, 10-second window)

| Run | Temporal | Dex |
| --- | --- | --- |
| v1 | `order 42 confirmed, sent to kitchen` after 10 s | same result after 10 s |
| v2, no trigger | finished after 10 s | finished after 11 s |
| v2, trigger sent after 2 s | finished after 3 s | finished after 3 s |

### A run started on v1, continued on v2 (`upgrade.sh`, 30-second window)

The script starts an order on the v1 worker and waits until the window has started. It stops the v1 worker and starts the v2 worker.

Temporal:

- With no signal, the v2 worker completed the workflow when the 30-second timer fired. The history has no error. `WorkflowTaskCompleted` after the timer shows the identity `order-worker-v2`.
- With `send_order_now` sent after the v2 worker started, the workflow finished at once. The history shows `WorkflowExecutionSignaled`, then `TimerCanceled` by `order-worker-v2`.
- `replay_check.py` replays both saved histories against `flow_v2.py` with no error.
- `flow_v2.py` has no `workflow.patched` call. For this change, v1 and v2 both start one timer at the same point. The Temporal docs list "Adding a Signal Handler for a Signal Type that has not been sent to this Workflow Execution" as a safe change ([Workflow Definition](https://docs.temporal.io/workflow-definition)). A change that adds, removes, or reorders an activity or timer still needs a patch.

Dex:

- With no RPC, the v2 Worker ran `SendToKitchenStep` when the 30-second Timer fired, and the Flow completed.
- With `send_order_now` invoked after the v2 Worker started, the RPC succeeded. The wait did not end early: that Step had registered its wait under v1, which waits on the Timer only. The Flow completed when the Timer fired, 30 s after it started.
- A new order started on v2, with `send_order_now` invoked after 2 s, completed 2 s after it started.
- No code change was needed for the run started on v1 to finish on v2.

## Skipping a v1 Timer without a code change

Dex also has an operator call, `skip_timer`, that makes a waiting Timer fire now. `skip-timer.sh` starts an order on the v1 code and calls it on `SendToKitchenStep-1`. The order went to the kitchen 1 s after it started, with no code change.
