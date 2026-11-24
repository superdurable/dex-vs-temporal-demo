#!/usr/bin/env bash
# Usage: upgrade-in-send.sh v2_no_patch|v2
# Starts three replies on the v1 worker and approves them under v1, so each
# enters send_reply. The test-only SEND_REPLY_FAILED_ATTEMPTS=3 makes the first
# three send_reply attempts fail; the default retry policy retries them. Once
# every reply runs attempt 2, stops the v1 worker and starts a new worker
# process with the given code, which also registers send_reply. Waits 30 s,
# prints each history, `temporal workflow show`, and the worker logs, then
# checks the outcome; exits 1 on an unexpected result:
#   v2_no_patch  each reply is RUNNING with the non-determinism error below
#   v2           each reply COMPLETED with draft_reply, send_reply
# Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CODE="${1:?usage: upgrade-in-send.sh v2_no_patch|v2}"
export SEND_REPLY_FAILED_ATTEMPTS=3
EXPECTED_ERROR="[TMPRL1100] Nondeterminism error: Activity type of scheduled event 'send_reply' does not match activity type of activity command 'notify_reviewer'"
STAMP="$(date -u +%H%M%S)"
IDS=("in-send-a-$STAMP" "in-send-b-$STAMP" "in-send-c-$STAMP")

# History shows ActivityTaskStarted only when an attempt ends, so the worker log is used.
wait_until_attempt_2() {
  for _ in $(seq 1 80); do
    if grep -q "send_reply $1 attempt 2 started" "$RUN_DIR/worker-v1.log"; then
      log "$1 runs send_reply attempt 2 on v1"
      return
    fi
    sleep 0.25
  done
  log "ERROR: $1 did not reach send_reply attempt 2"
  exit 1
}

start_server
log "--- three replies on v1, approved under v1; new code: $CODE"
start_worker v1
for id in "${IDS[@]}"; do
  client start "$id" "ticket-$id"
done
for id in "${IDS[@]}"; do
  wait_until_waiting "$id"
  client approve "$id"
done
for id in "${IDS[@]}"; do
  wait_until_attempt_2 "$id"
done
stop_worker

log "--- new worker process with $CODE"
start_worker "$CODE"
sleep 30

log "--- histories"
for id in "${IDS[@]}"; do
  log "$id: history_summary.py"
  show_history "$id"
  log "$id: temporal workflow show"
  temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$id"
done
log "--- v1 worker log"
grep -E "worker|started" "$RUN_DIR/worker-v1.log"
log "--- $CODE worker log"
grep -E "worker|started|Failing workflow task" "$RUN_DIR/worker-$CODE.log" | cut -c1-400

log "--- checks"
for id in "${IDS[@]}"; do
  if grep -q "send_reply $id attempt 4 started" "$RUN_DIR/worker-$CODE.log"; then
    log "check ok: $id ran send_reply attempt 4 on $CODE"
  else
    check_failed "$id did not run send_reply attempt 4 on $CODE"
  fi
  if [[ "$CODE" == "v2_no_patch" ]]; then
    expect_status "$id" RUNNING
    expect_output_contains "$id history has the non-determinism error" \
      "$EXPECTED_ERROR" "$(cat "$RUN_DIR/history-$id.json")"
  else
    expect_status "$id" COMPLETED
    expect_activities "$id" draft_reply send_reply
  fi
done
finish_checks
