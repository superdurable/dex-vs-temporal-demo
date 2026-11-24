#!/usr/bin/env bash
# Usage: query-completed.sh v2_no_patch|v2
# Completes three replies under the v1 worker, stops it, and starts a new
# worker process with the given code. For each completed reply:
#   - `temporal workflow stack` is a query; a worker answers it from the whole history
#   - `client.py result` reads the stored result from the server
# Then stops the worker and reads each result again with no worker running.
# Checks: stack fails with the non-determinism error below on v2_no_patch and
# answers on v2; every result read returns the reply. Exits 1 otherwise.
# Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CODE="${1:?usage: query-completed.sh v2_no_patch|v2}"
EXPECTED_ERROR="Error: querying workflow failed: [TMPRL1100] Nondeterminism error: Activity type of scheduled event 'send_reply' does not match activity type of activity command 'notify_reviewer'"
STAMP="$(date -u +%H%M%S)"
IDS=("done-a-$STAMP" "done-b-$STAMP" "done-c-$STAMP")

start_server
log "--- three replies completed under v1"
start_worker v1
for id in "${IDS[@]}"; do
  client start "$id" "ticket-$id"
  wait_until_waiting "$id"
  client approve "$id"
  client result "$id" 30
done
stop_worker

log "--- stack query and result read with a new $CODE worker process"
start_worker "$CODE"
for id in "${IDS[@]}"; do
  log "temporal workflow stack -w $id"
  if stack_output="$(temporal workflow stack --address "$TEMPORAL_ADDRESS" -w "$id" 2>&1)"; then
    stack_status=0
  else
    stack_status=$?
  fi
  cut -c1-300 <<<"$stack_output" | head -3
  if [[ "$CODE" == "v2_no_patch" ]]; then
    [[ "$stack_status" -ne 0 ]] || check_failed "stack query of $id succeeded, expected an error"
    expect_output_contains "stack query of $id fails with the expected error" "$EXPECTED_ERROR" "$stack_output"
  else
    [[ "$stack_status" -eq 0 ]] || check_failed "stack query of $id failed"
    expect_output_contains "stack query of $id answers" "QueryResult" "$stack_output"
  fi
  expect_output_contains "result read of $id with a worker" "result of $id: sent:" "$(client result "$id" 5)"
done
log "$CODE worker log:"
cut -c1-400 "$RUN_DIR/worker-$CODE.log"
stop_worker

log "--- result read with no worker running"
for id in "${IDS[@]}"; do
  expect_output_contains "result read of $id with no worker" "result of $id: sent:" "$(client result "$id" 5)"
done
finish_checks
