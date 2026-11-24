#!/usr/bin/env bash
# Starts three replies on the v1 Worker and approves them under v1, so each
# runs SendReplyStep.execute. The test-only SEND_REPLY_FAILED_ATTEMPTS=3 makes
# the first three attempts fail; the default retry policy retries them. Once
# every reply runs attempt 2, stops the v1 Worker and starts the v2 Worker.
# Waits 30 s, prints each history and both Worker logs, then checks that each
# reply COMPLETED with DraftReplyStep, SendReplyStep and that attempt 4 ran on
# v2; exits 1 otherwise. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export SEND_REPLY_FAILED_ATTEMPTS=3
STAMP="$(date -u +%H%M%S)"
IDS=("in-send-a-$STAMP" "in-send-b-$STAMP" "in-send-c-$STAMP")

wait_until_attempt_2() {
  for _ in $(seq 1 80); do
    if grep -q "SendReplyStep.execute $1 attempt 2 started" "$RUN_DIR/worker-v1.log"; then
      log "$1 runs SendReplyStep attempt 2 on v1"
      return
    fi
    sleep 0.25
  done
  log "ERROR: $1 did not reach SendReplyStep attempt 2"
  exit 1
}

start_server
log "--- three replies on v1, approved under v1"
start_worker v1
for id in "${IDS[@]}"; do
  client v1 start "$id" "ticket-$id"
done
for id in "${IDS[@]}"; do
  wait_until_waiting "$id"
  client v1 approve "$id"
done
for id in "${IDS[@]}"; do
  wait_until_attempt_2 "$id"
done
stop_worker

log "--- v2 Worker"
start_worker v2
sleep 30

log "--- histories"
for id in "${IDS[@]}"; do
  log "$id"
  show_history "$id"
done
log "--- v1 Worker log"
grep -E "worker|started" "$RUN_DIR/worker-v1.log"
log "--- v2 Worker log"
grep -E "worker|started" "$RUN_DIR/worker-v2.log"

log "--- checks"
for id in "${IDS[@]}"; do
  if grep -q "SendReplyStep.execute $id attempt 4 started" "$RUN_DIR/worker-v2.log"; then
    log "check ok: $id ran SendReplyStep attempt 4 on v2"
  else
    check_failed "$id did not run SendReplyStep attempt 4 on v2"
  fi
  expect_status "$id" COMPLETED
  expect_steps "$id" DraftReplyStep SendReplyStep
done
finish_checks
