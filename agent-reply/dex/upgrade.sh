#!/usr/bin/env bash
# Starts three replies on the v1 Worker and leaves them waiting for approval.
# Stops the v1 Worker and starts the v2 Worker. Then:
#   old-approve-a, old-approve-b  get approve_reply
#   old-idle                      gets nothing
#   new-approve, new-idle         start on v2; one is approved
# Waits 15 s, prints each history, and checks each status and Step order;
# exits 1 on an unexpected result. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
STAMP="$(date -u +%H%M%S)"
OLD=("old-approve-a-$STAMP" "old-approve-b-$STAMP" "old-idle-$STAMP")
NEW=("new-approve-$STAMP" "new-idle-$STAMP")

start_server
log "--- three replies on v1"
start_worker v1
for id in "${OLD[@]}"; do
  client v1 start "$id" "ticket-$id"
done
for id in "${OLD[@]}"; do
  wait_until_waiting "$id"
done
stop_worker

log "--- v2 Worker"
start_worker v2
client v2 approve "${OLD[0]}"
client v2 approve "${OLD[1]}"
for id in "${NEW[@]}"; do
  client v2 start "$id" "ticket-$id"
done
sleep 2
client v2 approve "${NEW[0]}"
sleep 15

log "--- histories"
for id in "${OLD[@]}" "${NEW[@]}"; do
  log "$id"
  show_history "$id"
done
log "--- v2 Worker log"
cat "$RUN_DIR/worker-v2.log"

log "--- checks"
for id in "${OLD[0]}" "${OLD[1]}"; do
  expect_status "$id" COMPLETED
  expect_steps "$id" DraftReplyStep SendReplyStep
done
expect_status "${OLD[2]}" RUNNING
expect_status "${NEW[0]}" COMPLETED
expect_steps "${NEW[0]}" DraftReplyStep NotifyReviewerStep SendReplyStep
expect_status "${NEW[1]}" RUNNING
finish_checks
