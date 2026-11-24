#!/usr/bin/env bash
# Usage: upgrade.sh v2_no_patch|v2
# Starts four replies on the v1 worker and leaves them waiting for approval.
# Stops the v1 worker and starts a new worker process with the given code,
# so nothing is cached. Then:
#   old-approve-a, old-approve-b  get approve_reply
#   old-idle                      gets nothing
#   old-query                     is queried with `temporal workflow stack`
#   new-approve, new-idle         start on the new code; one is approved
# Waits 15 s, prints each history and the worker log, and checks each status
# and activity order; exits 1 on an unexpected result. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CODE="${1:?usage: upgrade.sh v2_no_patch|v2}"
STAMP="$(date -u +%H%M%S)"
OLD=("old-approve-a-$STAMP" "old-approve-b-$STAMP" "old-idle-$STAMP" "old-query-$STAMP")
NEW=("new-approve-$STAMP" "new-idle-$STAMP")
case "$CODE" in
  v2_no_patch) OLD_APPROVED_ACTIVITIES="draft_reply notify_reviewer send_reply" ;;
  v2) OLD_APPROVED_ACTIVITIES="draft_reply send_reply" ;;
  *) echo "code must be v2_no_patch or v2" >&2; exit 2 ;;
esac

start_server
log "--- four replies on v1"
start_worker v1
for id in "${OLD[@]}"; do
  client start "$id" "ticket-$id"
done
for id in "${OLD[@]}"; do
  wait_until_waiting "$id"
done
stop_worker

log "--- new worker process with $CODE"
start_worker "$CODE"
client approve "${OLD[0]}"
client approve "${OLD[1]}"
log "query ${OLD[3]} with temporal workflow stack"
stack_output="$(temporal workflow stack --address "$TEMPORAL_ADDRESS" -w "${OLD[3]}" 2>&1)" \
  || check_failed "temporal workflow stack failed for ${OLD[3]}"
cut -c1-300 <<<"$stack_output" | head -5
for id in "${NEW[@]}"; do
  client start "$id" "ticket-$id"
done
sleep 2
client approve "${NEW[0]}"
sleep 15

log "--- histories"
for id in "${OLD[@]}" "${NEW[@]}"; do
  log "$id"
  show_history "$id"
done
log "--- $CODE worker log"
cut -c1-400 "$RUN_DIR/worker-$CODE.log"

log "--- checks"
expect_output_contains "stack query of a waiting old reply answers" "QueryResult" "$stack_output"
for id in "${OLD[0]}" "${OLD[1]}"; do
  expect_status "$id" COMPLETED
  expect_activities "$id" $OLD_APPROVED_ACTIVITIES
done
expect_status "${OLD[2]}" RUNNING
expect_status "${OLD[3]}" RUNNING
expect_status "${NEW[0]}" COMPLETED
expect_activities "${NEW[0]}" draft_reply notify_reviewer send_reply
expect_status "${NEW[1]}" RUNNING
finish_checks
