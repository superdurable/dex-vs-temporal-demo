#!/usr/bin/env bash
# Usage: query-completed.sh v2_no_patch|v2
# Completes three replies under the v1 worker, stops it, and starts a new
# worker process with the given code. Queries each completed reply with the
# built-in stack trace query (`temporal workflow stack`), which a worker
# answers from the whole history, and reads each result (served by the
# server). Then completes one reply under the given code and queries it with
# a new v1 worker. Prints the worker logs. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CODE="${1:?usage: query-completed.sh v2_no_patch|v2}"
STAMP="$(date -u +%H%M%S)"
IDS=("done-a-$STAMP" "done-b-$STAMP" "done-c-$STAMP")
NEW_ID="done-on-$CODE-$STAMP"

stack_query() {
  log "temporal workflow stack -w $1"
  temporal workflow stack --address "$TEMPORAL_ADDRESS" -w "$1" 2>&1 | cut -c1-400 || true
}

complete_reply() {
  client start "$1" "ticket-$1"
  wait_until_waiting "$1"
  client approve "$1"
  client result "$1" 30
}

start_server

log "--- three replies completed under v1"
start_worker v1
for id in "${IDS[@]}"; do
  complete_reply "$id"
done
stop_worker

log "--- query and read with a new $CODE worker process"
start_worker "$CODE"
for id in "${IDS[@]}"; do
  stack_query "$id"
  client result "$id" 5
done

log "--- one reply completed under $CODE, then queried by a new v1 worker"
complete_reply "$NEW_ID"
stop_worker
log "$CODE worker log:"
cat "$RUN_DIR/worker-$CODE.log"
start_worker v1
stack_query "$NEW_ID"
log "v1 worker log:"
cat "$RUN_DIR/worker-v1.log"
