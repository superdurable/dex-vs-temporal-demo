#!/usr/bin/env bash
# Completes three replies under the v1 Worker, stops it, and starts the v2
# Worker. Reads each result with `client.py result` (wait_for_flow) and its
# status (describe_flow). Then stops the v2 Worker and reads again with no
# Worker running. Checks that every read returns the reply and COMPLETED;
# exits 1 otherwise. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
STAMP="$(date -u +%H%M%S)"
IDS=("done-a-$STAMP" "done-b-$STAMP" "done-c-$STAMP")

# Usage: read_all <description>
read_all() {
  for id in "${IDS[@]}"; do
    expect_output_contains "result read of $id $1" \
      "result of $id: COMPLETED ['sent:" "$(client v2 result "$id" 5)"
    expect_status "$id" COMPLETED
  done
}

start_server
log "--- three replies completed under v1"
start_worker v1
for id in "${IDS[@]}"; do
  client v1 start "$id" "ticket-$id"
  wait_until_waiting "$id"
  client v1 approve "$id"
  client v1 result "$id" 30
done
stop_worker

log "--- read with the v2 Worker running"
start_worker v2
read_all "with the v2 Worker"
stop_worker

log "--- read with no Worker running"
read_all "with no Worker"
finish_checks
