#!/usr/bin/env bash
# Starts four replies on the v1 Worker and leaves them waiting for approval.
# Stops the v1 Worker and starts the v2 Worker. Then:
#   old-approve-a, old-approve-b  get approve_reply
#   old-idle-a, old-idle-b        get nothing
#   new-approve, new-idle         start on v2; one is approved
# Waits for the Timer plus 10 s, then prints each status and history.
# Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export AGENT_REPLY_AUTO_SEND_SECONDS="${AGENT_REPLY_AUTO_SEND_SECONDS:-10}"
STAMP="$(date -u +%H%M%S)"
OLD=("old-approve-a-$STAMP" "old-approve-b-$STAMP" "old-idle-a-$STAMP" "old-idle-b-$STAMP")
NEW=("new-approve-$STAMP" "new-idle-$STAMP")

start_server
log "auto-send timer: $AGENT_REPLY_AUTO_SEND_SECONDS s"

log "--- four replies on v1"
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

log "--- wait for the Timer plus 10 s"
sleep "$((AGENT_REPLY_AUTO_SEND_SECONDS + 10))"
for id in "${OLD[@]}" "${NEW[@]}"; do
  client v2 result "$id" 1
  client v2 status "$id"
done

log "--- histories"
for id in "${OLD[@]}" "${NEW[@]}"; do
  log "$id"
  show_history "$id"
done
log "--- v2 Worker log"
cat "$RUN_DIR/worker-v2.log"
