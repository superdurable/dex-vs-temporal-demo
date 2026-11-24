#!/usr/bin/env bash
# Usage: upgrade-in-send.sh slow|retry
# Starts three replies on the v1 Worker and approves them under v1, so each
# runs SendReplyStep.execute. Test-only settings change that execute:
#   slow   each attempt takes SEND_REPLY_SECONDS (8 s)
#   retry  the first SEND_REPLY_FAILED_ATTEMPTS (3) attempts fail; default retry policy
# Once every reply runs attempt 1 (slow) or attempt 2 (retry), the script
# stops the v1 Worker with SIGTERM and starts the v2 Worker. Waits 30 s, then
# prints each result, status, history, and both Worker logs. Stops
# everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
MODE="${1:?usage: upgrade-in-send.sh slow|retry}"
case "$MODE" in
  slow) export SEND_REPLY_SECONDS=8 SEND_REPLY_FAILED_ATTEMPTS=0; SWAP_AT_ATTEMPT=1 ;;
  retry) export SEND_REPLY_SECONDS=0 SEND_REPLY_FAILED_ATTEMPTS=3; SWAP_AT_ATTEMPT=2 ;;
  *) echo "mode must be slow or retry" >&2; exit 2 ;;
esac
STAMP="$(date -u +%H%M%S)"
IDS=("in-send-a-$STAMP" "in-send-b-$STAMP" "in-send-c-$STAMP")

wait_until_attempt() {
  for _ in $(seq 1 80); do
    if grep -q "SendReplyStep.execute $1 attempt $SWAP_AT_ATTEMPT started" "$RUN_DIR/worker-v1.log"; then
      log "$1 runs SendReplyStep attempt $SWAP_AT_ATTEMPT on v1"
      return
    fi
    sleep 0.25
  done
  log "ERROR: $1 did not reach SendReplyStep attempt $SWAP_AT_ATTEMPT"
  exit 1
}

start_server
log "mode $MODE: execute takes $SEND_REPLY_SECONDS s, first $SEND_REPLY_FAILED_ATTEMPTS attempts fail"

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
  wait_until_attempt "$id"
done
stop_started=$SECONDS
stop_worker
log "stopping the v1 Worker took $((SECONDS - stop_started)) s"

log "--- v2 Worker"
start_worker v2
sleep 30
for id in "${IDS[@]}"; do
  client v2 result "$id" 1
  client v2 status "$id"
done

log "--- histories"
for id in "${IDS[@]}"; do
  log "$id"
  show_history "$id"
done
log "--- v1 Worker log"
cat "$RUN_DIR/worker-v1.log"
log "--- v2 Worker log"
cat "$RUN_DIR/worker-v2.log"
