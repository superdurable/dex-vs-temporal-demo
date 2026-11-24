#!/usr/bin/env bash
# Usage: upgrade-in-send.sh v2_no_patch|v2 slow|retry
# Starts three replies on the v1 worker and approves them under v1, so each
# enters send_reply. Test-only settings change send_reply:
#   slow   each attempt takes SEND_REPLY_SECONDS (8 s)
#   retry  the first SEND_REPLY_FAILED_ATTEMPTS (3) attempts fail; default retry policy
# Once every reply runs send_reply attempt 1 (slow) or attempt 2 (retry), the
# script stops the v1 worker with SIGTERM and starts a new worker process with
# the given code, which also registers send_reply. Waits 30 s, then prints
# each result, status, `temporal workflow show`, `temporal workflow describe`,
# and both worker logs. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CODE="${1:?usage: upgrade-in-send.sh v2_no_patch|v2 slow|retry}"
MODE="${2:?usage: upgrade-in-send.sh v2_no_patch|v2 slow|retry}"
export AGENT_REPLY_AUTO_SEND_SECONDS="${AGENT_REPLY_AUTO_SEND_SECONDS:-10}"
case "$MODE" in
  slow) export SEND_REPLY_SECONDS=8 SEND_REPLY_FAILED_ATTEMPTS=0; SWAP_AT_ATTEMPT=1 ;;
  retry) export SEND_REPLY_SECONDS=0 SEND_REPLY_FAILED_ATTEMPTS=3; SWAP_AT_ATTEMPT=2 ;;
  *) echo "mode must be slow or retry" >&2; exit 2 ;;
esac
STAMP="$(date -u +%H%M%S)"
IDS=("in-send-a-$STAMP" "in-send-b-$STAMP" "in-send-c-$STAMP")

# History shows ActivityTaskStarted only when an attempt ends, so the worker log is used.
wait_until_attempt() {
  for _ in $(seq 1 80); do
    if grep -q "send_reply $1 attempt $SWAP_AT_ATTEMPT started" "$RUN_DIR/worker-v1.log"; then
      log "$1 runs send_reply attempt $SWAP_AT_ATTEMPT on v1"
      return
    fi
    sleep 0.25
  done
  log "ERROR: $1 did not reach send_reply attempt $SWAP_AT_ATTEMPT"
  exit 1
}

start_server
log "mode $MODE: send_reply takes $SEND_REPLY_SECONDS s, first $SEND_REPLY_FAILED_ATTEMPTS attempts fail; new code: $CODE"

log "--- three replies on v1, approved under v1"
start_worker v1
for id in "${IDS[@]}"; do
  client start "$id" "ticket-$id"
done
for id in "${IDS[@]}"; do
  wait_until_waiting "$id"
  client approve "$id"
done
for id in "${IDS[@]}"; do
  wait_until_attempt "$id"
done
stop_worker

log "--- new worker process with $CODE"
start_worker "$CODE"
sleep 30
for id in "${IDS[@]}"; do
  client result "$id" 1
  client status "$id"
done

log "--- histories"
for id in "${IDS[@]}"; do
  log "$id: history_summary.py"
  show_history "$id"
  log "$id: temporal workflow show"
  temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$id"
  log "$id: temporal workflow describe"
  temporal workflow describe --address "$TEMPORAL_ADDRESS" -w "$id" | head -40
done
log "--- v1 worker log"
cat "$RUN_DIR/worker-v1.log"
log "--- $CODE worker log"
cat "$RUN_DIR/worker-$CODE.log"
