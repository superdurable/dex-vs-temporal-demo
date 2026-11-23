#!/usr/bin/env bash
# T1: the naive idle timer fires at its original time although a message
# arrived; the updatable timer fires one idle window after the last reply.
# The timeline comes from history event times; the outbox lines prove the writes.
# Usage: scripts/temporal/t1-naive-timer.sh   (KEEP=1 leaves the server running)
# Raw output: $RUN_DIR/t1.log; exits non-zero when a timeline is not the expected one.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

IDLE_SECONDS=30
CLOSE_AFTER_SECONDS=30
POST_AT_SECONDS=20
LOG_FILE="${LOG_FILE:-$RUN_DIR/t1.log}"

trap stop_all EXIT
start_temporal_server "$RUN_DIR/t1-server.log"
start_worker "$RUN_DIR/t1-worker.log"
# Sync the environment once, so it does not delay the scripted follow-up.
drive --help >/dev/null

# Waits until the workflow has completed; history decides, not the outbox.
wait_for_completion() {
  local conversation_id="$1"
  for _ in $(seq 1 300); do
    if drive status "$conversation_id" | grep -q '"workflow_status": "COMPLETED"'; then
      return
    fi
    sleep 0.5
  done
  echo "OBSERVATION FAILED: $conversation_id did not complete within 150 s" >&2
  exit 1
}

run_variant() {
  local variant="$1"
  local conversation_id="t1-$variant-$(date +%Y%m%d-%H%M%S)"
  echo
  echo "=== $variant variant: $conversation_id (idle ${IDLE_SECONDS}s, close after ${CLOSE_AFTER_SECONDS}s more, follow-up about t=${POST_AT_SECONDS}s)"
  local start_epoch
  start_epoch="$(epoch_now)"
  drive start "$conversation_id" --variant "$variant" \
    --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS" >/dev/null
  sleep "$(python3 -c "import time; print(max(0.0, $start_epoch + $POST_AT_SECONDS - time.time()))")"
  drive post "$conversation_id" "Is there any update on the delivery?" >/dev/null
  wait_for_completion "$conversation_id"
  drive timeline "$conversation_id" --expect "$variant" \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS"
}

{
  print_versions
  run_variant naive
  run_variant correct
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
