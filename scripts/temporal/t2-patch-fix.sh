#!/usr/bin/env bash
# T2 part 2: fix the deploy with a patch. Run after
# `KEEP=1 scripts/temporal/t2-change-code.sh`, which leaves the server, the
# v2 worker, and conversations A (started on v1) and B (started on v2).
# Replaces the v2 worker with v3, which guards analyze_sentiment with
# workflow.patched().
#   v1-to-v3 path: A (started on v1) keeps the v1 path and recovers on its
#   next workflow task retry; C (started on v3) takes the new path.
#   Extra incident case: B (started on the unguarded v2) has no patch marker,
#   so v3 replays it on the v1 path and it stalls with a non-determinism
#   error. It needs a v2-compatible worker or a reset; none is done here.
# Usage: scripts/temporal/t2-patch-fix.sh   (KEEP=1 leaves everything running)
#   A_RECOVERY_TIMEOUT  seconds to wait for A's next retry (default 900; the
#                       retry backoff grows while A stalls on v2)
# Raw output: $RUN_DIR/t2-patch-fix.log, history dumps $RUN_DIR/t2-*-history-after-v3.json
# Exits non-zero when an outcome differs from the expected one.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

LOG_FILE="${LOG_FILE:-$RUN_DIR/t2-patch-fix.log}"
STATE_FILE="$RUN_DIR/t2-conversations.env"
A_RECOVERY_TIMEOUT="${A_RECOVERY_TIMEOUT:-900}"
if [ ! -f "$STATE_FILE" ]; then
  echo "Run KEEP=1 scripts/temporal/t2-change-code.sh first." >&2
  exit 1
fi
source "$STATE_FILE"

stop_everything() {
  if [ "${KEEP:-0}" = "1" ]; then
    stop_all
    return
  fi
  stop_worker
  stop_server
}
trap stop_everything EXIT
start_temporal_server "$RUN_DIR/t2-server.log"

conversation_c="t2-c-$(date +%Y%m%d-%H%M%S)"
{
  echo "=== deploy v3: stop the v2 worker, start a v3 worker (sentiment analysis behind workflow.patched)"
  start_worker "$RUN_DIR/t2-worker-v3.log" v3

  echo
  echo "=== v1-to-v3 path, A ($CONVERSATION_A, started on v1): the 4th message, stuck since the v2 deploy"
  echo "--- waits for the server's next workflow task retry, up to ${A_RECOVERY_TIMEOUT}s"
  drive wait-replies "$CONVERSATION_A" --count 4 --timeout "$A_RECOVERY_TIMEOUT"
  echo "--- A: customer posts a 5th message"
  drive post "$CONVERSATION_A" "Thanks. Can you also send me the invoice?" >/dev/null
  drive wait-replies "$CONVERSATION_A" --count 5 --timeout 30

  echo
  echo "=== v1-to-v3 path, C ($conversation_c), started on v3: 2 customer messages"
  drive start "$conversation_c" --first-message "My refund has not arrived." >/dev/null
  drive wait-replies "$conversation_c" --count 1 --expect-patch-marker
  drive post "$conversation_c" "It has been two weeks." >/dev/null
  drive wait-replies "$conversation_c" --count 2 --expect-patch-marker

  echo
  echo "=== extra incident case, B ($CONVERSATION_B, started on the unguarded v2): customer posts a 3rd message"
  drive post "$CONVERSATION_B" "Please confirm the change." >/dev/null
  echo "--- B on v3: expected to stall with a non-determinism error (v3 replays it on the v1 path)"
  drive wait-replies "$CONVERSATION_B" --count 3 --timeout 30 \
    --expect stall --failure-cause NON_DETERMINISTIC_ERROR

  for label_and_id in "A:$CONVERSATION_A" "C:$conversation_c" "B:$CONVERSATION_B"; do
    label="${label_and_id%%:*}"
    conversation_id="${label_and_id#*:}"
    echo
    echo "=== $label ($conversation_id) after the v3 deploy"
    drive inspect "$conversation_id" --dump "$RUN_DIR/t2-$(echo "$label" | tr 'A-Z' 'a-z')-history-after-v3.json"
  done
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
