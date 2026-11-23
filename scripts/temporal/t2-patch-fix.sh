#!/usr/bin/env bash
# T2 part 2: fix the deploy with a patch. Run after
# `KEEP=1 scripts/temporal/t2-change-code.sh`, which leaves the server, the
# v2 worker, and conversations A (started on v1) and B (started on v2).
# Replaces the v2 worker with v3, which guards analyze_sentiment with
# workflow.patched(), then posts to A and B and starts conversation C on v3.
# Usage: scripts/temporal/t2-patch-fix.sh   (KEEP=1 leaves everything running)
# Raw output: $RUN_DIR/t2-patch-fix.log, history dumps $RUN_DIR/t2-*-history-after-v3.json
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

LOG_FILE="${LOG_FILE:-$RUN_DIR/t2-patch-fix.log}"
STATE_FILE="$RUN_DIR/t2-conversations.env"
if [ ! -f "$STATE_FILE" ]; then
  echo "Run KEEP=1 scripts/temporal/t2-change-code.sh first." >&2
  exit 1
fi
source "$STATE_FILE"

stop_everything() {
  stop_all
  if [ "${KEEP:-0}" != "1" ]; then
    stop_kept_server
  fi
}
trap stop_everything EXIT
start_temporal_server "$RUN_DIR/t2-server.log"

conversation_c="t2-c-$(date +%Y%m%d-%H%M%S)"
{
  echo "=== deploy v3: stop the v2 worker, start a v3 worker (sentiment analysis behind workflow.patched)"
  start_worker "$RUN_DIR/t2-worker-v3.log" v3

  echo
  echo "=== $CONVERSATION_A (started on v1): the 4th message, stuck since the v2 deploy"
  drive wait-replies "$CONVERSATION_A" --count 4 --timeout 30
  echo "--- customer posts a 5th message"
  drive post "$CONVERSATION_A" "Thanks. Can you also send me the invoice?" >/dev/null
  drive wait-replies "$CONVERSATION_A" --count 5 --timeout 30

  echo
  echo "=== $CONVERSATION_B (started on v2): customer posts a 3rd message"
  drive post "$CONVERSATION_B" "Please confirm the change." >/dev/null
  drive wait-replies "$CONVERSATION_B" --count 3 --timeout 30

  echo
  echo "=== new conversation $conversation_c on v3, 2 customer messages"
  drive start "$conversation_c" --first-message "My refund has not arrived." >/dev/null
  drive wait-replies "$conversation_c" --count 1
  drive post "$conversation_c" "It has been two weeks." >/dev/null
  drive wait-replies "$conversation_c" --count 2

  for conversation_id in "$CONVERSATION_A" "$CONVERSATION_B" "$conversation_c"; do
    echo
    echo "=== $conversation_id after the v3 deploy"
    drive inspect "$conversation_id" --dump "$RUN_DIR/${conversation_id%%-2*}-history-after-v3.json"
  done
} 2>&1 | tee "$LOG_FILE"
