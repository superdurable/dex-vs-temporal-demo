#!/usr/bin/env bash
# D1: idle timer. t=0 start a conversation with the customer's first message
# (the AI replies), t=20 s a follow-up (the AI replies). The idle window
# (30 s) starts after each reply, so the nudge lands about 30 s after the
# second reply (about t=50 s). The close waits CLOSE_AFTER_SECONDS (default
# 3600) after the nudge, so D1 does not wait for it.
# Fails when the nudge does not arrive in time or lands outside the window.
# KEEP=1 leaves Dex running for Dex Web after a successful run.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

IDLE_SECONDS="${IDLE_SECONDS:-30}"
FOLLOW_UP_AT_SECONDS="${FOLLOW_UP_AT_SECONDS:-20}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d1-timer-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d1-timer.log"
PAGE_FILE="$RUN_DIR/d1-page.json"

monotonic_seconds() { python3 -c 'import time; print(time.monotonic())'; }
epoch_seconds() { python3 -c 'import time; print(time.time())'; }

main() {
  ensure_python_environment
  start_dex_dev
  start_worker

  local t0_monotonic started_at start_output run_id follow_up_at
  t0_monotonic="$(monotonic_seconds)"
  started_at="$(epoch_seconds)"
  log "t=0  start $CONVERSATION_ID with the first message (idle ${IDLE_SECONDS}s)"
  start_output="$(drive start "$CONVERSATION_ID" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS")"
  echo "$start_output"
  run_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["runId"])' <<<"$start_output")"

  # Sleep until the follow-up is due on the monotonic clock; report a late follow-up.
  python3 - "$t0_monotonic" "$FOLLOW_UP_AT_SECONDS" <<'PY'
import sys, time
due = float(sys.argv[1]) + float(sys.argv[2])
remaining = due - time.monotonic()
if remaining > 0:
    time.sleep(remaining)
else:
    print(f"follow-up is {-remaining:.1f}s late", flush=True)
PY
  follow_up_at="$(epoch_seconds)"
  log "t=$(python3 -c "import sys; print(round(float(sys.argv[1]) - float(sys.argv[2]), 1))" "$follow_up_at" "$started_at") post a follow-up customer message"
  drive post "$CONVERSATION_ID" "It was due last Friday. Can you check?" --message-id follow-up-1

  log "waiting for the nudge of run $run_id in $OUTBOX"
  checks outbox --outbox "$OUTBOX" --conversation "$CONVERSATION_ID" --run-id "$run_id" \
    --kind nudge --wait-count 1 --timeout-seconds $((2 * IDLE_SECONDS + 30)) >/dev/null

  drive page "$CONVERSATION_ID" >"$PAGE_FILE"
  echo
  local timeline_status=0
  checks d1-timeline --outbox "$OUTBOX" --conversation "$CONVERSATION_ID" --run-id "$run_id" \
    --started-at "$started_at" --follow-up-at "$follow_up_at" --page-json "$PAGE_FILE" \
    --idle-seconds "$IDLE_SECONDS" || timeline_status=$?
  echo
  drive status "$CONVERSATION_ID"
  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
  return "$timeline_status"
}

run_logged "$LOG_FILE" main
