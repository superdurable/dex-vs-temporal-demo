#!/usr/bin/env bash
# T1: the naive idle timer fires at its original time although a message
# arrived; the updatable timer fires one idle window after the last message.
# Usage: scripts/temporal/t1-naive-timer.sh   (KEEP=1 leaves the server running)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

IDLE_SECONDS=30
CLOSE_AFTER_SECONDS=30
POST_AT_SECONDS=20

trap stop_all EXIT
start_temporal_server "$RUN_DIR/t1-server.log"
start_worker "$RUN_DIR/t1-worker.log"

wait_for_close_summary() {
  local conversation_id="$1"
  local outbox_file="${DEMO_OUTBOX_DIR:-/tmp/dex-vs-temporal-demo/outbox}/temporal.jsonl"
  for _ in $(seq 1 240); do
    if grep -q "\"$conversation_id\".*close-summary" "$outbox_file" 2>/dev/null; then
      return
    fi
    sleep 0.5
  done
  echo "No close-summary for $conversation_id within 120 s" >&2
  exit 1
}

run_variant() {
  local variant="$1"
  local expected="$2"
  local conversation_id="t1-$variant-$(date +%Y%m%d-%H%M%S)"
  echo
  echo "=== $variant variant: $conversation_id (idle ${IDLE_SECONDS}s, close after ${CLOSE_AFTER_SECONDS}s more, follow-up at t=${POST_AT_SECONDS}s)"
  local start_epoch post_epoch
  start_epoch="$(epoch_now)"
  drive start "$conversation_id" --variant "$variant" \
    --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS" >/dev/null
  sleep_until_epoch "$(python3 -c "print($start_epoch + $POST_AT_SECONDS)")"
  post_epoch="$(epoch_now)"
  drive post "$conversation_id" "Is there any update on the delivery?" >/dev/null
  wait_for_close_summary "$conversation_id"
  local reply_marks=()
  local reply_mark
  while IFS= read -r reply_mark; do
    reply_marks+=("$reply_mark")
  done < <(drive page "$conversation_id" current | uv run --quiet --project "$TEMPORAL_APP_DIR" python -c '
import datetime, json, sys
records = [r for r in json.load(sys.stdin)["messages"] if r["role"] == "assistant"]
for number, record in enumerate(reversed(records), 1):
    epoch = datetime.datetime.fromisoformat(record["time"]).timestamp()
    print(f"AI reply {number} recorded={epoch}")')
  uv run --quiet --project "$TEMPORAL_APP_DIR" python "$TEMPORAL_APP_DIR/outbox_timeline.py" \
    "$conversation_id" "$start_epoch" "customer follow-up posted=$post_epoch" \
    ${reply_marks[@]+"${reply_marks[@]}"}
  echo "  expected: $expected"
}

run_variant naive "nudge at about t=30s (30 s after the first reply; the follow-up reply did not re-arm the sleep), close at about t=60s"
run_variant correct "nudge at about t=50s (30 s after the second reply), close at about t=80s"
