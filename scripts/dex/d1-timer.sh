#!/usr/bin/env bash
# D1: idle timer. t=0 start a conversation with the customer's first message
# (the AI replies), t=20 s a follow-up (the AI replies). The idle window
# (30 s) starts after each reply, so the nudge lands about 30 s after the
# second reply (about t=50 s). The close waits CLOSE_AFTER_SECONDS (default
# 3600) after the nudge, so D1 does not wait for it.
# KEEP=1 leaves Dex running for Dex Web.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

IDLE_SECONDS="${IDLE_SECONDS:-30}"
FOLLOW_UP_AT_SECONDS="${FOLLOW_UP_AT_SECONDS:-20}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d1-timer-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d1-timer.log"

trap stop_everything_unless_kept EXIT

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

{
  start_dex_dev
  start_worker

  t0_ms=$(now_ms)
  log "t=0  start $CONVERSATION_ID with the first message (idle ${IDLE_SECONDS}s)"
  drive start "$CONVERSATION_ID" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS"

  sleep $((FOLLOW_UP_AT_SECONDS - ($(now_ms) - t0_ms) / 1000))
  log "t=${FOLLOW_UP_AT_SECONDS} post a follow-up customer message"
  follow_up_ms=$(now_ms)
  drive post "$CONVERSATION_ID" "It was due last Friday. Can you check?"

  log "waiting for the nudge in $OUTBOX"
  deadline=$((SECONDS + 2 * IDLE_SECONDS + 30))
  until grep -q "\"conversation\": \"$CONVERSATION_ID\", \"kind\": \"nudge\"" "$OUTBOX" 2>/dev/null; do
    if ((SECONDS > deadline)); then
      log "timed out waiting for the nudge"
      break
    fi
    sleep 1
  done

  page="$(drive page "$CONVERSATION_ID")"
  echo
  echo "Timeline for $CONVERSATION_ID (seconds since start; nudge expected ~${IDLE_SECONDS}s after the second reply)"
  python3 - "$OUTBOX" "$CONVERSATION_ID" "$t0_ms" "$follow_up_ms" "$page" <<'PY'
import json, sys
from datetime import datetime
outbox, conversation, t0_ms, follow_up_ms, page = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]

def seconds_since_start(iso_time):
    return datetime.fromisoformat(iso_time.replace("Z", "+00:00")).timestamp() - t0_ms / 1000

events = [(0.0, "start: customer first message"), ((follow_up_ms - t0_ms) / 1000, "customer follow-up posted")]
for record in json.loads(page)["records"]:
    if record["role"] == "assistant":
        events.append((seconds_since_start(record["recorded_at"]), f"AI reply recorded (seq {record['sequence']}, tools {', '.join(record['tool_calls'])})  {record['recorded_at']}"))
with open(outbox, encoding="utf-8") as lines:
    for line in lines:
        entry = json.loads(line)
        if entry["conversation"] == conversation:
            events.append((seconds_since_start(entry["time"]), f"outbox {entry['kind']} (messages={entry['messages']})  {entry['time']}"))
for offset, text in sorted(events):
    print(f"  t={offset:6.1f}s  {text}")
PY
  echo
  drive status "$CONVERSATION_ID"
  echo "$page"
  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
} 2>&1 | tee "$LOG_FILE"
