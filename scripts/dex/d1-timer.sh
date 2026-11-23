#!/usr/bin/env bash
# D1: idle timer. Start a conversation with a 30 s idle window, post one
# customer message at t=20 s, and show when the AI reply is recorded and when
# the nudge (about t=50 s plus reply time) reaches the outbox. The close after
# the nudge waits CLOSE_AFTER_SECONDS (default 3600), so D1 does not wait for it.
# KEEP=1 leaves Dex running for Dex Web.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

IDLE_SECONDS="${IDLE_SECONDS:-30}"
POST_AT_SECONDS="${POST_AT_SECONDS:-20}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d1-timer-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d1-timer.log"

trap stop_everything_unless_kept EXIT

{
  start_dex_dev
  start_worker

  t0_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
  log "t=0  start $CONVERSATION_ID with idle ${IDLE_SECONDS}s"
  drive start "$CONVERSATION_ID" --idle-seconds "$IDLE_SECONDS" \
    --close-after-seconds "$CLOSE_AFTER_SECONDS"

  sleep "$POST_AT_SECONDS"
  log "t=${POST_AT_SECONDS} post one customer message"
  drive post "$CONVERSATION_ID" "Hi, my order has not arrived yet."
  post_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')

  log "waiting for the AI reply in the transcript"
  until drive page "$CONVERSATION_ID" | grep -q '"role": "assistant"'; do sleep 0.2; done
  reply_seen_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')

  log "waiting for the nudge in $OUTBOX"
  deadline=$((SECONDS + 2 * IDLE_SECONDS + 30))
  until grep -q "\"conversation\": \"$CONVERSATION_ID\", \"kind\": \"nudge\"" "$OUTBOX" 2>/dev/null; do
    if ((SECONDS > deadline)); then
      log "timed out waiting for the nudge"
      break
    fi
    sleep 1
  done

  echo
  echo "Timeline for $CONVERSATION_ID (seconds since start; expected nudge ~t=$((POST_AT_SECONDS + IDLE_SECONDS)) plus reply time)"
  python3 - "$OUTBOX" "$CONVERSATION_ID" "$t0_ms" "$post_ms" "$reply_seen_ms" <<'PY'
import json, sys
from datetime import datetime
outbox, conversation = sys.argv[1], sys.argv[2]
t0_ms, post_ms, reply_seen_ms = (int(value) for value in sys.argv[3:6])
print(f"  t={0:6.1f}s  start")
print(f"  t={(post_ms - t0_ms) / 1000:6.1f}s  customer message posted")
print(f"  t={(reply_seen_ms - t0_ms) / 1000:6.1f}s  AI reply visible in the transcript (upper bound, polled)")
with open(outbox, encoding="utf-8") as lines:
    for line in lines:
        entry = json.loads(line)
        if entry["conversation"] != conversation:
            continue
        written_ms = datetime.fromisoformat(entry["time"].replace("Z", "+00:00")).timestamp() * 1000
        print(f"  t={(written_ms - t0_ms) / 1000:6.1f}s  outbox {entry['kind']} (messages={entry['messages']})  {entry['time']}")
PY
  echo
  drive status "$CONVERSATION_ID"
  drive page "$CONVERSATION_ID"
  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
} 2>&1 | tee "$LOG_FILE"
