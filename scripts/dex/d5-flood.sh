#!/usr/bin/env bash
# D5: flood one long-lived AI support conversation with COUNT customer
# messages, as fast as the producer limits allow; each customer message gets
# its own AI reply, in Channel order. COUNT=60000 takes far longer than the
# 30-minute posting bound at about 10 replies/s. The driver reports offered,
# acknowledged, unknown, and recorded messages, throttling, and a one-to-one
# check of message IDs and replies, then reads the newest and oldest pages.
# A backlog, a Flow failure, or a timeout fails the script.
# Producer policy: see LoadRun in dex/drive.py. KEEP=1 leaves Dex running.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COUNT="${COUNT:-60000}"
RATE="${RATE:-0}"
CONCURRENCY="${CONCURRENCY:-16}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d5-flood-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d5-flood.log"

main() {
  ensure_python_environment
  start_dex_dev
  start_worker

  log "start $CONVERSATION_ID with idle ${IDLE_SECONDS}s, close after ${CLOSE_AFTER_SECONDS}s"
  drive start "$CONVERSATION_ID" --first-message "Hi, I have many questions about my orders." \
    --idle-seconds "$IDLE_SECONDS" \
    --close-after-seconds "$CLOSE_AFTER_SECONDS"
  drive assign "$CONVERSATION_ID" "flood-agent"

  log "flood $COUNT customer messages (rate=${RATE}, 0 is unpaced; concurrency=$CONCURRENCY)"
  local flood_status=0
  # --max-seconds bounds the posting and, separately, the drain at 30 minutes.
  drive flood "$CONVERSATION_ID" --count "$COUNT" --rate "$RATE" \
    --concurrency "$CONCURRENCY" --max-seconds 1800 || flood_status=$?
  log "flood exit status $flood_status"

  drive pages-summary "$CONVERSATION_ID"
  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
  return "$flood_status"
}

run_logged "$LOG_FILE" main
