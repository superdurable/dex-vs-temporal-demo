#!/usr/bin/env bash
# D5: flood one long-lived AI support conversation with 60,000 customer
# messages; the Flow answers each one with an AI reply. Record accepted
# messages, wall-clock time until every reply is recorded, final status,
# record count, retained chunks, and the Summary, then read the newest page
# and older pages. KEEP=1 leaves Dex running.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COUNT="${COUNT:-60000}"
RATE="${RATE:-0}"
CONCURRENCY="${CONCURRENCY:-16}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d5-flood-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d5-flood.log"

trap stop_everything_unless_kept EXIT

summarize_page='
import json, sys
page = json.load(sys.stdin)
records = page["records"]
print(json.dumps({
    "page_token": page["page_token"],
    "records": len(records),
    "customer_records": sum(1 for r in records if r["role"] == "customer"),
    "assistant_records": sum(1 for r in records if r["role"] == "assistant"),
    "sequences_newest_to_oldest": [records[0]["sequence"], records[-1]["sequence"]] if records else None,
    "next_older_page_token": page["next_older_page_token"],
    "oldest_retained_page_token": page["oldest_retained_page_token"],
    "is_beyond_retention": page["is_beyond_retention"],
    "newest_two": records[:2],
}, indent=2))
'

{
  start_dex_dev
  start_worker

  log "start $CONVERSATION_ID with idle ${IDLE_SECONDS}s, close after ${CLOSE_AFTER_SECONDS}s"
  drive start "$CONVERSATION_ID" --idle-seconds "$IDLE_SECONDS" \
    --close-after-seconds "$CLOSE_AFTER_SECONDS"
  drive assign "$CONVERSATION_ID" "flood-agent"

  log "flood $COUNT customer messages (rate=${RATE}, 0 is unlimited; concurrency=$CONCURRENCY)"
  flood_status=0
  # --max-seconds bounds the flood and the wait for replies at 30 minutes.
  drive flood "$CONVERSATION_ID" --count "$COUNT" --rate "$RATE" \
    --concurrency "$CONCURRENCY" --max-seconds 1800 || flood_status=$?
  log "flood exit status $flood_status"

  log "newest page (token current)"
  newest_page="$(drive page "$CONVERSATION_ID" current)"
  python3 -c "$summarize_page" <<<"$newest_page"

  older_token="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["next_older_page_token"])' <<<"$newest_page")"
  oldest_token="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["oldest_retained_page_token"])' <<<"$newest_page")"
  for token in "$older_token" "$oldest_token"; do
    [[ -n "$token" ]] || continue
    log "older page $token"
    drive page "$CONVERSATION_ID" "$token" | python3 -c "$summarize_page"
  done

  log "page older than the retention window (token 00000000000000000001)"
  drive page "$CONVERSATION_ID" 00000000000000000001

  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
} 2>&1 | tee "$LOG_FILE"
