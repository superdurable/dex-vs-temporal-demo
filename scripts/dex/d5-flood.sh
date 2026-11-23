#!/usr/bin/env bash
# D5: flood one long-lived conversation with 60,000 messages. Record accepted
# messages, wall-clock time, final status, retained chunks, and the Summary,
# then read the newest page and one older page. KEEP=1 leaves Dex running.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COUNT="${COUNT:-60000}"
RATE="${RATE:-0}"
CONCURRENCY="${CONCURRENCY:-16}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
CONVERSATION_ID="${CONVERSATION_ID:-d5-flood-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d5-flood.log"

trap stop_everything_unless_kept EXIT

{
  start_dex_dev
  start_worker

  log "start $CONVERSATION_ID with idle ${IDLE_SECONDS}s"
  drive start "$CONVERSATION_ID" --idle-seconds "$IDLE_SECONDS"
  drive assign "$CONVERSATION_ID" "flood-agent"

  log "flood $COUNT messages (rate=${RATE:-unlimited}, concurrency=$CONCURRENCY)"
  flood_status=0
  # --max-seconds bounds the whole flood at 30 minutes.
  drive flood "$CONVERSATION_ID" --count "$COUNT" --rate "$RATE" \
    --concurrency "$CONCURRENCY" --max-seconds 1800 || flood_status=$?
  log "flood exit status $flood_status"

  # The waiting Step records queued messages shortly after the last RPC.
  sleep 5
  log "final status"
  drive status "$CONVERSATION_ID"

  log "newest page (token current)"
  newest_page="$(drive page "$CONVERSATION_ID" current)"
  python3 -c 'import json,sys; p=json.loads(sys.argv[1]); r=p["records"]; print(json.dumps({"page_token": p["page_token"], "records": len(r), "newest_sequence": r[0]["sequence"] if r else None, "next_older_page_token": p["next_older_page_token"], "oldest_retained_page_token": p["oldest_retained_page_token"]}))' "$newest_page"

  older_token="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["next_older_page_token"])' "$newest_page")"
  oldest_token="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["oldest_retained_page_token"])' "$newest_page")"
  for token in "$older_token" "$oldest_token"; do
    [[ -n "$token" ]] || continue
    log "older page $token"
    drive page "$CONVERSATION_ID" "$token" | python3 -c 'import json,sys; p=json.load(sys.stdin); r=p["records"]; print(json.dumps({"page_token": p["page_token"], "records": len(r), "sequences": [r[0]["sequence"], r[-1]["sequence"]] if r else None, "next_older_page_token": p["next_older_page_token"], "first_text": r[-1]["text"][:40] if r else None}))'
  done

  log "page older than the retention window (token 00000000000000000001)"
  drive page "$CONVERSATION_ID" 00000000000000000001

  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
} 2>&1 | tee "$LOG_FILE"
