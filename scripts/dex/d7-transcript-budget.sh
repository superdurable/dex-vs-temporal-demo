#!/usr/bin/env bash
# D7: transcript retention by size. Post MESSAGE_BYTES (default 8 KiB) customer
# messages at RATE per second (default 8) for DURATION_SECONDS (default 900),
# so the transcript passes its 32 MiB budget within minutes. Progress lines
# show the transcript bytes over time. At the end report the dropped chunks
# (Summary), Flow status, page reads, any failed Step, and server log lines
# with RESOURCE_EXHAUSTED. Runs Flow code v1. KEEP=1 leaves Dex running.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export SUPPORT_FLOW_VERSION=v1

RATE="${RATE:-8}"
DURATION_SECONDS="${DURATION_SECONDS:-900}"
MESSAGE_BYTES="${MESSAGE_BYTES:-8192}"
DRAIN_SECONDS="${DRAIN_SECONDS:-120}"
CONVERSATION_ID="${CONVERSATION_ID:-d7-budget-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d7-transcript-budget.log"
HISTORY_FILE="$RUN_DIR/d7-transcript-budget-history.jsonl"

trap stop_everything_unless_kept EXIT

summarize_page='
import json, sys
page = json.load(sys.stdin)
records = page["records"]
print(json.dumps({
    "page_token": page["page_token"],
    "records": len(records),
    "sequences_newest_to_oldest": [records[0]["sequence"], records[-1]["sequence"]] if records else None,
    "next_older_page_token": page["next_older_page_token"],
    "oldest_retained_page_token": page["oldest_retained_page_token"],
    "is_beyond_retention": page["is_beyond_retention"],
}, indent=2))
'

{
  start_dex_dev
  start_worker

  log "start $CONVERSATION_ID"
  drive start "$CONVERSATION_ID" --first-message "Hi, I have many questions about my orders." \
    --idle-seconds 86400 --close-after-seconds 3600

  log "post ${RATE} messages/s of ${MESSAGE_BYTES} bytes for ${DURATION_SECONDS}s"
  paced_status=0
  drive paced "$CONVERSATION_ID" --rate "$RATE" --duration-seconds "$DURATION_SECONDS" \
    --message-bytes "$MESSAGE_BYTES" --drain-seconds "$DRAIN_SECONDS" || paced_status=$?
  log "paced exit status $paced_status"

  log "newest page (token current)"
  newest_page="$(drive page "$CONVERSATION_ID" current)"
  python3 -c "$summarize_page" <<<"$newest_page"
  oldest_token="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["oldest_retained_page_token"])' <<<"$newest_page")"
  if [[ -n "$oldest_token" ]]; then
    log "oldest retained page $oldest_token"
    drive page "$CONVERSATION_ID" "$oldest_token" | python3 -c "$summarize_page"
  fi
  log "page older than the retention window (token 00000000000000000001)"
  drive page "$CONVERSATION_ID" 00000000000000000001 | python3 -c "$summarize_page"

  log "failed Step attempts and RESOURCE_EXHAUSTED in history and server log"
  dexcli flow history "$CONVERSATION_ID" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 120s >"$HISTORY_FILE"
  python3 "$REPO_ROOT/scripts/dex/history_steps.py" count <"$HISTORY_FILE"
  echo "history events with Fail or RESOURCE_EXHAUSTED: $(grep -o -E '"type":"[A-Za-z]*Fail[A-Za-z]*"|RESOURCE_EXHAUSTED' "$HISTORY_FILE" | sort | uniq -c | tr '\n' ' ')"
  echo "server log lines with RESOURCE_EXHAUSTED: $(grep -c RESOURCE_EXHAUSTED "$RUN_DIR/server-logs/dex-server-zd.log" || true)"
  echo "Worker log lines with Error or Traceback: $(grep -c -E 'Error|Traceback' "$(worker_log_path)" || true)"

  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
} 2>&1 | tee "$LOG_FILE"
