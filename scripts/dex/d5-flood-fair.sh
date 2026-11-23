#!/usr/bin/env bash
# D5-fair: one long AI support conversation fed at a fixed rate the Flow can
# answer in real time. Post RATE customer messages per second (default 8) for
# DURATION_SECONDS (default 1800), each answered by its own reply. Snapshots
# are taken when the accepted count reaches each SNAPSHOT_AT value. Then report
# accepted, replies, backlog, Flow status, records, retained chunks, Summary,
# history events, and page reads. Runs Flow code v1. KEEP=1 leaves Dex running.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export SUPPORT_FLOW_VERSION=v1

RATE="${RATE:-8}"
DURATION_SECONDS="${DURATION_SECONDS:-1800}"
SNAPSHOT_AT="${SNAPSHOT_AT:-7300,14000}"
DRAIN_SECONDS="${DRAIN_SECONDS:-60}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d5-fair-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d5-flood-fair.log"
HISTORY_FILE="$RUN_DIR/d5-flood-fair-history.jsonl"

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
  drive start "$CONVERSATION_ID" --first-message "Hi, I have many questions about my orders." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS"

  log "post ${RATE} messages/s for ${DURATION_SECONDS}s (snapshots at accepted=$SNAPSHOT_AT)"
  paced_status=0
  drive paced "$CONVERSATION_ID" --rate "$RATE" --duration-seconds "$DURATION_SECONDS" \
    --snapshot-at "$SNAPSHOT_AT" --drain-seconds "$DRAIN_SECONDS" || paced_status=$?
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
  drive page "$CONVERSATION_ID" 00000000000000000001

  log "history events Dex keeps for this run"
  dexcli flow history "$CONVERSATION_ID" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 120s >"$HISTORY_FILE"
  python3 "$REPO_ROOT/scripts/dex/history_steps.py" count <"$HISTORY_FILE"
  python3 "$REPO_ROOT/scripts/dex/history_steps.py" timing --last 600 <"$HISTORY_FILE"

  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
} 2>&1 | tee "$LOG_FILE"
