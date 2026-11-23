#!/usr/bin/env bash
# D7 (Dex-only demonstration of the bounded transcript): post MESSAGE_CHARACTERS
# (default 8,192) character customer messages at RATE per second (default 8)
# for DURATION_SECONDS (default 900), so the transcript reaches its 32 MiB
# budget within minutes and its oldest chunks are folded into the Summary.
# Progress lines show the transcript bytes over time. The driver checks the
# retained transcript record by record (serialized bytes equal the
# bookkeeping and stay within the budget), the dropped part by the Summary
# counters, and message IDs against replies. Then it reports labeled page
# reads, failed Step events in the retained history, and RESOURCE_EXHAUSTED
# lines in the server log. MESSAGE_KIND=non-bmp uses 4-byte characters.
# Producer policy: see LoadRun in dex/drive.py. Runs Flow code v1.
# KEEP=1 leaves Dex running after a successful run.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export SUPPORT_FLOW_VERSION=v1

RATE="${RATE:-8}"
DURATION_SECONDS="${DURATION_SECONDS:-900}"
MESSAGE_CHARACTERS="${MESSAGE_CHARACTERS:-8192}"
MESSAGE_KIND="${MESSAGE_KIND:-ascii}"
DRAIN_SECONDS="${DRAIN_SECONDS:-120}"
CONVERSATION_ID="${CONVERSATION_ID:-d7-budget-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d7-transcript-budget.log"
HISTORY_FILE="$RUN_DIR/d7-transcript-budget-history.jsonl"

main() {
  ensure_python_environment
  start_dex_dev
  start_worker

  log "start $CONVERSATION_ID"
  drive start "$CONVERSATION_ID" --first-message "Hi, I have many questions about my orders." \
    --idle-seconds 86400 --close-after-seconds 3600

  log "post ${RATE} messages/s of ${MESSAGE_CHARACTERS} $MESSAGE_KIND characters for ${DURATION_SECONDS}s"
  local paced_status=0
  drive paced "$CONVERSATION_ID" --rate "$RATE" --duration-seconds "$DURATION_SECONDS" \
    --message-characters "$MESSAGE_CHARACTERS" --message-kind "$MESSAGE_KIND" \
    --drain-seconds "$DRAIN_SECONDS" || paced_status=$?
  log "paced exit status $paced_status"

  drive pages-summary "$CONVERSATION_ID"
  drive status "$CONVERSATION_ID" | python3 -c '
import json, sys
status = json.load(sys.stdin)
print(json.dumps({key: status[key] for key in (
    "flowStatus", "conversationStatus", "customerMessageCount", "assistantReplyCount",
    "transcriptBytes", "transcriptBudgetBytes", "retainedArchivedChunkCount",
    "oldestArchivedChunkToken", "newestArchivedChunkToken", "summary", "history")}, indent=2))'

  log "failed Step events and RESOURCE_EXHAUSTED"
  dexcli flow history "$CONVERSATION_ID" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 120s >"$HISTORY_FILE"
  checks failures --history "$HISTORY_FILE" \
    --server-log "$RUN_DIR/server-logs/dex-server-zd.log" --worker-log "$(worker_log_path)"

  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
  return "$paced_status"
}

run_logged "$LOG_FILE" main
