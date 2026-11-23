#!/usr/bin/env bash
# D2: change Flow code while a conversation is open.
#  1. Start Dex and a Worker with Flow code v1.
#  2. Conversation A: first message plus 2 follow-ups, each answered.
#  3. Stop the v1 Worker; start a Worker with Flow code V2_VERSION (default v2,
#     which adds AnalyzeSentimentStep before GenerateReplyStep).
#  4. Post one more message to A (it waits in WaitForCustomerStep from v1).
#  5. Start conversation B on v2: first message plus 1 follow-up.
#  6. Print for A and B: Flow status, whether the last message got a reply,
#     and the Steps each ran after the deploy, with the Worker that ran them.
# KEEP=1 (default) leaves Dex and the v2 Worker running for Dex Web.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
KEEP="${KEEP:-1}"

V2_VERSION="${V2_VERSION:-v2}"
IDLE_SECONDS="${IDLE_SECONDS:-600}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
CONVERSATION_A="${CONVERSATION_A:-d2-a-$STAMP}"
CONVERSATION_B="${CONVERSATION_B:-d2-b-$STAMP}"
LOG_FILE="$RUN_DIR/d2-change-code.log"
HISTORY_DIR="$RUN_DIR/d2-history"

trap stop_everything_unless_kept EXIT

history_of() {
  dexcli flow history "$1" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 60s >"$HISTORY_DIR/$1.jsonl"
}

highest_event_id() {
  history_of "$1"
  python3 "$REPO_ROOT/scripts/dex/history_steps.py" count <"$HISTORY_DIR/$1.jsonl" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["highestEventId"])'
}

report_conversation() {
  local conversation="$1" since_event="$2"
  echo
  echo "=== $conversation"
  drive status "$conversation" | python3 -c '
import json, sys
status = json.load(sys.stdin)
records = status["latestRecords"]
print(json.dumps({
    "flowStatus": status["flowStatus"],
    "customerMessages": status["customerMessageCount"],
    "replies": status["assistantReplyCount"],
    "lastMessageGotReply": bool(records) and records[-1]["role"] == "assistant",
    "lastTwoRecords": records[-2:],
}, indent=2))'
  history_of "$conversation"
  echo "Steps from event $since_event on (Worker ID shows which Flow code ran each method):"
  python3 "$REPO_ROOT/scripts/dex/history_steps.py" steps --since-event "$since_event" \
    <"$HISTORY_DIR/$conversation.jsonl"
}

{
  mkdir -p "$HISTORY_DIR"
  start_dex_dev

  export SUPPORT_FLOW_VERSION=v1
  start_worker
  log "conversation A on v1: $CONVERSATION_A"
  drive start "$CONVERSATION_A" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$IDLE_SECONDS"
  drive wait-replies "$CONVERSATION_A" 1
  drive post "$CONVERSATION_A" "It was due last Friday. Can you check?"
  drive wait-replies "$CONVERSATION_A" 2
  drive post "$CONVERSATION_A" "The tracking page says the label was created only."
  drive wait-replies "$CONVERSATION_A" 3

  log "deploy: stop the v1 Worker, start the $V2_VERSION Worker"
  stop_worker
  deploy_event_a="$(highest_event_id "$CONVERSATION_A")"
  log "A's history before the deploy ends at event $deploy_event_a"
  export SUPPORT_FLOW_VERSION="$V2_VERSION"
  start_worker
  grep -i "flow definition" "$(worker_log_path)" || true

  log "conversation A after the deploy: one more message"
  drive post "$CONVERSATION_A" "This is wrong, I need it before the weekend."
  drive wait-replies "$CONVERSATION_A" 4

  log "conversation B on $V2_VERSION: $CONVERSATION_B"
  drive start "$CONVERSATION_B" --first-message "Hello, I want to change my delivery address." \
    --idle-seconds "$IDLE_SECONDS"
  drive wait-replies "$CONVERSATION_B" 1
  drive post "$CONVERSATION_B" "Thanks, the new address is the office one."
  drive wait-replies "$CONVERSATION_B" 2

  report_conversation "$CONVERSATION_A" 1
  echo "(A: events up to $deploy_event_a were written before the deploy; later events after it)"
  report_conversation "$CONVERSATION_B" 1

  web="http://127.0.0.1:$WEB_PORT"
  echo
  echo "Dex Web: run A $web/v1/flows/$CONVERSATION_A  run B $web/v1/flows/$CONVERSATION_B  Designs $web/v1/rendering"
} 2>&1 | tee "$LOG_FILE"
