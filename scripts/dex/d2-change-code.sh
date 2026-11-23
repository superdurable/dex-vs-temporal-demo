#!/usr/bin/env bash
# D2: change Flow code while a conversation is open.
#  1. Start Dex and the Worker demo-worker-v1 with Flow code v1.
#  2. Conversation A: first message plus 2 follow-ups, each answered.
#  3. Before the deploy, wait until A's history shows WaitForCustomerStep-3
#     registered its wait on v1; record the Worker, Timer, and event ID.
#  4. Stop the v1 Worker; start demo-worker-$V2_VERSION (default v2, which adds
#     AnalyzeSentimentStep before GenerateReplyStep).
#  5. Post one more message to A. Assert: the v2 Worker finished the wait
#     registered on v1, then ran AnalyzeSentimentStep, GenerateReplyStep, and
#     the next wait; the new reply carries the sentiment tag.
#  6. Start conversation B on v2 with one follow-up. Assert: B started at
#     AnalyzeSentimentStep on the v2 Worker, and both messages got replies.
# Any failed assertion fails the script. KEEP=1 (default) leaves Dex and the
# v2 Worker running for Dex Web after a successful run.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
KEEP="${KEEP:-1}"

V2_VERSION="${V2_VERSION:-v2}"
IDLE_SECONDS="${IDLE_SECONDS:-600}"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
CONVERSATION_A="${CONVERSATION_A:-d2-a-$STAMP}"
CONVERSATION_B="${CONVERSATION_B:-d2-b-$STAMP}"
LOG_FILE="$RUN_DIR/d2-change-code.log"
HISTORY_DIR="$RUN_DIR/d2-history"
HISTORY_A="$HISTORY_DIR/conversation-a.jsonl"
HISTORY_B="$HISTORY_DIR/conversation-b.jsonl"

save_history() {
  dexcli flow history "$1" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 60s >"$2"
}

json_field() {
  python3 -c '
import json, sys
value = json.load(sys.stdin)
for key in sys.argv[1].split("."):
    value = value[int(key)] if isinstance(value, list) else value[key]
print(value)' "$1"
}

# The Server keeps a stored definition unless the upload is different and
# generated later. Re-uploading the Worker's file reports UNCHANGED only when
# the Server stores exactly that file, so Dex Web Designs shows this version.
assert_stored_flow_definition() {
  local version="$1" definition outcome
  definition="$DEX_APP_DIR/flow-definitions/$version/SupportConversationFlow.json"
  outcome="$(dexcli flow-definition upload "$definition" \
    -server "$DEX_SERVER_ADDRESS" -timeout 30s | json_field definitions.0.outcome)"
  if [[ "$outcome" != "UNCHANGED" ]]; then
    log "ERROR: the Server did not store the $version Flow definition (re-upload outcome $outcome)"
    return 1
  fi
  log "the Server stores the $version Flow definition"
}

main() {
  mkdir -p "$HISTORY_DIR"
  ensure_python_environment
  start_dex_dev

  export SUPPORT_FLOW_VERSION=v1
  start_worker
  log "conversation A on v1: $CONVERSATION_A"
  drive start "$CONVERSATION_A" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$IDLE_SECONDS"
  drive wait-replies "$CONVERSATION_A" 1
  drive post "$CONVERSATION_A" "It was due last Friday. Can you check?" --message-id a-2
  drive wait-replies "$CONVERSATION_A" 2
  drive post "$CONVERSATION_A" "The tracking page says the label was created only." --message-id a-3
  drive wait-replies "$CONVERSATION_A" 3

  log "before the deploy: wait until A's WaitForCustomerStep-3 registered its wait on v1"
  local wait_json wait_event
  wait_json="$(checks await-wait --flow-id "$CONVERSATION_A" --step-execution WaitForCustomerStep-3 \
    --server "$DEX_SERVER_ADDRESS" --timeout-seconds 30 --history-out "$HISTORY_A")"
  echo "$wait_json"
  wait_event="$(json_field waitRegistered.eventId <<<"$wait_json")"
  if [[ "$(json_field waitRegistered.workerId <<<"$wait_json")" != "demo-worker-v1" ]]; then
    log "ERROR: the wait was not registered by demo-worker-v1"
    return 1
  fi

  log "deploy: stop the v1 Worker, start the $V2_VERSION Worker"
  stop_worker
  export SUPPORT_FLOW_VERSION="$V2_VERSION"
  start_worker
  grep -i "flow definition" "$(worker_log_path)" || true
  assert_stored_flow_definition "$V2_VERSION"

  log "conversation A after the deploy: one more message"
  drive post "$CONVERSATION_A" "This is wrong, I need it before the weekend." --message-id a-4
  local replies_a
  replies_a="$(drive wait-replies "$CONVERSATION_A" 4)"
  echo "$replies_a"
  checks await-wait --flow-id "$CONVERSATION_A" --step-execution WaitForCustomerStep-4 \
    --server "$DEX_SERVER_ADDRESS" --timeout-seconds 30 --history-out "$HISTORY_A" >/dev/null

  log "conversation B on $V2_VERSION: $CONVERSATION_B"
  drive start "$CONVERSATION_B" --first-message "Hello, I want to change my delivery address." \
    --idle-seconds "$IDLE_SECONDS"
  drive wait-replies "$CONVERSATION_B" 1
  drive post "$CONVERSATION_B" "Thanks, the new address is the office one." --message-id b-2
  drive wait-replies "$CONVERSATION_B" 2
  save_history "$CONVERSATION_B" "$HISTORY_B"

  echo
  echo "=== A: Steps (Worker ID shows which Flow code ran each method; event $wait_event is the wait registered on v1)"
  python3 "$SCRIPTS_DIR/history_steps.py" steps <"$HISTORY_A"
  echo "=== A: assertions"
  checks check-after-deploy --history "$HISTORY_A" --wait-event "$wait_event" \
    --new-worker "demo-worker-$V2_VERSION" \
    --expect-steps AnalyzeSentimentStep,GenerateReplyStep
  python3 -c '
import json, sys
newest = json.load(sys.stdin)["newestTwoRecords"][-1]
if newest["role"] != "assistant" or not any(c.startswith("analyze_sentiment:") for c in newest["tool_calls"]):
    print(json.dumps({"check": "failed", "reason": "the reply after the deploy has no sentiment tag", "record": newest}))
    sys.exit(1)
print(json.dumps({"check": "passed", "replyToolCalls": newest["tool_calls"]}))' <<<"$replies_a"

  echo
  echo "=== B: Steps"
  python3 "$SCRIPTS_DIR/history_steps.py" steps <"$HISTORY_B"
  echo "=== B: assertions"
  checks check-start --history "$HISTORY_B" --start-step AnalyzeSentimentStep \
    --worker "demo-worker-$V2_VERSION"

  local web="http://127.0.0.1:$WEB_PORT"
  echo
  echo "Dex Web: run A $web/v1/flows/$CONVERSATION_A  run B $web/v1/flows/$CONVERSATION_B  Designs $web/v1/rendering"
}

run_logged "$LOG_FILE" main
