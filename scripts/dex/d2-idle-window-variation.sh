#!/usr/bin/env bash
# D2 variation: does an already registered wait keep its old Timer after a deploy?
#  1. Worker with Flow code v1. Conversation C starts with a V1_IDLE_SECONDS
#     (default 60) idle window; the reply puts C in WaitForCustomerStep.
#  2. Deploy v2-short-idle (v2 plus a fixed 10-second idle window). Post nothing.
#  3. Record when the nudge arrives: about 60 s means the old Timer was kept.
#  4. Post one message. The reply enters WaitForCustomerStep again with v2
#     code, so the next nudge should come about 10 s after that reply.
# KEEP=0 (default) stops Dex at the end.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

V1_IDLE_SECONDS="${V1_IDLE_SECONDS:-60}"
CONVERSATION_C="${CONVERSATION_C:-d2-c-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d2-idle-window-variation.log"
HISTORY_DIR="$RUN_DIR/d2-history"

trap stop_everything_unless_kept EXIT

count_nudges() {
  [[ -f "$OUTBOX" ]] || { echo 0; return; }
  grep -c "\"conversation\": \"$CONVERSATION_C\", \"kind\": \"nudge\"" "$OUTBOX" || true
}

wait_for_nudge_count() {
  local expected="$1" deadline=$((SECONDS + $2))
  until (($(count_nudges) >= expected)); do
    if ((SECONDS > deadline)); then
      log "timed out waiting for nudge $expected"
      return 1
    fi
    sleep 1
  done
  log "nudge $expected is in the outbox"
}

{
  mkdir -p "$HISTORY_DIR"
  start_dex_dev

  export SUPPORT_FLOW_VERSION=v1
  start_worker
  log "conversation C on v1 with idle ${V1_IDLE_SECONDS}s: $CONVERSATION_C"
  drive start "$CONVERSATION_C" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$V1_IDLE_SECONDS"
  drive wait-replies "$CONVERSATION_C" 1

  log "deploy: stop the v1 Worker, start the v2-short-idle Worker (idle window 10 s)"
  stop_worker
  export SUPPORT_FLOW_VERSION=v2-short-idle
  start_worker

  log "post nothing; wait for the first nudge"
  wait_for_nudge_count 1 $((V1_IDLE_SECONDS + 60))

  log "post one message; the reply enters WaitForCustomerStep with v2-short-idle code"
  drive post "$CONVERSATION_C" "Sorry, I was away. Any news on the delivery?"
  drive wait-replies "$CONVERSATION_C" 2
  wait_for_nudge_count 2 60

  dexcli flow history "$CONVERSATION_C" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 60s >"$HISTORY_DIR/$CONVERSATION_C.jsonl"
  echo
  echo "Steps of $CONVERSATION_C (Worker ID shows which Flow code ran each method):"
  python3 "$REPO_ROOT/scripts/dex/history_steps.py" steps <"$HISTORY_DIR/$CONVERSATION_C.jsonl"
  echo
  echo "Replies and nudges of $CONVERSATION_C:"
  drive page "$CONVERSATION_C" | python3 -c '
import json, sys
for record in reversed(json.load(sys.stdin)["records"]):
    if record["role"] == "assistant":
        print("  reply recorded", record["recorded_at"])'
  grep "\"conversation\": \"$CONVERSATION_C\"" "$OUTBOX" | sed 's/^/  outbox /'
} 2>&1 | tee "$LOG_FILE"
