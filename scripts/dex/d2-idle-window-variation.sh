#!/usr/bin/env bash
# D2 variation: does an already registered wait keep its old Timer after a deploy?
#  1. Worker demo-worker-v1 with Flow code v1. Conversation C starts with a
#     V1_IDLE_SECONDS (default 60) idle window. Before the deploy, wait until
#     C's WaitForCustomerStep-1 registered that wait on v1.
#  2. Deploy v2-short-idle (v2 plus a fixed 10-second idle window). Post nothing.
#  3. The first nudge comes about 60 s after the wait was registered: the
#     wait registered on v1 kept its Timer.
#  4. Post one message. The reply enters WaitForCustomerStep again with v2
#     code, so the next nudge comes about 10 s after that wait.
# Asserts both wait-to-nudge intervals (with a tolerance), the Timer condition
# and Worker of each wait, and two distinct nudge notice keys.
# KEEP=0 (default) stops Dex at the end.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

V1_IDLE_SECONDS="${V1_IDLE_SECONDS:-60}"
SHORT_IDLE_SECONDS=10
CONVERSATION_C="${CONVERSATION_C:-d2-c-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d2-idle-window-variation.log"
HISTORY_DIR="$RUN_DIR/d2-history"
HISTORY_C="$HISTORY_DIR/conversation-c.jsonl"

main() {
  mkdir -p "$HISTORY_DIR"
  ensure_python_environment
  start_dex_dev

  export SUPPORT_FLOW_VERSION=v1
  start_worker
  log "conversation C on v1 with idle ${V1_IDLE_SECONDS}s: $CONVERSATION_C"
  local start_output run_id
  start_output="$(drive start "$CONVERSATION_C" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds "$V1_IDLE_SECONDS")"
  echo "$start_output"
  run_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["runId"])' <<<"$start_output")"
  drive wait-replies "$CONVERSATION_C" 1
  log "before the deploy: wait until WaitForCustomerStep-1 registered its wait on v1"
  checks await-wait --flow-id "$CONVERSATION_C" --step-execution WaitForCustomerStep-1 \
    --server "$DEX_SERVER_ADDRESS" --timeout-seconds 30 --history-out "$HISTORY_C"

  log "deploy: stop the v1 Worker, start the v2-short-idle Worker (idle window 10 s)"
  stop_worker
  export SUPPORT_FLOW_VERSION=v2-short-idle
  start_worker

  log "post nothing; wait for the first nudge"
  checks outbox --outbox "$OUTBOX" --conversation "$CONVERSATION_C" --run-id "$run_id" \
    --kind nudge --wait-count 1 --timeout-seconds $((V1_IDLE_SECONDS + 60)) >/dev/null
  log "nudge 1 is in the outbox"

  log "post one message; the reply enters WaitForCustomerStep with v2-short-idle code"
  drive post "$CONVERSATION_C" "Sorry, I was away. Any news on the delivery?" --message-id c-2
  drive wait-replies "$CONVERSATION_C" 2
  checks outbox --outbox "$OUTBOX" --conversation "$CONVERSATION_C" --run-id "$run_id" \
    --kind nudge --wait-count 2 --timeout-seconds 60
  log "nudge 2 is in the outbox"

  dexcli flow history "$CONVERSATION_C" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 60s >"$HISTORY_C"
  echo
  echo "Steps of $CONVERSATION_C (Worker ID shows which Flow code ran each method):"
  python3 "$SCRIPTS_DIR/history_steps.py" steps <"$HISTORY_C"
  echo
  echo "Wait-to-nudge intervals:"
  checks nudge-intervals --history "$HISTORY_C" --expect "$V1_IDLE_SECONDS,$SHORT_IDLE_SECONDS" \
    --wait-workers demo-worker-v1,demo-worker-v2-short-idle
}

run_logged "$LOG_FILE" main
