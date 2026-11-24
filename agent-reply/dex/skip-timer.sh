#!/usr/bin/env bash
# Operator call, not part of the example: start a reply on v2 with a
# 5-minute auto-send Timer and skip that Timer with skip_timer.py. Prints when
# the Flow finished and its history. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export AGENT_REPLY_AUTO_SEND_SECONDS="${AGENT_REPLY_AUTO_SEND_SECONDS:-300}"
FLOW_ID="reply-skip-timer-$(date -u +%H%M%S)"

start_server
log "auto-send timer: $AGENT_REPLY_AUTO_SEND_SECONDS s"
start_worker v2
started=$SECONDS
client v2 start "$FLOW_ID" "ticket-$FLOW_ID"
wait_until_waiting "$FLOW_ID"
(cd "$APP_DIR" && AGENT_REPLY_CODE=v2 "$PYTHON" skip_timer.py "$FLOW_ID")
client v2 result "$FLOW_ID" 60
log "$FLOW_ID finished $((SECONDS - started)) s after it started"
show_history "$FLOW_ID"
