#!/usr/bin/env bash
# Runs v1 (approved after 2 s), then v2 approved after 2 s and v2 left to
# the auto-send timer, on a fresh Temporal dev server. Stops everything on
# exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export AGENT_REPLY_AUTO_SEND_SECONDS="${AGENT_REPLY_AUTO_SEND_SECONDS:-10}"
STAMP="$(date -u +%H%M%S)"

# Usage: reply <workflow-id> <approve: yes|no>
reply() {
  local started=$SECONDS
  client start "$1" "ticket-$STAMP"
  if [[ "$2" == "yes" ]]; then
    sleep 2
    client approve "$1"
  fi
  client result "$1" 60
  log "$1 finished $((SECONDS - started)) s after it started"
}

start_server
log "auto-send timer: $AGENT_REPLY_AUTO_SEND_SECONDS s"

log "--- v1, approve after 2 s"
start_worker v1
reply "reply-v1-$STAMP" yes
stop_worker

log "--- v2, approve after 2 s"
start_worker v2
reply "reply-v2-approve-$STAMP" yes
log "--- v2, no approval"
reply "reply-v2-timer-$STAMP" no
