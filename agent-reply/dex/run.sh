#!/usr/bin/env bash
# Runs v1 (approved after 2 s), then v2 approved after 2 s and v2 left to
# the auto-send Timer, on a fresh `dexcli dev` server. Stops everything on
# exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export AGENT_REPLY_AUTO_SEND_SECONDS="${AGENT_REPLY_AUTO_SEND_SECONDS:-10}"
STAMP="$(date -u +%H%M%S)"

# Usage: reply v1|v2 <flow-id> <approve: yes|no>
reply() {
  local started=$SECONDS
  client "$1" start "$2" "ticket-$STAMP"
  if [[ "$3" == "yes" ]]; then
    sleep 2
    client "$1" approve "$2"
  fi
  client "$1" result "$2" 60
  log "$2 finished $((SECONDS - started)) s after it started"
}

start_server
log "auto-send timer: $AGENT_REPLY_AUTO_SEND_SECONDS s"

log "--- v1, approve after 2 s"
start_worker v1
reply v1 "reply-v1-$STAMP" yes
stop_worker

log "--- v2, approve after 2 s"
start_worker v2
reply v2 "reply-v2-approve-$STAMP" yes
log "--- v2, no approval"
reply v2 "reply-v2-timer-$STAMP" no
