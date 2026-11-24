#!/usr/bin/env bash
# Runs a v1 reply and a v2 reply, each approved after 2 s, on a fresh
# `dexcli dev` server, and prints the v2 history. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
STAMP="$(date -u +%H%M%S)"

# Usage: reply v1|v2 <flow-id>
reply() {
  local started=$SECONDS
  client "$1" start "$2" "ticket-$STAMP"
  sleep 2
  client "$1" approve "$2"
  client "$1" result "$2" 60
  log "$2 finished $((SECONDS - started)) s after it started"
}

start_server
log "--- v1"
start_worker v1
reply v1 "reply-v1-$STAMP"
stop_worker
log "--- v2"
start_worker v2
reply v2 "reply-v2-$STAMP"
show_history "reply-v2-$STAMP"
