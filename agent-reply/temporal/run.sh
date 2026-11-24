#!/usr/bin/env bash
# Runs a v1 reply and a v2 reply, each approved after 2 s, on a fresh
# Temporal dev server, and prints the v2 history. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
STAMP="$(date -u +%H%M%S)"

# Usage: reply <workflow-id>
reply() {
  local started=$SECONDS
  client start "$1" "ticket-$STAMP"
  sleep 2
  client approve "$1"
  client result "$1" 60
  log "$1 finished $((SECONDS - started)) s after it started"
}

start_server
log "--- v1"
start_worker v1
reply "reply-v1-$STAMP"
stop_worker
log "--- v2"
start_worker v2
reply "reply-v2-$STAMP"
show_history "reply-v2-$STAMP"
