#!/usr/bin/env bash
# Runs a v1 reply and a v2 reply, each approved after 2 s, on a fresh
# Temporal dev server. Checks that both complete and which activities they
# scheduled; exits 1 otherwise. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
STAMP="$(date -u +%H%M%S)"

# Usage: reply <workflow-id>
reply() {
  client start "$1" "ticket-$STAMP"
  sleep 2
  client approve "$1"
  client result "$1" 60
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

expect_status "reply-v1-$STAMP" COMPLETED
expect_activities "reply-v1-$STAMP" draft_reply send_reply
expect_status "reply-v2-$STAMP" COMPLETED
expect_activities "reply-v2-$STAMP" draft_reply notify_reviewer send_reply
finish_checks
