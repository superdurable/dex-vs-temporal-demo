#!/usr/bin/env bash
# Usage: upgrade.sh v2_no_patch|v2
# Starts five replies on the v1 worker and leaves them waiting for approval.
# Stops the v1 worker and starts a new worker process with the given code,
# so nothing is cached. Then:
#   old-approve-a, old-approve-b  get approve_reply
#   old-idle                      gets nothing
#   old-query                     is queried with `temporal workflow stack`
#   old-other-signal              gets a signal with no handler, which runs a
#                                 workflow task without approving
#   new-approve, new-idle         start on the new code; one is approved
# Waits for the timer plus 10 s, prints each status and history, and replays
# each saved old history against the given code offline. Stops everything
# on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
CODE="${1:?usage: upgrade.sh v2_no_patch|v2}"
export AGENT_REPLY_AUTO_SEND_SECONDS="${AGENT_REPLY_AUTO_SEND_SECONDS:-10}"
STAMP="$(date -u +%H%M%S)"
OLD=("old-approve-a-$STAMP" "old-approve-b-$STAMP" "old-idle-$STAMP" \
  "old-query-$STAMP" "old-other-signal-$STAMP")
NEW=("new-approve-$STAMP" "new-idle-$STAMP")

start_server
log "auto-send timer: $AGENT_REPLY_AUTO_SEND_SECONDS s; new code: $CODE"

log "--- five replies on v1"
start_worker v1
for id in "${OLD[@]}"; do
  client start "$id" "ticket-$id"
done
for id in "${OLD[@]}"; do
  wait_until_waiting "$id"
done
stop_worker

log "--- new worker process with $CODE"
start_worker "$CODE"
client approve "${OLD[0]}"
client approve "${OLD[1]}"
log "query ${OLD[3]} with temporal workflow stack"
temporal workflow stack --address "$TEMPORAL_ADDRESS" -w "${OLD[3]}" 2>&1 \
  | cut -c1-200 | head -5 || true
client signal "${OLD[4]}" ping
for id in "${NEW[@]}"; do
  client start "$id" "ticket-$id"
done
sleep 2
client approve "${NEW[0]}"

log "--- wait for the timer plus 10 s"
sleep "$((AGENT_REPLY_AUTO_SEND_SECONDS + 10))"
for id in "${OLD[@]}" "${NEW[@]}"; do
  client result "$id" 1
  client status "$id"
done

log "--- histories"
for id in "${OLD[@]}" "${NEW[@]}"; do
  log "$id"
  show_history "$id"
done
log "--- $CODE worker log"
cat "$RUN_DIR/worker-$CODE.log"

log "--- offline replay of each old history, saved after the run, against $CODE"
for id in "${OLD[@]}"; do
  (cd "$APP_DIR" && "$PYTHON" replay_check.py "$RUN_DIR/history-$id.json" "flow_$CODE" 2>/dev/null)
done
