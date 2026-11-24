#!/usr/bin/env bash
# In-flight upgrade: start a workflow on the v1 worker, stop that worker while
# the workflow sleeps, start the v2 worker (no workflow.patched call), and see
# what happens. Run 1 sends no signal; run 2 sends send_order_now after v2
# starts. Prints both histories and replays each saved history against v2
# offline. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export ORDER_DELAY_SECONDS="${ORDER_DELAY_SECONDS:-30}"
STAMP="$(date -u +%H%M%S)"

wait_for_timer_started() {
  for _ in $(seq 1 40); do
    temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$1" -o json \
      | grep -q TIMER_STARTED && { log "$1 has started its timer"; return; }
    sleep 0.5
  done
  log "ERROR: $1 did not start its timer"
  exit 1
}

# Usage: upgrade_run <workflow-id> <order-id> <send-signal: yes|no>
upgrade_run() {
  start_worker v1
  start_order "$1" "$2"
  wait_for_timer_started "$1"
  stop_worker
  start_worker v2
  if [[ "$3" == "yes" ]]; then
    client send-now "$1"
  fi
  timed_result "$1"
  stop_worker
  log "v2 worker log:"
  cat "$RUN_DIR/worker-v2.log"
  show_history "$1"
  (cd "$APP_DIR" && "$PYTHON" replay_check.py "$RUN_DIR/history-$1.json" flow_v2)
}

start_server
log "change window: $ORDER_DELAY_SECONDS s"
log "--- run 1: v1 -> v2 while sleeping, no signal"
upgrade_run "order-upgrade-wait-$STAMP" 45 no
log "--- run 2: v1 -> v2 while sleeping, then send_order_now"
upgrade_run "order-upgrade-now-$STAMP" 46 yes
