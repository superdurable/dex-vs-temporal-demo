#!/usr/bin/env bash
# In-flight upgrade: start a Flow on the v1 Worker, stop that Worker while
# SendToKitchenStep waits, start the v2 Worker, and see what happens. Run 1
# invokes no RPC; run 2 invokes send_order_now after the v2 Worker starts.
# Run 3 starts a fresh Flow on v2 and invokes send_order_now. Prints each
# history. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export ORDER_DELAY_SECONDS="${ORDER_DELAY_SECONDS:-30}"
STAMP="$(date -u +%H%M%S)"

wait_for_timer_wait() {
  for _ in $(seq 1 40); do
    if show_history "$1" | grep -q "SendToKitchenStep.*waits for"; then
      log "$1: SendToKitchenStep registered its wait"
      return
    fi
    sleep 0.5
  done
  log "ERROR: $1 did not register the SendToKitchenStep wait"
  exit 1
}

# Usage: upgrade_run <flow-id> <order-id> <invoke-rpc: yes|no>
upgrade_run() {
  start_worker v1
  start_order v1 "$1" "$2"
  wait_for_timer_wait "$1"
  stop_worker
  start_worker v2
  if [[ "$3" == "yes" ]]; then
    client v2 send-now "$1"
  fi
  timed_result "$1"
  stop_worker
  log "v2 Worker log:"
  cat "$RUN_DIR/worker-v2.log"
  show_history "$1"
}

start_server
log "change window: $ORDER_DELAY_SECONDS s"
log "--- run 1: v1 -> v2 while waiting, no RPC"
upgrade_run "order-upgrade-wait-$STAMP" 45 no
log "--- run 2: v1 -> v2 while waiting, then send_order_now"
upgrade_run "order-upgrade-now-$STAMP" 46 yes
log "--- run 3: fresh Flow on v2, send_order_now after 2 s"
start_worker v2
start_order v2 "order-fresh-v2-$STAMP" 47
sleep 2
client v2 send-now "order-fresh-v2-$STAMP"
timed_result "order-fresh-v2-$STAMP"
show_history "order-fresh-v2-$STAMP"
