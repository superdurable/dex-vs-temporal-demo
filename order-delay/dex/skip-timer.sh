#!/usr/bin/env bash
# Operator path with no code change: start a Flow on the v1 Worker and skip
# the Timer of SendToKitchenStep-1 with the Client skip_timer call. Prints
# when the Flow finished and its history. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export ORDER_DELAY_SECONDS="${ORDER_DELAY_SECONDS:-30}"
FLOW_ID="order-skip-timer-$(date -u +%H%M%S)"

start_server
log "change window: $ORDER_DELAY_SECONDS s"
start_worker v1
start_order v1 "$FLOW_ID" 48
for _ in $(seq 1 40); do
  show_history "$FLOW_ID" | grep -q "SendToKitchenStep.*waits for" && break
  sleep 0.5
done
log "$FLOW_ID: SendToKitchenStep registered its wait"
client v1 skip-timer "$FLOW_ID"
timed_result "$FLOW_ID"
show_history "$FLOW_ID"
