#!/usr/bin/env bash
# Runs v1, then v2 without and with the send_order_now RPC, on a fresh
# `dexcli dev` server. Stops everything on exit.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
export ORDER_DELAY_SECONDS="${ORDER_DELAY_SECONDS:-10}"
STAMP="$(date -u +%H%M%S)"

start_server
log "change window: $ORDER_DELAY_SECONDS s"

log "--- v1"
start_worker v1
start_order v1 "order-v1-$STAMP" 42
timed_result "order-v1-$STAMP"
stop_worker

log "--- v2, no RPC"
start_worker v2
start_order v2 "order-v2-wait-$STAMP" 43
timed_result "order-v2-wait-$STAMP"

log "--- v2, send_order_now after 2 s"
start_order v2 "order-v2-now-$STAMP" 44
sleep 2
client v2 send-now "order-v2-now-$STAMP"
timed_result "order-v2-now-$STAMP"
