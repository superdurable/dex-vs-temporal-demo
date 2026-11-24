# Helpers for the Temporal order-delay scripts. Source it; do not run it.
# Starts `temporal server start-dev` and workers, and stops what this shell
# started on exit.
#
# Environment:
#   RUN_DIR              logs, venv, server state (default /tmp/dex-video/two-step/temporal-run)
#   TEMPORAL_GRPC_PORT   server gRPC port (default 17233)
#   TEMPORAL_UI_PORT     server Web UI port (default 18233)
#   ORDER_DELAY_SECONDS  change window (scripts default to a few seconds)

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${RUN_DIR:-/tmp/dex-video/two-step/temporal-run}"
TEMPORAL_GRPC_PORT="${TEMPORAL_GRPC_PORT:-17233}"
TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-18233}"
export TEMPORAL_ADDRESS="localhost:$TEMPORAL_GRPC_PORT"
export UV_PROJECT_ENVIRONMENT="$RUN_DIR/venv"
export UV_CACHE_DIR="$RUN_DIR/uv-cache"
export PYTHONDONTWRITEBYTECODE=1
PYTHON="$RUN_DIR/venv/bin/python"
SERVER_PID=""
WORKER_PID=""

mkdir -p "$RUN_DIR"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

client() { (cd "$APP_DIR" && "$PYTHON" client.py "$@"); }

start_server() {
  if temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; then
    log "ERROR: a server already listens on $TEMPORAL_ADDRESS; choose other ports"
    exit 1
  fi
  (cd "$APP_DIR" && uv sync --locked -q)
  log "starting temporal server start-dev on $TEMPORAL_ADDRESS"
  temporal server start-dev --port "$TEMPORAL_GRPC_PORT" \
    --ui-port "$TEMPORAL_UI_PORT" --log-level warn \
    >"$RUN_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 60); do
    temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1 && return
    sleep 0.5
  done
  log "ERROR: server did not become healthy; see $RUN_DIR/server.log"
  exit 1
}

# Usage: start_worker v1|v2
start_worker() {
  local code="$1" worker_log="$RUN_DIR/worker-$1.log"
  (cd "$APP_DIR" && exec env ORDER_CODE="$code" "$PYTHON" worker.py) \
    >"$worker_log" 2>&1 &
  WORKER_PID=$!
  for _ in $(seq 1 60); do
    grep -q "worker polling" "$worker_log" 2>/dev/null && { log "$(cat "$worker_log")"; return; }
    sleep 0.5
  done
  log "ERROR: worker $code did not start; see $worker_log"
  exit 1
}

stop_worker() {
  [[ -n "$WORKER_PID" ]] || return 0
  kill -TERM "$WORKER_PID" 2>/dev/null || true
  wait "$WORKER_PID" 2>/dev/null || true
  WORKER_PID=""
  log "worker stopped"
}

stop_all() {
  stop_worker
  if [[ -n "$SERVER_PID" ]]; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
    log "server stopped"
  fi
}
trap stop_all EXIT

STARTED_AT=0

# Usage: start_order <workflow-id> <order-id>
start_order() {
  STARTED_AT=$(date +%s)
  client start "$1" "$2"
}

# Usage: timed_result <workflow-id>; prints the result and the seconds since start_order.
timed_result() {
  client result "$1"
  log "$1 finished $(($(date +%s) - STARTED_AT)) s after it started"
}

# Usage: show_history <workflow-id>; prints event types and saves the JSON.
show_history() {
  temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$1" -o json \
    >"$RUN_DIR/history-$1.json"
  temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$1"
}
