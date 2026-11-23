# Shared helpers for the Temporal demo scripts. Source this file; do not run it.
# Starts `temporal server start-dev` with default limits and the conversation
# worker, and stops both on exit unless KEEP=1.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEMPORAL_APP_DIR="$REPO_ROOT/temporal"
RUN_DIR="${RUN_DIR:-/tmp/dex-video/temporal-run}"
TEMPORAL_GRPC_PORT="${TEMPORAL_GRPC_PORT:-7233}"
TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-8233}"
export TEMPORAL_ADDRESS="localhost:$TEMPORAL_GRPC_PORT"
# Keep the virtual environment out of the repository.
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$RUN_DIR/venv}"
export PYTHONDONTWRITEBYTECODE=1

mkdir -p "$RUN_DIR"
STARTED_SERVER=0
SERVER_PID=""
WORKER_PID=""

epoch_now() {
  python3 -c 'import time; print(f"{time.time():.3f}")'
}

sleep_until_epoch() {
  local remaining
  remaining="$(python3 -c "import time; print(max(0.0, $1 - time.time()))")"
  sleep "$remaining"
}

drive() {
  uv run --quiet --project "$TEMPORAL_APP_DIR" python "$TEMPORAL_APP_DIR/drive.py" "$@"
}

server_is_healthy() {
  temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1
}

start_temporal_server() {
  local log_file="$1"
  if server_is_healthy; then
    echo "Reusing the Temporal server already listening on $TEMPORAL_ADDRESS."
    echo "Its limits are whatever it was started with; stop it first for a clean default run."
    return
  fi
  echo "Starting temporal server start-dev (default limits, UI http://localhost:$TEMPORAL_UI_PORT), log: $log_file"
  temporal server start-dev \
    --port "$TEMPORAL_GRPC_PORT" \
    --ui-port "$TEMPORAL_UI_PORT" \
    --log-level warn \
    >"$log_file" 2>&1 &
  SERVER_PID=$!
  STARTED_SERVER=1
  for _ in $(seq 1 60); do
    if server_is_healthy; then
      return
    fi
    sleep 0.5
  done
  echo "Temporal server did not become healthy; see $log_file" >&2
  exit 1
}

start_worker() {
  local log_file="$1"
  stop_worker_processes
  echo "Starting worker, log: $log_file"
  uv run --quiet --project "$TEMPORAL_APP_DIR" python "$TEMPORAL_APP_DIR/worker.py" \
    >"$log_file" 2>&1 &
  WORKER_PID=$!
  for _ in $(seq 1 60); do
    if grep -q "worker polling" "$log_file" 2>/dev/null; then
      return
    fi
    sleep 0.5
  done
  echo "Worker did not start; see $log_file" >&2
  exit 1
}

stop_worker_processes() {
  pkill -f "$TEMPORAL_APP_DIR/worker.py" 2>/dev/null || true
}

stop_all() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "KEEP=1: leaving the server (UI http://localhost:$TEMPORAL_UI_PORT) and the worker running."
    echo "Stop them with: pkill -f '$TEMPORAL_APP_DIR/worker.py'; pkill -f 'temporal server start-dev'"
    return
  fi
  stop_worker_processes
  if [ "$STARTED_SERVER" = "1" ] && [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
