# Shared helpers for the Temporal demo scripts. Source this file; do not run it.
# Starts `temporal server start-dev` with default limits and the conversation
# worker, and stops both on exit unless KEEP=1.
#
# Environment:
#   RUN_DIR             logs, PID files, outbox and run state (default /tmp/dex-video/temporal-run)
#   TEMPORAL_GRPC_PORT  server gRPC port (default 7233)
#   TEMPORAL_UI_PORT    server Web UI port (default 8233)
#   KEEP=1              leave the server and the worker running on exit

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEMPORAL_APP_DIR="$REPO_ROOT/temporal"
RUN_DIR="${RUN_DIR:-/tmp/dex-video/temporal-run}"
TEMPORAL_GRPC_PORT="${TEMPORAL_GRPC_PORT:-7233}"
TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-8233}"
export TEMPORAL_ADDRESS="localhost:$TEMPORAL_GRPC_PORT"
# Keep the virtual environment and the outbox out of the repository.
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$RUN_DIR/venv}"
export DEMO_OUTBOX_DIR="${DEMO_OUTBOX_DIR:-$RUN_DIR/outbox}"
export PYTHONDONTWRITEBYTECODE=1

mkdir -p "$RUN_DIR"
SERVER_PID_FILE="$RUN_DIR/server.pid"
WORKER_PID_FILE="$RUN_DIR/worker.pid"
STARTED_SERVER=0

epoch_now() {
  python3 -c 'import time; print(f"{time.time():.3f}")'
}

# Runs drive.py; exit codes: 0 as expected, 1 command or observation failed, 2 assertion failed.
drive() {
  uv run --locked --quiet --project "$TEMPORAL_APP_DIR" python "$TEMPORAL_APP_DIR/drive.py" "$@"
}

server_is_healthy() {
  temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1
}

print_versions() {
  echo "versions: $(temporal --version 2>&1 | head -1); $(uv run --locked --quiet --project "$TEMPORAL_APP_DIR" python -c 'import sys, importlib.metadata as m; print("python " + sys.version.split()[0] + ", temporalio " + m.version("temporalio"))')"
}

# Usage: start_temporal_server <log-file> [fresh]
# With "fresh", refuses to reuse a server that is already listening.
start_temporal_server() {
  local log_file="$1"
  local mode="${2:-}"
  if server_is_healthy; then
    if [ "$mode" = "fresh" ]; then
      echo "A server is already listening on $TEMPORAL_ADDRESS; this run needs a fresh one with default limits." >&2
      echo "Stop it, or choose other ports with TEMPORAL_GRPC_PORT and TEMPORAL_UI_PORT." >&2
      exit 1
    fi
    echo "REUSING the Temporal server already listening on $TEMPORAL_ADDRESS (its limits are whatever it was started with)."
    return
  fi
  echo "Starting a fresh temporal server start-dev with default limits and no dynamic config overrides"
  echo "  (gRPC $TEMPORAL_ADDRESS, UI http://localhost:$TEMPORAL_UI_PORT), log: $log_file"
  temporal server start-dev \
    --port "$TEMPORAL_GRPC_PORT" \
    --ui-port "$TEMPORAL_UI_PORT" \
    --log-level warn \
    >"$log_file" 2>&1 &
  echo "$!" >"$SERVER_PID_FILE"
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

# Usage: stop_pid_from_file <pid-file> <command-pattern> <name>
# Stops the recorded process only if its command line still matches, then waits for it to exit.
stop_pid_from_file() {
  local pid_file="$1"
  local pattern="$2"
  local name="$3"
  [ -f "$pid_file" ] || return 0
  local pid
  pid="$(cat "$pid_file")"
  rm -f "$pid_file"
  if ! ps -p "$pid" -o command= 2>/dev/null | grep -qF -- "$pattern"; then
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 60); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Stopped the $name (PID $pid)."
      return 0
    fi
    sleep 0.5
  done
  echo "The $name (PID $pid) did not exit within 30 s after SIGTERM; sending SIGKILL." >&2
  kill -KILL "$pid" 2>/dev/null || true
}

stop_worker() {
  stop_pid_from_file "$WORKER_PID_FILE" "$TEMPORAL_APP_DIR/worker.py" "worker"
}

stop_server() {
  stop_pid_from_file "$SERVER_PID_FILE" "temporal server start-dev" "Temporal server"
}

# Usage: start_worker <log-file> [v1|v2|v3]
# Stops the worker recorded in the PID file first and waits for it to exit.
start_worker() {
  local log_file="$1"
  # v1, v2 or v3: which ConversationWorkflow code the worker runs.
  local code_version="${2:-v1}"
  stop_worker
  echo "Starting worker with ConversationWorkflow code $code_version, log: $log_file"
  DEMO_CONVERSATION_CODE="$code_version" DEMO_WORKER_PID_FILE="$WORKER_PID_FILE" \
    uv run --locked --quiet --project "$TEMPORAL_APP_DIR" python "$TEMPORAL_APP_DIR/worker.py" \
    >"$log_file" 2>&1 &
  for _ in $(seq 1 120); do
    if grep -q "worker polling" "$log_file" 2>/dev/null && [ -s "$WORKER_PID_FILE" ]; then
      echo "Worker $code_version is polling (PID $(cat "$WORKER_PID_FILE"))."
      return
    fi
    sleep 0.5
  done
  echo "Worker did not start; see $log_file" >&2
  exit 1
}

stop_all() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "KEEP=1: leaving the server (UI http://localhost:$TEMPORAL_UI_PORT) and the worker running."
    echo "Their PIDs are in $SERVER_PID_FILE and $WORKER_PID_FILE."
    return
  fi
  stop_worker
  if [ "$STARTED_SERVER" = "1" ]; then
    stop_server
  fi
}
