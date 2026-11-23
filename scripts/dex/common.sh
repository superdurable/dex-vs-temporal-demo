# Shared helpers for the Dex demo scripts. Source it; do not run it.
# Everything the scripts write at run time stays under RUN_DIR.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEX_APP_DIR="$REPO_ROOT/dex"
RUN_DIR="${DEX_DEMO_RUN_DIR:-/tmp/dex-video/dex-run}"
DEX_PORT="${DEX_PORT:-18801}"
WEB_PORT="${WEB_PORT:-18802}"
OUTBOX="${DEMO_OUTBOX_PATH:-/tmp/dex-vs-temporal-demo/outbox/dex.jsonl}"

export DEX_SERVER_ADDRESS="127.0.0.1:$DEX_PORT"
export DEX_DEMO_RUN_DIR="$RUN_DIR"
export DEMO_OUTBOX_PATH="$OUTBOX"
# Keep the Python environment out of the repository.
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$RUN_DIR/venv}"

DEX_DEV_PID_FILE="$RUN_DIR/dexcli-dev.pid"
WORKER_PID_FILE="$RUN_DIR/worker.pid"

mkdir -p "$RUN_DIR" "$(dirname "$OUTBOX")"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

is_pid_alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1")" 2>/dev/null; }

dex_server_healthy() { dexcli health -server "$DEX_SERVER_ADDRESS" >/dev/null 2>&1; }

start_dex_dev() {
  if dex_server_healthy; then
    log "Dex Server already running on $DEX_SERVER_ADDRESS"
    return
  fi
  mkdir -p "$RUN_DIR/zd-data" "$RUN_DIR/server-logs"
  log "starting dexcli dev (gRPC $DEX_PORT, Dex Web http://127.0.0.1:$WEB_PORT)"
  nohup dexcli dev -open=false -connectors-mcp=false \
    -dex-port "$DEX_PORT" -web-port "$WEB_PORT" \
    -zd-data-dir "$RUN_DIR/zd-data" \
    -server-log-folder "$RUN_DIR/server-logs" \
    >"$RUN_DIR/dexcli-dev.log" 2>&1 </dev/null &
  echo $! >"$DEX_DEV_PID_FILE"
  for _ in $(seq 1 60); do
    dex_server_healthy && { log "Dex Server is healthy"; return; }
    sleep 1
  done
  log "Dex Server did not become healthy; see $RUN_DIR/dexcli-dev.log"
  exit 1
}

start_worker() {
  if is_pid_alive "$WORKER_PID_FILE"; then
    log "Worker already running (pid $(cat "$WORKER_PID_FILE"))"
    return
  fi
  (cd "$DEX_APP_DIR" && uv sync -q)
  log "starting the Worker"
  # Redirect the whole subshell so it never holds the caller's stdout pipe.
  (cd "$DEX_APP_DIR" && exec uv run python worker.py) >"$RUN_DIR/worker.log" 2>&1 </dev/null &
  echo $! >"$WORKER_PID_FILE"
  for _ in $(seq 1 60); do
    grep -q "worker ready" "$RUN_DIR/worker.log" 2>/dev/null && { log "Worker is ready"; return; }
    sleep 1
  done
  log "Worker did not become ready; see $RUN_DIR/worker.log"
  exit 1
}

stop_pid_file() {
  local pid_file="$1" name="$2"
  if is_pid_alive "$pid_file"; then
    local pid
    pid="$(cat "$pid_file")"
    # uv run spawns python as a child; stop the whole group of children too.
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    log "stopped $name"
  fi
  rm -f "$pid_file"
}

stop_everything_unless_kept() {
  if [[ "${KEEP:-0}" == "1" ]]; then
    log "KEEP=1: Dex Server and Worker keep running. Dex Web: http://127.0.0.1:$WEB_PORT"
    log "stop later with: KEEP=0 $REPO_ROOT/scripts/dex/stop.sh"
    return
  fi
  stop_pid_file "$WORKER_PID_FILE" "Worker"
  stop_pid_file "$DEX_DEV_PID_FILE" "dexcli dev"
}

drive() { (cd "$DEX_APP_DIR" && uv run python drive.py "$@"); }
