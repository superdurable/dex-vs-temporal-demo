# Shared helpers for the Dex demo scripts. Source it; do not run it.
# Everything the scripts and their processes write stays under RUN_DIR.
# Exception: dexcli dev also keeps its codec discovery file for the port in
# its own default state directory; dexcli has no flag to move it.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEX_APP_DIR="$REPO_ROOT/dex"
SCRIPTS_DIR="$REPO_ROOT/scripts/dex"
RUN_DIR="${DEX_DEMO_RUN_DIR:-/tmp/dex-video/dex-run}"
DEX_PORT="${DEX_PORT:-18801}"
WEB_PORT="${WEB_PORT:-18802}"

case "$RUN_DIR" in
  /*) ;;
  *) echo "DEX_DEMO_RUN_DIR must be an absolute path" >&2; exit 2 ;;
esac

OUTBOX="$RUN_DIR/outbox-dex.jsonl"
PROCESS_DIR="$RUN_DIR/processes"
PYTHON="$RUN_DIR/venv/bin/python"

export DEX_SERVER_ADDRESS="127.0.0.1:$DEX_PORT"
export DEX_DEMO_RUN_DIR="$RUN_DIR"
export DEMO_OUTBOX_PATH="$OUTBOX"
# Keep the Python environment, uv's cache and downloads, and temp files in RUN_DIR.
export UV_PROJECT_ENVIRONMENT="$RUN_DIR/venv"
export UV_CACHE_DIR="$RUN_DIR/uv-cache"
export UV_PYTHON_INSTALL_DIR="$RUN_DIR/uv-python"
export PYTHONDONTWRITEBYTECODE=1
export TMPDIR="$RUN_DIR/tmp"

mkdir -p "$RUN_DIR" "$PROCESS_DIR" "$TMPDIR"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

dex_server_healthy() { dexcli health -server "$DEX_SERVER_ADDRESS" >/dev/null 2>&1; }

ensure_python_environment() {
  (cd "$DEX_APP_DIR" && uv sync --locked -q)
}

drive() { (cd "$DEX_APP_DIR" && "$PYTHON" drive.py "$@"); }

checks() { python3 "$SCRIPTS_DIR/demo_checks.py" "$@"; }

# --- Process ownership -------------------------------------------------------
# Each process this run starts has a record: PID, start time, role, Flow code.
# A process is reused or signaled only when its PID and start time match.

process_start_time() { ps -o lstart= -p "$1" 2>/dev/null | tr -s ' ' || true; }

process_record() { echo "$PROCESS_DIR/$1.record"; }

write_process_record() {
  local role="$1" pid="$2" version="$3"
  printf '%s\n%s\n%s\n%s\n' "$pid" "$(process_start_time "$pid")" "$role" "$version" \
    >"$(process_record "$role")"
}

record_field() { sed -n "${2}p" "$(process_record "$1")"; }

# Succeeds when the record's process still runs with the recorded start time.
owns_process() {
  local role="$1" record pid
  record="$(process_record "$role")"
  [[ -f "$record" ]] || return 1
  pid="$(record_field "$role" 1)"
  kill -0 "$pid" 2>/dev/null || return 1
  [[ "$(process_start_time "$pid")" == "$(record_field "$role" 2)" ]]
}

descendant_pids() {
  local child
  for child in $(pgrep -P "$1" 2>/dev/null || true); do
    echo "$child"
    descendant_pids "$child"
  done
}

# Stops the role's process and its descendants. Fails and keeps the record
# when they do not exit within 30 seconds.
stop_owned_process() {
  local role="$1" record pid pids alive
  record="$(process_record "$role")"
  [[ -f "$record" ]] || return 0
  if ! owns_process "$role"; then
    log "$role: the recorded process is gone or is another process; not signaling it"
    rm -f "$record"
    return 0
  fi
  pid="$(record_field "$role" 1)"
  pids="$pid $(descendant_pids "$pid" | tr '\n' ' ')"
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 30); do
    alive=""
    for p in $pids; do kill -0 "$p" 2>/dev/null && alive="$alive $p"; done
    [[ -z "$alive" ]] && break
    sleep 1
  done
  if [[ -n "$alive" ]]; then
    log "ERROR: $role did not stop within 30s; still running:$alive (record kept: $record)"
    return 1
  fi
  rm -f "$record"
  log "stopped $role"
}

start_dex_dev() {
  if owns_process dexcli-dev && dex_server_healthy; then
    log "Dex Server of this run already running on $DEX_SERVER_ADDRESS"
    return
  fi
  if dex_server_healthy; then
    log "ERROR: $DEX_SERVER_ADDRESS answers, but this run did not start that server"
    return 1
  fi
  mkdir -p "$RUN_DIR/zd-data" "$RUN_DIR/server-logs"
  log "starting dexcli dev (gRPC $DEX_PORT, Dex Web http://127.0.0.1:$WEB_PORT)"
  nohup dexcli dev -open=false -connectors-mcp=false \
    -dex-port "$DEX_PORT" -web-port "$WEB_PORT" \
    -zd-data-dir "$RUN_DIR/zd-data" \
    -server-log-folder "$RUN_DIR/server-logs" \
    >"$RUN_DIR/dexcli-dev.log" 2>&1 </dev/null &
  write_process_record dexcli-dev $! -
  for _ in $(seq 1 60); do
    dex_server_healthy && { log "Dex Server is healthy"; return; }
    sleep 1
  done
  log "ERROR: Dex Server did not become healthy; see $RUN_DIR/dexcli-dev.log"
  return 1
}

# SUPPORT_FLOW_VERSION (v1, v2, v2-short-idle) selects the Flow code of the
# Worker and the driver; each version writes its own Worker log.
worker_log_path() { echo "$RUN_DIR/worker-${SUPPORT_FLOW_VERSION:-v1}.log"; }

start_worker() {
  local version="${SUPPORT_FLOW_VERSION:-v1}" worker_log pid
  if owns_process worker; then
    if [[ "$(record_field worker 4)" == "$version" ]]; then
      log "Worker with Flow code $version already running (pid $(record_field worker 1))"
      return
    fi
    log "ERROR: a Worker with Flow code $(record_field worker 4) runs; stop it first"
    return 1
  fi
  worker_log="$(worker_log_path)"
  log "starting the Worker demo-worker-$version with Flow code $version"
  # exec: the recorded PID is the Python process itself.
  (cd "$DEX_APP_DIR" && exec env SUPPORT_FLOW_VERSION="$version" \
    DEMO_WORKER_ID="demo-worker-$version" "$PYTHON" worker.py) \
    >"$worker_log" 2>&1 </dev/null &
  pid=$!
  write_process_record worker "$pid" "$version"
  for _ in $(seq 1 60); do
    grep -q "worker ready" "$worker_log" 2>/dev/null && { log "Worker is ready"; return; }
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  log "ERROR: Worker did not become ready; see $worker_log"
  return 1
}

stop_worker() { stop_owned_process worker; }

stop_owned_processes() {
  local status=0
  stop_owned_process worker || status=1
  stop_owned_process dexcli-dev || status=1
  return $status
}

# EXIT trap: keep the processes only when KEEP=1 and the script succeeded.
finish_run() {
  local status="$1"
  if [[ "$status" -eq 0 && "${KEEP:-0}" == "1" ]]; then
    log "KEEP=1: Dex Server and Worker keep running. Dex Web: http://127.0.0.1:$WEB_PORT"
    log "stop later with: $REPO_ROOT/scripts/dex/stop.sh (same DEX_DEMO_RUN_DIR)"
  else
    stop_owned_processes || status=1
  fi
  log "exit status $status"
  exit "$status"
}

# Usage at the end of each script: run_logged "$LOG_FILE" main
run_logged() {
  local log_file="$1"
  shift
  trap 'finish_run $?' EXIT
  "$@" 2>&1 | tee "$log_file"
}
