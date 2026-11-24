# Helpers for the Dex agent-reply scripts. Source it; do not run it.
# Starts `dexcli dev` and Workers, and stops what this shell started on exit.
#
# Environment:
#   RUN_DIR                         logs, venv, server data (default /tmp/dex-video/two-step/dex-reply-run)
#   DEX_PORT                        Dex Server gRPC port (default 18911)
#   WEB_PORT                        Dex Web port (default 18912)
# dexcli dev also keeps its codec discovery file in its own state directory.

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${RUN_DIR:-/tmp/dex-video/two-step/dex-reply-run}"
DEX_PORT="${DEX_PORT:-18911}"
WEB_PORT="${WEB_PORT:-18912}"
export DEX_SERVER_ADDRESS="127.0.0.1:$DEX_PORT"
export AGENT_REPLY_RUN_DIR="$RUN_DIR"
export UV_PROJECT_ENVIRONMENT="$RUN_DIR/venv"
export UV_CACHE_DIR="$RUN_DIR/uv-cache"
export UV_PYTHON_INSTALL_DIR="$RUN_DIR/uv-python"
export PYTHONDONTWRITEBYTECODE=1
PYTHON="$RUN_DIR/venv/bin/python"
SERVER_PID=""
WORKER_PID=""

mkdir -p "$RUN_DIR"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# Usage: client v1|v2 <command> <flow-id> [args...]
client() {
  local code="$1"
  shift
  (cd "$APP_DIR" && AGENT_REPLY_CODE="$code" "$PYTHON" client.py "$@")
}

start_server() {
  if dexcli health -server "$DEX_SERVER_ADDRESS" >/dev/null 2>&1; then
    log "ERROR: a Dex Server already answers on $DEX_SERVER_ADDRESS; choose other ports"
    exit 1
  fi
  (cd "$APP_DIR" && uv sync --locked -q)
  rm -rf "$RUN_DIR/zd-data"
  mkdir -p "$RUN_DIR/zd-data" "$RUN_DIR/server-logs"
  log "starting dexcli dev on $DEX_SERVER_ADDRESS"
  dexcli dev -open=false -connectors-mcp=false \
    -dex-port "$DEX_PORT" -web-port "$WEB_PORT" \
    -zd-data-dir "$RUN_DIR/zd-data" -server-log-folder "$RUN_DIR/server-logs" \
    >"$RUN_DIR/dexcli-dev.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 60); do
    dexcli health -server "$DEX_SERVER_ADDRESS" >/dev/null 2>&1 && return
    sleep 1
  done
  log "ERROR: Dex Server did not become healthy; see $RUN_DIR/dexcli-dev.log"
  exit 1
}

# Usage: start_worker v1|v2
start_worker() {
  local code="$1" worker_log="$RUN_DIR/worker-$1.log"
  (cd "$APP_DIR" && exec env AGENT_REPLY_CODE="$code" "$PYTHON" worker.py) \
    >"$worker_log" 2>&1 &
  WORKER_PID=$!
  for _ in $(seq 1 60); do
    grep -q "worker ready" "$worker_log" 2>/dev/null && { log "$(cat "$worker_log")"; return; }
    sleep 0.5
  done
  log "ERROR: Worker $code did not start; see $worker_log"
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

# Usage: wait_until_waiting <flow-id>
# Returns once SendReplyStep registered its wait.
wait_until_waiting() {
  for _ in $(seq 1 40); do
    if show_history "$1" | grep -q "SendReplyStep.*waits for"; then
      log "$1: SendReplyStep is waiting"
      return
    fi
    sleep 0.5
  done
  log "ERROR: $1 did not reach the SendReplyStep wait"
  exit 1
}

# Usage: show_history <flow-id>; saves the history JSON lines and prints a summary.
show_history() {
  dexcli flow history "$1" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 60s >"$RUN_DIR/history-$1.jsonl"
  python3 "$APP_DIR/history_summary.py" "$RUN_DIR/history-$1.jsonl"
}

# --- Checks: each failed check is logged; finish_checks exits 1 if any failed.
FAILED_CHECKS=0

check_failed() {
  log "CHECK FAILED: $*"
  FAILED_CHECKS=$((FAILED_CHECKS + 1))
}

# Usage: expect_status <flow-id> COMPLETED|RUNNING
expect_status() {
  local status
  status="$(client v2 status "$1" | sed -E 's/^[^:]+: ([A-Z_]+)$/\1/')"
  if [[ "$status" == "$2" ]]; then
    log "check ok: $1 is $2"
  else
    check_failed "$1 is $status, expected $2"
  fi
}

# Usage: expect_steps <flow-id> <Step type>...
# Compares the completed Step executions, in order, with the expected list.
expect_steps() {
  local flow_id="$1" steps
  shift
  dexcli flow history "$flow_id" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 60s >"$RUN_DIR/history-$flow_id.jsonl"
  steps="$(python3 "$APP_DIR/history_summary.py" --steps "$RUN_DIR/history-$flow_id.jsonl")"
  if [[ "$steps" == "$*" ]]; then
    log "check ok: $flow_id completed $steps"
  else
    check_failed "$flow_id completed '$steps', expected '$*'"
  fi
}

# Usage: expect_output_contains <description> <text> <output>
expect_output_contains() {
  if grep -qF -- "$2" <<<"$3"; then
    log "check ok: $1"
  else
    check_failed "$1: output does not contain '$2': $3"
  fi
}

finish_checks() {
  if [[ "$FAILED_CHECKS" -gt 0 ]]; then
    log "$FAILED_CHECKS check(s) failed"
    exit 1
  fi
  log "all checks passed"
}
