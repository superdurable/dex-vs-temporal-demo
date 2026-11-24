# Helpers for the Temporal agent-reply scripts. Source it; do not run it.
# Starts `temporal server start-dev` and workers, and stops what this shell
# started on exit.
#
# Environment:
#   RUN_DIR                         logs, venv, histories (default /tmp/dex-video/two-step/temporal-reply-run)
#   TEMPORAL_GRPC_PORT              server gRPC port (default 17233)
#   TEMPORAL_UI_PORT                server Web UI port (default 18233)

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${RUN_DIR:-/tmp/dex-video/two-step/temporal-reply-run}"
TEMPORAL_GRPC_PORT="${TEMPORAL_GRPC_PORT:-17233}"
TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-18233}"
export TEMPORAL_ADDRESS="localhost:$TEMPORAL_GRPC_PORT"
export UV_PROJECT_ENVIRONMENT="$RUN_DIR/venv"
export UV_CACHE_DIR="$RUN_DIR/uv-cache"
export UV_PYTHON_INSTALL_DIR="$RUN_DIR/uv-python"
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

# Usage: start_worker v1|v2|v2_no_patch
# Each worker is a new process, so it has no cached workflow state.
start_worker() {
  local code="$1" worker_log="$RUN_DIR/worker-$1.log"
  (cd "$APP_DIR" && exec env AGENT_REPLY_CODE="$code" "$PYTHON" worker.py) \
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

# Usage: wait_until_waiting <workflow-id>
# Returns once draft_reply completed and the next workflow task completed (v1 code).
wait_until_waiting() {
  local events
  for _ in $(seq 1 40); do
    events="$(temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$1" -o json)"
    if grep -q ACTIVITY_TASK_COMPLETED <<<"$events" \
      && [[ "$(grep -c WORKFLOW_TASK_COMPLETED <<<"$events")" -ge 2 ]]; then
      log "$1 is waiting for approve_reply"
      return
    fi
    sleep 0.5
  done
  log "ERROR: $1 did not reach the wait"
  exit 1
}

# Usage: show_history <workflow-id>; prints the events and saves the JSON.
show_history() {
  temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$1" -o json \
    >"$RUN_DIR/history-$1.json"
  python3 "$APP_DIR/history_summary.py" "$RUN_DIR/history-$1.json"
}

# --- Checks: each failed check is logged; finish_checks exits 1 if any failed.
FAILED_CHECKS=0

check_failed() {
  log "CHECK FAILED: $*"
  FAILED_CHECKS=$((FAILED_CHECKS + 1))
}

# Usage: expect_status <workflow-id> COMPLETED|RUNNING
expect_status() {
  local status
  status="$(client status "$1" | sed -E 's/^[^:]+: ([A-Z_]+);.*/\1/')"
  if [[ "$status" == "$2" ]]; then
    log "check ok: $1 is $2"
  else
    check_failed "$1 is $status, expected $2"
  fi
}

# Usage: expect_activities <workflow-id> <activity type>...
# Compares the scheduled activity types, in order, with the expected list.
expect_activities() {
  local workflow_id="$1" activities
  shift
  temporal workflow show --address "$TEMPORAL_ADDRESS" -w "$workflow_id" -o json \
    >"$RUN_DIR/history-$workflow_id.json"
  activities="$(python3 "$APP_DIR/history_summary.py" --activities "$RUN_DIR/history-$workflow_id.json")"
  if [[ "$activities" == "$*" ]]; then
    log "check ok: $workflow_id scheduled $activities"
  else
    check_failed "$workflow_id scheduled '$activities', expected '$*'"
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
