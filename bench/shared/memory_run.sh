# Process handling shared by scripts/temporal/t3-memory.sh and scripts/dex/d3-memory.sh.
# Source it after the engine helpers; do not run it. The caller sets RUN_ID and
# defines a log function. Only one memory run of either script runs at a time.

RUN_LABEL="dvt-mem-run"
# One lock for both scripts: two runs at once would compete for CPU and disk.
MEMORY_RUN_LOCK_DIR="${MEMORY_RUN_LOCK_DIR:-/tmp/dex-video/memory-bench.lock}"
OWNS_MEMORY_RUN_LOCK=0
# IDs of the containers this run started.
RUN_CONTAINER_IDS=()

memory_run_lock_owner_alive() {
  local owner="$MEMORY_RUN_LOCK_DIR/owner" pid start
  [[ -f "$owner" ]] || return 0
  pid="$(head -n 1 "$owner")"
  start="$(head -n 2 "$owner" | tail -n 1)"
  kill -0 "$pid" 2>/dev/null || return 1
  [[ "$(ps -o lstart= -p "$pid" 2>/dev/null | tr -s ' ')" == "$start" ]]
}

# Fails when another memory run holds the lock. A lock whose process is gone is removed.
acquire_memory_run_lock() {
  mkdir -p "$(dirname "$MEMORY_RUN_LOCK_DIR")"
  if ! mkdir "$MEMORY_RUN_LOCK_DIR" 2>/dev/null; then
    if memory_run_lock_owner_alive; then
      log "ERROR: another memory run is active: $(tail -n 1 "$MEMORY_RUN_LOCK_DIR/owner" 2>/dev/null || echo 'owner not recorded yet')"
      log "Lock: $MEMORY_RUN_LOCK_DIR. Wait for that run to finish."
      return 1
    fi
    log "removing the lock of a memory run that no longer runs: $(tail -n 1 "$MEMORY_RUN_LOCK_DIR/owner")"
    rm -rf "$MEMORY_RUN_LOCK_DIR"
    if ! mkdir "$MEMORY_RUN_LOCK_DIR" 2>/dev/null; then
      log "ERROR: another memory run took the lock $MEMORY_RUN_LOCK_DIR"
      return 1
    fi
  fi
  OWNS_MEMORY_RUN_LOCK=1
  printf '%s\n%s\n%s\n' "$$" "$(ps -o lstart= -p "$$" | tr -s ' ')" \
    "run ID $RUN_ID ($(basename "$0"), PID $$)" >"$MEMORY_RUN_LOCK_DIR/owner"
}

release_memory_run_lock() {
  if [[ "$OWNS_MEMORY_RUN_LOCK" == "1" ]]; then
    rm -rf "$MEMORY_RUN_LOCK_DIR"
    OWNS_MEMORY_RUN_LOCK=0
  fi
}

# Usage: start_run_container <docker run arguments...>
# Starts a detached container with this run's label and records its ID.
# Call it directly, not in $(...), so the ID is recorded in this shell.
start_run_container() {
  local container_id
  container_id="$(docker run -d --label "$RUN_LABEL=$RUN_ID" "$@")"
  RUN_CONTAINER_IDS+=("$container_id")
}

last_run_container_id() {
  echo "${RUN_CONTAINER_IDS[${#RUN_CONTAINER_IDS[@]}-1]}"
}

# Usage: run_probe_container <docker run arguments...>
# A short-lived container, removed on exit, with this run's label.
run_probe_container() {
  docker run --rm --label "$RUN_LABEL=$RUN_ID" "$@"
}

# Usage: stop_run_container <container ID> <stop timeout seconds>
stop_run_container() {
  docker stop -t "$2" "$1" >/dev/null 2>&1 || true
  docker rm -f "$1" >/dev/null 2>&1 || true
}

# Removes the recorded containers and any container with this run's exact label value.
remove_run_containers() {
  local stop_seconds="$1" container_id
  for container_id in ${RUN_CONTAINER_IDS[@]+"${RUN_CONTAINER_IDS[@]}"} \
    $(docker ps -aq --no-trunc --filter "label=$RUN_LABEL=$RUN_ID" 2>/dev/null); do
    stop_run_container "$container_id" "$stop_seconds"
  done
  if [[ -n "$(docker ps -aq --filter "label=$RUN_LABEL=$RUN_ID" 2>/dev/null)" ]]; then
    log "ERROR: containers with label $RUN_LABEL=$RUN_ID remain; remove them with: $0 cleanup $RUN_ID"
    return 1
  fi
}

# Usage: cleanup_run_containers <run ID>
# Removes the containers of one earlier run; refuses while that run is active.
cleanup_run_containers() {
  local target_run_id="${1:-}" container_id
  if [[ -z "$target_run_id" ]]; then
    log "Usage: $0 cleanup <run ID>; runs with leftover containers:"
    docker ps -a --filter "label=$RUN_LABEL" --format "{{.Label \"$RUN_LABEL\"}} {{.Names}}"
    return 2
  fi
  if [[ -d "$MEMORY_RUN_LOCK_DIR" ]] && memory_run_lock_owner_alive \
    && grep -qF "run ID $target_run_id " "$MEMORY_RUN_LOCK_DIR/owner" 2>/dev/null; then
    log "ERROR: run $target_run_id is active; it removes its containers when it exits"
    return 1
  fi
  for container_id in $(docker ps -aq --no-trunc --filter "label=$RUN_LABEL=$target_run_id"); do
    stop_run_container "$container_id" 30
  done
  log "removed the containers of run $target_run_id"
}

# Usage: require_commands <command...>
require_commands() {
  local command_name
  for command_name in "$@"; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
      log "ERROR: $command_name is not installed; see bench/README.md, Requirements"
      exit 1
    fi
  done
}

require_free_port() {
  if lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; then
    log "ERROR: port $1 is in use; choose another with $2"
    exit 1
  fi
}

# Usage: require_host_reachable_from_container <image> <port>
# The Worker containers reach the server on the host loopback through host.docker.internal.
require_host_reachable_from_container() {
  local image="$1" port="$2"
  if ! run_probe_container "$image" python -c \
    'import socket, sys; socket.create_connection(("host.docker.internal", int(sys.argv[1])), 5).close()' \
    "$port" >/dev/null 2>&1; then
    log "ERROR: a container cannot reach the server at host.docker.internal:$port."
    log "These scripts need a Docker backend that forwards host.docker.internal to the host"
    log "loopback, such as OrbStack or Docker Desktop on macOS. Native Linux Docker is untested."
    exit 1
  fi
  log "a container reaches the server at host.docker.internal:$port"
}

# Usage: run_step <log-file> <command...>
# Runs the command in the background and waits, so a stop signal is handled at once.
run_step() {
  local step_log="$1" status=0
  shift
  "$@" >"$step_log" 2>&1 </dev/null &
  STEP_PID="$!"
  wait "$STEP_PID" || status=$?
  STEP_PID=""
  tail -n 3 "$step_log"
  return "$status"
}

# Usage: stop_background_tree <pid> <name>
# SIGTERM to the process and its descendants, up to 30 s, then SIGKILL.
stop_background_tree() {
  local pid="$1" name="$2" pids alive="" p
  kill -0 "$pid" 2>/dev/null || return 0
  pids="$pid $(descendant_pids "$pid" | tr '\n' ' ')"
  kill -TERM $pids 2>/dev/null || true
  for _ in $(seq 1 30); do
    alive=""
    for p in $pids; do kill -0 "$p" 2>/dev/null && alive="$alive $p"; done
    [[ -z "$alive" ]] && return 0
    sleep 1
  done
  log "$name did not stop within 30s; sending SIGKILL to$alive"
  kill -KILL $alive 2>/dev/null || true
}

# Usage: start_watchdog <seconds>
# Sends SIGTERM to this script after the time limit.
start_watchdog() {
  ( sleep "$1"; echo "run exceeded MAX_RUN_SECONDS=$1; stopping" >&2; kill -TERM $$ ) &
  WATCHDOG_PID="$!"
  # No job notice when cleanup stops the watchdog.
  disown "$WATCHDOG_PID"
}
