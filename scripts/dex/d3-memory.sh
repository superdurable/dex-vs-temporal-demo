#!/usr/bin/env bash
# D3: Worker memory when many idle conversations come back at once.
# The Dex Server (dexcli dev) runs on the host. Only the Worker runs in a
# container with explicit --memory and --cpus limits. Same workload and
# harness as scripts/temporal/t3-memory.sh; see bench/README.md.
#
# Usage: scripts/dex/d3-memory.sh [run | cleanup <run ID>]
#   run       (default) one complete run: start a fresh dexcli dev, build the
#             Worker image, fill N conversations with a fill Worker, run the
#             rush against a clean, limited Worker container, summarize, stop
#             everything this run started, and delete the server data this
#             run created.
#   cleanup   remove the leftover containers of one earlier run (label dvt-mem-run=<run ID>)
#
# Parameters (environment):
#   DEX_DEMO_RUN_DIR   output directory (default /tmp/dex-video/dex-mem-run)
#   RUN_NAME           results go to DEX_DEMO_RUN_DIR/RUN_NAME (default run)
#   DEX_PORT WEB_PORT METRICS_PORT   (default 18841, 18842, 18843)
#   CONVERSATIONS=N EXCHANGES=M MESSAGE_BYTES=B FILL_CONCURRENCY FILL_TASKS   (fill)
#   MEM_LIMIT CPUS TASKS (Worker task slots, default 32) BLOB_CACHE_BYTES
#   RETURNING=K RATE=R FOLLOW_UPS=F THINK_SECONDS=T SEED
#   BASELINE_SECONDS COOLDOWN_SECONDS MAX_RUSH_SECONDS                        (rush)
#   MIN_FREE_GB        refuse to fill with less free disk, in 10^9 bytes (default 80)
#   KEEP_SERVER_DATA=1 keep the server data after the run
#   MAX_RUN_SECONDS    the whole run is stopped after this many seconds (default 3600)
export DEX_DEMO_RUN_DIR="${DEX_DEMO_RUN_DIR:-/tmp/dex-video/dex-mem-run}"
export DEX_PORT="${DEX_PORT:-18841}"
export WEB_PORT="${WEB_PORT:-18842}"
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

RUN_ID="$(date -u +%Y%m%dT%H%M%S)-$$"
source "$REPO_ROOT/bench/shared/memory_run.sh"

METRICS_PORT="${METRICS_PORT:-18843}"
IMAGE="dvt-dex-worker"
WORKER_CONTAINER="dvt-mem-$RUN_ID-dex-worker"
FILL_CONTAINER="dvt-mem-$RUN_ID-dex-fill-worker"

RUN_NAME="${RUN_NAME:-run}"
CONVERSATIONS="${CONVERSATIONS:-1200}"
EXCHANGES="${EXCHANGES:-40}"
MESSAGE_BYTES="${MESSAGE_BYTES:-8192}"
FILL_CONCURRENCY="${FILL_CONCURRENCY:-200}"
FILL_TASKS="${FILL_TASKS:-64}"
PREFIX="${PREFIX:-$RUN_NAME}"
MEM_LIMIT="${MEM_LIMIT:-512m}"
CPUS="${CPUS:-2}"
TASKS="${TASKS:-32}"
BLOB_CACHE_BYTES="${BLOB_CACHE_BYTES:-1073741824}"
RETURNING="${RETURNING:-1000}"
RATE="${RATE:-4}"
FOLLOW_UPS="${FOLLOW_UPS:-3}"
THINK_SECONDS="${THINK_SECONDS:-10}"
SEED="${SEED:-1}"
BASELINE_SECONDS="${BASELINE_SECONDS:-15}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-20}"
MAX_RUSH_SECONDS="${MAX_RUSH_SECONDS:-1500}"
MIN_FREE_GB="${MIN_FREE_GB:-80}"
MAX_RUN_SECONDS="${MAX_RUN_SECONDS:-3600}"

OUT_DIR="$RUN_DIR/$RUN_NAME"
MANIFEST="$OUT_DIR/manifest.json"
SERVER_DATA_DIR="$RUN_DIR/zd-data"
SERVER_LOG_DIR="$RUN_DIR/server-logs"
# The server's data and the load driver's BlobCache; both must not exist before a run.
RUN_DATA_DIRS=("$SERVER_DATA_DIR" "$RUN_DIR/blob-cache")
export PYTHONPATH="$REPO_ROOT/bench/shared:$REPO_ROOT/bench/dex:$DEX_APP_DIR"
# The idle window of the benchmark conversations: no nudge or close during a run.
export DEMO_IDLE_SECONDS=86400

# Background processes of this run; empty when none.
WATCHDOG_PID=""
STEP_PID=""
SAMPLER_PID=""
# 1 once this run creates the server data directories.
RUN_CREATED_DATA=0

driver() {
  "$PYTHON" "$REPO_ROOT/bench/shared/load_driver.py" \
    --adapter dex_adapter:DexConversationAdapter "$@"
}

require_free_disk() {
  local free_kib min_kib
  free_kib="$(df -Pk "$RUN_DIR" | awk 'NR == 2 { print $4 }')"
  min_kib=$((MIN_FREE_GB * 1000000000 / 1024))
  if (( free_kib < min_kib )); then
    log "ERROR: $((free_kib * 1024 / 1000000000)) GB free under $RUN_DIR; a fill needs more than $MIN_FREE_GB GB"
    exit 1
  fi
  log "free disk: $((free_kib * 1024 / 1000000000)) GB"
}

# start_worker_container <name> <memory> <cpus> <task slots> <metrics port or empty> <role>
start_worker_container() {
  local name="$1" memory="$2" cpus="$3" tasks="$4" metrics_port="$5" role="$6"
  local publish=() metrics_env=()
  if [[ -n "$metrics_port" ]]; then
    publish=(-p "127.0.0.1:$metrics_port:9464")
    metrics_env=(-e DEMO_METRICS_BIND=0.0.0.0:9464)
  fi
  # --memory-swap equal to --memory disables swap, so the limit is hard.
  start_run_container --name "$name" --hostname dex-worker \
    --memory "$memory" --memory-swap "$memory" --cpus "$cpus" \
    --restart on-failure \
    -e DEX_SERVER_ADDRESS="host.docker.internal:$DEX_PORT" \
    -e SUPPORT_FLOW_VERSION=v1 \
    -e DEMO_WORKER_ID="memory-bench-$role" -e DEMO_WORKER_ID_PER_START=1 \
    -e DEMO_REPLY_BYTES="$MESSAGE_BYTES" \
    -e DEMO_BLOB_CACHE_BYTES="$BLOB_CACHE_BYTES" \
    -e DEMO_MAX_CONCURRENT_TASKS="$tasks" \
    ${metrics_env[@]+"${metrics_env[@]}"} ${publish[@]+"${publish[@]}"} \
    "$IMAGE" >/dev/null
  for _ in $(seq 1 60); do
    if docker logs "$name" 2>&1 | grep -q "worker ready"; then
      log "Worker container $name is ready"
      return
    fi
    sleep 0.5
  done
  log "ERROR: Worker container $name did not start"
  docker logs "$name" >&2 || true
  exit 1
}

stop_sampler() {
  if [[ -n "$SAMPLER_PID" ]]; then
    stop_background_tree "$SAMPLER_PID" "sampler"
    SAMPLER_PID=""
  fi
}

# Succeeds when no process uses this run's server data and nothing listens on DEX_PORT.
server_fully_stopped() {
  ! owns_process dexcli-dev \
    && ! pgrep -f -- "$SERVER_DATA_DIR" >/dev/null 2>&1 \
    && ! lsof -nP -iTCP:"$DEX_PORT" -sTCP:LISTEN >/dev/null 2>&1
}

# Deletes the data only when this run created it and its server is fully stopped.
delete_run_data() {
  if [[ "$RUN_CREATED_DATA" != "1" ]]; then
    return 0
  fi
  if [[ "${KEEP_SERVER_DATA:-0}" == "1" ]]; then
    log "KEEP_SERVER_DATA=1: kept the server data in $SERVER_DATA_DIR"
    return 0
  fi
  if ! server_fully_stopped; then
    log "ERROR: a server process may still use $SERVER_DATA_DIR; kept the data. Delete it after the process is gone."
    return 1
  fi
  rm -rf "${RUN_DATA_DIRS[@]}"
  log "deleted the server data of $RUN_NAME"
}

# Moves this run's server logs next to its results.
keep_server_logs() {
  [[ "$RUN_CREATED_DATA" == "1" ]] || return 0
  cp "$RUN_DIR/dexcli-dev.log" "$OUT_DIR/dexcli-dev.log" 2>/dev/null
  if [[ -d "$SERVER_LOG_DIR" ]]; then
    mv "$SERVER_LOG_DIR" "$OUT_DIR/server-logs"
  fi
}

on_exit() {
  local status="$1"
  # A repeated stop signal does not interrupt the cleanup.
  trap '' INT TERM
  trap - EXIT
  # Cleanup continues past a failing command.
  set +e
  if [[ -n "$WATCHDOG_PID" ]]; then
    stop_background_tree "$WATCHDOG_PID" "watchdog"
  fi
  if [[ -n "$STEP_PID" ]]; then
    stop_background_tree "$STEP_PID" "running step"
  fi
  stop_sampler
  # SIGTERM lets the Worker drain its running tasks before it exits.
  remove_run_containers 40 || status=1
  if [[ "$RUN_CREATED_DATA" == "1" ]]; then
    stop_owned_processes || status=1
  fi
  keep_server_logs
  delete_run_data || status=1
  release_memory_run_lock
  if [[ "$status" -ne 0 ]]; then
    log "run $RUN_NAME failed with status $status; logs in $OUT_DIR (server: dexcli-dev.log, server-logs/)"
  fi
  exit "$status"
}

write_parameters() {
  local dexcli_version
  dexcli_version="$(dexcli version 2>/dev/null | head -1)"
  {
    echo "run=$RUN_NAME engine=dex memory=$MEM_LIMIT cpus=$CPUS"
    echo "worker_task_slots=$TASKS blob_cache_bytes=$BLOB_CACHE_BYTES"
    echo "returning=$RETURNING rate=$RATE follow_ups=$FOLLOW_UPS think_seconds=$THINK_SECONDS seed=$SEED"
    echo "conversations=$CONVERSATIONS exchanges=$EXCHANGES message_bytes=$MESSAGE_BYTES fill_concurrency=$FILL_CONCURRENCY"
    echo "baseline_seconds=$BASELINE_SECONDS cooldown_seconds=$COOLDOWN_SECONDS max_rush_seconds=$MAX_RUSH_SECONDS"
    echo "fill_worker: memory=4g cpus=4 task_slots=$FILL_TASKS"
    echo "dexcli: ${dexcli_version%% (*}"
    echo "dex-python-sdk: $(run_probe_container "$IMAGE" python -c 'import importlib.metadata as m; print(m.version("dex-python-sdk"))')"
    echo "worker_python: $(run_probe_container "$IMAGE" python -c 'import sys; print(sys.version.split()[0])')"
    echo "docker_server: $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
    echo "docker_backend: $(docker info --format '{{.OperatingSystem}}' 2>/dev/null)"
  } | tee "$OUT_DIR/parameters.txt"
}

run_all() {
  require_commands uv docker lsof dexcli
  if [[ -e "$OUT_DIR" ]]; then
    log "ERROR: $OUT_DIR exists; choose another RUN_NAME"
    exit 1
  fi
  acquire_memory_run_lock
  trap 'on_exit $?' EXIT
  trap 'exit 143' INT TERM
  local data_dir
  for data_dir in "${RUN_DATA_DIRS[@]}"; do
    if [[ -e "$data_dir" ]]; then
      log "ERROR: $data_dir exists; each run starts with an empty server. It was not created by this run and is kept."
      exit 1
    fi
  done
  mkdir -p "$OUT_DIR"
  start_watchdog "$MAX_RUN_SECONDS"
  require_free_port "$METRICS_PORT" METRICS_PORT
  require_free_disk
  log "run $RUN_NAME (run ID $RUN_ID), output $OUT_DIR"
  ensure_python_environment
  docker build -q -t "$IMAGE" -f "$REPO_ROOT/bench/dex/Dockerfile" "$DEX_APP_DIR" >/dev/null
  write_parameters
  # From here on the server data directories are this run's.
  RUN_CREATED_DATA=1
  start_dex_dev
  require_host_reachable_from_container "$IMAGE" "$DEX_PORT"

  start_worker_container "$FILL_CONTAINER" 4g 4 "$FILL_TASKS" "" fill-worker
  local fill_container_id
  fill_container_id="$(last_run_container_id)"
  log "fill: N=$CONVERSATIONS M=$EXCHANGES B=$MESSAGE_BYTES concurrency=$FILL_CONCURRENCY fill_tasks=$FILL_TASKS"
  run_step "$OUT_DIR/fill-driver.log" driver fill --manifest "$MANIFEST" \
    --latency-csv "$OUT_DIR/fill-latency.csv" --prefix "$PREFIX" \
    --conversations "$CONVERSATIONS" --exchanges "$EXCHANGES" \
    --message-bytes "$MESSAGE_BYTES" --concurrency "$FILL_CONCURRENCY"
  stop_run_container "$fill_container_id" 40
  du -sk "$SERVER_DATA_DIR" | awk '{ printf "server data after the fill (du -sk): %d KiB, %.1f GB (10^9 bytes)\n", $1, $1 * 1024 / 1e9 }' \
    | tee "$OUT_DIR/server-data-size.txt"

  start_worker_container "$WORKER_CONTAINER" "$MEM_LIMIT" "$CPUS" "$TASKS" "$METRICS_PORT" worker
  local worker_container_id
  worker_container_id="$(last_run_container_id)"
  "$PYTHON" "$REPO_ROOT/bench/shared/docker_sampler.py" --container "$WORKER_CONTAINER" \
    --out "$OUT_DIR/stats.csv" --events-out "$OUT_DIR/events.jsonl" \
    --metrics-url "http://127.0.0.1:$METRICS_PORT/metrics" \
    --metric dex_demo_blob_cache_hits --metric dex_demo_blob_cache_misses \
    --metric dex_demo_blob_cache_puts --metric dex_demo_blob_cache_puts_admitted \
    --metric dex_demo_blob_cache_directory_bytes \
    --metric dex_demo_process_rss_bytes --metric dex_demo_process_rss_peak_bytes \
    --metric dex_demo_cgroup_anon_bytes --metric dex_demo_cgroup_file_bytes \
    --metric dex_demo_cgroup_active_file_bytes --metric dex_demo_cgroup_inactive_file_bytes \
    </dev/null &
  SAMPLER_PID="$!"
  log "rush: K=$RETURNING R=$RATE/s F=$FOLLOW_UPS T=${THINK_SECONDS}s"
  run_step "$OUT_DIR/driver.log" driver rush --manifest "$MANIFEST" \
    --latency-csv "$OUT_DIR/rush-latency.csv" \
    --returning "$RETURNING" --rate "$RATE" --follow-ups "$FOLLOW_UPS" \
    --think-seconds "$THINK_SECONDS" --seed "$SEED" --start-delay "$BASELINE_SECONDS" \
    --max-seconds "$MAX_RUSH_SECONDS"
  sleep "$COOLDOWN_SECONDS"
  stop_sampler
  docker logs "$WORKER_CONTAINER" >"$OUT_DIR/worker.log" 2>&1 || true
  stop_run_container "$worker_container_id" 40
  "$PYTHON" "$REPO_ROOT/bench/shared/summarize.py" --name "$RUN_NAME" \
    --stats "$OUT_DIR/stats.csv" --events "$OUT_DIR/events.jsonl" \
    --latency "$OUT_DIR/rush-latency.csv" --json-out "$OUT_DIR/summary.json" \
    --planned-messages $((RETURNING * FOLLOW_UPS)) | tee "$OUT_DIR/summary.md"
  "$PYTHON" "$REPO_ROOT/bench/shared/fill_summary.py" \
    --fill-latency "$OUT_DIR/fill-latency.csv" --fill-log "$OUT_DIR/fill-driver.log" \
    --rush-log "$OUT_DIR/driver.log" --conversations "$CONVERSATIONS" \
    --exchanges "$EXCHANGES" | tee "$OUT_DIR/fill-summary.txt"
}

case "${1:-run}" in
  run) run_all ;;
  cleanup) cleanup_run_containers "${2:-}" ;;
  *) sed -n '2,25p' "$0"; exit 2 ;;
esac
