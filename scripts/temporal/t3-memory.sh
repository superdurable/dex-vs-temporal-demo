#!/usr/bin/env bash
# T3: worker memory when many idle conversations come back at once.
# The Temporal dev server runs on the host with default limits and in-memory
# storage. Only the worker runs in a container with explicit --memory and
# --cpus limits. See bench/README.md.
#
# Usage: scripts/temporal/t3-memory.sh [run | cleanup <run ID>]
#   run       (default) one complete run: start a fresh dev server, build the
#             worker image, fill N conversations with a fill worker, run the
#             rush against a clean, limited worker container, summarize, and
#             stop everything this run started. The server data is in memory
#             and goes away with the server.
#   cleanup   remove the leftover containers of one earlier run (label dvt-mem-run=<run ID>)
#
# Parameters (environment):
#   RUN_DIR            output directory (default /tmp/dex-video/temporal-mem-run)
#   RUN_NAME           results go to RUN_DIR/RUN_NAME (default run)
#   TEMPORAL_GRPC_PORT TEMPORAL_UI_PORT METRICS_PORT   (default 7253, 8253, 9473)
#   CONVERSATIONS=N EXCHANGES=M MESSAGE_BYTES=B FILL_CONCURRENCY            (fill)
#   CACHE_SIZE (empty = SDK default 1000) MEM_LIMIT CPUS WF_TASK_SLOTS ACTIVITY_SLOTS
#   RETURNING=K RATE=R FOLLOW_UPS=F THINK_SECONDS=T SEED
#   BASELINE_SECONDS COOLDOWN_SECONDS MAX_RUSH_SECONDS                      (rush)
#   MAX_RUN_SECONDS    the whole run is stopped after this many seconds (default 3600)
set -euo pipefail

export RUN_DIR="${RUN_DIR:-/tmp/dex-video/temporal-mem-run}"
export TEMPORAL_GRPC_PORT="${TEMPORAL_GRPC_PORT:-7253}"
export TEMPORAL_UI_PORT="${TEMPORAL_UI_PORT:-8253}"
# PID files of this shell only, so no other script adopts this run's server.
export PRIVATE_PIDS=1
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# This run's server is private: an inherited KEEP=1 does not keep it running.
KEEP=0

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

RUN_ID="$(date -u +%Y%m%dT%H%M%S)-$$"
source "$REPO_ROOT/bench/shared/memory_run.sh"

METRICS_PORT="${METRICS_PORT:-9473}"
IMAGE="dvt-temporal-worker"
WORKER_CONTAINER="dvt-mem-$RUN_ID-temporal-worker"
FILL_CONTAINER="dvt-mem-$RUN_ID-temporal-fill-worker"

RUN_NAME="${RUN_NAME:-run}"
CONVERSATIONS="${CONVERSATIONS:-1200}"
EXCHANGES="${EXCHANGES:-40}"
MESSAGE_BYTES="${MESSAGE_BYTES:-8192}"
FILL_CONCURRENCY="${FILL_CONCURRENCY:-200}"
PREFIX="${PREFIX:-$RUN_NAME}"
CACHE_SIZE="${CACHE_SIZE:-}"
MEM_LIMIT="${MEM_LIMIT:-512m}"
CPUS="${CPUS:-2}"
WF_TASK_SLOTS="${WF_TASK_SLOTS:-100}"
ACTIVITY_SLOTS="${ACTIVITY_SLOTS:-100}"
RETURNING="${RETURNING:-1000}"
RATE="${RATE:-4}"
FOLLOW_UPS="${FOLLOW_UPS:-3}"
THINK_SECONDS="${THINK_SECONDS:-10}"
SEED="${SEED:-1}"
BASELINE_SECONDS="${BASELINE_SECONDS:-15}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-20}"
MAX_RUSH_SECONDS="${MAX_RUSH_SECONDS:-1500}"
MAX_RUN_SECONDS="${MAX_RUN_SECONDS:-3600}"

OUT_DIR="$RUN_DIR/$RUN_NAME"
MANIFEST="$OUT_DIR/manifest.json"
export DEMO_CLIENT_IDENTITY="memory-bench-driver"
export PYTHONPATH="$REPO_ROOT/bench/shared:$REPO_ROOT/bench/temporal:$TEMPORAL_APP_DIR"
PYTHON="$UV_PROJECT_ENVIRONMENT/bin/python"

# Background processes of this run; empty when none.
WATCHDOG_PID=""
STEP_PID=""
SAMPLER_PID=""

driver() {
  "$PYTHON" "$REPO_ROOT/bench/shared/load_driver.py" \
    --adapter temporal_adapter:TemporalConversationAdapter "$@"
}

# start_worker_container <name> <memory> <cpus> <cache size or empty> <metrics port or empty>
start_worker_container() {
  local name="$1" memory="$2" cpus="$3" cache_size="$4" metrics_port="$5"
  local publish=() metrics_env=()
  if [ -n "$metrics_port" ]; then
    publish=(-p "127.0.0.1:$metrics_port:9464")
    metrics_env=(-e DEMO_METRICS_BIND=0.0.0.0:9464)
  fi
  # --memory-swap equal to --memory disables swap, so the limit is hard.
  start_run_container --name "$name" --hostname temporal-worker \
    --memory "$memory" --memory-swap "$memory" --cpus "$cpus" \
    --restart on-failure \
    -e TEMPORAL_ADDRESS="host.docker.internal:$TEMPORAL_GRPC_PORT" \
    -e DEMO_WORKER_IDENTITY=memory-bench-worker \
    -e DEMO_REPLY_BYTES="$MESSAGE_BYTES" \
    -e DEMO_MAX_CACHED_WORKFLOWS="$cache_size" \
    -e DEMO_MAX_CONCURRENT_WORKFLOW_TASKS="$WF_TASK_SLOTS" \
    -e DEMO_MAX_CONCURRENT_ACTIVITIES="$ACTIVITY_SLOTS" \
    ${metrics_env[@]+"${metrics_env[@]}"} ${publish[@]+"${publish[@]}"} \
    "$IMAGE" >/dev/null
  for _ in $(seq 1 60); do
    if docker logs "$name" 2>&1 | grep -q "worker polling"; then
      log "worker container $name is polling"
      return
    fi
    sleep 0.5
  done
  log "ERROR: worker container $name did not start"
  docker logs "$name" >&2 || true
  exit 1
}

stop_sampler() {
  if [ -n "$SAMPLER_PID" ]; then
    stop_process_tree "$SAMPLER_PID" "sampler"
    SAMPLER_PID=""
  fi
}

on_exit() {
  local status="$1"
  # A repeated stop signal does not interrupt the cleanup.
  trap '' INT TERM
  trap - EXIT
  # Cleanup continues past a failing command.
  set +e
  if [ -n "$WATCHDOG_PID" ]; then
    stop_process_tree "$WATCHDOG_PID" "watchdog" >/dev/null
  fi
  if [ -n "$STEP_PID" ]; then
    stop_process_tree "$STEP_PID" "running step"
  fi
  stop_sampler
  # A graceful stop (SIGINT, the image's stop signal) releases the worker's sticky queue.
  remove_run_containers 30 || status=1
  stop_all
  release_memory_run_lock
  if [ "$status" -ne 0 ]; then
    log "run $RUN_NAME failed with status $status; logs in $OUT_DIR"
  fi
  exit "$status"
}

write_parameters() {
  {
    echo "run=$RUN_NAME engine=temporal cache_size=${CACHE_SIZE:-sdk-default} memory=$MEM_LIMIT cpus=$CPUS"
    echo "wf_task_slots=$WF_TASK_SLOTS activity_slots=$ACTIVITY_SLOTS"
    echo "returning=$RETURNING rate=$RATE follow_ups=$FOLLOW_UPS think_seconds=$THINK_SECONDS seed=$SEED"
    echo "conversations=$CONVERSATIONS exchanges=$EXCHANGES message_bytes=$MESSAGE_BYTES fill_concurrency=$FILL_CONCURRENCY"
    echo "baseline_seconds=$BASELINE_SECONDS cooldown_seconds=$COOLDOWN_SECONDS max_rush_seconds=$MAX_RUSH_SECONDS"
    echo "fill_worker: memory=6g cpus=4 cache_size=$((FILL_CONCURRENCY + 100))"
    echo "temporal_cli: $(temporal --version 2>&1 | head -1)"
    echo "temporalio: $(run_probe_container "$IMAGE" python -c 'import importlib.metadata as m; print(m.version("temporalio"))')"
    echo "worker_python: $(run_probe_container "$IMAGE" python -c 'import sys; print(sys.version.split()[0])')"
    echo "docker_server: $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
    echo "docker_backend: $(docker info --format '{{.OperatingSystem}}' 2>/dev/null)"
  } | tee "$OUT_DIR/parameters.txt"
}

run_all() {
  require_commands uv docker lsof temporal
  if [ -e "$OUT_DIR" ]; then
    log "ERROR: $OUT_DIR exists; choose another RUN_NAME"
    exit 1
  fi
  acquire_memory_run_lock
  trap 'on_exit $?' EXIT
  trap 'exit 143' INT TERM
  mkdir -p "$OUT_DIR"
  start_watchdog "$MAX_RUN_SECONDS"
  require_free_port "$METRICS_PORT" METRICS_PORT
  log "run $RUN_NAME (run ID $RUN_ID), output $OUT_DIR"
  uv sync --locked --quiet --project "$TEMPORAL_APP_DIR"
  docker build -q -t "$IMAGE" -f "$REPO_ROOT/bench/temporal/Dockerfile" "$TEMPORAL_APP_DIR" >/dev/null
  write_parameters
  start_temporal_server "$OUT_DIR/server.log" fresh
  require_host_reachable_from_container "$IMAGE" "$TEMPORAL_GRPC_PORT"

  # The fill worker caches every active conversation, so no fill message makes it reload history.
  start_worker_container "$FILL_CONTAINER" 6g 4 $((FILL_CONCURRENCY + 100)) ""
  local fill_container_id
  fill_container_id="$(last_run_container_id)"
  log "fill: N=$CONVERSATIONS M=$EXCHANGES B=$MESSAGE_BYTES concurrency=$FILL_CONCURRENCY"
  run_step "$OUT_DIR/fill-driver.log" driver fill --manifest "$MANIFEST" \
    --latency-csv "$OUT_DIR/fill-latency.csv" --prefix "$PREFIX" \
    --conversations "$CONVERSATIONS" --exchanges "$EXCHANGES" \
    --message-bytes "$MESSAGE_BYTES" --concurrency "$FILL_CONCURRENCY"
  stop_run_container "$fill_container_id" 30

  start_worker_container "$WORKER_CONTAINER" "$MEM_LIMIT" "$CPUS" "$CACHE_SIZE" "$METRICS_PORT"
  local worker_container_id
  worker_container_id="$(last_run_container_id)"
  "$PYTHON" "$REPO_ROOT/bench/shared/docker_sampler.py" --container "$WORKER_CONTAINER" \
    --out "$OUT_DIR/stats.csv" --events-out "$OUT_DIR/events.jsonl" \
    --metrics-url "http://127.0.0.1:$METRICS_PORT/metrics" \
    --metric temporal_sticky_cache_size --metric temporal_sticky_cache_hit \
    --metric temporal_sticky_cache_miss --metric temporal_sticky_cache_total_forced_eviction \
    --metric temporal_workflow_task_replay_latency_count \
    --metric temporal_workflow_task_replay_latency_sum </dev/null &
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
  stop_run_container "$worker_container_id" 30
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
  *) sed -n '2,24p' "$0"; exit 2 ;;
esac
