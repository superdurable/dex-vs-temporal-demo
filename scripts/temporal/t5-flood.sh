#!/usr/bin/env bash
# T5: one long-lived conversation (correct variant, no Continue-As-New) takes
# messages until a default per-execution server limit stops it.
# Usage: scripts/temporal/t5-flood.sh   (COUNT, RATE, KEEP=1 are optional)
# Raw output: /tmp/dex-video/temporal-run/t5-flood.log
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COUNT="${COUNT:-60000}"
RATE="${RATE:-0}"
IDLE_SECONDS=86400
LOG_FILE="${LOG_FILE:-$RUN_DIR/t5-flood.log}"

trap stop_all EXIT
start_temporal_server "$RUN_DIR/t5-server.log"
start_worker "$RUN_DIR/t5-worker.log"

conversation_id="t5-flood-$(date +%Y%m%d-%H%M%S)"
{
  echo "=== T5 flood: $conversation_id (correct variant, idle ${IDLE_SECONDS}s, count $COUNT, rate ${RATE} msg/s, 0 = unthrottled)"
  echo "server: temporal server start-dev with no dynamic config overrides for per-execution limits"
  drive start "$conversation_id" --variant correct --idle-seconds "$IDLE_SECONDS"
  drive assign "$conversation_id" "Avery"
  wall_start="$(epoch_now)"
  drive flood "$conversation_id" --count "$COUNT" --rate "$RATE"
  wall_end="$(epoch_now)"
  echo "wall clock for flood: $(python3 -c "print(f'{$wall_end - $wall_start:.1f}')") s"
  echo "--- one more message after the flood stopped"
  drive post "$conversation_id" "Are you still there?" || true
  echo "--- an update (not a signal) after the flood stopped"
  drive assign "$conversation_id" "Blake" || true
  echo "--- newest transcript page, page tokens only"
  drive page "$conversation_id" current | grep -E '"(page_token|next_page_token)"' || true
  echo "--- server log lines about per-execution limits"
  limit_lines="$(grep -E 'exceeds (warn|error) limit|exceeded workflow execution limit' "$RUN_DIR/t5-server.log" || true)"
  if [ -n "$limit_lines" ]; then
    echo "$limit_lines" | sed -E 's/ address=[^ ]+//; s/ wf-namespace-id=[^ ]+//' | awk '!seen[$3 $4 $5 $6]++'
    echo "($(echo "$limit_lines" | wc -l | tr -d ' ') matching lines in $RUN_DIR/t5-server.log)"
  fi
} 2>&1 | tee "$LOG_FILE"
