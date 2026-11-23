#!/usr/bin/env bash
# T6: flood ConversationWorkflowWithContinueAsNew, which carries the whole
# transcript to the next run each time Temporal suggests Continue-As-New.
# Reports how many runs it chained, events per run, workflow task failures,
# and whether every flooded message got exactly one reply.
# Usage: scripts/temporal/t6-continue-as-new.sh
#   COUNT            customer messages to send (default 10000)
#   RATE             customer messages per second (default 20)
#   MESSAGE_BYTES    size of each flood message (default 200)
#   BACKLOG_TIMEOUT  seconds to wait for replies after the flood (default 120)
#   IDLE_SECONDS     idle window and close window (default 86400); a short one
#                    lets the conversation complete after the flood
#   DEMO_EXTERNAL_STORAGE_DIR  if set, worker and driver store payloads of
#                    256 KiB or more in this directory (Temporal External Storage)
#   LOG_FILE  where to save the output; KEEP=1 leaves the server running
# Raw output: $RUN_DIR/t6-continue-as-new.log
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COUNT="${COUNT:-10000}"
RATE="${RATE:-20}"
MESSAGE_BYTES="${MESSAGE_BYTES:-200}"
BACKLOG_TIMEOUT="${BACKLOG_TIMEOUT:-120}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
LOG_FILE="${LOG_FILE:-$RUN_DIR/t6-continue-as-new.log}"
SERVER_LOG="$RUN_DIR/t6-server.log"

trap stop_all EXIT
start_temporal_server "$SERVER_LOG"
start_worker "$RUN_DIR/t6-worker.log" v1

conversation_id="t6-can-$(date +%Y%m%d-%H%M%S)"
{
  echo "=== T6 Continue-As-New flood: $conversation_id (count $COUNT, rate $RATE msg/s, message $MESSAGE_BYTES bytes)"
  echo "external storage: ${DEMO_EXTERNAL_STORAGE_DIR:-off}"
  drive start "$conversation_id" --variant continue-as-new \
    --first-message "Hi, I need help with my order." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$IDLE_SECONDS"
  drive assign "$conversation_id" "Avery"
  drive flood "$conversation_id" --count "$COUNT" --rate "$RATE" \
    --message-bytes "$MESSAGE_BYTES" --backlog-timeout "$BACKLOG_TIMEOUT"

  echo
  echo "=== runs of $conversation_id (events, history bytes, workflow task failures per run)"
  drive runs "$conversation_id"
  echo
  echo "=== latest run"
  drive inspect "$conversation_id" --dump "$RUN_DIR/t6-latest-run-history.json"
  echo
  echo "=== transcript check: every flood message exactly once, one reply each"
  drive verify "$conversation_id" --flood-count "$COUNT" || true
  echo
  echo "--- server log lines about size and count limits"
  limit_lines="$(grep -E 'exceeds (warn|error) limit|exceeds limit|too large' "$SERVER_LOG" || true)"
  if [ -n "$limit_lines" ]; then
    echo "$limit_lines" | sed -E 's/ address=[^ ]+//; s/ wf-namespace-id=[^ ]+//' | awk '!seen[$3 $4 $5 $6]++' | cut -c1-600
    echo "($(echo "$limit_lines" | wc -l | tr -d ' ') matching lines in $SERVER_LOG)"
  fi
  if [ "${WAIT_FOR_CLOSE:-0}" = "1" ]; then
    echo
    echo "--- waiting for the conversation to close (idle ${IDLE_SECONDS}s, then close ${IDLE_SECONDS}s)"
    for _ in $(seq 1 $((IDLE_SECONDS * 2 / 5 + 24))); do
      if ! drive inspect "$conversation_id" | grep -q '"workflow_status": "RUNNING"'; then
        break
      fi
      sleep 5
    done
    drive inspect "$conversation_id" | grep -E '"(workflow_status|run_id|history_event_count)"'
  fi
  if [ -n "${DEMO_EXTERNAL_STORAGE_DIR:-}" ]; then
    echo
    echo "--- objects left in the external store"
    echo "files: $(find "$DEMO_EXTERNAL_STORAGE_DIR" -type f | wc -l | tr -d ' ') bytes: $(du -sk "$DEMO_EXTERNAL_STORAGE_DIR" | cut -f1) KiB"
  fi
} 2>&1 | tee "$LOG_FILE"
