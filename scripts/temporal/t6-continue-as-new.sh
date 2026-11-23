#!/usr/bin/env bash
# T6: flood ConversationWorkflowContinueAsNewInlineTranscript, which carries
# its whole transcript inline to the next run each time Temporal suggests
# Continue-As-New. Reports the run chain, every accepted message and reply
# across all runs, and, when the carried input grows past the payload limit,
# the evidence of the stall from this same run.
# Usage: scripts/temporal/t6-continue-as-new.sh
#   COUNT            flood messages to send after the first message (default 10000)
#   RATE             flood messages per second (default 20)
#   MESSAGE_BYTES    size of each flood message (default 200)
#   BACKLOG_TIMEOUT  seconds to wait for replies after the flood (default 120)
#   IDLE_SECONDS     idle window and close window (default 86400)
#   EXPECT           stall (default) or all-answered
#   EXTERNAL_STORAGE=1  worker and driver store payloads of 256 KiB or more
#                    in a new empty directory of this run (Temporal External Storage)
#   RUN_TAG          names every file of one run (default t6-inline or t6-external-storage)
#   KEEP=1           leave the server running
# Raw output: $RUN_DIR/$RUN_TAG.log; exits non-zero when the outcome is not EXPECT.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COUNT="${COUNT:-10000}"
RATE="${RATE:-20}"
MESSAGE_BYTES="${MESSAGE_BYTES:-200}"
BACKLOG_TIMEOUT="${BACKLOG_TIMEOUT:-120}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
EXPECT="${EXPECT:-stall}"
if [ "${EXTERNAL_STORAGE:-0}" = "1" ]; then
  RUN_TAG="${RUN_TAG:-t6-external-storage}"
  export DEMO_EXTERNAL_STORAGE_DIR="$RUN_DIR/$RUN_TAG-store"
  if [ -e "$DEMO_EXTERNAL_STORAGE_DIR" ]; then
    echo "$DEMO_EXTERNAL_STORAGE_DIR exists; use a new RUN_TAG so the object count belongs to this run." >&2
    exit 1
  fi
  mkdir -p "$DEMO_EXTERNAL_STORAGE_DIR"
else
  RUN_TAG="${RUN_TAG:-t6-inline}"
fi
LOG_FILE="${LOG_FILE:-$RUN_DIR/$RUN_TAG.log}"
SERVER_LOG="$RUN_DIR/$RUN_TAG-server.log"
WORKER_LOG="$RUN_DIR/$RUN_TAG-worker.log"
RESULT_FILE="$RUN_DIR/$RUN_TAG-flood-result.json"

trap stop_all EXIT
start_temporal_server "$SERVER_LOG"
start_worker "$WORKER_LOG" v1

conversation_id="t6-$(date +%Y%m%d-%H%M%S)"
{
  print_versions
  echo "=== T6 Continue-As-New with the transcript carried inline: $conversation_id"
  echo "    target $COUNT flood messages, rate $RATE/s, message $MESSAGE_BYTES bytes, expect $EXPECT"
  echo "    external storage: $([ -n "${DEMO_EXTERNAL_STORAGE_DIR:-}" ] && echo "on, in an empty directory of this run" || echo off)"
  drive start "$conversation_id" --variant continue-as-new-inline-transcript \
    --first-message "Hi, I need help with my order." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$IDLE_SECONDS"
  drive assign "$conversation_id" "Avery"
  drive flood "$conversation_id" --count "$COUNT" --rate "$RATE" \
    --message-bytes "$MESSAGE_BYTES" --backlog-timeout "$BACKLOG_TIMEOUT" --result-file "$RESULT_FILE"

  echo
  echo "=== runs of $conversation_id, linked by run IDs (events, history bytes, workflow task failures per run)"
  drive runs "$conversation_id"
  echo
  echo "=== latest run"
  drive inspect "$conversation_id" --dump "$RUN_DIR/$RUN_TAG-latest-run-history.json"
  echo
  echo "=== every accepted message and reply across all runs, from history (completed generate_reply activities)"
  drive verify-history "$conversation_id" --flood-result "$RESULT_FILE" --expect "$EXPECT"
  echo
  echo "=== the same, as written to the transcript (read by query)"
  if [ "$EXPECT" = "stall" ]; then
    drive verify "$conversation_id" --flood-result "$RESULT_FILE" --allow-query-failure
  else
    drive verify "$conversation_id" --flood-result "$RESULT_FILE"
  fi

  echo
  echo "--- worker log: payload size lines of this run (TMPRL1103), first of each limit, with counts"
  for level in warning error; do
    sed 's/\x1b\[[0-9;]*m//g' "$WORKER_LOG" | grep -m1 -E "TMPRL1103.*$level limit" | cut -c1-400 || true
    echo "  ($(grep -cE "TMPRL1103.*$level limit" "$WORKER_LOG" || true) lines at the $level limit)"
  done
  echo "  largest payload the worker tried to send: $(sed 's/\x1b\[[0-9;]*m//g' "$WORKER_LOG" | grep -oE 'TMPRL1103.*error limit\. size=[0-9]+' | grep -oE '[0-9]+$' | sort -n | tail -1 || true) bytes"
  for pattern in TMPRL1101 TMPRL1104; do
    echo "  ($(grep -cE "$pattern" "$WORKER_LOG" || true) lines match $pattern)"
  done
  echo "--- server log: limit and workflow task failure lines (first of each kind)"
  limit_lines="$(grep -E 'Blob data size|exceeds (warn|error) limit|PayloadsTooLarge|Critical attempts|workflow task failed' "$SERVER_LOG" || true)"
  if [ -n "$limit_lines" ]; then
    echo "$limit_lines" | sed -E 's/ address=[^ ]+//; s/ wf-namespace(-id)?=[^ ]+//; s/ shard-id=[^ ]+//; s/ attempt=[0-9]+//' \
      | awk '!seen[$3 $4 $5 $6]++' | cut -c1-500
    echo "($(echo "$limit_lines" | wc -l | tr -d ' ') matching lines in $RUN_TAG-server.log)"
  fi
  if [ -n "${DEMO_EXTERNAL_STORAGE_DIR:-}" ]; then
    echo
    echo "--- objects this run wrote to the external store (Temporal never deletes them)"
    find "$DEMO_EXTERNAL_STORAGE_DIR" -type f -name '[0-9a-f]*' | python3 -c '
import os, sys
paths = [line.strip() for line in sys.stdin if len(os.path.basename(line.strip())) == 64]
print(f"objects: {len(paths)} bytes: {sum(os.path.getsize(p) for p in paths)}")'
  fi
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
