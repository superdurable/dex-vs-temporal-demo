#!/usr/bin/env bash
# T5: one long-lived AI conversation (correct variant, no Continue-As-New)
# takes customer messages, each answered by one generate_reply activity,
# until a default per-execution server limit stops it.
# Always starts a fresh `temporal server start-dev` on its own ports, so the
# limits are the defaults; it refuses to reuse a listening server.
# Usage: scripts/temporal/t5-flood.sh
#   COUNT  flood messages to send after the first message (default 60000)
#   RATE   flood messages per second (default 8; 0 = as fast as possible)
#   LOG_FILE  where to save the output; KEEP=1 leaves the server running
# Raw output: $RUN_DIR/t5-flood.log, flood counts $RUN_DIR/t5-flood-result.json,
# history summary of the terminated run $RUN_DIR/t5-history-summary.json
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COUNT="${COUNT:-60000}"
RATE="${RATE:-8}"
IDLE_SECONDS=86400
LOG_FILE="${LOG_FILE:-$RUN_DIR/t5-flood.log}"
RESULT_FILE="$RUN_DIR/t5-flood-result.json"
SUMMARY_FILE="$RUN_DIR/t5-history-summary.json"
SERVER_LOG="$RUN_DIR/t5-server.log"

trap stop_all EXIT
start_temporal_server "$SERVER_LOG" fresh
start_worker "$RUN_DIR/t5-worker.log"

conversation_id="t5-flood-$(date +%Y%m%d-%H%M%S)"
{
  print_versions
  echo "=== T5 flood: $conversation_id (correct variant, idle ${IDLE_SECONDS}s, target $COUNT flood messages, rate ${RATE}/s, 0 = unthrottled)"
  echo "server: fresh temporal server start-dev with no dynamic config overrides for per-execution limits"
  drive start "$conversation_id" --variant correct \
    --first-message "Hi, I need help with my order." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$IDLE_SECONDS"
  drive assign "$conversation_id" "Avery"
  drive flood "$conversation_id" --count "$COUNT" --rate "$RATE" --result-file "$RESULT_FILE"

  echo
  echo "--- one more message after the flood stopped (expected to fail)"
  if drive post "$conversation_id" "Are you still there?"; then
    echo "ASSERTION FAILED: the message was accepted after the flood stopped" >&2
    exit 2
  fi
  echo "--- an update (not a signal) after the flood stopped (expected to fail)"
  if drive assign "$conversation_id" "Blake"; then
    echo "ASSERTION FAILED: the update was accepted after the flood stopped" >&2
    exit 2
  fi

  echo
  echo "=== history of the terminated run (saved to $SUMMARY_FILE)"
  drive history-summary "$conversation_id" --output "$SUMMARY_FILE" >/dev/null
  python3 - "$RESULT_FILE" "$SUMMARY_FILE" <<'REPORT'
import json, sys
flood = json.load(open(sys.argv[1]))
summary = json.load(open(sys.argv[2]))
signals = summary["post_message_signals"]
replies = summary["completed_activities_by_type"].get("generate_reply", 0)
print(f"flood target:                          {flood['target_count']}")
print(f"signal RPC attempts:                   {flood['rpc_attempts']}")
print(f"acknowledged by the client:            {flood['acknowledged_count']}")
print(f"client-side timeouts (no ack):         {flood['client_timeout_count']}")
rejection = flood["rejection"]
print(f"rejected by the server:                {'flood message ' + str(rejection['index']) + ': ' + rejection['error'] if rejection else 'none'}")
print(f"post_message WorkflowExecutionSignaled events in history: {signals}")
print(f"customer messages in history:          {signals + 1} = 1 first message (with the start) + {signals} signals")
print(f"completed generate_reply activities:   {replies} (of {signals + 1} customer messages, first message included)")
print(f"history events:                        {summary['history_event_count']} ({summary['history_size_bytes']} bytes)")
print(f"first event:                           {summary['first_event']}")
print(f"last event:                            {summary['last_event']}")
print("events by type:")
for name, count in summary["events_by_type"].items():
    print(f"  {count:7d}  {name}")
window = summary.get("steady_window")
if window:
    print(f"steady middle half: {window['events_per_message']} events per customer message "
          f"({window['events']} events / {window['customer_messages']} messages):")
    for name, count in window["events_per_message_by_type"].items():
        print(f"  {count:7.3f}  {name}")
REPORT

  echo
  echo "--- server log lines about per-execution limits (first of each kind)"
  limit_lines="$(grep -E 'exceeds (warn|error) limit|exceeded workflow execution limit' "$SERVER_LOG" || true)"
  if [ -n "$limit_lines" ]; then
    echo "$limit_lines" | sed -E 's/ address=[^ ]+//; s/ wf-namespace-id=[^ ]+//' | awk '!seen[$3 $4 $5 $6]++'
    echo "($(echo "$limit_lines" | wc -l | tr -d ' ') matching lines in t5-server.log)"
  fi
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
