#!/usr/bin/env bash
# T8: ConversationWithContextWorkflow, the correct workflow with one change:
# each reply activity receives the whole transcript so far, as a real model
# call needs it. Floods it until the server closes the workflow, its workflow
# task keeps failing, or replies stop, then reports where and why it stopped.
# Always starts a fresh `temporal server start-dev` on its own ports, so the
# limits are the defaults; it refuses to reuse a listening server.
# Usage: scripts/temporal/t8-context-in-activity.sh
#   COUNT          flood messages to send after the first message (default 12000)
#   RATE           flood messages per second (default 8)
#   MESSAGE_BYTES  size of each flood message (default 200)
#   STALL_SECONDS  stop when no activity completes for this long (default 60)
#   FLOOD_MAX_SECONDS  stop sending after this long (default 1500)
#   RUN_TAG        names every file of one run (default t8-context)
#   KEEP=1         leave the server running
# Refuses to run with DEMO_EXTERNAL_STORAGE_DIR set: External Storage would
# change what history records. Stops only the server and worker it started.
# Raw output: $RUN_DIR/$RUN_TAG.log, flood counts $RUN_DIR/$RUN_TAG-flood-result.json,
# final state $RUN_DIR/$RUN_TAG-report.json
set -euo pipefail
if [ -n "${DEMO_EXTERNAL_STORAGE_DIR:-}" ]; then
  echo "DEMO_EXTERNAL_STORAGE_DIR is set; T8 measures history without External Storage. Unset it and run again." >&2
  exit 1
fi
export PRIVATE_PIDS=1
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COUNT="${COUNT:-12000}"
RATE="${RATE:-8}"
MESSAGE_BYTES="${MESSAGE_BYTES:-200}"
STALL_SECONDS="${STALL_SECONDS:-60}"
FLOOD_MAX_SECONDS="${FLOOD_MAX_SECONDS:-1500}"
# Hard limits per phase, in seconds, so the exit trap always runs.
SHORT_PHASE_SECONDS=60
FLOOD_PHASE_SECONDS=$((FLOOD_MAX_SECONDS + 120))
REPORT_PHASE_SECONDS=300
IDLE_SECONDS=86400
RUN_TAG="${RUN_TAG:-t8-context}"
LOG_FILE="${LOG_FILE:-$RUN_DIR/$RUN_TAG.log}"
SERVER_LOG="$RUN_DIR/$RUN_TAG-server.log"
WORKER_LOG="$RUN_DIR/$RUN_TAG-worker.log"
RESULT_FILE="$RUN_DIR/$RUN_TAG-flood-result.json"
REPORT_FILE="$RUN_DIR/$RUN_TAG-report.json"

# Usage: run_with_deadline <seconds> <command...>
# Runs the command; stops it and returns 124 when it is still running after <seconds>.
run_with_deadline() {
  local seconds="$1"
  shift
  "$@" &
  local command_pid="$!"
  local deadline=$((SECONDS + seconds))
  while kill -0 "$command_pid" 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "DEADLINE: no result within ${seconds}s from: $*" >&2
      stop_process_tree "$command_pid" "timed-out command" >&2
      wait "$command_pid" 2>/dev/null || true
      return 124
    fi
    sleep 0.5
  done
  wait "$command_pid"
}

describe_workflow() {
  temporal workflow describe --address "$TEMPORAL_ADDRESS" -w "$1" -o json \
    | python3 -c '
import json, sys
info = json.load(sys.stdin)["workflowExecutionInfo"]
for key in ("type", "status", "startTime", "closeTime", "historyLength", "historySizeBytes"):
    value = info.get(key)
    if isinstance(value, dict):
        value = value.get("name")
    print(f"{key}: {value}")'
}

trap stop_all EXIT
start_temporal_server "$SERVER_LOG" fresh
start_worker "$WORKER_LOG"

conversation_id="t8-context-$(date +%Y%m%d-%H%M%S)"
{
  print_versions
  echo "=== T8 context in the activity input: $conversation_id"
  echo "    target $COUNT flood messages, rate ${RATE}/s, message $MESSAGE_BYTES bytes, stall watch ${STALL_SECONDS}s"
  echo "server: fresh temporal server start-dev with no dynamic config overrides"
  echo "external storage: off (DEMO_EXTERNAL_STORAGE_DIR is not set); payloads use the default JSON converter"
  run_with_deadline "$SHORT_PHASE_SECONDS" drive --command-timeout "$SHORT_PHASE_SECONDS" \
    start "$conversation_id" --variant context-in-activity \
    --first-message "Hi, I need help with my order." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$IDLE_SECONDS"
  run_with_deadline "$SHORT_PHASE_SECONDS" drive --command-timeout "$SHORT_PHASE_SECONDS" \
    assign "$conversation_id" "Avery"
  run_with_deadline "$FLOOD_PHASE_SECONDS" drive --command-timeout "$((FLOOD_MAX_SECONDS + 60))" \
    flood "$conversation_id" --count "$COUNT" --rate "$RATE" \
    --message-bytes "$MESSAGE_BYTES" --backlog-timeout 0 --progress-seconds 5 \
    --stop-when-stalled "$STALL_SECONDS" --max-seconds "$FLOOD_MAX_SECONDS" \
    --result-file "$RESULT_FILE"

  echo
  echo "=== temporal workflow describe"
  run_with_deadline "$SHORT_PHASE_SECONDS" describe_workflow "$conversation_id"

  echo
  echo "=== final state, read from history in small pages (saved to $REPORT_FILE)"
  run_with_deadline "$REPORT_PHASE_SECONDS" drive --command-timeout "$REPORT_PHASE_SECONDS" \
    context-report "$conversation_id" --output "$REPORT_FILE" >/dev/null
  python3 - "$RESULT_FILE" "$REPORT_FILE" <<'REPORT'
import json, sys
flood = json.load(open(sys.argv[1]))
report = json.load(open(sys.argv[2]))
rejection = flood["rejection"]
print(f"flood: {flood['acknowledged_count']} acknowledged, {flood['client_timeout_count']} client timeouts, "
      f"{flood['elapsed_seconds']} s, achieved {flood['achieved_rate_per_second']}/s")
print(f"send loop stopped by:             {flood['send_loop_stop']}")
print(f"stall watch stop reason:          {flood['stall_watch_stop_reason'] or '-'}")
print(f"rejected signal:                  {'flood message ' + str(rejection['index']) + ': ' + rejection['error'] if rejection else '-'}")
print(f"follow-up messages accepted:      {report['followup_messages_accepted']} (post_message signals in history)")
print(f"follow-up messages answered:      {report['followup_messages_answered']} (completed replies, first message excluded)")
print(f"workflow status:                  {report['workflow_status']}")
print(f"seconds from start to close:      {report['seconds_from_start_to_close']}")
print(f"history:                          {report['history_event_count']} events, {report['history_size_bytes']} bytes")
print(f"termination reason:               {report['termination_reason'] or '-'}")
print(f"workflow task failed events:      {report['workflow_task_failed_events']}")
for label in ("first_workflow_task_failure", "last_workflow_task_failure"):
    failure = report[label]
    if failure:
        print(f"  {label.replace('_', ' ')}: event {failure['event_id']} cause {failure['cause']}: {failure['message'][:400]}")
pending = report["pending_workflow_task"]
if pending:
    print(f"pending workflow task attempt:    {pending['attempt']}")
for label in ("last_reply_scheduled", "last_reply_completed"):
    request = report[label]
    if request:
        print(f"{label.replace('_', ' ')} input: {request['payloads_bytes']} bytes as Payloads "
              f"({request['payload_data_bytes']} data bytes), {request['context_messages']} context messages, "
              f"answers flood message {request['answers_flood_message']}")
print(f"largest reply input:              {report['largest_reply_input_payloads_bytes']} bytes")
print(f"last event:                       {report['last_event']}")
print("default thresholds, first crossed (counted from history):")
thresholds = report["thresholds_first_crossed"]
if not thresholds:
    print("  none")
for name, crossing in thresholds.items():
    print(f"  {name}: event {crossing['event_id']} after {crossing['followups_accepted']} follow-up messages accepted, "
          f"{crossing['replies_completed']} replies completed ({crossing['detail'] or 'event count'})")
REPORT

  echo
  echo "--- server log: limit lines (first of each kind)"
  limit_lines="$(grep -E 'exceeds (warn|error) limit|Blob data size|PayloadsTooLarge|exceeded workflow execution limit|Critical attempts' "$SERVER_LOG" || true)"
  if [ -n "$limit_lines" ]; then
    echo "$limit_lines" | sed -E 's/ address=[^ ]+//; s/ wf-namespace(-id)?=[^ ]+//; s/ shard-id=[^ ]+//' \
      | awk '!seen[$3 $4 $5 $6]++' | cut -c1-500
    echo "($(echo "$limit_lines" | wc -l | tr -d ' ') matching lines in $RUN_TAG-server.log)"
  else
    echo "none"
  fi
  echo "--- worker log: payload size and workflow task failure lines (first of each kind)"
  for pattern in 'TMPRL1103.*warning limit' 'TMPRL1103.*error limit' 'TMPRL1' 'RESOURCE_EXHAUSTED|message length too large|larger than max'; do
    first_line="$(sed 's/\x1b\[[0-9;]*m//g' "$WORKER_LOG" | grep -m1 -E "$pattern" | cut -c1-400 || true)"
    echo "  /$pattern/: $(grep -cE "$pattern" "$WORKER_LOG" || true) lines${first_line:+; first: $first_line}"
  done
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
