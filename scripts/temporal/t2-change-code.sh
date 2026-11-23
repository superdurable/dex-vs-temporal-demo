#!/usr/bin/env bash
# T2 part 1: deploy a plain code change while a conversation is open.
# v2 calls analyze_sentiment before generate_reply under the same workflow
# type. Conversation A started on v1 hits a non-determinism error on its next
# message; conversation B started on v2 works.
# Usage: scripts/temporal/t2-change-code.sh
#   RETRY_WATCH_SECONDS  how long to sample A's workflow task retries (default 180)
#   KEEP=1  leave the server and the v2 worker running, for the Web UI and for
#           scripts/temporal/t2-patch-fix.sh
# Raw output: $RUN_DIR/t2-change-code.log, history dumps $RUN_DIR/t2-*-history-*.json
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RETRY_WATCH_SECONDS="${RETRY_WATCH_SECONDS:-180}"
LOG_FILE="${LOG_FILE:-$RUN_DIR/t2-change-code.log}"
STATE_FILE="$RUN_DIR/t2-conversations.env"

trap stop_all EXIT
start_temporal_server "$RUN_DIR/t2-server.log"
start_worker "$RUN_DIR/t2-worker-v1.log" v1

stamp="$(date +%Y%m%d-%H%M%S)"
conversation_a="t2-a-$stamp"
conversation_b="t2-b-$stamp"
printf 'CONVERSATION_A=%s\nCONVERSATION_B=%s\n' "$conversation_a" "$conversation_b" >"$STATE_FILE"

{
  echo "=== v1 worker: conversation $conversation_a, 3 customer messages"
  drive start "$conversation_a" --first-message "Hi, my order 1234 has not arrived." >/dev/null
  drive wait-replies "$conversation_a" --count 1
  drive post "$conversation_a" "It was supposed to be delivered last week." >/dev/null
  drive wait-replies "$conversation_a" --count 2
  drive post "$conversation_a" "Can you check the tracking number?" >/dev/null
  drive wait-replies "$conversation_a" --count 3

  echo
  echo "=== deploy v2: stop the v1 worker, start a v2 worker (same workflow type, new code)"
  start_worker "$RUN_DIR/t2-worker-v2.log" v2
  echo "--- customer posts a 4th message to $conversation_a"
  drive post "$conversation_a" "Any news? I am getting worried." >/dev/null
  echo "signal accepted at $(date -u +%H:%M:%SZ)"

  echo
  echo "=== v2 worker: new conversation $conversation_b, 2 customer messages"
  drive start "$conversation_b" --first-message "Hello, I want to change my delivery address." >/dev/null
  drive wait-replies "$conversation_b" --count 1
  drive post "$conversation_b" "The new address is 5 Main Street." >/dev/null
  drive wait-replies "$conversation_b" --count 2

  echo
  echo "=== $conversation_a: does the 4th message get a reply?"
  drive wait-replies "$conversation_a" --count 4 --timeout 20
  echo "--- workflow task attempts of $conversation_a over ${RETRY_WATCH_SECONDS}s (a line per change)"
  drive watch-task-retries "$conversation_a" --seconds "$RETRY_WATCH_SECONDS" --interval 1

  echo
  echo "=== $conversation_a after the v2 deploy"
  drive inspect "$conversation_a" --dump "$RUN_DIR/t2-a-history-after-v2.json"
  echo
  echo "=== $conversation_b on v2"
  drive inspect "$conversation_b" --dump "$RUN_DIR/t2-b-history-after-v2.json"
  echo
  echo "--- v2 worker log: first non-determinism warning"
  grep -m1 -A3 -i "nondetermin" "$RUN_DIR/t2-worker-v2.log" | cut -c1-600 || true
  echo "($(grep -ci "nondetermin" "$RUN_DIR/t2-worker-v2.log" || true) matching lines in $RUN_DIR/t2-worker-v2.log)"
} 2>&1 | tee "$LOG_FILE"
