#!/usr/bin/env bash
# T2 part 1: deploy a plain code change while a conversation is open.
# v2 calls analyze_sentiment before generate_reply under the same workflow
# type, without a patch. This is the extra incident case: the v1-to-v3 path
# is conversation A (started on v1) and C (started on v3, part 2).
#   A, started on v1: its next message hits a non-determinism error on v2.
#   B, started on v2: works on v2 (and breaks on v3 in part 2).
# Usage: scripts/temporal/t2-change-code.sh
#   RETRY_WATCH_SECONDS  how long to sample A's workflow task retries (default 60)
#   KEEP=1  leave the server and the v2 worker running, for the Web UI and for
#           scripts/temporal/t2-patch-fix.sh
# Raw output: $RUN_DIR/t2-change-code.log, history dumps $RUN_DIR/t2-*-history-*.json
# Exits non-zero when an expected reply or the expected non-determinism error is missing.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RETRY_WATCH_SECONDS="${RETRY_WATCH_SECONDS:-60}"
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
  print_versions
  echo "=== A ($conversation_a), started on v1: 3 customer messages on the v1 worker"
  drive start "$conversation_a" --first-message "Hi, my order 1234 has not arrived." >/dev/null
  drive wait-replies "$conversation_a" --count 1
  drive post "$conversation_a" "It was supposed to be delivered last week." >/dev/null
  drive wait-replies "$conversation_a" --count 2
  drive post "$conversation_a" "Can you check the tracking number?" >/dev/null
  drive wait-replies "$conversation_a" --count 3

  echo
  echo "=== deploy v2 (unguarded change): stop the v1 worker, start a v2 worker (same workflow type, new code)"
  start_worker "$RUN_DIR/t2-worker-v2.log" v2
  echo "--- A: customer posts a 4th message"
  drive post "$conversation_a" "Any news? I am getting worried." >/dev/null
  echo "signal accepted at $(date -u +%H:%M:%SZ)"

  echo
  echo "=== B ($conversation_b), started on the unguarded v2 (extra incident case): 2 customer messages"
  drive start "$conversation_b" --first-message "Hello, I want to change my delivery address." >/dev/null
  drive wait-replies "$conversation_b" --count 1
  drive post "$conversation_b" "The new address is 5 Main Street." >/dev/null
  drive wait-replies "$conversation_b" --count 2

  echo
  echo "=== A on v2: expected to stall with a non-determinism error (no 4th reply)"
  drive wait-replies "$conversation_a" --count 4 --timeout 20 \
    --expect stall --failure-cause NON_DETERMINISTIC_ERROR
  echo "--- A: sampled workflow task attempts over ${RETRY_WATCH_SECONDS}s (a line per change; a trend, not a complete count)"
  drive watch-task-retries "$conversation_a" --seconds "$RETRY_WATCH_SECONDS" --interval 1

  echo
  echo "=== A after the v2 deploy"
  drive inspect "$conversation_a" --dump "$RUN_DIR/t2-a-history-after-v2.json"
  echo
  echo "=== B on v2"
  drive inspect "$conversation_b" --dump "$RUN_DIR/t2-b-history-after-v2.json"
  echo
  echo "--- v2 worker log: first non-determinism line"
  nondeterminism_lines="$(grep -ci "nondetermin" "$RUN_DIR/t2-worker-v2.log" || true)"
  if [ "$nondeterminism_lines" -eq 0 ]; then
    echo "ASSERTION FAILED: no non-determinism line in the v2 worker log" >&2
    exit 2
  fi
  sed 's/\x1b\[[0-9;]*m//g' "$RUN_DIR/t2-worker-v2.log" | grep -m1 -i "nondetermin" | cut -c1-600
  echo "($nondeterminism_lines matching lines in t2-worker-v2.log)"
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
