#!/usr/bin/env bash
# Deterministic checks that a customer message accepted before a conversation
# completes is answered and reopens it (timer and message in one workflow
# task, a message during the close summary, and an idle Continue-As-New
# suggestion). See temporal/check_close_race.py.
# Usage: scripts/temporal/check-close-race.sh [scenario ...]
# Raw output: $RUN_DIR/check-close-race.log; exits non-zero when a check fails.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

LOG_FILE="${LOG_FILE:-$RUN_DIR/check-close-race.log}"
trap stop_all EXIT
start_temporal_server "$RUN_DIR/check-close-race-server.log"
{
  print_versions
  uv run --locked --quiet --project "$TEMPORAL_APP_DIR" python "$TEMPORAL_APP_DIR/check_close_race.py" "$@"
} 2>&1 | tee "$LOG_FILE"
exit "${PIPESTATUS[0]}"
