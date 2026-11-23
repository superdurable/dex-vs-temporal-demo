#!/usr/bin/env bash
# Transcript budget regression on a live Dex Server (Flow code v1):
#  0. Offline: dex/transcript_budget_regression.py with the SDK codec.
#  1. SMALL_COUNT short ASCII messages fill many small archived chunks.
#  2. LARGE_COUNT messages of 8,192 non-BMP characters (12 JSON bytes each)
#     follow. Two small chunks free far less than one large exchange, so
#     appends need trimming over several Step executions.
# After each phase the driver checks the retained transcript: serialized
# bytes equal the bookkeeping and stay within 32 MiB, sequences are
# contiguous, each message has one reply, and message IDs are unique.
# Any failed check fails the script.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export SUPPORT_FLOW_VERSION=v1

RATE="${RATE:-8}"
SMALL_COUNT="${SMALL_COUNT:-500}"
LARGE_COUNT="${LARGE_COUNT:-480}"
CONVERSATION_ID="${CONVERSATION_ID:-d7-small-large-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d7-small-then-large.log"

main() {
  ensure_python_environment
  log "offline regression with the SDK codec"
  (cd "$DEX_APP_DIR" && "$PYTHON" transcript_budget_regression.py)

  start_dex_dev
  start_worker
  drive start "$CONVERSATION_ID" --first-message "Hi, I have many questions about my orders." \
    --idle-seconds 86400 --close-after-seconds 3600

  log "phase 1: $SMALL_COUNT short ASCII messages"
  drive paced "$CONVERSATION_ID" --rate "$RATE" \
    --duration-seconds "$(python3 -c "print($SMALL_COUNT / $RATE)")" \
    --message-characters 30 --message-id-prefix small- --drain-seconds 120

  log "phase 2: $LARGE_COUNT messages of 8,192 non-BMP characters"
  drive paced "$CONVERSATION_ID" --rate "$RATE" \
    --duration-seconds "$(python3 -c "print($LARGE_COUNT / $RATE)")" \
    --message-characters 8192 --message-kind non-bmp --message-id-prefix large- \
    --drain-seconds 300

  drive pages-summary "$CONVERSATION_ID"
  drive status "$CONVERSATION_ID" | python3 -c '
import json, sys
status = json.load(sys.stdin)
print(json.dumps({key: status[key] for key in (
    "flowStatus", "customerMessageCount", "assistantReplyCount", "transcriptBytes",
    "transcriptBudgetBytes", "retainedArchivedChunkCount", "summary")}, indent=2))'
}

run_logged "$LOG_FILE" main
