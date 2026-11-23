#!/usr/bin/env bash
# D5-fair: one long AI support conversation fed at a fixed rate. Post RATE
# customer messages per second (default 8) for DURATION_SECONDS (default 1800),
# each answered by its own reply, then drain for up to DRAIN_SECONDS.
# The first message is answered before the load starts and is reported apart.
# Snapshots are taken when the acknowledged count reaches each SNAPSHOT_AT
# value. Every 30 s a progress line records the counts and the host load
# (uptime). At the end the driver checks message IDs against replies one to
# one. Backlog, Flow failure, an unreached snapshot, or a timeout fail the
# script. Producer policy: see LoadRun in dex/drive.py. Runs Flow code v1.
# KEEP=1 leaves Dex running after a successful run.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export SUPPORT_FLOW_VERSION=v1

RATE="${RATE:-8}"
DURATION_SECONDS="${DURATION_SECONDS:-1800}"
SNAPSHOT_AT="${SNAPSHOT_AT:-4266,7300,14000}"
DRAIN_SECONDS="${DRAIN_SECONDS:-120}"
IDLE_SECONDS="${IDLE_SECONDS:-86400}"
CLOSE_AFTER_SECONDS="${CLOSE_AFTER_SECONDS:-3600}"
CONVERSATION_ID="${CONVERSATION_ID:-d5-fair-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d5-flood-fair.log"
HISTORY_FILE="$RUN_DIR/d5-flood-fair-history.jsonl"

main() {
  ensure_python_environment
  start_dex_dev
  start_worker

  log "host load before the run: $(uptime)"
  log "start $CONVERSATION_ID with idle ${IDLE_SECONDS}s, close after ${CLOSE_AFTER_SECONDS}s"
  drive start "$CONVERSATION_ID" --first-message "Hi, I have many questions about my orders." \
    --idle-seconds "$IDLE_SECONDS" --close-after-seconds "$CLOSE_AFTER_SECONDS"

  log "post ${RATE} messages/s for ${DURATION_SECONDS}s (snapshots at acknowledged=$SNAPSHOT_AT)"
  local paced_status=0
  drive paced "$CONVERSATION_ID" --rate "$RATE" --duration-seconds "$DURATION_SECONDS" \
    --snapshot-at "$SNAPSHOT_AT" --drain-seconds "$DRAIN_SECONDS" || paced_status=$?
  log "paced exit status $paced_status"
  log "host load after the run: $(uptime)"

  drive pages-summary "$CONVERSATION_ID"

  log "history Dex keeps for this run (retained window, not a running total)"
  dexcli flow history "$CONVERSATION_ID" -all -no-hydrate -page-size 1000 \
    -server "$DEX_SERVER_ADDRESS" -timeout 120s >"$HISTORY_FILE"
  python3 "$SCRIPTS_DIR/history_steps.py" count <"$HISTORY_FILE"
  python3 "$SCRIPTS_DIR/history_steps.py" timing --last 600 <"$HISTORY_FILE"
  log "Step timing over the whole retained window (older events are not in history)"
  python3 "$SCRIPTS_DIR/history_steps.py" timing --last 1000000 <"$HISTORY_FILE"

  echo "Dex Web: http://127.0.0.1:$WEB_PORT  (Flow ID $CONVERSATION_ID)"
  return "$paced_status"
}

run_logged "$LOG_FILE" main
