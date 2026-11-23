#!/usr/bin/env bash
# Design: render SupportConversationFlow with dexcli visualize, start Dex and the
# Worker (the Worker uploads dex/flow-definitions/ at start), start one
# conversation, and print the Dex Web URLs for the Designs page and the run's
# Design view. Dex keeps running (KEEP=0 stops it at the end); stop it with
# scripts/dex/stop.sh.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
KEEP="${KEEP:-1}"

VISUALIZE_DIR="$RUN_DIR/visualize"
CONVERSATION_ID="${CONVERSATION_ID:-design-demo-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d-design.log"

trap stop_everything_unless_kept EXIT

{
  mkdir -p "$VISUALIZE_DIR"
  log "dexcli visualize (FDG 2.0) -> $VISUALIZE_DIR/SupportConversationFlow.json"
  (cd "$DEX_APP_DIR" && dexcli visualize support_conversation/flow.py \
    --schema-version 2.0 --json --open=false \
    --out "$VISUALIZE_DIR/SupportConversationFlow")
  python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print("valid:", d["valid"], "diagnostics:", d["diagnostics"])' \
    "$VISUALIZE_DIR/SupportConversationFlow.json"
  if ! cmp -s <(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); d.pop("generatedAt"); print(json.dumps(d, sort_keys=True))' "$VISUALIZE_DIR/SupportConversationFlow.json") \
             <(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); d.pop("generatedAt"); print(json.dumps(d, sort_keys=True))' "$DEX_APP_DIR/flow-definitions/SupportConversationFlow.json"); then
    log "WARNING: dex/flow-definitions/SupportConversationFlow.json is stale; regenerate it:"
    log "  cd dex && dexcli visualize support_conversation/flow.py --schema-version 2.0 --json --open=false --out flow-definitions/SupportConversationFlow"
  fi

  start_dex_dev
  start_worker
  grep -i "flow definition" "$RUN_DIR/worker.log" || true

  drive start "$CONVERSATION_ID" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds 30 --close-after-seconds 3600

  web="http://127.0.0.1:$WEB_PORT"
  echo
  echo "Open in Dex Web:"
  echo "  Designs page:        $web/v1/rendering"
  echo "  Flows of this type:  $web/v1/flows?q=$(python3 -c 'import urllib.parse; print(urllib.parse.quote("FlowType = \"SupportConversationFlow\""))')"
  echo "  Run page:            $web/v1/flows/$CONVERSATION_ID"
} 2>&1 | tee "$LOG_FILE"
