#!/usr/bin/env bash
# Design: render SupportConversationFlow with dexcli visualize for the Flow code
# version the Worker runs (SUPPORT_FLOW_VERSION, default v1), check it is valid
# and equal to dex/flow-definitions/<version>/, start Dex and that Worker (it
# uploads the same directory at start), confirm the upload, start one
# conversation, and print the Dex Web URLs. Fails on an invalid or mismatched
# definition or an upload the server did not store. Dex keeps running after a
# successful run (KEEP=0 stops it); stop it with scripts/dex/stop.sh.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
KEEP="${KEEP:-1}"

export SUPPORT_FLOW_VERSION="${SUPPORT_FLOW_VERSION:-v1}"
VISUALIZE_DIR="$RUN_DIR/visualize/$SUPPORT_FLOW_VERSION"
CONVERSATION_ID="${CONVERSATION_ID:-design-demo-$(date -u +%Y%m%d-%H%M%S)}"
LOG_FILE="$RUN_DIR/d-design.log"

flow_source_for_version() {
  case "$1" in
    v1) echo support_conversation/flow.py ;;
    v2) echo support_conversation/flow_v2.py ;;
    v2-short-idle) echo support_conversation/flow_v2_short_idle.py ;;
    *) echo "unknown Flow code version $1" >&2; return 1 ;;
  esac
}

main() {
  local source committed rendered
  source="$(flow_source_for_version "$SUPPORT_FLOW_VERSION")"
  committed="$DEX_APP_DIR/flow-definitions/$SUPPORT_FLOW_VERSION/SupportConversationFlow.json"
  rendered="$VISUALIZE_DIR/SupportConversationFlow.json"
  mkdir -p "$VISUALIZE_DIR"
  log "dexcli visualize $source (FDG 2.0) -> $rendered"
  (cd "$DEX_APP_DIR" && dexcli visualize "$source" \
    --schema-version 2.0 --json --open=false \
    --out "$VISUALIZE_DIR/SupportConversationFlow")
  python3 - "$rendered" "$committed" <<'PY'
import json, sys
rendered, committed = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:3])
print("valid:", rendered["valid"], "diagnostics:", rendered["diagnostics"])
if not rendered["valid"] or rendered["diagnostics"]:
    sys.exit("the rendered Flow definition is invalid or has diagnostics")
for document in (rendered, committed):
    document.pop("generatedAt", None)
if rendered != committed:
    sys.exit(f"{sys.argv[2]} differs from the rendered source; regenerate it with dexcli visualize")
print("the committed definition matches the rendered source")
PY

  ensure_python_environment
  start_dex_dev
  start_worker
  local upload_lines
  upload_lines="$(grep "_flow_definition_upload" "$(worker_log_path)" || true)"
  echo "$upload_lines"
  if grep -q "WARNING" <<<"$upload_lines" ||
    ! grep -q -E "Flow definition (created|replaced|unchanged): flow_type=SupportConversationFlow" <<<"$upload_lines"; then
    log "ERROR: the server did not store this Worker's Flow definition"
    return 1
  fi

  drive start "$CONVERSATION_ID" --first-message "Hi, my order has not arrived yet." \
    --idle-seconds 30 --close-after-seconds 3600

  local web="http://127.0.0.1:$WEB_PORT"
  echo
  echo "Open in Dex Web (Flow code $SUPPORT_FLOW_VERSION):"
  echo "  Designs page:        $web/v1/rendering"
  echo "  Flows of this type:  $web/v1/flows?q=$(python3 -c 'import urllib.parse; print(urllib.parse.quote("FlowType = \"SupportConversationFlow\""))')"
  echo "  Run page:            $web/v1/flows/$CONVERSATION_ID"
}

run_logged "$LOG_FILE" main
