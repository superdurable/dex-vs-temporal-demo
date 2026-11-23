#!/usr/bin/env bash
# Stop the Worker and the dexcli dev server that the Dex demo scripts started.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
KEEP=0 stop_everything_unless_kept
