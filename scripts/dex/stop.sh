#!/usr/bin/env bash
# Stop the Worker and the dexcli dev server that the Dex demo scripts of this
# DEX_DEMO_RUN_DIR started. Fails when one of them does not stop in time.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
stop_owned_processes
