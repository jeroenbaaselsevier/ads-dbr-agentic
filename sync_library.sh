#!/bin/bash
#
# Pull /Workspace/rads/library from Databricks into ./rads_library/
#
# This gives you local type information and function signatures so your editor
# (and the LLM) can reason about the available utilities.
#
# Usage:
#   ./sync_library.sh
#   DATABRICKS_PROFILE=myprofile ./sync_library.sh
#
# Databricks CLI required: v0.282.0+
# Install: https://github.com/databricks/cli/releases
#
set -euo pipefail

REMOTE_PATH="/Workspace/rads/library"
LOCAL_PATH="rads_library"

if ! command -v databricks >/dev/null 2>&1; then
    echo "Error: databricks CLI not found."
    exit 1
fi

PROFILE_ARGS=()
if [[ -n "${DATABRICKS_PROFILE:-}" ]]; then
    PROFILE_ARGS=(--profile "${DATABRICKS_PROFILE}")
fi

echo "Syncing $REMOTE_PATH → $LOCAL_PATH ..."
rm -rf "$LOCAL_PATH"
databricks workspace export-dir "${PROFILE_ARGS[@]}" "$REMOTE_PATH" "$LOCAL_PATH"
echo "Done: $LOCAL_PATH"
