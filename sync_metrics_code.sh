#!/bin/bash
#
# Clone or update the rads-derived-metrics-code repo into ./rads_metrics_code/
#
# This gives the local editor (and the LLM agent) full read access to the
# production pipeline notebooks that generate the ADS derived metrics tables
# available via snapshot_functions.ads.
#
# The folder is gitignored — it is a read-only mirror of an external repo.
# Re-run this script whenever you suspect the pipeline has changed and the
# table reference docs in .github/agents/ads-derived/ may be out of date.
#
# Usage:
#   ./sync_metrics_code.sh
#
# Requirements:
#   - git
#   - SSH key or HTTPS credentials for github.com/elsevier-research
#
set -euo pipefail

REPO_URL="https://github.com/elsevier-research/rads-derived-metrics-code.git"
LOCAL_PATH="rads_metrics_code"

if ! command -v git >/dev/null 2>&1; then
    echo "Error: git not found." >&2
    exit 1
fi

if [[ -d "$LOCAL_PATH/.git" ]]; then
    echo "Updating $LOCAL_PATH ..."
    git -C "$LOCAL_PATH" fetch --quiet origin
    git -C "$LOCAL_PATH" reset --quiet --hard origin/main
    echo "Done: $LOCAL_PATH is up to date with origin/main"
else
    echo "Cloning $REPO_URL → $LOCAL_PATH ..."
    rm -rf "$LOCAL_PATH"
    git clone --depth 1 "$REPO_URL" "$LOCAL_PATH"
    echo "Done: $LOCAL_PATH"
fi

echo ""
echo "Commit: $(git -C "$LOCAL_PATH" log -1 --format='%h %ai %s')"
