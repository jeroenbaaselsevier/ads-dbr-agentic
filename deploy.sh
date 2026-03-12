#!/bin/bash
#
# Deploy a local .py notebook to Databricks and optionally run it.
#
# Usage:
#   ./deploy.sh <local_file.py> [remote_workspace_path] [--run] [--cluster-id ID]
#
# Examples:
#   ./deploy.sh notebooks/retraction_citation_cascade.py
#   ./deploy.sh notebooks/retraction_citation_cascade.py /Workspace/Users/J.Baas@elsevier.com/retraction_citation_cascade
#   ./deploy.sh notebooks/retraction_citation_cascade.py --run
#   ./deploy.sh notebooks/retraction_citation_cascade.py --run --cluster-id 0303-153342-zrgyfy1c
#
# Databricks CLI required: v0.282.0
#
set -euo pipefail

LOCAL_FILE=""
REMOTE_PATH=""
DO_RUN=false
CLUSTER_ID="${DATABRICKS_CLUSTER_ID:-0107-154653-j5wd510m}"  # default: rads-private-unity
WORKSPACE_ROOT="${DEPLOY_WORKSPACE_ROOT:-/Workspace/Users/J.Baas@elsevier.com}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run)        DO_RUN=true; shift ;;
        --cluster-id) CLUSTER_ID="$2"; shift 2 ;;
        --*)          echo "Unknown option: $1"; exit 1 ;;
        *)
            if [[ -z "$LOCAL_FILE" ]]; then
                LOCAL_FILE="$1"
            elif [[ -z "$REMOTE_PATH" ]]; then
                REMOTE_PATH="$1"
            fi
            shift ;;
    esac
done

if [[ -z "$LOCAL_FILE" ]]; then
    echo "Usage: ./deploy.sh <local_file.py> [remote_path] [--run] [--cluster-id ID]"
    exit 1
fi

# Derive remote path from filename if not provided
if [[ -z "$REMOTE_PATH" ]]; then
    BASENAME=$(basename "$LOCAL_FILE" .py)
    REMOTE_PATH="${WORKSPACE_ROOT}/${BASENAME}"
fi

echo "▶ Uploading: $LOCAL_FILE → $REMOTE_PATH"
databricks workspace import "$REMOTE_PATH" \
    --file "$LOCAL_FILE" \
    --format SOURCE \
    --language PYTHON \
    --overwrite
echo "  ✓ Uploaded"

if [[ "$DO_RUN" == "true" ]]; then
    echo "▶ Submitting run on cluster: $CLUSTER_ID"
    RUN_OUTPUT=$(databricks jobs submit --no-wait --json "{
        \"run_name\": \"$(basename "$REMOTE_PATH")-$(date +%Y%m%d-%H%M%S)\",
        \"tasks\": [{
            \"task_key\": \"notebook\",
            \"existing_cluster_id\": \"$CLUSTER_ID\",
            \"notebook_task\": {
                \"notebook_path\": \"$REMOTE_PATH\"
            }
        }]
    }" --output json)
    RUN_ID=$(echo "$RUN_OUTPUT" | python3 -c "import json,sys; print(json.load(sys.stdin)['run_id'])")
    echo "  ✓ Run submitted: run_id=$RUN_ID"
    echo "  ✓ Monitor: databricks jobs get-run $RUN_ID"
    echo "  ✓ Output:  databricks jobs export-run $RUN_ID --views-to-export ALL"
fi
