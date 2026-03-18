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

# Derive remote path: prefer project manifest, fall back to WORKSPACE_ROOT
if [[ -z "$REMOTE_PATH" ]]; then
    # Check if notebook lives under a project with a manifest
    _project_yaml=""
    _dir="$(dirname "$LOCAL_FILE")"
    while [[ "$_dir" != "." && "$_dir" != "/" ]]; do
        if [[ -f "$_dir/project.yaml" ]]; then
            _project_yaml="$_dir/project.yaml"
            break
        fi
        _dir="$(dirname "$_dir")"
    done

    if [[ -n "$_project_yaml" ]]; then
        # Extract databricks_workspace_root from manifest (simple grep)
        _ws_root=$(grep 'databricks_workspace_root:' "$_project_yaml" | head -1 | sed 's/.*: *//')
        if [[ -n "$_ws_root" ]]; then
            # Compute relative path from project root to notebook
            _proj_dir="$(dirname "$_project_yaml")"
            _rel="${LOCAL_FILE#"$_proj_dir/"}"
            # Strip .py extension for Databricks workspace path
            _rel_no_ext="${_rel%.py}"
            REMOTE_PATH="${_ws_root}/${_rel_no_ext}"
        fi
    fi

    # Fall back to legacy WORKSPACE_ROOT if no manifest found
    if [[ -z "$REMOTE_PATH" ]]; then
        BASENAME=$(basename "$LOCAL_FILE" .py)
        REMOTE_PATH="${WORKSPACE_ROOT}/${BASENAME}"
    fi
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
