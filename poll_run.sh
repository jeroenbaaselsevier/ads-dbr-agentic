#!/usr/bin/env bash
# poll_run.sh — wait for a Databricks job run to reach a terminal state
#
# Usage:
#   ./poll_run.sh <run_id> [interval_seconds]
#
# Arguments:
#   run_id            Databricks run ID (required)
#   interval_seconds  Poll interval (default: 30)
#
# Exit codes:
#   0  run finished with result_state SUCCESS
#   1  run finished with result_state FAILED or CANCELED
#   2  run hit INTERNAL_ERROR or SKIPPED
#   3  timed out after max polls (default: 120 polls = 60 min at 30s)
#
# Example:
#   RUN_ID=$(./deploy.sh notebooks/my_analysis.py --run | grep run_id | ...)
#   ./poll_run.sh "$RUN_ID" && ./fetch_run.sh "$RUN_ID"

set -euo pipefail

RUN_ID="${1:?Usage: poll_run.sh <run_id> [interval_seconds]}"
INTERVAL="${2:-30}"
MAX_POLLS=120

echo "[$(date +%H:%M:%S)] Polling run $RUN_ID every ${INTERVAL}s (max ${MAX_POLLS} polls)"

for i in $(seq 1 "$MAX_POLLS"); do
  sleep "$INTERVAL"

  STATE_JSON=$(databricks jobs get-run "$RUN_ID" -o json 2>&1) || {
    echo "[$(date +%H:%M:%S)] poll $i: CLI error — $STATE_JSON"
    continue
  }

  READ=$(python3 -c "
import json, sys
r = json.loads(sys.stdin.read())
s = r['state']
print(s['life_cycle_state'], s.get('result_state', ''))
" <<< "$STATE_JSON")

  LIFECYCLE=$(echo "$READ" | awk '{print $1}')
  RESULT=$(echo "$READ" | awk '{print $2}')

  echo "[$(date +%H:%M:%S)] poll $i: $LIFECYCLE ${RESULT:-(running)}"

  case "$LIFECYCLE" in
    TERMINATED)
      if [[ "$RESULT" == "SUCCESS" ]]; then
        echo "[$(date +%H:%M:%S)] ✓ Run $RUN_ID succeeded."
        exit 0
      else
        echo "[$(date +%H:%M:%S)] ✗ Run $RUN_ID finished: $RESULT"
        exit 1
      fi
      ;;
    INTERNAL_ERROR)
      echo "[$(date +%H:%M:%S)] ✗ Run $RUN_ID hit INTERNAL_ERROR (cluster/infra failure)."
      echo "  Check the run in the Databricks UI for the cluster event log."
      echo "  Do NOT re-submit automatically — investigate first."
      exit 2
      ;;
    SKIPPED)
      echo "[$(date +%H:%M:%S)] ✗ Run $RUN_ID was SKIPPED (concurrency policy)."
      exit 2
      ;;
  esac
done

echo "[$(date +%H:%M:%S)] ✗ Timed out after $MAX_POLLS polls ($((MAX_POLLS * INTERVAL / 60)) min)."
exit 3
