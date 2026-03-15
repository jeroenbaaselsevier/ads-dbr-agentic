# Databricks Runbook

## Notebook format

All notebooks are plain `.py` files using Databricks cell separators:

```python
# Databricks notebook source

# COMMAND ----------
# cell 1 code here

# COMMAND ----------
# cell 2 code here
```

## Deploying notebooks

### Upload only
```bash
./deploy.sh notebooks/my_analysis.py
# deploys to /Workspace/Users/J.Baas@elsevier.com/my_analysis
```

### Upload and run on default cluster
```bash
./deploy.sh notebooks/my_analysis.py --run
```

### Upload and run on a specific cluster
```bash
./deploy.sh notebooks/my_analysis.py --run --cluster-id 0303-153342-zrgyfy1c
```

### Upload to a custom remote path
```bash
./deploy.sh notebooks/my_analysis.py /Workspace/Users/J.Baas@elsevier.com/subfolder/my_analysis --run
```

## Clusters

| cluster_id | name | notes |
|---|---|---|
| `0107-154653-j5wd510m` | **rads-private-unity** | Default for development |

Pass `--cluster-id` to `deploy.sh` or set `DATABRICKS_CLUSTER_ID` in
`.databricks/.databricks.env` to use a different cluster.

## Monitoring a running job

`RUN_ID` is a **positional argument** in CLI v0.282.0 (no `--run-id` flag):
```bash
databricks jobs get-run <run_id> -o json | python3 -c "
import json, sys
r = json.load(sys.stdin)
state = r['state']
print(f\"Status: {state['life_cycle_state']}  Result: {state.get('result_state', 'n/a')}\")
"
```

**Terminal `life_cycle_state` values** — stop polling immediately on any of these:

| `life_cycle_state` | Meaning |
|---|---|
| `TERMINATED` | Job finished (check `result_state` for SUCCESS/FAILED) |
| `INTERNAL_ERROR` | Databricks infrastructure failure (cluster start, driver crash) |
| `SKIPPED` | Job was skipped due to a concurrency policy |

> **Do not** poll only for `TERMINATED`. `INTERNAL_ERROR` is also a terminal
> state — continuing to poll after seeing it creates an infinite loop.

### Using `poll_run.sh` (preferred)

```bash
./poll_run.sh <run_id>              # poll every 30s (default)
./poll_run.sh <run_id> 60           # poll every 60s
```

Exit codes: `0` = SUCCESS, `1` = FAILED/CANCELED, `2` = INTERNAL_ERROR/SKIPPED,
`3` = timed out (120 polls × interval).

Typical deploy-and-wait one-liner:
```bash
RUN_ID=$(./deploy.sh notebooks/my_analysis.py --run | python3 -c "
import json,sys; print(json.load(sys.stdin)['run_id'])
") && ./poll_run.sh "$RUN_ID"
```

### Manual one-off poll (without helper)

```bash
for i in $(seq 1 60); do
  sleep 30
  STATUS=$(databricks jobs get-run <run_id> -o json | python3 -c "
import json, sys
r = json.load(sys.stdin); s = r['state']
print(s['life_cycle_state'], s.get('result_state', ''))
")
  echo "[$(date +%H:%M)] poll $i: $STATUS"
  echo "$STATUS" | grep -qE "TERMINATED|INTERNAL_ERROR|SKIPPED" && break
done
```

- `result_state` = `SUCCESS` → fetch output
- `result_state` = `FAILED` → fetch error via `export-run`, diagnose, fix, re-deploy (up to 2 attempts)
- `life_cycle_state` = `INTERNAL_ERROR` → cluster/infrastructure problem; check the Databricks run UI for the cluster event log, do **not** re-submit blindly

## Fetching results

### Full per-cell output (preferred)
```bash
databricks jobs export-run <run_id> --views-to-export ALL -o json > ./tmp/run_output.html
```
Returns HTML with a double-encoded notebook model inside a
`__DATABRICKS_NOTEBOOK_MODEL` JS variable. Decode order:
```python
import json, urllib.parse, base64, re

raw = open('./tmp/run_output.html').read()
match = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", raw)
encoded = match.group(1)
model = json.loads(
    urllib.parse.unquote(
        base64.b64decode(
            urllib.parse.unquote(encoded)
        ).decode('utf-8', errors='replace')
    )
)
# model['commands'] — list of cells, each has 'command' (source) and 'results'
```

### Reading cell results and finding errors

Each cell's `results` object has a `type` field. **Always inspect both the
`data` array and top-level fields** — Spark/Analysis exceptions are stored
differently from normal cell output:

| `results.type` | Where output lives | Notes |
|---|---|---|
| `"text"` | `results.data` — list of `{type, data}` items | Normal `print()` / display output |
| `"ansi"` | `results.data[*].data` | ANSI-coloured text (e.g. DataFrames) |
| `"error"` | `results.data[*]` items with `type == "ansi"` | Python exceptions |
| `"listResults"` | `results.data` | Multi-output cells; may be **empty** even when an error occurred |

**Spark AnalysisException / cluster-level errors** are stored at the *top level*
of `results`, not inside `data`:

```python
for i, cmd in enumerate(model['commands']):
    r = cmd.get('results') or {}
    rtype = r.get('type', '')

    # Normal printed output
    for item in r.get('data', []):
        if item.get('type') == 'ansi':
            print(f"Cell {i}: {item['data']}")

    # Spark / Analysis exceptions — sit at top level, not in data[]
    # These appear even when type == 'listResults' with empty data
    if r.get('cause'):
        print(f"Cell {i} CAUSE:\n{r['cause']}")
    if r.get('summary'):
        print(f"Cell {i} SUMMARY:\n{r['summary']}")

    # Explicit error type
    if rtype == 'error':
        for item in r.get('data', []):
            print(f"Cell {i} ERROR: {item.get('data', '')}")
```

Key gotcha: a cell with `type == "listResults"` and `data == []` is **not
necessarily clean** — always check `results.cause` and `results.summary` on
every cell regardless of type, to catch Spark analysis exceptions that bypass
the normal error path.

### Simple exit value only
```bash
databricks jobs get-run-output <run_id>
```
Only returns `dbutils.notebook.exit()` output (first 5 MB). Use this only
when the notebook explicitly calls `dbutils.notebook.exit(result_str)`.

## Submitting a one-time run manually (without deploy.sh)

`databricks runs submit` **does not exist** in v0.282.0. Use
`databricks jobs submit`:
```bash
databricks jobs submit --no-wait --json '{
  "run_name": "my-run",
  "tasks": [{
    "task_key": "notebook",
    "existing_cluster_id": "0107-154653-j5wd510m",
    "notebook_task": { "notebook_path": "/Workspace/Users/J.Baas@elsevier.com/my_analysis" }
  }]
}' -o json
# stdout: JSON with run_id
```

## Uploading plain files (library modules, not notebooks)

```bash
databricks workspace import /Workspace/rads/library/my_file.py \
    --file rads_library/my_file.py --format RAW --overwrite
```

- `--format SOURCE --language PYTHON` creates a **NOTEBOOK** object
- `--format RAW` creates a **FILE** object
- These types cannot be overwritten with each other — remote type must match

## Workspace notebook path convention

For notebooks to be stored persistently on Databricks:
```
/Workspace/rads/projects/<year>_<CCC>_<shortname>/<notebook_name>
```
In most cases, notebooks stay in the git repo and deploy via `deploy.sh`.
