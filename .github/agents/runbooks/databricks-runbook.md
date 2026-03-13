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
Poll until `life_cycle_state` is `TERMINATED`.

- `result_state` = `SUCCESS` → fetch output
- `result_state` = `FAILED` → fetch error, diagnose, fix, re-deploy (up to 2 attempts)

## Fetching results

### Full per-cell output (preferred)
```bash
databricks jobs export-run <run_id> --views-to-export ALL -o json
```
Returns HTML with a double-encoded notebook model inside a
`__DATABRICKS_NOTEBOOK_MODEL` JS variable. Decode order:
```python
import json, urllib.parse, base64, re

raw = open('output.html').read()          # or from subprocess stdout
match = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", raw)
encoded = match.group(1)
model = json.loads(
    urllib.parse.unquote(
        base64.b64decode(
            urllib.parse.unquote(encoded)
        ).decode('utf-8', errors='replace')
    )
)
# model['commands'] — list of cells, each has 'command' (code) and 'results'
```

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
/Workspace/rads/projects/<year>_<CC>_<shortname>/<notebook_name>
```
In most cases, notebooks stay in the git repo and deploy via `deploy.sh`.
