# Copilot Instructions — ads-dbr-agentic

This workspace develops PySpark analytics notebooks that run on a remote
**Databricks** cluster. Code is written locally and deployed via the
Databricks CLI.

---

## Environment

| Item | Value |
|---|---|
| Databricks workspace | `https://elsevier-dev.cloud.databricks.com` |
| Databricks CLI version | **v0.282.0** |
| Default cluster | `rads-private-unity` (`0107-154653-j5wd510m`) |
| Default Spark library path | `/Workspace/rads/library/` |
| Local library mirror | `./rads_library/` (run `./sync_library.sh` to refresh) |
| ADS metrics pipeline code | `./rads_metrics_code/` (run `./sync_metrics_code.sh` to refresh) |
| Temp file scratch space | `./tmp/` — use this for all temp files to avoid OS-level permission prompts |

---

## Agent knowledge base

All detailed knowledge for authoring analytics notebooks lives under
`.github/agents/`. The entry points are:

| File | Purpose |
|---|---|
| `.github/agents/hard-rules.md` | Non-negotiable behavioural rules — always apply |
| `.github/agents/knowledge-index.yaml` | Index of all reference files, runbooks, and recipes with keywords and join caveats |
| `.github/agents/references/` | Per-table schemas: `ani.md`, `apr.md`, `orgdb.md`, `source.md`, `scival.md`, `sdg.md`, `patents.md`, `ads-derived/`, `library.md` |
| `.github/agents/runbooks/` | Step-by-step procedures: `databricks-runbook.md`, `local-python-runbook.md`, `aws-and-s3-runbook.md` |
| `.github/agents/recipes/` | Copy-paste notebook templates for common analysis patterns |

For analytics tasks, invoke the **analyst** agent (`.github/agents/analyst.agent.md`).
The analyst reads `hard-rules.md` and `knowledge-index.yaml` at the start of
every conversation, then consults the relevant reference files and recipes as needed.
