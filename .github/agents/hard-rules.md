# Hard Rules — always apply these

These rules are non-negotiable. Read this file at the start of every conversation.

1. **Never hallucinate column names.** Only use columns documented in the
   relevant reference file or discovered by inspecting `df.printSchema()` in a
   notebook cell.

2. **Always filter preprints** with `column_functions.nopp()` as the first
   filter on any ANI query.

3. **Use `df_cached`** for any intermediate step that takes > 30 seconds or
   is reused downstream.

4. **Project path naming** — use `<year>_<CCC>_<shortname>` where `<CCC>` is the
   client's ISO 3166-1 **alpha-3** country code (3 letters, e.g. `USA`, `GBR`,
   `NLD`), or `INTERNAL` when there is no external client. Keep `<shortname>`
   short, lowercase, underscores only.
   Full path pattern:
   `/mnt/els/rads-projects/short_term/<year>/<year>_<CCC>_<shortname>/`

5. **Do not overwrite** existing notebooks without asking the user first.

6. **Store all code in `notebooks/`** — every file written for a project (both
   the Databricks `.py` notebook and any local post-processing scripts) lives
   under `notebooks/<project_shortname>/` in the git repo. This makes the
   analysis fully reproducible. Use sub-folders per project:
   ```
   notebooks/<shortname>/<shortname>_spark.py       # Databricks notebook
   notebooks/<shortname>/<shortname>_postprocess.py  # local charts / exports
   ```
   Flat placement directly under `notebooks/` is acceptable for single-file
   analyses, but a sub-folder is preferred whenever there is more than one file.

6. **Never embed credentials.** The Databricks CLI uses the pre-configured
   profile. Never store or request AWS keys.

7. **Idempotency** — `df_cached` is write-once. If the user says "re-run from
   scratch", delete the cache folder before re-deploying:
   ```python
   dbutils.fs.rm(cache_folder, recurse=True)
   ```

8. **Journal lookup** — when the user asks about a specific journal and you
   don't know its `source.srcid`, add a notebook cell to discover it:
   ```python
   spark.table(f'scopus.ani_{ani_stamp}').filter(
       F.lower(F.col('source.sourcetitle')).contains('<journal name lower>')
   ).select('source.srcid', 'source.sourcetitle').distinct().show(truncate=False)
   ```

9. **Present answers in user-friendly language** — no raw PySpark jargon
   in the summary paragraph.

10. **Local venv** — always use `.venv` in the repo root for local Python work.
    Never install into system Python. Ensure it exists before running local
    scripts:
    ```bash
    [[ -d .venv ]] || { python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt; }
    source .venv/bin/activate
    ```

11. **S3 / AWS** — before making AWS CLI calls, try them first. Only run
    `~/go-aws-sso` if a call fails with `InvalidClientTokenId`, `ExpiredToken`,
    or any other authentication error. Running `~/go-aws-sso` always triggers
    a browser prompt, so do not run it preemptively. When authentication is
    needed, it prints a browser URL + device code — present this to the user
    and wait for approval. It may then prompt for account/role selection;
    present the list to the user and ask which to choose, defaulting to
    `Data Science Production 029211843733` / `EnterpriseAdmin`. Full EDC
    procedure is in `runbooks/aws-and-s3-runbook.md`.

12. **Subagent orchestration** — for larger tasks (multiple notebooks, multiple
    analytical questions, or a mix of Spark + local post-processing), decompose
    the work and delegate self-contained steps to subagents using
    `agent/runSubagent`. The main session acts as **orchestrator** (planning,
    sequencing, synthesising results); subagents act as **executors**
    (one well-scoped task each). Rules:
    - Write a complete, self-contained prompt per subagent — include all
      context it needs (paths, column names, expected output) since subagents
      have no shared state.
    - Prefer the `Explore` agent for pure read/search tasks.
    - Prefer the `analyst` agent for Spark notebook generation and deployment.
    - Do not launch subagents in parallel when they depend on each other's
      output — sequence them and pass results forward in each prompt.
    - Collect and synthesise all subagent results before presenting to the user.

12. **DuckDB for local analytics** — prefer DuckDB over pandas for GROUP BY,
    JOIN, or window operations on parquet read from S3. Use pandas only for
    final formatting and chart data prep.

13. **Charts** — when generating visualisations, save them as files (PNG/HTML)
    in a local `output/` folder and report the path to the user.

14. **Snapshot convention** — always use 1st-of-month ANI stamps (`20260301`,
    not `20260312`). Daily snapshots are deleted after ~2 weeks; monthly
    snapshots persist ~1 year.

15. **LEFT JOIN by default** when enriching ANI with any secondary table
    (APR, OrgDB, Source, SciVal, SDG, Patents, ADS). Coverage is always partial.
    Cast types as needed (e.g. `afid` long → string for OrgDB).
