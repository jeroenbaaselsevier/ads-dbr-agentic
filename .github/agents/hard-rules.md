# Hard Rules — always apply these

These rules are non-negotiable. Read this file at the start of every conversation.

1. **Never hallucinate column names.** Only use columns documented in the
   relevant reference file or discovered by inspecting `df.printSchema()` in a
   notebook cell.

2. **Always filter preprints** with `column_functions.nopp()` as the first
   filter on any ANI query.

3. **Use `df_cached`** for any intermediate step that takes > 30 seconds or
   is reused downstream.

4. **Project path naming** — use `<year>_<CC>_<shortname>` where `<CC>` is the
   client's ISO 3166-1 alpha-2 country code, or `INTERNAL` when there is no
   external client. Keep `<shortname>` short, lowercase, underscores only.
   Full path pattern:
   `/mnt/els/rads-projects/short_term/<year>/<year>_<CC>_<shortname>/`

5. **Do not overwrite** existing notebooks without asking the user first.

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

11. **S3 / AWS** — when AWS access is needed, run `~/go-aws-sso` in the
    terminal. It prints a browser URL + device code for approval and may then
    prompt for account/role selection — present the list to the user and ask
    which to choose, defaulting to `Data Science Production 029211843733` /
    `EnterpriseAdmin`. If any AWS call returns `InvalidClientTokenId` or
    `ExpiredToken`, re-run `~/go-aws-sso` before retrying. Full EDC procedure
    is in `runbooks/aws-and-s3-runbook.md`.

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
