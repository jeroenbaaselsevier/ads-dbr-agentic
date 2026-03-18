# Lessons Learned — AUM Burst Score Analysis
# Session: 2026-03-14  |  Project: 2026_KW_aum_burst
# Status: captured — ready for triage

---

## LES-2026-03-14-001

id: LES-2026-03-14-001
project_id: 2026_KW_aum_burst
scope: global
memory_type: semantic
category: schema
confidence: high
status: captured
captured_at: 2026-03-14
impact: high
recurrence_hint: systematic

summary: >
  The `topiccluster_keywords` and `topic_keywords` SciVal tables store keywords
  as ARRAY<STRUCT<KeywordRank: BIGINT, Keyword: STRING>>, NOT as
  ARRAY<STRING>. Calling `array_join(slice(col, 1, 3), ', ')` directly fails
  with DATATYPE_MISMATCH. The correct pattern is to first extract the string
  field with transform():
    F.array_join(
        F.slice(F.transform(F.col('Keywords'), lambda kw: kw['Keyword']), 1, 3),
        ', '
    )
  This applies to both `topiccluster_keywords` and `topic_keywords`.

observed_in:
  - notebooks/aum_burst_topic_clusters.py

suggested_action:
  target: agent-core/references/scival.md
  change_type: doc_update
  note: >
    Add a note under the `topiccluster_keywords` and `topic_keywords` table
    entries clarifying the Keywords column type and providing the correct
    transform pattern.

---

## LES-2026-03-14-002

id: LES-2026-03-14-002
project_id: 2026_KW_aum_burst
scope: global
memory_type: semantic
category: schema
confidence: high
status: captured
captured_at: 2026-03-14
impact: medium
recurrence_hint: systematic

summary: >
  The `topic_burst.topic_cluster` table includes a column `PromPerc_by_year`
  of type MAP<BIGINT, FLOAT>. CSV export via `export_df_csv` fails on this
  column with:
    [UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The CSV datasource doesn't support
    the column `PromPerc_by_year` of the type "MAP<BIGINT, FLOAT>".
  Always drop map/array/struct columns from the DataFrame before calling
  export_df_csv. The underlying parquet cache can retain them.
  Pattern: `df_export = df_result.drop('PromPerc_by_year')`

observed_in:
  - notebooks/aum_burst_topic_clusters.py

suggested_action:
  target: agent-core/references/ads-derived/  (new burst reference file)
  change_type: doc_update
  note: >
    Document the schema of topic_burst.topic_cluster and topic_burst.topic,
    explicitly calling out which columns are non-CSV-serialisable map types
    and must be dropped before export_df_csv.

---

## LES-2026-03-14-003

id: LES-2026-03-14-003
project_id: 2026_KW_aum_burst
scope: global
memory_type: procedural
category: workflow
confidence: high
status: captured
captured_at: 2026-03-14
impact: high
recurrence_hint: repeated

summary: >
  The Databricks polling loop must break on INTERNAL_ERROR / SKIPPED as well
  as TERMINATED. Checking only for "TERMINATED" causes the loop to run
  forever when the job fails with INTERNAL_ERROR (which is a terminal state
  but does not contain the word "TERMINATED").
  Correct break condition:
    echo "$RESULT" | grep -qE "TERMINATED|INTERNAL_ERROR|SKIPPED" && break
  Also: `databricks jobs get-run-output <run_id>` only works for single-task
  runs. For multi-task runs submitted via `jobs submit` use the task-level
  run_id extracted from `get-run .tasks[].run_id`.

observed_in:
  - session monitoring commands

suggested_action:
  target: agent-core/runbooks/databricks.md
  change_type: doc_update
  note: >
    Update the "Poll run status" code snippet to break on all terminal states,
    not just TERMINATED. Add a note on using the task-level run_id for
    get-run-output.

---

## LES-2026-03-14-004

id: LES-2026-03-14-004
project_id: 2026_KW_aum_burst
scope: global
memory_type: procedural
category: workflow
confidence: high
status: captured
captured_at: 2026-03-14
impact: medium
recurrence_hint: systematic

summary: >
  When decoding Databricks notebook export output, cell errors do NOT always
  appear under resultType="error". Cells can have resultType="listResults"
  with an empty data array while the actual Python/Spark exception text is
  hidden in `results.cause` and `results.summary`. The decode script must
  always print `cause` and `summary` for every cell regardless of resultType.
  Additionally, the `data` field can be either a list of dicts OR a plain
  string — the decoder must handle both.

observed_in:
  - tmp/ decode scripts across multiple runs

suggested_action:
  target: agent-core/runbooks/databricks.md
  change_type: doc_update
  note: >
    Update the "Fetch and decode cell output" code example to always extract
    and print `cause` and `summary` fields, and to handle both list and string
    forms of the `data` field.

---

## LES-2026-03-14-005

id: LES-2026-03-14-005
project_id: 2026_KW_aum_burst
scope: global
memory_type: semantic
category: schema
confidence: high
status: captured
captured_at: 2026-03-14
impact: medium
recurrence_hint: first_time

summary: >
  The OrgDB lookup is the correct way to identify institution afids — NOT
  string-matching on ANI.Af[].affiliation_organization. OrgDB gives clean,
  consistent org_ids that include all sub-units via the hierarchy table.
  For AUM specifically: a single org_id (60105846) with no sub-units was
  found, so the hierarchy expansion returned only that one afid.
  The full lookup pattern:
    1. Search orgdb_* by orgname contains lower-case institution name
    2. Filter orglevel != 'Skeletal' AND orgvisibility == 'true'
    3. Collect top-level org_ids
    4. Expand via hierarchy_* WHERE final_attribution='include' AND
       toplevel_orgid IN (top_level_ids) → collect all child org_ids
    5. Use collected org_ids as afid_strs to filter ANI.Af[].afid.cast('string')

observed_in:
  - tmp/aum_orgdb_lookup.py
  - notebooks/aum_burst_topic_clusters.py

suggested_action:
  target: agent-core/references/orgdb.md
  change_type: doc_update
  note: >
    Confirm the recommended pattern for institution lookup and add a note that
    string-matching ANI.Af[].affiliation_organization is unreliable and should
    NOT be used. The OrgDB → hierarchy → afid_str approach is canonical.

---

## LES-2026-03-14-006

id: LES-2026-03-14-006
project_id: 2026_KW_aum_burst
scope: global
memory_type: procedural
category: workflow
confidence: high
status: captured
captured_at: 2026-03-14
impact: medium
recurrence_hint: first_time

summary: >
  `dbfs:/` copy via `databricks fs cp` does not work for instance-profile-based
  mounts (e.g. /mnt/els/rads-projects/). The correct method to download results
  is direct AWS S3 access:
    aws s3 cp s3://rads-projects/short_term/<year>/<project>/file.csv.gz ./tmp/
  The Databricks path /mnt/els/rads-projects/... maps to s3://rads-projects/...
  (drop the /mnt/els/ prefix).

observed_in:
  - session download commands

suggested_action:
  target: agent-core/runbooks/databricks.md
  change_type: doc_update
  note: >
    Add an explicit warning that `databricks fs cp` does not work for
    instance-profile-based mounts. Document the aws s3 route as the canonical
    download method and reference the path mapping table in aws-and-s3-runbook.md.

---

## LES-2026-03-14-007

id: LES-2026-03-14-007
project_id: 2026_KW_aum_burst
scope: global
memory_type: procedural
category: workflow
confidence: medium
status: captured
captured_at: 2026-03-14
impact: low
recurrence_hint: first_time

summary: >
  `~/go-aws-sso --force` should be used when a previous SSO session left a
  lock file (error: "There is already an authorization flow running"). Using
  `--force` clears the lock and starts fresh. However, any subsequent `aws`
  CLI call that is configured to use go-aws-sso as a credential process will
  re-invoke it, so AWS CLI tests should only be run after the SSO flow has
  fully completed and credentials are cached.

observed_in:
  - session AWS auth commands

suggested_action:
  target: agent-core/runbooks/aws-and-s3-runbook.md
  change_type: doc_update
  note: >
    Add a note: when go-aws-sso reports "authorization flow already running",
    use --force. Do not interleave aws CLI calls with an in-progress go-aws-sso
    invocation.

---

## LES-2026-03-14-008

id: LES-2026-03-14-008
project_id: 2026_KW_aum_burst
scope: client
memory_type: episodic
category: profile
confidence: high
status: captured
captured_at: 2026-03-14
impact: medium
recurrence_hint: first_time

summary: >
  Client context: AUM (American University of the Middle East), Kuwait.
  OrgDB org_id: 60105846. Single afid, no sub-units in hierarchy.
  Output: 2,789 papers 2020–2024 (265→880, strong growth), across 584 topic
  clusters and 1,386 topics. Strongest topic by paper count: Topic 805
  (Hadron/Lepton/Standard Model, 202 papers, 11.7% world share). Fastest
  trending cluster with meaningful output: Cluster 499 (Green Innovation/ESG,
  48 papers, burst=2.3). Highest topic share: Topic 28273 (Autonomous
  Vehicles, 23 papers, 18.1% world share).
  Analysis type: burst score / prominent topic cluster analysis for a
  university visit — forward-looking, slide-ready output.

observed_in:
  - notebooks/aum_burst_topic_clusters.py
  - output/aum_clusters_*.png
  - output/aum_topics_*.png

suggested_action:
  target: agent-core/profiles/  (new client profile or existing KW profile)
  change_type: profile_update
  note: >
    Consider creating a KW (Kuwait) client profile capturing AUM's org_id,
    paper volume, and primary research profile for future reference.

---

## LES-2026-03-14-009

id: LES-2026-03-14-009
project_id: 2026_KW_aum_burst
scope: global
memory_type: procedural
category: output_style
confidence: medium
status: captured
captured_at: 2026-03-14
impact: medium
recurrence_hint: first_time

summary: >
  For the NSFC-style bubble chart analysis (prominence vs burst, institution
  share on Y-axis), three distinct charts are expected:
    1. Top 20 by global prominence rank
    2. Top 20 by institution's highest share (within prominent clusters)
    3. Top 20 by fastest trending (burst score)
  And this should be done at both topic cluster AND topic granularity = 6
  charts total. The original request referenced 3 slides, each showing a
  different cut of the data. Generating only 2 charts (prominent + trending)
  misses the "highest share" view which is arguably the most client-relevant
  (where is the institution already a world leader?).

observed_in:
  - tmp/make_aum_charts.py
  - output/aum_clusters_*.png
  - output/aum_topics_*.png

suggested_action:
  target: agent-core/recipes/scival-topic-analysis.md
  change_type: recipe_update
  note: >
    Extend the burst score recipe to specify all three chart types (prominent,
    highshare, trending) and note that both topic and topic-cluster granularity
    should be produced. Add the correct chart axis definitions:
    X = burst_prominence, Y = institution_share_pct, size = paper_count,
    colour = burst_prominence (RdYlGn colormap, vmin=-10 vmax=10).
