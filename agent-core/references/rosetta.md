# Rosetta Reference — `mi_team.rosetta_YYYYMMDD`

Rosetta is a metastore snapshot table family in database `mi_team`.
Each snapshot is a table named `rosetta_<YYYYMMDD>`.

Use via library helper:
```python
import snapshot_functions as sf

snapshots = sf.rosetta.list_snapshots()      # newest first

# latest snapshot
df_rosetta = sf.rosetta.get_table()

# specific snapshot
df_rosetta = sf.rosetta.get_table('20251022')
```

---

## Join contract

- Rosetta key: `srcid` (string)
- ANI key: `source.srcid` (long)
- Join rule: cast ANI `source.srcid` to string before joining Rosetta
- Join direction: always `LEFT JOIN` from ANI

Example:
```python
df_ani_src = spark.table(f"scopus.ani_{ani_stamp}") \
    .filter(column_functions.nopp()) \
    .select(F.col("Eid"), F.col("source.srcid").cast("string").alias("srcid"))

df_rosetta = sf.rosetta.get_table()

df_joined = df_ani_src.join(df_rosetta, on="srcid", how="left")
```

---

## What Rosetta contains

Rosetta is a source/journal reference mapping table with fields including:

- Source identifiers: `srcid`, `cwts_id`, `crossref_id`, `jcr_abbrev`, `nlm_id`
- Title/flags: `title`, `flag`
- ISSN variants: `issn1`, `issn2`, `issn1h`, `issn2h`
- Publisher/imprint tracking across years
- Elsevier category/business fields
- BM and BM delay fields across years

Observed latest schema profile:

- ~69 columns (string-heavy schema)
- Snapshot naming: `rosetta_YYYYMMDD`

---

## Notes

- Snapshot selection should use closest-date logic when exact date is absent.
- Keep this as a metastore read (`spark.table`), not DBFS path loading.
