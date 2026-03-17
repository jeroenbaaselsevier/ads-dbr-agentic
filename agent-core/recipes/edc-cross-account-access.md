---
name: edc-cross-account-access
triggers:
  - EDC
  - cross-account
  - ANI parsed
  - ANI raw
  - Scopus content team
  - cross-account S3
required_tables: []
required_functions: []
common_outputs:
  - parquet
pitfalls:
  - requires cross-account IAM role assumption
  - must use EDC-specific S3 bucket paths
review_checks:
  - verify EDC role is assumed before accessing bucket
  - refer to aws-and-s3 runbook for full procedure
---

# Recipe: EDC Cross-Account Access

## When to use
Accessing datasets from other Elsevier teams (e.g. ANI parsed/raw from the
Scopus content team) via the EDC cross-account S3 role chain.

For the full procedure and access request details, see
`.github/agents/runbooks/aws-and-s3-runbook.md`.

## Prerequisites
- Active AWS session (`~/go-aws-sso` in WSL)
- Collibra access approval (both Databricks and direct S3 grants)
- Target role ARN (emailed after Collibra approval)

## Notebook template — reading EDC data on Databricks

```python
# Databricks notebook source
# EDC data is accessed via the Databricks mount after Databricks access is approved

# COMMAND ----------
import sys, os
sys.path.append('/Workspace/rads/library/')
import dataframe_functions

# ANI parsed (EDC mount — available after Databricks access is granted)
edc_mount    = '/mnt/els/edc/seccont-anicore-parsed-edc'
ani_stamp    = '20260301'

# List available snapshots
dbutils.fs.ls(edc_mount)

# Load a snapshot
df_edc_ani = spark.read.parquet(f'{edc_mount}/{ani_stamp}/')
df_edc_ani.printSchema()
```

## Local S3 access (after direct S3 grant approved)

Run this in the terminal to set up the role-chained credentials:

```bash
# 1. Ensure active AWS session
~/go-aws-sso

# 2. Assume our cross-account consumer role
STEP1=$(aws sts assume-role \
  --role-arn arn:aws:iam::029211843733:role/ads_crossaccount_data_consumer_role \
  --role-session-name edc-session \
  --output json)

export AWS_ACCESS_KEY_ID=$(echo $STEP1 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['AccessKeyId'])")
export AWS_SECRET_ACCESS_KEY=$(echo $STEP1 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SecretAccessKey'])")
export AWS_SESSION_TOKEN=$(echo $STEP1 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SessionToken'])")

# 3. Assume the target dataset role (ANI parsed example)
STEP2=$(aws sts assume-role \
  --role-arn arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-parsed-edc-01 \
  --role-session-name edc-data-session \
  --output json)

export AWS_ACCESS_KEY_ID=$(echo $STEP2 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['AccessKeyId'])")
export AWS_SECRET_ACCESS_KEY=$(echo $STEP2 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SecretAccessKey'])")
export AWS_SESSION_TOKEN=$(echo $STEP2 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SessionToken'])")

# 4. Access the bucket
aws s3 ls s3://sccontent-parsed-ani-core-parquet-prod/
```

## Known external datasets

| Dataset | S3 bucket | Databricks mount | Target role ARN |
|---|---|---|---|
| ANI parsed | `sccontent-parsed-ani-core-parquet-prod` | `/mnt/els/edc/seccont-anicore-parsed-edc` | `arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-parsed-edc-01` |
| ANI raw | `sccontent-ani-parquet-prod` | `/mnt/els/edc/seccont-anicore-raw-edc` | `arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-raw-edc` |

## Common pitfalls
- Role chain credentials expire after 1 hour; re-run from step 2 (not step 1) to refresh.
- The Databricks mount and the direct S3 access are separate approvals — both
  must be requested independently on Collibra.
- `--no-sign-request` does NOT work for EDC buckets; role chaining is required.
