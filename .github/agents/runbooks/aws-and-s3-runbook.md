# AWS and S3 Runbook

## Databricks mount → S3 bucket mapping

Paths under `/mnt/els/` map directly to S3 buckets. Drop `/mnt/els/` and
replace with `s3://`.

| Databricks path | S3 URI |
|---|---|
| `/mnt/els/rads-projects/…` | `s3://rads-projects/…` |
| `/mnt/els/rads-main/…` | `s3://rads-main/…` |
| `/mnt/els/rads-mappings/…` | `s3://rads-mappings/…` |
| `/mnt/els/rads-pipelines/…` | `s3://rads-pipelines/…` |
| `/mnt/els/rads-users/…` | `s3://rads-users/…` |
| `/mnt/els/rads-restricted/…` | `s3://rads-restricted/…` |

## Common AWS CLI commands

```bash
# List a path
aws s3 ls s3://rads-projects/short_term/2026/my_project/

# Check _SUCCESS marker (confirms a Spark write completed)
aws s3 ls s3://rads-main/mappings_and_metrics/.../TableName/_SUCCESS

# Download a small result file
aws s3 cp s3://rads-projects/short_term/2026/my_project/output.csv ./tmp/

# Sync a folder locally
aws s3 sync s3://rads-projects/short_term/2026/my_project/results/ ./tmp/results/
```

## Starting an AWS session (go-aws-sso)

Run `~/go-aws-sso` in the terminal. It will print a browser URL + device code.
Tell the user to open the URL and approve. After approval it may prompt for
account/role selection — present the list to the user and ask which to choose,
defaulting to `Data Science Production 029211843733` / `EnterpriseAdmin`.

Credentials are stored in the WSL environment only (not shared with Windows).

If any AWS CLI call returns `InvalidClientTokenId` or `ExpiredToken`,
credentials have expired — re-run `~/go-aws-sso` before retrying.

Never store or request AWS access keys.

## EDC — accessing external (sister-team) buckets

Data from other Elsevier teams is accessed via the **EDC (Elsevier Data Catalog)**,
managed through Collibra at `https://elsevier.collibra.com/apps/`.
Each dataset has a Collibra page listing its Databricks mount point and metadata.

Two types of access grant are required — file both as separate Collibra forms.

### Databricks access request

Grants the Databricks AWS account permission to mount and read the bucket.
After approval, the dataset is accessible at its Collibra-listed mount
(typically `/mnt/els/edc/…`).

| Field | Value |
|---|---|
| Term | `Long Term` |
| AWS Account Name | `aws-rt-databricks-prod` |
| AWS Account Number | `533013353365` |
| AWS Role Name | `AcademicLeadersFunders-AnalyticalDataServices-dev` |

### Direct S3 / AWS CLI access request

Grants our AWS account cross-account read via role chaining.

| Field | Value |
|---|---|
| Term | `Long Term` |
| AWS Account Name | `Data Science Production` |
| AWS Account Number | `029211843733` |
| AWS Role Name | `ads_crossaccount_data_consumer_role` |

### Role chain procedure for EDC

```bash
# Prerequisites: active AWS session (run ~/go-aws-sso if needed)
aws sts get-caller-identity --output json

# Step 1: assume our cross-account consumer role
STEP1=$(aws sts assume-role \
  --role-arn arn:aws:iam::029211843733:role/ads_crossaccount_data_consumer_role \
  --role-session-name edc-session \
  --output json)

# Step 2: export credentials and assume the target dataset role
export AWS_ACCESS_KEY_ID=$(echo $STEP1 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['AccessKeyId'])")
export AWS_SECRET_ACCESS_KEY=$(echo $STEP1 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SecretAccessKey'])")
export AWS_SESSION_TOKEN=$(echo $STEP1 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SessionToken'])")

STEP2=$(aws sts assume-role \
  --role-arn <target_role_arn> \
  --role-session-name edc-data-session \
  --output json)

# Step 3: export second set of credentials, then access the bucket
export AWS_ACCESS_KEY_ID=$(echo $STEP2 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['AccessKeyId'])")
export AWS_SECRET_ACCESS_KEY=$(echo $STEP2 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SecretAccessKey'])")
export AWS_SESSION_TOKEN=$(echo $STEP2 | python3 -c "import json,sys; print(json.load(sys.stdin)['Credentials']['SessionToken'])")

aws s3 ls s3://<bucket-name>/
```

## Known external datasets

| Dataset | S3 bucket | Databricks mount | Target role ARN |
|---|---|---|---|
| ANI parsed (Scopus ANI core) | `sccontent-parsed-ani-core-parquet-prod` | `/mnt/els/edc/seccont-anicore-parsed-edc` | `arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-parsed-edc-01` |
| ANI raw | `sccontent-ani-parquet-prod` | `/mnt/els/edc/seccont-anicore-raw-edc` | `arn:aws:iam::838239208477:role/EDC_814132467461_seccont-anicore-raw-edc` |

## Storage path conventions

| Purpose | Databricks path | S3 equivalent |
|---|---|---|
| Temporary (auto-deleted 1 day) | `/mnt/els/rads-projects/temporary_to_be_deleted/1d/` | `s3://rads-projects/temporary_to_be_deleted/1d/` |
| Short-term projects | `/mnt/els/rads-projects/short_term/<year>/<year>_<CC>_<shortname>/` | `s3://rads-projects/short_term/<year>/<year>_<CC>_<shortname>/` |
| ADS metrics outputs | `/mnt/els/rads-main/mappings_and_metrics/bibliometrics/…` | `s3://rads-main/mappings_and_metrics/bibliometrics/…` |
| Restricted data (gender) | `/mnt/els/rads-restricted/namsor/…` | `s3://rads-restricted/namsor/…` |
