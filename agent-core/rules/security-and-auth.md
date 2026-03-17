# Security and Auth Rules

Rules governing credentials, AWS authentication, and secret handling.
Extracted from `core-rules.md` rules 7 and 12 for quick reference.

## Credentials

- **Never embed credentials** in notebooks, scripts, or config files.
- The Databricks CLI uses a pre-configured profile — never store or request
  tokens manually.
- Never store or request AWS keys inline; use `go-aws-sso` for session
  credentials.

## AWS / S3 authentication

Run `~/go-aws-sso` **at the very start of the session** when any S3/AWS access
is required — before doing anything else. This avoids browser prompts mid-task
when the user may be AFK.

When `go-aws-sso` runs:
1. It prints a browser URL + device code — present this to the user and wait
   for approval before continuing.
2. It may then prompt for account/role selection. Present the list to the user
   and ask which to choose, defaulting to:
   - Account: `Data Science Production 029211843733`
   - Role: `EnterpriseAdmin`

Only re-run mid-task if a call fails with `InvalidClientTokenId`,
`ExpiredToken`, or another authentication error.

Full procedure: `runbooks/aws-and-s3.md`.

## Databricks CLI profile

All Databricks CLI commands use the configured workspace profile in
`.databricks.env.template`. The cluster ID is `0107-154653-j5wd510m`.
Do not override or expose the token.
