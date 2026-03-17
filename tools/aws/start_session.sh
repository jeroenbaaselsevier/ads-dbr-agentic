#!/usr/bin/env bash
# tools/aws/start_session.sh
# ==========================
# Start an AWS SSO session via go-aws-sso.
#
# Usage:
#   ./tools/aws/start_session.sh
#
# This is an interactive script — it will open a browser for device
# authentication. Only run when the user is present.
#
# Exit codes:
#   0 — session started
#   1 — failed or cancelled

set -e

if ! command -v "$HOME/go-aws-sso" &>/dev/null; then
    echo '{"authenticated": false, "error": "go-aws-sso not found at ~/go-aws-sso"}' >&2
    exit 1
fi

echo "Starting AWS SSO session. A browser window will open for device authentication." >&2
echo "Present the URL and device code to the user and wait for approval." >&2

"$HOME/go-aws-sso"

# Verify the session is now valid
if python3 "$(dirname "$0")/check_auth.py" > /dev/null 2>&1; then
    echo '{"authenticated": true}' 
    exit 0
else
    echo '{"authenticated": false, "error": "Session start attempted but credentials still invalid"}' >&2
    exit 1
fi
