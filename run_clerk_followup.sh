#!/bin/bash
# Cron wrapper: send the Lyman + Chatham + Francestown primary follow-up on 2026-08-31 only.
# Sources the SES/AWS credentials from the recruitment app's .env (same account
# used for the original outreach), then runs the send. Idempotent (the Python
# script writes a sentinel so it won't double-send).
set -euo pipefail
[ "$(date +%Y%m%d)" = "20260831" ] || exit 0
cd /opt/nh-candidate-recruitment && set -a && . ./.env 2>/dev/null && set +a
cd /opt/nh-election-results && /usr/bin/python3 send_clerk_followup.py --send
