"""Configuration for the results intake service.

Everything is environment-driven so the same code runs locally against a copy
of the database and on the server against the live one.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATABASE = os.environ.get("INTAKE_DB", str(BASE_DIR / "nh_elections.db"))

# The election event we are ingesting. 2026 state primary = R + D together.
EVENT_YEAR = int(os.environ.get("INTAKE_YEAR", "2026"))
EVENT_TYPE = os.environ.get("INTAKE_ELECTION_TYPE", "state_primary")

# Attachments (photos of tally tapes) are written here before parsing.
ATTACH_DIR = Path(os.environ.get("INTAKE_ATTACH_DIR", str(BASE_DIR / "data" / "intake_attachments")))

# --- Anthropic ------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = os.environ.get("INTAKE_MODEL", "claude-opus-5")
# A town's full return of votes is a lot of structured output, and adaptive
# thinking is billed against the same ceiling.
# Kept under the SDK's non-streaming ceiling; the oversize retry streams.
MAX_OUTPUT_TOKENS = int(os.environ.get("INTAKE_MAX_TOKENS", "16000"))
MAX_OUTPUT_TOKENS_RETRY = int(os.environ.get("INTAKE_MAX_TOKENS_RETRY", "48000"))

# --- Email (IMAP) ---------------------------------------------------------
# Mail to results@electhouserepublicans.com is a Google Group; the mailbox
# below is a member of that group, so every clerk report lands in it.
# Gmail API (preferred): a self-contained OAuth token file, so no app
# password has to be minted. IMAP below is only used if this is unset.
GMAIL_TOKEN_PATH = os.environ.get("INTAKE_GMAIL_TOKEN", "")

IMAP_HOST = os.environ.get("INTAKE_IMAP_HOST", "imap.gmail.com")
IMAP_USER = os.environ.get("INTAKE_IMAP_USER", "")
IMAP_PASSWORD = os.environ.get("INTAKE_IMAP_PASSWORD", "")   # Google app password
IMAP_FOLDER = os.environ.get("INTAKE_IMAP_FOLDER", "INBOX")
# Only messages addressed to this address are read. Everything else in the
# mailbox is ignored outright - the service never parses unrelated mail.
GROUP_ADDRESS = os.environ.get("INTAKE_GROUP_ADDRESS", "results@electhouserepublicans.com")

# Clerks send results wherever they already have an address for us. Concord's
# clerk mailed four wards' returns straight to chris@ on primary night and every
# one of them was dropped, because the filter only accepted the group address.
# Accept any address we publish; the parser's own chatter check throws out mail
# that is not a set of results.
ACCEPT_ADDRESSES = [a.strip().lower() for a in os.environ.get(
    "INTAKE_ACCEPT_ADDRESSES",
    f"{GROUP_ADDRESS},chris@electhouserepublicans.com").split(",") if a.strip()]
EMAIL_POLL_SECONDS = int(os.environ.get("INTAKE_EMAIL_POLL", "20"))

# --- Signal ---------------------------------------------------------------
SIGNAL_HOST = os.environ.get("SIGNAL_CLI_HOST", "127.0.0.1")
SIGNAL_PORT = int(os.environ.get("SIGNAL_CLI_PORT", "7583"))
SIGNAL_ACCOUNT = os.environ.get("SIGNAL_BOT_NUMBER", "")
# Only this group is watched. Matched by internal group id, or by exact name
# if INTAKE_SIGNAL_GROUP_NAME is set instead.
SIGNAL_GROUP_ID = os.environ.get("INTAKE_SIGNAL_GROUP_ID", "")
SIGNAL_GROUP_NAME = os.environ.get("INTAKE_SIGNAL_GROUP_NAME", "")
SIGNAL_ATTACH_DIR = os.environ.get("SIGNAL_ATTACH_DIR", "/opt/signal-cli-config/attachments")

# Offline tests and back-tests run the same code path, so they must be able to
# stay off the operator's phone.
NOTIFY_ENABLED = os.environ.get("INTAKE_NOTIFY", "1") not in ("0", "false", "no")

# Where to send "something needs review" pings. A Signal number or group id.
NOTIFY_TARGET = os.environ.get("INTAKE_NOTIFY_TARGET", "")

# --- Publishing policy ----------------------------------------------------
# A parsed line auto-publishes only if it clears every check in apply.py and
# the model's own confidence is at least this high. Everything else queues.
AUTO_APPLY = os.environ.get("INTAKE_AUTO_APPLY", "1") not in ("0", "false", "no")
MIN_CONFIDENCE = float(os.environ.get("INTAKE_MIN_CONFIDENCE", "0.85"))

# Username the service writes audit rows as. Created on first run.
# How many reports to work on at once. The bound that matters is the model
# API, not the box.
WORKERS = int(os.environ.get("INTAKE_WORKERS", "16"))

# Five reads, because _verdict's supermajority rules are written for two or
# five - at three, a 2-1 line has no rule and falls through to review, which
# stranded 87 Goffstown lines. The escalation now runs as one parallel batch,
# so five reads cost two round trips rather than four.
MAX_READS = int(os.environ.get("INTAKE_MAX_READS", "5"))

# How often to look for messages stranded mid-parse, and how old one must be
# before it counts as stranded rather than simply still being worked on.
STRANDED_SWEEP_SECONDS = int(os.environ.get("INTAKE_STRANDED_SWEEP", "60"))
STRANDED_AFTER_SECONDS = int(os.environ.get("INTAKE_STRANDED_AFTER", "300"))

BOT_USERNAME = os.environ.get("INTAKE_BOT_USER", "intake-bot")

# Don't publish before this. Polls close at 7pm; anything that arrives earlier
# is stored but not applied. Empty = no gate.
#
# Read in ELECTION_TZ, never in the server's timezone: this box runs UTC, so a
# naive "19:00" would open the gate at 3pm Eastern, while people are still
# voting.
OPEN_AFTER = os.environ.get("INTAKE_OPEN_AFTER", "")
ELECTION_TZ = os.environ.get("INTAKE_TZ", "America/New_York")
