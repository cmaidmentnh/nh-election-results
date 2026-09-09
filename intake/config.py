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

# Collect the mail and the Signal photos, but do not read them with the model.
# Every message is still stored, still attachment-for-attachment on disk, and
# still shows up in the review queue - it just waits for a person (or a session
# already being paid for) to read it, instead of spending an API call the
# moment it lands.  Primary night ran the parser on several hundred messages
# with several reads each; the feeds themselves cost nothing.
COLLECT_ONLY = os.environ.get("INTAKE_COLLECT_ONLY", "").strip().lower() \
    in ("1", "true", "yes", "on")
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

# One token watches one mailbox, and one mailbox is not where the results are.
# On 2026 primary night the service held a token for chris@electhouserepublicans.com
# and nothing else. Hinsdale and Albany both sent their returns to the
# results@nhgop.org list, which carries chris@maidmentnh.com and no
# electhouserepublicans.com address at all, so neither town existed as far as the
# pipeline was concerned until they were forwarded in by hand mid-count. Clerks
# mail whichever list they were already on; we do not get to pick. So the token
# path is a LIST, and every mailbox we are on gets polled.
#
# Message-ID is the dedupe key in runner.process, and it is stamped by the
# clerk's own mail server before any list fans the message out. A report that
# lands in two watched mailboxes is therefore stored exactly once - which is
# what makes it safe to add a mailbox here without first auditing it for
# overlap with the ones already listed.
GMAIL_TOKEN_PATHS = [p.strip() for p in os.environ.get(
    "INTAKE_GMAIL_TOKENS", GMAIL_TOKEN_PATH).split(",") if p.strip()]

# Which mail the Gmail poller has already taken is tracked with a label of our
# own rather than with read state. Read state belongs to whoever is triaging
# the inbox, and on primary night a human opening a clerk's mail before the
# 20-second poll reached it would drop that town for good.
GMAIL_INGESTED_LABEL = os.environ.get("INTAKE_GMAIL_LABEL", "intake-ingested")
# How far back each poll looks. The label makes re-listing cheap, so this only
# has to cover the night; a wide window costs a search, not a re-parse.
GMAIL_LOOKBACK = os.environ.get("INTAKE_GMAIL_LOOKBACK", "2d")
# Ceiling per poll. The old code took a flat 50 with no paging, so once more
# than fifty reports were waiting at once the rest were silently invisible.
GMAIL_MAX_FETCH = int(os.environ.get("INTAKE_GMAIL_MAX_FETCH", "300"))

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
#
# results@nhgop.org and chris@maidmentnh.com are on this list for the same
# reason, learned the same way. Adding a second mailbox to GMAIL_TOKEN_PATHS
# would NOT on its own have rescued Hinsdale or Albany on 2026 primary night:
# their To: lines name results@nhgop.org and no address that was in this list,
# so _addressed_to_group would have fetched the mail and then discarded it. The
# token says which mailboxes we can see into; this list says which mail in them
# is ours. Both have to know about a list before a town on it can be counted.
ACCEPT_ADDRESSES = [a.strip().lower() for a in os.environ.get(
    "INTAKE_ACCEPT_ADDRESSES",
    f"{GROUP_ADDRESS},chris@electhouserepublicans.com,"
    "results@nhgop.org,chris@maidmentnh.com").split(",") if a.strip()]
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

# A written-in name is held to a lower bar than a printed one, because the
# reader's confidence on a write-in is mostly about the SPELLING of a
# handwritten name it has no reference for: "Ellen Labrecque" is an 80% read
# however clear the "3" beside it. Carroll's return came back with 61 write-ins
# between 0.40 and 0.83, and under the race-publishes-whole rule each one of
# them held an entire race. The count itself is still guarded by having to
# agree across every read, which is the stronger of the two checks (see
# parser._verdict) - this only stops an unsure spelling from being treated as
# an unsure number.
MIN_WRITEIN_CONFIDENCE = float(os.environ.get("INTAKE_MIN_WRITEIN_CONFIDENCE", "0.4"))

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
