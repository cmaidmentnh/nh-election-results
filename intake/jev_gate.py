"""Jev (TypeSafe) check: is this email an election-results report at all?

The poller watches Chris's own mailboxes, not a dedicated results inbox, so
between elections almost everything it sees is ordinary candidate mail. It was
taking all of it - labelling it, clearing UNREAD and filing it in the review
queue - which hid replies like "I tried to sign in, but it would not let me"
from the one person who needed to see them.

Jev answers a fixed-choice question with a confidence, so the answer is always
one of the labels handled below. Policy stays here, not in the prompt:

- results, confident      -> taken as before (stored, queued, marked read)
- other, confident        -> not stored and left UNREAD; the label is still
                             applied as a bookmark so the poller does not ask again.
                             Only when there is NO attachment: Gilford and
                             Chichester sent returns as a bare "Attached" or a
                             reply to our own request, and Jev read both as other
- unsure, or Jev failed   -> stored for review but left UNREAD, so a real return
                             is never lost and ordinary mail stays visible

INTAKE_JEV_MODE:
- off     no call; every message is taken as before
- shadow  the answer is logged, behaviour is unchanged
- on      (default when a key is present) the rules above apply

Docs: https://docs.typesafe.ai/llms.txt
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request

log = logging.getLogger("intake.jev")

API_KEY = os.environ.get("TYPESAFE_API_KEY", "")
URL = os.environ.get("TYPESAFE_BASE_URL", "https://api.typesafe.ai") + "/v1/systemone"
MODEL = os.environ.get("JEV_MODEL", "jev-latest")
MODE = os.environ.get("INTAKE_JEV_MODE", "on" if API_KEY else "off").strip().lower()
# Confidence needed before acting on either answer.
MIN_CONFIDENCE = float(os.environ.get("INTAKE_JEV_MIN_CONFIDENCE", "0.8"))
MAX_BODY_CHARS = 20000

QUESTION = {
    "kind": {
        "type": "choice",
        "instructions": {
            "question": "Is this email a report of election results or vote counts?",
            "context": ("A New Hampshire election-results desk. Town clerks, moderators and "
                        "volunteers email vote totals, photos of tally tapes, or returns as "
                        "PDF/spreadsheet attachments. The same mailbox also receives ordinary "
                        "campaign mail from candidates and volunteers."),
        },
        "criteria": {
            "results": {
                "what": ("Reports vote counts or returns for an election, or sends a tally tape, "
                         "return of votes, or results file as an attachment"),
                "examples": ["Weare 2026 Primary Results", "Concord NH Primary Results - Revised",
                             "Ward 3 tape attached", "Smith 412, Jones 388"],
            },
            "other": {
                "what": ("Anything else: candidate or volunteer correspondence, questions, meeting "
                         "or Zoom replies, requests for signs, walkbooks, logins or help, "
                         "newsletters, pledges, forwarded headshots"),
                "not_for": "An email whose main content is vote totals or a results attachment",
            },
        },
    },
}


def enabled():
    return MODE in ("shadow", "on") and bool(API_KEY)


def _post(payload, timeout=15.0, attempts=3):
    body = json.dumps(payload).encode()
    for attempt in range(attempts):
        req = urllib.request.Request(URL, data=body, method="POST", headers={
            "Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            # 429 rate limit and 529 overloaded are retryable per the API docs.
            if e.code in (429, 529) and attempt < attempts - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def classify(sender, subject, body, attachment_names):
    """Return ("results" | "other", confidence), or None if Jev is off or failed."""
    if not enabled():
        return None
    try:
        data = _post({
            "model": MODEL,
            "state": {
                "from": sender or "",
                "subject": subject or "",
                "attachments": list(attachment_names or []),
                "body": (body or "")[:MAX_BODY_CHARS],
            },
            "questions": QUESTION,
        })
        ans = (data.get("answers") or {}).get("kind") or {}
        choice, conf = ans.get("choice"), float(ans.get("confidence") or 0)
        if choice not in ("results", "other"):
            return None
        return choice, conf
    except Exception:
        log.exception("Jev classify failed; treating the message as unsure")
        return None


def decide(verdict, has_attachments=False):
    """Map a classify() result to (take, mark_read).

    take=False: do not store it; only bookmark it with the label.
    mark_read=False: leave it UNREAD in the inbox.
    """
    if MODE != "on":
        return (True, True)  # off/shadow: behaviour unchanged
    if verdict is None:
        # A failed call must never drop a return, and must not hide mail either.
        return (True, False)
    choice, conf = verdict
    if choice == "results" and conf >= MIN_CONFIDENCE:
        return (True, True)
    if choice == "other" and conf >= MIN_CONFIDENCE:
        # An attachment may be a tape or a return whatever the text says.
        return (True, False) if has_attachments else (False, False)
    return (True, False)
