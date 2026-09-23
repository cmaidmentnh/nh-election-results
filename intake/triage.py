"""What to do with an email that is not a results report.

Chris's rule (9/23/26): if one of our apps can do it without input, do it; if it
needs his input, send him a Signal message; if we don't know, leave it unread.

Jev classifies the request into a fixed list. Each automatic action is a handler
below - add one only when an app can already do the job end to end and a wrong
guess costs nothing worse than a redundant email. Everything else is a Signal
ping or nothing.

- auto action done       -> Signal FYI of what was done; marked read if the
                            message is short (a long one may carry more)
- needs Chris            -> left unread, Signal message with the gist
- alert (failing/expiring/legal) -> left unread, Signal message (SES rate alarms excepted)
- no reply needed/unsure -> left unread, nothing sent

INTAKE_TRIAGE_MODE: on / shadow (log only) / off. Default on when Jev is on.
"""

import json
import logging
import os
import re
import urllib.request
from email.utils import parseaddr

from intake import jev_gate

log = logging.getLogger("intake.triage")

MODE = os.environ.get("INTAKE_TRIAGE_MODE", jev_gate.MODE).strip().lower()
AUTO_CONFIDENCE = float(os.environ.get("INTAKE_TRIAGE_AUTO_CONFIDENCE", "0.85"))
PING_CONFIDENCE = float(os.environ.get("INTAKE_TRIAGE_PING_CONFIDENCE", "0.6"))
# Alerts need a firmer answer: a newsletter headline like "Diesel prices could crush
# Republicans" scored 0.78 as an alert, every real one (failed renewal, domain pending
# delete, subscriber not welcomed, site indexing) scored 0.84+.
ALERT_CONFIDENCE = float(os.environ.get("INTAKE_TRIAGE_ALERT_CONFIDENCE", "0.8"))
# Beyond this many characters of new text, an auto-handled email is left unread.
SHORT_CHARS = int(os.environ.get("INTAKE_TRIAGE_SHORT_CHARS", "400"))
CANDIDATE_DB_URL = os.environ.get("CANDIDATE_DB_URL", "")
PORTAL_URL = os.environ.get("PORTAL_API_URL", "http://127.0.0.1:5008/portal/api")
# Alerts Chris has said he does not want pinged about (9/23/26: "ses complaint rate
# is fine the way it is").
QUIET_ALERTS = re.compile(r'ALARM: "SES-|SES-(Complaint|Bounce)Rate', re.I)
# Our own people and systems - never act on these.
OWN_DOMAINS = ("electhouserepublicans.com", "maidmentnh.com", "winthehouse.gop", "nhcivicrm.com")

QUESTION = {
    "action": {
        "type": "choice",
        "instructions": {
            "question": "What does the person who wrote this email need from us?",
            "who": "the person who wrote the newest message (ignore quoted earlier messages)",
            "context": ("Email to the Committee to Elect House Republicans (CTEHR), which supports "
                        "NH State House candidates: check-in form, portal logins, walkbooks, signs, "
                        "mail, websites, events and Zoom calls."),
        },
        "criteria": {
            "portal_link": {
                "what": ("Cannot sign in to the candidate portal or check-in form, or says the link "
                         "we sent does not work, expired, or never arrived"),
                "examples": ["I tried to sign in, but it would not let me",
                             "The check in link says expired", "I never got a login link"],
                "not_for": "Walkbook or canvassing app logins, Zoom links, website editor logins",
            },
            "needs_chris": {
                "what": ("Asks a question, asks for something to be done or sent, reports a problem, "
                         "or needs a decision or a personal reply"),
                "examples": ["When will our yard signs arrive?", "Can Melodye get walkbook access?",
                             "I registered for the Zoom but got no link"],
            },
            "alert": {
                "what": ("An automated or third-party warning that something is broken, failing, expiring or "
                         "at risk: failed payments or renewals, domains expiring or pending deletion, security "
                         "or sign-in alerts, sites down, delivery failures, legal filings or court deadlines"),
                "examples": ["Renewal Payment Failed", "Domain Pending Delete Notification",
                             "Motions to Dismiss filed in Gordon-Darby", "Someone subscribed but did not receive"],
                "not_for": "Routine receipts, approvals, newsletters and marketing",
            },
            "no_reply_needed": {
                "what": ("Thanks, acknowledgements, FYIs, 'can't make it', or automated notices that "
                         "ask nothing of us"),
                "examples": ["Thanks.", "Unfortunately I could not attend tonight"],
            },
        },
    },
}


def _sender_email(sender):
    return (parseaddr(sender or "")[1] or "").strip().lower()


def _is_own(addr):
    return any(addr.endswith("@" + d) or addr.endswith("." + d) for d in OWN_DOMAINS)


def _newest_text(body):
    """The new part of a reply: cut at the first quoted-history marker."""
    body = body or ""
    cut = re.split(r"\n\s*(On .{5,120}wrote:|-{2,} ?Original Message|From: .+\n\s*Sent: )", body, 1)[0]
    return re.sub(r"\s+", " ", cut).strip()


def classify(sender, subject, body):
    if not jev_gate.enabled():
        return None
    try:
        data = jev_gate._post({
            "model": jev_gate.MODEL,
            "state": {"from": sender or "", "subject": subject or "",
                      "body": (body or "")[:jev_gate.MAX_BODY_CHARS]},
            "questions": QUESTION,
        })
        ans = (data.get("answers") or {}).get("action") or {}
        choice, conf = ans.get("choice"), float(ans.get("confidence") or 0)
        if choice not in QUESTION["action"]["criteria"]:
            return None
        return choice, conf
    except Exception:
        log.exception("Jev triage failed")
        return None


def _candidate_for(addr):
    """(candidate_id, first_name) for an address on a candidate record, else None."""
    if not CANDIDATE_DB_URL or not addr:
        return None
    import psycopg2
    conn = psycopg2.connect(CANDIDATE_DB_URL)
    try:
        cur = conn.cursor()
        cur.execute("""SELECT candidate_id, first_name FROM candidates
                       WHERE LOWER(email)=%s OR LOWER(email1)=%s OR LOWER(email2)=%s
                       ORDER BY candidate_id""", (addr, addr, addr))
        rows = cur.fetchall()
        return rows[0] if len(rows) == 1 else None  # ambiguous -> not ours to guess
    finally:
        conn.close()


def _send_portal_link(addr):
    """The portal's own self-serve recovery: emails the address ON FILE a one-click link to /checkin."""
    req = urllib.request.Request(PORTAL_URL + "/forgot-password", method="POST",
                                 data=json.dumps({"identifier": addr, "dest": "checkin"}).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.status == 200


def handle(sender, subject, body, notify, where=""):
    """Triage one non-results email. Returns True if it may be marked read."""
    if MODE not in ("on", "shadow"):
        return False
    addr = _sender_email(sender)
    if not addr or _is_own(addr):
        return False
    verdict = classify(sender, subject, body)
    gist = _newest_text(body)[:300]
    log.info("Triage %s: %s - %s", MODE, verdict, (subject or "")[:80])
    if verdict is None or MODE != "on":
        return False
    choice, conf = verdict

    if choice == "portal_link" and conf >= AUTO_CONFIDENCE:
        cand = _candidate_for(addr)
        if cand:
            try:
                if _send_portal_link(addr):
                    # A long message usually carries more than the login problem -
                    # Kurt Wuelper's also had his survey answers - so the link goes out
                    # but the email stays unread for a person to read the rest.
                    short = len(_newest_text(body)) <= SHORT_CHARS
                    notify(f"Email auto-handled ({where}): sent {sender} a fresh check-in/portal link "
                           f"(they wrote: \"{gist[:160]}\")."
                           + ("" if short else " Left unread - there's more in it."))
                    return short
            except Exception:
                log.exception("portal link send failed for %s", addr)
        # Not a candidate on file, or the send failed: a person has to look.
        choice, conf = "needs_chris", max(conf, PING_CONFIDENCE)

    if choice == "alert" and conf >= ALERT_CONFIDENCE and not QUIET_ALERTS.search(subject or ""):
        notify(f"Alert - {sender}\n{subject}\n\"{gist}\"\n\n"
               f"({where}.) Reply here if you want it handled.")
        return False

    if choice in ("needs_chris", "portal_link") and conf >= PING_CONFIDENCE:
        notify(f"Email needs you - {sender}\n{subject}\n\"{gist}\"\n\n"
               f"({where}.) Reply here with what to do and I'll handle it.")
        return False

    return False  # no reply needed, or unsure: leave it unread
