"""The intake service: poll email, listen to Signal, parse, apply, queue.

  python -m intake.runner              # run both feeds forever
  python -m intake.runner --once       # drain the mailbox once and exit
  python -m intake.runner --replay ID  # re-parse a stored message
"""

import argparse
import json
import re
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from intake import apply as apply_mod
from intake import commands
from intake import config, parser, roster, store
from intake.sources import email_source, gmail_source, signal_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("intake")

# Reports are handled concurrently: the slow part is the model reads, and one
# town's reads must not hold up another's. SQLite is in WAL mode with a busy
# timeout, so the short writes at the end serialise themselves.
_pool = ThreadPoolExecutor(max_workers=config.WORKERS, thread_name_prefix="intake")


def _handle(msg):
    conn = store.connect()
    try:
        process(conn, msg)
    except Exception:
        log.exception("unhandled error processing a report")
    finally:
        conn.close()


def _guess_town(cursor, msg):
    """Exact match on the obvious places first; ask the model only if needed."""
    candidates = [msg.get("subject") or ""]
    body = msg.get("body") or ""
    candidates += [ln.strip(" \t:-*#") for ln in body.splitlines() if ln.strip()][:6]
    for text in candidates:
        name, conf = roster.resolve_municipality(cursor, text)
        if name:
            return name, conf, "exact"

    # A town clerk mailing from their listed address identifies their own town.
    # Ward cities share one clerk address across wards, so this returns nothing
    # for them and the ward still has to come from the report itself.
    name, conf = roster.resolve_by_sender(cursor, msg.get("sender"))
    if name:
        return name, conf, "sender"

    guess = parser.identify_town(
        roster.town_list_text(cursor), body,
        msg.get("subject", ""), msg.get("sender", ""), msg.get("attachments"),
    )
    name, _ = roster.resolve_municipality(cursor, guess.municipality)
    return name, guess.confidence, "model"


def process(conn, msg):
    """Store, parse and apply one inbound message."""
    message_id, is_new = store.record_message(
        conn, msg["source"], msg["external_id"], msg.get("sender"),
        msg.get("subject"), msg.get("body"), msg.get("attachments"),
        msg.get("received_at"),
    )
    if not is_new:
        log.info("Already seen message %s (%s)", message_id, msg["external_id"])
        return None

    cursor = conn.cursor()
    try:
        # Chatter with no town and no numbers is not a results report. Queueing
        # it would bury the real reports on a busy night.
        body_text = msg.get("body") or ""
        if not msg.get("attachments") and not re.search(r"\d", body_text):
            store.set_message_status(conn, message_id, "ignored")
            log.info("msg %s: no numbers and no image, ignoring", message_id)
            return {"applied": 0, "queued": 0, "town": None}

        town, town_conf, how = _guess_town(cursor, msg)
        if not town:
            store.set_message_status(conn, message_id, "queued")
            store.add_item(conn, message_id, status="pending",
                           municipality_text=(msg.get("subject") or msg.get("body") or "")[:120],
                           reason="Could not tell which town this is from")
            log.warning("msg %s: town unresolved", message_id)
            _notify(f"Results report with no identifiable town (from {msg.get('sender')}). "
                    f"Needs review: {_review_url()}")
            return {"applied": 0, "queued": 1, "town": None}

        roster_text, index, elections = roster.roster_for(cursor, town)
        extraction, agreed, disagreements = parser.extract_consensus(
            town, roster_text, msg.get("body"), msg.get("subject", ""),
            msg.get("sender", ""), msg.get("attachments"),
        )

        parse_json = json.dumps(extraction.model_dump(), default=str)
        if not extraction.contains_results:
            store.set_message_status(conn, message_id, "ignored", parse_json=parse_json)
            log.info("msg %s: no results in it (%s)", message_id, town)
            return {"applied": 0, "queued": 0, "town": town}

        summary = apply_mod.apply_extraction(conn, message_id, town, extraction, index,
                                             elections, agreed, disagreements)
        status = ("applied" if summary["queued"] == 0
                  else "partial" if summary["applied"] else "queued")
        store.set_message_status(conn, message_id, status, parse_json=parse_json)

        log.info("msg %s: %s via %s - %d applied, %d queued",
                 message_id, town, how, summary["applied"], summary["queued"])
        _notify(_format_parse(town, msg, summary, extraction))
        summary["town"] = town
        return summary

    except Exception as exc:
        log.exception("msg %s failed", message_id)
        store.set_message_status(conn, message_id, "error", error=str(exc))
        _notify(f"Intake error on a report from {msg.get('sender')}: {exc}. {_review_url()}")
        return None


MAX_NOTIFY_CHARS = 3500


def _format_parse(town, msg, summary, extraction):
    """Everything read out of one report, as a message to read on a phone."""
    src = msg.get("source", "?")
    sender = (msg.get("sender") or "").strip()
    head = f"{town} - {src}"
    if sender:
        head += f" from {sender}"

    counts = f"{summary['applied']} published"
    if summary["queued"]:
        counts += f" | {summary['queued']} need review"
    lines = [head, counts, ""]

    published = [d for d in summary.get("details", []) if d["ok"]]
    held = [d for d in summary.get("details", []) if not d["ok"]]

    current = None
    for d in published:
        label = f"{d['party']} {d['label']}"
        if label != current:
            lines.append(label)
            current = label
        lines.append(f"   {d['name']} {d['votes']:,}")

    if held:
        lines.append("")
        lines.append("NEEDS REVIEW")
        for d in held:
            lines.append(f"   {d['party']} {d['label']} - {d['name']} "
                         f"{d['votes']:,} ({d['reason']})")

    for b in extraction.ballots:
        if b.ballots_cast:
            lines.append("")
            lines.append(f"Ballots cast: {b.ballots_cast:,}")
            break

    if extraction.notes:
        lines.append("")
        lines.append(f"Note: {extraction.notes[:300]}")

    text = "\n".join(lines)
    if len(text) > MAX_NOTIFY_CHARS:
        text = text[:MAX_NOTIFY_CHARS] + "\n... (truncated)"
    if summary["queued"]:
        text += f"\n{_review_url()}"
    return text


def _review_url():
    return "https://elections.nhhouse.gop/entry/intake"


def _notify(text):
    if not config.NOTIFY_ENABLED:
        log.info("(notify suppressed) %s", text[:120])
        return
    try:
        signal_source.send(text)
    except Exception:
        log.exception("notify failed")


def email_loop(stop):
    while not stop.is_set():
        try:
            # Gmail API when the mailbox is already authorised; IMAP otherwise.
            if gmail_source.available():
                source = gmail_source
            elif config.IMAP_USER and config.IMAP_PASSWORD:
                source = email_source
            else:
                log.warning("Email feed idle: set INTAKE_GMAIL_TOKEN or IMAP credentials")
                stop.wait(300)
                continue
            for msg in source.fetch_new():
                _pool.submit(_handle, msg)
        except Exception:
            log.exception("email poll failed")
        stop.wait(config.EMAIL_POLL_SECONDS)


def signal_loop(stop):
    def on_message(msg):
        _pool.submit(_handle, msg)

    def on_command(text):
        def run():
            conn = store.connect()
            try:
                log.info("operator instruction: %s", text[:120])
                _notify(commands.handle(conn, text))
            except Exception:
                log.exception("could not act on an instruction")
                _notify("I could not act on that - see the review page.")
            finally:
                conn.close()
        _pool.submit(run)

    if not config.SIGNAL_ACCOUNT:
        log.warning("Signal feed idle: SIGNAL_BOT_NUMBER not set")
        return
    if not (config.SIGNAL_GROUP_ID or config.SIGNAL_GROUP_NAME):
        log.warning("Signal feed idle: no group configured")
        return
    signal_source.listen(on_message, on_command, stop)


def replay(message_id):
    """Re-parse a stored message - use after fixing the parser or a roster."""
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM intake_messages WHERE id = ?", (message_id,))
    row = cur.fetchone()
    if not row:
        print(f"No message {message_id}")
        return
    conn.execute("DELETE FROM intake_items WHERE message_id = ? AND status = 'pending'", (message_id,))
    conn.execute("DELETE FROM intake_messages WHERE id = ?", (message_id,))
    conn.commit()
    process(conn, {
        "source": row["source"], "external_id": row["external_id"], "sender": row["sender"],
        "subject": row["subject"], "body": row["body"],
        "attachments": json.loads(row["attachments"] or "[]"),
        "received_at": row["received_at"],
    })
    conn.close()


def stranded_loop(stop):
    """Re-parse messages left at 'new' or 'error'.

    A message is marked seen in the mailbox (and consumed from Signal) before it
    is parsed, so anything in flight when the process restarts is stranded at
    'new' forever and no feed will ever offer it again. Primary night restarted
    the service several times and left six real reports sitting there. Sweep
    them back up.
    """
    while not stop.is_set():
        stop.wait(config.STRANDED_SWEEP_SECONDS)
        if stop.is_set():
            return
        try:
            conn = store.connect()
            try:
                rows = conn.execute(
                    """SELECT id FROM intake_messages
                        WHERE status IN ('new','error')
                          AND created_at < datetime('now', ?)
                        ORDER BY id LIMIT 20""",
                    (f"-{config.STRANDED_AFTER_SECONDS} seconds",)).fetchall()
            finally:
                conn.close()
            for row in rows:
                log.warning("msg %s was stranded; re-parsing", row["id"])
                try:
                    replay(row["id"])
                except Exception:
                    log.exception("could not re-parse stranded message %s", row["id"])
        except Exception:
            log.exception("stranded sweep failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="drain the mailbox once and exit")
    ap.add_argument("--replay", type=int, help="re-parse a stored message id")
    args = ap.parse_args()

    conn = store.connect()
    store.ensure_schema(conn)
    store.bot_user_id(conn)
    conn.close()

    if args.replay:
        replay(args.replay)
        return

    if args.once:
        source = gmail_source if gmail_source.available() else email_source
        for msg in source.fetch_new():
            conn = store.connect()
            try:
                process(conn, msg)
            finally:
                conn.close()
        return

    stop = threading.Event()
    threads = [
        threading.Thread(target=email_loop, args=(stop,), daemon=True, name="email"),
        threading.Thread(target=signal_loop, args=(stop,), daemon=True, name="signal"),
        threading.Thread(target=stranded_loop, args=(stop,), daemon=True, name="stranded"),
    ]
    for t in threads:
        t.start()
    log.info("Intake running: email every %ss, Signal group %s",
             config.EMAIL_POLL_SECONDS,
             config.SIGNAL_GROUP_NAME or config.SIGNAL_GROUP_ID or "(none)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop.set()


if __name__ == "__main__":
    main()
