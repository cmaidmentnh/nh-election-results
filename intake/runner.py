"""The intake service: poll email, listen to Signal, parse, apply, queue.

  python -m intake.runner              # run both feeds forever
  python -m intake.runner --once       # drain the mailbox once and exit
  python -m intake.runner --replay ID  # re-parse a stored message
"""

import argparse
import json
import sys
import re
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from intake import apply as apply_mod
from intake import commands
from intake import config, parser, roster, store
from intake.sources import email_source, gmail_source, ses_source, signal_source

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


def _attachment_label(path):
    """The human part of a stored attachment's filename.

    Attachments are stored as '<8 hex>_<the name the sender gave it>.pdf', and
    clerks name those files after the polling place: Concord's deputy city clerk
    sends 'Concord_Ward_Three_-_Preliminary_Results.pdf'. That is a town name we
    can read without a model call, so read it.
    """
    stem = Path(path or "").name
    stem = re.sub(r"^[0-9a-f]{8}_", "", stem)
    stem = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", stem)
    return stem.replace("_", " ").replace("-", " ").strip()


_WARD_IN_LABEL = re.compile(r"(?:^|[\s_-])W(?:AR)?D?\.?\s*(\d{1,2})(?=$|[\s_-])", re.I)


def _attachment_towns(cursor, attachments, msg=None):
    """Which polling place each attachment names, by filename. None if unclear.

    Franklin's clerk named her files 'State_Primary_W1_Democratic' - a ward and
    no city - so none resolved and the three wards were filed as one message.
    A ward-only filename takes its city from the subject or sender."""
    city = None
    if msg:
        city = _ward_city_named(cursor, f"{msg.get('subject') or ''} {msg.get('sender') or ''} "
                                        f"{msg.get('body') or ''}")
    out = []
    for path in attachments or []:
        label = _attachment_label(path)
        name, _ = roster.resolve_municipality(cursor, label)
        if not name and city:
            m = _WARD_IN_LABEL.search(roster._ward_words_to_digits(label))
            if m:
                name, _ = roster.resolve_municipality(cursor, f"{city} Ward {int(m.group(1))}")
        out.append(name)
    return out


def _guess_town(cursor, msg):
    """Exact match on the obvious places first; ask the model only if needed."""
    candidates = [msg.get("subject") or ""]
    body = msg.get("body") or ""
    candidates += [ln.strip(" \t:-*#") for ln in body.splitlines() if ln.strip()][:6]
    # A clerk who attaches 'Concord_Ward_Nine_-_Preliminary_Results.pdf' has
    # named the polling place even if the covering note never does.
    candidates += [_attachment_label(p) for p in (msg.get("attachments") or [])]
    for text in candidates:
        name, conf = roster.resolve_municipality(cursor, text)
        if name:
            # 0.85 is resolve_municipality's near-miss spelling, not a name we
            # found; label it so the wrong-town hold still applies.
            return name, conf, ("exact" if conf > 0.85 else "spelling guess")

    # A town clerk mailing from their listed address identifies their own town.
    # Ward cities share one clerk address across wards, so this returns nothing
    # for them and the ward still has to come from the report itself.
    name, conf = roster.resolve_by_sender(cursor, msg.get("sender"))
    if name:
        return name, conf, "sender"

    # The model can decline to name a town at all, and returns nothing rather
    # than an empty guess. Reading through that killed whole reports.
    guess = parser.identify_town(
        roster.town_list_text(cursor), body,
        msg.get("subject", ""), msg.get("sender", ""), msg.get("attachments"),
    )
    if guess is not None and getattr(guess, "municipality", None):
        name, _ = roster.resolve_municipality(cursor, guess.municipality)
        if name:
            return name, guess.confidence, "model"

    # Nothing in the report names a town. Fall back to where this reporter has
    # been filing from all night - see resolve_by_sender_history.
    name, conf = roster.resolve_by_sender_history(
        cursor, msg.get("source"), msg.get("sender"))
    if name:
        return name, conf, "sender history"
    return None, 0.0, "none"


def _ward_city_named(cursor, text):
    """The ward city ('Manchester', 'Nashua', ...) named in text, if any."""
    cities = sorted({re.sub(r"\s+Ward\s*\d+\*?$", "", p["municipality"])
                     for p in roster.polling_places(cursor)
                     if re.search(r"\sWard\s*\d+\*?$", p["municipality"])}, key=len, reverse=True)
    for c in cities:
        if re.search(r"\b" + re.escape(c) + r"\b", text or "", re.I):
            return c
    return None


def _town_mismatch(cursor, town, how, extraction):
    """A reason to hold the whole report, or None.

    The town is chosen from the subject line before the sheet is read, so a
    volunteer who types the wrong town files a whole tape under it: on the dry
    run a tape headed TOWN OF NORTHWOOD, sent as "Nottingham results",
    published 37 lines to Nottingham (both are in Executive Council 4, so the
    district total moved too). The reader copies the place name off the sheet
    into municipality_text; when that names a different polling place, or a
    ward we only guessed, hold everything for a human.
    """
    header = (getattr(extraction, "header_place", "") or "").strip()
    said = (getattr(extraction, "municipality_text", "") or "").strip()
    # What the sheet itself says outranks everything. The reader is asked for it
    # separately because municipality_text came back on the dry run as
    # 'TOWN OF NORTHWOOD (tape header); email subject says "Nottingham results"'
    # - two towns, which resolved to neither, and the tape published to the
    # wrong one.
    on_sheet = roster.places_named(cursor, header) if header else set()
    if on_sheet and town not in on_sheet:
        return (f"The sheet is headed {header!r} ({', '.join(sorted(on_sheet))}) but was "
                f"filed under {town} (town taken from {how}) - check which town this is")
    named, _conf = roster.resolve_municipality(cursor, said) if said else (None, 0.0)
    if not on_sheet and named and named != town:
        return (f"The report names {named} but was filed under {town} "
                f"(town taken from {how}) - check which town this is")
    if not on_sheet:
        mentioned = roster.places_named(cursor, said)
        if mentioned and town not in mentioned:
            return (f"The report names {', '.join(sorted(mentioned))} but was filed under "
                    f"{town} (town taken from {how}) - check which town this is")
        named = named or (town if town in mentioned else None)
    else:
        named = town
    if how != "exact" and not on_sheet:
        # municipality_text does not count here: with no place on the sheet the
        # reader fills it with the town it inferred, so it always "agrees".
        # Nothing on the sheet or in the message names a place: the reader (or
        # this sender's history) inferred the town from the races on the tape.
        # On the primary-night replay that filed two cropped, header-less Hudson
        # tapes - one machine each - as the whole of Hudson. Right town, wrong
        # numbers; a person has to say which town and whether it is all of it.
        return (f"No place is named on the sheet or in the message; {town} was "
                f"inferred ({how}) - confirm the town, and that this is its whole count")
    if how != "exact" and re.search(r"\bWard\s*\d+", town) and named != town:
        return (f"Ward not shown on the report ({said or 'no place named'}); "
                f"{town} was a {how} guess - check which ward")
    return None


_PRIMARY = re.compile(r"\b(state\s+)?primary\b|\b(republican|democratic)\s+(ballot|party|primary)\b|"
                      r"\bsept(ember)?\.?\s+\d|\b0?9/0?8/(20)?26\b", re.I)


def _primary_return(extraction, msg):
    """A reason to hold a report that looks like a PRIMARY return, or None.

    A primary-night tape resent on general night reads cleanly against the
    general ballot - Littleton's "Cinde Warmington 407" from September is a
    perfectly good-looking line for the general Governor race - and would
    publish. Only the sheet and the covering note can tell them apart."""
    if config.EVENT_TYPE in parser.PRIMARY_EVENT_TYPES:
        return None
    for label, text in (("sheet", getattr(extraction, "election_header", "") or ""),
                        ("message", f"{msg.get('subject') or ''} {msg.get('body') or ''}")):
        m = _PRIMARY.search(text)
        if m:
            return (f"This looks like a primary-election return ({label} says "
                    f"{m.group(0)!r}) - check before publishing")
    return None


_COMBINED = re.compile(r"\b(all\s+wards|combined|town\s+total|totals?\s+for\s+all|"
                       r"all\s+(polling\s+)?(places|locations|machines))\b", re.I)
_PARTIAL = re.compile(r"\b(partial|one\s+of\s+(two|three|four|five|\d)|\d\s+of\s+\d\s+wards|"
                      r"(this|one)\s+ward\s+only|ward\s+\d+\s+only)\b", re.I)


def _tapes_left_out(extraction):
    """Hold a report whose reader entered one tape out of several it was shown.

    The reader is told never to add tapes together, so a message with five
    tabulator tapes comes back as ONE tape's figures plus a note - and those
    published as the whole town: Derry (5 tapes) and Hudson (4 tapes, 29 photos)
    on the primary-night replay, about a third of the true totals."""
    n = getattr(extraction, "tapes_not_entered", 0) or 0
    if n > 0:
        return (f"This message carries {n} more tape(s) that were not read into it - "
                f"the figures are one tape, not the town. Sum them (scripts/sum_tapes.py) "
                f"or enter by hand")
    return None


def _partial_of_combined_town(cursor, town, extraction, msg):
    """A reason to hold one ward's tape from a town that files ONE total, or None.

    Salem, Derry, Hudson and the rest vote at several ward locations but report
    a single town total, so 'Salem Ward 2' resolves to Salem. A tape from just
    one of those locations then published as if it were all of Salem (the Fisk
    Ward 3 tape on primary night). Hold it unless the sheet or note says it is
    the combined total. A machine tape is partial by nature and is handled by
    the machine-tape path, so it is left alone."""
    if re.search(r"\bWard\s*\d+", town):
        return None                                   # a ward city: the ward IS the place
    header = getattr(extraction, "header_place", "") or ""
    note = f"{msg.get('subject') or ''} {msg.get('body') or ''}"
    labels = " ".join(_attachment_label(p) for p in (msg.get("attachments") or []))
    said = roster._ward_words_to_digits(f"{header} {note} {labels}")
    text = roster._ward_words_to_digits(f"{said} {extraction.municipality_text or ''}")
    if _PARTIAL.search(said):
        return (f"The report says it is partial ({_PARTIAL.search(said).group(0)!r}) - "
                f"{town} reports ONE combined total; check before publishing")
    if _COMBINED.search(said) or apply_mod._report_scope(extraction) == "machine_tape":
        return None
    ward = re.search(r"\bW(?:AR)?D\.?\s*(\d{1,2})\b", text, re.I)
    if ward:
        return (f"Names Ward {ward.group(1)} of {town}, which reports ONE combined town "
                f"total - is this one location's tape (partial)? Check before publishing")
    return None


_CORRECTION = re.compile(r"\b(correct(ion|ed|ing)?|should (be|read|have been)|typo|"
                         r"mis-?typed|wrong number|revised|amended)\b", re.I)


def _is_correction(msg):
    return bool(_CORRECTION.search(f"{msg.get('subject') or ''} {msg.get('body') or ''}"))


def _split_by_attachment(conn, cursor, message_id, msg):
    """Handle a message whose attachments come from more than one polling place.

    Returns the combined summary, or None if this is an ordinary one-town
    message. Each attachment is re-submitted as its own message - same sender,
    same body, one tape - so it is parsed against the ballot of the ward it
    actually came from. The external_id gets a suffix per attachment so the
    children are distinct rows and can be replayed individually.
    """
    attachments = msg.get("attachments") or []
    if len(attachments) < 2:
        return None
    towns = _attachment_towns(cursor, attachments, msg)
    if len({t for t in towns if t}) < 2:
        return None

    log.info("msg %s: %d attachments from %d polling places, splitting",
             message_id, len(attachments), len({t for t in towns if t}))
    # Claim the parent before reading a single tape. A split takes as long as
    # all of its wards put together - Concord's five-ward email ("Concord Wards:
    # Three, Five, Six, Eight, Ten") ran close to a minute per tape - and until
    # the loop below finishes the parent is still sitting at 'new'. The stranded
    # sweep replays anything left at 'new' for five minutes, and a replay of the
    # parent deletes every child it has already made. That would have thrown
    # away the finished Ward 3, 5 and 6 rows halfway through Ward 8 and started
    # the whole email over, re-reading tapes whose votes were already on file -
    # so the second pass would have found them 'already recorded' and dumped the
    # lot into the review queue. A status the sweep does not collect keeps a
    # long split off its list; the real outcome is written when the loop ends.
    store.set_message_status(conn, message_id, "splitting")
    totals = {"applied": 0, "queued": 0}
    children = []
    for i, path in enumerate(attachments):
        child = dict(msg)
        child["attachments"] = [path]
        child["external_id"] = f"{msg['external_id']}#{i}"
        # The covering note names every ward at once, so it cannot be allowed to
        # decide this tape's ward. The filename can, and _guess_town reads it -
        # with the city from the parent when the filename names only a ward.
        label = _attachment_label(path)
        if towns[i] and towns[i] != (roster.resolve_municipality(cursor, label)[0]):
            label = towns[i]
        child["subject"] = label
        children.append(child)

    # In parallel: Concord's five-ward email took 813 s one tape at a time.
    # Each child has its own connection; SQLite serialises the short writes.
    def run_child(child):
        c = store.connect()
        try:
            return process(c, child) or {}
        finally:
            c.close()

    from concurrent.futures import ThreadPoolExecutor as _TPE
    with _TPE(max_workers=min(4, len(children))) as pool:
        for summary in pool.map(run_child, children):
            totals["applied"] += summary.get("applied", 0)
            totals["queued"] += summary.get("queued", 0)

    status = ("applied" if totals["applied"] and not totals["queued"]
              else "partial" if totals["applied"] else "queued")
    store.set_message_status(conn, message_id, status)
    return {**totals, "town": None, "details": []}


# Message ids being worked on right now, and ids handed to the pool for a
# re-parse. The stranded sweep skips both: a slow read is not a stranded one,
# and re-parsing it while it runs deleted its rows out from under it.
_inflight = set()
_replay_queued = set()
_inflight_lock = threading.Lock()


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
    with _inflight_lock:
        _inflight.add(message_id)
    try:
        return _process_new(conn, msg, message_id)
    finally:
        with _inflight_lock:
            _inflight.discard(message_id)


def _process_new(conn, msg, message_id):

    if config.COLLECT_ONLY:
        # Keep the feeds, drop the bill.  The message and its attachments are
        # on disk; it goes to the review queue to be read by hand.
        store.set_message_status(conn, message_id, "queued")
        log.info("msg %s: stored, not parsed (collect-only)", message_id)
        return {"applied": 0, "queued": 1, "town": None}

    cursor = conn.cursor()
    try:
        # Chatter with no town and no numbers is not a results report. Queueing
        # it would bury the real reports on a busy night.
        body_text = msg.get("body") or ""
        if not msg.get("attachments") and not re.search(r"\d", body_text):
            store.set_message_status(conn, message_id, "ignored")
            log.info("msg %s: no numbers and no image, ignoring", message_id)
            return {"applied": 0, "queued": 0, "town": None}

        # One email, several polling places. Concord's deputy city clerk sent
        # five ward tapes in a single message ("Concord Wards: Three, Five, Six,
        # Eight, Ten"); the pipeline files one town per message, so all five
        # wards were dropped as unidentifiable. Split it and parse each tape
        # against its own ward's ballot.
        split = _split_by_attachment(conn, cursor, message_id, msg)
        if split is not None:
            return split

        town, town_conf, how = _guess_town(cursor, msg)
        if not town and not msg.get("attachments"):
            # "Manchester: Ayotte 1402, ..." names a city that votes by ward
            # without saying which ward. That is a real report the resolver
            # cannot file, not chatter - it goes to a human, never the bin.
            city = _ward_city_named(cursor, f"{msg.get('subject') or ''} {body_text}")
            if city and re.search(r"\d{2,}", body_text):
                store.set_message_status(conn, message_id, "queued")
                store.add_item(conn, message_id, status="pending", municipality_text=city,
                               reason=f"{city} report with no ward - which ward?")
                log.warning("msg %s: %s with no ward; queued", message_id, city)
                _notify(f"{city} report with no ward number. Needs review: {_review_url()}")
                return {"applied": 0, "queued": 1, "town": None}
        if not town:
            # A text-only report always names its town - a reporter typing
            # numbers into Signal says where they are. When it does not, it is
            # chatter that happened to contain digits: primary night queued an
            # RSA quotation ("659:63"), a printer's phone number and a Google
            # Drive share notice as unidentifiable towns, burying the real
            # reports in the review queue. Anything with a tape attached still
            # goes to review, because that is a photo of real returns.
            if not msg.get("attachments"):
                store.set_message_status(conn, message_id, "ignored")
                log.info("msg %s: no town and no tape, ignoring as chatter", message_id)
                return {"applied": 0, "queued": 0, "town": None}
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
            settle_names=lambda reads: roster.canonicalise_writeins(reads, index),
        )

        parse_json = json.dumps(extraction.model_dump(), default=str)
        if (not extraction.contains_results and msg.get("attachments")
                and not parser.readable_content(msg.get("attachments"))):
            # A .csv, an .xls without xlrd, a 25MB PDF: the reader never saw it,
            # which is not the same as there being no results in it.
            store.set_message_status(conn, message_id, "queued", parse_json=parse_json)
            store.add_item(conn, message_id, status="pending", municipality=town,
                           reason="Attachment format could not be read - open it by hand")
            log.warning("msg %s: no attachment could be read; queued", message_id)
            return {"applied": 0, "queued": 1, "town": town}
        if not extraction.contains_results:
            store.set_message_status(conn, message_id, "ignored", parse_json=parse_json)
            log.info("msg %s: no results in it (%s)", message_id, town)
            return {"applied": 0, "queued": 0, "town": town}

        hold = (_primary_return(extraction, msg) or _town_mismatch(cursor, town, how, extraction)
                or _tapes_left_out(extraction)
                or _partial_of_combined_town(cursor, town, extraction, msg))
        if hold:
            log.warning("msg %s: %s", message_id, hold)
        unread = parser.unread_attachments(msg.get("attachments"))
        summary = apply_mod.apply_extraction(conn, message_id, town, extraction, index,
                                             elections, agreed, disagreements,
                                             hold_reason=hold,
                                             correction=_is_correction(msg))
        if unread:
            store.add_item(conn, message_id, status="pending", municipality=town,
                           municipality_text=extraction.municipality_text,
                           reason=f"{len(unread)} of {len(msg.get('attachments') or [])} "
                                  f"attachments were NOT read (over the per-report image "
                                  f"limit) - open them by hand")
            summary["queued"] += 1
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
        text = (str(exc) or type(exc).__name__)[:300]
        # A report the reader could not finish must still reach a person: an
        # 'error' row is invisible on the review page. The stranded sweep
        # retries an 'error' once; the second failure parks it as 'queued',
        # which the sweep leaves alone, instead of retrying it all night.
        retried = bool(msg.get("_retry"))
        try:
            conn.rollback()
            store.add_item(conn, message_id, status="pending",
                           municipality_text=(msg.get("subject") or msg.get("body") or "")[:120],
                           reason=f"Could not be read automatically: {text}")
        except Exception:
            log.exception("msg %s: could not queue the failure", message_id)
        store.set_message_status(conn, message_id, "queued" if retried else "error", error=text)
        _notify(f"Intake error on a report from {msg.get('sender')}: {text}. {_review_url()}")
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
        # A hand-count town writes down every name a voter scrawled, so one
        # sheet can hold thirty unreadable one-vote write-ins - Mickey Mouse,
        # Donald Duck, one person spelled four ways. Listing each of those made
        # the message look like a page of failures and buried the line that
        # actually mattered: a real candidate with 258 votes held at 80%.
        # Count the noise, name the rest.
        def _trivial(d):
            return (d.get("votes") or 0) <= 1 and "write-in" in (d["name"] or "").lower()

        real = [d for d in held if not _trivial(d)]
        trivial = len(held) - len(real)

        lines.append("")
        lines.append("NEEDS REVIEW")
        for d in sorted(real, key=lambda x: -(x.get("votes") or 0)):
            lines.append(f"   {d['party']} {d['label']} - {d['name']} "
                         f"{d['votes']:,} ({d['reason']})")
        if trivial:
            lines.append(f"   (+{trivial} unreadable one-vote write-ins, "
                         f"on the review page)")

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


def results_inbox_loop(stop):
    """results@nhhouse.gop (SES -> S3): the dedicated results inbox."""
    if not ses_source.available():
        log.warning("Results inbox idle: INTAKE_S3_BUCKET is not set")
        return
    while not stop.is_set():
        try:
            for msg in ses_source.fetch_new():
                _pool.submit(_handle, msg)
        except Exception:
            log.exception("results inbox poll failed")
        stop.wait(config.S3_POLL_SECONDS)


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
    # A multi-town message is re-submitted as one child message per attachment
    # (see _split_by_attachment). Those children are keyed off the parent's
    # external_id, so they have to go too or the replay is a no-op the second
    # time round - every child would be recognised as already seen.
    conn.execute(
        """DELETE FROM intake_items WHERE status = 'pending' AND message_id IN
               (SELECT id FROM intake_messages
                 WHERE source = ? AND external_id LIKE ? || '#%')""",
        (row["source"], row["external_id"]))
    conn.execute(
        "DELETE FROM intake_messages WHERE source = ? AND external_id LIKE ? || '#%'",
        (row["source"], row["external_id"]))
    conn.commit()
    process(conn, {
        "source": row["source"], "external_id": row["external_id"], "sender": row["sender"],
        "subject": row["subject"], "body": row["body"],
        "attachments": json.loads(row["attachments"] or "[]"),
        "received_at": row["received_at"],
        "_retry": row["status"] == "error",
    })
    conn.close()


def web_loop(stop):
    """Pick up reports sent from volunteers' personal pages (volunteers.py).

    The web app stores each one as an intake_messages row with status 'web'.
    Claim it (status -> 'claimed', so a second pass cannot take it too) and
    re-run it through process() via replay(), exactly like a stored email.
    """
    while not stop.is_set():
        stop.wait(config.WEB_POLL_SECONDS)
        if stop.is_set():
            return
        try:
            conn = store.connect()
            try:
                rows = conn.execute(
                    "SELECT id FROM intake_messages WHERE source = 'web' AND status = 'web' "
                    "ORDER BY id LIMIT 20").fetchall()
                claimed = []
                for row in rows:
                    cur = conn.execute(
                        "UPDATE intake_messages SET status = 'claimed' WHERE id = ? AND status = 'web'",
                        (row["id"],))
                    if cur.rowcount:
                        claimed.append(row["id"])
                conn.commit()
            finally:
                conn.close()
            for mid in claimed:
                log.info("volunteer web report %s claimed", mid)
                _pool.submit(_replay_safely, mid)
        except Exception:
            log.exception("web loop failed")


def _replay_safely(message_id):
    try:
        replay(message_id)
    except Exception:
        log.exception("could not re-parse message %s", message_id)
    finally:
        with _inflight_lock:
            _replay_queued.discard(message_id)


def stranded_loop(stop):
    """Re-parse messages left at 'new' or 'error'.

    A message is marked seen in the mailbox (and consumed from Signal) before it
    is parsed, so anything in flight when the process restarts is stranded at
    'new' forever and no feed will ever offer it again. Primary night restarted
    the service several times and left six real reports sitting there. Sweep
    them back up.

    A multi-ward email in mid-split sits at 'splitting' instead (see
    _split_by_attachment) so this does not restart it while it is still working.
    That status is still collected, but only long after any real split could
    still be running, because a process killed mid-split would otherwise leave
    the parent parked there and its remaining wards unread forever.
    """
    split_after = config.STRANDED_AFTER_SECONDS * 6
    while not stop.is_set():
        stop.wait(config.STRANDED_SWEEP_SECONDS)
        if stop.is_set():
            return
        try:
            conn = store.connect()
            try:
                rows = conn.execute(
                    """SELECT id FROM intake_messages
                        WHERE (status IN ('new','error')
                                   AND created_at < datetime('now', ?))
                           OR (status IN ('splitting','claimed')
                                   AND created_at < datetime('now', ?))
                        ORDER BY id LIMIT 20""",
                    (f"-{config.STRANDED_AFTER_SECONDS} seconds",
                     f"-{split_after} seconds")).fetchall()
            finally:
                conn.close()
            for row in rows:
                mid = row["id"]
                with _inflight_lock:
                    if mid in _inflight or mid in _replay_queued:
                        continue                 # still being read, or already queued
                    _replay_queued.add(mid)
                log.warning("msg %s was stranded; re-parsing", mid)
                # On the pool, not inline: one slow re-read used to stall the
                # sweep (and every other stranded message) behind it.
                _pool.submit(_replay_safely, mid)
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
        threading.Thread(target=web_loop, args=(stop,), daemon=True, name="web"),
        threading.Thread(target=results_inbox_loop, args=(stop,), daemon=True, name="inbox"),
    ]
    # The stranded sweeper exists to re-read messages the parser dropped.  With
    # the parser switched off there is nothing for it to rescue, and running it
    # would only churn the queue.
    if not config.COLLECT_ONLY:
        threads.append(threading.Thread(target=stranded_loop, args=(stop,),
                                        daemon=True, name="stranded"))
    for t in threads:
        t.start()
    log.info("Intake running%s: email every %ss, Signal group %s",
             " (collect-only, no model reads)" if config.COLLECT_ONLY else "",
             config.EMAIL_POLL_SECONDS,
             config.SIGNAL_GROUP_NAME or config.SIGNAL_GROUP_ID or "(none)")
    # The feed threads only ever log their exceptions, but a thread that dies
    # anyway (an import error mid-night, a BaseException) used to leave the
    # service "active" and deaf. Exit instead, so systemd Restart= restarts it.
    # Loops that return on purpose when their feed is not configured are
    # recorded at start and not watched.
    time.sleep(5)
    watched = [t for t in threads if t.is_alive()]
    try:
        while True:
            time.sleep(1)
            dead = [t.name for t in watched if not t.is_alive()]
            if dead:
                log.critical("intake loop thread(s) died: %s - exiting for a restart",
                             ", ".join(dead))
                stop.set()
                sys.exit(1)
    except KeyboardInterrupt:
        stop.set()


if __name__ == "__main__":
    main()
