"""Validate parsed lines and write the ones that clear every check.

A line auto-publishes only if all of these hold:

  * the town resolved to a real polling place
  * the race is on that town's ballot
  * the candidate is on that race's roster
  * the count is a plausible number, and does not exceed ballots cast
  * the model's confidence clears the threshold
  * it does not contradict a value already on file

Anything else is written to the review queue with the reason attached. Nothing
is ever silently dropped, and every write goes through result_audit under the
bot's user id, so the existing audit trail and undo path still apply.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import logging

from entry import log_audit
from intake import config, store
from intake.parser import line_key

log = logging.getLogger("intake.apply")

MAX_PLAUSIBLE_VOTES = 60000  # comfortably above the largest NH ward


def _ballots_cast(cursor, election_id, municipality):
    cursor.execute(
        "SELECT ballots_cast FROM voter_registration WHERE election_id = ? AND municipality = ?",
        (election_id, municipality),
    )
    row = cursor.fetchone()
    return row["ballots_cast"] if row and row["ballots_cast"] else None


def _existing_votes(cursor, race_id, candidate_id, municipality):
    cursor.execute(
        "SELECT votes FROM results WHERE race_id = ? AND candidate_id = ? AND municipality = ?",
        (race_id, candidate_id, municipality),
    )
    row = cursor.fetchone()
    return row["votes"] if row else None


def _gate_open():
    """Whether results may publish yet.

    Compared in the election's own timezone. The server runs UTC, so treating
    the configured time as server-local would open the gate four hours early -
    publishing results while the polls are still open.
    """
    if not config.OPEN_AFTER:
        return True
    try:
        tz = ZoneInfo(config.ELECTION_TZ)
        opens = datetime.fromisoformat(config.OPEN_AFTER)
        if opens.tzinfo is None:
            opens = opens.replace(tzinfo=tz)
        return datetime.now(tz) >= opens
    except (ValueError, KeyError):
        log.warning("Bad INTAKE_OPEN_AFTER %r; not gating", config.OPEN_AFTER)
        return True


def validate_line(cursor, line, municipality, index, agreed=None, disagreements=None,
                  extraction_scope=None):
    """Return (ok, reason, race_meta, old_votes). ok=False means queue it."""
    race = index.get(line.race_id)
    old = None

    if not line.race_id or race is None:
        return False, "Race not identified on this town's ballot", None, None
    if not line.candidate_id or line.candidate_id not in race["candidate_ids"]:
        return False, f"Candidate '{line.candidate_text}' not on this race's roster", race, None
    if line.votes is None or line.votes < 0:
        return False, "Vote count missing or negative", race, None
    if line.votes > MAX_PLAUSIBLE_VOTES:
        return False, f"Implausible count ({line.votes:,})", race, None

    old = _existing_votes(cursor, line.race_id, line.candidate_id, municipality)
    if old is not None and old != line.votes:
        # Towns with several machines send several tapes, so a second, different
        # number is usually another machine rather than a contradiction - but it
        # can also be a correction or a resend. Only a human can tell, so hold it
        # and let the review page offer both Add and Replace.
        scope = getattr(extraction_scope, "value", extraction_scope) or "unknown"
        if scope == "town_total":
            hint = f"town total supersedes the {old:,} recorded - Replace"
        elif scope == "machine_tape":
            hint = f"another machine's tape - Add = {old + line.votes:,}"
        else:
            hint = f"add = {old + line.votes:,}, or a correction?"
        return False, f"{old:,} already recorded; {hint}", race, old

    cast = _ballots_cast(cursor, race["election_id"], municipality)
    if cast and line.votes > cast:
        return False, f"Exceeds {cast:,} ballots cast for this town", race, old

    if (line.confidence or 0) < config.MIN_CONFIDENCE:
        return False, f"Low parser confidence ({line.confidence:.0%})", race, old

    # Two independent reads must agree. Confidence alone missed real errors.
    if agreed is not None:
        key = line_key(line)
        if key not in agreed:
            a, b = (disagreements or {}).get(key, (line.votes, None))
            other = f"{b:,}" if b is not None else "nothing"
            return False, f"Two reads disagreed ({a:,} vs {other})", race, old

    return True, None, race, old


def reconcile(extraction, index):
    """Races whose numbers do not add up to the total printed on their own sheet.

    Back-testing showed the model misreading a value and reporting high
    confidence on it, and reading it the same wrong way every pass - so neither
    confidence nor repetition catches it. The sheet, though, states its own
    totals: Hollis printed 5,870 votes cast for a sheriff's race read as 2,571
    plus 47 write-ins and 486 undervotes. That is arithmetic, not another
    opinion from the same model, and it fails loudly when a number is wrong.

    Returns {race_id: explanation} for races to hold.
    """
    bad = {}
    for check in getattr(extraction, "checks", None) or []:
        if not check.race_id or not check.stated_total:
            continue                      # no printed total: nothing to check
        counted = sum(l.votes or 0 for l in extraction.lines
                      if l.race_id == check.race_id)
        blanks = (check.blanks or 0) + (check.overvotes or 0)

        # NH sheets use "TOTAL VOTES CAST" both ways: some mean the sum of the
        # candidate votes, others mean every ballot including blanks. Accept
        # either, or this rejects a correct sheet by exactly its blank count -
        # which is what it did on primary night, holding scores of good races.
        # A small tolerance absorbs a scattering line the reader missed.
        tol = max(3, int(0.01 * check.stated_total))
        matches_votes_only = abs(counted - check.stated_total) <= tol
        matches_with_blanks = abs(counted + blanks - check.stated_total) <= tol
        if not (matches_votes_only or matches_with_blanks):
            accounted = counted + blanks
            meta = index.get(check.race_id) or {}
            label = meta.get("display") or meta.get("label") or check.race_id
            bad[check.race_id] = (
                f"Does not reconcile: {counted:,} votes"
                f"{f' + {blanks:,} blanks' if blanks else ''} "
                f"against {check.stated_total:,} printed on the sheet"
            )
            log.warning("%s: race %s does not reconcile - %s", label, check.race_id,
                        bad[check.race_id])
    return bad


def apply_extraction(conn, message_id, municipality, extraction, index, elections,
                     agreed=None, disagreements=None):
    """Validate every line, write the clean ones, queue the rest.

    Returns a summary dict for logging and for the operator ping.
    """
    if not municipality:
        raise ValueError("apply_extraction requires a resolved municipality")
    cursor = conn.cursor()
    user_id = store.bot_user_id(conn)
    applied, queued = 0, 0
    queued_reasons = []
    details = []
    gate_open = _gate_open()
    unreconciled = reconcile(extraction, index)

    for line in extraction.lines:
        ok, reason, race, old = validate_line(cursor, line, municipality, index,
                                              agreed, disagreements,
                                              getattr(extraction, "report_scope", None))
        if ok and line.race_id in unreconciled:
            ok, reason = False, unreconciled[line.race_id]
        if ok and not gate_open:
            ok, reason = False, f"Held until polls close ({config.OPEN_AFTER})"
        if ok and not config.AUTO_APPLY:
            ok, reason = False, "Review-everything mode is on"

        common = dict(
            kind="result",
            municipality=municipality,
            municipality_text=extraction.municipality_text,
            election_id=race["election_id"] if race else None,
            race_id=line.race_id or None,
            race_text=line.race_text or (race["label"] if race else None),
            candidate_id=line.candidate_id or None,
            candidate_text=line.candidate_text,
            votes=line.votes,
            old_votes=old,
            confidence=line.confidence,
        )

        label = (race.get("display") or race["label"]) if race else (line.race_text or "?")
        party = race["party"][:1] if race else "?"
        name = (race["names"].get(line.candidate_id) if race else None) or line.candidate_text

        if not ok:
            store.add_item(conn, message_id, status="pending", reason=reason, **common)
            queued += 1
            queued_reasons.append(f"{label}: {reason}")
            details.append({"ok": False, "party": party, "label": label,
                            "name": name, "votes": line.votes, "reason": reason})
            continue

        if old is None:
            cursor.execute(
                "INSERT INTO results (race_id, candidate_id, municipality, votes) VALUES (?,?,?,?)",
                (line.race_id, line.candidate_id, municipality, line.votes),
            )
            log_audit(cursor, user_id, line.race_id, municipality, line.candidate_id,
                      "create", None, {"votes": line.votes})
        # old == line.votes is a duplicate report; record it, change nothing.
        item_id = store.add_item(conn, message_id, status="applied", reason=None, **common)
        conn.execute(
            "UPDATE intake_items SET applied_at = CURRENT_TIMESTAMP WHERE id = ?", (item_id,)
        )
        applied += 1
        details.append({"ok": True, "party": party, "label": label,
                        "name": name, "votes": line.votes, "reason": None})

    for b in extraction.ballots:
        valid_ids = {e["id"] for e in elections}
        if b.election_id in valid_ids and b.ballots_cast is not None and gate_open:
            existing = _ballots_cast(cursor, b.election_id, municipality)
            if existing is None:
                cursor.execute("SELECT county FROM polling_places WHERE municipality = ?", (municipality,))
                cr = cursor.fetchone()
                cursor.execute(
                    """INSERT INTO voter_registration (election_id, county, municipality, ballots_cast)
                       VALUES (?,?,?,?)""",
                    (b.election_id, (cr["county"] if cr else "") or "", municipality, b.ballots_cast),
                )
                store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                               election_id=b.election_id, votes=b.ballots_cast,
                               confidence=b.confidence, status="applied")
                applied += 1
                continue
            if existing != b.ballots_cast:
                store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                               election_id=b.election_id, votes=b.ballots_cast,
                               old_votes=existing, confidence=b.confidence, status="pending",
                               reason=f"Conflicts with {existing:,} ballots already on file")
                queued += 1
        elif b.election_id:
            store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                           election_id=b.election_id, votes=b.ballots_cast,
                           confidence=b.confidence, status="pending",
                           reason="Could not tell which ballot these are for")
            queued += 1

    conn.commit()
    return {"applied": applied, "queued": queued, "reasons": queued_reasons,
            "details": details}
