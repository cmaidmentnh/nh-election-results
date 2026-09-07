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


def validate_line(cursor, line, municipality, index):
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
        return False, f"Conflicts with {old:,} already on file", race, old

    cast = _ballots_cast(cursor, race["election_id"], municipality)
    if cast and line.votes > cast:
        return False, f"Exceeds {cast:,} ballots cast for this town", race, old

    if (line.confidence or 0) < config.MIN_CONFIDENCE:
        return False, f"Low parser confidence ({line.confidence:.0%})", race, old

    return True, None, race, old


def apply_extraction(conn, message_id, municipality, extraction, index, elections):
    """Validate every line, write the clean ones, queue the rest.

    Returns a summary dict for logging and for the operator ping.
    """
    if not municipality:
        raise ValueError("apply_extraction requires a resolved municipality")
    cursor = conn.cursor()
    user_id = store.bot_user_id(conn)
    applied, queued = 0, 0
    queued_reasons = []
    gate_open = _gate_open()

    for line in extraction.lines:
        ok, reason, race, old = validate_line(cursor, line, municipality, index)
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

        if not ok:
            store.add_item(conn, message_id, status="pending", reason=reason, **common)
            queued += 1
            queued_reasons.append(f"{race['label'] if race else '?'}: {reason}")
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
    return {"applied": applied, "queued": queued, "reasons": queued_reasons}
