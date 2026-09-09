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
from intake import config, roster, store
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


def _ballots_already_counted(cursor, election_id, municipality, ballots_cast):
    """Whether this exact Ballot Count has already been added for this town+party.

    Once tapes are added together, the only thing standing between us and
    counting a machine twice is having seen its figure before: the same photo
    sent again, or a message replayed by the stranded-message sweep. An applied
    ballots item records every figure we have taken, so a repeat of one is a
    resend, not a new machine.

    Two machines can of course print the identical Ballot Count, and this treats
    the second as a resend. That is the same trade the vote path already makes
    (old == line.votes is recorded as a duplicate report and changes nothing),
    and the cost of being wrong is a town short by one machine - visible, and
    fixable by hand - rather than a silent double count.
    """
    cursor.execute(
        """SELECT 1 FROM intake_items
            WHERE kind = 'ballots' AND status = 'applied'
              AND municipality = ? AND election_id = ? AND votes = ?""",
        (municipality, election_id, ballots_cast),
    )
    return cursor.fetchone() is not None


def _report_scope(extraction):
    """'machine_tape' | 'town_total' | 'unknown' for this report, however stored."""
    scope = getattr(extraction, "report_scope", None)
    return getattr(scope, "value", scope) or "unknown"


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
                  extraction_scope=None, resolve_writein=None):
    """Return (ok, reason, race_meta, old_votes). ok=False means queue it.

    The gates that do not need a candidate id are taken first so that the
    write-in gate below is only reached by a line that is otherwise going to
    publish. A roster row belongs to the race, not to the town that reported it,
    so a write-in invented from a blurred line would follow the race into every
    other town in the district - and creating one for a line we were about to
    hold anyway is exactly how that happens.
    """
    race = index.get(line.race_id)
    old = None

    if not line.race_id or race is None:
        return False, "Race not identified on this town's ballot", None, None
    if line.votes is None or line.votes < 0:
        return False, "Vote count missing or negative", race, None
    if line.votes > MAX_PLAUSIBLE_VOTES:
        return False, f"Implausible count ({line.votes:,})", race, None
    if (line.confidence or 0) < config.MIN_CONFIDENCE:
        return False, f"Low parser confidence ({line.confidence:.0%})", race, None

    # Two independent reads must agree. Confidence alone missed real errors.
    if agreed is not None:
        key = line_key(line)
        if key not in agreed:
            # Reads escalate from two to five when they cannot settle, so this
            # holds however many values the reads produced - never assume a pair.
            values = list((disagreements or {}).get(key) or [line.votes])
            shown = " vs ".join(f"{v:,}" if isinstance(v, int) else str(v)
                                for v in values)
            return False, f"Reads disagreed ({shown})", race, None

    name = roster.writein_name(line) if resolve_writein else None
    # A named write-in on the race's shared aggregate id is not the aggregate
    # line. Stoddard's sheet named eight write-ins in one race and the reader
    # put all eight on that one id, so seven of them collided on the same
    # results row and the town published 1 vote against a tape showing 8.
    on_aggregate = name and line.candidate_id == race["writein_id"]

    if on_aggregate or not line.candidate_id or line.candidate_id not in race["candidate_ids"]:
        # A write-in names somebody the ballot does not, by definition, so the
        # roster check can never pass for one and every write-in a hand-count
        # town reports was being held. Carroll's return alone had 102 lines
        # stuck behind this. Give the name a roster row of its own instead.
        line.candidate_id = (resolve_writein(race, name) or 0) if name else 0
        if not line.candidate_id:
            return (False, f"Candidate '{line.candidate_text}' not on this race's roster",
                    race, None)

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

    # Judge every line first, then decide race by race. A race publishes whole
    # or not at all: releasing the candidates that read cleanly while holding
    # the rest of the same field puts a race on the page with one name at 100%
    # and everybody else at zero. Concord Ward 6 went out showing Chris Pappas
    # unopposed in the Democratic Senate race while Karishma Manzur, who carried
    # the ward 341-223, sat in the queue; Grafton published its three fringe
    # candidates and held the two real ones. A missing race is honest, a race
    # missing half its field is not.
    # Nothing has been written to results yet at this point, so the write-in
    # rows this creates are the only thing its commit flushes.
    def resolve_writein(race_meta, name):
        return roster.ensure_named_writein(conn, race_meta and race_meta.get("race_id"),
                                           name, race_meta)

    verdicts = {}
    for line in extraction.lines:
        ok, reason, race, old = validate_line(cursor, line, municipality, index,
                                              agreed, disagreements,
                                              getattr(extraction, "report_scope", None),
                                              resolve_writein)
        if ok and line.race_id in unreconciled:
            ok, reason = False, unreconciled[line.race_id]
        if ok and not gate_open:
            ok, reason = False, f"Held until polls close ({config.OPEN_AFTER})"
        if ok and not config.AUTO_APPLY:
            ok, reason = False, "Review-everything mode is on"
        verdicts[id(line)] = (ok, reason, race, old)

    held_race = {}
    for line in extraction.lines:
        ok, reason, _race, _old = verdicts[id(line)]
        if not ok and line.race_id:
            held_race.setdefault(line.race_id, reason)

    for line in extraction.lines:
        ok, reason, race, old = verdicts[id(line)]
        if ok and line.race_id in held_race:
            ok = False
            reason = ("Another candidate in this race is held "
                      f"({held_race[line.race_id]})")

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

    # A town's ballots cast is the SUM of its machines' Ballot Counts, exactly
    # like its votes. This loop used to treat the second tape's figure as a
    # contradiction and throw it away, which left towns holding one machine's
    # ballots against every machine's votes - and _ballots_cast() then gates
    # publishing on that number, so the town's own real votes were rejected as
    # "Exceeds N ballots cast". Goffstown sat at 254 ballots on file while the
    # three tapes it sent totalled 1,386 Democratic ballots and its Democratic
    # Governor race alone had 1,160 votes; Raymond sat at 291 against a true
    # 1,111. So add tapes here, the way sum_tapes.py already adds the vote rows.
    #
    # Which reports may be added is not a new judgement: report_scope already
    # tells the vote path apart - 'machine_tape' is part of the town and adds,
    # 'town_total' is the whole town and would supersede, 'unknown' is a guess.
    # Only 'machine_tape' adds itself automatically. A town total that disagrees
    # with the running sum, and anything unscoped, still goes to review, because
    # replacing a figure is destructive and only a human should choose it.
    scope = _report_scope(extraction)
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

            # Already agrees, or repeats a figure we have taken before: a resend
            # or a town total confirming the sum. Record it, change nothing.
            if existing == b.ballots_cast or _ballots_already_counted(
                    cursor, b.election_id, municipality, b.ballots_cast):
                store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                               election_id=b.election_id, votes=b.ballots_cast,
                               old_votes=existing, confidence=b.confidence, status="applied")
                applied += 1
                continue

            if scope == "machine_tape":
                total = existing + b.ballots_cast
                cursor.execute(
                    """UPDATE voter_registration SET ballots_cast = ?
                        WHERE election_id = ? AND municipality = ?""",
                    (total, b.election_id, municipality),
                )
                store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                               election_id=b.election_id, votes=b.ballots_cast,
                               old_votes=existing, confidence=b.confidence, status="applied",
                               reason=f"Another machine's tape: {existing:,} + "
                                      f"{b.ballots_cast:,} = {total:,}")
                applied += 1
                continue

            hint = ("town total supersedes it - Replace" if scope == "town_total"
                    else f"add = {existing + b.ballots_cast:,}, or a correction?")
            store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                           election_id=b.election_id, votes=b.ballots_cast,
                           old_votes=existing, confidence=b.confidence, status="pending",
                           reason=f"Conflicts with {existing:,} ballots already on file; {hint}")
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
