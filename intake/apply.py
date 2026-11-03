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
import re

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
                  extraction_scope=None, resolve_writein=None, report_cast=None,
                  correction=False, add_device=None, resent_device=None):
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

    # Pure string work, so it costs nothing to know here whether this is a
    # write-in - which decides the confidence bar it has to clear.
    name = roster.writein_name(line) if resolve_writein else None
    floor = config.MIN_WRITEIN_CONFIDENCE if name else config.MIN_CONFIDENCE
    if (line.confidence or 0) < floor:
        return False, f"Low parser confidence ({line.confidence:.0%})", race, None

    # Two independent reads must agree. Confidence alone missed real errors.
    if agreed is not None:
        key = line_key(line)
        # Consensus keys carry an occurrence number (parser._occ_keys); a line
        # is agreed only when every occurrence of its key was.
        occ = [k for k in list(agreed) + list(disagreements or {})
               if isinstance(k, tuple) and k[:-1] == key]
        if occ and all(k in agreed for k in occ):
            agreed = agreed | {key}
        if key not in agreed:
            # Reads escalate from two to five when they cannot settle, so this
            # holds however many values the reads produced - never assume a pair.
            values = list(next((v for k, v in (disagreements or {}).items()
                                if isinstance(k, tuple) and k[:-1] == key), None)
                          or (disagreements or {}).get(key) or [line.votes])
            shown = " vs ".join(f"{v:,}" if isinstance(v, int) else str(v)
                                for v in values)
            return False, f"Reads disagreed ({shown})", race, None

    # A named write-in on the race's shared aggregate id is not the aggregate
    # line. Stoddard's sheet named eight write-ins in one race and the reader
    # put all eight on that one id, so seven of them collided on the same
    # results row and the town published 1 vote against a tape showing 8.
    # An unnamed write-in line - "wi 9", "W/I", "Write-ins" - is the race's
    # aggregate. Left at candidate_id 0 it was either held as an unknown
    # candidate or, before writein_name knew "wi", rostered as a person.
    if (resolve_writein and not name and not line.candidate_id
            and getattr(line, "is_writein", False)):
        line.candidate_id = race["writein_id"]

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

    # The ballots cast printed on THIS report caps its own lines. The check
    # below only sees a figure already on file, and a town's first report
    # writes its figure after its lines are judged - so on the dry run a tape
    # printing Ballots Cast 1,820 published a 2,410 line.
    rc = (report_cast or {}).get(race["election_id"])
    if rc and line.votes > rc:
        return False, f"Exceeds the {rc:,} ballots cast printed on this report", race, None

    old = _existing_votes(cursor, line.race_id, line.candidate_id, municipality)
    if old is not None and add_device:
        # A second machine's tape for a town whose other machine is on file:
        # this line is added, exactly as its Ballot Count is (see
        # _machine_add_mode). Caps for the summed figure are checked below.
        cast = _ballots_cast(cursor, race["election_id"], municipality)
        if cast and rc and old + line.votes > cast + rc:
            return (False, f"{old:,} + {line.votes:,} exceeds the {cast + rc:,} ballots "
                           f"of both machines", race, old)
        return True, None, race, old
    if old is not None and old != line.votes:
        # Towns with several machines send several tapes, so a second, different
        # number is usually another machine rather than a contradiction - but it
        # can also be a correction or a resend. Only a human can tell, so hold it
        # and let the review page offer both Add and Replace.
        scope = getattr(extraction_scope, "value", extraction_scope) or "unknown"
        if correction:
            # The sender said this corrects a number. Offering "Add" here is
            # how a correction becomes a double count.
            return (False, f"Correction: {old:,} on file -> {line.votes:,} - Replace "
                           f"(do not add)", race, old)
        if resent_device:
            hint = (f"machine {resent_device} is already counted - a resend; Reject "
                    f"(do NOT Add)")
        elif scope == "town_total":
            hint = f"town total supersedes the {old:,} recorded - Replace"
        elif scope == "machine_tape":
            hint = f"another machine's tape - Add = {old + line.votes:,}"
        else:
            hint = f"add = {old + line.votes:,}, or a correction?"
        return False, f"{old:,} already recorded; {hint}", race, old

    cast = _ballots_cast(cursor, race["election_id"], municipality)
    if rc and cast and rc > cast:
        cast = None          # this report's own, larger figure governs (checked above);
                             # the ballots conflict itself is queued separately
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


def _multi_tape(extraction):
    """Whether this one report carries several machines' tapes.

    Then two lines for one candidate are two machines, even when they print the
    same number, and must never be collapsed into one as a duplicate. Seen
    from the device label ('T1;T2;T3;T4', 'ICP-41 and ICP-42') or from most of
    the report's candidates appearing more than once - a single tape
    photographed in overlapping pieces repeats only the few lines at a seam."""
    if _report_scope(extraction) != "machine_tape":
        return False
    label = getattr(extraction, "device_label", "") or ""
    if len([p for p in re.split(r"\s*(?:;|,|&|/|\band\b|\+)\s*", label) if p.strip()]) > 1:
        return True
    keys = {}
    for l in extraction.lines:
        if l.race_id and l.candidate_id:
            keys[(l.race_id, l.candidate_id)] = keys.get((l.race_id, l.candidate_id), 0) + 1
    return bool(keys) and sum(1 for n in keys.values() if n > 1) > len(keys) / 2


def _ballots_doubt(b, extraction, index):
    """Why a Ballots Cast figure on this report is not to be trusted, or None.

    Hudson's was read as 1 at 70% on primary night, went on file, and then
    blocked the clerk's real return line by line as 'Exceeds 1 ballots cast'."""
    if (b.confidence or 0) < config.MIN_CONFIDENCE:
        return f"Ballots cast read at low confidence ({(b.confidence or 0):.0%})"
    votes = sorted((l.votes or 0 for l in extraction.lines
                    if l.race_id in index and index[l.race_id]["election_id"] == b.election_id
                    and (l.votes or 0) <= MAX_PLAUSIBLE_VOTES), reverse=True)
    top = votes[0] if votes else 0
    # One line above the ballots figure is that line's misread, not the
    # ballots': it is held on its own, against this figure. Distrusting the
    # ballots instead switched the cap off for the whole report, and a line
    # one vote over ballots cast published. A ballots figure several lines
    # beat (Hudson's "1") is the misread one.
    if b.ballots_cast is not None and b.ballots_cast >= 5 and \
            sum(1 for v in votes if v > b.ballots_cast) == 1:
        return None
    if b.ballots_cast is None or b.ballots_cast < max(top, 5):
        return (f"Ballots cast {b.ballots_cast} is below the largest vote line "
                f"({top:,}) on the same report")
    return None


def _report_cast(extraction, index=None):
    """{election_id: ballots cast} as printed on this report, plausible figures only."""
    out = {}
    for b in getattr(extraction, "ballots", None) or []:
        if b.election_id and b.ballots_cast and not _ballots_doubt(b, extraction, index or {}):
            out[b.election_id] = max(out.get(b.election_id, 0), b.ballots_cast)
    return out


def _over_seats(cursor, extraction, index, municipality, report_cast, add_device):
    """{race_id: reason} for races whose votes exceed ballots x seats.

    A voter casts at most `seats` votes in a race, so a race whose candidate
    lines sum past ballots x seats has a misread in it somewhere, even when
    every single line is under the ballots figure."""
    sums = {}
    for l in extraction.lines:
        if l.race_id in index and l.votes:
            sums[l.race_id] = sums.get(l.race_id, 0) + l.votes
    bad = {}
    for rid, total in sums.items():
        meta = index[rid]
        seats = meta.get("seats") or 1
        rc = report_cast.get(meta["election_id"])
        if rc and total > rc * seats:
            bad[rid] = (f"Race votes {total:,} exceed {rc:,} ballots x {seats} "
                        f"seat{'s' if seats != 1 else ''} on this report")
            continue
        if not add_device and not rc:
            cast = _ballots_cast(cursor, meta["election_id"], municipality)
            if cast and total > cast * seats:
                bad[rid] = (f"Race votes {total:,} exceed {cast:,} ballots x {seats} "
                            f"seat{'s' if seats != 1 else ''} for this town")
    return bad


def _writein_remainders(extraction, index):
    """Named write-ins beside a Write-In total are a breakdown of that total.

    A machine tape prints one Write-In figure and the clerk lists the names
    under it; VxAdmin's Write-In Adjudication Report does the same. Publishing
    both counts every write-in twice - Alton's governor race on primary night
    (Write-In 27 plus 27 named), Lyndeborough on the general dry run (4 on the
    tape, 8 on the page). So the aggregate keeps only the unnamed remainder:
    A - N, and the names publish as themselves, which totals A. Named votes
    for a candidate PRINTED on the ballot are not part of the Write-In line
    and are left out of N. If the names add up to more than the total, the
    sheet is not a breakdown of it and the race is held.

    Mutates the aggregate line's votes; returns {race_id: hold reason}."""
    races = {}
    for l in extraction.lines:
        meta = index.get(l.race_id)
        if not meta:
            continue
        slot = races.setdefault(l.race_id, {"agg": [], "named": []})
        if roster.writein_name(l):
            if l.candidate_id not in meta.get("printed_ids", ()):
                slot["named"].append(l)
        elif l.candidate_id == meta["writein_id"] or (
                not l.candidate_id and getattr(l, "is_writein", False)):
            slot["agg"].append(l)
    holds = {}
    for rid, slot in races.items():
        if not slot["agg"] or not slot["named"] or len(slot["agg"]) > 1:
            continue
        agg = slot["agg"][0]
        a = agg.votes or 0
        n = sum(l.votes or 0 for l in slot["named"])
        if n <= a:
            agg.votes = a - n
        else:
            holds[rid] = f"Named write-ins ({n:,}) exceed the Write-In line ({a:,})"
    return holds


def _device_key(label):
    return re.sub(r"[^A-Z0-9]", "", (label or "").upper())


def _device_already_counted(cursor, extraction, municipality, index):
    """The device label if this machine tape's machine is already on file, else None."""
    if _report_scope(extraction) != "machine_tape":
        return None
    key = _device_key(getattr(extraction, "device_label", ""))
    if len(key) < 2:
        return None
    import json as _json
    event_ids = sorted({m["election_id"] for m in index.values()})
    if not event_ids:
        return None
    cursor.execute(
        """SELECT DISTINCT m.parse_json FROM intake_items i
             JOIN intake_messages m ON m.id = i.message_id
            WHERE i.municipality = ? AND i.kind = 'result' AND i.status = 'applied'
              AND i.election_id IN ({})""".format(",".join("?" * len(event_ids))),
        (municipality, *event_ids))
    for row in cursor.fetchall():
        try:
            other = _device_key(_json.loads(row["parse_json"] or "{}").get("device_label"))
        except (TypeError, ValueError):
            continue
        if len(other) >= 2 and (other == key or other in key or key in other):
            return extraction.device_label
    return None


def _machine_add_mode(cursor, extraction, municipality, index):
    """The device label to ADD this report under, or None.

    Many towns run two or more tabulators and send one tape each. The second
    tape's every line used to queue as "already recorded - Add = ...": 48
    review items for one Goffstown tape on the dry run, on a night with a lot
    of tapes. Ballot Counts are already added automatically for machine tapes
    (see the ballots loop below); this does the same for the votes, and only
    when nothing about it is in doubt:

      * this report is a machine tape with a device label, and
      * every report already applied for the town was a machine tape with a
        label, none the same device (or a substring of it, so 'ICP-0101' and
        'Tabulator ICP-0101 (Machine 1 of 2)' are one machine), and
      * no line of 10+ votes repeats the number on file exactly - a resend of
        the machine that is already counted looks exactly like that.

    Anything else - a town total, an unlabelled tape, a resend - falls back to
    the review queue as before. The cost of declining is a review item; the
    cost of a wrong yes is a double count, so every doubt declines.
    """
    if _report_scope(extraction) != "machine_tape":
        return None
    key = _device_key(getattr(extraction, "device_label", ""))
    if len(key) < 2:
        return None
    # Only this event's reports: every town also has primary-night items on
    # file, and counting those made this decline for every town.
    event_ids = sorted({m["election_id"] for m in index.values()})
    cursor.execute(
        """SELECT DISTINCT m.id, m.parse_json FROM intake_items i
             JOIN intake_messages m ON m.id = i.message_id
            WHERE i.municipality = ? AND i.kind = 'result' AND i.status = 'applied'
              AND i.election_id IN ({})""".format(",".join("?" * len(event_ids))),
        (municipality, *event_ids))
    prior = cursor.fetchall()
    if not prior:
        return None
    import json as _json
    for row in prior:
        try:
            pj = _json.loads(row["parse_json"] or "{}")
        except (TypeError, ValueError):
            return None
        if pj.get("report_scope") != "machine_tape":
            return None
        other = _device_key(pj.get("device_label"))
        if len(other) < 2 or other == key or other in key or key in other:
            return None
    # Hand-entered rows (no intake item) mean someone has already typed a
    # figure for the town; adding a tape to it is not ours to decide.
    for l in extraction.lines:
        if l.race_id not in index or not l.candidate_id:
            continue
        old = _existing_votes(cursor, l.race_id, l.candidate_id, municipality)
        if old is not None and old >= 10 and old == l.votes:
            return None
    cursor.execute(
        """SELECT COUNT(*) AS n FROM results r JOIN races ra ON ra.id = r.race_id
            WHERE r.municipality = ? AND ra.election_id IN ({})
              AND NOT EXISTS (SELECT 1 FROM intake_items i
                               WHERE i.municipality = r.municipality AND i.race_id = r.race_id
                                 AND i.candidate_id = r.candidate_id AND i.status = 'applied')
        """.format(",".join("?" * len({m["election_id"] for m in index.values()}))),
        (municipality, *{m["election_id"] for m in index.values()}))
    if cursor.fetchone()["n"]:
        return None
    return extraction.device_label


def apply_extraction(conn, message_id, municipality, extraction, index, elections,
                     agreed=None, disagreements=None, hold_reason=None, correction=False):
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
    # Before reconcile: the sheet's printed total counts each write-in once.
    writein_holds = _writein_remainders(extraction, index)
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

    report_cast = _report_cast(extraction, index)
    add_device = None if (hold_reason or correction) else \
        _machine_add_mode(cursor, extraction, municipality, index)
    if add_device:
        log.info("%s: adding %s to the machine(s) already on file", municipality, add_device)
    resent_device = None if add_device else \
        _device_already_counted(cursor, extraction, municipality, index)
    over_seats = _over_seats(cursor, extraction, index, municipality, report_cast, add_device)

    verdicts = {}
    for line in extraction.lines:
        ok, reason, race, old = validate_line(cursor, line, municipality, index,
                                              agreed, disagreements,
                                              getattr(extraction, "report_scope", None),
                                              resolve_writein, report_cast, correction,
                                              add_device, resent_device)
        if ok and hold_reason:
            ok, reason = False, hold_reason
        if ok and line.race_id in over_seats:
            ok, reason = False, over_seats[line.race_id]
        if ok and line.race_id in writein_holds:
            ok, reason = False, writein_holds[line.race_id]
        if ok and line.race_id in unreconciled:
            ok, reason = False, unreconciled[line.race_id]
        if ok and not gate_open:
            ok, reason = False, f"Held until polls close ({config.OPEN_AFTER})"
        if ok and not config.AUTO_APPLY:
            ok, reason = False, "Review-everything mode is on"
        verdicts[id(line)] = (ok, reason, race, old)

    # Two lines that resolved to one candidate. A voter can write in somebody
    # who is already printed on the same ballot - Carroll wrote in Kelly Ayotte
    # on a sheet that also has her printed line - and once the write-in is
    # matched to her, the sheet holds two different numbers for one person in
    # one race. Whether they add or one supersedes the other is a judgement
    # only the sheet's reader can make, so hold both and say so. Left
    # unhandled, the second one hit results' UNIQUE constraint and took the
    # whole report down with it, which is how Carroll's replay ended in error.
    claimed = {}
    for line in extraction.lines:
        ok, _reason, _race, _old = verdicts[id(line)]
        if ok:
            claimed.setdefault((line.race_id, line.candidate_id), []).append(line)

    duplicate = set()
    multi_tape = _multi_tape(extraction)
    for (_race_id, _cand_id), lines in claimed.items():
        if len(lines) < 2:
            continue
        if len({l.votes for l in lines}) == 1 and not multi_tape:
            # One line the reader listed twice. Carroll's write-in section gave
            # David Holmander's 14 both as "DAVID HOLMANDER" and as "DAVID
            # HOLMANDER (write-in)"; once the write-in is matched to the row the
            # first one made, they are the same line saying the same number.
            # Holding them would take the whole race down over an agreement.
            duplicate.update(id(l) for l in lines[1:])
            continue
        shown = " and ".join(f"{l.votes:,}" for l in lines)
        for line in lines:
            ok, reason, race, old = verdicts[id(line)]
            verdicts[id(line)] = (
                False,
                f"Two lines in this report are for this candidate ({shown})",
                race, old)

    held_race = {}
    for line in extraction.lines:
        ok, reason, _race, _old = verdicts[id(line)]
        if not ok and line.race_id and id(line) not in duplicate:
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

        if id(line) in duplicate:
            # Recorded, never counted twice - nothing is silently dropped.
            store.add_item(conn, message_id, status="superseded",
                           reason="Same line reported twice in this report", **common)
            continue

        if not ok:
            store.add_item(conn, message_id, status="pending", reason=reason, **common)
            queued += 1
            queued_reasons.append(f"{label}: {reason}")
            details.append({"ok": False, "party": party, "label": label,
                            "name": name, "votes": line.votes, "reason": reason})
            continue

        if old is not None and add_device:
            total = old + line.votes
            # Guarded on the value read in the verdict pass: if another report
            # moved this row in between, nothing is added and the line queues.
            cur2 = cursor.execute(
                "UPDATE results SET votes = ? WHERE race_id = ? AND candidate_id = ?"
                " AND municipality = ? AND votes = ?",
                (total, line.race_id, line.candidate_id, municipality, old))
            if cur2.rowcount != 1:
                store.add_item(conn, message_id, status="pending",
                               reason=f"Changed while adding {add_device}; check", **common)
                queued += 1
                continue
            log_audit(cursor, user_id, line.race_id, municipality, line.candidate_id,
                      "update", {"votes": old}, {"votes": total})
            item_id = store.add_item(conn, message_id, status="applied",
                                     reason=f"Another machine ({add_device}): {old:,} + "
                                            f"{line.votes:,} = {total:,}", **common)
            conn.execute("UPDATE intake_items SET applied_at = CURRENT_TIMESTAMP WHERE id = ?",
                         (item_id,))
            applied += 1
            details.append({"ok": True, "party": party, "label": label,
                            "name": name, "votes": total, "reason": None})
            continue

        if old is None:
            # OR IGNORE, because `old` was read in the verdict pass and another
            # report can land between then and here. A lost insert is a line
            # somebody else already filed; a raised IntegrityError here would
            # abandon every remaining line in the report, which is far worse.
            cursor.execute(
                "INSERT OR IGNORE INTO results (race_id, candidate_id, municipality, votes)"
                " VALUES (?,?,?,?)",
                (line.race_id, line.candidate_id, municipality, line.votes),
            )
            if cursor.rowcount == 1:
                log_audit(cursor, user_id, line.race_id, municipality, line.candidate_id,
                          "create", None, {"votes": line.votes})
            else:
                # Another report filed this line first. The same number is a
                # duplicate (recorded below, no second audit row); a different
                # one is a contradiction, and marking it 'applied' would drop
                # it with nobody asked.
                won = _existing_votes(cursor, line.race_id, line.candidate_id, municipality)
                if won != line.votes:
                    reason = (f"Conflicts with {won:,} filed by another report "
                              f"at the same moment")
                    store.add_item(conn, message_id, status="pending", reason=reason,
                                   **{**common, "old_votes": won})
                    queued += 1
                    queued_reasons.append(f"{label}: {reason}")
                    details.append({"ok": False, "party": party, "label": label,
                                    "name": name, "votes": line.votes, "reason": reason})
                    continue
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
    # Review-everything mode holds ballots cast too, not just the vote lines.
    ballots_hold = hold_reason or (None if config.AUTO_APPLY else "Review-everything mode is on")
    for b in extraction.ballots:
        valid_ids = {e["id"] for e in elections}
        doubt = _ballots_doubt(b, extraction, index) if b.election_id in valid_ids else None
        if doubt and gate_open and not ballots_hold:
            store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                           election_id=b.election_id, votes=b.ballots_cast,
                           confidence=b.confidence, status="pending", reason=doubt)
            queued += 1
            continue
        if (b.election_id in valid_ids and b.ballots_cast is not None and gate_open
                and not ballots_hold):
            existing = _ballots_cast(cursor, b.election_id, municipality)
            if existing is None:
                cursor.execute("SELECT county FROM polling_places WHERE municipality = ?", (municipality,))
                cr = cursor.fetchone()
                # Upsert, because "no figure on file" and "no row on file" are
                # different things: a ballots figure cleared to NULL (because a
                # later return disproved it) leaves the row in place, and a
                # plain INSERT then dies on the unique key and takes the whole
                # report down with it.
                cursor.execute(
                    """INSERT INTO voter_registration
                              (election_id, county, municipality, ballots_cast)
                       VALUES (?,?,?,?)
                       ON CONFLICT(election_id, county, municipality)
                       DO UPDATE SET ballots_cast = excluded.ballots_cast""",
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

            # Add only when this tape's votes were added too (add_device). A
            # tape arriving after a town total has every vote line held as a
            # conflict; adding its ballots anyway took Raymond from 390 to 600.
            if scope == "machine_tape" and add_device:
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
            # Shut gate: hold it for the same reason the vote lines are held,
            # not as a ballot we could not identify.
            if b.election_id in valid_ids and b.ballots_cast is not None and not gate_open:
                reason = f"Held until polls close ({config.OPEN_AFTER})"
            elif b.election_id in valid_ids and ballots_hold:
                reason = ballots_hold
            else:
                reason = "Could not tell which ballot these are for"
            store.add_item(conn, message_id, kind="ballots", municipality=municipality,
                           election_id=b.election_id, votes=b.ballots_cast,
                           confidence=b.confidence, status="pending",
                           reason=reason)
            queued += 1

    conn.commit()
    return {"applied": applied, "queued": queued, "reasons": queued_reasons,
            "details": details}
