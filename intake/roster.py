"""Town resolution and ballot-roster context for the parser.

The roster is the whole trick: instead of asking the model to invent races and
candidates, we hand it the exact ballot the town votes on - every race id and
candidate id - and ask it only to attach numbers to those ids. A name that is
not on the town's ballot cannot be matched to a candidate id by accident.

Roster construction is imported from entry.py rather than reimplemented, so the
automated path and the hand-entry path can never disagree about what is on a
town's ballot.

The one thing a town can report that is not on its ballot is a write-in, and
hand-count towns report the name the voter actually wrote. Those names are
resolved and added to the roster here - see the named write-ins section at the
bottom of this file.
"""

import difflib
import itertools
import re
import threading
from collections import Counter

from entry import event_elections, normalize_name, place_races  # single source of truth
from entry_sources import normkey
from intake import config


def elections_for_event(cursor):
    rows = event_elections(cursor, config.EVENT_YEAR, config.EVENT_TYPE)
    return [dict(r) for r in rows]


def polling_places(cursor):
    """Canonical polling-place names, as results must be filed against them."""
    cursor.execute("SELECT municipality, county FROM polling_places ORDER BY municipality")
    return [dict(r) for r in cursor.fetchall()]


# Places whose state-list spelling cannot be derived from the results spelling.
# Unincorporated grants and purchases, where the two sources disagree outright.
ALIASES = {
    "ATGILACGT": "Atkinson & Gilmanton Academy Grant",
    "ATKINSONGILMANTONACADEMYGRANT": "Atkinson & Gilmanton Academy Grant",
    "LOWBURBANKSGRANT": "Low and Burbanks Grant",
    "THOMPSONMESERVESPURCHASE": "Thompson and Meserves Purchase",
    # Hale's Location is deliberately NOT aliased. It is a separate place from
    # Harts Location and has no row in polling_places, so a report from there
    # must go to review rather than be filed under a different town.
}


# Ward numbers written as words. Concord's deputy city clerk files every ward as
# "Concord Ward Two", with the tape attached as
# "Concord_Ward_Three_-_Preliminary_Results.pdf", while the state list spells the
# same place "Concord Ward 3". On primary night that cost a model call on every
# single Concord ward, and lost the one email that carried five wards at once.
# Manchester has twelve wards, so that is as far as this needs to count.
_WARD_WORDS = {
    "ONE": 1, "FIRST": 1, "TWO": 2, "SECOND": 2, "THREE": 3, "THIRD": 3,
    "FOUR": 4, "FOURTH": 4, "FIVE": 5, "FIFTH": 5, "SIX": 6, "SIXTH": 6,
    "SEVEN": 7, "SEVENTH": 7, "EIGHT": 8, "EIGHTH": 8, "NINE": 9, "NINTH": 9,
    "TEN": 10, "TENTH": 10, "ELEVEN": 11, "ELEVENTH": 11,
    "TWELVE": 12, "TWELFTH": 12,
}


def _ward_words_to_digits(text):
    """'Ward Three' -> 'Ward 3'. Left alone if the word is not a ward number."""
    return re.sub(
        r"\b(WARDS?)\s+([A-Z]+)\b",
        lambda m: (f"Ward {_WARD_WORDS[m.group(2).upper()]}"
                   if m.group(2).upper() in _WARD_WORDS else m.group(0)),
        text or "", flags=re.I,
    )


_WARD_TOKEN = r"(?:\d{1,2}|" + "|".join(sorted(_WARD_WORDS)) + r")"


def _wards_named(text):
    """How many different wards one line names.

    'Concord Wards: Three, Five, Six, Eight, Ten' is five polling places in one
    subject line, not one. Without this the ward normalisation below would read
    the first one and quietly file all five tapes under Concord Ward 3.
    """
    m = re.search(r"\bWARDS?\b[:\s]*((?:" + _WARD_TOKEN + r"|and\b|[\s,&])+)", text or "", re.I)
    if not m:
        return 0
    return len(re.findall(r"\b" + _WARD_TOKEN + r"\b", m.group(1), re.I))


def _key_variants(text):
    """Every spelling of one place name we are willing to treat as the same.

    Covers the conventions that actually differ between the state clerk list
    and the way results are reported: '&' vs 'and', ward suffixes, and ward
    numbers spelled out as words. Salem votes at four ward polling places but
    reports one town total, so 'Salem Ward 2' has to collapse to 'Salem'.
    """
    raw = (text or "").strip()
    if not raw:
        return []
    # The model is asked to copy a name from the polling-place list, which is
    # rendered as "Bedford (Hillsborough)" - so drop a trailing parenthetical
    # before matching, or the county it helpfully included defeats the lookup.
    raw = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
    forms = {raw, raw.replace("&", " and "), raw.replace(" and ", " & ")}
    # 'Ward Three' means Ward 3, and has to become one before the digit forms
    # below can normalise it.
    forms |= {_ward_words_to_digits(f) for f in list(forms)}
    # 'W3' / 'WD 3' / 'Ward 03' all mean Ward 3.
    forms |= {re.sub(r"\bW(?:AR)?D?\.?\s*0*(\d+)\b", r"WARD \1", f, flags=re.I) for f in list(forms)}

    keys = []
    for f in forms:
        k = normkey(f)
        if k and k not in keys:
            keys.append(k)
    # Base town with any ward suffix removed, tried last.
    for f in list(forms):
        stripped = re.sub(r"\s*WARD\s*\d+\*?\s*$", "", f, flags=re.I).strip()
        k = normkey(stripped)
        if k and k not in keys:
            keys.append(k)
    return keys


def resolve_municipality(cursor, text):
    """Map free text ('Bedford Ward 1', 'SALEM WARD 02', 'Low & Burbanks Grant')
    to a canonical polling place. Returns (name, confidence) or (None, 0.0).

    Only deterministic spellings are accepted here. Anything genuinely
    ambiguous is left to the model, which sees the whole report for context.
    """
    if not text:
        return None, 0.0
    # One line covering several wards is not one polling place. The report has
    # to be split before any of it can be filed.
    if _wards_named(text) > 1:
        return None, 0.0
    places = {normkey(p["municipality"]): p["municipality"] for p in polling_places(cursor)}
    variants = _key_variants(text)
    for i, key in enumerate(variants):
        if key in places:
            # An exact hit is certain; a collapsed ward suffix is near-certain.
            return places[key], 1.0 if i == 0 else 0.95
        if key in ALIASES and normkey(ALIASES[key]) in places:
            return places[normkey(ALIASES[key])], 0.95

    # Real subject lines read "Bedford - R primary", not "Bedford". Look for a
    # place name inside the text, longest first so "Manchester Ward 3" wins over
    # any shorter name it contains. Only unambiguous hits count.
    hay = " " + re.sub(r"[^A-Z0-9]+", " ", _ward_words_to_digits(text).upper()) + " "
    hits = set()
    for k, name in places.items():
        pattern = " " + re.sub(r"[^A-Z0-9]+", " ", name.upper()).strip() + " "
        if pattern in hay:
            hits.add(name)
    if hits:
        # Drop names wholly contained in a longer hit ("Salem" inside "Salem Depot").
        best = {h for h in hits if not any(h != o and h in o for o in hits)}
        if len(best) == 1:
            return best.pop(), 0.9

    # Last resort: a near-miss spelling of the whole field ("bedfortd").
    # Deliberately strict, and never applied to ward cities, where picking the
    # wrong ward would be worse than asking a human.
    import difflib
    close = difflib.get_close_matches(normkey(text), list(places), n=2, cutoff=0.87)
    if len(close) == 1 or (len(close) == 2 and close[0] != close[1]
                           and difflib.SequenceMatcher(None, normkey(text), close[0]).ratio()
                               - difflib.SequenceMatcher(None, normkey(text), close[1]).ratio() > 0.06):
        name = places[close[0]]
        if not re.search(r"\bWard\s*\d+", name, re.I):
            return name, 0.85
    return None, 0.0


def resolve_by_sender(cursor, sender):
    """A town clerk mailing from their listed address identifies their own town.

    Only used as a fallback when the report text does not name a town, and
    never for ward-split cities, where the ward still has to come from the
    report itself.
    """
    if not sender:
        return None, 0.0
    m = re.search(r"[\w.+-]+@[\w.-]+", sender)
    if not m:
        return None, 0.0
    addr = m.group(0).lower()
    cursor.execute(
        "SELECT municipality FROM polling_places WHERE LOWER(email) = ?", (addr,)
    )
    rows = [r["municipality"] for r in cursor.fetchall()]
    if len(rows) == 1:
        return rows[0], 0.9
    return None, 0.0


def resolve_by_sender_history(cursor, source, sender):
    """A reporter who has already filed a town tonight is filing from it again.

    Two Signal volunteers sent bare tape photos with no caption at all on
    primary night - the "Polls Closed Report - <Town>" header was cropped off
    the top of the picture, so neither the text nor the image named a town, and
    real returns were dropped. Both had sent a captioned tape from the same
    polling place minutes earlier.

    Deliberately the last thing tried, after the text, the sender's clerk
    address and the model have all failed. Only accepted when every report this
    sender has filed tonight came from the same one place - a reporter covering
    two towns gets a human instead.
    """
    if not sender:
        return None, 0.0
    cursor.execute(
        """SELECT DISTINCT i.municipality
             FROM intake_items i
             JOIN intake_messages m ON m.id = i.message_id
            WHERE m.sender = ? AND m.source = ? AND i.municipality IS NOT NULL""",
        (sender, source),
    )
    rows = [r["municipality"] for r in cursor.fetchall()]
    if len(rows) == 1:
        return rows[0], 0.75
    return None, 0.0


def town_list_text(cursor):
    """Every polling place, for the town-identification call."""
    return "\n".join(f"{p['municipality']} ({p['county']})" for p in polling_places(cursor))


# Roster building creates the shared write-in pseudo-candidate on first use
# with a read-then-insert that is not atomic. Reports are handled concurrently,
# so serialise this short, DB-only step rather than risk two write-in rows that
# would split a race's write-in votes across two candidate ids.
_roster_lock = threading.Lock()


def roster_for(cursor, municipality):
    """Every race the town votes on, with candidate ids, as model-readable text
    plus a lookup structure the validator uses to check the model's output."""
    elections = elections_for_event(cursor)
    election_ids = [e["id"] for e in elections]
    party_of = {e["id"]: (e["party"] or "") for e in elections}

    with _roster_lock:
        races = place_races(cursor, municipality, election_ids)

    lines = []
    index = {}
    by_party = {}
    for race in races:
        by_party.setdefault(party_of.get(race["election_id"], ""), []).append(race)

    eid_of_party = {}
    for e in elections:
        eid_of_party[e["party"] or ""] = e["id"]

    for party in sorted(by_party, key=lambda p: (p != "Republican", p)):
        lines.append(f"\n=== {party.upper() or 'GENERAL'} BALLOT "
                     f"(election_id={eid_of_party.get(party, 0)}) ===")
        for race in by_party[party]:
            seats = race["seats"] or 1
            label = race["label"] or race["office"]
            lines.append(f"[race_id={race['id']}] {race['office']} - {label} ({seats} seat{'s' if seats != 1 else ''})")
            cand_ids = set()
            for c in race["candidates"]:
                tag = " (write-in)" if c["is_writein"] else ""
                lines.append(f"    candidate_id={c['candidate_id']}  {c['name']}{tag}")
                cand_ids.add(c["candidate_id"])
            lines.append(f"    candidate_id={race['writein_id']}  Write-in (aggregate line)")
            cand_ids.add(race["writein_id"])
            # race_label() gives only the district or county, so several
            # different offices share one label - every county-wide office in a
            # county is just "Hillsborough", and State Representative and
            # Delegate to the State Convention are both "Hillsborough District 2".
            # Anything shown to a human needs the office named.
            display = label if label == race["office"] else f"{race['office']} - {label}"

            index[race["id"]] = {
                "race_id": race["id"],
                "election_id": race["election_id"],
                "party": party,
                "office": race["office"],
                "label": label,
                "display": display,
                "seats": seats,
                "candidate_ids": cand_ids,
                "writein_id": race["writein_id"],
                "names": {c["candidate_id"]: c["name"] for c in race["candidates"]},
            }

    return "\n".join(lines), index, elections


# ---------------------------------------------------------------------------
# Named write-ins
#
# A machine town reports one aggregate "WRITE-IN" or "SCATTERING" figure per
# race. A hand-count town reports the name the voter actually wrote, one line
# each, and none of those names is on the ballot the roster was built from - so
# every one of them failed the validator's roster check. Carroll's return alone
# put 102 lines into the review queue that way, and most of the towns still out
# on primary night are small hand-count towns that will report the same shape.
#
# So a named write-in creates its own roster row, flagged with
# recruitment_filing_id = -1, exactly as the hand-entry form and the operator's
# "that's a write-in" DM already do (entry.py save_place / commands._apply_action).
# ---------------------------------------------------------------------------

# "(write-in)" as the readers actually emit it, plus the forms clerks print.
# Deliberately anchored on the whole word: a bare "wi" would eat the first two
# letters of "Wilson".
_WRITEIN_MARK = re.compile(
    r"[\(\[]?\s*\b(?:write[\s._-]*ins?|w\s*/\s*i)\b\.?\s*[\)\]]?", re.I)

# Generational suffixes are written down inconsistently by the same voter pool -
# Carroll produced both "George Brodeur Sr" and "George Brodeur" - so they are
# not part of the identity being matched.
_NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}


# What a sheet calls its aggregate write-in figure. These are not people, and
# creating a candidate called SCATTERING would take the whole race's write-in
# total away from the shared line that is meant to hold it.
_AGGREGATE_LINE = re.compile(
    r"^(?:ALL\s+)?(?:OTHERS?|MISC\w*|SCATTER\w*|WRITE\s*INS?|TOTALS?|VOTES?)"
    r"(?:\s+(?:VOTES?|TOTALS?|CANDIDATES?|NAMES?))?$", re.I)


def writein_name(line):
    """The person written in on this line, or None if it is not a named write-in.

    The aggregate figure still belongs to the race's shared write-in candidate:
    "SCATTERING", a bare "WRITE-IN" and "OTHER VOTES" are the whole race's
    write-in total, not somebody's name, and returning None keeps them on that
    path. "ME" is somebody's name - Stoddard really did report it - so this
    filters by what the word is, never by how short it is.
    """
    text = getattr(line, "candidate_text", None) or ""
    if not (getattr(line, "is_writein", False) or _WRITEIN_MARK.search(text)):
        return None
    # Tested before the marker is stripped as well as after: "Write In Votes"
    # would otherwise come back as a candidate called "Votes".
    if _AGGREGATE_LINE.match(normalize_name(text)):
        return None
    name = _WRITEIN_MARK.sub(" ", text)
    name = re.sub(r"[\(\[]\s*[\)\]]", " ", name)         # emptied parentheses
    name = re.sub(r"\s+", " ", name).strip(" ,;:-–—")
    if not re.search(r"[A-Za-z]", name) or _AGGREGATE_LINE.match(normalize_name(name)):
        return None
    return name


def _name_tokens(name):
    tokens = normalize_name(name).split()
    while len(tokens) > 1 and tokens[-1] in _NAME_SUFFIXES:
        tokens = tokens[:-1]
    return tokens


def _ratio(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()


def same_person(a, b):
    """Whether two written-in names are one person, read two ways.

    Tonight's five reads of Carroll's tally sheet produced "Ellen LaBrack",
    "Ellen LaBreck", "Ellen Labreck" and "Ellen Labrecque" for one woman, and
    "David Perkay" / "Perkey" / "Perray" / "Perry" for one man. Publishing her
    four times with a quarter of her votes each is worse than not publishing her
    at all, so names are matched before any of them becomes a roster row.

    A single flat difflib cutoff cannot do it - "LABRACK" against "LABRECQUE"
    scores 0.79, below the 0.87 resolve_municipality uses - so the comparison
    uses the structure of a name instead: one half has to be right for a
    misreading of the other half to be forgiven. Callers cluster transitively,
    which is what carries "Labrecque" to "LaBrack" by way of "Labreck".
    """
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return False
    if ta == tb:
        return True
    # A single word is not a name with halves to check, and the protest votes
    # arrive as single words: "NOTA" must never be fuzzed into anybody.
    if len(ta) == 1 or len(tb) == 1:
        return False

    if _ratio(" ".join(ta), " ".join(tb)) >= 0.85:
        return True                        # "Connie Osgood Dan" / "... Don"
    given, surname = _ratio(ta[0], tb[0]), _ratio(ta[-1], tb[-1])
    if ta[0] == tb[0] and surname >= 0.72:
        return True                        # David Perkay / Perkey / Perray / Perry
    if ta[-1] == tb[-1] and given >= 0.5:
        return True                        # Crystal Bailey / Xtal Bailey
    return given >= 0.8 and surname >= 0.8


def _cluster(names):
    """Group names that are the same person. Returns a list of lists.

    Transitive on purpose: see same_person - the chain is what links the widest
    pair of spellings of one name.
    """
    parent = {n: n for n in names}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in itertools.combinations(names, 2):
        if find(a) != find(b) and same_person(a, b):
            parent[find(b)] = find(a)

    groups = {}
    for n in names:
        groups.setdefault(find(n), []).append(n)
    return list(groups.values())


def canonicalise_writeins(reads, index):
    """Settle on one spelling of each written-in name across every read.

    Has to happen before the reads are compared, not after. An unmatched line is
    identified by the text the reporter wrote (parser.line_key), so five reads
    that spelled Ellen LaBrack five ways are five different lines that each
    appear once - the consensus check then finds no agreement on any of them and
    holds the lot. Rewriting every variant to one spelling first turns them back
    into one line read five times, which is what they are.

    Where the cluster matches somebody already on the race - a real candidate
    written in on the other party's ballot, or a write-in a previous report from
    this district already created - the line is pointed at that candidate_id
    instead, so nobody is filed twice.
    """
    by_race = {}
    for read in reads:
        for line in getattr(read, "lines", None) or []:
            name = writein_name(line)
            if not name or line.race_id not in index:
                continue
            # A named write-in the reader parked on the race's shared write-in
            # id counts as unassigned. Stoddard's sheet listed eight names in
            # one race - BOB FEE, FRED PASLER, MARY AUGELL and five more - and
            # every one of them came back on the aggregate id, so seven of the
            # eight collided on the same results row and the town published 1
            # vote where the tape showed 8.
            if not line.candidate_id or line.candidate_id == index[line.race_id]["writein_id"]:
                by_race.setdefault(line.race_id, []).append((line, name))

    for race_id, entries in by_race.items():
        race = index[race_id]
        on_ballot = {name: cid for cid, name in race["names"].items()}
        counts = Counter(name for _line, name in entries)
        universe = list(dict.fromkeys(list(counts) + list(on_ballot)))

        for group in _cluster(universe):
            known = [n for n in group if n in on_ballot]
            if len(known) > 1:
                # Two people already on this ballot look alike, so a written-in
                # name near both of them cannot be assigned. Leave it for a human.
                continue
            if known:
                # Already on this race: use the roster spelling and its id.
                canonical, candidate_id = known[0], on_ballot[known[0]]
            else:
                # Otherwise the spelling the most reads agreed on wins; a tie
                # goes to the longer form, which is the one carrying a suffix
                # or an accent rather than the one that dropped it.
                canonical = max(group, key=lambda n: (counts[n], len(n), n))
                candidate_id = None
            for line, name in entries:
                if name in group:
                    line.candidate_text = f"{canonical} (write-in)"
                    # Cleared, not left alone: a name parked on the aggregate id
                    # has to reach the named-write-in path in validate_line, or
                    # it collides on the shared row with every other name in the
                    # race - which is what happened to Stoddard.
                    line.candidate_id = candidate_id or 0


def ensure_named_writein(conn, race_id, name, race_meta):
    """Find or create the candidate written in on this race, and roster them.

    Returns a candidate_id, or None if the race cannot be read.

    Serialised on the same lock roster_for uses, and committed straight away,
    for the same reason: reports are handled by a thread pool on their own
    connections, so an uncommitted insert is invisible to the thread beside it
    and two towns in one district reporting the same write-in would otherwise
    each create their own row and split the district total.
    """
    name = (name or "").strip()
    if not (race_id and name):
        return None
    with _roster_lock:
        cur = conn.cursor()
        cur.execute("""SELECT r.election_id, e.party FROM races r
                       JOIN elections e ON r.election_id = e.id WHERE r.id = ?""", (race_id,))
        race = cur.fetchone()
        if not race:
            return None

        # Re-read the roster from the database rather than trusting the index
        # this thread built: another report may have created this write-in in
        # between. Matched tolerantly, so a second town's spelling of one name
        # joins the row the first town created.
        cur.execute("""SELECT rc.candidate_id, c.name
                         FROM race_candidates rc
                         JOIN candidates c ON c.id = rc.candidate_id
                        WHERE rc.race_id = ?""", (race_id,))
        for row in cur.fetchall():
            if same_person(name, row["name"]):
                candidate_id = row["candidate_id"]
                break
        else:
            norm = normalize_name(name)
            cur.execute("SELECT id FROM candidates WHERE name_normalized = ? AND party IS ?",
                        (norm, race["party"]))
            existing = cur.fetchone()
            if existing:
                candidate_id = existing["id"]
            else:
                # Party is the ballot the vote was cast on, not the person's own:
                # "Kelly Ayotte (write-in)" on a Democratic sheet is a Democratic
                # primary vote and has to be counted as one. Same convention as
                # entry.py's hand-entry form, so the two paths agree.
                cur.execute("INSERT INTO candidates (name, name_normalized, party) VALUES (?,?,?)",
                            (name, norm, race["party"]))
                candidate_id = cur.lastrowid
            cur.execute("""INSERT OR IGNORE INTO race_candidates
                           (race_id, candidate_id, party, ballot_order, is_incumbent,
                            recruitment_candidate_id, recruitment_filing_id)
                           VALUES (?, ?, ?, 900, 0, NULL, -1)""",
                        (race_id, candidate_id, race["party"]))
        conn.commit()

    if race_meta is not None:
        race_meta["candidate_ids"].add(candidate_id)
        race_meta["names"].setdefault(candidate_id, name)
    return candidate_id
