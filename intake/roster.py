"""Town resolution and ballot-roster context for the parser.

The roster is the whole trick: instead of asking the model to invent races and
candidates, we hand it the exact ballot the town votes on - every race id and
candidate id - and ask it only to attach numbers to those ids. A name that is
not on the town's ballot cannot be matched to a candidate id by accident.

Roster construction is imported from entry.py rather than reimplemented, so the
automated path and the hand-entry path can never disagree about what is on a
town's ballot.
"""

import re
import threading

from entry import event_elections, place_races  # single source of truth
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
