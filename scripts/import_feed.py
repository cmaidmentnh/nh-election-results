#!/usr/bin/env python3
"""Import town-level primary returns from the AP election-results feed.

WHY THIS EXISTS
---------------
On the night of the 2026 state primary (8 September 2026) this site had results
from 38 of the 320 polling places, because every number we had came from a town
clerk who happened to email or Signal us their tape. Decision Desk, the AP and
every newsroom in the state had most of the state on the board while we were
still refreshing an inbox. The bottleneck was never parsing - `intake/` already
reads a tape well - it was that we only ever saw the towns that chose to write
to us.

The Secretary of State does NOT help here. sos.nh.gov/2026-republican-state-primary
lists every race by name but says "Election results will be linked on this page
upon completion" - the county PDFs/spreadsheets appear days later, after the
canvass. There is no state election-night reporting feed in New Hampshire.

What does exist is the AP's live feed, which NH newsrooms embed (NHPR's results
page is an apelections.org iframe). AP collects NH returns by TOWN, not county,
because that is how New Hampshire reports, and publishes a per-race
`detail.json` keyed by reporting unit. That is exactly the shape we need:

    https://interactives.apelections.org/election-results/data-live/
        <electionDate>/results/races/NH/<raceID>/detail.json

It covers Governor, U.S. Senate, U.S. House, Executive Council, State Senate
AND State House primaries - 431 NH races on primary night - and updates roughly
every minute.

RULES THIS SCRIPT FOLLOWS
-------------------------
1. A clerk's own tape always wins. Our hand-entered numbers are read off the
   machine tape by a human; the wire number is a stringer phoning in a total.
   If a row already exists and did not come from this script, we leave it alone
   and print the disagreement instead. Feed rows this script wrote itself may be
   refreshed, because AP revises upward as a town finishes counting.
2. Candidates are matched by normalised name against the roster we already hold
   for that race. We never create a candidate row. An AP name that does not
   match anything on our ballot is reported, not guessed at.
3. Only fully-reported towns are imported. AP publishes a row for every town
   from the moment polls close, with zeros in it. Writing those zeros would put
   fake 0-vote returns on the board for towns that simply have not counted yet,
   so a unit is skipped unless precinctsReporting >= precinctsTotal.
4. Idempotent. Safe to run every few minutes as the feed fills in.
5. --dry-run is the default. --apply is explicit.

Usage:
    /opt/nh-results-intake-venv/bin/python scripts/import_feed.py            # dry run
    /opt/nh-results-intake-venv/bin/python scripts/import_feed.py --apply
    /opt/nh-results-intake-venv/bin/python scripts/import_feed.py --rediscover
"""

import argparse
import concurrent.futures
import gzip
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from entry import ensure_writein_candidate, log_audit, normalize_name  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(REPO, "nh_elections.db")

# The 2026 state primary. election_id 29 = Republican, 30 = Democratic; every
# query in this file is filtered on those two, because the same tables hold
# every past general as well.
ELECTION_DATE = "2026-09-08"
ELECTION_ID = {"GOP": 29, "Dem": 30}

AP_BASE = ("https://interactives.apelections.org/election-results/data-live/"
           f"{ELECTION_DATE}/results/races/NH")
# AP's CDN 403s a bare client. It wants a browser UA and a referer from the
# microsite that serves the public embeds.
AP_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"),
    "Referer": ("https://interactives.apelections.org/election-results/customers/"
                "layouts/organization-layouts/published/128298/32161.html"),
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
}

# AP has no public index of race ids, so they are found by probing the id space
# once and cached here. NH's 2026 primary ids all fall inside 30000-32200.
RACE_ID_CACHE = os.path.join(REPO, "data", "ap_nh_race_ids.json")
SCAN_RANGE = (30000, 32400)

# AP office name -> our offices.name. AP's "State House" is our "State
# Representative"; note we ALSO carry "Delegate to the State Convention" races
# under the identical county+district label, so the office name has to be
# matched exactly or every delegate race would be filled with State Rep votes.
OFFICE_MAP = {
    "U.S. Senate": "United States Senator",
    "Governor": "Governor",
    "U.S. House": "Representative in Congress",
    "Executive Council": "Executive Councilor",
    "State Senate": "State Senator",
    "State House": "State Representative",
}

# Audit action written for every row this script touches. It is also how a
# later run recognises its own work and is allowed to refresh it - anything
# without this marker is treated as a human's entry and never overwritten.
FEED_ACTION = "import_ap_feed"


# ---------------------------------------------------------------------------
# Feed access
# ---------------------------------------------------------------------------

def fetch(url):
    req = urllib.request.Request(url, headers=AP_HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        return json.loads(body)


def race_metadata(race_id):
    return fetch(f"{AP_BASE}/{race_id}/metadata.json")


def race_detail(race_id):
    return fetch(f"{AP_BASE}/{race_id}/detail.json")


def discover_race_ids(rediscover=False):
    """Every AP race id for NH on this date.

    AP publishes no index, so the id space is probed once and the answer cached
    in the repo. On primary night the scan takes about two minutes; the cache
    makes every subsequent refresh instant, which matters when this runs on a
    loop as returns come in.
    """
    if not rediscover and os.path.exists(RACE_ID_CACHE):
        with open(RACE_ID_CACHE) as fh:
            return json.load(fh)

    found = {}

    def probe(n):
        rid = f"{ELECTION_DATE.replace('-', '')}NH{n:05d}"
        try:
            meta = race_metadata(rid)
        except Exception:
            return None
        return rid, {
            "office": meta.get("officeName"),
            "party": meta.get("party"),
            "seat": meta.get("seatName"),
        }

    with concurrent.futures.ThreadPoolExecutor(40) as pool:
        for hit in pool.map(probe, range(*SCAN_RANGE)):
            if hit:
                found[hit[0]] = hit[1]

    os.makedirs(os.path.dirname(RACE_ID_CACHE), exist_ok=True)
    with open(RACE_ID_CACHE, "w") as fh:
        json.dump(found, fh, indent=1, sort_keys=True)
    return found


# ---------------------------------------------------------------------------
# Race mapping
# ---------------------------------------------------------------------------

def parse_seat(office, seat):
    """AP seatName -> (county, district) as our races table stores them."""
    seat = (seat or "").strip()
    if office == "Governor" or office == "U.S. Senate":
        return "", ""                       # statewide; no county, no district
    m = re.match(r"^District\s+(\S+)$", seat)
    if m:                                    # U.S. House / Exec Council / State Senate
        return "", m.group(1)
    m = re.match(r"^(.+?)\s+District\s+(\S+)$", seat)
    if m:                                    # State House: "Hillsborough District 12"
        return m.group(1), m.group(2)
    return None, None


def build_race_index(cursor):
    """(election_id, office, county, district) -> race_id, for both ballots."""
    cursor.execute("""
        SELECT r.id, r.election_id, o.name AS office,
               COALESCE(r.county, '') AS county, COALESCE(r.district, '') AS district
          FROM races r JOIN offices o ON o.id = r.office_id
         WHERE r.election_id IN (29, 30)
    """)
    return {(r["election_id"], r["office"], r["county"], r["district"]): r["id"]
            for r in cursor.fetchall()}


# ---------------------------------------------------------------------------
# Candidate mapping
# ---------------------------------------------------------------------------

def race_roster(cursor, race_id, writein_id):
    """Our ballot for one race: normalised name -> candidate_id."""
    cursor.execute("""
        SELECT rc.candidate_id, c.name
          FROM race_candidates rc JOIN candidates c ON c.id = rc.candidate_id
         WHERE rc.race_id = ? AND rc.candidate_id != ?
    """, (race_id, writein_id))
    return [(r["candidate_id"], r["name"]) for r in cursor.fetchall()]


def ap_name(cand):
    parts = [cand.get("first"), cand.get("middle"), cand.get("last")]
    return " ".join(p for p in parts if p)


def match_candidate(cand, roster):
    """AP candidate -> our candidate_id, or None.

    Deliberately conservative: full normalised name, then a unique last-name
    hit on the same ballot. A guess here would attach one candidate's votes to
    another, which is worse than reporting the town as unmatched.
    """
    full = normalize_name(ap_name(cand))
    for cid, name in roster:
        if normalize_name(name) == full:
            return cid
    last = normalize_name(cand.get("last") or "")
    first = normalize_name(cand.get("first") or "")
    if not last:
        return None
    hits = [cid for cid, name in roster
            if normalize_name(name).split() and normalize_name(name).split()[-1] == last]
    if len(hits) == 1:
        return hits[0]
    # Two people on one ballot share a surname (it happens in State Rep races).
    # Break the tie only on an exact first name, never on an initial.
    if len(hits) > 1 and first:
        exact = [cid for cid, name in roster
                 if normalize_name(name).split()[-1] == last
                 and normalize_name(name).split()[0] == first]
        if len(exact) == 1:
            return exact[0]
    return None


# ---------------------------------------------------------------------------
# Municipality mapping
# ---------------------------------------------------------------------------

def build_place_index(cursor):
    """Canonical polling places, split into ward cities and everything else.

    AP writes wards without a separator ("Manchester1", "Concord W1",
    "Dover3") and ALSO publishes a whole-city rollup row under the bare city
    name. Which of the two we want depends entirely on how the city files with
    us: Manchester files twelve ward tapes, Berlin files one town total. So a
    city that has ward rows in polling_places takes only AP's ward units, and a
    city that does not takes only AP's rollup - never both, or the city would be
    counted twice.
    """
    cursor.execute("SELECT municipality FROM polling_places")
    places = [r["municipality"] for r in cursor.fetchall()]
    wards = defaultdict(dict)
    plain = {}
    for p in places:
        m = re.match(r"^(.+) Ward (\d+)$", p)
        if m:
            wards[normalize_name(m.group(1))][int(m.group(2))] = p
        else:
            plain[normalize_name(p)] = p
    return plain, wards


# Why a reporting unit was skipped. Only SKIP_UNKNOWN is a coverage gap; the
# other two are this script deliberately refusing a row that would double-count
# a city we already have.
SKIP_ROLLUP = "citywide rollup, we file this city by ward"
SKIP_WARD = "ward row, we file this city as one town"
SKIP_UNKNOWN = "no polling place of this name"


def resolve_unit(name, plain, wards):
    """AP reportingunitName -> (our polling place name, skip reason).

    Exactly one of the pair is None.

    AP publishes every ward city TWICE: once as a row per ward ("Manchester1",
    "Concord W1", "Dover3") and once as a whole-city rollup under the bare city
    name ("Manchester", "Concord", "Dover"). The rollup is arithmetically
    identical to the sum of that city's reported ward rows - checked across all
    431 NH races on 2026-09-08, where every one of the thirteen rollups matched
    its own wards to the vote. So the rollup carries nothing the ward rows have
    not already given us, and taking both would count Manchester's ~58k votes
    twice in every statewide total.

    Which of the two we want depends entirely on how the city files with us:
    Manchester files twelve ward tapes, Berlin files one town total. A city
    with ward rows in polling_places takes only AP's ward units; a city without
    them takes only AP's rollup. Never both.

    The bare city name must therefore never reach results.municipality for a
    ward city. polling_places is keyed on the ward names, so a bare
    'Manchester' row would join to no county in app.py's _precinct_counties()
    and would count as ONE of the 320 reporting precincts on the front page
    while actually standing for twelve.
    """
    if not name:
        return None, SKIP_UNKNOWN
    m = re.match(r"^(.*?)\s*W?(\d+)$", name)
    if m:
        city = normalize_name(m.group(1))
        ward = int(m.group(2))
        # A ward row is only usable if we file that city by ward.
        if city in wards and ward in wards[city]:
            return wards[city][ward], None
        if city in wards:
            return None, SKIP_UNKNOWN   # ward city, but not a ward we hold
        if city in plain:
            # AP splits a town we file whole - it lists Berlin1 and Berlin3
            # alongside the 'Berlin' rollup we actually want.
            return None, SKIP_WARD
        return None, SKIP_UNKNOWN
    key = normalize_name(name)          # strips the apostrophe in "Hart's Location"
    if key in plain:
        return plain[key], None
    if key in wards:
        return None, SKIP_ROLLUP        # city rollup for a city we file by ward
    return None, SKIP_UNKNOWN


def double_counted_cities(cursor):
    """Cities on the board as BOTH a bare name and wards - i.e. counted twice.

    This is the one error mode that would silently inflate every statewide
    total, and it is invisible in a per-town view, so it is checked rather than
    trusted. Returns the offending city names; empty means clean.
    """
    cursor.execute("""
        SELECT DISTINCT r.municipality
          FROM results r JOIN races ra ON ra.id = r.race_id
         WHERE ra.election_id IN (29, 30)
    """)
    munis = {r["municipality"] for r in cursor.fetchall()}
    ward_cities = {m.rsplit(" Ward ", 1)[0] for m in munis if re.search(r" Ward \d+$", m)}
    return sorted(ward_cities & munis)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

def human_entered(cursor, race_id, municipality, candidate_id):
    """True if anything other than this script last touched the row.

    A row with no audit trail at all counts as human: the safe reading of an
    unexplained number is that a person put it there.
    """
    cursor.execute("""
        SELECT action FROM result_audit
         WHERE race_id = ? AND municipality = ? AND candidate_id = ?
    """, (race_id, municipality, candidate_id))
    actions = [r["action"] for r in cursor.fetchall()]
    if not actions:
        return True
    return any(a != FEED_ACTION for a in actions)


def main():
    ap_args = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    ap_args.add_argument("--apply", action="store_true",
                         help="write to the database (default is a dry run)")
    ap_args.add_argument("--rediscover", action="store_true",
                         help="re-probe AP for the race id list instead of using the cache")
    ap_args.add_argument("--db", default=DB_PATH)
    ap_args.add_argument("--limit", type=int, help="only process the first N AP races (testing)")
    args = ap_args.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    writein_id = ensure_writein_candidate(cursor)
    race_index = build_race_index(cursor)
    plain, wards = build_place_index(cursor)

    from intake.store import bot_user_id
    user_id = bot_user_id(conn)

    race_ids = discover_race_ids(args.rediscover)
    ids = sorted(race_ids)
    if args.limit:
        ids = ids[:args.limit]
    print(f"AP races discovered: {len(race_ids)}  (processing {len(ids)})")

    inserted = updated = 0
    towns_touched, races_touched = set(), set()
    conflicts, unmatched_cand, unmatched_race = [], [], set()
    # AP unit name -> why we skipped it. Kept per reason so a deliberate refusal
    # (a duplicate city rollup) is never mistaken for a hole in our coverage.
    skipped_units = defaultdict(set)
    skipped_incomplete = 0

    for rid in ids:
        try:
            meta = race_metadata(rid)
            detail = race_detail(rid)
        except Exception as exc:                      # a single race failing must not
            print(f"  ! {rid}: fetch failed: {exc}")  # abandon the other 430
            continue

        office = OFFICE_MAP.get(meta.get("officeName"))
        eid = ELECTION_ID.get(meta.get("party"))
        county, district = parse_seat(meta.get("officeName"), meta.get("seatName"))
        if not office or not eid or county is None:
            unmatched_race.add(f"{rid} {meta.get('officeName')} {meta.get('seatName')} {meta.get('party')}")
            continue
        race_id = race_index.get((eid, office, county, district))
        if not race_id:
            unmatched_race.add(f"{rid} {meta.get('officeName')} {meta.get('seatName')} {meta.get('party')}")
            continue

        roster = race_roster(cursor, race_id, writein_id)
        cand_map = {}
        for ap_id, cand in (meta.get("candidates") or {}).items():
            name = ap_name(cand)
            if re.search(r"write[- ]?ins?", name, re.I):
                cand_map[ap_id] = writein_id      # AP's "Total Write-ins" is our
                continue                          # aggregate write-in line
            cid = match_candidate(cand, roster)
            if cid:
                cand_map[ap_id] = cid
            else:
                unmatched_cand.append(f"{rid} {meta.get('officeName')} {meta.get('seatName')} "
                                      f"{meta.get('party')}: {name!r} not on our race {race_id}")

        for key, unit in detail.items():
            if key == "summary":
                continue
            muni, skip = resolve_unit(unit.get("reportingunitName"), plain, wards)
            if not muni:
                skipped_units[skip].add(unit.get("reportingunitName"))
                continue
            # Only fully-counted towns. AP seeds every town with zeros at poll
            # close; importing those would show 0-vote returns for towns that
            # have simply not counted yet - the exact false impression we are
            # trying to fix tonight.
            if (unit.get("precinctsTotal") or 0) == 0 or \
               (unit.get("precinctsReporting") or 0) < unit["precinctsTotal"]:
                skipped_incomplete += 1
                continue

            for c in unit.get("candidates") or []:
                cid = cand_map.get(c.get("candidateID"))
                if not cid:
                    continue
                votes = int(c.get("voteCount") or 0)
                cursor.execute("""SELECT votes FROM results
                                   WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                               (race_id, cid, muni))
                row = cursor.fetchone()
                if row is None:
                    if args.apply:
                        cursor.execute("""INSERT INTO results
                                          (race_id, candidate_id, municipality, votes)
                                          VALUES (?, ?, ?, ?)""",
                                       (race_id, cid, muni, votes))
                        log_audit(cursor, user_id, race_id, muni, cid, FEED_ACTION,
                                  None, {"votes": votes, "source": "AP", "ap_race": rid})
                    inserted += 1
                    towns_touched.add(muni)
                    races_touched.add(race_id)
                elif row["votes"] != votes:
                    if human_entered(cursor, race_id, muni, cid):
                        # A clerk's tape beats the wire. Report, do not touch.
                        conflicts.append(f"{muni} race {race_id} cand {cid}: "
                                         f"ours={row['votes']} AP={votes} ({rid})")
                    else:
                        if args.apply:
                            cursor.execute("""UPDATE results SET votes = ?
                                               WHERE race_id = ? AND candidate_id = ?
                                                 AND municipality = ?""",
                                           (votes, race_id, cid, muni))
                            log_audit(cursor, user_id, race_id, muni, cid, FEED_ACTION,
                                      {"votes": row["votes"]},
                                      {"votes": votes, "source": "AP", "ap_race": rid})
                        updated += 1
                        towns_touched.add(muni)
                        races_touched.add(race_id)

        # Commit each race before fetching the next. The loop interleaves
        # network calls with writes, so holding one transaction across all 431
        # races kept SQLite's write lock for minutes at a time - past the
        # intake service's 30-second busy timeout. Five real reports died with
        # "database is locked" while this ran on a 90-second cycle. A race is a
        # complete unit of work; there is nothing to gain by batching them.
        if args.apply:
            doubled = double_counted_cities(cursor)
            if doubled:
                conn.rollback()
                print(f"  !! {rid}: would double-count {', '.join(doubled)} - rolled back")
                continue
            conn.commit()

    # Fail closed. Check the board WITH this run's rows in the transaction but
    # BEFORE the commit, so a run that would put a city on the board twice is
    # rolled back rather than published.
    doubled = double_counted_cities(cursor)
    if doubled:
        conn.rollback()
        print()
        print("!! ABORTED - these cities would appear as both a bare name and wards,")
        print("!! which double-counts them in every statewide total:")
        for c in doubled:
            print(f"!!     {c}")
        print("!! Nothing was written.")
        sys.exit(1)

    if args.apply:
        conn.commit()

    print()
    print("=" * 70)
    print(f"{'APPLIED' if args.apply else 'DRY RUN'}")
    print(f"  rows to insert          : {inserted}")
    print(f"  feed rows to refresh    : {updated}")
    print(f"  polling places touched  : {len(towns_touched)}")
    print(f"  races touched           : {len(races_touched)}")
    print(f"  town/race units skipped as not fully reported: {skipped_incomplete}")
    print(f"  conflicts with our own numbers (left alone)  : {len(conflicts)}")
    for c in conflicts[:40]:
        print(f"      {c}")
    if len(conflicts) > 40:
        print(f"      ... and {len(conflicts) - 40} more")
    print(f"  AP races we could not map: {len(unmatched_race)}")
    for r in sorted(unmatched_race)[:20]:
        print(f"      {r}")
    print(f"  AP candidates not on our ballot: {len(unmatched_cand)}")
    for c in unmatched_cand[:25]:
        print(f"      {c}")
    if len(unmatched_cand) > 25:
        print(f"      ... and {len(unmatched_cand) - 25} more")
    # Split deliberately-refused units from genuine gaps. Reporting them in one
    # undifferentiated list read as "the biggest cities in the state are
    # missing" when in fact every one of their wards was already imported and
    # the refused rows were duplicate citywide rollups.
    deliberate = sorted(skipped_units[SKIP_ROLLUP] | skipped_units[SKIP_WARD])
    unknown = sorted(x for x in skipped_units[SKIP_UNKNOWN] if x)
    print(f"  AP units skipped as duplicates of rows we already hold: {len(deliberate)}")
    print(f"      (AP publishes ward cities twice - per ward AND as one citywide")
    print(f"       rollup; we take whichever form matches how the city files)")
    print(f"      {deliberate}")
    print(f"  AP units we cannot map at all (REAL coverage gap): {len(unknown)}")
    print(f"      {unknown}")
    print(f"  cities on the board twice (bare name AND wards): 0  [checked]")
    # One compact line last, because nightloop.sh only keeps `tail -4` of this
    # output and the counts are what the operator watches between passes.
    print(f"  == {'applied' if args.apply else 'dry run'}: +{inserted} new, "
          f"{updated} refreshed, {len(towns_touched)} places, "
          f"{len(conflicts)} conflicts kept, {len(unknown)} unmappable, "
          f"0 double-counted")
    if not args.apply:
        print()
        print("  Nothing was written. Re-run with --apply.")


if __name__ == "__main__":
    main()
