#!/usr/bin/env python3
"""
Import 2026 filings from the candidate-recruitment system into the results DB
as the 2026 STATE PRIMARY ballot roster.

Input: a JSON snapshot of the recruitment `filings` table (list of objects with
office, district_code, first_name, last_name, party, town, candidate_id,
incumbent). Generate it on the server with:

    psql "$DATABASE_URL" -t -A -c "SELECT json_agg(row_to_json(t)) FROM (
        SELECT f.filing_id, f.office, f.district_code, f.first_name, f.last_name,
               f.party, f.town, f.candidate_id,
               COALESCE(c.incumbent,false) AS incumbent
        FROM filings f LEFT JOIN candidates c ON f.candidate_id=c.candidate_id
        WHERE f.election_year=2026) t;" > filings_2026.json

What it creates (idempotent):
  * elections: (2026, state_primary, Republican) and (2026, state_primary, Democratic)
  * the Delegate office, if missing
  * races: (election, office, county, district) with seats from the 2024 general
  * candidates: reused on a unique name_normalized+party match, else created
  * race_candidates: the ballot roster

Independents (party I) are declaration-of-intent candidates who are NOT on any
primary ballot (RSA 655) and are skipped here.

Usage: python3 import_filings_2026.py filings_2026.json [nh_elections.db]
"""

import json
import re
import sqlite3
import sys

YEAR = 2026
CYCLE = "2022-2030"

PARTY_FULL = {"R": "Republican", "D": "Democratic", "I": "Independent"}
PRIMARY_PARTIES = {"R": "Republican", "D": "Democratic"}

# filings.office -> offices.name in the results DB
OFFICE_MAP = {
    "State Representative": "State Representative",
    "Delegate to the State Convention": "Delegate to the State Convention",
    "State Senator": "State Senator",
    "Executive Councilor": "Executive Councilor",
    "Representative in Congress": "Representative in Congress",
    "United States Senator": "United States Senator",
    "Governor": "Governor",
    "County Commissioner": "County Commissioner",
    "Sheriff": "County Sheriff",
    "County Attorney": "County Attorney",
    "County Treasurer": "County Treasurer",
    "Register of Deeds": "Register of Deeds",
    "Register of Probate": "Register of Probate",
}

COUNTY_FROM_TOWN_OFFICES = {  # filing office names that need county derived from candidate town
    "County Commissioner", "Sheriff", "County Attorney",
    "County Treasurer", "Register of Deeds", "Register of Probate",
}

# Messy filing town -> canonical base town (uppercase) for county lookup.
TOWN_ALIASES = {
    "CENTER OSSIPEE": "OSSIPEE",
    "CONTOOCOOK": "HOPKINTON",
    "GILMANTON IW": "GILMANTON",
    "MOULTONBORO": "MOULTONBOROUGH",
    "PENACOOK": "CONCORD",
    "PIKE": "HAVERHILL",
    "PINE RIVER PATH EFFINGHAM": "EFFINGHAM",
    "WINNISQUAM": "TILTON",
    "WOLFEBORO FLS": "WOLFEBORO",
}


def normalize_name(name):
    """Match the results DB convention: uppercase, drop punctuation, collapse spaces."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", "", name.upper())).strip()


def ensure_office(cur, name, level="state"):
    cur.execute("SELECT id FROM offices WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO offices (name, level) VALUES (?, ?)", (name, level))
    return cur.lastrowid


def ensure_election(cur, party_full):
    cur.execute(
        "SELECT id FROM elections WHERE year=? AND election_type='state_primary' AND party=?",
        (YEAR, party_full),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO elections (year, election_type, party, redistricting_cycle) VALUES (?, 'state_primary', ?, ?)",
        (YEAR, party_full, CYCLE),
    )
    return cur.lastrowid


from entry_sources import town_county_map as build_town_county


def build_seats_map(cur):
    """(office_id, county, district) -> seats, from the 2024 general election."""
    cur.execute(
        """SELECT r.office_id, COALESCE(r.county,''), COALESCE(r.district,''), r.seats
               FROM races r JOIN elections e ON r.election_id=e.id
               WHERE e.year=2024 AND e.election_type='general'"""
    )
    return {(oid, c, d): s for oid, c, d, s in cur.fetchall()}


def parse_district(office, district_code, town, town_county):
    """Return (county, district) for the results race, or (None, None) to skip."""
    code = (district_code or "").strip()
    if office in ("State Representative", "Delegate to the State Convention"):
        county, _, district = code.rpartition(" ")
        return county.strip(), district.strip()
    if office == "State Senator":
        return "", code.rsplit(" ", 1)[-1] if code else ""
    if office == "Executive Councilor":
        return "", code.rsplit(" ", 1)[-1] if code else ""
    if office == "Representative in Congress":
        return "", code.split("-")[-1] if code else ""  # NH-1 -> 1
    if office in ("United States Senator", "Governor"):
        return "", ""  # statewide
    if office in COUNTY_FROM_TOWN_OFFICES:
        t = town.strip().upper()
        county = town_county.get(TOWN_ALIASES.get(t, t))
        if county is None:
            return None, None
        if office == "County Commissioner":
            return county, code.rsplit(" ", 1)[-1] if code else ""
        return county, ""  # county-wide
    return None, None


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: import_filings_2026.py filings_2026.json [nh_elections.db]")
    snapshot = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "nh_elections.db"

    filings = json.load(open(snapshot))
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    ensure_office(cur, "Delegate to the State Convention", "state")
    election_ids = {p: ensure_election(cur, full) for p, full in PRIMARY_PARTIES.items()}

    # Idempotent reset: clear this primary's races/roster/results and any
    # candidates a previous run created but no longer references. Lets re-runs
    # pick up corrected seats, rosters, and filings cleanly.
    eids = list(election_ids.values())
    qm = ",".join("?" * len(eids))
    cur.execute(f"SELECT id FROM races WHERE election_id IN ({qm})", eids)
    old_race_ids = [r[0] for r in cur.fetchall()]
    if old_race_ids:
        rq = ",".join("?" * len(old_race_ids))
        cur.execute(f"DELETE FROM results WHERE race_id IN ({rq})", old_race_ids)
        cur.execute(f"DELETE FROM race_candidates WHERE race_id IN ({rq})", old_race_ids)
        cur.execute(f"DELETE FROM races WHERE id IN ({rq})", old_race_ids)
    # Drop candidates left orphaned by the reset (created by a prior import,
    # not part of any historical results or remaining roster).
    cur.execute("""DELETE FROM candidates
                   WHERE id NOT IN (SELECT candidate_id FROM results)
                     AND id NOT IN (SELECT candidate_id FROM race_candidates)""")

    # resolve office ids by results-office name
    oid_by_office = {}
    for fname, rname in OFFICE_MAP.items():
        cur.execute("SELECT id FROM offices WHERE name=?", (rname,))
        r = cur.fetchone()
        oid_by_office[fname] = r[0] if r else None

    town_county = build_town_county(cur)
    seats_map = build_seats_map(cur)

    stats = {"skipped_indep": 0, "skipped_other": 0, "races": 0, "cand_reused": 0,
             "cand_created": 0, "roster": 0, "no_county": 0, "roster_exists": 0}
    race_cache = {}        # (election_id, office_id, county, district) -> race_id
    ballot_order = {}      # race_id -> next order index

    for f in filings:
        party = f["party"]
        if party == "I":
            stats["skipped_indep"] += 1
            continue
        if party not in PRIMARY_PARTIES:
            stats["skipped_other"] += 1
            continue
        office = f["office"]
        if office not in OFFICE_MAP or oid_by_office.get(office) is None:
            stats["skipped_other"] += 1
            continue

        oid = oid_by_office[office]
        county, district = parse_district(office, f["district_code"], f.get("town") or "", town_county)
        if county is None:
            stats["no_county"] += 1
            continue

        election_id = election_ids[party]
        key = (election_id, oid, county, district)
        race_id = race_cache.get(key)
        if race_id is None:
            cur.execute(
                "SELECT id FROM races WHERE election_id=? AND office_id=? AND "
                "COALESCE(county,'')=? AND COALESCE(district,'')=?",
                (election_id, oid, county, district),
            )
            row = cur.fetchone()
            if row:
                race_id = row[0]
            else:
                # Delegates are apportioned exactly like State Reps for the
                # district, so borrow the State Rep seat count.
                seats_oid = (oid_by_office["State Representative"]
                             if office == "Delegate to the State Convention" else oid)
                seats = seats_map.get((seats_oid, county, district), 1)
                cur.execute(
                    "INSERT INTO races (election_id, office_id, district, county, seats) VALUES (?,?,?,?,?)",
                    (election_id, oid, district or None, county or None, seats),
                )
                race_id = cur.lastrowid
                stats["races"] += 1
            race_cache[key] = race_id

        # --- candidate: reuse on unique normalized-name+party match, else create
        name = f"{f['first_name']} {f['last_name']}".strip()
        norm = normalize_name(name)
        party_full = PARTY_FULL[party]
        cur.execute(
            "SELECT id FROM candidates WHERE name_normalized=? AND party=?", (norm, party_full)
        )
        matches = cur.fetchall()
        if len(matches) == 1:
            candidate_id = matches[0][0]
            stats["cand_reused"] += 1
        else:
            cur.execute(
                "INSERT INTO candidates (name, name_normalized, party) VALUES (?,?,?)",
                (name, norm, party_full),
            )
            candidate_id = cur.lastrowid
            stats["cand_created"] += 1

        # --- roster row
        order = ballot_order.get(race_id, 0)
        cur.execute(
            """INSERT OR IGNORE INTO race_candidates
                   (race_id, candidate_id, party, ballot_order, is_incumbent,
                    recruitment_candidate_id, recruitment_filing_id)
                   VALUES (?,?,?,?,?,?,?)""",
            (race_id, candidate_id, party_full, order,
             0, f.get("candidate_id"), f.get("filing_id")),
        )
        if cur.rowcount:
            stats["roster"] += 1
            ballot_order[race_id] = order + 1
        else:
            stats["roster_exists"] += 1

    conn.commit()

    print("Import summary:")
    for k in ("races", "roster", "cand_reused", "cand_created", "roster_exists",
              "skipped_indep", "skipped_other", "no_county"):
        print(f"  {k:16s} {stats[k]}")
    for p, eid in election_ids.items():
        cur.execute("SELECT COUNT(*) FROM races WHERE election_id=?", (eid,))
        nr = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM race_candidates rc JOIN races r ON rc.race_id=r.id WHERE r.election_id=?",
            (eid,),
        )
        ncand = cur.fetchone()[0]
        print(f"  {PRIMARY_PARTIES[p]} primary (election {eid}): {nr} races, {ncand} candidates")
    conn.close()


if __name__ == "__main__":
    main()
