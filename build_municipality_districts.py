#!/usr/bin/env python3
"""
Build the municipality_districts map: every (municipality -> office, county,
district) a polling place votes on, for the 2022-2030 redistricting cycle.

Sources (all local, no external dependencies):
  * State Representative + Delegate to the State Convention
        -> district_compositions (cycle 2022-2030). Delegates are elected by
           the same House districts, so they reuse the House town membership.
  * State Senator, Executive Councilor, Representative in Congress
        -> town membership derived from the 2024 general election results
           (same redistricting cycle, ward-level for cities).
  * County-wide offices (Sheriff, Attorney, Treasurer, Registers)
        -> every municipality in the county (county taken from its House district).
  * County Commissioner
        -> every commissioner district in the town's own county. The districts
           decide who may run, not who may vote; the county elects all three
           seats. See section 4 for the evidence.

NOT stored here (handled elsewhere by the entry layer):
  * Statewide offices (Governor, US Senator, President) - apply to every town.

Idempotent. Usage: python3 build_municipality_districts.py [nh_elections.db]
"""

import json
import os
import re
import sqlite3
import sys

DEFAULT_DB = "nh_elections.db"
CYCLE = "2022-2030"
HERE = os.path.dirname(os.path.abspath(__file__))

# Filing/office name -> offices.name in the results DB.
DELEGATE_OFFICE = "Delegate to the State Convention"

# County-wide county offices (single race per county, every town votes).
COUNTY_WIDE_OFFICES = [
    "County Sheriff",
    "County Attorney",
    "County Treasurer",
    "Register of Deeds",
    "Register of Probate",
]


def ensure_office(cur, name, level):
    cur.execute("SELECT id FROM offices WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO offices (name, level) VALUES (?, ?)", (name, level))
    return cur.lastrowid


def office_id(cur, name):
    cur.execute("SELECT id FROM offices WHERE name = ?", (name,))
    row = cur.fetchone()
    return row[0] if row else None


def upsert(cur, muni, off_id, county, district, source):
    cur.execute(
        """INSERT OR IGNORE INTO municipality_districts
               (municipality, office_id, county, district, redistricting_cycle, source)
               VALUES (?, ?, ?, ?, ?, ?)""",
        (muni, off_id, county or "", district or "", CYCLE, source),
    )


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    delegate_oid = ensure_office(cur, DELEGATE_OFFICE, "state")
    rep_oid = office_id(cur, "State Representative")
    senate_oid = office_id(cur, "State Senator")
    exec_oid = office_id(cur, "Executive Councilor")
    ushouse_oid = office_id(cur, "Representative in Congress")

    # Wipe and rebuild for a clean cycle map.
    cur.execute("DELETE FROM municipality_districts WHERE redistricting_cycle = ?", (CYCLE,))

    # --- 1. State Rep + Delegate, from the 2024 general results --------------
    # The 2024 general is the cleanest, most complete source: every House seat
    # is up every cycle, names are ward-correct, and there is none of the
    # duplicate/empty-county/asterisk junk that pollutes district_compositions.
    # Delegates use the same House districts (base + floterial).
    cur.execute(
        """SELECT DISTINCT res.municipality, r.county, r.district
               FROM results res
               JOIN races r    ON res.race_id = r.id
               JOIN offices o  ON r.office_id = o.id
               JOIN elections e ON r.election_id = e.id
               WHERE o.name = 'State Representative'
                 AND e.year = 2024 AND e.election_type = 'general'"""
    )
    rep_rows = cur.fetchall()
    muni_county = {}  # municipality -> county (for county-wide offices)
    for muni, county, district in rep_rows:
        upsert(cur, muni, rep_oid, county, district, "results2024")
        upsert(cur, muni, delegate_oid, county, district, "results2024(delegate)")
        muni_county.setdefault(muni, county)
    print(f"  State Rep + Delegate: {len(rep_rows)} town/district rows ({len(muni_county)} munis)")

    # --- 2. Senate / Exec Council / US House, from 2024 general results -------
    for label, oid in (("State Senator", senate_oid),
                        ("Executive Councilor", exec_oid),
                        ("Representative in Congress", ushouse_oid)):
        cur.execute(
            """SELECT DISTINCT res.municipality, r.district
                   FROM results res
                   JOIN races r       ON res.race_id = r.id
                   JOIN offices o      ON r.office_id = o.id
                   JOIN elections e    ON r.election_id = e.id
                   WHERE o.name = ? AND e.year = 2024 AND e.election_type = 'general'""",
            (label,),
        )
        rows = cur.fetchall()
        for muni, district in rows:
            upsert(cur, muni, oid, "", district, "results2024")
        print(f"  {label}: {len(rows)} town rows")

    # --- 3. County-wide offices, one race per county -------------------------
    cw_count = 0
    for off_name in COUNTY_WIDE_OFFICES:
        oid = office_id(cur, off_name)
        if oid is None:
            continue
        for muni, county in muni_county.items():
            upsert(cur, muni, oid, county, "", "county-wide")
            cw_count += 1
    print(f"  County-wide offices: {cw_count} town rows across {len(COUNTY_WIDE_OFFICES)} offices")

    # --- 4. County Commissioner: every district in the town's own county ------
    # commissioner_districts.json (town.UPPER -> {county, district}) is the same
    # authoritative map used by candidates.electhouserepublicans.com. But that
    # map answers "which commissioner district is this town IN" - a RESIDENCY
    # fact, about who may run - and this table answers "what does this town VOTE
    # ON". For NH county commissioners those are not the same question: a
    # candidate must live in the district, and then the whole county elects all
    # three seats. Filing one row per town, for its own district only, therefore
    # understated every town's ballot by two races.
    #
    # The Return of Votes proves it. Goshen's official Republican return prints
    # "For County Commissioner, 1st District - Joe Osgood 53, Undervotes 21" AND
    # "2nd District - Bennie Nelson 53, Undervotes 21" against 74 Republican
    # ballots cast: 53 + 21 = 74 in BOTH races. You cannot account for every
    # ballot in a race the town does not vote in. Ossipee's Republican ballot and
    # Chatham's Democratic ballot likewise print all three Carroll districts,
    # each "Vote for not more than 1".
    #
    # On primary night this cost 76 real County Commissioner lines from ten
    # towns, all held as "Race not identified on this town's ballot", because the
    # roster we hand the parser showed one commissioner race where the ballot has
    # three. So: a town votes on every commissioner district in its county.
    comm_oid = office_id(cur, "County Commissioner")
    comm_path = os.path.join(HERE, "data", "commissioner_districts.json")
    comm_count = 0
    try:
        comm_map = json.load(open(comm_path))
    except FileNotFoundError:
        comm_map = {}
        print("  County Commissioner: data/commissioner_districts.json not found, skipped")
    if comm_map and comm_oid:
        # county -> every commissioner district in it, from the residency map.
        districts_in_county = {}
        for rec in comm_map.values():
            districts_in_county.setdefault(rec["county"], set()).add(str(rec["district"]))
        for muni in muni_county:
            base = re.sub(r"\s+Ward\s+\d+\*?$", "", muni).strip().upper()
            rec = comm_map.get(base)
            if rec:
                for district in sorted(districts_in_county[rec["county"]]):
                    upsert(cur, muni, comm_oid, rec["county"], district,
                           "commissioner-json(county-wide)")
                    comm_count += 1
        print(f"  County Commissioner: {comm_count} town rows")

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM municipality_districts WHERE redistricting_cycle = ?", (CYCLE,))
    total = cur.fetchone()[0]
    cur.execute(
        """SELECT o.name, COUNT(*) FROM municipality_districts md
               JOIN offices o ON md.office_id = o.id
               WHERE md.redistricting_cycle = ? GROUP BY o.name ORDER BY o.name""",
        (CYCLE,),
    )
    print(f"\nmunicipality_districts total: {total}")
    for name, n in cur.fetchall():
        print(f"    {name:34s} {n}")
    conn.close()


if __name__ == "__main__":
    main()
