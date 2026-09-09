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
        -> the town's own district, EXCEPT in the counties that elect their
           commissioners county-wide (Carroll and Sullivan this cycle), where
           every district in the county is on every town's ballot. Which
           counties those are is read from past general results, not assumed.
           See section 4.

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

    # --- 4. County Commissioner -----------------------------------------------
    # commissioner_districts.json (town.UPPER -> {county, district}) is the same
    # authoritative map used by candidates.electhouserepublicans.com. But it
    # answers "which commissioner district is this town IN" - a RESIDENCY fact,
    # about who may run - and this table answers "what does this town VOTE ON".
    # For county commissioner those are not the same question, and the answer
    # differs BY COUNTY. Two counties elect all their commissioners county-wide,
    # with the districts deciding only residency; the other eight elect each
    # commissioner from its own district, one race per voter.
    #
    # Do not guess which is which - the past two general elections say it
    # outright, and they are already in this database. If a town has results in
    # more than one commissioner district of its county, that county votes
    # county-wide. In the 2022-2030 cycle that is Carroll (every town in all
    # three districts) and Sullivan (every town in both), and nowhere else.
    #
    # Primary night proved the same thing from the paper. Goshen's official
    # Republican Return of Votes prints "For County Commissioner, 1st District -
    # Joe Osgood 53, Undervotes 21" AND "2nd District - Bennie Nelson 53,
    # Undervotes 21" against 74 Republican ballots cast: 53 + 21 = 74 in BOTH
    # races, so every ballot is accounted for in each. Ossipee's Republican
    # ballot and Chatham's Democratic ballot each print all three Carroll
    # districts, "Vote for not more than 1". Against that, Rochester Ward 5's
    # VotingWorks tape prints exactly one, headed "Wards 1, 5, 6 - For County
    # Commissioner", 393 + 5 write-ins + 53 undervotes = its 451 ballots.
    #
    # Filing only the residency row held 76 real commissioner lines from ten
    # Carroll and Sullivan towns as "Race not identified on this town's ballot".
    # Filing every district everywhere would be the opposite error: it would put
    # two races on a Strafford ballot that has one.
    comm_oid = office_id(cur, "County Commissioner")
    comm_path = os.path.join(HERE, "data", "commissioner_districts.json")
    comm_count = 0
    try:
        comm_map = json.load(open(comm_path))
    except FileNotFoundError:
        comm_map = {}
        print("  County Commissioner: data/commissioner_districts.json not found, skipped")
    if comm_map and comm_oid:
        cur.execute(
            """SELECT county FROM (
                     SELECT r.county AS county, res.municipality,
                            COUNT(DISTINCT r.district) AS n
                       FROM results res
                       JOIN races r     ON r.id = res.race_id
                       JOIN elections e ON e.id = r.election_id
                      WHERE r.office_id = ? AND e.election_type = 'general'
                        AND e.year >= ?
                      GROUP BY r.county, res.municipality)
                WHERE n > 1
                GROUP BY county""",
            (comm_oid, int(CYCLE.split("-")[0])),
        )
        county_wide = {row[0] for row in cur.fetchall()}

        # Where the same general elections say what a place actually voted on,
        # that beats the JSON outright. commissioner_districts.json is keyed by
        # TOWN, so it cannot express a city split across districts - and two
        # cities are split. Rochester's wards 1, 5 and 6 vote in Strafford's 1st
        # while 2, 3 and 4 vote in the 3rd (its tape is literally headed "Wards
        # 1, 5, 6"); Dover's wards 5 and 6 vote in the 3rd while 1-4 vote in the
        # 2nd; Laconia's ward 2 votes in Belknap's 3rd, not the 1st. Stripping
        # "Ward N" and looking up the city put all six on the wrong ballot, which
        # filed Rochester Ward 5's commissioner write-ins into a district it does
        # not vote in and held John Frank Scruton's 393 as "not on this race's
        # roster".
        cur.execute(
            """SELECT res.municipality, r.county, r.district
                 FROM results res
                 JOIN races r     ON r.id = res.race_id
                 JOIN elections e ON e.id = r.election_id
                WHERE r.office_id = ? AND e.election_type = 'general' AND e.year >= ?
                GROUP BY res.municipality, r.county, r.district""",
            (comm_oid, int(CYCLE.split("-")[0])),
        )
        voted = {}
        for muni, county, district in cur.fetchall():
            voted.setdefault(muni, (county, set()))[1].add(str(district or ""))

        # county -> every commissioner district in it, from the residency map.
        districts_in_county = {}
        for rec in comm_map.values():
            districts_in_county.setdefault(rec["county"], set()).add(str(rec["district"]))
        for muni in muni_county:
            if muni in voted:
                county, districts = voted[muni]
                for district in sorted(districts):
                    upsert(cur, muni, comm_oid, county, district, "results-general")
                    comm_count += 1
                continue
            base = re.sub(r"\s+Ward\s+\d+\*?$", "", muni).strip().upper()
            rec = comm_map.get(base)
            if not rec:
                continue
            if rec["county"] in county_wide:
                for district in sorted(districts_in_county[rec["county"]]):
                    upsert(cur, muni, comm_oid, rec["county"], district,
                           "commissioner-json(county-wide)")
                    comm_count += 1
            else:
                upsert(cur, muni, comm_oid, rec["county"], str(rec["district"]),
                       "commissioner-json")
                comm_count += 1
        print(f"  County Commissioner: {comm_count} town rows "
              f"({len(voted)} from results; county-wide: "
              f"{', '.join(sorted(county_wide)) or 'none'})")

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
