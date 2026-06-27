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

NOT stored here (handled elsewhere by the entry layer):
  * Statewide offices (Governor, US Senator, President) - apply to every town.
  * County Commissioner - sub-district town membership is not available in any
    current data source; entered via the race-centric view until a map exists.

Idempotent. Usage: python3 build_municipality_districts.py [nh_elections.db]
"""

import sqlite3
import sys

DEFAULT_DB = "nh_elections.db"
CYCLE = "2022-2030"

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
