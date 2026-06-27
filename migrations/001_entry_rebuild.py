#!/usr/bin/env python3
"""
Migration 001: Entry system rebuild.

Adds the schema needed for polling-place-centric results entry:

  * race_candidates       - explicit ballot roster per race (fixes the
                            chicken-and-egg problem where candidates only
                            existed in a race once a results row was written).
  * polling_places        - clerk / hours / address per municipality, loaded
                            from the SoS clerks & polling-places list.
  * municipality_districts - map of municipality -> (office, county, district)
                            so we know every race a polling place votes on.

Idempotent: safe to run repeatedly. Run locally against nh_elections.db and
on the server after deploy.

    python3 migrations/001_entry_rebuild.py [path/to/nh_elections.db]
"""

import sqlite3
import sys

DEFAULT_DB = "nh_elections.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS race_candidates (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id                  INTEGER NOT NULL,
    candidate_id             INTEGER NOT NULL,
    party                    TEXT,
    ballot_order             INTEGER DEFAULT 0,
    is_incumbent             INTEGER DEFAULT 0,
    recruitment_candidate_id INTEGER,   -- candidates.candidate_id in recruitment Postgres
    recruitment_filing_id    INTEGER,   -- filings.filing_id in recruitment Postgres
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (race_id)      REFERENCES races(id),
    FOREIGN KEY (candidate_id) REFERENCES candidates(id),
    UNIQUE(race_id, candidate_id)
);
CREATE INDEX IF NOT EXISTS idx_race_candidates_race ON race_candidates(race_id);
CREATE INDEX IF NOT EXISTS idx_race_candidates_cand ON race_candidates(candidate_id);

CREATE TABLE IF NOT EXISTS polling_places (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    municipality  TEXT NOT NULL UNIQUE,  -- normalized to match district_compositions
    county        TEXT,
    clerk         TEXT,
    clerk_address TEXT,
    phone         TEXT,
    fax           TEXT,
    email         TEXT,
    website       TEXT,
    polling_hours TEXT,
    polling_place TEXT,
    raw_name      TEXT                   -- original name from the source CSV
);
CREATE INDEX IF NOT EXISTS idx_polling_places_county ON polling_places(county);

-- One row per (municipality, office, district) the municipality votes in.
-- Statewide offices (Governor, US Senator, President) are intentionally NOT
-- stored here; the entry layer always includes statewide races for every town.
CREATE TABLE IF NOT EXISTS municipality_districts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    municipality        TEXT NOT NULL,
    office_id           INTEGER NOT NULL,
    county              TEXT NOT NULL DEFAULT '',
    district            TEXT NOT NULL DEFAULT '',
    redistricting_cycle TEXT NOT NULL DEFAULT '2022-2030',
    source              TEXT,
    FOREIGN KEY (office_id) REFERENCES offices(id),
    UNIQUE(municipality, office_id, county, district, redistricting_cycle)
);
CREATE INDEX IF NOT EXISTS idx_municipality_districts_muni ON municipality_districts(municipality);
CREATE INDEX IF NOT EXISTS idx_municipality_districts_office ON municipality_districts(office_id);
"""


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
        # Report what exists now.
        cur = conn.cursor()
        for tbl in ("race_candidates", "polling_places", "municipality_districts"):
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            print(f"  {tbl:24s} rows={cur.fetchone()[0]}")
        print(f"Migration 001 applied to {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
