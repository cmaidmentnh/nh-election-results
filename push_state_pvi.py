#!/usr/bin/env python3
"""Compute the NH State PVI for every State Rep district and push it to the
recruitment database, so both apps quote the same number.

    python3 push_state_pvi.py --dry-run
    python3 push_state_pvi.py

The index itself lives in analysis.state_pvi_for_municipalities. This script is
only the transport: elections database in, candidate_recruitment out.
"""
import os
import re
import sys

import psycopg2

import analysis

DSN = os.environ.get(
    "RECRUITMENT_DSN",
    "host=127.0.0.1 dbname=candidate_recruitment user=postgres password=postgres123")

TIER = {'SAFE GOP': 1, 'LIKELY GOP': 2, 'LEAN GOP': 3, 'SWING': 4,
        'LEAN DEM': 5, 'LIKELY DEM': 6, 'SAFE DEM': 7}


def main():
    dry = "--dry-run" in sys.argv
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("""SELECT full_district_code,
                          STRING_AGG(DISTINCT CASE WHEN ward IS NOT NULL AND ward <> 0
                               THEN town || ' Ward ' || ward ELSE town END, '|'),
                          MAX(pvi), MAX(pvi_rating)
                   FROM districts GROUP BY full_district_code""")
    rows = cur.fetchall()

    updated = skipped = 0
    for code, munis, old_pvi, old_rating in rows:
        # the districts table carries stray double spaces in a few town names
        names = [re.sub(r"\s+", " ", m.strip()) for m in (munis or "").split("|") if m.strip()]
        state = analysis.state_pvi_for_municipalities(names)
        if not state:
            print("  no data for %s (%s)" % (code, ", ".join(names)))
            skipped += 1
            continue
        pvi, rating = state['pvi'], state['rating']
        if old_pvi is not None and abs(float(old_pvi) - pvi) < 0.05 and old_rating == rating:
            continue
        print("  %-17s %+6.1f %-11s   was %+6.1f %s"
              % (code, pvi, rating, float(old_pvi) if old_pvi is not None else 0, old_rating))
        if not dry:
            cur.execute("""UPDATE districts SET pvi = %s, pvi_rating = %s, pvi_tier = %s
                           WHERE full_district_code = %s""",
                        (pvi, rating, TIER[rating], code))
        updated += 1

    if dry:
        conn.rollback()
        print("\ndry run: %d districts would change, %d had no data" % (updated, skipped))
    else:
        conn.commit()
        print("\nupdated %d districts, %d had no data" % (updated, skipped))
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
