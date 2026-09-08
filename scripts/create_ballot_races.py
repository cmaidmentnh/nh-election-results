#!/usr/bin/env python3
"""Create every race a town actually votes on but that has no row in `races`.

Races were built from candidate filings, so a contest nobody filed for was never
created - even though it is printed on the ballot and voters write names in on
it. On primary night that meant real write-in totals (Littleton's 42 for
sheriff, Strafford's 150 for county commissioner) came back from the parser as
"Race not identified on this town's ballot", because the roster we hand the
model genuinely did not contain the race.

`municipality_districts` is the authority on what each town votes on. Anything
in there with no matching race gets one, with no candidates - the aggregate
write-in line is added by the roster builder on demand.

Idempotent: run it as often as you like.
"""
import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# County offices are single-seat. Delegate to the State Convention elects the
# same number of delegates as the floterial-free House district it mirrors, so
# it borrows State Representative's seat count for the same county/district.
SINGLE_SEAT = {"County Attorney", "County Sheriff", "County Treasurer",
               "Register of Deeds", "Register of Probate", "County Commissioner"}


def missing(cur, election_ids):
    qs = ",".join("?" * len(election_ids))
    cur.execute(f"""
        SELECT DISTINCT md.office_id, o.name AS office, md.county, md.district, e.id AS election_id
          FROM municipality_districts md
          JOIN offices o ON o.id = md.office_id
          CROSS JOIN elections e
         WHERE e.id IN ({qs})
           AND NOT EXISTS (
                 SELECT 1 FROM races r
                  WHERE r.election_id = e.id
                    AND r.office_id  = md.office_id
                    AND COALESCE(r.county,'')   = md.county
                    AND COALESCE(r.district,'') = md.district)
         ORDER BY o.name, md.county, md.district, e.id
    """, election_ids)
    return [dict(r) for r in cur.fetchall()]


def seats_for(cur, row):
    if row["office"] in SINGLE_SEAT:
        return 1
    # The same contest on the other party's ballot is the best answer.
    cur.execute("""SELECT seats FROM races
                    WHERE office_id = ? AND COALESCE(county,'') = ?
                      AND COALESCE(district,'') = ? AND seats IS NOT NULL
                    ORDER BY election_id DESC LIMIT 1""",
                (row["office_id"], row["county"], row["district"]))
    hit = cur.fetchone()
    if hit:
        return hit["seats"] or 1
    if row["office"] == "Delegate to the State Convention":
        cur.execute("""SELECT r.seats FROM races r JOIN offices o ON o.id = r.office_id
                        WHERE o.name = 'State Representative'
                          AND COALESCE(r.county,'') = ? AND COALESCE(r.district,'') = ?
                        ORDER BY r.seats DESC LIMIT 1""",
                    (row["county"], row["district"]))
        hit = cur.fetchone()
        if hit:
            return hit["seats"] or 1
    return 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="nh_elections.db")
    ap.add_argument("--elections", default="29,30")
    ap.add_argument("--apply", action="store_true",
                    help="write the races; without it, only report")
    args = ap.parse_args()

    election_ids = [int(x) for x in args.elections.split(",")]
    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = missing(cur, election_ids)
    by_office = {}
    for r in rows:
        by_office[r["office"]] = by_office.get(r["office"], 0) + 1
    for office, n in sorted(by_office.items()):
        print(f"  {office:34s} {n:4d}")
    print(f"{len(rows)} races missing across elections {election_ids}")

    if not args.apply:
        print("(dry run - pass --apply to create them)")
        return

    made = 0
    for r in rows:
        cur.execute("""INSERT OR IGNORE INTO races
                         (election_id, office_id, district, county, seats, is_official)
                       VALUES (?,?,?,?,?,0)""",
                    (r["election_id"], r["office_id"], r["district"] or None,
                     r["county"] or None, seats_for(cur, r)))
        made += cur.rowcount
    conn.commit()
    print(f"created {made} races")


if __name__ == "__main__":
    main()
