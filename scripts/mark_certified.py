#!/usr/bin/env python3
"""Mark the races whose figures came from the Secretary of State's sheets.

A certified race is not a projection. Every town is counted, the numbers are
final, and the page should say so rather than quote a share of the expected
vote and a chance the order holds.

Which races qualify is not a guess: it is the list of sheets the Secretary of
State has published, minus what was refused. Anything not covered here keeps
its projection, which is the honest answer for a race we are still reading off
election-night returns.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# office name -> which parties the SoS has published it for
PUBLISHED = {
    "Governor": ("Republican", "Democratic"),
    "United States Senator": ("Republican", "Democratic"),
    "Representative in Congress": ("Republican", "Democratic"),
    "Executive Councilor": ("Republican", "Democratic"),
    "State Senator": ("Republican", "Democratic"),
    "State Representative": ("Republican", "Democratic"),
    "County Attorney": ("Republican", "Democratic"),
    "County Sheriff": ("Republican", "Democratic"),
    "County Treasurer": ("Republican", "Democratic"),
    "County Commissioner": ("Republican", "Democratic"),
    "Register of Deeds": ("Republican", "Democratic"),
    "Register of Probate": ("Republican", "Democratic"),
    # Only Republicans elect delegates to the state convention, so there is no
    # Democratic sheet to wait for. The 203 Democratic delegate races on file
    # are empty shells - no candidate, no vote - and the board already leaves
    # out a race with neither.
    "Delegate to the State Convention": ("Republican",),
}

# Refused on purpose, and not certified as a result. Strafford 3's own sheet
# prints a total of 666 for Adamo against town rows making 363, and credits two
# candidates who appear in no row.
REFUSED = [("State Representative", "Democratic", "Strafford", "3")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS certified_races (
                     race_id INTEGER PRIMARY KEY,
                     source TEXT,
                     certified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    refused = set()
    for office, party, county, district in REFUSED:
        for row in cur.execute("""SELECT ra.id FROM races ra
                                    JOIN offices o ON o.id = ra.office_id
                                    JOIN elections e ON e.id = ra.election_id
                                   WHERE o.name = ? AND e.party = ?
                                     AND IFNULL(ra.county,'') = ?
                                     AND IFNULL(ra.district,'') = ?
                                     AND e.year = 2026
                                     AND e.election_type = 'state_primary'""",
                               (office, party, county, district)):
            refused.add(row["id"])

    wanted = []
    for office, parties in PUBLISHED.items():
        for party in parties:
            for row in cur.execute("""SELECT ra.id FROM races ra
                                        JOIN offices o ON o.id = ra.office_id
                                        JOIN elections e ON e.id = ra.election_id
                                       WHERE o.name = ? AND e.party = ?
                                         AND e.year = 2026
                                         AND e.election_type = 'state_primary'""",
                                   (office, party)):
                if row["id"] not in refused:
                    wanted.append(row["id"])

    have = {r["race_id"] for r in cur.execute("SELECT race_id FROM certified_races")}
    new = [r for r in wanted if r not in have]
    stale = [r for r in have if r not in set(wanted)]

    print(f"{len(wanted)} race(s) covered by a published sheet")
    print(f"  already marked : {len(have)}")
    print(f"  to mark        : {len(new)}")
    print(f"  to unmark      : {len(stale)}")
    print(f"  refused, left as projections: {len(refused)}")
    if not args.apply:
        print("\n(report only - pass --apply to write it)")
        return

    cur.executemany("INSERT OR IGNORE INTO certified_races (race_id, source) VALUES (?, 'sos')",
                    [(r,) for r in new])
    if stale:
        cur.executemany("DELETE FROM certified_races WHERE race_id = ?", [(r,) for r in stale])
    conn.commit()
    print(f"\nmarked {len(new)}, unmarked {len(stale)}")


if __name__ == "__main__":
    main()
