#!/usr/bin/env python3
"""Compute the NH State PVI once for every geography and store it.

Every page on the site reads the `state_pvi` table. Nothing computes its own
index any more, which is the whole point: one formula, one number, everywhere.

    python3 build_state_pvi.py            # rebuild the table
    python3 build_state_pvi.py --push     # also push districts to recruitment

Run it after loading new results.
"""
import sys
import time
from collections import defaultdict

import analysis

KINDS = ("town", "district", "senate", "exec", "congress", "county")


def municipality_sets(cur):
    """geography key -> its municipalities, for every kind of page the site has."""
    out = defaultdict(lambda: defaultdict(set))

    cur.execute("""SELECT DISTINCT municipality FROM results
                   WHERE municipality NOT GLOB '[0-9]*'
                     AND municipality NOT IN ('Undervotes','Overvotes','Write-Ins','TOTALS',
                                              'Court ordered recount','court ordered recount')""")
    for (m,) in cur.fetchall():
        base = m.split(" Ward ")[0]
        out["town"][base].add(m)

    # State Rep districts, keyed the way the rest of the stack keys them
    cur.execute("""SELECT DISTINCT r.county, r.district, res.municipality
                   FROM results res JOIN races r ON r.id = res.race_id
                   JOIN elections e ON e.id = r.election_id
                   WHERE r.office_id = 7 AND e.year = 2024 AND e.election_type = 'general'""")
    for county, district, m in cur.fetchall():
        out["district"]["%s %s" % (county, district)].add(m)
        out["county"][county].add(m)

    for kind, office_id in (("senate", 6), ("exec", 5), ("congress", 3)):
        cur.execute("""SELECT DISTINCT r.district, res.municipality
                       FROM results res JOIN races r ON r.id = res.race_id
                       JOIN elections e ON e.id = r.election_id
                       WHERE r.office_id = ? AND e.year = 2024 AND e.election_type = 'general'
                         AND res.municipality NOT GLOB '[0-9]*'""", (office_id,))
        for district, m in cur.fetchall():
            out[kind][str(district)].add(m)
    return out


def main():
    started = time.time()
    conn = analysis.get_connection()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS state_pvi (
                     kind TEXT NOT NULL, key TEXT NOT NULL, pvi REAL, rating TEXT,
                     trend REAL, prior_pvi REAL, n_components INTEGER, components TEXT,
                     updated_at TEXT, PRIMARY KEY (kind, key))""")
    conn.commit()

    sets = municipality_sets(cur)
    rows, skipped = [], []
    for kind in KINDS:
        for key, munis in sorted(sets[kind].items()):
            val = analysis.state_pvi_for_municipalities(sorted(munis))
            if not val:
                skipped.append("%s/%s" % (kind, key))
                continue
            rows.append((kind, key, val["pvi"], val["rating"], val["trend"],
                         val["prior_pvi"], val["n_components"],
                         "; ".join("%s %+.2f" % (k, v) for k, v in sorted(val["components"].items())),
                         time.strftime("%Y-%m-%d %H:%M:%S")))
        print("  %-9s %d" % (kind, sum(1 for r in rows if r[0] == kind)))

    cur.execute("DELETE FROM state_pvi")
    cur.executemany("INSERT INTO state_pvi VALUES (?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    print("wrote %d rows in %.0fs" % (len(rows), time.time() - started))
    if skipped:
        print("no data for: %s" % ", ".join(skipped[:12]))

    if "--push" in sys.argv:
        import push_state_pvi
        push_state_pvi.main()


if __name__ == "__main__":
    main()
