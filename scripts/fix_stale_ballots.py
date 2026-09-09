#!/usr/bin/env python3
"""Clear a town's ballots-cast figure when its own results disprove it.

Towns send a machine tape first and a totalled return later, and the ballots
figure often comes from the earlier one. The votes then overtake it, and a
single-seat race publishes more votes than the town supposedly cast - which is
impossible, and which makes correct vote totals look like a counting error.

A ballots figure below the town's largest single-seat race is disproved by the
town's own arithmetic, so it goes. Absent is honest; wrong is not. The votes are
never touched: they came from a document, the ballots figure is bookkeeping.

    python3 scripts/fix_stale_ballots.py            # report
    python3 scripts/fix_stale_ballots.py --apply
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store   # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--elections", default="29,30")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    election_ids = [int(x) for x in args.elections.split(",")]
    qs = ",".join("?" * len(election_ids))
    conn = store.connect()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT v.rowid AS rid, v.municipality, v.election_id, v.ballots_cast,
               MAX(t.total) AS biggest
          FROM voter_registration v
          JOIN (SELECT r.municipality, ra.election_id, ra.id AS race_id,
                       SUM(r.votes) AS total
                  FROM results r
                  JOIN races ra ON ra.id = r.race_id
                 WHERE ra.election_id IN ({qs}) AND ra.seats = 1
                 GROUP BY r.municipality, ra.id) t
            ON t.municipality = v.municipality AND t.election_id = v.election_id
         WHERE v.election_id IN ({qs}) AND v.ballots_cast > 0
         GROUP BY v.rowid
        HAVING MAX(t.total) > v.ballots_cast
    """, (*election_ids, *election_ids))
    rows = [dict(r) for r in cur.fetchall()]

    for row in rows:
        print(f"  {row['municipality']:20s} el{row['election_id']}  "
              f"ballots {row['ballots_cast']:,} < largest race {row['biggest']:,}")
    print(f"{len(rows)} ballot figures disproved by their own town's results")

    if not args.apply:
        print("(report only - pass --apply to clear them)")
        return

    for row in rows:
        cur.execute("UPDATE voter_registration SET ballots_cast = NULL WHERE rowid = ?",
                    (row["rid"],))
    conn.commit()
    print(f"cleared {len(rows)}")


if __name__ == "__main__":
    main()
