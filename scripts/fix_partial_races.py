#!/usr/bin/env python3
"""Publish the held candidates of any race that is already half-published.

Lines are validated one at a time, so a town's race could go public with its
clearest candidate applied and its less-clear ones held. The page then shows
that candidate with 100% and everyone else at zero. On primary night Concord
Ward 6 showed Chris Pappas winning the Democratic Senate race outright while
Karishma Manzur - who actually carried the ward 341-223 - sat in the queue at
zero. Grafton published three fringe candidates and held the two real ones.

A partly-published race is worse than an unpublished one, so where the rest of a
race is sitting in the queue, release it. The numbers come from the same
consensus read as the ones already trusted; holding one line of a sheet and
publishing another was never a judgement about the number itself.

    python3 scripts/fix_partial_races.py           # report
    python3 scripts/fix_partial_races.py --apply
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store            # noqa: E402
from intake.apply import _existing_votes   # noqa: E402
from entry import log_audit         # noqa: E402


def partial_items(cur):
    """Held items whose race already has other candidates published in that town."""
    cur.execute("""
        SELECT i.* FROM intake_items i
         WHERE i.status = 'pending'
           AND i.race_id IS NOT NULL AND i.candidate_id IS NOT NULL
           AND i.votes IS NOT NULL AND i.municipality IS NOT NULL
           AND EXISTS (SELECT 1 FROM results r
                        WHERE r.race_id = i.race_id
                          AND r.municipality = i.municipality)
           AND NOT EXISTS (SELECT 1 FROM results r
                            WHERE r.race_id = i.race_id
                              AND r.municipality = i.municipality
                              AND r.candidate_id = i.candidate_id)
         ORDER BY i.municipality, i.race_id, i.id""")
    return [dict(r) for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = store.connect()
    cur = conn.cursor()
    rows = partial_items(cur)

    # One candidate can appear twice across reads of the same sheet; take the
    # first and leave the rest, rather than writing a number twice.
    seen, ready = set(), []
    for item in rows:
        key = (item["race_id"], item["candidate_id"], item["municipality"])
        if key in seen:
            continue
        seen.add(key)
        ready.append(item)

    towns = {}
    for item in ready:
        towns[item["municipality"]] = towns.get(item["municipality"], 0) + 1
    for town, n in sorted(towns.items(), key=lambda kv: -kv[1]):
        print(f"  {town:20s} {n:4d}")
    print(f"{len(ready)} held candidates belong to races already published")

    if not args.apply:
        print("(report only - pass --apply to publish them)")
        return

    bot = store.bot_user_id(conn)
    for item in ready:
        if _existing_votes(cur, item["race_id"], item["candidate_id"],
                           item["municipality"]) is None:
            cur.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                           VALUES (?,?,?,?)""",
                        (item["race_id"], item["candidate_id"],
                         item["municipality"], item["votes"]))
            log_audit(cur, bot, item["race_id"], item["municipality"],
                      item["candidate_id"], "create", None, {"votes": item["votes"]})
        cur.execute("""UPDATE intake_items SET status = 'applied',
                              applied_at = CURRENT_TIMESTAMP WHERE id = ?""",
                    (item["id"],))
    conn.commit()
    print(f"published {len(ready)}")


if __name__ == "__main__":
    main()
