#!/usr/bin/env python3
"""Publish the held lines of a town's totalled return over its earlier tapes.

Towns report twice: machine tapes as each one finishes, then the clerk's
totalled Return of Votes for the whole town. The second is authoritative - it is
the number the town certifies - so when the parser recognises one it queues the
line as "town total supersedes the N recorded" and waits. Nothing was applying
those, so a town that did the right thing and sent its final sheet stayed on its
first partial tape: North Hampton sat on zeros with 137 real lines queued behind
them.

Only items the parser itself marked as a town total are touched, and only where
that flag is in the reason it recorded at parse time.

    python3 scripts/apply_town_totals.py            # report
    python3 scripts/apply_town_totals.py --apply
"""
import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store                    # noqa: E402
from intake.apply import _existing_votes    # noqa: E402
from entry import log_audit                 # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = store.connect()
    cur = conn.cursor()
    cur.execute("""SELECT * FROM intake_items
                    WHERE status = 'pending'
                      AND reason LIKE '%town total supersedes%'
                      AND race_id IS NOT NULL AND candidate_id IS NOT NULL
                      AND votes IS NOT NULL AND municipality IS NOT NULL
                    ORDER BY municipality, race_id, id""")
    rows = [dict(r) for r in cur.fetchall()]

    # A sheet read more than once produces the same line more than once. Take
    # the first and retire the rest rather than writing a number twice.
    seen, ready, dupes = set(), [], []
    for item in rows:
        key = (item["race_id"], item["candidate_id"], item["municipality"])
        (dupes if key in seen else ready).append(item)
        seen.add(key)

    for town, n in Counter(i["municipality"] for i in ready).most_common():
        print(f"  {town:20s} {n:4d}")
    print(f"{len(ready)} town-total lines to publish"
          f"{f', {len(dupes)} duplicates to retire' if dupes else ''}")

    if not args.apply:
        print("(report only - pass --apply to publish them)")
        return

    bot = store.bot_user_id(conn)
    for item in ready:
        old = _existing_votes(cur, item["race_id"], item["candidate_id"],
                              item["municipality"])
        if old is None:
            cur.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                           VALUES (?,?,?,?)""",
                        (item["race_id"], item["candidate_id"],
                         item["municipality"], item["votes"]))
            log_audit(cur, bot, item["race_id"], item["municipality"],
                      item["candidate_id"], "create", None, {"votes": item["votes"]})
        elif old != item["votes"]:
            cur.execute("""UPDATE results SET votes = ?
                            WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                        (item["votes"], item["race_id"], item["candidate_id"],
                         item["municipality"]))
            log_audit(cur, bot, item["race_id"], item["municipality"],
                      item["candidate_id"], "update", {"votes": old},
                      {"votes": item["votes"]})
        cur.execute("""UPDATE intake_items SET status = 'applied',
                              applied_at = CURRENT_TIMESTAMP WHERE id = ?""",
                    (item["id"],))
    for item in dupes:
        cur.execute("""UPDATE intake_items SET status = 'superseded',
                              reason = 'same town-total line read more than once'
                        WHERE id = ?""", (item["id"],))
    conn.commit()
    print(f"published {len(ready)}")


if __name__ == "__main__":
    main()
