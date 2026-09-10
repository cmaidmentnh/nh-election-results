#!/usr/bin/env python3
"""Take a name off a race it was never on, and delete its empty result rows.

Ben Baroody ran for Hillsborough 16 and our roster had him on the Hillsborough
39 ballot as well.  His 753 Ward 6 votes landed in 39, where he read as third
of three behind two people he never stood against - a winner shown as a loser.
Moving the votes is not enough: while he stays on that race's roster the board
still lists him, at zero.

Refuses to touch a candidate who still has votes in the race, because that is
never a roster error - it is a counting question, and deleting it would hide
real votes.

    python3 scripts/remove_ballot_candidate.py --race 5452 --candidate 23325 --apply
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store   # noqa: E402
from entry import log_audit  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--race", type=int, required=True)
    ap.add_argument("--candidate", type=int, required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = store.connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM candidates WHERE id = ?", (args.candidate,))
    row = cur.fetchone()
    if not row:
        print(f"No candidate {args.candidate}")
        return
    name = row["name"]

    cur.execute("""SELECT municipality, votes FROM results
                    WHERE race_id = ? AND candidate_id = ?""",
                (args.race, args.candidate))
    rows = cur.fetchall()
    live = [r for r in rows if (r["votes"] or 0) > 0]
    if live:
        print(f"REFUSING: {name} still has votes in race {args.race}: "
              + ", ".join(f"{r['municipality']} {r['votes']}" for r in live))
        print("Move or zero those first - a roster fix must not delete real votes.")
        return

    print(f"race {args.race}: remove {name} (candidate {args.candidate}) "
          f"and {len(rows)} empty result row(s)")
    if not args.apply:
        print("(report only - pass --apply to write it)")
        return

    bot = store.bot_user_id(conn)
    for r in rows:
        log_audit(cur, bot, args.race, r["municipality"], args.candidate,
                  "delete", {"votes": r["votes"]}, None)
    cur.execute("DELETE FROM results WHERE race_id = ? AND candidate_id = ?",
                (args.race, args.candidate))
    cur.execute("DELETE FROM race_candidates WHERE race_id = ? AND candidate_id = ?",
                (args.race, args.candidate))
    conn.commit()
    print("removed")


if __name__ == "__main__":
    main()
