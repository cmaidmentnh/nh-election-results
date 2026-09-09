#!/usr/bin/env python3
"""Write one operator-verified result, the way the review page would.

Some numbers cannot be recovered by re-reading the sheet, because the sheet
itself is the problem: Manchester Ward 10's county commissioner tape has its
"Write-In 6" line cut off at the right edge of the photo, so the race reads 379
against a printed 385 and fails reconciliation by six votes every time it is
parsed. A person reads the tape, sees the total, and enters the missing line.

Every write is logged to result_audit under the bot user, so a correction is as
traceable as anything the parser did.

    python3 scripts/apply_correction.py --race 5674 --town 'Manchester Ward 10' \
            --candidate 12345 --votes 379 --apply
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store                    # noqa: E402
from intake.apply import _existing_votes    # noqa: E402
from entry import log_audit                 # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--race", type=int, required=True)
    ap.add_argument("--town", required=True)
    ap.add_argument("--candidate", type=int, required=True)
    ap.add_argument("--votes", type=int, required=True)
    ap.add_argument("--item", type=int, help="intake_items id to mark applied")
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
    old = _existing_votes(cur, args.race, args.candidate, args.town)
    print(f"{args.town} race {args.race}: {name} "
          f"{old if old is not None else '(none)'} -> {args.votes}")

    if not args.apply:
        print("(report only - pass --apply to write it)")
        return

    bot = store.bot_user_id(conn)
    if old is None:
        cur.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                       VALUES (?,?,?,?)""",
                    (args.race, args.candidate, args.town, args.votes))
        log_audit(cur, bot, args.race, args.town, args.candidate,
                  "create", None, {"votes": args.votes})
    else:
        cur.execute("""UPDATE results SET votes = ?
                        WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                    (args.votes, args.race, args.candidate, args.town))
        log_audit(cur, bot, args.race, args.town, args.candidate,
                  "update", {"votes": old}, {"votes": args.votes})
    if args.item:
        cur.execute("""UPDATE intake_items SET status = 'applied',
                              applied_at = CURRENT_TIMESTAMP WHERE id = ?""",
                    (args.item,))
    conn.commit()
    print("written")


if __name__ == "__main__":
    main()
