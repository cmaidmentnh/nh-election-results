#!/usr/bin/env python3
"""Put a name on a race's roster when the clerk's printed ballot has one we don't.

Hillsborough 39 is Manchester wards 6, 8 and 9 and nothing else.  The city's
ward summary and its floterial sheet both show Jon DiPietro on that ballot with
1,200 votes; our roster carries a Jonathan Morton in that slot who received no
votes anywhere in the state.  The clerk's ballot is what voters actually saw.

This only adds - it never removes the name already on file, because deciding
that a filed candidate does not exist is not a job for a script at 4am.

    python3 scripts/add_ballot_candidate.py --race 5801 --name 'Jon DiPietro' \
            --party Republican --apply
"""
import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store   # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--race", type=int, required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--party")
    ap.add_argument("--writein", action="store_true",
                    help="a write-in the certified sheet names, not a ballot candidate")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    normalized = re.sub(r"[^A-Za-z ]", " ", args.name.upper())
    normalized = " ".join(normalized.split())

    conn = store.connect()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM candidates WHERE name_normalized = ?",
                (normalized,))
    existing = cur.fetchall()
    cand_id = existing[0]["id"] if existing else None
    print(f"race {args.race}: {args.name} -> "
          f"{'candidate ' + str(cand_id) if cand_id else 'new candidate'}")

    if not args.apply:
        print("(report only - pass --apply to write it)")
        return

    if cand_id is None:
        cur.execute("""INSERT INTO candidates (name, name_normalized, party)
                       VALUES (?,?,?)""", (args.name, normalized, args.party))
        cand_id = cur.lastrowid
    # -2 marks a write-in the Secretary of State itemised: a real result under
    # a real name, but not somebody who was printed on the ballot. It shows on
    # the page as a candidate, labelled, where -1 (a name the old feed recorded
    # beside an aggregate figure) is folded into the write-in line.
    filing = -2 if args.writein else 0
    cur.execute("""INSERT OR IGNORE INTO race_candidates
                       (race_id, candidate_id, party, recruitment_filing_id)
                   VALUES (?,?,?,?)""", (args.race, cand_id, args.party, filing))
    conn.commit()
    print(f"candidate id {cand_id} on race {args.race}")


if __name__ == "__main__":
    main()
