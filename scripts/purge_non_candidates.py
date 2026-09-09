#!/usr/bin/env python3
"""Remove result rows recorded against things that are not people.

A return of votes prints more than candidates: "Blanks", "(Not on Ballot)",
"no vote", "NONE". Those are the sheet's bookkeeping - undervotes and empty
lines - and recording them as candidates adds phantom votes to a race, which is
how several towns came to publish more votes than they had ballots.

The match is deliberately narrow and checked against a real roster entry before
anything is removed: "Dennis Blankenbeker" is a State Senate candidate with two
thousand votes and contains the word "blank".

    python3 scripts/purge_non_candidates.py            # report
    python3 scripts/purge_non_candidates.py --apply
"""
import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store            # noqa: E402
from entry import log_audit         # noqa: E402

# Whole-string patterns only. A name that merely contains one of these words is
# a person until proven otherwise.
NOT_A_PERSON = re.compile(
    r"^\s*(?:\(?\s*not\s+on\s+(?:the\s+)?ballot\s*\)?"
    r"|blanks?|no\s*vote|no\s*name|none|n/?a|under\s*votes?|over\s*votes?"
    r"|total(?:\s+votes?(?:\s+cast)?)?|ballots?\s+cast|scattering|-{1,3})\s*$",
    re.IGNORECASE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--elections", default="29,30")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    election_ids = [int(x) for x in args.elections.split(",")]
    qs = ",".join("?" * len(election_ids))
    conn = store.connect()
    cur = conn.cursor()

    cur.execute(f"""SELECT r.rowid AS rid, r.race_id, r.candidate_id, r.municipality,
                           r.votes, c.name
                      FROM results r
                      JOIN races ra ON ra.id = r.race_id
                      JOIN candidates c ON c.id = r.candidate_id
                     WHERE ra.election_id IN ({qs})""", election_ids)
    doomed = [dict(r) for r in cur.fetchall() if NOT_A_PERSON.match(r["name"] or "")]

    by_name = {}
    for row in doomed:
        entry = by_name.setdefault(row["name"], [0, 0])
        entry[0] += 1
        entry[1] += row["votes"] or 0
    for name, (n, v) in sorted(by_name.items(), key=lambda kv: -kv[1][1]):
        print(f"  {name:24s} {n:4d} rows  {v:6,} phantom votes")
    print(f"{len(doomed)} rows, {sum(r['votes'] or 0 for r in doomed):,} phantom votes")

    if not args.apply:
        print("(report only - pass --apply to remove them)")
        return

    bot = store.bot_user_id(conn)
    for row in doomed:
        cur.execute("DELETE FROM results WHERE rowid = ?", (row["rid"],))
        log_audit(cur, bot, row["race_id"], row["municipality"], row["candidate_id"],
                  "delete", {"votes": row["votes"]}, None)
    cur.execute("""DELETE FROM race_candidates
                    WHERE candidate_id IN (SELECT id FROM candidates)
                      AND candidate_id NOT IN (SELECT candidate_id FROM results)
                      AND recruitment_filing_id = -1
                      AND candidate_id IN ({})""".format(
        ",".join(str(r["candidate_id"]) for r in doomed) or "NULL"))
    conn.commit()
    print(f"removed {len(doomed)} rows")


if __name__ == "__main__":
    main()
