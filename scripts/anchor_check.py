#!/usr/bin/env python3
"""Flag towns whose down-ballot races are far short of their own top of ticket.

Derry runs five tabulators and reports as one municipality. Its statewide races
carried town-wide numbers while every down-ballot race held a single tabulator,
so the town looked fully reported: it had results, it passed every completeness
check, and no total exceeded any ballot count. The State Rep race published the
wrong nominee for hours - the eleventh-place candidate shown elected tenth,
because one tabulator in five was being read as the whole town.

Nothing about a single row reveals that. What reveals it is the ratio a town
holds against ITSELF: voters who show up for governor mostly keep voting down
the ballot, so a town's best single-seat down-ballot race normally lands between
0.55 and 0.95 of its governor or senate total. Derry sat at 0.30.

This is a smell test, not a verdict. A low ratio means read the source document.

    python3 scripts/anchor_check.py
    python3 scripts/anchor_check.py --threshold 0.5
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store   # noqa: E402

TOP = ("Governor", "United States Senator", "Representative in Congress")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--elections", default="29,30")
    ap.add_argument("--threshold", type=float, default=0.45,
                    help="ratio below which a town is reported (default 0.45)")
    args = ap.parse_args()

    election_ids = [int(x) for x in args.elections.split(",")]
    qs = ",".join("?" * len(election_ids))
    conn = store.connect()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT r.municipality, ra.election_id, o.name AS office, ra.seats,
               SUM(r.votes) AS total
          FROM results r
          JOIN races ra ON ra.id = r.race_id
          JOIN offices o ON o.id = ra.office_id
         WHERE ra.election_id IN ({qs})
         GROUP BY r.municipality, ra.id
    """, election_ids)

    top, down = {}, {}
    for row in cur.fetchall():
        key = (row["municipality"], row["election_id"])
        per_seat = (row["total"] or 0) / max(1, row["seats"] or 1)
        if row["office"] in TOP:
            top[key] = max(top.get(key, 0), row["total"] or 0)
        else:
            down[key] = max(down.get(key, 0), per_seat)

    flagged = []
    for key, ceiling in top.items():
        if ceiling < 50:            # too small for the ratio to mean anything
            continue
        best = down.get(key, 0)
        ratio = best / ceiling if ceiling else 0
        if ratio < args.threshold:
            flagged.append((ratio, key[0], key[1], round(best), ceiling))

    flagged.sort()
    for ratio, town, eid, best, ceiling in flagged:
        party = "R" if eid == 29 else "D"
        print(f"  {ratio:.2f}  {town:22s} {party}  best down-ballot {best:>6,} "
              f"vs top of ticket {ceiling:>6,}")
    print(f"{len(flagged)} town/party pairs below {args.threshold:.2f} "
          f"- read their source documents")


if __name__ == "__main__":
    main()
