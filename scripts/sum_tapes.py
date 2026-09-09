#!/usr/bin/env python3
"""Rebuild one town's results from a complete set of machine tapes.

A town with several machines prints one tape per machine, and the town's result
is their sum. Feeding all of them to the extractor as a single report overflows
the output ceiling, so read each tape on its own - in parallel - and add them up.

The town's existing rows are replaced, not added to, because partial reports of
the same machines are usually already on file and adding would double-count.
Use this only when the tapes given are the town's complete set.

    python3 scripts/sum_tapes.py Goffstown intake_attachments/manual/IMG_142*.jpg
"""
import argparse
import collections
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store, roster, parser   # noqa: E402
from entry import log_audit                # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("town")
    ap.add_argument("tapes", nargs="+")
    ap.add_argument("--elections", default="29,30")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    election_ids = [int(x) for x in args.elections.split(",")]
    conn = store.connect()
    cur = conn.cursor()
    roster_text, index, _elections = roster.roster_for(cur, args.town)

    def read(path):
        extraction, _agreed, _split = parser.extract_consensus(
            args.town, roster_text, f"{args.town} machine tape",
            "", "operator", attachments=[path])
        return path, extraction

    totals = collections.defaultdict(int)
    with ThreadPoolExecutor(max_workers=len(args.tapes)) as pool:
        for path, extraction in pool.map(read, args.tapes):
            kept = 0
            for line in extraction.lines:
                if not (line.race_id and line.candidate_id and line.votes is not None):
                    continue
                race = index.get(line.race_id)
                if race and line.candidate_id in race["candidate_ids"]:
                    totals[(line.race_id, line.candidate_id)] += line.votes
                    kept += 1
            print(f"  {Path(path).name}: {kept} lines")

    print(f"{len(totals)} race/candidate totals from {len(args.tapes)} tapes")
    if not args.apply:
        print("(report only - pass --apply to write them)")
        return

    qs = ",".join("?" * len(election_ids))
    cur.execute(f"""DELETE FROM results WHERE municipality = ?
                     AND race_id IN (SELECT id FROM races WHERE election_id IN ({qs}))""",
                (args.town, *election_ids))
    print(f"replaced {cur.rowcount} existing rows")

    bot = store.bot_user_id(conn)
    for (race_id, candidate_id), votes in totals.items():
        cur.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                       VALUES (?,?,?,?)""", (race_id, candidate_id, args.town, votes))
        log_audit(cur, bot, race_id, args.town, candidate_id, "create", None,
                  {"votes": votes})
    cur.execute("""UPDATE intake_items SET status = 'superseded'
                    WHERE municipality = ? AND status = 'pending'""", (args.town,))
    conn.commit()
    print(f"wrote {len(totals)} summed totals for {args.town}")


if __name__ == "__main__":
    main()
