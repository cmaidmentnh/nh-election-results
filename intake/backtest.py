"""Back-test the parser against real clerk reports whose answers we already know.

The 2024 general results are in the database, and the clerk emails that reported
them are in the mailbox. Feeding those PDFs and photos back through the parser
and diffing against the stored numbers measures real accuracy - the thing that
decides how much lands in the review queue on election night.

2024 races carry no race_candidates roster (only 2026 does), so the roster here
is rebuilt from the results themselves. That mirrors election night, where the
roster comes from filings: the candidate list is given, and only the numbers
have to be read off the page.

    python -m intake.backtest /path/to/dir   # dir/<Town>/<attachments>
"""

import sys
from pathlib import Path

from intake import parser, store
from intake.apply import reconcile

YEAR, ETYPE = 2024, "general"


def town_roster(cur, municipality):
    """Races and candidates for one town, rebuilt from the stored results."""
    cur.execute(
        """SELECT r.id AS race_id, o.name AS office, COALESCE(r.county,'') AS county,
                  COALESCE(r.district,'') AS district, r.seats,
                  c.id AS candidate_id, c.name, COALESCE(c.party,'') AS party,
                  res.votes
             FROM results res
             JOIN races r      ON res.race_id = r.id
             JOIN offices o    ON r.office_id = o.id
             JOIN elections e  ON r.election_id = e.id
             JOIN candidates c ON res.candidate_id = c.id
            WHERE e.year = ? AND e.election_type = ? AND res.municipality = ?
            ORDER BY o.name, r.district, c.name""",
        (YEAR, ETYPE, municipality),
    )
    races, truth = {}, {}
    for row in cur.fetchall():
        r = races.setdefault(row["race_id"], {
            "office": row["office"], "county": row["county"],
            "district": row["district"], "seats": row["seats"] or 1, "cands": [],
        })
        r["cands"].append((row["candidate_id"], row["name"], row["party"]))
        truth[(row["race_id"], row["candidate_id"])] = row["votes"]

    lines = ["=== GENERAL ELECTION BALLOT (election_id=16) ==="]
    index = {}
    for rid, r in races.items():
        label = f"{r['office']}" + (f" - {r['county']} {r['district']}".rstrip()
                                    if r["county"] or r["district"] else "")
        lines.append(f"[race_id={rid}] {label} ({r['seats']} seat(s))")
        for cid, name, party in r["cands"]:
            lines.append(f"    candidate_id={cid}  {name}" + (f" ({party})" if party else ""))
        index[rid] = {"label": label, "office": r["office"],
                      "names": {cid: n for cid, n, _ in r["cands"]}}
    return "\n".join(lines), index, truth


def main():
    root = Path(sys.argv[1])
    conn = store.connect()
    cur = conn.cursor()

    totals = {"exact": 0, "wrong": 0, "missed": 0, "spurious": 0,
              "caught": 0, "slipped": 0, "held_ok": 0}
    for town_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        town = town_dir.name
        atts = sorted(str(p) for p in town_dir.iterdir() if p.is_file())
        roster_text, index, truth = town_roster(cur, town)
        if not truth:
            print(f"{town:14s} SKIP - no 2024 results on file under that name")
            continue

        try:
            ex, agreed, disagreements = parser.extract_consensus(
                town, roster_text, "", f"{town} results", "clerk@town.nh.gov", atts)
        except Exception as exc:
            print(f"{town:14s} ERROR {exc}")
            continue

        got = {}
        spurious = 0
        for l in ex.lines:
            if l.race_id and l.candidate_id and (l.race_id, l.candidate_id) in truth:
                got[(l.race_id, l.candidate_id)] = l.votes
            else:
                spurious += 1

        exact = sum(1 for k, v in got.items() if truth[k] == v)
        wrong = [(k, got[k], truth[k]) for k in got if truth[k] != got[k]]
        missed = len(truth) - len(got)

        # Would either gate have stopped the wrong ones publishing?
        held_races = reconcile(ex, index)
        caught = [w for w in wrong if w[0] not in agreed or w[0][0] in held_races]
        slipped = [w for w in wrong if w[0] in agreed and w[0][0] not in held_races]
        # ...and what does it cost in correct values held back?
        held_ok = sum(1 for k, v in got.items()
                      if truth[k] == v and (k not in agreed or k[0] in held_races))

        totals["exact"] += exact
        totals["wrong"] += len(wrong)
        totals["missed"] += max(0, missed)
        totals["spurious"] += spurious
        totals["caught"] += len(caught)
        totals["slipped"] += len(slipped)
        totals["held_ok"] += held_ok

        pct = 100 * exact / len(truth) if truth else 0
        print(f"{town:14s} {exact:4d}/{len(truth):4d} exact ({pct:5.1f}%)  "
              f"wrong={len(wrong):<3} caught={len(caught):<3} SLIPPED={len(slipped):<3} "
              f"good_held={held_ok:<3} missed={max(0,missed):<4} unreconciled_races={len(held_races)}")
        for (rid, cid), g, t in slipped[:4]:
            print(f"                 SLIPPED {index[rid]['label']} / "
                  f"{index[rid]['names'][cid]}: read {g}, actual {t}")

    print("\nTOTAL", totals)


if __name__ == "__main__":
    main()
