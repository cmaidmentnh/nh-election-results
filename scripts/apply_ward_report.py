#!/usr/bin/env python3
"""Apply a city clerk's ward-by-ward summary, one tab-separated line per figure.

Manchester reports all twelve wards in a single PDF, and the intake pipeline
threw the whole thing away with "town unresolved" because the message names no
one town.  This reads the clerk's own table instead.

Input is  ward <TAB> party <TAB> office <TAB> candidate <TAB> votes,
with a "__TOTAL__" candidate on each race carrying the clerk's printed subtotal.
Those totals are the check: a race whose candidate lines do not sum to the
printed figure is never written.

A race is found through its candidates, not through its office name.  Ten
counties run a "County Attorney" race and the races table does not always carry
the county, so matching on the office would pick one of ten at random; matching
on the person is unambiguous.

    python3 scripts/apply_ward_report.py --file wards.tsv --city Manchester --apply
"""
import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store                    # noqa: E402
from intake.apply import _existing_votes    # noqa: E402
from entry import log_audit                 # noqa: E402

PARTY_ELECTION = {"DEMOCRATIC": 30, "REPUBLICAN": 29}
SUFFIXES = {"JR", "SR", "II", "III", "IV"}


def norm(name):
    n = re.sub(r"[^A-Za-z ]", " ", name.upper())
    parts = [p for p in n.split() if p and p not in SUFFIXES]
    return " ".join(parts)


def surname(name):
    parts = norm(name).split()
    return parts[-1] if parts else ""


def load(path):
    races = defaultdict(list)
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        ward, party, office, cand, votes = line.split("\t")
        races[(int(ward), party, office)].append((cand, int(votes)))
    return races


def candidates_for(cur, election_id, municipality):
    """Every candidate this ward could legitimately vote for, with its race."""
    cur.execute("""
        SELECT rc.candidate_id, c.name, rc.race_id
          FROM municipality_districts md
          JOIN races ra ON ra.office_id = md.office_id
                       AND IFNULL(ra.district,'') = IFNULL(md.district,'')
                       AND (ra.county IS NULL OR ra.county = '' OR ra.county = md.county)
          JOIN race_candidates rc ON rc.race_id = ra.id
          JOIN candidates c ON c.id = rc.candidate_id
         WHERE md.municipality = ? AND ra.election_id = ?
    """, (municipality, election_id))
    by_name = defaultdict(set)
    by_surname = defaultdict(set)
    for row in cur.fetchall():
        pair = (row["race_id"], row["candidate_id"])
        by_name[norm(row["name"])].add(pair)
        by_surname[surname(row["name"])].add(pair)
    return by_name, by_surname


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--city", required=True, help="e.g. Manchester")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = store.connect()
    cur = conn.cursor()
    bot = store.bot_user_id(conn)
    cache = {}

    written = created = unchanged = 0
    skipped_race = []
    unmatched = []

    for (ward, party, office), lines in sorted(load(args.file).items()):
        election_id = PARTY_ELECTION[party]
        municipality = f"{args.city} Ward {ward}"
        printed = [v for n, v in lines if n == "__TOTAL__"]
        cands = [(n, v) for n, v in lines if n != "__TOTAL__"]
        if not cands:
            continue
        total = sum(v for _n, v in cands)
        if printed and total != printed[-1] and total != sum(printed):
            skipped_race.append((municipality, party, office,
                                 f"sums to {total}, clerk printed {printed}"))
            continue

        key = (election_id, municipality)
        if key not in cache:
            cache[key] = candidates_for(cur, election_id, municipality)
        by_name, by_surname = cache[key]

        resolved = []
        for name, votes in cands:
            hits = by_name.get(norm(name)) or by_surname.get(surname(name)) or set()
            if len(hits) != 1:
                unmatched.append((municipality, party, office, name, votes,
                                  f"{len(hits)} matches"))
                resolved = None
                break
            resolved.append((next(iter(hits)), name, votes))
        if resolved is None:
            continue
        # every figure in a race lands together or none of it does
        race_ids = {r for (r, _c), _n, _v in resolved}
        if len(race_ids) != 1:
            skipped_race.append((municipality, party, office,
                                 f"candidates span races {sorted(race_ids)}"))
            continue

        for (race_id, cand_id), name, votes in resolved:
            old = _existing_votes(cur, race_id, cand_id, municipality)
            if old == votes:
                unchanged += 1
                continue
            print(f"{municipality} {party[:1]} {office}: {name} "
                  f"{old if old is not None else '(none)'} -> {votes}")
            if not args.apply:
                continue
            if old is None:
                cur.execute("""INSERT INTO results (race_id, candidate_id,
                                    municipality, votes) VALUES (?,?,?,?)""",
                            (race_id, cand_id, municipality, votes))
                log_audit(cur, bot, race_id, municipality, cand_id,
                          "create", None, {"votes": votes})
                created += 1
            else:
                cur.execute("""UPDATE results SET votes = ? WHERE race_id = ?
                                AND candidate_id = ? AND municipality = ?""",
                            (votes, race_id, cand_id, municipality))
                log_audit(cur, bot, race_id, municipality, cand_id,
                          "update", {"votes": old}, {"votes": votes})
                written += 1

    if args.apply:
        conn.commit()

    print(f"\n{created} created, {written} updated, {unchanged} already correct")
    if skipped_race:
        print(f"\n{len(skipped_race)} races held back:")
        for row in skipped_race:
            print("  " + " | ".join(str(x) for x in row))
    if unmatched:
        print(f"\n{len(unmatched)} names not resolved (their whole race is held):")
        for row in unmatched:
            print("  " + " | ".join(str(x) for x in row))


if __name__ == "__main__":
    main()
