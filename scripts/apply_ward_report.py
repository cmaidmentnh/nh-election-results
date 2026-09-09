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

# The clerk's headings, mapped to offices.name.  Order matters: the longest
# distinctive phrase is tested first so "COUNTY TREASURER" is not swallowed by
# a looser rule, and the two convention rows are told apart from state rep.
OFFICE_LABELS = [
    ("DELEGATE", "Delegate to the State Convention"),
    ("SHERRIFF", "County Sheriff"),          # Lebanon spells it with two Rs
    ("SENTATOR", "United States Senator"),   # and drops a letter here
    ("STATE REPRESENTATIVE", "State Representative"),
    ("STATE SENATOR", "State Senator"),
    ("US SENATOR", "United States Senator"),
    ("UNITED STATES SENATOR", "United States Senator"),
    ("REPRESENTATIVE IN CONGRESS", "Representative in Congress"),
    ("GOVERNOR", "Governor"),
    ("EXECUTIVE COUNCILOR", "Executive Councilor"),
    ("SHERIFF", "County Sheriff"),
    ("COUNTY ATTORNEY", "County Attorney"),
    ("COUNTY TREASURER", "County Treasurer"),
    ("COUNTY COMMISSIONER", "County Commissioner"),
    ("REGISTER OF DEEDS", "Register of Deeds"),
    ("REGISTER OF PROBATE", "Register of Probate"),
]


def office_name(label):
    up = label.upper()
    for needle, name in OFFICE_LABELS:
        if needle in up:
            return name
    return None
SUFFIXES = {"JR", "SR", "II", "III", "IV"}


def norm(name):
    n = re.sub(r"[^A-Za-z ]", " ", name.upper())
    parts = [p for p in n.split() if p and p not in SUFFIXES]
    return " ".join(parts)


def surname(name):
    parts = norm(name).split()
    return parts[-1] if parts else ""


def district_of(label):
    """The trailing number in "STATE REPRESENTATIVES GRAFTON DISTRICT 17"."""
    m = re.search(r"DISTRICT\s+(\d+)\s*$", label.upper())
    return m.group(1) if m else None


def race_by_office(cur, election_id, municipality, office, district):
    """Find the race the long way, for lines no candidate name can anchor.

    A write-in-only race has nobody on the ballot to match against, and
    Lebanon files several of them - "State Representative District 13,
    Write-Ins* 8" is the whole race.  Those votes are real and belong to
    somebody, so the race is found through the ward's own district table.
    """
    cur.execute("""
        SELECT DISTINCT ra.id
          FROM municipality_districts md
          JOIN races ra ON ra.office_id = md.office_id
                       AND (ra.county IS NULL OR ra.county = '' OR ra.county = md.county)
          JOIN offices o ON o.id = ra.office_id
         WHERE md.municipality = ? AND ra.election_id = ? AND o.name = ?
           AND (? IS NULL OR IFNULL(ra.district,'') = ?)
           AND IFNULL(ra.district,'') = IFNULL(md.district,'')
    """, (municipality, election_id, office, district, district or ""))
    hits = [r["id"] for r in cur.fetchall()]
    return hits[0] if len(hits) == 1 else None


def writein_id(cur):
    """The shared aggregate write-in candidate.

    Its normalised name is 'WRITEIN', with no space - punctuation is stripped
    rather than replaced.  Looking for 'WRITE IN' finds nothing, and the first
    version of this script then dropped every write-in line without a word,
    which is how Campton's returns went in missing all of theirs.
    """
    cur.execute("""SELECT id FROM candidates
                    WHERE name_normalized IN ('WRITEIN', 'WRITE IN', 'WRITE-IN')
                    ORDER BY (name_normalized = 'WRITEIN') DESC, id LIMIT 1""")
    row = cur.fetchone()
    if row is None:
        raise SystemExit("No aggregate write-in candidate on file - refusing "
                         "to run, because every write-in line would be lost.")
    return row["id"]


def load(path):
    races = defaultdict(list)
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        ward, party, office, cand, votes = line.split("\t")
        races[(int(ward), party, office)].append((cand, int(votes)))
    return races


def candidates_for(cur, election_id, municipality):
    """Every candidate this ward could legitimately vote for, with its race.

    Two sources, because municipality_districts only knows the districted
    offices.  Governor and U.S. Senator have no district and no row there, so
    they are read straight off the races table - there is only one of each.
    """
    cur.execute("""
        SELECT rc.candidate_id, c.name, rc.race_id, o.name AS office,
               IFNULL(ra.district,'') AS district
          FROM municipality_districts md
          JOIN races ra ON ra.office_id = md.office_id
                       AND IFNULL(ra.district,'') = IFNULL(md.district,'')
                       AND (ra.county IS NULL OR ra.county = '' OR ra.county = md.county)
          JOIN offices o ON o.id = ra.office_id
          JOIN race_candidates rc ON rc.race_id = ra.id
          JOIN candidates c ON c.id = rc.candidate_id
         WHERE md.municipality = ? AND ra.election_id = ?
        UNION
        SELECT rc.candidate_id, c.name, rc.race_id, o.name AS office,
               IFNULL(ra.district,'') AS district
          FROM races ra
          JOIN offices o ON o.id = ra.office_id
          JOIN race_candidates rc ON rc.race_id = ra.id
          JOIN candidates c ON c.id = rc.candidate_id
         WHERE ra.election_id = ?
           AND o.name IN ('Governor', 'United States Senator')
    """, (municipality, election_id, election_id))
    by_name = defaultdict(set)
    by_surname = defaultdict(set)
    for row in cur.fetchall():
        entry = (row["race_id"], row["candidate_id"], row["office"],
                 row["district"])
        by_name[norm(row["name"])].add(entry)
        by_surname[surname(row["name"])].add(entry)
    return by_name, by_surname


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--city", required=True,
                    help="e.g. Manchester; with ward 0 rows, a plain town name")
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
        # Ward 0 means the town does not have wards - Campton faxed a single
        # Return of Votes for the whole town, and the same reader should take it.
        municipality = args.city if ward == 0 else f"{args.city} Ward {ward}"
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

        want_office = office_name(office)
        want_district = district_of(office)
        wildcard = writein_id(cur)
        resolved = []
        deferred = []          # write-in lines, placed once the race is known
        for name, votes in cands:
            if norm(name) == "WRITE IN":
                deferred.append(votes)
                continue
            # Carlos Gonzalez is on the ballot twice in ward 40 - once for the
            # House and once as a convention delegate - so the clerk's heading
            # decides.  Search inside that office FIRST: Amherst's delegate
            # roster spells her "Diane H. Layton" while the House roster says
            # "Diane Layton", so an exact match across all offices lands on the
            # House and drags the whole delegate race onto the wrong ballot.
            exact, sur = norm(name), surname(name)
            hits = set()
            if want_office:
                for pool in (by_name.get(exact), by_surname.get(sur)):
                    found = {h for h in (pool or set()) if h[2] == want_office}
                    # Jennie Gomarlo is on two of Richmond's ballots - Cheshire
                    # 10 and Cheshire 17 - so the office alone cannot say which
                    # race a figure belongs to.  The heading's district can.
                    if want_district:
                        narrowed = {h for h in found if h[3] == want_district}
                        if narrowed:
                            found = narrowed
                    if found:
                        hits = found
                        break
            if not hits:
                hits = by_name.get(exact) or by_surname.get(sur) or set()
            if len(hits) != 1:
                unmatched.append((municipality, party, office, name, votes,
                                  f"{len(hits)} matches"))
                resolved = None
                break
            race_id, cand_id, _office, _district = next(iter(hits))
            resolved.append(((race_id, cand_id), name, votes))
        if resolved is None:
            continue
        # every figure in a race lands together or none of it does
        race_ids = {r for (r, _c), _n, _v in resolved}
        if len(race_ids) > 1:
            skipped_race.append((municipality, party, office,
                                 f"candidates span races {sorted(race_ids)}"))
            continue
        race_id = next(iter(race_ids)) if race_ids else None
        if race_id is None and want_office:
            race_id = race_by_office(cur, election_id, municipality,
                                     want_office, want_district)
        if race_id is None:
            if deferred:
                skipped_race.append((municipality, party, office,
                                     "write-in only, and the race is not "
                                     "on this ward's district list"))
            continue
        for votes in deferred:
            if wildcard is None:
                break
            resolved.append(((race_id, wildcard), "Write-in", votes))

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
