#!/usr/bin/env python3
"""Build the 2026 GENERAL election ballot roster from the Secretary of State's
official candidate list (data/general_2026_candidates.json, produced from the
SoS PDF by scripts/parse_sos_candidate_list.py).

What it creates (idempotent - run it as often as you like):
  * elections: (2026, 'general', party NULL) - one ballot, like 2024 general
  * races: one per contest on the list, keyed (election, office, county, district).
    County/district are NULL where the office has none, as the 2026 primary rows are.
    Seats come from the 2026 primary race for the same contest (which came from
    the district data), falling back to the 2024 general; county offices are 1.
  * candidates: names exactly as printed on the SoS list. An existing row is
    reused only on an exact name + party match; otherwise a new row is created.
  * race_candidates: one row per person per contest, ballot_order = list order.

A person printed on more than one party line in the same contest (cross-filed,
e.g. DEM and REP for sheriff) is ONE roster row, as 2024 stored Emily Garod: one
candidate whose votes are combined. Their party is their 2026 primary filing
party when there is one, otherwise the first line printed. Every such case is
printed in the report.

County offices and County Commissioner races are created only where the list
has a candidate: which county seats are up varies by county, and the list is
the authority. (The 2026 primary also holds empty placeholder races for
contests nobody filed for - those are reported, not copied.)

Re-running after the list changes: roster rows in the general that are no longer
on the list are removed if they have no results and are not named write-ins
(recruitment_filing_id = -1). Anything else is left alone and reported.

Usage:
    python3 scripts/import_general_2026.py [--db nh_elections.db] [--data FILE] [--dry-run]
"""
import argparse
import collections
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

YEAR = 2026
CYCLE = "2022-2030"
PRIMARY_TYPE = "state_primary"
COUNTY_OFFICES = {"County Sheriff", "County Attorney", "County Treasurer",
                  "Register of Deeds", "Register of Probate"}
SINGLE_SEAT = COUNTY_OFFICES | {"County Commissioner", "Governor", "United States Senator",
                                "Representative in Congress", "Executive Councilor",
                                "State Senator"}


def normalize_name(name):
    import re
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", "", (name or "").upper())).strip()


def ensure_general(cur, dry):
    cur.execute("SELECT id FROM elections WHERE year=? AND election_type='general' AND party IS NULL",
                (YEAR,))
    row = cur.fetchone()
    if row:
        return row[0], False
    cur.execute("INSERT INTO elections (year, election_type, party, redistricting_cycle) "
                "VALUES (?, 'general', NULL, ?)", (YEAR, CYCLE))
    return cur.lastrowid, True


def office_ids(cur):
    cur.execute("SELECT id, name FROM offices")
    return {n: i for i, n in cur.fetchall()}


def seats_for(cur, office, office_id, county, district, primary_ids):
    if office in SINGLE_SEAT:
        return 1, "single-seat office"
    qs = ",".join("?" * len(primary_ids))
    cur.execute(f"""SELECT DISTINCT seats FROM races
                     WHERE election_id IN ({qs}) AND office_id=?
                       AND COALESCE(county,'')=? AND COALESCE(district,'')=?""",
                (*primary_ids, office_id, county or "", district or ""))
    got = [r[0] for r in cur.fetchall() if r[0]]
    if len(got) == 1:
        return got[0], "2026 primary"
    if len(got) > 1:
        raise SystemExit(f"conflicting primary seats for {office} {county} {district}: {got}")
    cur.execute("""SELECT r.seats FROM races r JOIN elections e ON e.id=r.election_id
                    WHERE e.year=2024 AND e.election_type='general' AND r.office_id=?
                      AND COALESCE(r.county,'')=? AND COALESCE(r.district,'')=?""",
                (office_id, county or "", district or ""))
    row = cur.fetchone()
    if row and row[0]:
        return row[0], "2024 general"
    raise SystemExit(f"no seat count for {office} {county} {district}")


def ensure_race(cur, election_id, office_id, county, district, seats):
    cur.execute("""SELECT id, seats FROM races WHERE election_id=? AND office_id=?
                    AND COALESCE(county,'')=? AND COALESCE(district,'')=?""",
                (election_id, office_id, county or "", district or ""))
    row = cur.fetchone()
    if row:
        if row[1] != seats:
            cur.execute("UPDATE races SET seats=? WHERE id=?", (seats, row[0]))
        return row[0], False
    cur.execute("INSERT INTO races (election_id, office_id, district, county, seats) VALUES (?,?,?,?,?)",
                (election_id, office_id, district, county, seats))
    return cur.lastrowid, True


def primary_filed(cur, office_id, county, district, primary_ids):
    """Filed (not write-in) 2026 primary roster rows for the same contest."""
    qs = ",".join("?" * len(primary_ids))
    cur.execute(f"""SELECT c.name, e.party, rc.is_incumbent, rc.recruitment_candidate_id,
                           rc.recruitment_filing_id
                      FROM race_candidates rc
                      JOIN races r ON r.id=rc.race_id
                      JOIN elections e ON e.id=r.election_id
                      JOIN candidates c ON c.id=rc.candidate_id
                     WHERE r.election_id IN ({qs}) AND r.office_id=?
                       AND COALESCE(r.county,'')=? AND COALESCE(r.district,'')=?
                       AND rc.recruitment_filing_id > 0""",
                (*primary_ids, office_id, county or "", district or ""))
    return [dict(zip(("name", "party", "is_incumbent", "rcid", "rfid"), r)) for r in cur.fetchall()]


def link_primary(name, filed):
    """Match a list name to a filed primary row in the same contest."""
    from intake.roster import same_person
    exact = [f for f in filed if normalize_name(f["name"]) == normalize_name(name)]
    if exact:
        return exact[0], "exact"
    fuzzy = [f for f in filed if same_person(f["name"], name)]
    if len(fuzzy) == 1:
        return fuzzy[0], "fuzzy"
    return None, ("ambiguous" if fuzzy else None)


def ensure_candidate(cur, name, party):
    cur.execute("SELECT id FROM candidates WHERE name=? AND party=? ORDER BY id LIMIT 1", (name, party))
    row = cur.fetchone()
    if row:
        return row[0], False
    cur.execute("INSERT INTO candidates (name, name_normalized, party) VALUES (?,?,?)",
                (name, normalize_name(name), party))
    return cur.lastrowid, True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "nh_elections.db"))
    ap.add_argument("--data", default=str(ROOT / "data" / "general_2026_candidates.json"))
    ap.add_argument("--dry-run", action="store_true", help="do everything, then roll back")
    args = ap.parse_args()

    doc = json.load(open(args.data))
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    cur.execute("BEGIN")

    cur.execute("SELECT id FROM elections WHERE year=? AND election_type=?", (YEAR, PRIMARY_TYPE))
    primary_ids = [r[0] for r in cur.fetchall()]
    if not primary_ids:
        raise SystemExit("no 2026 state_primary elections found")
    offices = office_ids(cur)
    general_id, made_election = ensure_general(cur, args.dry_run)

    # contest -> ordered list of people, each with every party line printed
    contests = collections.OrderedDict()
    for row in doc["candidates"]:
        key = (row["office"], row["county"], row["district"])
        people = contests.setdefault(key, collections.OrderedDict())
        p = people.setdefault(normalize_name(row["name"]), {"name": row["name"], "lines": []})
        p["lines"].append(row["party"])

    stats = collections.Counter()
    per_office = collections.defaultdict(lambda: collections.Counter())
    notes = collections.defaultdict(list)
    keep = collections.defaultdict(set)   # race_id -> candidate_ids on the list

    for (office, county, district), people in contests.items():
        oid = offices.get(office)
        if oid is None:
            raise SystemExit(f"office not in DB: {office}")
        seats, seat_src = seats_for(cur, office, oid, county, district, primary_ids)
        race_id, made = ensure_race(cur, general_id, oid, county, district, seats)
        stats["races_created" if made else "races_existing"] += 1
        per_office[office]["races"] += 1
        per_office[office]["seats"] += seats
        filed = primary_filed(cur, oid, county, district, primary_ids)
        label = f"{office} {county or ''} {district or ''}".strip()

        party_count = collections.Counter()
        for order, p in enumerate(people.values(), 1):
            link, how = link_primary(p["name"], filed)
            if how == "fuzzy":
                notes["fuzzy primary link"].append(f"{label}: '{p['name']}' ~ primary '{link['name']}'")
            elif how == "ambiguous":
                notes["ambiguous primary link (not linked)"].append(f"{label}: '{p['name']}'")
            lines = p["lines"]
            if len(lines) > 1:
                party = link["party"] if link and link["party"] in lines else lines[0]
                notes["on more than one party line (one roster row)"].append(
                    f"{label}: {p['name']} {'/'.join(lines)} -> {party}")
            else:
                party = lines[0]
            for ln in set(lines):
                party_count[ln] += 1
            cand_id, cmade = ensure_candidate(cur, p["name"], party)
            stats["candidates_created" if cmade else "candidates_reused"] += 1
            cur.execute("SELECT id FROM race_candidates WHERE race_id=? AND candidate_id=?",
                        (race_id, cand_id))
            existing = cur.fetchone()
            # recruitment_filing_id: the primary filing when linked, else 0 - the
            # scripts/add_ballot_candidate.py convention for a name printed on the
            # ballot without a recruitment filing. Never NULL: place_races sorts on
            # (recruitment_filing_id = -1), and NULL would sort ahead of the list.
            vals = (party, order, int(bool(link and link["is_incumbent"])),
                    link["rcid"] if link else None, link["rfid"] if link else 0)
            if not link:
                notes["printed on the list, no 2026 primary filing (recruitment_filing_id=0)"].append(
                    f"{label}: {p['name']} ({'/'.join(lines)})")
            if existing:
                cur.execute("""UPDATE race_candidates SET party=?, ballot_order=?, is_incumbent=?,
                               recruitment_candidate_id=?, recruitment_filing_id=? WHERE id=?""",
                            (*vals, existing[0]))
                stats["roster_rows_existing"] += 1
            else:
                cur.execute("""INSERT INTO race_candidates (race_id, candidate_id, party, ballot_order,
                               is_incumbent, recruitment_candidate_id, recruitment_filing_id)
                               VALUES (?,?,?,?,?,?,?)""", (race_id, cand_id, *vals))
                stats["roster_rows_created"] += 1
            per_office[office]["candidates"] += 1
            keep[race_id].add(cand_id)
        for party_name, n in party_count.items():
            if n > seats:
                notes["more nominees than seats for one party"].append(
                    f"{label}: {party_name} {n} for {seats} seat(s)")

    # prune roster rows no longer on the list (never write-ins, never rows with results)
    cur.execute("SELECT id FROM races WHERE election_id=?", (general_id,))
    for (race_id,) in cur.fetchall():
        cur.execute("""SELECT rc.id, rc.candidate_id, c.name, rc.recruitment_filing_id,
                              EXISTS(SELECT 1 FROM results WHERE race_id=rc.race_id
                                        AND candidate_id=rc.candidate_id)
                         FROM race_candidates rc JOIN candidates c ON c.id=rc.candidate_id
                        WHERE rc.race_id=?""", (race_id,))
        for rcid, cid, name, rfid, has_results in cur.fetchall():
            if cid in keep.get(race_id, set()) or rfid == -1:
                continue
            if has_results:
                notes["off the list but has results (left in place)"].append(f"race {race_id}: {name}")
                continue
            cur.execute("DELETE FROM race_candidates WHERE id=?", (rcid,))
            stats["roster_rows_pruned"] += 1

    # primary contests with no general counterpart (placeholder races nobody filed for)
    qs = ",".join("?" * len(primary_ids))
    cur.execute(f"""SELECT DISTINCT o.name, COALESCE(r.county,''), COALESCE(r.district,'')
                      FROM races r JOIN offices o ON o.id=r.office_id
                     WHERE r.election_id IN ({qs}) AND o.name <> 'Delegate to the State Convention'
                       AND NOT EXISTS (SELECT 1 FROM races g WHERE g.election_id=? AND g.office_id=r.office_id
                                         AND COALESCE(g.county,'')=COALESCE(r.county,'')
                                         AND COALESCE(g.district,'')=COALESCE(r.district,''))
                     ORDER BY 1,2,3""", (*primary_ids, general_id))
    for o, c, d in cur.fetchall():
        notes["2026 primary contest not on the SoS general list (not created)"].append(f"{o} {c} {d}".strip())

    print(f"general election_id={general_id}{' (created)' if made_election else ''}"
          f"  source list date {doc.get('list_date')}")
    print("per office:")
    for office, c in per_office.items():
        print(f"  {office:32s} races={c['races']:3d} seats={c['seats']:3d} candidates={c['candidates']:3d}")
    for k, v in stats.items():
        print(f"{k}: {v}")
    for k, items in notes.items():
        print(f"\n{k} ({len(items)}):")
        for i in items:
            print(f"  {i}")

    if args.dry_run:
        conn.rollback()
        print("\nDRY RUN - rolled back")
    else:
        conn.commit()
        print("\ncommitted")


if __name__ == "__main__":
    main()
