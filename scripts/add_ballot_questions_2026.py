#!/usr/bin/env python3
"""Put the two Nov 3, 2026 ballot questions on the general-election ballot.

The 2026 general (elections.id 31) was built from the candidate filings, so it
has no races for the questions printed on the back of the ballot. On election
night every YES / NO line on a tape had no race_id to land on and went to the
review queue for good. This adds them.

What is on the ballot (from the 319 official SOS ballots harvested to
/opt/ballots-2026/official_general, Nov 2026):

  Question 1 - Constitutional amendment, CACR 13 (eliminates the office of
               register of probate). On EVERY ballot in the state: all 320
               polling places, wards and unincorporated places included.
               Needs two-thirds of those voting on it, statewide.

  Question 2 - Statutory question required by HB 1300, Chapter 324, 2026: the
               property tax cap (RSA 32:5-i). Printed on 296 of the 319 PDFs,
               plus Atkinson (image-only PDF; a town, so it carries the
               question like every other town). It is NOT on the ballot in the
               23 unincorporated places (grants, purchases, locations and
               townships such as Dixville, Livermore, Odell and Success), which
               have no town or school district of their own - listed in NO_Q2
               below. Each town adopts or rejects it for itself, by a
               three-fifths vote of its own voters; city wards add up to the
               city.

How it is modelled, so the existing machinery needs no change:

  * Q1 is a statewide race (county = '', district = ''), which
    entry.place_races already puts on every polling place's ballot.
  * Q2 is ONE race with district 'HB1300' and county ''. A race with a
    district is only placed on the towns mapped to it in
    municipality_districts, so the 23 places without the question never see
    it. Those mapping rows use their own redistricting_cycle
    ('ballot-questions-2026') so that build_municipality_districts.py, which
    deletes and rebuilds the '2022-2030' cycle, cannot wipe them.
  * Both races get two roster rows, Yes and No (shared candidates, party
    NULL, filing id 0 so the general site counts them as printed).
  * offices.level is 'question', and the office names are not in app.py's
    CONTESTED_OFFICES, so chamber tallies, the topline, swing and projections
    never read them.

Idempotent: every row is looked up before it is inserted, and the Q2 town map
is synced (missing rows added, rows for excluded places removed).

    python3 scripts/add_ballot_questions_2026.py --db /root/intake-test-G/nh_elections.db
    python3 scripts/add_ballot_questions_2026.py --db /root/intake-test-G/nh_elections.db --apply
"""
import argparse
import sqlite3
import sys

YEAR, ELECTION_TYPE = 2026, "general"
LEVEL = "question"
MD_CYCLE = "ballot-questions-2026"
MD_SOURCE = "SOS official general ballots 2026 (HB 1300, Ch. 324)"

Q1_OFFICE = "Question 1: Constitutional Amendment (CACR 13)"
Q2_OFFICE = "Question 2: Property Tax Cap (HB 1300)"
Q2_DISTRICT = "HB1300"

# polling_places spellings of the places whose official ballot carries
# Question 1 only. Checked against polling_places on every run.
NO_Q2 = [
    "Atkinson & Gilmanton Academy Grant", "Beans Grant", "Beans Purchase",
    "Cambridge", "Chandlers Purchase", "Crawfords Purchase", "Cutts Grant",
    "Dixs Grant", "Dixville", "Ervings Location", "Greens Grant",
    "Hadleys Purchase", "Kilkenny", "Livermore", "Low and Burbanks Grant",
    "Martins Location", "Odell", "Pinkhams Grant", "Sargents Purchase",
    "Second College Grant", "Success", "Thompson and Meserves Purchase",
    "Wentworths Location",
]

CHOICES = [("Yes", "YES", 1), ("No", "NO", 2)]


def one(cur, sql, args=()):
    cur.execute(sql, args)
    row = cur.fetchone()
    return row[0] if row else None


def ensure(cur, log, what, select, sel_args, insert, ins_args):
    found = one(cur, select, sel_args)
    if found is not None:
        log.append(f"  exists   {what} -> id {found}")
        return found
    cur.execute(insert, ins_args)
    log.append(f"  INSERT   {what} -> id {cur.lastrowid}")
    return cur.lastrowid


def run(conn, apply):
    cur = conn.cursor()
    log = []

    eids = [r[0] for r in cur.execute(
        "SELECT id FROM elections WHERE year = ? AND election_type = ?",
        (YEAR, ELECTION_TYPE))]
    if len(eids) != 1:
        sys.exit(f"expected exactly one {YEAR} {ELECTION_TYPE} election, found {eids}")
    eid = eids[0]
    log.append(f"election {YEAR} {ELECTION_TYPE}: id {eid}")

    places = [r[0] for r in cur.execute(
        "SELECT municipality FROM polling_places ORDER BY municipality")]
    missing = [p for p in NO_Q2 if p not in places]
    if missing:
        sys.exit(f"NO_Q2 names not in polling_places: {missing}")
    q2_places = [p for p in places if p not in set(NO_Q2)]
    log.append(f"polling places: {len(places)}  Q1 on all {len(places)}, "
               f"Q2 on {len(q2_places)}, Q2 excluded: {len(NO_Q2)}")

    office_ids, race_ids = {}, {}
    for name, district in ((Q1_OFFICE, ""), (Q2_OFFICE, Q2_DISTRICT)):
        oid = ensure(cur, log, f"office '{name}'",
                     "SELECT id FROM offices WHERE name = ?", (name,),
                     "INSERT INTO offices (name, level) VALUES (?, ?)", (name, LEVEL))
        lvl = one(cur, "SELECT level FROM offices WHERE id = ?", (oid,))
        if lvl != LEVEL:
            sys.exit(f"office {oid} '{name}' exists with level '{lvl}', not '{LEVEL}'")
        rid = ensure(cur, log, f"race {name} [district '{district}']",
                     """SELECT id FROM races WHERE election_id = ? AND office_id = ?
                         AND IFNULL(county,'') = '' AND IFNULL(district,'') = ?""",
                     (eid, oid, district),
                     """INSERT INTO races (election_id, office_id, district, county, seats)
                        VALUES (?, ?, ?, '', 1)""", (eid, oid, district))
        office_ids[name], race_ids[name] = oid, rid

    cand = {}
    for label, norm, _order in CHOICES:
        cand[label] = ensure(cur, log, f"candidate '{label}'",
                             """SELECT id FROM candidates
                                 WHERE name_normalized = ? AND party IS NULL
                                 ORDER BY id LIMIT 1""", (norm,),
                             """INSERT INTO candidates (name, name_normalized, party)
                                VALUES (?, ?, NULL)""", (label, norm))

    for name, rid in race_ids.items():
        for label, _norm, order in CHOICES:
            ensure(cur, log, f"roster race {rid} {label}",
                   "SELECT id FROM race_candidates WHERE race_id = ? AND candidate_id = ?",
                   (rid, cand[label]),
                   """INSERT INTO race_candidates
                      (race_id, candidate_id, party, ballot_order, is_incumbent,
                       recruitment_candidate_id, recruitment_filing_id)
                      VALUES (?, ?, NULL, ?, 0, NULL, 0)""", (rid, cand[label], order))
        # Anything else on a question's roster is wrong (the aggregate write-in
        # pseudo-candidate is never stored there; place_races adds it on read).
        extra = [r[0] for r in cur.execute(
            """SELECT c.name FROM race_candidates rc JOIN candidates c ON c.id = rc.candidate_id
                WHERE rc.race_id = ? AND rc.candidate_id NOT IN (?, ?)""",
            (rid, cand["Yes"], cand["No"]))]
        if extra:
            log.append(f"  WARNING  race {rid} also carries {extra} - left alone")

    q2_oid = office_ids[Q2_OFFICE]
    have = {r[0] for r in cur.execute(
        """SELECT municipality FROM municipality_districts
            WHERE office_id = ? AND county = '' AND district = ?""", (q2_oid, Q2_DISTRICT))}
    add = [p for p in q2_places if p not in have]
    drop = sorted(have - set(q2_places))
    cur.executemany(
        """INSERT OR IGNORE INTO municipality_districts
           (municipality, office_id, county, district, redistricting_cycle, source)
           VALUES (?, ?, '', ?, ?, ?)""",
        [(p, q2_oid, Q2_DISTRICT, MD_CYCLE, MD_SOURCE) for p in add])
    if drop:
        cur.executemany(
            """DELETE FROM municipality_districts
                WHERE office_id = ? AND county = '' AND district = ? AND municipality = ?""",
            [(q2_oid, Q2_DISTRICT, p) for p in drop])
    log.append(f"  Q2 town map: {len(have)} existing, +{len(add)} added, -{len(drop)} removed"
               + (f" {drop}" if drop else ""))
    n = one(cur, """SELECT COUNT(*) FROM municipality_districts
                     WHERE office_id = ? AND district = ?""", (q2_oid, Q2_DISTRICT))
    log.append(f"  Q2 town map now {n} rows")

    print("\n".join(log))
    for name, rid in race_ids.items():
        print(f"RESULT {name}: race_id={rid} Yes={cand['Yes']} No={cand['No']}")
    if apply:
        conn.commit()
        print("APPLIED (committed).")
    else:
        conn.rollback()
        print("DRY RUN - rolled back. Pass --apply to write.")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", default="nh_elections.db")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="report what would change and roll back (default)")
    g.add_argument("--apply", action="store_true", help="write the changes")
    args = ap.parse_args()
    conn = sqlite3.connect(args.db, timeout=30)
    try:
        run(conn, args.apply)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
