#!/usr/bin/env python3
"""
Import 1998-2014 NH state general-election results rebuilt from the Secretary of
State's own result files, as saved by the Internet Archive (web.archive.org:
www.sos.nh.gov/general98 ... /general%202010 pages, and the 2012/2014 spreadsheets
behind sos.nh.gov/WorkArea/DownloadAsset.aspx).

The pages were parsed off-line into data/sos_archive/general_<year>.json.gz:
one record per municipality per candidate per race, every district block checked
against the TOTALS row the SoS printed under it. Each record:
  {office, county, district, seats, municipality, candidate, party, votes}
county is null for races that cross county lines (Senate, Council, Congress,
statewide), as the 2016+ imports store them.

Only years <= 2014 are touched: for each year in the data, the 'general' election
row and everything under it is deleted and rebuilt, so re-running is safe and
2016+ data (hand entry, intake bot, recounts) can never be affected.
DRY RUN by default; pass --commit to write. Back up the DB first.
"""
import argparse
import glob
import gzip
import json
import os
import sqlite3
import sys
from collections import defaultdict

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nh_elections.db")
DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sos_archive")
MAX_YEAR = 2014


def cycle_for(year):
    return "1992-2000" if year <= 2000 else "2002-2010" if year <= 2010 else "2012-2020"


def load(path):
    with gzip.open(path, "rt") as f:
        return json.load(f)


def import_year(cur, year, recs, office_id):
    if year > MAX_YEAR:
        raise SystemExit(f"refusing to touch {year}: this importer only owns years <= {MAX_YEAR}")
    row = cur.execute("SELECT id FROM elections WHERE year=? AND election_type='general'", (year,)).fetchone()
    if row:
        eid = row[0]
        cur.execute("DELETE FROM results WHERE race_id IN (SELECT id FROM races WHERE election_id=?)", (eid,))
        cur.execute("DELETE FROM race_candidates WHERE race_id IN (SELECT id FROM races WHERE election_id=?)", (eid,))
        cur.execute("DELETE FROM races WHERE election_id=?", (eid,))
    else:
        cur.execute("INSERT INTO elections (year, election_type, party, redistricting_cycle) VALUES (?, 'general', NULL, ?)",
                    (year, cycle_for(year)))
        eid = cur.lastrowid

    cands = {}

    def cand_id(name, party):
        key = (name.upper(), party)
        if key in cands:
            return cands[key]
        r = cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND party=?", key).fetchone()
        if r:
            cands[key] = r[0]
        else:
            cur.execute("INSERT INTO candidates (name, name_normalized, party, display_order) VALUES (?,?,?,999)",
                        (name, name.upper(), party))
            cands[key] = cur.lastrowid
        return cands[key]

    races = defaultdict(list)
    for r in recs:
        races[(r["office"], r["county"], r["district"] or "", r["seats"])].append(r)
    n = 0
    for (office, county, district, seats), rs in sorted(races.items(), key=lambda x: str(x[0])):
        cur.execute("INSERT INTO races (election_id, office_id, district, county, seats, is_official) VALUES (?,?,?,?,?,1)",
                    (eid, office_id[office], district or None, county, seats))
        rid = cur.lastrowid
        seen = set()
        for r in rs:
            cid = cand_id(r["candidate"], r["party"])
            if cid not in seen and r["candidate"] != "Write-Ins":
                cur.execute("INSERT INTO race_candidates (race_id, candidate_id, party) VALUES (?,?,?)",
                            (rid, cid, r["party"]))
                seen.add(cid)
            cur.execute("INSERT INTO results (race_id, candidate_id, municipality, votes, votes_original) VALUES (?,?,?,?,?)",
                        (rid, cid, r["municipality"], r["votes"], r["votes"]))
            n += 1
    return eid, len(races), n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true", help="write to the DB (default: dry run)")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--years", help="comma-separated subset, e.g. 2008,2010")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(DATA, "general_*.json.gz")))
    want = {int(y) for y in args.years.split(",")} if args.years else None
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    office_id = {name: oid for oid, name in cur.execute("SELECT id, name FROM offices")}
    for f in files:
        year = int(os.path.basename(f).split("_")[1].split(".")[0])
        if want and year not in want:
            continue
        recs = load(f)
        missing = {r["office"] for r in recs} - set(office_id)
        if missing:
            sys.exit(f"{year}: offices not in DB: {missing}")
        by_office = defaultdict(int)
        for r in recs:
            by_office[r["office"]] += 1
        eid, nraces, nres = import_year(cur, year, recs, office_id)
        print(f"{year}: election {eid}, {nraces} races, {nres:,} result rows  {dict(by_office)}")
    if args.commit:
        conn.commit()
        print("COMMITTED")
    else:
        conn.rollback()
        print("DRY RUN - rolled back. Re-run with --commit to write.")
    conn.close()


if __name__ == "__main__":
    main()
