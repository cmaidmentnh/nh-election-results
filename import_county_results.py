#!/usr/bin/env python3
"""
Import historical NH COUNTY-office election results (Sheriff, County Attorney,
Treasurer, Register of Deeds, Register of Probate, County Commissioner) from the
Secretary of State per-county result spreadsheets into nh_elections.db.

Source files: the SoS "county offices" .xls/.xlsx exports (named like
'2024-ge-county-offices-belknap.xls' or '2016-ge-belknap-county.xls'). Layout:
  row with office names (Sheriff/Attorney/...), next row candidate names
  ("Name, r&d") + Undervotes/Overvotes/Write-Ins(/Scatter), then one row per
  municipality/ward with vote counts.

Idempotent: clears previously-imported county results/races for each affected
election before re-inserting. DRY RUN by default; pass --commit to write.
"""
import argparse
import glob
import json
import os
import re
import sqlite3
import sys

SRC_DIR = os.path.expanduser("~/Downloads")
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nh_elections.db")

COUNTIES = ["belknap", "carroll", "cheshire", "coos", "grafton", "hillsborough",
            "merrimack", "rockingham", "strafford", "sullivan"]
OFFICE_RE = re.compile(r"sheriff|attorney|treasurer|register of deeds|register of probate|"
                       r"reg\.?\s*of\s*deeds|reg\.?\s*of\s*probate|deeds|probate|commissioner", re.I)
NONCAND = {"undervotes", "overvotes", "scatter", "scattering", "nan", "total votes", ""}


_MUNI_ALIAS = {
    "atk. & gilm. ac. gt.": "Atkinson & Gilmanton Academy Grant",
    "low & burbank's gt.": "Low and Burbanks Grant",
    "thompson & mes's pur.": "Thompson and Meserves Purchase",
    "wentworth's loc.": "Wentworths Location",
}


def normalize_muni(raw):
    m = re.sub(r"\*+$", "", re.sub(r"\s+", " ", raw)).strip()   # drop SoS footnote asterisk
    if m.lower() in _MUNI_ALIAS:
        return _MUNI_ALIAS[m.lower()]
    return m.replace("’", "").replace("'", "")             # DB stores grants apostrophe-free


def office_name(raw):
    s = raw.lower()
    if "sheriff" in s: return "County Sheriff"
    if "attorney" in s: return "County Attorney"
    if "treasurer" in s: return "County Treasurer"
    if "deeds" in s: return "Register of Deeds"
    if "probate" in s: return "Register of Probate"
    if "commissioner" in s: return "County Commissioner"
    return None


def split_party(cell):
    """'Bill Wright, r&d' -> ('Bill Wright', 'r&d'); 'Write-Ins' handled by caller."""
    parts = cell.rsplit(",", 1)
    if len(parts) == 2 and len(parts[1].strip()) <= 6:
        return parts[0].strip(), parts[1].strip().lower()
    return cell.strip(), ""


def meta_from_name(fn):
    b = os.path.basename(fn).lower()
    yr = re.search(r"20\d\d", b); typ = "general" if "-ge" in b else ("state_primary" if "-sp" in b else None)
    cty = next((c for c in COUNTIES if c in b), None)
    party = "republican" if "republican" in b else ("democratic" if "democrat" in b else None)
    return (yr.group(0) if yr else None), typ, (cty.title() if cty else None), party


def parse_file(path):
    """Parse EVERY sheet of a county file. Offices are split across sheets in the newer
    (2024) files (Sheriff-Attorney / Treasurer-Deeds / Probate / Commissioners) and bundled
    in one sheet in older years."""
    import pandas as pd
    import warnings
    warnings.simplefilter("ignore")
    yr, typ, county, party_hint = meta_from_name(path)
    if not (yr and typ and county):
        return []
    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        print(f"  ! read fail {os.path.basename(path)}: {e}", file=sys.stderr); return []
    recs = []
    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, header=None)
        except Exception:
            continue
        recs += parse_sheet(df, yr, typ, county, party_hint)
    return recs


def _isnum(v):
    try:
        import math
        return v is not None and not isinstance(v, bool) and not math.isnan(float(v))
    except (ValueError, TypeError):
        return False


_DIST_RE = re.compile(r"(?:district|dist\.?)\s*(\d+)", re.I)


def parse_sheet(df, yr, typ, county, party_hint):
    """Parse one sheet, which may stack SEVERAL office blocks vertically (Sheriff/Attorney/
    Treasurer/Deeds up top; Probate + Commissioner-by-district below, with the town list
    repeating). Detect each block by its run of town rows, then attribute every candidate
    column to the office (and, for Commissioner, the district) whose header sits at/above
    it in that block."""
    nrows, ncols = df.shape

    def townish(r):
        v = df.iat[r, 0]
        if not isinstance(v, str) or not v.strip():
            return False
        s = v.strip().lower()
        if OFFICE_RE.search(s) or "district" in s or _DIST_RE.search(s) or s.startswith("total") or s.startswith("*"):
            return False
        return any(_isnum(df.iat[r, c]) for c in range(1, ncols))

    def blank(r):
        return all((not isinstance(x, str) or not x.strip()) and not _isnum(x) for x in df.iloc[r])

    # contiguous runs of town rows = one office block each
    runs, r = [], 0
    while r < nrows:
        if townish(r):
            start = r
            while r < nrows and townish(r):
                r += 1
            runs.append((start, r))
        else:
            r += 1

    def owner(col, items):
        best = None
        for cc, val in items:
            if cc <= col:
                best = val
            else:
                break
        return best

    recs = []
    for start, end in runs:
        cand_row = start - 1
        if cand_row < 0:
            continue
        top = cand_row
        while top - 1 >= 0 and not blank(top - 1) and not townish(top - 1):
            top -= 1
        offices, districts = [], []
        for hr in range(top, cand_row):
            for c in range(ncols):
                v = df.iat[hr, c]
                if isinstance(v, str):
                    if OFFICE_RE.search(v):
                        offices.append((c, office_name(v)))
                    md = _DIST_RE.search(v)
                    if md:
                        districts.append((c, md.group(1)))
        if not offices:
            continue
        offices.sort(); districts.sort()
        for c in range(ncols):
            v = df.iat[cand_row, c]
            if not isinstance(v, str):
                continue
            vs = v.strip()
            if vs.lower() in NONCAND or vs.lower() == "no election":
                continue
            office = owner(c, offices)
            if not office:
                continue
            dist = owner(c, districts) if office == "County Commissioner" else ""
            if re.fullmatch(r"write[- ]?ins?", vs, re.I):
                name, party = "Write-in", ""
            else:
                name, party = split_party(vs)
                if not name:
                    continue
                party = party or (party_hint[:1] if party_hint else "")
            for tr in range(start, end):
                val = df.iat[tr, c]
                if not _isnum(val):
                    continue
                recs.append({"year": yr, "election_type": typ, "county": county, "office": office,
                             "district": dist, "candidate": name, "party": party,
                             "municipality": normalize_muni(df.iat[tr, 0]), "votes": int(float(val))})
    return recs


def collect():
    files = [f for f in glob.glob(os.path.join(SRC_DIR, "*county*.xls*"))
             if re.search(r"20\d\d-(ge|sp)", os.path.basename(f).lower())]
    all_recs = []
    for f in sorted(files):
        all_recs.extend(parse_file(f))
    # dedupe identical rows across duplicate downloads
    seen, uniq = set(), []
    for r in all_recs:
        k = (r["year"], r["election_type"], r["county"], r["office"], r.get("district", ""),
             r["candidate"], r["municipality"])
        if k in seen:
            continue
        seen.add(k); uniq.append(r)
    return uniq


def report(recs):
    from collections import defaultdict
    by = defaultdict(lambda: defaultdict(set))
    votes = defaultdict(int)
    for r in recs:
        key = f"{r['year']} {r['election_type']}"
        by[key][r["office"]].add(r["county"])
        votes[key] += r["votes"]
    print(f"\nParsed {len(recs):,} result rows.")
    for k in sorted(by):
        print(f"\n{k}  (total votes {votes[k]:,}):")
        for o in sorted(by[k]):
            print(f"   {o:20} {len(by[k][o])} counties")


# Canonical election_id per (year, type) — the record the existing statewide results use.
ELECTION_MAP = {("2016", "general"): 1, ("2018", "general"): 4, ("2020", "general"): 8,
                ("2022", "general"): 13, ("2024", "general"): 16}
# county-wide offices we import (Commissioner is district-based w/ only 1 partial file -> skipped)
OFFICE_ID = {"County Sheriff": 8, "County Attorney": 9, "County Treasurer": 10,
             "Register of Deeds": 11, "Register of Probate": 12, "County Commissioner": 13}


def norm_name(n):
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", "", n.upper())).strip()


def party_full(p):
    p = (p or "").lower()
    if "r" in p and "d" in p: return "Republican"    # cross-filed (won both) -> R by default
    if p.startswith("r"): return "Republican"
    if p.startswith("d"): return "Democratic"
    if p.startswith("l"): return "lib"
    if p.startswith("i"): return "Independent"
    return ""


def write_db(recs, conn):
    from collections import defaultdict
    cur = conn.cursor()
    targets = tuple(sorted(set(ELECTION_MAP.values())))
    oids = tuple(sorted(OFFICE_ID.values()))
    # idempotent: clear any previously-imported county races (+ their results) for these elections
    qmarks_e, qmarks_o = ",".join("?" * len(targets)), ",".join("?" * len(oids))
    old = [r[0] for r in cur.execute(
        f"SELECT id FROM races WHERE election_id IN ({qmarks_e}) AND office_id IN ({qmarks_o})",
        (*targets, *oids))]
    for rid in old:
        cur.execute("DELETE FROM results WHERE race_id=?", (rid,))
        cur.execute("DELETE FROM races WHERE id=?", (rid,))

    candcache = {}
    def get_cand(name, party):
        key = (norm_name(name), party)
        if key in candcache:
            return candcache[key]
        row = cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND IFNULL(party,'')=?",
                          key).fetchone()
        if row:
            cid = row[0]
        else:
            cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",
                        (name, key[0], party))
            cid = cur.lastrowid
        candcache[key] = cid
        return cid

    races_map = defaultdict(list)
    skipped = 0
    for r in recs:
        et = (r["year"], r["election_type"])
        if et not in ELECTION_MAP or r["office"] not in OFFICE_ID:
            skipped += 1
            continue
        races_map[(ELECTION_MAP[et], OFFICE_ID[r["office"]], r["county"], r.get("district", ""))].append(r)

    inserted = 0
    for (eid, oid, county, district), rs in races_map.items():
        cur.execute("INSERT INTO races(election_id,office_id,district,county,seats,is_official) "
                    "VALUES(?,?,?,?,1,1)", (eid, oid, district, county))
        rid = cur.lastrowid
        for r in rs:
            party = "" if r["candidate"] == "Write-in" else party_full(r["party"])
            cid = get_cand(r["candidate"], party)
            cur.execute("INSERT INTO results(race_id,candidate_id,municipality,votes,votes_original) "
                        "VALUES(?,?,?,?,?)", (rid, cid, r["municipality"], r["votes"], r["votes"]))
            inserted += 1
    conn.commit()
    return inserted, skipped, len(races_map)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true", help="write to the DB (default: dry run)")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--src", default=SRC_DIR, help="directory with the SoS county .xls/.xlsx files")
    ap.add_argument("--dump", help="parse and write records to this JSON file (no DB write)")
    ap.add_argument("--from-json", help="load pre-parsed records from JSON instead of parsing .xls")
    args = ap.parse_args()
    if args.from_json:
        recs = json.load(open(args.from_json))
    else:
        SRC_DIR = args.src
        recs = collect()
    report(recs)
    if args.dump:
        json.dump(recs, open(args.dump, "w"))
        print(f"\nwrote {len(recs):,} records -> {args.dump}")
        sys.exit(0)
    if not args.commit:
        print("\nDRY RUN — nothing written. Re-run with --commit to import.")
        sys.exit(0)
    conn = sqlite3.connect(args.db)
    ins, skip, nraces = write_db(recs, conn)
    conn.close()
    print(f"\nIMPORTED: {ins:,} result rows into {nraces} county races "
          f"(skipped {skip} rows: commissioner/primary/out-of-scope).")
