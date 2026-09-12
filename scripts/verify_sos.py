#!/usr/bin/env python3
"""Compare every figure we hold against the Secretary of State's sheets.

Not a check of the importer's own reading - it re-reads every published sheet
and asks whether the number on it is the number in the database. Anything that
disagrees is printed, with the sheet it came from.
"""
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import re
import import_sos as I

_CACHE = {}


def download(url):
    """Fetch a sheet once and keep it, so a full verification reads each file
    from the Secretary of State a single time."""
    import tempfile
    if url not in _CACHE:
        data = I.fetch(url)
        fh = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        fh.write(data)
        fh.close()
        _CACHE[url] = fh.name
    return _CACHE[url]


I.download = download

B = "https://www.sos.nh.gov/sites/g/files/ehbemt561/files/inline-documents/sonh"
COUNTIES = ["belknap", "carroll", "cheshire", "coos", "grafton", "hillsborough",
            "merrimack", "rockingham", "strafford", "sullivan"]
SENATE = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10-11", "12-13",
          "14-16", "17-18", "19-21", "22-24"]


def squash(s):
    return re.sub(r"[^A-Za-z0-9]", "", str(s or "")).upper()


def norm_name(s):
    return I.norm(s)


def certified_rows():
    """(office, county, district, party, town, candidate) -> votes, from the sheets."""
    out = {}
    sheets = 0
    for party in ("republican", "democratic"):
        pf = "Republican" if party == "republican" else "Democratic"
        for c in COUNTIES:
            for kind, office in (("house", "State Representative"),
                                 ("delegate", "Delegate to the State Convention")):
                if kind == "delegate" and party == "democratic":
                    continue        # only Republicans elect convention delegates
                url = f"{B}/2026-sp-{kind}-{c}-{party}.xlsx"
                try:
                    path = I.download(url)
                except Exception:
                    continue
                g = I.parse_house_xlsx(path, url)
                if g.get("error"):
                    print(f"  ! {kind}-{c}-{party}: {g['error']}")
                    continue
                sheets += 1
                for b in g["blocks"]:
                    for town, figs in b["rows"]:
                        for cand, v in figs.items():
                            out[(office, g["county"], b["district"], pf, squash(town),
                                 norm_name(cand))] = v
            url = f"{B}/2026-sp-county-offices-{c}-{party}.xlsx"
            try:
                path = I.download(url)
            except Exception:
                continue
            g = I.parse_county_offices_xlsx(path, url)
            if g.get("error"):
                print(f"  ! county-offices-{c}-{party}: {g['error']}")
                continue
            sheets += 1
            seen = {}
            for race in g["races"]:
                office = race["office"]
                seen[office] = seen.get(office, 0) + 1
                dist = str(seen[office]) if office == "County Commissioner" else ""
                for town, figs in race["rows"]:
                    for cand, v in figs.items():
                        out[(office, g["county"], dist, pf, squash(town),
                             norm_name(cand))] = v
        for d in SENATE:
            url = f"{B}/2026-sp-state-senate-{d}-{party}.xlsx"
            try:
                path = I.download(url)
            except Exception:
                continue
            g = I.parse_senate_xlsx(path, url)
            if g.get("error"):
                continue
            sheets += 1
            for b in g["blocks"]:
                for town, figs in b["rows"]:
                    for cand, v in figs.items():
                        out[("State Senator", "", b["district"], pf, squash(town),
                             norm_name(cand))] = v
        for d in ("1", "2", "3", "4", "5"):
            url = f"{B}/2026-sp-executive-council-{d}-{party}.xlsx"
            try:
                path = I.download(url)
            except Exception:
                continue
            g = I.parse_senate_xlsx(path, url, I.COUNCIL_TITLE)
            if g.get("error"):
                continue
            sheets += 1
            for b in g["blocks"]:
                for town, figs in b["rows"]:
                    for cand, v in figs.items():
                        out[("Executive Councilor", "", b["district"], pf, squash(town),
                             norm_name(cand))] = v
        for slug, office, dist in (("governor", "Governor", ""),
                                   ("us-senator", "United States Senator", ""),
                                   ("congressional-district-1", "Representative in Congress", "1"),
                                   ("congressional-district-2", "Representative in Congress", "2")):
            targets = ([f"{B}/2026-sp-{slug}-{c}-{party}.xlsx" for c in COUNTIES]
                       if dist == "" else [f"{B}/2026-sp-{slug}-{party}.xlsx"])
            for url in targets:
                try:
                    path = I.download(url)
                except Exception:
                    continue
                g = I.parse_xlsx(path, url)
                if g.get("error"):
                    continue
                sheets += 1
                for town, figs in g["rows"]:
                    for cand, v in figs.items():
                        out[(office, "", g.get("district") or dist, pf, squash(town),
                             norm_name(cand))] = v
    print(f"read {sheets} sheet(s), {len(out)} certified figure(s)")
    return out


def main():
    cert = certified_rows()
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    ours = {}
    for r in cur.execute("""SELECT o.name office, IFNULL(ra.county,'') county,
                                   IFNULL(ra.district,'') district, e.party,
                                   res.municipality town, c.name cand, res.votes v
                              FROM results res
                              JOIN races ra ON ra.id = res.race_id
                              JOIN offices o ON o.id = ra.office_id
                              JOIN elections e ON e.id = ra.election_id
                              JOIN candidates c ON c.id = res.candidate_id
                             WHERE e.year = 2026 AND e.election_type = 'state_primary'"""):
        key = (r["office"], r["county"], r["district"], r["party"],
               squash(r["town"]), norm_name(r["cand"]))
        ours[key] = r["v"]

    agree = differ = missing = 0
    bad = defaultdict(list)
    for k, v in cert.items():
        mine = ours.get(k)
        if mine is None:
            if v:
                missing += 1
                bad[k[0]].append((k, "not held", v))
            continue
        if mine == v:
            agree += 1
        else:
            differ += 1
            bad[k[0]].append((k, mine, v))

    print(f"\nexact match : {agree}")
    print(f"differs     : {differ}")
    print(f"not held    : {missing}")
    for office, rows in sorted(bad.items()):
        wrong = [r for r in rows if r[1] != "not held"]
        absent = [r for r in rows if r[1] == "not held"]
        print(f"\n{office}: {len(wrong)} wrong, {len(absent)} not held")
        for k, mine, sos in wrong[:10]:
            print(f"   {k[1]} {k[2]} {k[3][:1]} {k[4]} {k[5]}: ours {mine} certified {sos}")
        for k, _m, sos in absent[:5]:
            print(f"   {k[1]} {k[2]} {k[3][:1]} {k[4]} {k[5]}: missing, certified {sos}")


if __name__ == "__main__":
    main()
