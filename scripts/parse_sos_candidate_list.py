#!/usr/bin/env python3
"""Parse the Secretary of State's general-election CANDIDATE LIST W/ ADDRESS PDF
into the JSON data file import_general_2026.py loads.

Source (official): https://mm.nh.gov/files/uploads/sos/docs/2026-ge-candidate-list.pdf
("CANDIDATE LIST W/ ADDRESS - 09/17/2026", 46 pages). sos.nh.gov / mm.nh.gov
403 plain curl; download it with a browser or curl_cffi impersonate="chrome".

Reads `pdftotext -layout` output. Candidate rows start in column 0 and end in a
party code; lines that start with whitespace are address continuations. Office,
"District N" and "<COUNTY> County" lines are headers. Names are kept exactly as
printed. Only candidate identity is kept - addresses are not written out.

Usage: python3 scripts/parse_sos_candidate_list.py LIST.pdf data/general_2026_candidates.json
"""
import json
import re
import subprocess
import sys

PARTIES = {
    "DEM": "Democratic", "REP": "Republican", "LIB": "Libertarian",
    "CON": "Constitution", "IND": "Independent", "UND": "Undeclared",
    "CLA": "CLA",  # printed as-is; the list has no legend for this code
}

STATEWIDE = {"Governor": "Governor", "United States Senator": "United States Senator"}
DISTRICTED = {"Representative in Congress", "Executive Councilor", "State Senator"}
COUNTY_OFFICES = {
    "Sheriff": "County Sheriff", "County Attorney": "County Attorney",
    "County Treasurer": "County Treasurer", "Register of Deeds": "Register of Deeds",
    "Register of Probate": "Register of Probate",
}
ORDINAL = re.compile(r"^County Commissioner(?:,\s*(\d+)(?:st|nd|rd|th) District| District (\d+))$")
ROW = re.compile(r"^(\S.*?)\s{2,}.*\s(" + "|".join(PARTIES) + r")\s*$")
NOISE = re.compile(r"OFFICE OF THE SECRETARY OF STATE|CANDIDATE LIST W/|^Candidate Name")


def parse(text):
    office = county = district = None
    in_house = False          # State Representative section (county -> district)
    out, seen_house_counties = [], set()
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.rstrip()
        s = line.strip()
        if not s or NOISE.search(line):
            continue
        if line[0].isspace():
            continue  # address / zip continuation
        m = re.match(r"^([A-Z]+) County$", s)
        if m:
            county = m.group(1).title()
            district = None
            if in_house and county in seen_house_counties:
                in_house = False  # second pass through the counties = county offices
                office = None
            if in_house:
                seen_house_counties.add(county)
            continue
        if s in STATEWIDE:
            office, county, district, in_house = STATEWIDE[s], None, None, False
            continue
        if s in DISTRICTED:
            office, county, district, in_house = s, None, None, False
            continue
        if s == "State Representative":
            office, county, district, in_house = s, None, None, True
            continue
        if s in COUNTY_OFFICES and not in_house:
            office, district = COUNTY_OFFICES[s], None
            continue
        m = ORDINAL.match(s)
        if m and not in_house:
            office, district = "County Commissioner", m.group(1) or m.group(2)
            continue
        m = re.match(r"^District (\d+)$", s)
        if m:
            district = m.group(1)
            continue
        m = ROW.match(line)
        if not m:
            raise ValueError(f"line {n}: unrecognised: {line!r}")
        if office is None:
            raise ValueError(f"line {n}: candidate before any office header")
        code = m.group(2)
        out.append({
            "office": office, "county": county, "district": district,
            "name": m.group(1).strip(), "party_code": code, "party": PARTIES[code],
            "source_line": n,
        })
    return out


def main():
    pdf, dest = sys.argv[1], sys.argv[2]
    text = subprocess.run(["pdftotext", "-layout", pdf, "-"], check=True,
                          capture_output=True, text=True).stdout
    rows = parse(text)
    header = re.search(r"CANDIDATE LIST W/ ADDRESS - (\S+)", text)
    doc = {
        "source": "NH Secretary of State, CANDIDATE LIST W/ ADDRESS",
        "source_url": "https://mm.nh.gov/files/uploads/sos/docs/2026-ge-candidate-list.pdf",
        "list_date": header.group(1) if header else None,
        "year": 2026, "election_type": "general",
        "candidates": rows,
    }
    with open(dest, "w") as f:
        json.dump(doc, f, indent=1)
        f.write("\n")
    print(f"{len(rows)} candidate lines -> {dest}")


if __name__ == "__main__":
    main()
