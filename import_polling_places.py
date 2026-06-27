#!/usr/bin/env python3
"""
Load the SoS "Clerks & Polling Places" CSV into the polling_places table.

The hard part is matching the CSV's town names (e.g. "MANCHESTER WARD 01",
"ACWORTH") to the canonical municipality names used everywhere else in the DB
(district_compositions / municipality_districts: "Manchester Ward 1", "Acworth").
We match on a normalized key (uppercase, ward number un-padded, punctuation and
spaces removed) and store the canonical name so polling_places joins cleanly to
the entry ballot.

CSV columns: Town/City, Clerk, Address, Phone, Fax, E-Mail, Website,
Polling Hours, Polling Place.

Usage: python3 import_polling_places.py "Clerks & PollingPlaces.csv" [nh_elections.db]
"""

import csv
import re
import sqlite3
import sys

from entry_sources import normkey, canonical_map, town_county_map

# Unincorporated places whose SoS clerk-list name differs from the canonical
# 2024-results spelling. Mapping them lets the polling place link to its ballot
# (these grants/locations vote a full ballot: State Rep, Senate, Exec Council...).
UNINCORPORATED_ALIASES = {
    "AT.& GIL. AC. GT.": "Atkinson & Gilmanton Academy Grant",
    "LOW & BURBANKS GRANT": "Low and Burbanks Grant",
    "THOMPSON & MESERVE'S PURCHASE": "Thompson and Meserves Purchase",
    "WENTWORTH'S LOCATION": "Wentworths Location",
}


def main():
    if len(sys.argv) < 2:
        sys.exit('usage: import_polling_places.py "Clerks & PollingPlaces.csv" [nh_elections.db]')
    csv_path = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "nh_elections.db"

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM polling_places")  # full authoritative reload from the CSV
    canon = canonical_map(cur)
    town_county = town_county_map(cur)

    def resolve(raw):
        """Resolve a CSV name to (canonical municipality, matched?).
        1. exact ward/city match (e.g. Manchester Ward 1, Concord Ward 10)
        2. ward-town -> base town (Derry Ward 1 -> Derry: admin wards, votes as town)
        3. unmatched (grants/purchases with no House district)
        """
        alias = UNINCORPORATED_ALIASES.get(raw.upper().strip())
        if alias:
            return alias, True
        muni = canon.get(normkey(raw))
        if muni:
            return muni, True
        base = re.sub(r"\s+WARD\s+\d+\*?$", "", raw.upper()).strip()
        muni = canon.get(normkey(base))
        if muni:
            return muni, True
        return raw.title(), False

    # Group rows by resolved municipality so admin-ward towns merge their
    # multiple polling locations into one entry.
    groups = {}  # municipality -> dict
    unmatched_names = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            raw = (row.get("Town/City") or "").strip()
            if not raw:
                continue
            muni, matched = resolve(raw)
            if not matched:
                unmatched_names.append(raw)
            g = groups.get(muni)
            place = (row.get("Polling Place") or "").strip()
            if g is None:
                groups[muni] = {
                    "county": town_county.get(normkey(muni))
                              or town_county.get(re.sub(r"\s+Ward\s+\d+$", "", muni).strip().upper()),
                    "clerk": row.get("Clerk"), "clerk_address": row.get("Address"),
                    "phone": row.get("Phone (Area Code 603)"), "fax": row.get("Fax"),
                    "email": row.get("E-Mail"), "website": row.get("Town Website Address"),
                    "polling_hours": row.get("Polling Hours"),
                    "places": [place] if place else [], "raw": raw,
                }
            elif place and place not in g["places"]:
                g["places"].append(place)  # extra polling location for an admin-ward town

    for muni, g in groups.items():
        cur.execute(
            """INSERT INTO polling_places
                   (municipality, county, clerk, clerk_address, phone, fax, email,
                    website, polling_hours, polling_place, raw_name)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(municipality) DO UPDATE SET
                   county=excluded.county, clerk=excluded.clerk,
                   clerk_address=excluded.clerk_address, phone=excluded.phone,
                   fax=excluded.fax, email=excluded.email, website=excluded.website,
                   polling_hours=excluded.polling_hours,
                   polling_place=excluded.polling_place, raw_name=excluded.raw_name""",
            (muni, g["county"], g["clerk"], g["clerk_address"], g["phone"], g["fax"],
             g["email"], g["website"], g["polling_hours"], " | ".join(g["places"]), g["raw"]),
        )

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM polling_places")
    total = cur.fetchone()[0]
    print(f"{len(groups)} municipalities loaded; {total} polling_places total; "
          f"{len(unmatched_names)} CSV rows unmatched to a municipality.")
    if unmatched_names:
        print("Unmatched (stored best-effort, no ballot link — unincorporated places):")
        for n in unmatched_names:
            print(f"    {n}")
    conn.close()


if __name__ == "__main__":
    main()
