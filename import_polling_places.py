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


def _col(row, *names):
    """First non-empty value among alternative column spellings.

    The SoS has shipped this file with different headers over time: "Phone" vs
    "Phone (Area Code 603)", and a single "Polling Hours" vs separate state and
    local election windows. Accept either so a re-download does not silently
    blank a column.
    """
    for n in names:
        v = (row.get(n) or "").strip()
        if v:
            return v
    return ""


def main():
    if len(sys.argv) < 2:
        sys.exit('usage: import_polling_places.py "Clerks & PollingPlaces.csv" [nh_elections.db]')
    csv_path = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "nh_elections.db"

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
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
                    "clerk": _col(row, "Clerk"), "clerk_address": _col(row, "Address"),
                    "phone": _col(row, "Phone (Area Code 603)", "Phone"), "fax": _col(row, "Fax"),
                    "email": _col(row, "E-Mail"), "website": _col(row, "Town Website Address"),
                    "polling_hours": _col(row, "Polling Hours",
                                          "State Election Start Time - End Time"),
                    "places": [place] if place else [], "raw": raw,
                }
            else:
                if place and place not in g["places"]:
                    g["places"].append(place)  # extra polling location for an admin-ward town
                # Some towns appear twice: once bare, once with the election
                # attached. Let the row that actually has a value win.
                for field, col in (("clerk", "Clerk"), ("clerk_address", "Address"),
                                   ("fax", "Fax"), ("email", "E-Mail"),
                                   ("website", "Town Website Address")):
                    if not g[field]:
                        g[field] = _col(row, col)
                if not g["phone"]:
                    g["phone"] = _col(row, "Phone (Area Code 603)", "Phone")
                if not g["polling_hours"]:
                    g["polling_hours"] = _col(row, "Polling Hours",
                                              "State Election Start Time - End Time")

    for muni, g in groups.items():
        cur.execute(
            """INSERT INTO polling_places
                   (municipality, county, clerk, clerk_address, phone, fax, email,
                    website, polling_hours, polling_place, raw_name)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(municipality) DO UPDATE SET
                   county=COALESCE(NULLIF(excluded.county,''), county),
                   clerk=COALESCE(NULLIF(excluded.clerk,''), clerk),
                   clerk_address=COALESCE(NULLIF(excluded.clerk_address,''), clerk_address),
                   phone=COALESCE(NULLIF(excluded.phone,''), phone),
                   fax=COALESCE(NULLIF(excluded.fax,''), fax),
                   email=COALESCE(NULLIF(excluded.email,''), email),
                   website=COALESCE(NULLIF(excluded.website,''), website),
                   polling_hours=COALESCE(NULLIF(excluded.polling_hours,''), polling_hours),
                   polling_place=COALESCE(NULLIF(excluded.polling_place,''), polling_place),
                   raw_name=excluded.raw_name""",
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
