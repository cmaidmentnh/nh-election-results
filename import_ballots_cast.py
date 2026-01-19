#!/usr/bin/env python3
"""
Import ballots cast data from XLS files into the database.
"""

import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "nh_elections.db"

# Map years to election IDs (from elections table)
ELECTION_IDS = {
    2016: 1,
    2018: 4,
    2020: 8,
    2022: 13,
    2024: 16
}

# Files to import
FILES_DIR = "/Users/chrismaidment/Desktop/Data-Elections/election_files"
BALLOTS_FILES = {
    2016: f"{FILES_DIR}/2016-ge-ballots-cast.xls",
    2018: f"{FILES_DIR}/2018-ge-ballots-cast_0.xls",
    2020: f"{FILES_DIR}/2020-ge-ballots-cast.xls",
    2022: f"{FILES_DIR}/2022-ge-ballots-cast_3.xls",
    2024: f"{FILES_DIR}/2024-ge-ballots-cast_14.xls"
}


def parse_ballots_file(filepath, year):
    """Parse a ballots cast XLS file and return list of (county, municipality, ballots_cast) tuples."""
    xls = pd.ExcelFile(filepath)
    records = []

    # Process each sheet
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)

        # Find all county columns by looking for "COUNTY/BALLOTS CAST" in column headers or first column
        # Sheets can have multiple counties side by side (e.g., columns 0-3 for one, 6-9 for another)
        county_columns = []  # List of (county_name, name_col, total_col) tuples

        # Check column headers for county names
        for col_idx, col_name in enumerate(df.columns):
            if 'COUNTY' in str(col_name).upper() and 'BALLOTS' in str(col_name).upper():
                county_name = str(col_name).split('/')[0].strip()
                if county_name.upper().endswith(' COUNTY'):
                    county_name = county_name[:-7].strip()
                county_name = county_name.title()
                # Total is 3 columns after the county name column
                county_columns.append((county_name, col_idx, col_idx + 3))

        # If no counties found in headers, check first column for COUNTY/BALLOTS CAST
        if not county_columns:
            for idx, row in df.iterrows():
                first_col = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                if 'COUNTY/BALLOTS CAST' in first_col.upper() or 'COUNTY / BALLOTS CAST' in first_col.upper():
                    county_name = first_col.split('/')[0].strip()
                    if county_name.upper().endswith(' COUNTY'):
                        county_name = county_name[:-7].strip()
                    county_name = county_name.title()
                    county_columns.append((county_name, 0, 3))
                    break

        # If still no counties found, try to infer from sheet name
        if not county_columns:
            sheet_lower = sheet_name.lower()
            SHEET_COUNTY_MAP = {
                'belk': 'Belknap', 'carr': 'Carroll', 'ches': 'Cheshire',
                'coos': 'Coos', 'graf': 'Grafton', 'hill': 'Hillsborough',
                'merr': 'Merrimack', 'rock': 'Rockingham', 'stra': 'Strafford', 'sull': 'Sullivan',
            }
            for key, county in SHEET_COUNTY_MAP.items():
                if key in sheet_lower:
                    county_columns.append((county, 0, 3))
                    break

        # Parse each county column set
        for county_name, name_col, total_col in county_columns:
            for idx, row in df.iterrows():
                try:
                    municipality = str(row.iloc[name_col]) if pd.notna(row.iloc[name_col]) else ""

                    # Skip header rows, empty rows, dates, totals
                    if not municipality or municipality == "nan":
                        continue
                    if hasattr(row.iloc[name_col], 'strftime'):  # datetime
                        continue
                    municipality_upper = municipality.upper().strip()
                    if municipality_upper in ["TOTALS", "TOTAL", "REGULAR", "ABSENTEE", "NAN"]:
                        continue
                    if "COUNTY" in municipality_upper:
                        continue
                    if "TOTAL" in municipality_upper:  # Catch any variation
                        continue

                    # Get total
                    if total_col < len(row) and pd.notna(row.iloc[total_col]):
                        val = row.iloc[total_col]
                        if isinstance(val, (int, float)) and val > 0:
                            records.append((county_name, municipality.strip(), int(val)))
                except (ValueError, IndexError, TypeError):
                    continue

    return records


def import_ballots_cast():
    """Import all ballots cast files into the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Clear existing ballots_cast data
    cursor.execute("DELETE FROM voter_registration")

    total_imported = 0

    for year, filepath in BALLOTS_FILES.items():
        if not Path(filepath).exists():
            print(f"Warning: File not found for {year}: {filepath}")
            continue

        election_id = ELECTION_IDS.get(year)
        if not election_id:
            print(f"Warning: No election ID for {year}")
            continue

        print(f"\nProcessing {year}...")
        records = parse_ballots_file(filepath, year)

        for county, municipality, ballots_cast in records:
            try:
                cursor.execute("""
                    INSERT INTO voter_registration (election_id, county, municipality, ballots_cast)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(election_id, county, municipality) DO UPDATE SET ballots_cast = ?
                """, (election_id, county, municipality, ballots_cast, ballots_cast))
            except sqlite3.Error as e:
                print(f"Error inserting {municipality}, {county}: {e}")

        conn.commit()
        print(f"  Imported {len(records)} records for {year}")
        total_imported += len(records)

    conn.close()
    print(f"\nTotal imported: {total_imported} records")


if __name__ == "__main__":
    import_ballots_cast()
