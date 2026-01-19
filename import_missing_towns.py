#!/usr/bin/env python3
"""
Re-import data for towns that share county names.
These towns were incorrectly deleted: Hillsborough, Carroll, Grafton, Strafford, Sullivan, Merrimack
"""

import sqlite3
import pandas as pd
import re
from pathlib import Path

DB_PATH = Path(__file__).parent / "nh_elections.db"
ELECTION_FILES = Path("/Users/chrismaidment/Desktop/Data-Elections/election_files")

# Towns to restore (share names with counties)
TARGET_TOWNS = {'Hillsborough', 'Carroll', 'Grafton', 'Strafford', 'Sullivan', 'Merrimack'}

# Map county to files (using the most recent version with highest suffix number)
COUNTY_FILES = {
    2024: {
        'Hillsborough': '2024-ge-house-hillsborough_7.xlsx',
        'Coos': '2024-ge-house-coos_0.xlsx',
        'Grafton': '2024-ge-house-grafton_2.xlsx',
        'Strafford': '2024-ge-house-strafford_3.xlsx',
        'Cheshire': '2024-ge-house-cheshire_0.xlsx',
    },
    2022: {
        'Hillsborough': '2022-ge-house-hillsborough_12.xlsx',
        'Coos': '2022-ge-house-coos_1.xlsx',
        'Grafton': '2022-ge-house-grafton_1.xlsx',
        'Strafford': '2022-ge-house-strafford_0.xlsx',
        'Cheshire': '2022-ge-house-cheshire_0.xlsx',
    },
    2020: {
        'Hillsborough': '2020-ge-house-hillsborough.xlsx',
        'Coos': '2020-ge-house-coos.xlsx',
        'Grafton': '2020-ge-house-grafton.xlsx',
        'Strafford': '2020-ge-house-strafford.xlsx',
        'Cheshire': '2020-ge-house-cheshire.xlsx',
    },
    2018: {
        'Hillsborough': '2018-ge-house-hillsborough_0.xlsx',
        'Coos': '2018-ge-house-coos.xlsx',
        'Grafton': '2018-ge-house-grafton.xlsx',
        'Strafford': '2018-ge-house-strafford.xlsx',
        'Cheshire': '2018-ge-house-cheshire_0.xlsx',
    },
    2016: {
        'Hillsborough': '2016-ge-house-hillsborough.xlsx',
        'Coos': '2016-ge-house-coos.xlsx',
        'Grafton': '2016-ge-house-grafton.xlsx',
        'Strafford': '2016-ge-house-strafford.xlsx',
        'Cheshire': '2016-ge-house-cheshire.xlsx',
    },
}

# Map town -> county it's actually in
TOWN_COUNTY = {
    'Hillsborough': 'Hillsborough',
    'Merrimack': 'Hillsborough',
    'Carroll': 'Coos',
    'Grafton': 'Grafton',
    'Strafford': 'Strafford',
    'Sullivan': 'Cheshire',
}

# Senate files by year
SENATE_FILES = {
    2024: '2024-ge-state-senate-district-1-24.xls',
    2022: '2022-ge-state-senate-district-1-24_1.xls',
    2020: ['2020-state-senate-district-1-11.xls', '2020-state-senate-district-12-24.xls'],
    2018: '2018-ge-state-senate-district-9-24.xls',  # Has districts 9-24
    2016: '2016-ge-state-senate-districts-9-11.xls',  # Has districts 9-11
}

# Statewide office files (Governor, Congress, Exec Council, President)
STATEWIDE_FILES = {
    2024: {
        'Governor': '2024-ge-governor_3.xls',
        'Representative in Congress': ['2024-ge-congressional-district-1_3.xlsx', '2024-ge-congressional-district-2_4.xlsx'],
        'Executive Councilor': '2024-ge-executive-council-district-1-5_4.xls',
    },
    2022: {
        'Governor': '2022-ge-governor_2.xls',
        'Representative in Congress': ['2022-ge-congressional-district-1.xls', '2022-ge-congressional-district-2.xls'],
        'Executive Councilor': '2022-executive-council-district-1-5_0.xls',
    },
    2020: {
        'Governor': '2020-governor.xls',
        'Representative in Congress': ['2020-congressional-district-1.xlsx', '2020-congressional-district-2.xlsx'],
        'Executive Councilor': '2020-executive-council-district-1-5.xls',
        'President of the United States': '2020-president.xls',
    },
}


def get_election_id(cursor, year):
    """Get election ID for a general election year."""
    cursor.execute(
        "SELECT id FROM elections WHERE year = ? AND election_type = 'general' AND party IS NULL",
        (year,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_or_create_candidate(cursor, name, party):
    """Get or create a candidate."""
    name_normalized = name.upper().strip()
    cursor.execute(
        "SELECT id FROM candidates WHERE name_normalized = ? AND party = ?",
        (name_normalized, party)
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        "INSERT INTO candidates (name, name_normalized, party) VALUES (?, ?, ?)",
        (name.strip(), name_normalized, party)
    )
    return cursor.lastrowid


def get_race_id(cursor, election_id, office_id, district, county):
    """Get race ID for State Representative race."""
    cursor.execute(
        "SELECT id FROM races WHERE election_id = ? AND office_id = ? AND district = ? AND county = ?",
        (election_id, office_id, str(district), county)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def parse_candidate_name(name_str):
    """Parse candidate name like 'John Smith, r' or 'Jane Doe, d'."""
    name_str = str(name_str).strip()
    if not name_str or name_str == 'nan':
        return None, None

    # Match pattern: Name, party_letter
    match = re.match(r'^(.+?),?\s*\(?(r|d)\)?$', name_str, re.IGNORECASE)
    if match:
        name = match.group(1).strip().rstrip(',')
        party_letter = match.group(2).lower()
        party = 'Republican' if party_letter == 'r' else 'Democratic'
        return name, party

    return None, None


def parse_xls_file(filepath, county, year):
    """Parse an XLS file and extract results for target towns."""
    results = []

    try:
        xls = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"  Error opening {filepath}: {e}")
        return results

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        current_district = None
        current_seats = 0
        current_candidates = []  # List of (name, party, column_index)

        i = 0
        while i < len(df):
            row = df.iloc[i]
            first_col = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''

            # Check for district header
            dist_match = re.match(r'District No\.?\s*(\d+)\s*\((\d+)\)', first_col)
            if dist_match:
                current_district = dist_match.group(1)
                current_seats = int(dist_match.group(2))
                current_candidates = []

                # Parse candidate names from this row
                for col_idx in range(1, len(row)):
                    cell = row.iloc[col_idx]
                    if pd.notna(cell):
                        cell_str = str(cell).strip()
                        if cell_str and cell_str not in ['Undervotes', 'Overvotes', 'Write-Ins', ' ']:
                            name, party = parse_candidate_name(cell_str)
                            if name and party:
                                current_candidates.append((name, party, col_idx))

                i += 1
                continue

            # Check if this row has a target town in first column
            if first_col in TARGET_TOWNS and current_district:
                municipality = first_col

                # This row has vote data for the candidates we've collected
                for name, party, col_idx in current_candidates:
                    if col_idx < len(row):
                        votes = row.iloc[col_idx]
                        if pd.notna(votes):
                            try:
                                votes = int(float(votes))
                                if votes > 0:
                                    results.append({
                                        'year': year,
                                        'county': county,
                                        'district': current_district,
                                        'municipality': municipality,
                                        'candidate': name,
                                        'party': party,
                                        'votes': votes
                                    })
                            except (ValueError, TypeError):
                                pass

                i += 1
                continue

            # Check if this is a continuation row (more candidates)
            # These start with empty first column but have candidate names
            if not first_col and current_district:
                new_candidates = []
                for col_idx in range(1, len(row)):
                    cell = row.iloc[col_idx]
                    if pd.notna(cell):
                        cell_str = str(cell).strip()
                        if cell_str and cell_str not in ['Undervotes', 'Overvotes', 'Write-Ins', ' ']:
                            name, party = parse_candidate_name(cell_str)
                            if name and party:
                                new_candidates.append((name, party, col_idx))

                if new_candidates:
                    current_candidates = new_candidates

            i += 1

    return results


def parse_statewide_file(filepath, year, office):
    """Parse a statewide office XLS file (Governor, Congress, Exec Council, President)."""
    results = []

    try:
        xls = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"  Error opening {filepath}: {e}")
        return results

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        # Try to get district from sheet name (e.g., "council 1", "congress 2")
        sheet_dist_match = re.search(r'(\d+)', sheet_name)
        current_district = sheet_dist_match.group(1) if sheet_dist_match else None
        candidates = []  # List of (name, party, column_index)

        for i, row in df.iterrows():
            first_col = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''

            # Check for district header in row (for Congress/Exec Council)
            dist_match = re.search(r'District\s*(?:No\.?\s*)?(\d+)', first_col, re.IGNORECASE)
            if dist_match:
                current_district = dist_match.group(1)
                candidates = []
                continue

            # Check for candidate header row
            has_candidates = False
            new_candidates = []
            for col_idx in range(1, min(8, len(row))):
                cell = row.iloc[col_idx] if col_idx < len(row) else None
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    name, party = parse_candidate_name(cell_str)
                    if name and party:
                        has_candidates = True
                        new_candidates.append((name, party, col_idx))

            if has_candidates:
                candidates = new_candidates
                continue

            # Check if this row has a target town
            if first_col in TARGET_TOWNS and candidates:
                for name, party, col_idx in candidates:
                    if col_idx < len(row):
                        votes = row.iloc[col_idx]
                        if pd.notna(votes):
                            try:
                                votes = int(float(votes))
                                if votes > 0:
                                    results.append({
                                        'year': year,
                                        'office': office,
                                        'district': current_district,
                                        'municipality': first_col,
                                        'candidate': name,
                                        'party': party,
                                        'votes': votes
                                    })
                            except (ValueError, TypeError):
                                pass

    return results


def parse_senate_file(filepath, year):
    """Parse a Senate XLS file and extract results for target towns."""
    results = []

    try:
        xls = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"  Error opening {filepath}: {e}")
        return results

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        current_district = None
        candidates = []  # List of (name, party, column_index)

        for i, row in df.iterrows():
            first_col = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''

            # Check for district header like "State Senate District 11" or just row with district number
            dist_match = re.search(r'District\s*(\d+)', first_col, re.IGNORECASE)
            if dist_match:
                current_district = dist_match.group(1)
                candidates = []
                continue

            # Check for candidate header row (contains names ending in ", r" or ", d")
            has_candidates = False
            for col_idx in range(1, min(6, len(row))):
                cell = row.iloc[col_idx] if col_idx < len(row) else None
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    name, party = parse_candidate_name(cell_str)
                    if name and party:
                        has_candidates = True
                        candidates.append((name, party, col_idx))

            if has_candidates:
                continue

            # Check if this row has a target town
            if first_col in TARGET_TOWNS and current_district and candidates:
                for name, party, col_idx in candidates:
                    if col_idx < len(row):
                        votes = row.iloc[col_idx]
                        if pd.notna(votes):
                            try:
                                votes = int(float(votes))
                                if votes > 0:
                                    results.append({
                                        'year': year,
                                        'district': current_district,
                                        'municipality': first_col,
                                        'candidate': name,
                                        'party': party,
                                        'votes': votes
                                    })
                            except (ValueError, TypeError):
                                pass

    return results


def import_missing_towns():
    """Main import function."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get office ID for State Representative
    cursor.execute("SELECT id FROM offices WHERE name = 'State Representative'")
    office_row = cursor.fetchone()
    if not office_row:
        print("Error: State Representative office not found!")
        return
    rep_office_id = office_row[0]

    # Get office ID for State Senator
    cursor.execute("SELECT id FROM offices WHERE name = 'State Senator'")
    office_row = cursor.fetchone()
    if not office_row:
        print("Error: State Senator office not found!")
        return
    senator_office_id = office_row[0]

    total_imported = 0

    # Import House data
    for year, county_files in COUNTY_FILES.items():
        election_id = get_election_id(cursor, year)
        if not election_id:
            print(f"Warning: No election found for {year}")
            continue

        print(f"\n=== {year} General Election (ID: {election_id}) ===")

        for county, filename in county_files.items():
            filepath = ELECTION_FILES / filename

            # Try alternate filenames if primary doesn't exist
            if not filepath.exists():
                # Try without suffix
                alt_name = re.sub(r'_\d+\.xlsx$', '.xlsx', filename)
                filepath = ELECTION_FILES / alt_name
            if not filepath.exists():
                alt_name = filename.replace('.xlsx', '.xls')
                filepath = ELECTION_FILES / alt_name

            if not filepath.exists():
                print(f"  {county}: File not found ({filename})")
                continue

            print(f"  {county}: Parsing {filepath.name}...")
            results = parse_xls_file(filepath, county, year)

            if not results:
                print(f"    No target town data found")
                continue

            # Insert results
            inserted = 0
            for r in results:
                # Get or create candidate
                candidate_id = get_or_create_candidate(cursor, r['candidate'], r['party'])

                # Get race ID
                race_id = get_race_id(cursor, election_id, rep_office_id, r['district'], r['county'])
                if not race_id:
                    print(f"    Warning: Race not found for {r['county']} District {r['district']}")
                    continue

                # Insert result (using INSERT OR REPLACE to handle duplicates)
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO results (race_id, candidate_id, municipality, votes)
                        VALUES (?, ?, ?, ?)
                    """, (race_id, candidate_id, r['municipality'], r['votes']))
                    inserted += 1
                except sqlite3.Error as e:
                    print(f"    Error inserting {r['candidate']} in {r['municipality']}: {e}")

            conn.commit()
            print(f"    Inserted {inserted} result rows")
            total_imported += inserted

    # Import Senate data
    print("\n\n=== IMPORTING STATE SENATE DATA ===")
    for year, filenames in SENATE_FILES.items():
        election_id = get_election_id(cursor, year)
        if not election_id:
            print(f"Warning: No election found for {year}")
            continue

        # Handle both single filename and list of filenames
        if isinstance(filenames, str):
            filenames = [filenames]

        results = []
        for filename in filenames:
            filepath = ELECTION_FILES / filename
            if not filepath.exists():
                # Try alternate extensions
                alt_name = filename.replace('.xls', '.xlsx')
                filepath = ELECTION_FILES / alt_name
            if not filepath.exists():
                print(f"  {year}: Senate file not found ({filename})")
                continue

            print(f"\n{year} Senate: Parsing {filepath.name}...")
            results.extend(parse_senate_file(filepath, year))

        if not results:
            print(f"  No target town data found")
            continue

        # Insert results
        inserted = 0
        for r in results:
            # Get or create candidate
            candidate_id = get_or_create_candidate(cursor, r['candidate'], r['party'])

            # Get race ID for Senate (no county for statewide districts)
            cursor.execute(
                "SELECT id FROM races WHERE election_id = ? AND office_id = ? AND district = ?",
                (election_id, senator_office_id, str(r['district']))
            )
            row = cursor.fetchone()
            if not row:
                print(f"  Warning: Senate race not found for District {r['district']}")
                continue
            race_id = row[0]

            # Insert result
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO results (race_id, candidate_id, municipality, votes)
                    VALUES (?, ?, ?, ?)
                """, (race_id, candidate_id, r['municipality'], r['votes']))
                inserted += 1
            except sqlite3.Error as e:
                print(f"  Error inserting {r['candidate']} in {r['municipality']}: {e}")

        conn.commit()
        print(f"  Inserted {inserted} result rows")
        total_imported += inserted

    # Import statewide office data (Governor, Congress, Exec Council, President)
    print("\n\n=== IMPORTING STATEWIDE OFFICES ===")
    for year, offices in STATEWIDE_FILES.items():
        election_id = get_election_id(cursor, year)
        if not election_id:
            print(f"Warning: No election found for {year}")
            continue

        for office_name, filenames in offices.items():
            # Get office ID
            cursor.execute("SELECT id FROM offices WHERE name = ?", (office_name,))
            office_row = cursor.fetchone()
            if not office_row:
                print(f"  Warning: Office '{office_name}' not found")
                continue
            office_id = office_row[0]

            # Handle both single filename and list
            if isinstance(filenames, str):
                filenames = [filenames]

            results = []
            for filename in filenames:
                filepath = ELECTION_FILES / filename
                if not filepath.exists():
                    alt_name = filename.replace('.xls', '.xlsx')
                    filepath = ELECTION_FILES / alt_name
                if not filepath.exists():
                    alt_name = filename.replace('.xlsx', '.xls')
                    filepath = ELECTION_FILES / alt_name
                if not filepath.exists():
                    print(f"  {year} {office_name}: File not found ({filename})")
                    continue

                print(f"  {year} {office_name}: Parsing {filepath.name}...")
                results.extend(parse_statewide_file(filepath, year, office_name))

            if not results:
                continue

            # Insert results
            inserted = 0
            for r in results:
                candidate_id = get_or_create_candidate(cursor, r['candidate'], r['party'])

                # Get race ID - for statewide offices without districts (Governor, President)
                if r['district']:
                    cursor.execute(
                        "SELECT id FROM races WHERE election_id = ? AND office_id = ? AND district = ?",
                        (election_id, office_id, str(r['district']))
                    )
                else:
                    cursor.execute(
                        "SELECT id FROM races WHERE election_id = ? AND office_id = ? AND district IS NULL",
                        (election_id, office_id)
                    )
                row = cursor.fetchone()
                if not row:
                    print(f"    Warning: Race not found for {office_name} District {r['district']}")
                    continue
                race_id = row[0]

                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO results (race_id, candidate_id, municipality, votes)
                        VALUES (?, ?, ?, ?)
                    """, (race_id, candidate_id, r['municipality'], r['votes']))
                    inserted += 1
                except sqlite3.Error as e:
                    print(f"    Error inserting {r['candidate']} in {r['municipality']}: {e}")

            conn.commit()
            if inserted > 0:
                print(f"    Inserted {inserted} result rows")
            total_imported += inserted

    conn.close()
    print(f"\n=== Total imported: {total_imported} result rows ===")


if __name__ == "__main__":
    import_missing_towns()
