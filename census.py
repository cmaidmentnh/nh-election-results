"""
Census data integration for NH Election Results.
Uses ACS 5-year estimates from Census API.
"""

import requests
import json
import os
from functools import lru_cache

# Census API endpoint
CENSUS_API = "https://api.census.gov/data/2023/acs/acs5"

# Variables to fetch
# B01001_001E: Total population
# B19013_001E: Median household income
# B25077_001E: Median home value
# B15003_022E: Bachelor's degree
# B15003_023E: Master's degree
# B15003_024E: Professional degree
# B15003_025E: Doctorate
# B15003_001E: Total 25+ for education denominator
# B01002_001E: Median age
# B02001_002E: White alone
# B02001_001E: Total for race denominator

VARIABLES = [
    "NAME",
    "B01001_001E",  # Total population
    "B19013_001E",  # Median household income
    "B25077_001E",  # Median home value
    "B01002_001E",  # Median age
    "B15003_001E",  # Education: Total 25+
    "B15003_022E",  # Bachelor's
    "B15003_023E",  # Master's
    "B15003_024E",  # Professional
    "B15003_025E",  # Doctorate
    "B02001_001E",  # Race: Total
    "B02001_002E",  # White alone
]

# Cache file path
CACHE_FILE = os.path.join(os.path.dirname(__file__), "census_cache.json")


def fetch_census_data():
    """Fetch Census data for all NH county subdivisions (towns) from API."""
    # Use county subdivision for New England states to get all towns
    url = f"{CENSUS_API}?get={','.join(VARIABLES)}&for=county%20subdivision:*&in=state:33"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # First row is headers
        headers = data[0]
        results = {}

        for row in data[1:]:
            record = dict(zip(headers, row))
            # Extract town name from "Townname town, County County, New Hampshire"
            name = record['NAME']
            # Parse: "Alton town, Belknap County, New Hampshire" -> "Alton"
            town = name.split(' town,')[0].split(' city,')[0].strip()

            # Parse numeric values
            pop = safe_int(record.get('B01001_001E'))
            income = safe_int(record.get('B19013_001E'))
            home_value = safe_int(record.get('B25077_001E'))
            median_age = safe_float(record.get('B01002_001E'))

            # Education: college degree or higher
            edu_total = safe_int(record.get('B15003_001E'))
            bachelors = safe_int(record.get('B15003_022E'))
            masters = safe_int(record.get('B15003_023E'))
            professional = safe_int(record.get('B15003_024E'))
            doctorate = safe_int(record.get('B15003_025E'))
            college_plus = bachelors + masters + professional + doctorate

            # Race
            race_total = safe_int(record.get('B02001_001E'))
            white = safe_int(record.get('B02001_002E'))

            results[town] = {
                'population': pop,
                'median_income': income if income > 0 else None,
                'median_home_value': home_value if home_value > 0 else None,
                'median_age': median_age if median_age and median_age > 0 else None,
                'college_pct': round(college_plus / edu_total * 100, 1) if edu_total > 0 else None,
                'white_pct': round(white / race_total * 100, 1) if race_total > 0 else None,
            }

        return results
    except Exception as e:
        print(f"Error fetching Census data: {e}")
        return {}


def safe_int(val):
    """Safely convert to int, handling None and negative sentinel values."""
    try:
        v = int(val)
        return v if v >= 0 else 0
    except (TypeError, ValueError):
        return 0


def safe_float(val):
    """Safely convert to float."""
    try:
        v = float(val)
        return v if v >= 0 else None
    except (TypeError, ValueError):
        return None


def load_census_data():
    """Load Census data from cache or fetch from API."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)

    data = fetch_census_data()
    if data:
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    return data


def refresh_census_cache():
    """Force refresh of Census data cache."""
    data = fetch_census_data()
    if data:
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    return data


@lru_cache(maxsize=1)
def get_census_data():
    """Get Census data (cached in memory)."""
    return load_census_data()


def get_town_demographics(town):
    """Get demographics for a single town."""
    data = get_census_data()
    return data.get(town, {})


def get_district_demographics(towns):
    """
    Aggregate demographics for a list of towns.
    Uses population-weighted averages where appropriate.
    """
    data = get_census_data()

    total_pop = 0
    weighted_income = 0
    weighted_home_value = 0
    weighted_age = 0
    weighted_college = 0
    weighted_white = 0

    income_pop = 0
    home_value_pop = 0
    age_pop = 0
    college_pop = 0
    white_pop = 0

    for town in towns:
        t = data.get(town, {})
        pop = t.get('population', 0) or 0
        if pop <= 0:
            continue

        total_pop += pop

        if t.get('median_income'):
            weighted_income += t['median_income'] * pop
            income_pop += pop

        if t.get('median_home_value'):
            weighted_home_value += t['median_home_value'] * pop
            home_value_pop += pop

        if t.get('median_age'):
            weighted_age += t['median_age'] * pop
            age_pop += pop

        if t.get('college_pct') is not None:
            weighted_college += t['college_pct'] * pop
            college_pop += pop

        if t.get('white_pct') is not None:
            weighted_white += t['white_pct'] * pop
            white_pop += pop

    return {
        'population': total_pop,
        'median_income': round(weighted_income / income_pop) if income_pop > 0 else None,
        'median_home_value': round(weighted_home_value / home_value_pop) if home_value_pop > 0 else None,
        'median_age': round(weighted_age / age_pop, 1) if age_pop > 0 else None,
        'college_pct': round(weighted_college / college_pop, 1) if college_pop > 0 else None,
        'white_pct': round(weighted_white / white_pop, 1) if white_pop > 0 else None,
    }


def get_statewide_demographics():
    """Get aggregated demographics for all of NH."""
    data = get_census_data()
    return get_district_demographics(list(data.keys()))


if __name__ == "__main__":
    # Test/refresh cache
    print("Fetching Census data...")
    data = refresh_census_cache()
    print(f"Loaded {len(data)} places")

    # Test a few towns
    for town in ['Manchester', 'Concord', 'Portsmouth', 'Nashua', 'Conway']:
        demo = get_town_demographics(town)
        if demo:
            print(f"\n{town}:")
            print(f"  Pop: {demo.get('population', 'N/A'):,}")
            print(f"  Income: ${demo.get('median_income', 'N/A'):,}" if demo.get('median_income') else "  Income: N/A")
            print(f"  College: {demo.get('college_pct', 'N/A')}%")
