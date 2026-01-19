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

# Variables to fetch - see https://api.census.gov/data/2023/acs/acs5/variables.html
VARIABLES = [
    "NAME",
    # Population & Age
    "B01001_001E",  # Total population
    "B01002_001E",  # Median age
    # Income & Housing
    "B19013_001E",  # Median household income
    "B25077_001E",  # Median home value
    "B25003_001E",  # Housing tenure: Total occupied units
    "B25003_002E",  # Owner occupied
    # Education (25+)
    "B15003_001E",  # Total 25+
    "B15003_022E",  # Bachelor's
    "B15003_023E",  # Master's
    "B15003_024E",  # Professional
    "B15003_025E",  # Doctorate
    # Race
    "B02001_001E",  # Total
    "B02001_002E",  # White alone
    "B02001_003E",  # Black alone
    "B02001_005E",  # Asian alone
    "B03001_003E",  # Hispanic/Latino (any race)
    # Veterans
    "B21001_001E",  # Civilian 18+ total
    "B21001_002E",  # Veterans
    # Foreign born
    "B05002_001E",  # Total for nativity
    "B05002_013E",  # Foreign born
    # Poverty
    "B17001_001E",  # Poverty status total
    "B17001_002E",  # Below poverty level
    # Employment
    "B23025_001E",  # Employment total 16+
    "B23025_005E",  # Unemployed
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
            black = safe_int(record.get('B02001_003E'))
            asian = safe_int(record.get('B02001_005E'))
            hispanic = safe_int(record.get('B03001_003E'))

            # Housing
            housing_total = safe_int(record.get('B25003_001E'))
            owner_occupied = safe_int(record.get('B25003_002E'))

            # Veterans
            civ_18_total = safe_int(record.get('B21001_001E'))
            veterans = safe_int(record.get('B21001_002E'))

            # Foreign born
            nativity_total = safe_int(record.get('B05002_001E'))
            foreign_born = safe_int(record.get('B05002_013E'))

            # Poverty
            poverty_total = safe_int(record.get('B17001_001E'))
            below_poverty = safe_int(record.get('B17001_002E'))

            # Employment
            labor_total = safe_int(record.get('B23025_001E'))
            unemployed = safe_int(record.get('B23025_005E'))

            results[town] = {
                'population': pop,
                'median_income': income if income > 0 else None,
                'median_home_value': home_value if home_value > 0 else None,
                'median_age': median_age if median_age and median_age > 0 else None,
                'college_pct': round(college_plus / edu_total * 100, 1) if edu_total > 0 else None,
                # Race
                'white_pct': round(white / race_total * 100, 1) if race_total > 0 else None,
                'black_pct': round(black / race_total * 100, 1) if race_total > 0 else None,
                'asian_pct': round(asian / race_total * 100, 1) if race_total > 0 else None,
                'hispanic_pct': round(hispanic / pop * 100, 1) if pop > 0 else None,
                # Other
                'homeowner_pct': round(owner_occupied / housing_total * 100, 1) if housing_total > 0 else None,
                'veteran_pct': round(veterans / civ_18_total * 100, 1) if civ_18_total > 0 else None,
                'foreign_born_pct': round(foreign_born / nativity_total * 100, 1) if nativity_total > 0 else None,
                'poverty_pct': round(below_poverty / poverty_total * 100, 1) if poverty_total > 0 else None,
                'unemployment_pct': round(unemployed / labor_total * 100, 1) if labor_total > 0 else None,
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

    # Weighted accumulators and population trackers for each metric
    metrics = {
        'median_income': {'weighted': 0, 'pop': 0},
        'median_home_value': {'weighted': 0, 'pop': 0},
        'median_age': {'weighted': 0, 'pop': 0},
        'college_pct': {'weighted': 0, 'pop': 0},
        'white_pct': {'weighted': 0, 'pop': 0},
        'black_pct': {'weighted': 0, 'pop': 0},
        'asian_pct': {'weighted': 0, 'pop': 0},
        'hispanic_pct': {'weighted': 0, 'pop': 0},
        'homeowner_pct': {'weighted': 0, 'pop': 0},
        'veteran_pct': {'weighted': 0, 'pop': 0},
        'foreign_born_pct': {'weighted': 0, 'pop': 0},
        'poverty_pct': {'weighted': 0, 'pop': 0},
        'unemployment_pct': {'weighted': 0, 'pop': 0},
    }

    for town in towns:
        t = data.get(town, {})
        pop = t.get('population', 0) or 0
        if pop <= 0:
            continue

        total_pop += pop

        for key in metrics:
            if t.get(key) is not None:
                metrics[key]['weighted'] += t[key] * pop
                metrics[key]['pop'] += pop

    result = {'population': total_pop}
    for key, vals in metrics.items():
        if vals['pop'] > 0:
            if key in ('median_income', 'median_home_value'):
                result[key] = round(vals['weighted'] / vals['pop'])
            else:
                result[key] = round(vals['weighted'] / vals['pop'], 1)
        else:
            result[key] = None

    return result


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
