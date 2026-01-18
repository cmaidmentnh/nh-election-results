#!/usr/bin/env python3
"""
Analysis functions for NH Election Results Explorer
Generates meaningful insights from election data
"""

import sqlite3
from pathlib import Path
from collections import defaultdict

DB_PATH = Path(__file__).parent / "nh_elections.db"

# Office ordering by importance (lower = more important)
# POTUS > GOV > US SEN > US REP > EC > SEN > REP
OFFICE_ORDER = {
    'President of the United States': 1,
    'Governor': 2,
    'United States Senator': 3,
    'United States Representative': 4,
    'Representative in Congress': 4,  # Alternative name
    'Executive Councilor': 5,
    'State Senator': 6,
    'State Representative': 7,
}


def get_office_sort_key(office_name):
    """Return sort key for office ordering."""
    return OFFICE_ORDER.get(office_name, 99)


def classify_lean(margin):
    """Classify a margin into a lean category."""
    if margin > 15:
        return "Safe R"
    elif margin > 8:
        return "Likely R"
    elif margin > 3:
        return "Lean R"
    elif margin > -3:
        return "Toss-up"
    elif margin > -8:
        return "Lean D"
    elif margin > -15:
        return "Likely D"
    else:
        return "Safe D"


def get_trend_arrow(change):
    """Return trend arrow based on margin change."""
    if change > 2:
        return "↗"  # Trending R
    elif change < -2:
        return "↘"  # Trending D
    else:
        return "→"  # Stable


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_town_summary(town):
    """
    Get a comprehensive summary of a town's voting patterns.
    Returns headline insights, not raw data.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all results for this town
    cursor.execute("""
        SELECT
            e.year,
            o.name as office,
            r.district,
            r.county,
            c.name as candidate,
            c.party,
            res.votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        ORDER BY e.year, o.name
    """, (town,))

    results = cursor.fetchall()
    if not results:
        conn.close()
        return None

    # Aggregate by year and office
    by_year = defaultdict(lambda: defaultdict(lambda: {'R': 0, 'D': 0, 'Other': 0, 'total': 0}))

    for row in results:
        year = row['year']
        office = row['office']
        party = row['party']
        votes = row['votes']

        if party == 'Republican':
            by_year[year][office]['R'] += votes
        elif party == 'Democratic':
            by_year[year][office]['D'] += votes
        else:
            by_year[year][office]['Other'] += votes
        by_year[year][office]['total'] += votes

    # Calculate margins by year
    years = sorted(by_year.keys())
    margins_by_year = {}

    for year in years:
        total_r = sum(d['R'] for d in by_year[year].values())
        total_d = sum(d['D'] for d in by_year[year].values())
        total_all = sum(d['total'] for d in by_year[year].values())

        if total_all > 0:
            r_pct = (total_r / total_all) * 100
            d_pct = (total_d / total_all) * 100
            margin = r_pct - d_pct
            margins_by_year[year] = {
                'r_pct': round(r_pct, 1),
                'd_pct': round(d_pct, 1),
                'margin': round(margin, 1),
                'r_votes': total_r,
                'd_votes': total_d,
                'total_votes': total_all
            }

    # Calculate trend (change from first to last year)
    if len(years) >= 2:
        first_margin = margins_by_year[years[0]]['margin']
        last_margin = margins_by_year[years[-1]]['margin']
        trend = last_margin - first_margin
        trend_direction = 'R' if trend > 0 else 'D' if trend < 0 else 'stable'
    else:
        trend = 0
        trend_direction = 'stable'

    # Detect ticket splitting
    ticket_splits = []
    for year in years:
        offices = by_year[year]

        # Find top of ticket winner
        top_ticket = None
        top_ticket_office = None
        for office in ['President of the United States', 'Governor']:
            if office in offices:
                data = offices[office]
                if data['R'] > data['D']:
                    top_ticket = 'R'
                elif data['D'] > data['R']:
                    top_ticket = 'D'
                top_ticket_office = office
                break

        if top_ticket:
            # Check down-ballot
            for office in ['State Representative', 'State Senator', 'Executive Councilor']:
                if office in offices:
                    data = offices[office]
                    if data['R'] > data['D']:
                        down_ballot = 'R'
                    elif data['D'] > data['R']:
                        down_ballot = 'D'
                    else:
                        continue

                    if down_ballot != top_ticket:
                        ticket_splits.append({
                            'year': year,
                            'top_ticket': f"{top_ticket_office}: {'Republican' if top_ticket == 'R' else 'Democratic'}",
                            'down_ballot': f"{office}: {'Republican' if down_ballot == 'R' else 'Democratic'}"
                        })

    # Get county
    cursor.execute("""
        SELECT DISTINCT r.county
        FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE res.municipality = ? AND r.county IS NOT NULL
        LIMIT 1
    """, (town,))
    row = cursor.fetchone()
    county = row[0] if row else None

    conn.close()

    # Build summary
    latest_year = years[-1]
    latest_margin = margins_by_year[latest_year]

    # Generate headline
    if latest_margin['margin'] > 10:
        lean = f"Strong R+{int(latest_margin['margin'])}"
    elif latest_margin['margin'] > 3:
        lean = f"Lean R+{int(latest_margin['margin'])}"
    elif latest_margin['margin'] > -3:
        lean = "Competitive (swing)"
    elif latest_margin['margin'] > -10:
        lean = f"Lean D+{int(abs(latest_margin['margin']))}"
    else:
        lean = f"Strong D+{int(abs(latest_margin['margin']))}"

    return {
        'name': town,
        'county': county,
        'lean': lean,
        'latest_year': latest_year,
        'latest_margin': latest_margin,
        'margins_by_year': margins_by_year,
        'years': years,
        'trend': round(trend, 1),
        'trend_direction': trend_direction,
        'ticket_splits': ticket_splits,
        'by_year': dict(by_year)
    }


def get_town_race_details(town, year):
    """Get detailed race results for a specific town and year."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get results grouped by race
    cursor.execute("""
        SELECT
            o.name as office,
            r.id as race_id,
            r.district,
            r.county,
            r.seats,
            c.id as candidate_id,
            c.name as candidate,
            c.party,
            res.votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.year = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        ORDER BY o.name, r.district, res.votes DESC
    """, (town, year))

    results = cursor.fetchall()

    # Get district-wide winners for each race
    race_ids = set(r['race_id'] for r in results)
    if not race_ids:
        conn.close()
        return []

    placeholders = ','.join('?' * len(race_ids))
    cursor.execute(f"""
        SELECT
            r.id as race_id,
            r.seats,
            c.id as candidate_id,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        WHERE r.id IN ({placeholders})
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.id, c.id
        ORDER BY r.id, total_votes DESC
    """, list(race_ids))

    # Build winner lookup - handle ties correctly
    # Group candidates by race first
    race_candidates = defaultdict(list)
    race_seats = {}
    for row in cursor.fetchall():
        race_id, seats, candidate_id, total_votes = row
        race_candidates[race_id].append({'id': candidate_id, 'votes': total_votes})
        race_seats[race_id] = seats

    # Determine winners, excluding ties at cutoff
    winners = {}
    for race_id, candidates in race_candidates.items():
        seats = race_seats[race_id]
        winners[race_id] = set()

        # Candidates already sorted by votes DESC
        for i, cand in enumerate(candidates):
            if i < seats:
                # Check for tie at cutoff
                if i == seats - 1 and i + 1 < len(candidates):
                    if cand['votes'] == candidates[i + 1]['votes']:
                        # Tie at cutoff - don't count as winner
                        continue
                winners[race_id].add(cand['id'])

    conn.close()

    # Group results by race
    races = {}
    for row in results:
        race_key = (row['office'], row['district'])
        if race_key not in races:
            races[race_key] = {
                'office': row['office'],
                'district': row['district'],
                'county': row['county'],
                'seats': row['seats'],
                'candidates': []
            }

        is_winner = row['candidate_id'] in winners.get(row['race_id'], set())
        races[race_key]['candidates'].append({
            'name': row['candidate'],
            'party': row['party'],
            'votes': row['votes'],
            'is_winner': is_winner
        })

    # Calculate margins for each race
    # Normalized margin: R% - D% where each is percentage of R+D total only
    # This normalizes for unequal number of candidates per party
    for race_key, race in races.items():
        r_votes = sum(c['votes'] for c in race['candidates'] if c['party'] == 'Republican')
        d_votes = sum(c['votes'] for c in race['candidates'] if c['party'] == 'Democratic')
        total = sum(c['votes'] for c in race['candidates'])
        rd_total = r_votes + d_votes  # R+D only for normalized margin

        if rd_total > 0:
            race['r_votes'] = r_votes
            race['d_votes'] = d_votes
            race['margin'] = r_votes - d_votes
            # Normalized margin: (R-D) / (R+D) * 100
            race['margin_pct'] = round((r_votes - d_votes) / rd_total * 100, 1)
            race['winner_party'] = 'R' if r_votes > d_votes else 'D' if d_votes > r_votes else 'Tie'
        else:
            race['r_votes'] = 0
            race['d_votes'] = 0
            race['margin'] = 0
            race['margin_pct'] = 0
            race['winner_party'] = None

    # Sort races by office importance (POTUS -> US SEN -> US REP -> GOV -> EXEC -> STATE SEN -> STATE REP)
    sorted_races = sorted(races.values(), key=lambda r: (get_office_sort_key(r['office']), r['district'] or ''))
    return sorted_races


def get_statewide_trends():
    """
    Get statewide party control trends over time.
    Handles ties correctly - if there's a tie at the cutoff, neither tied candidate wins.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all race results with vote totals
    cursor.execute("""
        SELECT
            e.year,
            o.name as office,
            r.id as race_id,
            r.seats,
            c.id as candidate_id,
            c.party,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.id, c.id
        ORDER BY e.year, r.id, total_votes DESC
    """)

    # Process race by race to handle ties correctly
    races = defaultdict(list)
    for row in cursor.fetchall():
        year, office, race_id, seats, cand_id, party, votes = row
        races[(year, office, race_id, seats)].append({'party': party, 'votes': votes})

    conn.close()

    # Count winners, handling ties
    results = defaultdict(lambda: defaultdict(lambda: {'R': 0, 'D': 0}))

    for (year, office, race_id, seats), candidates in races.items():
        # Sort by votes DESC
        candidates.sort(key=lambda x: -x['votes'])

        # Find winners - but if there's a tie at the cutoff, exclude tied candidates
        winners = []
        for i, cand in enumerate(candidates):
            if i < seats:
                # Check if this candidate is tied with someone who wouldn't win
                if i == seats - 1 and i + 1 < len(candidates):
                    # Last winning position - check for tie with next candidate
                    if cand['votes'] == candidates[i + 1]['votes']:
                        # Tie at cutoff - this candidate doesn't win
                        continue
                winners.append(cand)
            else:
                break

        # Count by party
        for w in winners:
            if w['party'] == 'Republican':
                results[year][office]['R'] += 1
            elif w['party'] == 'Democratic':
                results[year][office]['D'] += 1

    return dict(results)


def compare_years(town, year1, year2):
    """Compare a town's results between two years."""
    summary1 = get_town_summary(town)
    if not summary1 or year1 not in summary1['margins_by_year'] or year2 not in summary1['margins_by_year']:
        return None

    m1 = summary1['margins_by_year'][year1]
    m2 = summary1['margins_by_year'][year2]

    margin_shift = m2['margin'] - m1['margin']
    turnout_change = m2['total_votes'] - m1['total_votes']
    turnout_pct_change = (turnout_change / m1['total_votes']) * 100 if m1['total_votes'] > 0 else 0

    return {
        'year1': year1,
        'year2': year2,
        'margin1': m1['margin'],
        'margin2': m2['margin'],
        'margin_shift': round(margin_shift, 1),
        'shift_direction': 'R' if margin_shift > 0 else 'D' if margin_shift < 0 else 'none',
        'turnout1': m1['total_votes'],
        'turnout2': m2['total_votes'],
        'turnout_change': turnout_change,
        'turnout_pct_change': round(turnout_pct_change, 1)
    }


def get_party_control(year):
    """Get party control seat counts for legislative offices."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH race_totals AS (
            SELECT
                o.name as office,
                r.id as race_id,
                r.seats,
                c.party,
                SUM(res.votes) as total_votes,
                RANK() OVER (PARTITION BY r.id ORDER BY SUM(res.votes) DESC) as rank
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.year = ?
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND o.name IN ('State Representative', 'State Senator', 'Executive Councilor')
            GROUP BY r.id, c.id
        )
        SELECT office, party, COUNT(*) as seats
        FROM race_totals
        WHERE rank <= seats
        GROUP BY office, party
    """, (year,))

    results = {}
    for row in cursor.fetchall():
        office, party, seats = row
        if office not in results:
            results[office] = {'R': 0, 'D': 0, 'Other': 0}
        if party == 'Republican':
            results[office]['R'] = seats
        elif party == 'Democratic':
            results[office]['D'] = seats
        else:
            results[office]['Other'] += seats

    conn.close()
    return results


def get_closest_races(year, limit=10):
    """Get races with the smallest margins."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH race_totals AS (
            SELECT
                r.id as race_id,
                o.name as office,
                r.district,
                r.county,
                r.seats,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
                SUM(res.votes) as total_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.year = ?
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id
        )
        SELECT
            office, district, county,
            r_votes, d_votes, total_votes,
            CASE WHEN r_votes + d_votes > 0
                THEN ROUND((r_votes - d_votes) * 100.0 / (r_votes + d_votes), 1)
                ELSE 0
            END as margin
        FROM race_totals
        WHERE r_votes > 0 AND d_votes > 0
        ORDER BY ABS(margin)
        LIMIT ?
    """, (year, limit))

    results = []
    for row in cursor.fetchall():
        office, district, county, r_votes, d_votes, total, margin = row
        results.append({
            'office': office,
            'district': district,
            'county': county,
            'r_votes': r_votes,
            'd_votes': d_votes,
            'margin': margin,
            'label': f"{county} {district}" if county else f"District {district}"
        })

    conn.close()
    return results


def get_biggest_shifts(year1, year2, limit=10):
    """Get races with biggest margin shifts between two years."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH race_margins AS (
            SELECT
                e.year,
                o.name as office,
                r.district,
                r.county,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.year IN (?, ?)
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY e.year, r.id
        ),
        margins AS (
            SELECT
                year, office, district, county,
                CASE WHEN r_votes + d_votes > 0
                    THEN (r_votes - d_votes) * 100.0 / (r_votes + d_votes)
                    ELSE 0
                END as margin
            FROM race_margins
            WHERE r_votes > 0 AND d_votes > 0
        )
        SELECT
            m1.office, m1.district, m1.county,
            ROUND(m1.margin, 1) as margin1,
            ROUND(m2.margin, 1) as margin2,
            ROUND(m2.margin - m1.margin, 1) as shift
        FROM margins m1
        JOIN margins m2 ON m1.office = m2.office
            AND COALESCE(m1.district, '') = COALESCE(m2.district, '')
            AND COALESCE(m1.county, '') = COALESCE(m2.county, '')
        WHERE m1.year = ? AND m2.year = ?
        ORDER BY ABS(shift) DESC
        LIMIT ?
    """, (year1, year2, year1, year2, limit))

    results = []
    for row in cursor.fetchall():
        office, district, county, margin1, margin2, shift = row
        results.append({
            'office': office,
            'district': district,
            'county': county,
            'margin1': margin1,
            'margin2': margin2,
            'shift': shift,
            'direction': 'R' if shift > 0 else 'D',
            'label': f"{county} {district}" if county else f"District {district}"
        })

    conn.close()
    return results


def get_county_summary(county):
    """Get summary of a county's voting patterns, towns, and results by race."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get all towns in this county (from races that have this county)
    cursor.execute("""
        SELECT DISTINCT res.municipality
        FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE r.county = ?
        AND res.municipality NOT GLOB '[0-9]*'
        AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS', 'Court ordered recount', 'court ordered recount')
        ORDER BY res.municipality
    """, (county,))
    towns = [row[0] for row in cursor.fetchall()]

    if not towns:
        conn.close()
        return None

    placeholders = ','.join('?' * len(towns))

    # Get individual race results (with district) for these towns
    cursor.execute(f"""
        SELECT
            e.year,
            o.name as office,
            r.district,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality IN ({placeholders})
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, o.name, r.district
        ORDER BY e.year DESC, o.name, r.district
    """, towns)

    # Organize races by year
    races_by_year = defaultdict(list)
    years_set = set()
    presidential_results = []

    for row in cursor.fetchall():
        year, office, district, r_votes, d_votes, total = row
        years_set.add(year)
        rd_total = r_votes + d_votes
        margin = ((r_votes - d_votes) / rd_total * 100) if rd_total > 0 else 0

        race_data = {
            'office': office,
            'district': district,
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total,
            'margin': round(margin, 1)
        }

        # Separate presidential results for the special section
        if office == 'President of the United States':
            race_data['year'] = year
            presidential_results.append(race_data)
        else:
            races_by_year[year].append(race_data)

    # Sort races within each year by office importance
    for year in races_by_year:
        races_by_year[year].sort(key=lambda x: (get_office_sort_key(x['office']), x['district'] or ''))

    # Aggregate by office for summary view
    office_summary_by_year = defaultdict(lambda: defaultdict(lambda: {'r_seats': 0, 'd_seats': 0, 'total_seats': 0, 'r_votes': 0, 'd_votes': 0}))

    # Need to get seat winners from the database
    cursor.execute(f"""
        WITH race_winners AS (
            SELECT
                e.year,
                o.name as office,
                r.id as race_id,
                r.seats,
                c.party,
                SUM(res.votes) as total_votes,
                RANK() OVER (PARTITION BY r.id ORDER BY SUM(res.votes) DESC) as rank
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE res.municipality IN ({placeholders})
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id, c.id
        )
        SELECT year, office, party, COUNT(*) as seats_won,
               SUM(total_votes) as votes
        FROM race_winners
        WHERE rank <= seats
        GROUP BY year, office, party
    """, towns)

    for row in cursor.fetchall():
        year, office, party, seats, votes = row
        if party == 'Republican':
            office_summary_by_year[year][office]['r_seats'] += seats
            office_summary_by_year[year][office]['r_votes'] += votes
        elif party == 'Democratic':
            office_summary_by_year[year][office]['d_seats'] += seats
            office_summary_by_year[year][office]['d_votes'] += votes
        office_summary_by_year[year][office]['total_seats'] += seats

    # Sort presidential results by year descending
    presidential_results.sort(key=lambda x: -x['year'])

    # Get aggregate totals by year (for overall margin)
    cursor.execute(f"""
        SELECT
            e.year,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE res.municipality IN ({placeholders})
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year
        ORDER BY e.year
    """, towns)

    margins_by_year = {}
    for row in cursor.fetchall():
        year, r_votes, d_votes, total = row
        rd_total = r_votes + d_votes
        margin = ((r_votes - d_votes) / rd_total * 100) if rd_total > 0 else 0
        margins_by_year[year] = {
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total,
            'margin': round(margin, 1)
        }

    years = sorted(years_set)
    conn.close()

    if not years:
        return None

    # Calculate trend
    if len(years) >= 2:
        first_margin = margins_by_year[years[0]]['margin']
        last_margin = margins_by_year[years[-1]]['margin']
        trend = last_margin - first_margin
    else:
        trend = 0

    latest_margin = margins_by_year[years[-1]]

    # Generate lean label
    if latest_margin['margin'] > 10:
        lean = f"Strong R+{int(latest_margin['margin'])}"
    elif latest_margin['margin'] > 3:
        lean = f"Lean R+{int(latest_margin['margin'])}"
    elif latest_margin['margin'] > -3:
        lean = "Competitive (swing)"
    elif latest_margin['margin'] > -10:
        lean = f"Lean D+{int(abs(latest_margin['margin']))}"
    else:
        lean = f"Strong D+{int(abs(latest_margin['margin']))}"

    return {
        'name': county,
        'towns': towns,
        'lean': lean,
        'latest_year': years[-1],
        'latest_margin': latest_margin,
        'margins_by_year': margins_by_year,
        'races_by_year': dict(races_by_year),
        'office_summary': {year: dict(offices) for year, offices in office_summary_by_year.items()},
        'presidential': presidential_results,
        'years': years,
        'trend': round(trend, 1)
    }


def get_statewide_baseline(year=None):
    """
    Calculate statewide R% for competitive races only.
    A race is competitive if both R and D candidates ran.
    Returns dict by year with R percentage for all competitive races combined.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all race results with party totals
    year_filter = "AND e.year = ?" if year else ""
    params = (year,) if year else ()

    cursor.execute(f"""
        SELECT
            e.year,
            r.id as race_id,
            o.name as office,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        {year_filter}
        GROUP BY e.year, r.id
    """, params)

    # Aggregate only competitive races (both R and D have votes)
    by_year = defaultdict(lambda: {'r_votes': 0, 'd_votes': 0, 'total': 0, 'races': 0})

    for row in cursor.fetchall():
        year_val, race_id, office, r_votes, d_votes, total = row
        # Only count if BOTH parties had candidates
        if r_votes > 0 and d_votes > 0:
            by_year[year_val]['r_votes'] += r_votes
            by_year[year_val]['d_votes'] += d_votes
            by_year[year_val]['total'] += total
            by_year[year_val]['races'] += 1

    conn.close()

    # Calculate R percentage for each year
    result = {}
    for yr, data in by_year.items():
        if data['total'] > 0:
            r_pct = (data['r_votes'] / data['total']) * 100
            result[yr] = {
                'r_pct': round(r_pct, 2),
                'r_votes': data['r_votes'],
                'd_votes': data['d_votes'],
                'total': data['total'],
                'competitive_races': data['races']
            }

    return result


def get_town_pvi(town):
    """
    Calculate PVI (Partisan Voter Index) for a town.
    PVI = Town R% - Statewide R% (only for competitive races)

    Returns dict with PVI for each year and overall trend.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get town results by race
    cursor.execute("""
        SELECT
            e.year,
            r.id as race_id,
            o.name as office,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, r.id
    """, (town,))

    # Aggregate only competitive races
    town_by_year = defaultdict(lambda: {'r_votes': 0, 'd_votes': 0, 'total': 0, 'races': 0})

    for row in cursor.fetchall():
        year, race_id, office, r_votes, d_votes, total = row
        # Only count if BOTH parties had candidates
        if r_votes > 0 and d_votes > 0:
            town_by_year[year]['r_votes'] += r_votes
            town_by_year[year]['d_votes'] += d_votes
            town_by_year[year]['total'] += total
            town_by_year[year]['races'] += 1

    conn.close()

    # Get statewide baseline
    statewide = get_statewide_baseline()

    # Calculate PVI for each year
    pvi_by_year = {}
    years = sorted(town_by_year.keys())

    for year in years:
        town_data = town_by_year[year]
        state_data = statewide.get(year)

        if town_data['total'] > 0 and state_data:
            town_r_pct = (town_data['r_votes'] / town_data['total']) * 100
            state_r_pct = state_data['r_pct']
            pvi = town_r_pct - state_r_pct

            pvi_by_year[year] = {
                'pvi': round(pvi, 1),
                'town_r_pct': round(town_r_pct, 1),
                'state_r_pct': round(state_r_pct, 1),
                'competitive_races': town_data['races']
            }

    # Calculate trend
    if len(years) >= 2 and years[0] in pvi_by_year and years[-1] in pvi_by_year:
        first_pvi = pvi_by_year[years[0]]['pvi']
        last_pvi = pvi_by_year[years[-1]]['pvi']
        trend = last_pvi - first_pvi
    else:
        trend = 0

    # Current PVI
    current_pvi = pvi_by_year.get(years[-1], {}).get('pvi', 0) if years else 0

    return {
        'current_pvi': current_pvi,
        'pvi_by_year': pvi_by_year,
        'years': years,
        'trend': round(trend, 1),
        'trend_direction': 'R' if trend > 0 else 'D' if trend < 0 else 'stable'
    }


def get_towns_in_district(office, district, county=None):
    """Get the towns currently in a district (using most recent year's data)."""
    conn = get_connection()
    cursor = conn.cursor()

    if county:
        # County-based district
        cursor.execute("""
            SELECT DISTINCT res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND r.district = ?
            AND r.county = ?
            AND e.year = (SELECT MAX(e2.year) FROM elections e2)
            AND res.municipality NOT GLOB '[0-9]*'
            AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS', 'Court ordered recount', 'court ordered recount')
            ORDER BY res.municipality
        """, (office, district, county))
    else:
        # Statewide district
        cursor.execute("""
            SELECT DISTINCT res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND r.district = ?
            AND e.year = (SELECT MAX(e2.year) FROM elections e2)
            AND res.municipality NOT GLOB '[0-9]*'
            AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS', 'Court ordered recount', 'court ordered recount')
            ORDER BY res.municipality
        """, (office, district))

    towns = [row[0] for row in cursor.fetchall()]
    conn.close()
    return towns


def get_district_pvi(office, district, county=None):
    """
    Calculate PVI for a district based on CURRENT district composition.
    Uses the towns in the current district and calculates their combined voting
    history across all years (2016-2024).
    PVI = District R% - Statewide R% (only for competitive races)
    """
    # First, get the towns currently in this district
    towns = get_towns_in_district(office, district, county)

    if not towns:
        return {
            'current_pvi': 0,
            'pvi_by_year': {},
            'years': [],
            'trend': 0,
            'towns': []
        }

    conn = get_connection()
    cursor = conn.cursor()

    # Get voting data for these specific towns across ALL years
    # This aggregates votes from ALL races these towns voted in (not just the specific office)
    placeholders = ','.join('?' * len(towns))
    cursor.execute(f"""
        SELECT
            e.year,
            r.id as race_id,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE res.municipality IN ({placeholders})
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, r.id
    """, towns)

    # Aggregate only competitive races
    district_by_year = defaultdict(lambda: {'r_votes': 0, 'd_votes': 0, 'total': 0})

    for row in cursor.fetchall():
        year, race_id, r_votes, d_votes, total = row
        # Only count if BOTH parties had candidates
        if r_votes > 0 and d_votes > 0:
            district_by_year[year]['r_votes'] += r_votes
            district_by_year[year]['d_votes'] += d_votes
            district_by_year[year]['total'] += total

    conn.close()

    # Get statewide baseline
    statewide = get_statewide_baseline()

    # Calculate PVI for each year
    pvi_by_year = {}
    years = sorted(district_by_year.keys())

    for year in years:
        dist_data = district_by_year[year]
        state_data = statewide.get(year)

        if dist_data['total'] > 0 and state_data:
            dist_r_pct = (dist_data['r_votes'] / dist_data['total']) * 100
            state_r_pct = state_data['r_pct']
            pvi = dist_r_pct - state_r_pct

            pvi_by_year[year] = {
                'pvi': round(pvi, 1),
                'dist_r_pct': round(dist_r_pct, 1),
                'state_r_pct': round(state_r_pct, 1)
            }

    # Calculate trend
    if len(years) >= 2 and years[0] in pvi_by_year and years[-1] in pvi_by_year:
        first_pvi = pvi_by_year[years[0]]['pvi']
        last_pvi = pvi_by_year[years[-1]]['pvi']
        trend = last_pvi - first_pvi
    else:
        trend = 0

    current_pvi = pvi_by_year.get(years[-1], {}).get('pvi', 0) if years else 0

    return {
        'current_pvi': current_pvi,
        'pvi_by_year': pvi_by_year,
        'years': years,
        'trend': round(trend, 1),
        'towns': towns
    }


def get_town_key_races(town):
    """
    Get key race margins across years for a town.
    Returns dict with margins by office and year for the grid view.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get margins by office and year
    cursor.execute("""
        SELECT
            e.year,
            o.name as office,
            r.district,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, o.name
        ORDER BY e.year, o.name
    """, (town,))

    results = {}
    years = set()
    offices_seen = set()

    for row in cursor.fetchall():
        year, office, district, r_votes, d_votes = row
        rd_total = r_votes + d_votes
        if rd_total > 0:
            margin = round((r_votes - d_votes) / rd_total * 100, 1)
        else:
            margin = 0

        if office not in results:
            results[office] = {}
        results[office][year] = margin
        years.add(year)
        offices_seen.add(office)

    conn.close()

    # Define key offices to show (in order)
    key_offices = [
        'President of the United States',
        'Governor',
        'United States Senator',
        'State Senator',
        'State Representative'
    ]

    # Filter to only key offices that have data
    filtered = {}
    for office in key_offices:
        if office in results:
            filtered[office] = results[office]

    return {
        'by_office': filtered,
        'years': sorted(years, reverse=True)
    }


def get_town_representation(town):
    """
    Get the current districts this town is in (most recent year).
    Returns list of district assignments.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get districts for this town from the most recent year
    cursor.execute("""
        SELECT DISTINCT
            o.name as office,
            r.district,
            r.county
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.year = (SELECT MAX(e2.year) FROM elections e2)
        AND o.name IN ('State Representative', 'State Senator', 'Executive Councilor', 'Representative in Congress')
        ORDER BY o.name
    """, (town,))

    districts = []
    for row in cursor.fetchall():
        office, district, county = row
        districts.append({
            'office': office,
            'district': district,
            'county': county
        })

    conn.close()
    return districts


# ============== NEW ANALYSIS FUNCTIONS ==============

def get_turnout_analysis():
    """
    Analyze turnout trends across towns and years.
    Returns towns with biggest turnout changes, overall trends, etc.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get turnout by town and year
    cursor.execute("""
        SELECT
            res.municipality as town,
            e.year,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND o.name = 'President of the United States'
        AND res.municipality NOT GLOB '[0-9]*'
        AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
        GROUP BY res.municipality, e.year
        ORDER BY res.municipality, e.year
    """)

    town_turnout = defaultdict(dict)
    for row in cursor.fetchall():
        town, year, votes = row
        town_turnout[town][year] = votes

    # Calculate changes
    years = [2016, 2020, 2024]  # Presidential years
    turnout_changes = []

    for town, by_year in town_turnout.items():
        if 2020 in by_year and 2024 in by_year:
            change = by_year[2024] - by_year[2020]
            pct_change = (change / by_year[2020] * 100) if by_year[2020] > 0 else 0
            turnout_changes.append({
                'town': town,
                'votes_2020': by_year.get(2020, 0),
                'votes_2024': by_year.get(2024, 0),
                'change': change,
                'pct_change': round(pct_change, 1)
            })

    # Sort by absolute change
    biggest_gains = sorted(turnout_changes, key=lambda x: -x['change'])[:15]
    biggest_losses = sorted(turnout_changes, key=lambda x: x['change'])[:15]

    # Statewide totals
    statewide = {}
    for year in years:
        total = sum(by_year.get(year, 0) for by_year in town_turnout.values())
        statewide[year] = total

    conn.close()

    return {
        'by_town': dict(town_turnout),
        'biggest_gains': biggest_gains,
        'biggest_losses': biggest_losses,
        'statewide': statewide,
        'years': years
    }


def get_ticket_splitting_analysis():
    """
    Find towns where voters split tickets between top-of-ticket and down-ballot.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get results by town, year, and office
    cursor.execute("""
        SELECT
            res.municipality as town,
            e.year,
            o.name as office,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND res.municipality NOT GLOB '[0-9]*'
        AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
        GROUP BY res.municipality, e.year, o.name
    """)

    by_town_year = defaultdict(lambda: defaultdict(dict))
    for row in cursor.fetchall():
        town, year, office, r_votes, d_votes = row
        winner = 'R' if r_votes > d_votes else 'D' if d_votes > r_votes else 'Tie'
        margin = ((r_votes - d_votes) / (r_votes + d_votes) * 100) if (r_votes + d_votes) > 0 else 0
        by_town_year[town][year][office] = {'winner': winner, 'margin': round(margin, 1)}

    conn.close()

    # Find splits
    splits = []
    for town, by_year in by_town_year.items():
        for year, by_office in by_year.items():
            # Check President vs State Rep
            pres = by_office.get('President of the United States')
            state_rep = by_office.get('State Representative')

            if pres and state_rep and pres['winner'] != state_rep['winner'] and pres['winner'] != 'Tie' and state_rep['winner'] != 'Tie':
                splits.append({
                    'town': town,
                    'year': year,
                    'president': pres['winner'],
                    'president_margin': pres['margin'],
                    'state_rep': state_rep['winner'],
                    'state_rep_margin': state_rep['margin'],
                    'split_magnitude': abs(pres['margin']) + abs(state_rep['margin'])
                })

    # Sort by split magnitude (biggest splits first)
    splits.sort(key=lambda x: (-x['year'], -x['split_magnitude']))

    # Group by year
    by_year = defaultdict(list)
    for s in splits:
        by_year[s['year']].append(s)

    return {
        'splits': splits[:50],  # Top 50
        'by_year': dict(by_year),
        'total_splits': len(splits)
    }


def get_redistricting_impact():
    """
    Analyze impact of 2022 redistricting on districts.
    Compare pre-redistricting (2020) to post-redistricting (2022, 2024).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get 2020 district results (pre-redistricting)
    cursor.execute("""
        SELECT
            r.county,
            r.district,
            o.name as office,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.year = 2020
        AND e.election_type = 'general'
        AND o.name = 'State Representative'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.county, r.district
    """)

    pre_2022 = {}
    for row in cursor.fetchall():
        county, district, office, r_votes, d_votes = row
        key = f"{county}-{district}"
        total = r_votes + d_votes
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        pre_2022[key] = {'margin': round(margin, 1), 'r_votes': r_votes, 'd_votes': d_votes}

    # Get 2024 district results (post-redistricting)
    cursor.execute("""
        SELECT
            r.county,
            r.district,
            o.name as office,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.year = 2024
        AND e.election_type = 'general'
        AND o.name = 'State Representative'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.county, r.district
    """)

    post_2022 = {}
    for row in cursor.fetchall():
        county, district, office, r_votes, d_votes = row
        key = f"{county}-{district}"
        total = r_votes + d_votes
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        post_2022[key] = {'margin': round(margin, 1), 'r_votes': r_votes, 'd_votes': d_votes}

    conn.close()

    # Note: Direct comparison is difficult because district boundaries changed
    # Instead, show new districts and their composition

    return {
        'pre_2022_districts': len(pre_2022),
        'post_2022_districts': len(post_2022),
        'note': 'District boundaries changed significantly in 2022 redistricting. Direct comparison is not meaningful.',
        'post_2022': post_2022
    }


def get_office_results(office):
    """
    Get comprehensive results for a specific office across all years.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get results by year
    cursor.execute("""
        SELECT
            e.year,
            r.district,
            r.county,
            r.seats,
            c.name as candidate,
            c.party,
            SUM(res.votes) as total_votes,
            RANK() OVER (PARTITION BY r.id ORDER BY SUM(res.votes) DESC) as rank
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, r.id, c.id
        ORDER BY e.year DESC, r.county, r.district, total_votes DESC
    """, (office,))

    by_year = defaultdict(lambda: {'races': [], 'r_seats': 0, 'd_seats': 0, 'total_r_votes': 0, 'total_d_votes': 0})
    current_race = None

    for row in cursor.fetchall():
        year, district, county, seats, candidate, party, votes, rank = row
        race_key = (year, district, county)

        if race_key != current_race:
            current_race = race_key
            by_year[year]['races'].append({
                'district': district,
                'county': county,
                'seats': seats,
                'candidates': []
            })

        race = by_year[year]['races'][-1]
        is_winner = rank <= seats
        race['candidates'].append({
            'name': candidate,
            'party': party,
            'votes': votes,
            'is_winner': is_winner
        })

        if party == 'Republican':
            by_year[year]['total_r_votes'] += votes
            if is_winner:
                by_year[year]['r_seats'] += 1
        elif party == 'Democratic':
            by_year[year]['total_d_votes'] += votes
            if is_winner:
                by_year[year]['d_seats'] += 1

    conn.close()

    # Calculate margins per year
    for year, data in by_year.items():
        total = data['total_r_votes'] + data['total_d_votes']
        if total > 0:
            data['margin'] = round((data['total_r_votes'] - data['total_d_votes']) / total * 100, 1)
        else:
            data['margin'] = 0

    return {
        'by_year': dict(by_year),
        'years': sorted(by_year.keys(), reverse=True)
    }


def get_incumbent_analysis():
    """
    Track incumbents - candidates who won in year N and ran again in year N+2.
    Uses name + party + district to identify the same person across elections.
    Shows whether they won or lost re-election.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all candidate races with win/loss status
    # Group by name + party + town to identify the same person
    cursor.execute("""
        WITH candidate_races AS (
            SELECT
                c.name as candidate,
                c.party,
                e.year,
                o.name as office,
                r.district,
                r.county,
                res.town,
                r.seats,
                res.votes as total_votes,
                RANK() OVER (PARTITION BY r.id ORDER BY SUM(res.votes) OVER (PARTITION BY res.candidate_id, r.id) DESC) as rank
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND o.name IN ('State Representative', 'State Senator', 'Executive Councilor', 'Governor')
        )
        SELECT
            candidate,
            party,
            year,
            office,
            district,
            county,
            town,
            SUM(total_votes) as votes,
            MAX(rank <= seats) as won
        FROM candidate_races
        GROUP BY candidate, party, year, office, district, county, town
        ORDER BY candidate, party, town, year
    """)

    # Group by candidate + party + town (same person across years)
    # This identifies the same person by name + party + home town
    person_races = defaultdict(list)
    for row in cursor.fetchall():
        candidate, party, year, office, district, county, town, votes, won = row
        # Key: person identified by name + party + town
        key = (candidate, party or '', town or '')
        person_races[key].append({
            'party': party,
            'year': year,
            'office': office,
            'district': district,
            'county': county,
            'town': town,
            'votes': votes,
            'won': bool(won)
        })

    conn.close()

    # Track true incumbents: people who won their seat in year N and ran again in year N+2
    incumbents_2024 = []  # Won in 2022, ran in 2024
    incumbents_2022 = []  # Won in 2020, ran in 2022
    incumbents_2020 = []  # Won in 2018, ran in 2020

    repeat_candidates = []

    for (name, party, town), races in person_races.items():
        if len(races) < 2:
            continue

        # Build race lookup by year
        races_by_year = {r['year']: r for r in races}
        wins = sum(1 for r in races if r['won'])
        years = sorted([r['year'] for r in races])

        # Check for 2024 incumbents (won 2022, ran 2024)
        if 2022 in races_by_year and 2024 in races_by_year:
            if races_by_year[2022]['won']:
                incumbents_2024.append({
                    'name': name,
                    'party': races_by_year[2024]['party'],
                    'office': races_by_year[2024]['office'],
                    'district': races_by_year[2024]['district'],
                    'county': races_by_year[2024]['county'],
                    'won_reelection': races_by_year[2024]['won'],
                    'votes_2022': races_by_year[2022]['votes'],
                    'votes_2024': races_by_year[2024]['votes']
                })

        # Check for 2022 incumbents (won 2020, ran 2022)
        if 2020 in races_by_year and 2022 in races_by_year:
            if races_by_year[2020]['won']:
                incumbents_2022.append({
                    'name': name,
                    'party': races_by_year[2022]['party'],
                    'office': races_by_year[2022]['office'],
                    'won_reelection': races_by_year[2022]['won']
                })

        # Check for 2020 incumbents (won 2018, ran 2020)
        if 2018 in races_by_year and 2020 in races_by_year:
            if races_by_year[2018]['won']:
                incumbents_2020.append({
                    'name': name,
                    'party': races_by_year[2020]['party'],
                    'office': races_by_year[2020]['office'],
                    'won_reelection': races_by_year[2020]['won']
                })

        # Track repeat candidates for the table
        most_recent_race = races[-1]
        repeat_candidates.append({
            'name': name,
            'party': party,
            'town': town,
            'office': most_recent_race['office'],
            'district': most_recent_race['district'],
            'county': most_recent_race['county'],
            'races': races,
            'total_races': len(races),
            'wins': wins,
            'losses': len(races) - wins,
            'win_rate': round(wins / len(races) * 100, 1),
            'years': years,
            'first_year': min(years),
            'last_year': max(years)
        })

    # Sort repeat candidates by number of races
    repeat_candidates.sort(key=lambda x: (-x['total_races'], x['name']))

    # Calculate incumbent stats
    inc_2024_won = sum(1 for i in incumbents_2024 if i['won_reelection'])
    inc_2024_lost = len(incumbents_2024) - inc_2024_won
    inc_2022_won = sum(1 for i in incumbents_2022 if i['won_reelection'])
    inc_2022_lost = len(incumbents_2022) - inc_2022_won
    inc_2020_won = sum(1 for i in incumbents_2020 if i['won_reelection'])
    inc_2020_lost = len(incumbents_2020) - inc_2020_won

    # Sort incumbents_2024 - lost first, then by name
    incumbents_2024.sort(key=lambda x: (x['won_reelection'], x['name']))

    return {
        'repeat_candidates': repeat_candidates[:100],
        'total_repeat': len(repeat_candidates),
        'incumbents_2024': incumbents_2024,
        'incumbents_won_2024': inc_2024_won,
        'incumbents_lost_2024': inc_2024_lost,
        'incumbents_2022': {'won': inc_2022_won, 'lost': inc_2022_lost, 'total': len(incumbents_2022)},
        'incumbents_2020': {'won': inc_2020_won, 'lost': inc_2020_lost, 'total': len(incumbents_2020)}
    }


def compare_towns(town1, town2):
    """Compare two towns side by side."""
    summary1 = get_town_summary(town1)
    summary2 = get_town_summary(town2)

    if not summary1 or not summary2:
        return None

    pvi1 = get_town_pvi(town1)
    pvi2 = get_town_pvi(town2)

    return {
        'town1': {
            'name': town1,
            'summary': summary1,
            'pvi': pvi1
        },
        'town2': {
            'name': town2,
            'summary': summary2,
            'pvi': pvi2
        }
    }


def compare_districts(district1, district2):
    """Compare two districts side by side."""
    # Parse district strings like "Hillsborough-1" or "State Senate-1"
    # For now, just return None - would need more complex parsing
    return None


def get_districts_map_data():
    """
    Get district data keyed by district code for the map.
    Returns data for all district types:
    - House: BE1, HI35, etc.
    - Senate: 1, 2, 3, etc.
    - Exec Council: 1, 2, 3, 4, 5
    - Congress: 1, 2
    - Towns: town names
    """
    conn = get_connection()
    cursor = conn.cursor()

    data = {}

    # County code mapping for House districts
    county_codes = {
        'Belknap': 'BE', 'Carroll': 'CA', 'Cheshire': 'CH',
        'Coos': 'CO', 'Grafton': 'GR', 'Hillsborough': 'HI',
        'Merrimack': 'ME', 'Rockingham': 'RO', 'Strafford': 'ST', 'Sullivan': 'SU'
    }

    # House districts
    cursor.execute("""
        SELECT
            r.county,
            r.district,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = 'State Representative'
        AND e.year = 2024
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.county, r.district
    """)

    for row in cursor.fetchall():
        county, district, r_votes, d_votes, total = row
        if county in county_codes:
            code = county_codes[county] + str(district)
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            data[code] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total
            }

    # Senate districts (keyed by number)
    cursor.execute("""
        SELECT
            r.district,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = 'State Senator'
        AND e.year = 2024
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.district
    """)

    for row in cursor.fetchall():
        district, r_votes, d_votes, total = row
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        data[f'sen_{district}'] = {
            'margin': round(margin, 1),
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total
        }

    # Executive Council (keyed with ec_ prefix)
    cursor.execute("""
        SELECT
            r.district,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = 'Executive Councilor'
        AND e.year = 2024
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.district
    """)

    for row in cursor.fetchall():
        district, r_votes, d_votes, total = row
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        data[f'ec_{district}'] = {
            'margin': round(margin, 1),
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total
        }

    # Congress (keyed with cong_ prefix)
    cursor.execute("""
        SELECT
            r.district,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = 'Representative in Congress'
        AND e.year = 2024
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.district
    """)

    for row in cursor.fetchall():
        district, r_votes, d_votes, total = row
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        data[f'cong_{district}'] = {
            'margin': round(margin, 1),
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total
        }

    # Towns (keyed by name) - aggregate wards into cities
    # Use CASE to extract base town name (strip " Ward X" suffix)
    cursor.execute("""
        SELECT
            CASE
                WHEN res.municipality LIKE '% Ward %'
                THEN SUBSTR(res.municipality, 1, INSTR(res.municipality, ' Ward ') - 1)
                ELSE res.municipality
            END as town,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE e.year = 2024
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND res.municipality IS NOT NULL
        AND res.municipality != ''
        AND res.municipality NOT GLOB '[0-9]*'
        AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
        GROUP BY CASE
            WHEN res.municipality LIKE '% Ward %'
            THEN SUBSTR(res.municipality, 1, INSTR(res.municipality, ' Ward ') - 1)
            ELSE res.municipality
        END
    """)

    for row in cursor.fetchall():
        town, r_votes, d_votes, total = row
        if town and total > 0:
            margin = ((r_votes - d_votes) / total * 100)
            data[town] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total
            }

    conn.close()
    return data


def get_map_data(year, metric='pvi'):
    """
    Get data for map visualization.
    Returns dict with town -> value mapping.
    """
    conn = get_connection()
    cursor = conn.cursor()

    if metric == 'pvi':
        # Get PVI for each town
        cursor.execute("""
            SELECT
                res.municipality as town,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
                SUM(res.votes) as total_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.year = ?
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality NOT GLOB '[0-9]*'
            AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
            GROUP BY res.municipality
        """, (year,))

        # Get statewide baseline
        statewide = get_statewide_baseline(year)
        state_r_pct = statewide.get(year, {}).get('r_pct', 50)

        data = {}
        for row in cursor.fetchall():
            town, r_votes, d_votes, total = row
            if total > 0:
                town_r_pct = (r_votes / total) * 100
                pvi = town_r_pct - state_r_pct
                data[town] = round(pvi, 1)

    elif metric == 'margin':
        cursor.execute("""
            SELECT
                res.municipality as town,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.year = ?
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality NOT GLOB '[0-9]*'
            AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
            GROUP BY res.municipality
        """, (year,))

        data = {}
        for row in cursor.fetchall():
            town, r_votes, d_votes = row
            total = r_votes + d_votes
            if total > 0:
                margin = (r_votes - d_votes) / total * 100
                data[town] = round(margin, 1)

    else:  # turnout
        cursor.execute("""
            SELECT
                res.municipality as town,
                SUM(res.votes) as total_votes
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.year = ?
            AND e.election_type = 'general'
            AND o.name = 'President of the United States'
            AND res.municipality NOT GLOB '[0-9]*'
            AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
            GROUP BY res.municipality
        """, (year,))

        data = {}
        for row in cursor.fetchall():
            town, votes = row
            data[town] = votes

    conn.close()
    return data


def export_town_data(year=None):
    """Export town-level data."""
    conn = get_connection()
    cursor = conn.cursor()

    year_filter = "AND e.year = ?" if year else ""
    params = (year,) if year else ()

    cursor.execute(f"""
        SELECT
            res.municipality as town,
            e.year,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND res.municipality NOT GLOB '[0-9]*'
        AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
        {year_filter}
        GROUP BY res.municipality, e.year
        ORDER BY res.municipality, e.year
    """, params)

    data = []
    for row in cursor.fetchall():
        town, yr, r_votes, d_votes, total = row
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        data.append({
            'town': town,
            'year': yr,
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total,
            'margin': round(margin, 1)
        })

    conn.close()
    return data


def export_district_data(year=None):
    """Export district-level data."""
    conn = get_connection()
    cursor = conn.cursor()

    year_filter = "AND e.year = ?" if year else ""
    params = (year,) if year else ()

    cursor.execute(f"""
        SELECT
            o.name as office,
            r.county,
            r.district,
            e.year,
            r.seats,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        {year_filter}
        GROUP BY o.name, r.county, r.district, e.year
        ORDER BY o.name, r.county, r.district, e.year
    """, params)

    data = []
    for row in cursor.fetchall():
        office, county, district, yr, seats, r_votes, d_votes, total = row
        margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
        data.append({
            'office': office,
            'county': county,
            'district': district,
            'year': yr,
            'seats': seats,
            'r_votes': r_votes,
            'd_votes': d_votes,
            'total_votes': total,
            'margin': round(margin, 1)
        })

    conn.close()
    return data


def export_race_data(year=None):
    """Export race-level data."""
    conn = get_connection()
    cursor = conn.cursor()

    year_filter = "AND e.year = ?" if year else ""
    params = (year,) if year else ()

    cursor.execute(f"""
        SELECT
            e.year,
            o.name as office,
            r.county,
            r.district,
            c.name as candidate,
            c.party,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        {year_filter}
        GROUP BY e.year, r.id, c.id
        ORDER BY e.year, o.name, r.county, r.district, total_votes DESC
    """, params)

    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return data


def export_candidate_data(year=None):
    """Export candidate performance data."""
    conn = get_connection()
    cursor = conn.cursor()

    year_filter = "AND e.year = ?" if year else ""
    params = (year,) if year else ()

    cursor.execute(f"""
        SELECT
            c.name as candidate,
            c.party,
            e.year,
            o.name as office,
            r.county,
            r.district,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        {year_filter}
        GROUP BY c.name, e.year, r.id
        ORDER BY c.name, e.year
    """, params)

    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return data


def get_all_districts_with_pvi(office):
    """
    Get all districts for an office with their PVI data.
    OPTIMIZED: Uses batch query instead of per-district queries.
    Returns list sorted by current PVI (most R to most D).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get statewide baseline for 2024
    statewide = get_statewide_baseline(2024)
    state_r_pct = statewide.get(2024, {}).get('r_pct', 50)

    # Check if this is a county-based office
    is_county_based = office == 'State Representative'

    if is_county_based:
        # Batch query: Get all districts with their 2024 vote margins
        cursor.execute("""
            SELECT
                r.county,
                r.district,
                r.seats,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
                SUM(res.votes) as total_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND e.year = 2024
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.county, r.district
            ORDER BY r.county, CAST(r.district AS INTEGER)
        """, (office,))

        districts = []
        for row in cursor.fetchall():
            county, district, seats, r_votes, d_votes, total = row
            if total > 0:
                dist_r_pct = (r_votes / total) * 100
                pvi = dist_r_pct - state_r_pct
            else:
                pvi = 0

            districts.append({
                'district': district,
                'county': county,
                'seats': seats,
                'pvi': round(pvi, 1),
                'trend': 0,  # Skip trend calc for speed
                'r_votes': r_votes,
                'd_votes': d_votes
            })
    else:
        # Statewide districts - batch query
        cursor.execute("""
            SELECT
                r.district,
                r.seats,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
                SUM(res.votes) as total_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND e.year = 2024
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.district
            ORDER BY CAST(r.district AS INTEGER)
        """, (office,))

        districts = []
        for row in cursor.fetchall():
            district, seats, r_votes, d_votes, total = row
            if total > 0:
                dist_r_pct = (r_votes / total) * 100
                pvi = dist_r_pct - state_r_pct
            else:
                pvi = 0

            districts.append({
                'district': district,
                'county': None,
                'seats': seats,
                'pvi': round(pvi, 1),
                'trend': 0,
                'r_votes': r_votes,
                'd_votes': d_votes
            })

    conn.close()

    # Sort by PVI (most R first)
    districts.sort(key=lambda x: -x['pvi'])

    return districts
