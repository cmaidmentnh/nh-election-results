#!/usr/bin/env python3
"""
Analysis functions for NH Election Results Explorer
Generates meaningful insights from election data
"""

import sqlite3
from pathlib import Path
from collections import defaultdict
import queries

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

    For State Rep: uses total party votes from deduplicated canonical races.
    For other offices: uses top vote-getter per party (same as total for single-candidate races).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # First, get State Rep votes using deduplicated canonical races
    cursor.execute("""
        WITH race_totals AS (
            SELECT r.id as race_id, e.year, r.county, r.district, SUM(res.votes) as total
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.election_type = 'general'
            AND o.name = 'State Representative'
            GROUP BY r.id
        ),
        canonical_races AS (
            SELECT race_id, year, county, district
            FROM race_totals rt
            WHERE rt.total = (
                SELECT MAX(rt2.total)
                FROM race_totals rt2
                WHERE rt2.year = rt.year AND rt2.county = rt.county AND rt2.district = rt.district
            )
        )
        SELECT
            cr.year,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN canonical_races cr ON res.race_id = cr.race_id
        WHERE res.municipality = ?
        AND c.party IN ('Republican', 'Democratic')
        GROUP BY cr.year, c.party
    """, (town,))

    # Store State Rep totals by year
    state_rep_by_year = defaultdict(lambda: {'R': 0, 'D': 0})
    for year, party, votes in cursor.fetchall():
        p = 'R' if party == 'Republican' else 'D'
        state_rep_by_year[year][p] = votes

    # Get all other results for this town
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
        AND o.name != 'State Representative'
        ORDER BY e.year, o.name, res.votes DESC
    """, (town,))

    results = cursor.fetchall()
    if not results and not state_rep_by_year:
        conn.close()
        return None

    # Track individual candidate votes by year/office (for non-State-Rep)
    candidate_votes = defaultdict(lambda: defaultdict(list))

    for row in results:
        year = row['year']
        office = row['office']
        party = row['party']
        votes = row['votes']
        candidate_votes[year][office].append({'party': party, 'votes': votes})

    # Calculate margins by year
    # Combine years from both queries
    all_years = set(candidate_votes.keys()) | set(state_rep_by_year.keys())
    years = sorted(all_years)
    margins_by_year = {}
    by_year = defaultdict(lambda: defaultdict(lambda: {'top_r': 0, 'top_d': 0, 'total': 0}))

    for year in years:
        year_top_r = 0
        year_top_d = 0
        year_total = 0

        # Add non-State-Rep offices (using top vote-getter)
        for office, candidates in candidate_votes.get(year, {}).items():
            top_r = max((c['votes'] for c in candidates if c['party'] == 'Republican'), default=0)
            top_d = max((c['votes'] for c in candidates if c['party'] == 'Democratic'), default=0)
            total = sum(c['votes'] for c in candidates)

            by_year[year][office]['top_r'] = top_r
            by_year[year][office]['top_d'] = top_d
            by_year[year][office]['total'] = total

            year_top_r += top_r
            year_top_d += top_d
            year_total += total

        # Add State Rep (using deduplicated total party votes)
        if year in state_rep_by_year:
            sr_r = state_rep_by_year[year]['R']
            sr_d = state_rep_by_year[year]['D']
            by_year[year]['State Representative']['top_r'] = sr_r
            by_year[year]['State Representative']['top_d'] = sr_d
            by_year[year]['State Representative']['total'] = sr_r + sr_d

            year_top_r += sr_r
            year_top_d += sr_d
            year_total += sr_r + sr_d

        if year_top_r + year_top_d > 0:
            total_top = year_top_r + year_top_d
            r_pct = (year_top_r / total_top) * 100
            d_pct = (year_top_d / total_top) * 100
            margin = r_pct - d_pct
            margins_by_year[year] = {
                'r_pct': round(r_pct, 1),
                'd_pct': round(d_pct, 1),
                'margin': round(margin, 1),
                'r_votes': year_top_r,
                'd_votes': year_top_d,
                'total_votes': year_total
            }

    # Calculate trend (change from first to last year)
    if len(years) >= 2 and years[0] in margins_by_year and years[-1] in margins_by_year:
        first_margin = margins_by_year[years[0]]['margin']
        last_margin = margins_by_year[years[-1]]['margin']
        trend = last_margin - first_margin
        trend_direction = 'R' if trend > 0 else 'D' if trend < 0 else 'stable'
    else:
        trend = 0
        trend_direction = 'stable'

    # Detect ticket splitting using top vote-getter comparison
    ticket_splits = []
    for year in years:
        offices = by_year[year]

        # Find top of ticket winner
        top_ticket = None
        top_ticket_office = None
        for office in ['President of the United States', 'Governor']:
            if office in offices:
                data = offices[office]
                if data['top_r'] > data['top_d']:
                    top_ticket = 'R'
                elif data['top_d'] > data['top_r']:
                    top_ticket = 'D'
                top_ticket_office = office
                break

        if top_ticket:
            # Check down-ballot
            for office in ['State Representative', 'State Senator', 'Executive Councilor']:
                if office in offices:
                    data = offices[office]
                    if data['top_r'] > data['top_d']:
                        down_ballot = 'R'
                    elif data['top_d'] > data['top_r']:
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

    # Build summary - find latest year with R/D data
    if not margins_by_year:
        return None  # No R/D data at all

    latest_year = max(margins_by_year.keys())
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

    # Calculate margins for each race using TOP vote-getter per party
    # This is fair for multi-member races where one party may run more candidates
    for race_key, race in races.items():
        # Find top vote-getter per party
        top_r = max((c['votes'] for c in race['candidates'] if c['party'] == 'Republican'), default=0)
        top_d = max((c['votes'] for c in race['candidates'] if c['party'] == 'Democratic'), default=0)
        total = sum(c['votes'] for c in race['candidates'])
        rd_total = top_r + top_d

        if rd_total > 0:
            race['r_votes'] = top_r
            race['d_votes'] = top_d
            race['margin'] = top_r - top_d
            # Margin based on top vote-getters: (top_R - top_D) / (top_R + top_D) * 100
            race['margin_pct'] = round((top_r - top_d) / rd_total * 100, 1)
            race['winner_party'] = 'R' if top_r > top_d else 'D' if top_d > top_r else 'Tie'
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
    """Get party control seat counts for legislative offices.

    Handles ties correctly - if there's a tie at the cutoff, neither tied candidate wins.
    """
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
        ),
        -- Count candidates per race that would win (rank <= seats)
        race_winner_count AS (
            SELECT race_id, COUNT(*) as winner_count, MAX(seats) as seats
            FROM race_totals
            WHERE rank <= seats
            GROUP BY race_id
        ),
        -- A race has a tie if winner_count > seats
        winners AS (
            SELECT rt.office, rt.race_id, rt.party, rt.rank
            FROM race_totals rt
            JOIN race_winner_count rwc ON rt.race_id = rwc.race_id
            WHERE rt.rank <= rt.seats
            -- Exclude all candidates at the tied rank if there's a tie
            AND NOT (rwc.winner_count > rwc.seats AND rt.rank = rt.seats)
        )
        SELECT office, party, COUNT(*) as seats
        FROM winners
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
    """Get races with the smallest margins.

    Uses MAX (top vote-getter per party) to normalize for slate size in multi-member races.
    """
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
                MAX(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                MAX(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
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
            r_votes, d_votes,
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
        office, district, county, r_votes, d_votes, margin = row
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
    """Get races with biggest margin shifts between two years.

    Uses top vote-getter per party (not sum) to normalize for slate size differences.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH race_margins AS (
            SELECT
                e.year,
                o.name as office,
                r.district,
                r.county,
                MAX(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                MAX(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
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
    PVI = (R votes in district towns from contested races) / (R+D votes in district towns)
        - (R votes statewide from contested races) / (R+D votes statewide)
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

    placeholders = ','.join('?' * len(towns))

    # Get R/D votes by year for contested races only, summed for district towns
    cursor.execute(f"""
        WITH race_totals AS (
            SELECT e.year, r.id as race_id, res.municipality,
                   SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r,
                   SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE res.municipality IN ({placeholders})
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY e.year, r.id, res.municipality
        ),
        contested AS (
            SELECT year, municipality, r, d
            FROM race_totals
            WHERE r > 0 AND d > 0
        )
        SELECT year, SUM(r) as r, SUM(d) as d
        FROM contested
        GROUP BY year
    """, towns)

    district_by_year = {}
    for year, r, d in cursor.fetchall():
        district_by_year[year] = {'r_votes': r, 'd_votes': d, 'total': r + d}

    # Get statewide baseline for all contested races
    cursor.execute("""
        WITH race_totals AS (
            SELECT e.year, r.id as race_id,
                   SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r,
                   SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY e.year, r.id
            HAVING r > 0 AND d > 0
        )
        SELECT year, SUM(r) as total_r, SUM(d) as total_d
        FROM race_totals
        GROUP BY year
    """)
    statewide = {}
    for year, total_r, total_d in cursor.fetchall():
        statewide[year] = {'r_pct': total_r / (total_r + total_d) * 100}

    conn.close()

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

    # Calculate trend (2022 → 2024)
    if 2022 in pvi_by_year and 2024 in pvi_by_year:
        trend = pvi_by_year[2024]['pvi'] - pvi_by_year[2022]['pvi']
    else:
        trend = 0

    current_pvi = pvi_by_year.get(2024, {}).get('pvi', 0)

    return {
        'current_pvi': current_pvi,
        'pvi_by_year': pvi_by_year,
        'years': years,
        'trend': round(trend, 1),
        'towns': towns
    }


def get_district_topline_races(office, district, county=None):
    """
    Get POTUS and Governor results aggregated for a district's towns.
    Returns dict with margins for President and Governor by year.
    """
    towns = get_towns_in_district(office, district, county)
    if not towns:
        return {}

    conn = get_connection()
    cursor = conn.cursor()

    placeholders = ','.join('?' * len(towns))

    cursor.execute(f"""
        SELECT
            e.year,
            o.name as office,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality IN ({placeholders})
        AND e.election_type = 'general'
        AND o.name IN ('President of the United States', 'Governor')
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, o.name
        ORDER BY e.year DESC
    """, towns)

    results = {}
    for year, off, r, d in cursor.fetchall():
        if year not in results:
            results[year] = {}
        total = r + d
        if total > 0:
            margin = (r - d) / total * 100
            results[year][off] = {
                'r': r,
                'd': d,
                'margin': round(margin, 1)
            }

    conn.close()
    return results


def get_town_key_races(town):
    """
    Get key race margins across years for a town.
    Returns dict with margins by office and year for the grid view.

    For multi-member districts, compares TOP vote-getter from each party
    (not raw party totals, which would be skewed by number of candidates).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get individual candidate results by office and year
    cursor.execute("""
        SELECT
            e.year,
            o.name as office,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND c.party IN ('Republican', 'Democratic')
        GROUP BY e.year, o.name, c.name, c.party
        ORDER BY e.year, o.name, votes DESC
    """, (town,))

    # Group by year/office and find top vote-getter per party
    race_candidates = defaultdict(list)
    for row in cursor.fetchall():
        year, office, party, votes = row
        race_candidates[(year, office)].append({'party': party, 'votes': votes})

    results = {}
    years = set()

    for (year, office), candidates in race_candidates.items():
        # Find top Republican and top Democrat
        top_r = max((c['votes'] for c in candidates if c['party'] == 'Republican'), default=0)
        top_d = max((c['votes'] for c in candidates if c['party'] == 'Democratic'), default=0)

        total = top_r + top_d
        if total > 0:
            margin = round((top_r - top_d) / total * 100, 1)
        else:
            margin = 0

        if office not in results:
            results[office] = {}
        results[office][year] = margin
        years.add(year)

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
    Analyze turnout trends across towns and years using official ballots cast data.
    Returns towns with biggest turnout changes, overall trends, etc.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get turnout by town and year from voter_registration table (ballots_cast)
    cursor.execute("""
        SELECT
            v.municipality as town,
            e.year,
            v.ballots_cast
        FROM voter_registration v
        JOIN elections e ON v.election_id = e.id
        WHERE e.election_type = 'general'
        AND v.ballots_cast > 0
        ORDER BY v.municipality, e.year
    """)

    town_turnout = defaultdict(dict)
    for row in cursor.fetchall():
        town, year, ballots = row
        if not town:
            continue
        # Normalize ward names to town names
        if ' Ward ' in town:
            base_town = town[:town.index(' Ward ')]
            if base_town not in town_turnout or year not in town_turnout[base_town]:
                town_turnout[base_town][year] = 0
            town_turnout[base_town][year] += ballots
        else:
            town_turnout[town][year] = ballots

    # Calculate changes
    years = [2016, 2018, 2020, 2022, 2024]
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
    Uses SUM of all votes for aggregate totals (counts every vote cast).
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

        # Sum ALL votes for each party (counts every vote cast)
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


def get_office_year_results(office, year):
    """
    Get all races for a specific office and year.
    Returns list of races with candidates, ordered by county/district.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
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
        AND e.year = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY r.id, c.id
        ORDER BY r.county, CAST(r.district AS INTEGER), total_votes DESC
    """, (office, year))

    races = []
    current_race = None

    for row in cursor.fetchall():
        district, county, seats, candidate, party, votes, rank = row
        race_key = (district, county)

        if race_key != current_race:
            current_race = race_key
            # Calculate margin for this race
            races.append({
                'district': district,
                'county': county,
                'seats': seats,
                'candidates': [],
                'top_r': 0,
                'top_d': 0
            })

        race = races[-1]
        is_winner = rank <= seats
        race['candidates'].append({
            'name': candidate,
            'party': party,
            'votes': votes,
            'is_winner': is_winner
        })

        # Track top vote-getter per party for margin calculation
        if party == 'Republican':
            race['top_r'] = max(race['top_r'], votes)
        elif party == 'Democratic':
            race['top_d'] = max(race['top_d'], votes)

    conn.close()

    # Calculate margin for each race
    for race in races:
        total = race['top_r'] + race['top_d']
        if total > 0:
            race['margin'] = round((race['top_r'] - race['top_d']) / total * 100, 1)
        else:
            race['margin'] = 0
        # Clean up temp fields
        del race['top_r']
        del race['top_d']

    return races


def get_incumbent_analysis():
    """
    Track incumbents - candidates who won in year N and ran again in year N+2.
    For State Reps, matches by last name + district + party (since 2022 data has last names only).
    For other offices, matches by full name + party.
    """
    conn = get_connection()
    cursor = conn.cursor()

    def extract_lastname(name):
        """Extract last name from full name or return as-is if already just last name."""
        if not name:
            return ''
        parts = name.split()
        return parts[-1].upper() if parts else name.upper()

    # Get all race results with winner determination
    cursor.execute("""
        WITH candidate_totals AS (
            SELECT
                c.name as candidate,
                c.party,
                e.year,
                o.name as office,
                r.id as race_id,
                r.district,
                r.county,
                r.seats,
                SUM(res.votes) as total_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND o.name IN ('State Representative', 'State Senator', 'Executive Councilor', 'Governor')
            GROUP BY c.name, c.party, e.year, o.name, r.id, r.district, r.county, r.seats
        ),
        candidate_ranked AS (
            SELECT
                candidate,
                party,
                year,
                office,
                race_id,
                district,
                county,
                seats,
                total_votes,
                RANK() OVER (PARTITION BY race_id ORDER BY total_votes DESC) as rank
            FROM candidate_totals
        )
        SELECT
            candidate,
            party,
            year,
            office,
            district,
            county,
            total_votes as votes,
            rank <= seats as won
        FROM candidate_ranked
        ORDER BY year, office, county, district
    """)

    # Organize results by year
    results_by_year = defaultdict(list)
    for row in cursor.fetchall():
        candidate, party, year, office, district, county, votes, won = row
        results_by_year[year].append({
            'name': candidate,
            'lastname': extract_lastname(candidate),
            'party': party,
            'office': office,
            'district': district,
            'county': county,
            'votes': votes,
            'won': bool(won)
        })

    conn.close()

    def find_incumbents(prev_year, curr_year):
        """Find incumbents: won in prev_year and ran in curr_year."""
        if prev_year not in results_by_year or curr_year not in results_by_year:
            return []

        # Build lookup of winners from prev_year
        # For State Reps: key by (lastname, district, county, party)
        # For others: key by (name, party)
        prev_winners_rep = {}  # For State Reps
        prev_winners_other = {}  # For other offices

        for r in results_by_year[prev_year]:
            if r['won']:
                if r['office'] == 'State Representative':
                    key = (r['lastname'], r['district'], r['county'], r['party'])
                    prev_winners_rep[key] = r
                else:
                    key = (r['name'].upper(), r['party'])
                    prev_winners_other[key] = r

        # Find matches in curr_year
        incumbents = []
        for r in results_by_year[curr_year]:
            prev_race = None
            if r['office'] == 'State Representative':
                key = (r['lastname'], r['district'], r['county'], r['party'])
                prev_race = prev_winners_rep.get(key)
            else:
                key = (r['name'].upper(), r['party'])
                prev_race = prev_winners_other.get(key)

            if prev_race:
                incumbents.append({
                    'name': r['name'],
                    'party': r['party'],
                    'office': r['office'],
                    'district': r['district'],
                    'county': r['county'],
                    'won_reelection': r['won'],
                    'votes_prev': prev_race['votes'],
                    'votes_curr': r['votes']
                })

        return incumbents

    # Find incumbents for each cycle
    incumbents_2024 = find_incumbents(2022, 2024)
    incumbents_2022 = find_incumbents(2020, 2022)
    incumbents_2020 = find_incumbents(2018, 2020)

    # Build repeat candidates list (for display)
    repeat_candidates = []
    seen = set()
    for year in sorted(results_by_year.keys(), reverse=True):
        for r in results_by_year[year]:
            key = (r['lastname'], r['party'])
            if key not in seen:
                seen.add(key)
                repeat_candidates.append({
                    'name': r['name'],
                    'party': r['party'],
                    'office': r['office'],
                    'district': r['district'],
                    'county': r['county']
                })

    repeat_candidates = repeat_candidates[:100]  # Limit

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


def get_districts_map_data(year=None, metric='margin'):
    """
    Get district data keyed by district code for the map.

    Args:
        year: Election year (2016, 2018, 2020, 2022, 2024) or None for average
        metric: 'margin' for vote margin, 'pvi' for partisan lean

    Returns data for all district types:
    - House: BE1, HI35, etc.
    - Senate: sen_1, sen_2, etc.
    - Exec Council: ec_1, ec_2, etc.
    - Congress: cong_1, cong_2
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

    # Determine years to query
    if year:
        years = [int(year)]
        year_clause = f"e.year = {year}"
    else:
        years = [2016, 2018, 2020, 2022, 2024]
        year_clause = f"e.year IN ({','.join(str(y) for y in years)})"

    # House districts - need special handling for multi-seat
    # For average (year=None), use current district boundaries with historical town data
    if not year:
        # Get current (2024) district-to-town mapping for base districts
        cursor.execute("""
            SELECT DISTINCT r.county, r.district, res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'State Representative'
            AND e.year = 2024
            AND res.municipality IS NOT NULL
            AND res.municipality != ''
            AND r.district NOT LIKE '%F%'
        """)
        house_district_towns = defaultdict(set)
        house_district_seats = {}
        for county, district, muni in cursor.fetchall():
            if county in county_codes:
                code = county_codes[county] + str(district)
                if ' Ward ' in muni:
                    muni = muni[:muni.index(' Ward ')]
                house_district_towns[code].add(muni)

        # Get seat counts from 2024
        cursor.execute("""
            SELECT r.county, r.district, r.seats
            FROM races r
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'State Representative'
            AND e.year = 2024
            AND r.district NOT LIKE '%F%'
        """)
        for county, district, seats in cursor.fetchall():
            if county in county_codes:
                code = county_codes[county] + str(district)
                house_district_seats[code] = seats or 1

        # Get all historical House votes by town using TOP vote-getter per party per race
        # This is fair for multi-member races where one party may run more candidates
        cursor.execute("""
            SELECT
                res.municipality,
                r.id as race_id,
                c.party,
                SUM(res.votes) as votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Representative'
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality IS NOT NULL
            AND c.party IN ('Republican', 'Democratic')
            GROUP BY res.municipality, r.id, c.name, c.party
        """)

        # Group by town and race, then find top vote-getter per party
        town_race_candidates = defaultdict(lambda: defaultdict(list))
        for muni, race_id, party, votes in cursor.fetchall():
            if ' Ward ' in muni:
                base = muni[:muni.index(' Ward ')]
            else:
                base = muni
            town_race_candidates[base][race_id].append({'party': party, 'votes': votes})

        # Calculate total using top vote-getter per party per race
        house_town_votes = defaultdict(lambda: {'r': 0, 'd': 0})
        for town, races in town_race_candidates.items():
            for race_id, candidates in races.items():
                top_r = max((c['votes'] for c in candidates if c['party'] == 'Republican'), default=0)
                top_d = max((c['votes'] for c in candidates if c['party'] == 'Democratic'), default=0)
                house_town_votes[town]['r'] += top_r
                house_town_votes[town]['d'] += top_d

        # Calculate margin for each current district using historical town data
        for code, towns in house_district_towns.items():
            r_votes = sum(house_town_votes[t]['r'] for t in towns)
            d_votes = sum(house_town_votes[t]['d'] for t in towns)
            total = r_votes + d_votes
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            seats = house_district_seats.get(code, 1)
            data[code] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total,
                'seats': seats,
                'r_seats': seats if margin > 0 else 0,  # Approximate for average
                'd_seats': seats if margin < 0 else 0
            }

        # Floterial districts - same approach
        cursor.execute("""
            SELECT DISTINCT r.county, r.district, res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'State Representative'
            AND e.year = 2024
            AND res.municipality IS NOT NULL
            AND res.municipality != ''
            AND r.district LIKE '%F%'
        """)
        floterial_district_towns = defaultdict(set)
        for county, district, muni in cursor.fetchall():
            if county in county_codes:
                code = county_codes[county] + str(district)
                if ' Ward ' in muni:
                    muni = muni[:muni.index(' Ward ')]
                floterial_district_towns[code].add(muni)

        # Get floterial seat counts from 2024
        cursor.execute("""
            SELECT r.county, r.district, r.seats
            FROM races r
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'State Representative'
            AND e.year = 2024
            AND r.district LIKE '%F%'
        """)
        floterial_seats = {}
        for county, district, seats in cursor.fetchall():
            if county in county_codes:
                code = county_codes[county] + str(district)
                floterial_seats[code] = seats or 1

        for code, towns in floterial_district_towns.items():
            r_votes = sum(house_town_votes[t]['r'] for t in towns)
            d_votes = sum(house_town_votes[t]['d'] for t in towns)
            total = r_votes + d_votes
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            seats = floterial_seats.get(code, 1)
            data[code] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total,
                'seats': seats,
                'r_seats': seats if margin > 0 else 0,
                'd_seats': seats if margin < 0 else 0
            }
    else:
        # Specific year - use that year's data directly
        # First get seat counts
        cursor.execute(f"""
            SELECT r.county, r.district, r.seats
            FROM races r
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Representative'
            AND {year_clause}
            AND e.election_type = 'general'
        """)
        district_seats = {}
        for county, district, seats in cursor.fetchall():
            if county in county_codes:
                code = county_codes[county] + str(district)
                district_seats[code] = seats or 1

        cursor.execute(f"""
            SELECT
                r.county,
                r.district,
                c.name as candidate_name,
                c.party,
                SUM(res.votes) as votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Representative'
            AND {year_clause}
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.county, r.district, c.name, c.party
            ORDER BY r.county, r.district, votes DESC
        """)

        # Group candidates by district
        district_candidates = defaultdict(list)
        for row in cursor.fetchall():
            county, district, candidate_name, party, votes = row
            if county in county_codes:
                code = county_codes[county] + str(district)
                district_candidates[code].append({
                    'name': candidate_name,
                    'party': party,
                    'votes': votes
                })

        # Process each district - use actual seat count from database
        for code, candidates in district_candidates.items():
            # Sort by votes descending
            candidates.sort(key=lambda x: -x['votes'])

            # Count R and D candidates
            r_candidates = [c for c in candidates if c['party'] == 'Republican']
            d_candidates = [c for c in candidates if c['party'] == 'Democratic']

            # Use actual seat count from database
            num_seats = district_seats.get(code, 1)
            if len(candidates) == 0:
                continue

            # Get winners (top N vote-getters)
            winners = candidates[:num_seats]
            r_winners = sum(1 for w in winners if w['party'] == 'Republican')
            d_winners = sum(1 for w in winners if w['party'] == 'Democratic')

            # Calculate votes using TOP vote-getter per party (fair for multi-member)
            top_r = max((c['votes'] for c in r_candidates), default=0)
            top_d = max((c['votes'] for c in d_candidates), default=0)
            total_votes = sum(c['votes'] for c in candidates)

            if num_seats > 1 and len(candidates) > num_seats:
                # Multi-seat: use threshold margin (last winner vs first loser)
                last_winner = candidates[num_seats - 1]
                first_loser = candidates[num_seats]
                threshold_total = last_winner['votes'] + first_loser['votes']
                if threshold_total > 0:
                    # Positive if R would gain seat, negative if D would gain seat
                    if last_winner['party'] == 'Republican':
                        # R holds the last seat - margin is R advantage
                        margin = (last_winner['votes'] - first_loser['votes']) / threshold_total * 100
                    elif last_winner['party'] == 'Democratic':
                        # D holds the last seat - margin is D advantage (negative)
                        margin = -(last_winner['votes'] - first_loser['votes']) / threshold_total * 100
                    else:
                        margin = 0
                else:
                    margin = 0
            else:
                # Single seat or no losers: use top vote-getter margin
                rd_total = top_r + top_d
                margin = ((top_r - top_d) / rd_total * 100) if rd_total > 0 else 0

            # For State House, include top candidates for display
            top_candidates = []
            for c in candidates[:min(4, num_seats * 2)]:  # Top candidates up to 2x seats
                top_candidates.append({
                    'name': c['name'],
                    'party': c['party'][0] if c['party'] else '?',
                    'votes': c['votes']
                })

            data[code] = {
                'margin': round(margin, 1),
                'r_votes': top_r,
                'd_votes': top_d,
                'total_votes': total_votes,
                'seats': num_seats,
                'r_seats': r_winners,
                'd_seats': d_winners,
                'candidates': top_candidates if year else None  # Only include for specific year
            }

    # Senate districts
    # For average (year=None), use current district boundaries with historical town data
    if not year:
        # Get current (2024) district-to-town mapping
        cursor.execute("""
            SELECT DISTINCT r.district, res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'State Senator'
            AND e.year = 2024
            AND res.municipality IS NOT NULL
            AND res.municipality != ''
        """)
        sen_district_towns = defaultdict(set)
        for district, muni in cursor.fetchall():
            if ' Ward ' in muni:
                muni = muni[:muni.index(' Ward ')]
            sen_district_towns[district].add(muni)

        # Get all historical Senate votes by town
        cursor.execute("""
            SELECT
                res.municipality,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Senator'
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality IS NOT NULL
            GROUP BY res.municipality
        """)
        sen_town_votes = {}
        for muni, r_votes, d_votes in cursor.fetchall():
            if ' Ward ' in muni:
                base = muni[:muni.index(' Ward ')]
                if base not in sen_town_votes:
                    sen_town_votes[base] = {'r': 0, 'd': 0}
                sen_town_votes[base]['r'] += r_votes
                sen_town_votes[base]['d'] += d_votes
            else:
                if muni not in sen_town_votes:
                    sen_town_votes[muni] = {'r': 0, 'd': 0}
                sen_town_votes[muni]['r'] += r_votes
                sen_town_votes[muni]['d'] += d_votes

        # Calculate margin for each current district using historical town data
        for district, towns in sen_district_towns.items():
            r_votes = sum(sen_town_votes.get(t, {}).get('r', 0) for t in towns)
            d_votes = sum(sen_town_votes.get(t, {}).get('d', 0) for t in towns)
            total = r_votes + d_votes
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            data[f'sen_{district}'] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total
            }
    else:
        # Specific year - get candidate-level data for display
        cursor.execute(f"""
            SELECT
                r.district,
                c.name as candidate_name,
                c.party,
                SUM(res.votes) as votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Senator'
            AND {year_clause}
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.district, c.name, c.party
            ORDER BY r.district, votes DESC
        """)

        sen_candidates = defaultdict(list)
        for row in cursor.fetchall():
            district, candidate_name, party, votes = row
            sen_candidates[district].append({
                'name': candidate_name,
                'party': party[0] if party else '?',  # R or D
                'votes': votes
            })

        for district, candidates in sen_candidates.items():
            r_votes = sum(c['votes'] for c in candidates if c['party'] == 'R')
            d_votes = sum(c['votes'] for c in candidates if c['party'] == 'D')
            total = sum(c['votes'] for c in candidates)
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            # Get top 2 candidates for display
            top_candidates = candidates[:2]
            data[f'sen_{district}'] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total,
                'candidates': top_candidates
            }

    # Executive Council (keyed with ec_ prefix)
    # For average (year=None), use current district boundaries with historical town data
    if not year:
        # Get current (2024) district-to-town mapping
        cursor.execute("""
            SELECT DISTINCT r.district, res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'Executive Councilor'
            AND e.year = 2024
            AND res.municipality IS NOT NULL
            AND res.municipality != ''
        """)
        ec_district_towns = defaultdict(set)
        for district, muni in cursor.fetchall():
            # Normalize ward names
            if ' Ward ' in muni:
                muni = muni[:muni.index(' Ward ')]
            ec_district_towns[district].add(muni)

        # Get all historical EC votes by town
        cursor.execute("""
            SELECT
                res.municipality,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'Executive Councilor'
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality IS NOT NULL
            GROUP BY res.municipality
        """)
        ec_town_votes = {}
        for muni, r_votes, d_votes in cursor.fetchall():
            if ' Ward ' in muni:
                base = muni[:muni.index(' Ward ')]
                if base not in ec_town_votes:
                    ec_town_votes[base] = {'r': 0, 'd': 0}
                ec_town_votes[base]['r'] += r_votes
                ec_town_votes[base]['d'] += d_votes
            else:
                if muni not in ec_town_votes:
                    ec_town_votes[muni] = {'r': 0, 'd': 0}
                ec_town_votes[muni]['r'] += r_votes
                ec_town_votes[muni]['d'] += d_votes

        # Calculate margin for each current district using historical town data
        for district, towns in ec_district_towns.items():
            r_votes = sum(ec_town_votes.get(t, {}).get('r', 0) for t in towns)
            d_votes = sum(ec_town_votes.get(t, {}).get('d', 0) for t in towns)
            total = r_votes + d_votes
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            data[f'ec_{district}'] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total
            }
    else:
        # Specific year - get candidate-level data for display
        cursor.execute(f"""
            SELECT
                r.district,
                c.name as candidate_name,
                c.party,
                SUM(res.votes) as votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'Executive Councilor'
            AND {year_clause}
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.district, c.name, c.party
            ORDER BY r.district, votes DESC
        """)

        ec_candidates = defaultdict(list)
        for row in cursor.fetchall():
            district, candidate_name, party, votes = row
            ec_candidates[district].append({
                'name': candidate_name,
                'party': party[0] if party else '?',
                'votes': votes
            })

        for district, candidates in ec_candidates.items():
            r_votes = sum(c['votes'] for c in candidates if c['party'] == 'R')
            d_votes = sum(c['votes'] for c in candidates if c['party'] == 'D')
            total = sum(c['votes'] for c in candidates)
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            top_candidates = candidates[:2]
            data[f'ec_{district}'] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total,
                'candidates': top_candidates
            }

    # Congress (keyed with cong_ prefix)
    # For average, use current district boundaries with historical town data
    if not year:
        # Get current (2024) district-to-town mapping
        cursor.execute("""
            SELECT DISTINCT r.district, res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            JOIN elections e ON r.election_id = e.id
            WHERE o.name = 'Representative in Congress'
            AND e.year = 2024
            AND res.municipality IS NOT NULL
            AND res.municipality != ''
        """)
        cong_district_towns = defaultdict(set)
        for district, muni in cursor.fetchall():
            if ' Ward ' in muni:
                muni = muni[:muni.index(' Ward ')]
            cong_district_towns[district].add(muni)

        # Get all historical Congress votes by town
        cursor.execute("""
            SELECT
                res.municipality,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'Representative in Congress'
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality IS NOT NULL
            GROUP BY res.municipality
        """)
        cong_town_votes = {}
        for muni, r_votes, d_votes in cursor.fetchall():
            if ' Ward ' in muni:
                base = muni[:muni.index(' Ward ')]
                if base not in cong_town_votes:
                    cong_town_votes[base] = {'r': 0, 'd': 0}
                cong_town_votes[base]['r'] += r_votes
                cong_town_votes[base]['d'] += d_votes
            else:
                if muni not in cong_town_votes:
                    cong_town_votes[muni] = {'r': 0, 'd': 0}
                cong_town_votes[muni]['r'] += r_votes
                cong_town_votes[muni]['d'] += d_votes

        for district, towns in cong_district_towns.items():
            r_votes = sum(cong_town_votes.get(t, {}).get('r', 0) for t in towns)
            d_votes = sum(cong_town_votes.get(t, {}).get('d', 0) for t in towns)
            total = r_votes + d_votes
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            data[f'cong_{district}'] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total
            }
    else:
        # Specific year - get candidate-level data for display
        cursor.execute(f"""
            SELECT
                r.district,
                c.name as candidate_name,
                c.party,
                SUM(res.votes) as votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'Representative in Congress'
            AND {year_clause}
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.district, c.name, c.party
            ORDER BY r.district, votes DESC
        """)

        cong_candidates = defaultdict(list)
        for row in cursor.fetchall():
            district, candidate_name, party, votes = row
            cong_candidates[district].append({
                'name': candidate_name,
                'party': party[0] if party else '?',
                'votes': votes
            })

        for district, candidates in cong_candidates.items():
            r_votes = sum(c['votes'] for c in candidates if c['party'] == 'R')
            d_votes = sum(c['votes'] for c in candidates if c['party'] == 'D')
            total = sum(c['votes'] for c in candidates)
            margin = ((r_votes - d_votes) / total * 100) if total > 0 else 0
            top_candidates = candidates[:2]
            data[f'cong_{district}'] = {
                'margin': round(margin, 1),
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total_votes': total,
                'candidates': top_candidates
            }

    # Towns (keyed by name) - aggregate wards into cities
    # Calculate margin per race, then average across races (not cumulative totals)
    cursor.execute(f"""
        SELECT
            CASE
                WHEN res.municipality LIKE '% Ward %'
                THEN SUBSTR(res.municipality, 1, INSTR(res.municipality, ' Ward ') - 1)
                ELSE res.municipality
            END as town,
            r.id as race_id,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE {year_clause}
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
        END, r.id
    """)

    # Collect margins by town, then average them
    town_race_data = defaultdict(list)
    for row in cursor.fetchall():
        town, race_id, r_votes, d_votes, total = row
        if town and total > 0 and r_votes > 0 and d_votes > 0:  # Only count competitive races
            margin = ((r_votes - d_votes) / total * 100)
            town_race_data[town].append({
                'margin': margin,
                'r_votes': r_votes,
                'd_votes': d_votes,
                'total': total
            })

    for town, races in town_race_data.items():
        if races:
            # Average the margins across races
            avg_margin = sum(r['margin'] for r in races) / len(races)
            total_r = sum(r['r_votes'] for r in races)
            total_d = sum(r['d_votes'] for r in races)
            total_votes = sum(r['total'] for r in races)
            data[town] = {
                'margin': round(avg_margin, 1),
                'r_votes': total_r,
                'd_votes': total_d,
                'total_votes': total_votes,
                'num_races': len(races)
            }

    # If PVI metric requested, calculate proper PVI for each district type
    if metric == 'pvi':
        # Step 1: Get statewide baseline from competitive races
        # A competitive race has both R and D candidates with votes
        cursor.execute("""
            SELECT
                r.id as race_id,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id
            HAVING r_votes > 0 AND d_votes > 0
        """)

        competitive_races = set()
        statewide_r = 0
        statewide_total = 0
        for row in cursor.fetchall():
            race_id, r_votes, d_votes = row
            competitive_races.add(race_id)
            statewide_r += r_votes
            statewide_total += r_votes + d_votes

        statewide_r_pct = (statewide_r / statewide_total * 100) if statewide_total > 0 else 50

        # Step 2: Get municipality-level competitive votes
        # This will be used to calculate PVI for each district
        cursor.execute("""
            SELECT
                res.municipality,
                r.id as race_id,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND res.municipality IS NOT NULL
            AND res.municipality != ''
            AND res.municipality NOT GLOB '[0-9]*'
            GROUP BY res.municipality, r.id
        """)

        # Build municipality -> competitive votes mapping
        # IMPORTANT: Keep ward-level granularity (Manchester Ward 8, etc.)
        # This ensures PVI is calculated only for the specific wards in a district
        muni_votes = defaultdict(lambda: {'r': 0, 'total': 0})
        for row in cursor.fetchall():
            muni, race_id, r_votes, d_votes = row
            if race_id in competitive_races:
                muni_votes[muni]['r'] += r_votes
                muni_votes[muni]['total'] += r_votes + d_votes

        # Step 3: For each House district, find its municipalities and calculate PVI
        cursor.execute("""
            SELECT DISTINCT
                r.county,
                r.district,
                res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Representative'
            AND res.municipality IS NOT NULL
            AND res.municipality NOT GLOB '[0-9]*'
        """)

        district_munis = defaultdict(set)
        for row in cursor.fetchall():
            county, district, muni = row
            if county in county_codes:
                code = county_codes[county] + str(district)
                # Keep full municipality name (including ward info) for accurate PVI
                district_munis[code].add(muni)

        # Calculate PVI for each House district
        for code, munis in district_munis.items():
            district_r = sum(muni_votes[m]['r'] for m in munis)
            district_total = sum(muni_votes[m]['total'] for m in munis)
            if district_total > 0:
                district_r_pct = district_r / district_total * 100
                pvi = district_r_pct - statewide_r_pct
                if code in data:
                    data[code]['pvi'] = round(pvi, 1)

        # Step 4: For Senate districts, find municipalities and calculate PVI
        cursor.execute("""
            SELECT DISTINCT
                r.district,
                res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'State Senator'
            AND res.municipality IS NOT NULL
            AND res.municipality NOT GLOB '[0-9]*'
        """)

        senate_munis = defaultdict(set)
        for row in cursor.fetchall():
            district, muni = row
            # Keep full municipality name (including ward info) for accurate PVI
            senate_munis[f'sen_{district}'].add(muni)

        for code, munis in senate_munis.items():
            district_r = sum(muni_votes[m]['r'] for m in munis)
            district_total = sum(muni_votes[m]['total'] for m in munis)
            if district_total > 0:
                district_r_pct = district_r / district_total * 100
                pvi = district_r_pct - statewide_r_pct
                if code in data:
                    data[code]['pvi'] = round(pvi, 1)

        # Step 5: For Exec Council districts
        cursor.execute("""
            SELECT DISTINCT
                r.district,
                res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'Executive Councilor'
            AND res.municipality IS NOT NULL
            AND res.municipality NOT GLOB '[0-9]*'
        """)

        ec_munis = defaultdict(set)
        for row in cursor.fetchall():
            district, muni = row
            # Keep full municipality name (including ward info) for accurate PVI
            ec_munis[f'ec_{district}'].add(muni)

        for code, munis in ec_munis.items():
            district_r = sum(muni_votes[m]['r'] for m in munis)
            district_total = sum(muni_votes[m]['total'] for m in munis)
            if district_total > 0:
                district_r_pct = district_r / district_total * 100
                pvi = district_r_pct - statewide_r_pct
                if code in data:
                    data[code]['pvi'] = round(pvi, 1)

        # Step 6: For Congressional districts
        cursor.execute("""
            SELECT DISTINCT
                r.district,
                res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = 'Representative in Congress'
            AND res.municipality IS NOT NULL
            AND res.municipality NOT GLOB '[0-9]*'
        """)

        cong_munis = defaultdict(set)
        for row in cursor.fetchall():
            district, muni = row
            # Keep full municipality name (including ward info) for accurate PVI
            cong_munis[f'cong_{district}'].add(muni)

        for code, munis in cong_munis.items():
            district_r = sum(muni_votes[m]['r'] for m in munis)
            district_total = sum(muni_votes[m]['total'] for m in munis)
            if district_total > 0:
                district_r_pct = district_r / district_total * 100
                pvi = district_r_pct - statewide_r_pct
                if code in data:
                    data[code]['pvi'] = round(pvi, 1)

        # Step 7: For towns, aggregate ward data into cities for PVI
        # Since the towns data (in 'data' dict) is already aggregated,
        # we need to aggregate ward-level votes for city PVI calculation
        town_aggregated = defaultdict(lambda: {'r': 0, 'total': 0})
        for muni, votes in muni_votes.items():
            # Normalize ward names to city names for the towns layer
            if ' Ward ' in muni:
                base_town = muni[:muni.index(' Ward ')]
            else:
                base_town = muni
            town_aggregated[base_town]['r'] += votes['r']
            town_aggregated[base_town]['total'] += votes['total']

        for town, votes in town_aggregated.items():
            if votes['total'] > 0:
                town_r_pct = votes['r'] / votes['total'] * 100
                pvi = town_r_pct - statewide_r_pct
                if town in data:
                    data[town]['pvi'] = round(pvi, 1)

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
    Get all districts for an office with PVI data.
    PVI = district R% (all contested races) - statewide R% (all contested races).
    Returns list sorted by current PVI (most R to most D).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get statewide baseline: R% across all contested races
    cursor.execute("""
        WITH race_totals AS (
            SELECT e.year, r.id as race_id,
                   SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r,
                   SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.year IN (2022, 2024)
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY e.year, r.id
            HAVING r > 0 AND d > 0  -- contested only
        )
        SELECT year, SUM(r) as total_r, SUM(d) as total_d
        FROM race_totals
        GROUP BY year
    """)
    state_baseline = {}
    for year, total_r, total_d in cursor.fetchall():
        state_baseline[year] = total_r / (total_r + total_d) * 100 if (total_r + total_d) > 0 else 50
    state_r_pct_2024 = state_baseline.get(2024, 50)
    state_r_pct_2022 = state_baseline.get(2022, 50)

    # Check if this is a county-based office
    is_county_based = office == 'State Representative'

    if is_county_based:
        # Get district -> municipalities mapping
        cursor.execute("""
            SELECT DISTINCT r.county, r.district, r.seats, res.municipality
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND e.year = 2024
            AND e.election_type = 'general'
        """, (office,))

        district_towns = defaultdict(set)
        district_seats = {}
        for county, district, seats, municipality in cursor.fetchall():
            key = (county, district)
            district_towns[key].add(municipality)
            district_seats[key] = seats

        # Get all votes by municipality for contested races (for PVI calculation)
        cursor.execute("""
            WITH race_totals AS (
                SELECT e.year, r.id as race_id, res.municipality,
                       SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r,
                       SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d
                FROM results res
                JOIN candidates c ON res.candidate_id = c.id
                JOIN races r ON res.race_id = r.id
                JOIN elections e ON r.election_id = e.id
                WHERE e.year IN (2022, 2024)
                AND e.election_type = 'general'
                AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
                GROUP BY e.year, r.id, res.municipality
            ),
            contested AS (
                SELECT year, municipality, r, d
                FROM race_totals
                WHERE r > 0 AND d > 0
            )
            SELECT year, municipality, SUM(r) as r, SUM(d) as d
            FROM contested
            GROUP BY year, municipality
        """)

        town_votes = defaultdict(lambda: defaultdict(lambda: {'r': 0, 'd': 0}))
        for year, municipality, r, d in cursor.fetchall():
            town_votes[year][municipality] = {'r': r, 'd': d}

        # Get State Rep race results for each district (for showing winners/margin)
        cursor.execute("""
            SELECT e.year, r.county, r.district, c.party,
                   SUM(res.votes) as votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND e.year IN (2022, 2024)
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            AND c.party IN ('Republican', 'Democratic')
            GROUP BY e.year, r.county, r.district, c.id
        """, (office,))

        # Get top vote-getter per party per district for margin calc
        district_results = defaultdict(lambda: defaultdict(lambda: {'top_r': 0, 'top_d': 0}))
        for year, county, district, party, votes in cursor.fetchall():
            key = (county, district)
            if party == 'Republican':
                district_results[key][year]['top_r'] = max(district_results[key][year]['top_r'], votes)
            elif party == 'Democratic':
                district_results[key][year]['top_d'] = max(district_results[key][year]['top_d'], votes)

        districts = []
        for (county, district), towns in district_towns.items():
            # Calculate PVI from all contested races in district towns
            r_2024 = sum(town_votes[2024].get(t, {}).get('r', 0) for t in towns)
            d_2024 = sum(town_votes[2024].get(t, {}).get('d', 0) for t in towns)
            r_2022 = sum(town_votes[2022].get(t, {}).get('r', 0) for t in towns)
            d_2022 = sum(town_votes[2022].get(t, {}).get('d', 0) for t in towns)

            if (r_2024 + d_2024) > 0:
                dist_r_pct_2024 = r_2024 / (r_2024 + d_2024) * 100
                pvi_2024 = dist_r_pct_2024 - state_r_pct_2024
            else:
                pvi_2024 = 0

            if (r_2022 + d_2022) > 0:
                dist_r_pct_2022 = r_2022 / (r_2022 + d_2022) * 100
                pvi_2022 = dist_r_pct_2022 - state_r_pct_2022
            else:
                pvi_2022 = pvi_2024

            trend = pvi_2024 - pvi_2022

            # Get State Rep race results for this district
            res_2024 = district_results[(county, district)][2024]
            top_r = res_2024['top_r']
            top_d = res_2024['top_d']
            contested = top_r > 0 and top_d > 0

            districts.append({
                'district': district,
                'county': county,
                'seats': district_seats.get((county, district), 1),
                'pvi': round(pvi_2024, 1),
                'trend': round(trend, 1),
                'r_votes': top_r,
                'd_votes': top_d,
                'contested': contested
            })

        conn.close()
        return sorted(districts, key=lambda x: -x['pvi'])

    else:
        # Statewide districts (Senate, EC, Congress) - single seat, sum is OK
        cursor.execute("""
            SELECT
                e.year,
                r.district,
                r.seats,
                SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
                SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE o.name = ?
            AND e.year IN (2022, 2024)
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY e.year, r.district
            ORDER BY r.district, e.year
        """, (office,))

        district_data = defaultdict(dict)
        district_seats = {}
        for row in cursor.fetchall():
            year, district, seats, r_votes, d_votes = row
            district_data[district][year] = {'r': r_votes, 'd': d_votes}
            district_seats[district] = seats

        districts = []
        for district, years_data in district_data.items():
            # 2024 PVI
            if 2024 in years_data:
                r = years_data[2024]['r']
                d = years_data[2024]['d']
                rd_total = r + d
                if rd_total > 0:
                    dist_r_pct = (r / rd_total) * 100
                    pvi_2024 = dist_r_pct - state_r_pct_2024
                else:
                    pvi_2024 = 0
            else:
                pvi_2024 = 0
                r, d = 0, 0

            # 2022 PVI for trend
            if 2022 in years_data:
                r22 = years_data[2022]['r']
                d22 = years_data[2022]['d']
                rd_total_22 = r22 + d22
                if rd_total_22 > 0:
                    dist_r_pct_22 = (r22 / rd_total_22) * 100
                    pvi_2022 = dist_r_pct_22 - state_r_pct_2022
                else:
                    pvi_2022 = pvi_2024
            else:
                pvi_2022 = pvi_2024

            trend = pvi_2024 - pvi_2022

            districts.append({
                'district': district,
                'county': None,
                'seats': district_seats.get(district, 1),
                'pvi': round(pvi_2024, 1),
                'trend': round(trend, 1),
                'r_votes': r,
                'd_votes': d,
                'contested': r > 0 and d > 0
            })

    conn.close()

    # Sort by PVI (most R first)
    districts.sort(key=lambda x: -x['pvi'])

    return districts


# ============== DEEP ANALYSIS FUNCTIONS ==============

def get_undervote_analysis():
    """
    Analyze undervoting patterns - where voters skip downballot races.
    Compares total votes in top-of-ticket races vs downballot races.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get total votes by office and year for each municipality
    cursor.execute("""
        SELECT
            e.year,
            o.name as office,
            res.municipality,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND res.municipality IS NOT NULL
        AND res.municipality != ''
        AND res.municipality NOT GLOB '[0-9]*'
        GROUP BY e.year, o.name, res.municipality
    """)

    # Organize data by year and municipality
    data = defaultdict(lambda: defaultdict(dict))
    for year, office, muni, votes in cursor.fetchall():
        # Normalize ward names
        if ' Ward ' in muni:
            muni = muni[:muni.index(' Ward ')]
        if muni not in data[year]:
            data[year][muni] = {}
        if office not in data[year][muni]:
            data[year][muni][office] = 0
        data[year][muni][office] += votes

    # Calculate undervote rates by comparing to top-of-ticket
    results = {'by_year': {}, 'by_town': {}, 'worst_undervote': []}

    top_ticket = ['President of the United States', 'Governor']
    downballot = ['State Representative', 'State Senator', 'Executive Councilor']

    for year in sorted(data.keys()):
        year_stats = {'towns': 0, 'avg_undervote': 0, 'by_office': {}}
        town_undervotes = []

        for muni, offices in data[year].items():
            # Find top-of-ticket votes
            top_votes = max(offices.get(t, 0) for t in top_ticket)
            if top_votes == 0:
                continue

            # Calculate undervote for each downballot office
            for office in downballot:
                if office in offices:
                    office_votes = offices[office]
                    undervote_pct = ((top_votes - office_votes) / top_votes) * 100
                    town_undervotes.append({
                        'year': year,
                        'town': muni,
                        'office': office,
                        'top_votes': top_votes,
                        'office_votes': office_votes,
                        'undervote_pct': round(undervote_pct, 1)
                    })

                    if office not in year_stats['by_office']:
                        year_stats['by_office'][office] = []
                    year_stats['by_office'][office].append(undervote_pct)

        # Calculate averages for the year
        for office in year_stats['by_office']:
            vals = year_stats['by_office'][office]
            year_stats['by_office'][office] = round(sum(vals) / len(vals), 1) if vals else 0

        results['by_year'][year] = year_stats

        # Track worst undervotes
        results['worst_undervote'].extend(town_undervotes)

    # Sort to find worst undervote towns
    results['worst_undervote'].sort(key=lambda x: -x['undervote_pct'])
    results['worst_undervote'] = results['worst_undervote'][:50]

    conn.close()
    return results


def get_turnout_patterns():
    """
    Analyze turnout patterns by town, year using official ballots cast data.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get ballots cast from voter_registration table
    cursor.execute("""
        SELECT
            e.year,
            v.municipality,
            v.ballots_cast
        FROM voter_registration v
        JOIN elections e ON v.election_id = e.id
        WHERE e.election_type = 'general'
        AND v.ballots_cast > 0
    """)

    # Organize by town (aggregating wards into cities)
    town_data = defaultdict(lambda: defaultdict(int))
    for year, muni, ballots in cursor.fetchall():
        if ' Ward ' in muni:
            muni = muni[:muni.index(' Ward ')]
        town_data[muni][year] += ballots

    # Calculate turnout metrics
    results = {
        'by_town': [],
        'by_year': {},
        'presidential_vs_midterm': [],
        'highest_turnout_towns': [],
        'lowest_turnout_towns': []
    }

    # Get max turnout per town as proxy for voter base
    town_max = {}
    for muni, years in town_data.items():
        town_max[muni] = max(years.values()) if years else 0

    # Calculate year-over-year and pres vs midterm
    for muni, years in town_data.items():
        if town_max[muni] < 100:  # Skip very small towns
            continue

        town_info = {
            'town': muni,
            'max_turnout': town_max[muni],
            'years': {}
        }

        pres_years = []
        mid_years = []

        for year in sorted(years.keys()):
            turnout = years[year]
            turnout_pct = (turnout / town_max[muni]) * 100 if town_max[muni] > 0 else 0
            town_info['years'][year] = {
                'votes': turnout,
                'pct': round(turnout_pct, 1)
            }

            if year % 4 == 0:  # Presidential year
                pres_years.append(turnout)
            else:
                mid_years.append(turnout)

        # Presidential vs midterm drop
        if pres_years and mid_years:
            avg_pres = sum(pres_years) / len(pres_years)
            avg_mid = sum(mid_years) / len(mid_years)
            drop = ((avg_pres - avg_mid) / avg_pres) * 100 if avg_pres > 0 else 0
            town_info['pres_mid_drop'] = round(drop, 1)
            results['presidential_vs_midterm'].append({
                'town': muni,
                'avg_presidential': int(avg_pres),
                'avg_midterm': int(avg_mid),
                'drop_pct': round(drop, 1)
            })

        results['by_town'].append(town_info)

    # Sort for highest/lowest
    results['presidential_vs_midterm'].sort(key=lambda x: -x['drop_pct'])

    # Year totals
    year_totals = defaultdict(int)
    for muni, years in town_data.items():
        for year, votes in years.items():
            year_totals[year] += votes

    for year in sorted(year_totals.keys()):
        results['by_year'][year] = {
            'total_votes': year_totals[year],
            'is_presidential': year % 4 == 0
        }

    conn.close()
    return results


def get_ticket_splitting_analysis():
    """
    Analyze ticket splitting - where voters vote for different parties
    in different races.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get results by municipality, year, and office
    cursor.execute("""
        SELECT
            e.year,
            res.municipality,
            o.name as office,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.party IN ('Republican', 'Democratic')
        AND res.municipality IS NOT NULL
        AND res.municipality != ''
        AND res.municipality NOT GLOB '[0-9]*'
        GROUP BY e.year, res.municipality, o.name, c.party
    """)

    # Organize data
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'R': 0, 'D': 0})))
    for year, muni, office, party, votes in cursor.fetchall():
        if ' Ward ' in muni:
            muni = muni[:muni.index(' Ward ')]
        p = 'R' if party == 'Republican' else 'D'
        data[year][muni][office][p] += votes

    results = {
        'by_year': {},  # Old format: year -> list of splits (for ticket_splitting.html)
        'split_towns': [],  # New format: flat list of all splits (for deep_analysis.html)
        'total_splits': 0
    }

    # Compare Governor vs State House in each town
    comparisons = [
        ('Governor', 'State Representative'),
        ('Governor', 'State Senator'),
        ('President of the United States', 'State Representative'),
        ('President of the United States', 'Governor')
    ]

    all_pres_rep_splits = []  # Specifically President vs State Rep for old template

    for year in sorted(data.keys()):
        year_splits = []

        for muni, offices in data[year].items():
            for office1, office2 in comparisons:
                if office1 in offices and office2 in offices:
                    o1 = offices[office1]
                    o2 = offices[office2]

                    # Calculate R margin for each office
                    total1 = o1['R'] + o1['D']
                    total2 = o2['R'] + o2['D']

                    if total1 > 0 and total2 > 0:
                        margin1 = ((o1['R'] - o1['D']) / total1) * 100
                        margin2 = ((o2['R'] - o2['D']) / total2) * 100
                        split = margin2 - margin1  # Positive = more R downballot

                        if abs(split) > 5:  # Meaningful split
                            year_splits.append({
                                'year': year,
                                'town': muni,
                                'office1': office1,
                                'office2': office2,
                                'margin1': round(margin1, 1),
                                'margin2': round(margin2, 1),
                                'split': round(split, 1)
                            })

                            # Old format for President vs State Rep specifically
                            if office1 == 'President of the United States' and office2 == 'State Representative':
                                winner1 = 'R' if margin1 > 0 else 'D'
                                winner2 = 'R' if margin2 > 0 else 'D'
                                if winner1 != winner2:  # Actually split ticket
                                    all_pres_rep_splits.append({
                                        'year': year,
                                        'town': muni,
                                        'president': winner1,
                                        'president_margin': round(margin1, 1),
                                        'state_rep': winner2,
                                        'state_rep_margin': round(margin2, 1),
                                        'split_magnitude': abs(margin1) + abs(margin2)
                                    })

        results['split_towns'].extend(year_splits)

    # Group President vs State Rep splits by year (old format for ticket_splitting.html)
    for s in all_pres_rep_splits:
        year = s['year']
        if year not in results['by_year']:
            results['by_year'][year] = []
        results['by_year'][year].append(s)

    # Sort each year's splits by magnitude
    for year in results['by_year']:
        results['by_year'][year].sort(key=lambda x: -x['split_magnitude'])

    results['total_splits'] = len(all_pres_rep_splits)

    # Sort split_towns by largest splits (for deep_analysis.html)
    results['split_towns'].sort(key=lambda x: -abs(x['split']))
    results['split_towns'] = results['split_towns'][:100]

    conn.close()
    return results


def get_bellwether_analysis():
    """
    Identify bellwether towns - those that best predict NH House control.
    Based on State Rep votes and which party wins majority of House seats.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get House seats won by party for each year
    # Winners are top N candidates by total votes where N = seats
    cursor.execute("""
        WITH race_totals AS (
            SELECT
                r.id as race_id,
                e.year,
                c.id as candidate_id,
                c.party,
                r.seats,
                SUM(res.votes) as total_votes,
                RANK() OVER (PARTITION BY r.id ORDER BY SUM(res.votes) DESC) as rank
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.election_type = 'general'
            AND o.name = 'State Representative'
            AND c.party IN ('Republican', 'Democratic')
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id, c.id
        )
        SELECT year, party, COUNT(*) as seats_won
        FROM race_totals
        WHERE rank <= seats
        GROUP BY year, party
    """)

    house_control = {}
    seats_by_year = defaultdict(lambda: {'R': 0, 'D': 0})
    for year, party, seats in cursor.fetchall():
        p = 'R' if party == 'Republican' else 'D'
        seats_by_year[year][p] = seats

    for year, seats in seats_by_year.items():
        house_control[year] = 'R' if seats['R'] > seats['D'] else 'D'

    # Get statewide State Rep vote totals by year
    cursor.execute("""
        SELECT
            e.year,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND o.name = 'State Representative'
        AND c.party IN ('Republican', 'Democratic')
        GROUP BY e.year, c.party
    """)

    statewide_rep_votes = defaultdict(lambda: {'R': 0, 'D': 0})
    for year, party, votes in cursor.fetchall():
        p = 'R' if party == 'Republican' else 'D'
        statewide_rep_votes[year][p] = votes

    # Calculate statewide margins (for reference)
    statewide_margins = {}
    for year, votes in statewide_rep_votes.items():
        total = votes['R'] + votes['D']
        if total > 0:
            margin = ((votes['R'] - votes['D']) / total) * 100
            statewide_margins[year] = round(margin, 1)

    # Get town-level State Rep votes
    # Use CTE to pick only the canonical race (highest total votes) for each district/year
    # This avoids double-counting from duplicate race imports
    cursor.execute("""
        WITH race_totals AS (
            SELECT r.id as race_id, e.year, r.county, r.district, SUM(res.votes) as total
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.election_type = 'general'
            AND o.name = 'State Representative'
            GROUP BY r.id
        ),
        canonical_races AS (
            SELECT race_id, year, county, district
            FROM race_totals rt
            WHERE rt.total = (
                SELECT MAX(rt2.total)
                FROM race_totals rt2
                WHERE rt2.year = rt.year AND rt2.county = rt.county AND rt2.district = rt.district
            )
        )
        SELECT
            cr.year,
            res.municipality,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN canonical_races cr ON res.race_id = cr.race_id
        WHERE c.party IN ('Republican', 'Democratic')
        AND res.municipality IS NOT NULL
        AND res.municipality != ''
        AND res.municipality NOT GLOB '[0-9]*'
        GROUP BY cr.year, res.municipality, c.party
    """)

    town_data = defaultdict(lambda: defaultdict(lambda: {'R': 0, 'D': 0}))
    for year, muni, party, votes in cursor.fetchall():
        if ' Ward ' in muni:
            muni = muni[:muni.index(' Ward ')]
        p = 'R' if party == 'Republican' else 'D'
        town_data[muni][year][p] += votes

    # Calculate bellwether score for each town
    # Score = how often town's State Rep vote predicted House control
    bellwethers = []

    for muni, years_data in town_data.items():
        correct_calls = 0
        total_calls = 0
        deviations = []

        for year, votes in years_data.items():
            if year not in house_control:
                continue

            total = votes['R'] + votes['D']
            if total < 50:  # Skip tiny samples
                continue

            town_margin = ((votes['R'] - votes['D']) / total) * 100
            town_winner = 'R' if town_margin > 0 else 'D'

            # Did town's State Rep vote predict House control?
            if town_winner == house_control[year]:
                correct_calls += 1
            total_calls += 1

            # Also track deviation from statewide State Rep margin
            if year in statewide_margins:
                deviation = abs(town_margin - statewide_margins[year])
                deviations.append(deviation)

        if total_calls >= 3:  # Need at least 3 elections
            bellwethers.append({
                'town': muni,
                'avg_deviation': round(sum(deviations) / len(deviations), 1) if deviations else 0,
                'correct_calls': correct_calls,
                'total_calls': total_calls,
                'accuracy': round((correct_calls / total_calls) * 100, 1) if total_calls > 0 else 0,
                'elections': total_calls
            })

    # Sort by accuracy first, then lowest deviation
    bellwethers.sort(key=lambda x: (-x['accuracy'], x['avg_deviation']))

    conn.close()

    return {
        'statewide_margins': statewide_margins,
        'house_control': house_control,
        'seats_by_year': dict(seats_by_year),
        'bellwethers': bellwethers[:50],
        'best_predictors': [b for b in bellwethers if b['accuracy'] == 100][:20]
    }


# ============== ADVANCED STATISTICAL ANALYSIS ==============

def get_swing_analysis():
    """
    Identify districts most likely to flip based on:
    - Close margins (< 5%)
    - Trending toward the minority party
    - Historical volatility
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all State Rep districts with 2022 and 2024 data
    cursor.execute("""
        SELECT
            e.year,
            r.county,
            r.district,
            r.seats,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = 'State Representative'
        AND e.year IN (2020, 2022, 2024)
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND c.party IN ('Republican', 'Democratic')
        GROUP BY e.year, r.county, r.district, c.name, c.party
    """)

    # Aggregate by district and year using top vote-getter
    district_data = defaultdict(lambda: defaultdict(lambda: {'top_r': 0, 'top_d': 0, 'seats': 1}))
    for row in cursor.fetchall():
        year, county, district, seats, party, votes = row
        key = (county, district)
        if party == 'Republican':
            district_data[key][year]['top_r'] = max(district_data[key][year]['top_r'], votes)
        else:
            district_data[key][year]['top_d'] = max(district_data[key][year]['top_d'], votes)
        district_data[key][year]['seats'] = seats

    swing_districts = []
    for (county, district), years in district_data.items():
        if 2024 not in years:
            continue

        # Calculate 2024 margin
        r24, d24 = years[2024]['top_r'], years[2024]['top_d']
        total24 = r24 + d24
        if total24 == 0:
            continue
        margin24 = ((r24 - d24) / total24) * 100
        winner24 = 'R' if margin24 > 0 else 'D'
        contested24 = r24 > 0 and d24 > 0

        # Calculate trend from 2022 - ONLY if both years were contested
        trend = 0
        trend_valid = False
        margin22 = None
        if 2022 in years:
            r22, d22 = years[2022]['top_r'], years[2022]['top_d']
            total22 = r22 + d22
            contested22 = r22 > 0 and d22 > 0
            if total22 > 0:
                margin22 = round(((r22 - d22) / total22) * 100, 1)
            if contested22 and contested24 and total22 > 0:
                trend = margin24 - margin22
                trend_valid = True

        # Calculate volatility - ONLY from contested races
        margins = []
        for y in [2020, 2022, 2024]:
            if y in years:
                r, d = years[y]['top_r'], years[y]['top_d']
                t = r + d
                # Only include contested races in volatility calculation
                if t > 0 and r > 0 and d > 0:
                    margins.append(((r - d) / t) * 100)

        volatility = 0
        if len(margins) >= 2:
            avg = sum(margins) / len(margins)
            volatility = (sum((m - avg) ** 2 for m in margins) / len(margins)) ** 0.5

        # Skip uncontested races entirely
        if not contested24:
            continue

        # Score: closer margin = higher score, trending against winner = higher score
        is_competitive = abs(margin24) < 10
        trending_against = trend_valid and ((winner24 == 'R' and trend < 0) or (winner24 == 'D' and trend > 0))

        if is_competitive or trending_against or volatility > 5:
            # Get towns in this district
            towns = queries.get_district_towns(county, district, 'State Representative')

            # Get the actual 2024 winners
            winners = queries.get_district_winners(county, district, 'State Representative', 2024)

            swing_districts.append({
                'county': county,
                'district': district,
                'seats': years[2024]['seats'],
                'margin': round(margin24, 1),
                'margin22': margin22,
                'winner': winner24,
                'winners': winners,
                'trend': round(trend, 1) if trend_valid else None,
                'volatility': round(volatility, 1),
                'contested': True,
                'trend_valid': trend_valid,
                'towns': towns,
                'flip_likelihood': 'High' if abs(margin24) < 5 and trending_against else
                                   'Medium' if abs(margin24) < 10 else 'Low'
            })

    # Sort by closest margin
    swing_districts.sort(key=lambda x: abs(x['margin']))

    conn.close()

    # Get trending districts (sorted by trend magnitude, limited for display)
    trending_r = sorted([d for d in swing_districts if d['trend_valid'] and d['trend'] > 3],
                       key=lambda x: -x['trend'])[:15]
    trending_d = sorted([d for d in swing_districts if d['trend_valid'] and d['trend'] < -3],
                       key=lambda x: x['trend'])[:15]

    # High flip = single-seat districts with close margins trending against holder
    high_flip = [d for d in swing_districts
                 if d['flip_likelihood'] == 'High' and d['seats'] == 1]

    # Competitive multi-seat = multi-seat districts with close margins (likely to stay split)
    competitive_multi = [d for d in swing_districts
                        if d['seats'] > 1 and abs(d['margin']) < 5]

    return {
        'swing_districts': swing_districts[:50],
        'high_flip': high_flip,
        'competitive_multi': competitive_multi,
        'trending_r': trending_r,
        'trending_d': trending_d
    }


def get_multi_seat_analysis():
    """
    Analyze multi-seat districts looking at marginal seats.
    For each district, find the gap between the last winner and first loser.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all multi-seat State Rep districts
    cursor.execute("""
        SELECT DISTINCT r.county, r.district, r.seats
        FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = 'State Representative'
        AND r.seats > 1
        AND e.year = 2024
        AND e.election_type = 'general'
    """)

    districts = []
    for county, district, seats in cursor.fetchall():
        # Get candidate data for multiple years
        years_data = {}
        for year in [2016, 2018, 2020, 2022, 2024]:
            data = queries.get_district_candidates(county, district, 'State Representative', year)
            if data['candidates']:
                years_data[year] = data

        if 2024 not in years_data:
            continue

        data_2024 = years_data[2024]
        candidates = data_2024['candidates']
        seats = data_2024['seats']

        if len(candidates) <= seats:
            continue  # No losers = uncontested

        # Find last winner and first loser
        winners = [c for c in candidates if c['winner']]
        losers = [c for c in candidates if not c['winner']]

        if not winners or not losers:
            continue

        last_winner = winners[-1]  # Lowest vote-getter who won
        first_loser = losers[0]    # Highest vote-getter who lost

        # Calculate the gap
        gap = last_winner['votes'] - first_loser['votes']
        total_votes = last_winner['votes'] + first_loser['votes']
        gap_pct = round((gap / total_votes) * 100, 1) if total_votes > 0 else 0

        # Determine which party's seat is at risk
        at_risk_party = last_winner['party']
        challenger_party = first_loser['party']

        # Count seats by party
        r_seats = len([w for w in winners if w['party'] == 'Republican'])
        d_seats = len([w for w in winners if w['party'] == 'Democratic'])

        # Calculate long-term trend using all available years
        margins = []
        for year in sorted(years_data.keys()):
            yr_data = years_data[year]
            yr_candidates = yr_data['candidates']
            r_votes = max([c['votes'] for c in yr_candidates if c['party'] == 'Republican'], default=0)
            d_votes = max([c['votes'] for c in yr_candidates if c['party'] == 'Democratic'], default=0)
            if r_votes > 0 and d_votes > 0:
                total = r_votes + d_votes
                margins.append({'year': year, 'margin': round((r_votes - d_votes) / total * 100, 1)})

        # Calculate trend (average change per cycle)
        trend = None
        if len(margins) >= 2:
            first_margin = margins[0]['margin']
            last_margin = margins[-1]['margin']
            years_span = margins[-1]['year'] - margins[0]['year']
            if years_span > 0:
                trend = round((last_margin - first_margin) / (years_span / 2), 1)  # per cycle

        towns = queries.get_district_towns(county, district, 'State Representative')

        districts.append({
            'county': county,
            'district': district,
            'seats': seats,
            'r_seats': r_seats,
            'd_seats': d_seats,
            'last_winner': last_winner,
            'first_loser': first_loser,
            'gap': gap,
            'gap_pct': gap_pct,
            'at_risk_party': at_risk_party[0],  # R or D
            'challenger_party': challenger_party[0],
            'margins': margins,
            'trend': trend,
            'towns': towns
        })

    conn.close()

    # Sort by smallest gap (most vulnerable)
    districts.sort(key=lambda x: x['gap'])

    # Only include cross-party challenges (where challenger is opposite party)
    cross_party = [d for d in districts if d['at_risk_party'] != d['challenger_party']]

    # Separate by which party's seat is at risk
    r_at_risk = [d for d in cross_party if d['at_risk_party'] == 'R'][:15]
    d_at_risk = [d for d in cross_party if d['at_risk_party'] == 'D'][:15]

    return {
        'all': cross_party[:30],
        'r_at_risk': r_at_risk,
        'd_at_risk': d_at_risk
    }


def get_correlation_analysis():
    """
    Analyze correlations between various factors:
    - Turnout vs margin
    - Town size vs partisan lean
    - Incumbent advantage
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get town-level data for 2024
    cursor.execute("""
        SELECT
            res.municipality,
            o.name as office,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.year = 2024
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND c.party IN ('Republican', 'Democratic')
        GROUP BY res.municipality, o.name, c.party
    """)

    town_data = defaultdict(lambda: {'total_votes': 0, 'r_votes': 0, 'd_votes': 0})
    for row in cursor.fetchall():
        muni, office, party, votes = row
        town_data[muni]['total_votes'] += votes
        if party == 'Republican':
            town_data[muni]['r_votes'] += votes
        else:
            town_data[muni]['d_votes'] += votes

    # Calculate margin and size for each town
    towns = []
    for muni, data in town_data.items():
        total = data['r_votes'] + data['d_votes']
        if total < 100:
            continue
        margin = ((data['r_votes'] - data['d_votes']) / total) * 100
        towns.append({
            'town': muni,
            'total_votes': data['total_votes'],
            'margin': margin,
            'size': data['total_votes']
        })

    # Sort by size to find size-partisan correlation
    towns.sort(key=lambda x: -x['size'])
    large_towns = towns[:50]
    small_towns = towns[-50:]

    large_avg_margin = sum(t['margin'] for t in large_towns) / len(large_towns) if large_towns else 0
    small_avg_margin = sum(t['margin'] for t in small_towns) / len(small_towns) if small_towns else 0

    conn.close()

    return {
        'size_correlation': {
            'large_towns_avg_margin': round(large_avg_margin, 1),
            'small_towns_avg_margin': round(small_avg_margin, 1),
            'urban_rural_gap': round(small_avg_margin - large_avg_margin, 1)
        },
        'largest_r_towns': sorted([t for t in towns if t['margin'] > 0], key=lambda x: -x['size'])[:10],
        'largest_d_towns': sorted([t for t in towns if t['margin'] < 0], key=lambda x: -x['size'])[:10],
        'most_r_towns': sorted(towns, key=lambda x: -x['margin'])[:10],
        'most_d_towns': sorted(towns, key=lambda x: x['margin'])[:10]
    }


def get_long_term_trends():
    """
    Analyze long-term partisan trends by region/county.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get county-level margins over time
    cursor.execute("""
        SELECT
            e.year,
            r.county,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND c.party IN ('Republican', 'Democratic')
        AND r.county IS NOT NULL
        GROUP BY e.year, r.county, c.party
    """)

    county_data = defaultdict(lambda: defaultdict(lambda: {'r': 0, 'd': 0}))
    for row in cursor.fetchall():
        year, county, party, votes = row
        if party == 'Republican':
            county_data[county][year]['r'] += votes
        else:
            county_data[county][year]['d'] += votes

    county_trends = []
    for county, years in county_data.items():
        sorted_years = sorted(years.keys())
        if len(sorted_years) < 2:
            continue

        # Calculate margin for first and last year
        first_year = sorted_years[0]
        last_year = sorted_years[-1]

        r1, d1 = years[first_year]['r'], years[first_year]['d']
        r2, d2 = years[last_year]['r'], years[last_year]['d']

        if r1 + d1 == 0 or r2 + d2 == 0:
            continue

        margin1 = ((r1 - d1) / (r1 + d1)) * 100
        margin2 = ((r2 - d2) / (r2 + d2)) * 100
        shift = margin2 - margin1

        county_trends.append({
            'county': county,
            'first_year': first_year,
            'last_year': last_year,
            'first_margin': round(margin1, 1),
            'last_margin': round(margin2, 1),
            'total_shift': round(shift, 1),
            'shift_per_year': round(shift / (last_year - first_year), 2) if last_year > first_year else 0
        })

    # Sort by shift
    county_trends.sort(key=lambda x: -x['total_shift'])

    conn.close()

    return {
        'county_trends': county_trends,
        'shifting_r': [c for c in county_trends if c['total_shift'] > 0],
        'shifting_d': [c for c in county_trends if c['total_shift'] < 0],
        'most_stable': sorted(county_trends, key=lambda x: abs(x['total_shift']))[:5]
    }


def get_comprehensive_stats():
    """
    Get all statistical analyses in one call.
    """
    return {
        'swing': get_swing_analysis(),
        'correlation': get_correlation_analysis(),
        'trends': get_long_term_trends(),
        'bellwether': get_bellwether_analysis()
    }


def get_trump_comparison():
    """
    Compare R State Rep performance vs Trump in 2024.
    Returns districts where R candidates under/outperformed Trump.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get Trump margin by town in 2024
    cursor.execute("""
        SELECT
            res.municipality,
            SUM(CASE WHEN c.party = 'Republican' THEN res.votes ELSE 0 END) as r_votes,
            SUM(CASE WHEN c.party = 'Democratic' THEN res.votes ELSE 0 END) as d_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.year = 2024
        AND e.election_type = 'general'
        AND o.name = 'President of the United States'
        AND c.party IN ('Republican', 'Democratic')
        GROUP BY res.municipality
    """)

    trump_by_town = {}
    for row in cursor.fetchall():
        town, r, d = row
        if r + d > 0:
            trump_by_town[town] = {'r': r, 'd': d}

    # Get State Rep margins by district in 2024
    cursor.execute("""
        SELECT
            r.county,
            r.district,
            r.seats,
            res.municipality,
            c.name as candidate_name,
            c.party,
            res.votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE e.year = 2024
        AND e.election_type = 'general'
        AND o.name = 'State Representative'
        AND c.party IN ('Republican', 'Democratic')
        ORDER BY r.county, r.district, res.municipality, c.party
    """)

    # Aggregate by district
    district_data = defaultdict(lambda: {
        'towns': set(),
        'r_votes': 0,
        'd_votes': 0,
        'seats': 1,
        'r_candidates': set(),
        'd_candidates': set()
    })

    for row in cursor.fetchall():
        county, district, seats, town, candidate, party, votes = row
        key = (county, district)
        district_data[key]['towns'].add(town)
        district_data[key]['seats'] = seats or 1
        if party == 'Republican':
            district_data[key]['r_votes'] += votes
            district_data[key]['r_candidates'].add(candidate)
        else:
            district_data[key]['d_votes'] += votes
            district_data[key]['d_candidates'].add(candidate)

    conn.close()

    # Calculate comparisons
    results = []
    for (county, district), data in district_data.items():
        r_votes = data['r_votes']
        d_votes = data['d_votes']
        seats = data['seats']
        n_r = len(data['r_candidates'])
        n_d = len(data['d_candidates'])

        # Skip uncontested races
        if r_votes == 0 or d_votes == 0 or n_r == 0 or n_d == 0:
            continue

        # Normalize votes by number of candidates per party
        # This handles 1R vs 2D races fairly
        r_avg = r_votes / n_r
        d_avg = d_votes / n_d
        rep_margin = ((r_avg - d_avg) / (r_avg + d_avg)) * 100

        # Calculate Trump margin for this district's towns (vote-weighted)
        trump_r = 0
        trump_d = 0
        for town in data['towns']:
            if town in trump_by_town:
                trump_r += trump_by_town[town]['r']
                trump_d += trump_by_town[town]['d']

        if trump_r + trump_d == 0:
            continue

        trump_margin = ((trump_r - trump_d) / (trump_r + trump_d)) * 100

        # Gap: positive means R outperformed Trump
        gap = rep_margin - trump_margin

        results.append({
            'county': county,
            'district': district,
            'towns': ', '.join(sorted(data['towns'])),
            'trump': trump_margin,
            'rep': rep_margin,
            'gap': gap,
            'r_candidates': sorted(data['r_candidates'])
        })

    # Separate under/outperformers
    underperformers = sorted([r for r in results if r['gap'] < 0], key=lambda x: x['gap'])
    outperformers = sorted([r for r in results if r['gap'] >= 0], key=lambda x: -x['gap'])

    # Average gap
    avg_gap = sum(r['gap'] for r in results) / len(results) if results else 0

    return {
        'underperformers': underperformers,
        'outperformers': outperformers,
        'avg_gap': avg_gap
    }
