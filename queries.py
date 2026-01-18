#!/usr/bin/env python3
"""
Database queries for NH Election Results Explorer
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "nh_elections.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_towns():
    """Get list of all municipalities."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT municipality
        FROM results
        WHERE municipality NOT GLOB '[0-9]*'
        AND municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
        ORDER BY municipality
    """)
    towns = [row[0] for row in cursor.fetchall()]
    conn.close()
    return towns


def get_all_counties():
    """Get list of all counties."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT county FROM races WHERE county IS NOT NULL ORDER BY county")
    counties = [row[0] for row in cursor.fetchall()]
    conn.close()
    return counties


def get_districts_by_county(county):
    """Get all districts in a county (using most recent year's seat counts)."""
    conn = get_connection()
    cursor = conn.cursor()
    # Get the most recent seat count for each district/office combo
    cursor.execute("""
        SELECT r.district, r.seats, o.name as office
        FROM races r
        JOIN offices o ON r.office_id = o.id
        JOIN elections e ON r.election_id = e.id
        WHERE r.county = ?
        AND e.year = (
            SELECT MAX(e2.year)
            FROM races r2
            JOIN elections e2 ON r2.election_id = e2.id
            WHERE r2.county = r.county AND r2.district = r.district
            AND r2.office_id = r.office_id
        )
        GROUP BY r.district, o.name
        ORDER BY o.name, CAST(r.district AS INTEGER)
    """, (county,))
    districts = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return districts


def get_statewide_districts(office):
    """Get all districts for a statewide office (State Senate, Exec Council, Congress)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT r.district, r.seats
        FROM races r
        JOIN offices o ON r.office_id = o.id
        JOIN elections e ON r.election_id = e.id
        WHERE o.name = ?
        AND e.year = (SELECT MAX(e2.year) FROM elections e2)
        ORDER BY CAST(r.district AS INTEGER)
    """, (office,))
    districts = [{'district': row[0], 'seats': row[1], 'office': office} for row in cursor.fetchall()]
    conn.close()
    return districts


def get_statewide_district_results(office, district):
    """Get results for a statewide district across all years."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            e.year,
            r.seats,
            c.name as candidate,
            c.party,
            SUM(res.votes) as votes,
            RANK() OVER (PARTITION BY e.year ORDER BY SUM(res.votes) DESC) as rank
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = ?
        AND r.district = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        GROUP BY e.year, c.id
        ORDER BY e.year DESC, votes DESC
    """, (office, district))

    results = []
    for row in cursor.fetchall():
        year, seats, name, party, votes, rank = row
        results.append({
            'year': year,
            'seats': seats,
            'name': name,
            'party': party,
            'votes': votes,
            'is_winner': rank <= seats
        })

    conn.close()
    return results


def get_towns_in_statewide_district(office, district):
    """Get all towns that vote in a statewide district."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT res.municipality
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = ?
        AND r.district = ?
        AND res.municipality NOT GLOB '[0-9]*'
        AND res.municipality NOT IN ('Undervotes', 'Overvotes', 'Write-Ins', 'TOTALS')
        ORDER BY res.municipality
    """, (office, district))

    towns = [row[0] for row in cursor.fetchall()]
    conn.close()
    return towns


def get_statewide_party_control():
    """Get party control summary for all offices across years."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get winners by office and year
    cursor.execute("""
        WITH race_totals AS (
            SELECT
                e.year,
                o.name as office,
                r.id as race_id,
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
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id, c.id
        )
        SELECT
            year,
            office,
            party,
            COUNT(*) as seats_won
        FROM race_totals
        WHERE rank <= seats
        GROUP BY year, office, party
        ORDER BY year, office, party
    """)

    results = {}
    for row in cursor.fetchall():
        year, office, party, seats = row
        if year not in results:
            results[year] = {}
        if office not in results[year]:
            results[year][office] = {'Republican': 0, 'Democratic': 0, 'Other': 0}
        if party in ['Republican', 'Democratic']:
            results[year][office][party] = seats
        else:
            results[year][office]['Other'] += seats

    conn.close()
    return results


def get_town_results(town, year=None):
    """Get all election results for a town."""
    conn = get_connection()
    cursor = conn.cursor()

    # First, get the town's votes and race info
    query = """
        SELECT
            e.year,
            o.name as office,
            r.id as race_id,
            r.district,
            r.county,
            r.seats,
            c.id as candidate_id,
            c.name as candidate,
            c.party,
            res.votes as town_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
    """
    params = [town]

    if year:
        query += " AND e.year = ?"
        params.append(year)

    query += " ORDER BY e.year DESC, o.name, r.district, res.votes DESC"

    cursor.execute(query, params)
    town_results = [dict(row) for row in cursor.fetchall()]

    # Now get district-wide totals for winner determination
    race_ids = set(r['race_id'] for r in town_results)
    if not race_ids:
        conn.close()
        return []

    # Get totals per candidate per race
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

    # Build winner lookup
    race_winners = {}
    current_race = None
    rank = 0
    for row in cursor.fetchall():
        race_id, seats, candidate_id, total_votes = row
        if race_id != current_race:
            current_race = race_id
            rank = 0
        rank += 1
        if race_id not in race_winners:
            race_winners[race_id] = {}
        race_winners[race_id][candidate_id] = {
            'total_votes': total_votes,
            'is_winner': rank <= seats
        }

    # Merge winner info into town results
    for r in town_results:
        race_id = r['race_id']
        cand_id = r['candidate_id']
        if race_id in race_winners and cand_id in race_winners[race_id]:
            r['total_votes'] = race_winners[race_id][cand_id]['total_votes']
            r['is_winner'] = race_winners[race_id][cand_id]['is_winner']
        else:
            r['total_votes'] = r['town_votes']
            r['is_winner'] = False

    conn.close()
    return town_results


def get_town_trends(town):
    """Get voting trend data for a town."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            e.year,
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
        GROUP BY e.year, o.name
        ORDER BY e.year, o.name
    """, (town,))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_town_info(town):
    """Get basic info about a town including which districts it's in."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get county from races
    cursor.execute("""
        SELECT DISTINCT r.county
        FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE res.municipality = ?
        AND r.county IS NOT NULL
        LIMIT 1
    """, (town,))
    row = cursor.fetchone()
    county = row[0] if row else None

    # Get current districts
    cursor.execute("""
        SELECT DISTINCT o.name as office, r.district
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE res.municipality = ?
        AND e.year = 2024
        AND e.election_type = 'general'
        AND r.district IS NOT NULL
        ORDER BY o.name
    """, (town,))
    districts = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return {
        'name': town,
        'county': county,
        'districts': districts
    }


def get_district_results(county, district, office='State Representative'):
    """Get results for a specific district across years."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH race_totals AS (
            SELECT
                e.year,
                r.id as race_id,
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
            WHERE r.county = ?
            AND r.district = ?
            AND o.name = ?
            AND e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id, c.id
        )
        SELECT
            year, race_id, seats, candidate, party, total_votes,
            (rank <= seats) as is_winner, rank
        FROM race_totals
        ORDER BY year DESC, total_votes DESC
    """, (county, str(district), office))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_district_towns(county, district, office='State Representative'):
    """Get towns in a district with their vote breakdown."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT res.municipality
        FROM results res
        JOIN races r ON res.race_id = r.id
        JOIN offices o ON r.office_id = o.id
        WHERE r.county = ?
        AND r.district = ?
        AND o.name = ?
        ORDER BY res.municipality
    """, (county, str(district), office))

    towns = [row[0] for row in cursor.fetchall()]
    conn.close()
    return towns


def get_district_info(county, district, office='State Representative'):
    """Get info about a district."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT r.seats
        FROM races r
        JOIN offices o ON r.office_id = o.id
        WHERE r.county = ?
        AND r.district = ?
        AND o.name = ?
        LIMIT 1
    """, (county, str(district), office))

    row = cursor.fetchone()
    seats = row[0] if row else 1

    towns = get_district_towns(county, district, office)

    return {
        'county': county,
        'district': district,
        'office': office,
        'seats': seats,
        'towns': towns
    }


def search_candidates(query):
    """
    Search for candidates by name and return full race results.
    Returns races where the candidate participated, with all candidates in those races.
    Only returns general election results.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # First find matching candidates and their races (general elections only)
    cursor.execute("""
        SELECT DISTINCT r.id as race_id
        FROM candidates c
        JOIN results res ON c.id = res.candidate_id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE (c.name LIKE ? OR c.name_normalized LIKE ?)
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND e.election_type = 'general'
    """, (f'%{query}%', f'%{query.upper()}%'))

    race_ids = [row[0] for row in cursor.fetchall()]

    if not race_ids:
        conn.close()
        return []

    # Get full race details for these races
    placeholders = ','.join('?' * len(race_ids))
    cursor.execute(f"""
        SELECT
            e.year,
            o.name as office,
            r.id as race_id,
            r.district,
            r.county,
            r.seats,
            c.id as candidate_id,
            c.name as candidate_name,
            c.party,
            SUM(res.votes) as total_votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE r.id IN ({placeholders})
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
        AND e.election_type = 'general'
        GROUP BY r.id, c.id
        ORDER BY e.year DESC, o.name, r.district, total_votes DESC
    """, race_ids)

    # Group by race
    races = {}
    for row in cursor.fetchall():
        race_id = row['race_id']
        if race_id not in races:
            races[race_id] = {
                'year': row['year'],
                'office': row['office'],
                'district': row['district'],
                'county': row['county'],
                'seats': row['seats'],
                'candidates': []
            }
        races[race_id]['candidates'].append({
            'id': row['candidate_id'],
            'name': row['candidate_name'],
            'party': row['party'],
            'votes': row['total_votes'],
            'is_match': query.lower() in row['candidate_name'].lower()
        })

    conn.close()

    # Mark winners and sort
    race_list = []
    for race in races.values():
        # Sort candidates by votes and mark winners
        race['candidates'].sort(key=lambda x: -x['votes'])
        for i, cand in enumerate(race['candidates']):
            cand['is_winner'] = i < race['seats']
        race_list.append(race)

    # Sort by year desc
    race_list.sort(key=lambda x: (-x['year'], x['office']))

    return race_list


def get_candidate_history(candidate_id):
    """Get full election history for a candidate."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        WITH race_totals AS (
            SELECT
                e.year,
                o.name as office,
                r.district,
                r.county,
                r.seats,
                c.id as cand_id,
                c.name as candidate,
                c.party,
                SUM(res.votes) as total_votes,
                RANK() OVER (PARTITION BY r.id ORDER BY SUM(res.votes) DESC) as rank
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN offices o ON r.office_id = o.id
            WHERE e.election_type = 'general'
            AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
            GROUP BY r.id, c.id
        )
        SELECT
            year, office, district, county, seats,
            candidate, party, total_votes,
            (rank <= seats) as is_winner
        FROM race_totals
        WHERE cand_id = ?
        ORDER BY year DESC
    """, (candidate_id,))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_db_stats():
    """Get database statistics."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    cursor.execute("SELECT COUNT(DISTINCT year) FROM elections WHERE election_type = 'general'")
    stats['years'] = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(year), MAX(year) FROM elections WHERE election_type = 'general'")
    row = cursor.fetchone()
    stats['year_range'] = f"{row[0]}-{row[1]}"

    cursor.execute("SELECT COUNT(DISTINCT municipality) FROM results WHERE municipality NOT GLOB '[0-9]*'")
    stats['municipalities'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM candidates WHERE name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')")
    stats['candidates'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM races")
    stats['races'] = cursor.fetchone()[0]

    conn.close()
    return stats
