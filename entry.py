"""Results entry routes for data entry users."""

import sqlite3
import json
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from auth import get_db
from datetime import datetime

entry_bp = Blueprint('entry', __name__, url_prefix='/entry')

DATABASE = 'nh_elections.db'


def log_audit(user_id, race_id, municipality, candidate_id, action, old_values, new_values):
    """Log an audit entry."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO result_audit (user_id, race_id, municipality, candidate_id, action, old_values, new_values)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, race_id, municipality, candidate_id, action,
          json.dumps(old_values) if old_values else None,
          json.dumps(new_values) if new_values else None))
    conn.commit()
    conn.close()


@entry_bp.route('/')
@login_required
def index():
    """Select a race to enter results for."""
    conn = get_db()
    cursor = conn.cursor()

    # Get elections with active races (special elections or current year)
    cursor.execute("""
        SELECT e.id, e.year, e.election_type, e.party,
               COUNT(r.id) as race_count
        FROM elections e
        JOIN races r ON e.id = r.election_id
        WHERE e.election_type LIKE '%special%'
           OR e.year >= strftime('%Y', 'now')
        GROUP BY e.id
        ORDER BY e.year DESC, e.election_type
    """)
    elections = cursor.fetchall()

    conn.close()

    return render_template('entry/index.html', elections=elections)


@entry_bp.route('/election/<int:election_id>')
@login_required
def election_races(election_id):
    """List races in an election for entry."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM elections WHERE id = ?", (election_id,))
    election = cursor.fetchone()

    if not election:
        flash('Election not found.', 'error')
        return redirect(url_for('entry.index'))

    cursor.execute("""
        SELECT r.*, o.name as office_name,
               (SELECT COUNT(*) FROM results WHERE race_id = r.id AND votes > 0) as entered_count
        FROM races r
        JOIN offices o ON r.office_id = o.id
        WHERE r.election_id = ?
        ORDER BY o.name, r.county, r.district
    """, (election_id,))
    races = cursor.fetchall()

    conn.close()

    return render_template('entry/election.html', election=election, races=races)


@entry_bp.route('/race/<int:race_id>')
@login_required
def race_entry(race_id):
    """Enter results for a specific race."""
    conn = get_db()
    cursor = conn.cursor()

    # Get race info
    cursor.execute("""
        SELECT r.*, e.year, e.election_type, e.party as election_party, o.name as office_name
        FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE r.id = ?
    """, (race_id,))
    race = cursor.fetchone()

    if not race:
        flash('Race not found.', 'error')
        return redirect(url_for('entry.index'))

    # Get candidates
    cursor.execute("""
        SELECT DISTINCT c.id, c.name, c.party, c.display_order
        FROM candidates c
        JOIN results res ON c.id = res.candidate_id
        WHERE res.race_id = ?
        ORDER BY c.party DESC, c.display_order, c.name
    """, (race_id,))
    candidates = cursor.fetchall()

    # Get towns
    towns = []
    if race['county'] and race['district']:
        cursor.execute("""
            SELECT municipality FROM district_compositions
            WHERE office = 'State Representative'
            AND county = ? AND district = ?
            AND redistricting_cycle = '2022-2030'
            ORDER BY municipality
        """, (race['county'], race['district']))
        towns = [r['municipality'] for r in cursor.fetchall()]

    # Get current results
    cursor.execute("""
        SELECT res.municipality, res.candidate_id, res.votes
        FROM results res
        WHERE res.race_id = ?
    """, (race_id,))
    results_raw = cursor.fetchall()

    # Organize results by town
    results = {}
    for r in results_raw:
        town = r['municipality']
        if town not in results:
            results[town] = {}
        results[town][r['candidate_id']] = r['votes']

    conn.close()

    return render_template('entry/race.html',
                         race=race,
                         candidates=candidates,
                         towns=towns,
                         results=results)


@entry_bp.route('/race/<int:race_id>/save', methods=['POST'])
@login_required
def save_results(race_id):
    """Save results for a race."""
    conn = get_db()
    cursor = conn.cursor()

    # Get race to verify it exists
    cursor.execute("SELECT id FROM races WHERE id = ?", (race_id,))
    if not cursor.fetchone():
        conn.close()
        return jsonify({'error': 'Race not found'}), 404

    data = request.get_json()
    if not data or 'results' not in data:
        conn.close()
        return jsonify({'error': 'No results data'}), 400

    updated = 0
    for entry in data['results']:
        town = entry.get('town')
        candidate_id = entry.get('candidate_id')
        votes = entry.get('votes', 0)

        if not town or not candidate_id:
            continue

        # Get old value for audit
        cursor.execute("""
            SELECT votes FROM results
            WHERE race_id = ? AND candidate_id = ? AND municipality = ?
        """, (race_id, candidate_id, town))
        old_row = cursor.fetchone()
        old_votes = old_row['votes'] if old_row else None

        if old_row:
            # Update existing
            if old_votes != votes:
                cursor.execute("""
                    UPDATE results SET votes = ?
                    WHERE race_id = ? AND candidate_id = ? AND municipality = ?
                """, (votes, race_id, candidate_id, town))
                log_audit(current_user.id, race_id, town, candidate_id, 'update',
                         {'votes': old_votes}, {'votes': votes})
                updated += 1
        else:
            # Insert new
            cursor.execute("""
                INSERT INTO results (race_id, candidate_id, municipality, votes)
                VALUES (?, ?, ?, ?)
            """, (race_id, candidate_id, town, votes))
            log_audit(current_user.id, race_id, town, candidate_id, 'create',
                     None, {'votes': votes})
            updated += 1

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'updated': updated})


@entry_bp.route('/race/<int:race_id>/ballots', methods=['POST'])
@login_required
def save_ballots(race_id):
    """Save ballots cast for towns in a race."""
    conn = get_db()
    cursor = conn.cursor()

    # Get race info
    cursor.execute("""
        SELECT r.id, e.id as election_id
        FROM races r
        JOIN elections e ON r.election_id = e.id
        WHERE r.id = ?
    """, (race_id,))
    race = cursor.fetchone()

    if not race:
        conn.close()
        return jsonify({'error': 'Race not found'}), 404

    data = request.get_json()
    if not data or 'ballots' not in data:
        conn.close()
        return jsonify({'error': 'No ballots data'}), 400

    updated = 0
    for entry in data['ballots']:
        town = entry.get('town')
        ballots_cast = entry.get('ballots_cast', 0)

        if not town:
            continue

        # Check if voter_registration entry exists
        cursor.execute("""
            SELECT id, ballots_cast FROM voter_registration
            WHERE election_id = ? AND municipality = ?
        """, (race['election_id'], town))
        existing = cursor.fetchone()

        if existing:
            if existing['ballots_cast'] != ballots_cast:
                cursor.execute("""
                    UPDATE voter_registration SET ballots_cast = ?
                    WHERE id = ?
                """, (ballots_cast, existing['id']))
                updated += 1
        else:
            cursor.execute("""
                INSERT INTO voter_registration (election_id, municipality, ballots_cast)
                VALUES (?, ?, ?)
            """, (race['election_id'], town, ballots_cast))
            updated += 1

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'updated': updated})
