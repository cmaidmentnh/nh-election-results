"""Admin routes for election management."""

import sqlite3
import json
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from auth import admin_required, create_user, get_all_users, delete_user, change_password, get_db
from datetime import datetime

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

DATABASE = 'nh_elections.db'


@admin_bp.route('/')
@login_required
def dashboard():
    """Admin dashboard."""
    conn = get_db()
    cursor = conn.cursor()

    # Get recent elections
    cursor.execute("""
        SELECT id, year, election_type, party, redistricting_cycle
        FROM elections
        ORDER BY year DESC, id DESC
        LIMIT 10
    """)
    recent_elections = cursor.fetchall()

    # Get count of races needing results
    cursor.execute("""
        SELECT COUNT(DISTINCT r.id) as count
        FROM races r
        JOIN elections e ON r.election_id = e.id
        WHERE e.election_type LIKE '%special%'
    """)
    special_races = cursor.fetchone()['count']

    conn.close()

    return render_template('admin/dashboard.html',
                         recent_elections=recent_elections,
                         special_races=special_races)


# ============ USER MANAGEMENT ============

@admin_bp.route('/users')
@admin_required
def users():
    """List all users."""
    all_users = get_all_users()
    return render_template('admin/users.html', users=all_users)


@admin_bp.route('/users/create', methods=['POST'])
@admin_required
def create_user_route():
    """Create a new user."""
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '')
    role = request.form.get('role', 'user')

    if not username or not password:
        flash('Username and password are required.', 'error')
        return redirect(url_for('admin.users'))

    if role not in ('user', 'admin'):
        role = 'user'

    user_id = create_user(username, password, role)
    if user_id:
        flash(f'User "{username}" created successfully.', 'success')
    else:
        flash(f'Username "{username}" already exists.', 'error')

    return redirect(url_for('admin.users'))


@admin_bp.route('/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user_route(user_id):
    """Delete a user."""
    if user_id == current_user.id:
        flash('Cannot delete your own account.', 'error')
    else:
        delete_user(user_id)
        flash('User deleted.', 'success')
    return redirect(url_for('admin.users'))


@admin_bp.route('/users/<int:user_id>/reset-password', methods=['POST'])
@admin_required
def reset_password_route(user_id):
    """Reset a user's password."""
    new_password = request.form.get('password', '')
    if not new_password:
        flash('Password is required.', 'error')
    else:
        change_password(user_id, new_password)
        flash('Password updated.', 'success')
    return redirect(url_for('admin.users'))


# ============ ELECTION MANAGEMENT ============

@admin_bp.route('/elections')
@login_required
def elections():
    """List and manage elections."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT e.*, COUNT(r.id) as race_count
        FROM elections e
        LEFT JOIN races r ON e.id = r.election_id
        GROUP BY e.id
        ORDER BY e.year DESC, e.id DESC
    """)
    all_elections = cursor.fetchall()
    conn.close()
    return render_template('admin/elections.html', elections=all_elections)


@admin_bp.route('/elections/create', methods=['POST'])
@admin_required
def create_election():
    """Create a new election."""
    year = request.form.get('year', type=int)
    election_type = request.form.get('election_type', '')
    party = request.form.get('party', '') or None
    redistricting_cycle = request.form.get('redistricting_cycle', '2022-2030')

    if not year or not election_type:
        flash('Year and election type are required.', 'error')
        return redirect(url_for('admin.elections'))

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO elections (year, election_type, party, redistricting_cycle)
        VALUES (?, ?, ?, ?)
    """, (year, election_type, party, redistricting_cycle))
    conn.commit()
    election_id = cursor.lastrowid
    conn.close()

    flash(f'Election created (ID: {election_id}).', 'success')
    return redirect(url_for('admin.election_detail', election_id=election_id))


@admin_bp.route('/elections/<int:election_id>')
@login_required
def election_detail(election_id):
    """View/edit an election and its races."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM elections WHERE id = ?", (election_id,))
    election = cursor.fetchone()
    if not election:
        flash('Election not found.', 'error')
        return redirect(url_for('admin.elections'))

    # Get races for this election
    cursor.execute("""
        SELECT r.*, o.name as office_name,
               GROUP_CONCAT(c.name || ' (' || c.party || ')') as candidates
        FROM races r
        JOIN offices o ON r.office_id = o.id
        LEFT JOIN candidates c ON c.id IN (
            SELECT DISTINCT candidate_id FROM results WHERE race_id = r.id
        )
        WHERE r.election_id = ?
        GROUP BY r.id
        ORDER BY o.name, r.county, r.district
    """, (election_id,))
    races = cursor.fetchall()

    # Get available offices
    cursor.execute("SELECT id, name FROM offices ORDER BY name")
    offices = cursor.fetchall()

    conn.close()

    return render_template('admin/election_detail.html',
                         election=election,
                         races=races,
                         offices=offices)


@admin_bp.route('/elections/<int:election_id>/delete', methods=['POST'])
@admin_required
def delete_election(election_id):
    """Delete an election and all its races/results."""
    conn = get_db()
    cursor = conn.cursor()

    # Delete results first
    cursor.execute("""
        DELETE FROM results WHERE race_id IN (
            SELECT id FROM races WHERE election_id = ?
        )
    """, (election_id,))

    # Delete races
    cursor.execute("DELETE FROM races WHERE election_id = ?", (election_id,))

    # Delete election
    cursor.execute("DELETE FROM elections WHERE id = ?", (election_id,))

    conn.commit()
    conn.close()

    flash('Election deleted.', 'success')
    return redirect(url_for('admin.elections'))


# ============ RACE MANAGEMENT ============

@admin_bp.route('/elections/<int:election_id>/races/create', methods=['POST'])
@admin_required
def create_race(election_id):
    """Create a new race in an election."""
    office_id = request.form.get('office_id', type=int)
    district = request.form.get('district', '').strip()
    county = request.form.get('county', '').strip() or None
    seats = request.form.get('seats', 1, type=int)

    if not office_id:
        flash('Office is required.', 'error')
        return redirect(url_for('admin.election_detail', election_id=election_id))

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO races (election_id, office_id, district, county, seats)
        VALUES (?, ?, ?, ?, ?)
    """, (election_id, office_id, district, county, seats))
    conn.commit()
    race_id = cursor.lastrowid
    conn.close()

    flash(f'Race created (ID: {race_id}).', 'success')
    return redirect(url_for('admin.race_detail', race_id=race_id))


@admin_bp.route('/races/<int:race_id>')
@login_required
def race_detail(race_id):
    """View/edit a race and its candidates."""
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
        return redirect(url_for('admin.elections'))

    # Get candidates in this race
    cursor.execute("""
        SELECT DISTINCT c.id, c.name, c.party
        FROM candidates c
        JOIN results res ON c.id = res.candidate_id
        WHERE res.race_id = ?
        ORDER BY c.party, c.name
    """, (race_id,))
    candidates = cursor.fetchall()

    # Get towns for this district
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

    # Get results entered so far
    cursor.execute("""
        SELECT res.municipality, c.name as candidate_name, c.party, res.votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        WHERE res.race_id = ?
        ORDER BY res.municipality, res.votes DESC
    """, (race_id,))
    results = cursor.fetchall()

    conn.close()

    return render_template('admin/race_detail.html',
                         race=race,
                         candidates=candidates,
                         towns=towns,
                         results=results)


@admin_bp.route('/races/<int:race_id>/candidates/add', methods=['POST'])
@admin_required
def add_candidate(race_id):
    """Add a candidate to a race."""
    name = request.form.get('name', '').strip()
    party = request.form.get('party', '').strip()

    if not name:
        flash('Candidate name is required.', 'error')
        return redirect(url_for('admin.race_detail', race_id=race_id))

    conn = get_db()
    cursor = conn.cursor()

    # Check if candidate exists
    cursor.execute("SELECT id FROM candidates WHERE name = ? AND party = ?", (name, party))
    existing = cursor.fetchone()

    if existing:
        candidate_id = existing['id']
    else:
        # Create new candidate
        cursor.execute("""
            INSERT INTO candidates (name, name_normalized, party)
            VALUES (?, ?, ?)
        """, (name, name.upper(), party))
        candidate_id = cursor.lastrowid

    # Get towns for this race's district
    cursor.execute("""
        SELECT r.county, r.district FROM races r WHERE r.id = ?
    """, (race_id,))
    race_info = cursor.fetchone()

    if race_info['county'] and race_info['district']:
        cursor.execute("""
            SELECT municipality FROM district_compositions
            WHERE office = 'State Representative'
            AND county = ? AND district = ?
            AND redistricting_cycle = '2022-2030'
        """, (race_info['county'], race_info['district']))
        towns = [r['municipality'] for r in cursor.fetchall()]

        # Create placeholder results for each town (0 votes)
        for town in towns:
            try:
                cursor.execute("""
                    INSERT INTO results (race_id, candidate_id, municipality, votes)
                    VALUES (?, ?, ?, 0)
                """, (race_id, candidate_id, town))
            except sqlite3.IntegrityError:
                pass  # Already exists

    conn.commit()
    conn.close()

    flash(f'Candidate "{name}" added.', 'success')
    return redirect(url_for('admin.race_detail', race_id=race_id))


@admin_bp.route('/races/<int:race_id>/delete', methods=['POST'])
@admin_required
def delete_race(race_id):
    """Delete a race."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT election_id FROM races WHERE id = ?", (race_id,))
    race = cursor.fetchone()
    election_id = race['election_id'] if race else None

    cursor.execute("DELETE FROM results WHERE race_id = ?", (race_id,))
    cursor.execute("DELETE FROM races WHERE id = ?", (race_id,))
    conn.commit()
    conn.close()

    flash('Race deleted.', 'success')
    if election_id:
        return redirect(url_for('admin.election_detail', election_id=election_id))
    return redirect(url_for('admin.elections'))


# ============ RESULTS VIEW ============

@admin_bp.route('/results')
@login_required
def results_overview():
    """View all entered results."""
    conn = get_db()
    cursor = conn.cursor()

    # Get recent audit entries
    cursor.execute("""
        SELECT a.*, u.username
        FROM result_audit a
        JOIN users u ON a.user_id = u.id
        ORDER BY a.created_at DESC
        LIMIT 50
    """)
    audit_log = cursor.fetchall()

    conn.close()

    return render_template('admin/results.html', audit_log=audit_log)
