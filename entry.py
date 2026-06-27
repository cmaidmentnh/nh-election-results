"""Results entry routes.

Two entry modes share one data model (race_candidates rosters + results):

  * Polling-place-centric (primary workflow): pick an election event, pick a
    polling place, and enter every race that place votes on at once - matching
    how results actually arrive on election night (one tape per polling place).

  * Race-centric (recounts / cross-checks): the classic per-race grid of towns.
"""

import json
import sqlite3
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, abort
from flask_login import login_required, current_user
from auth import get_db

entry_bp = Blueprint('entry', __name__, url_prefix='/entry')

DATABASE = 'nh_elections.db'

# Display order for offices on a polling-place ballot (top of ticket first).
OFFICE_ORDER = [
    "President of the United States",
    "Governor",
    "United States Senator",
    "Representative in Congress",
    "Executive Councilor",
    "State Senator",
    "State Representative",
    "Delegate to the State Convention",
    "County Commissioner",
    "County Sheriff",
    "County Attorney",
    "County Treasurer",
    "Register of Deeds",
    "Register of Probate",
]
OFFICE_RANK = {name: i for i, name in enumerate(OFFICE_ORDER)}


def log_audit(cursor, user_id, race_id, municipality, candidate_id, action, old_values, new_values):
    cursor.execute("""
        INSERT INTO result_audit (user_id, race_id, municipality, candidate_id, action, old_values, new_values)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, race_id, municipality, candidate_id, action,
          json.dumps(old_values) if old_values else None,
          json.dumps(new_values) if new_values else None))


def event_elections(cursor, year, election_type):
    """All party elections that make up one entry event (primary => R + D)."""
    cursor.execute(
        "SELECT * FROM elections WHERE year = ? AND election_type = ? ORDER BY party",
        (year, election_type))
    return cursor.fetchall()


def race_label(office, county, district):
    """Human label for a race within a polling-place ballot."""
    if office == "Representative in Congress":
        return f"U.S. House District {district}"
    if office == "Executive Councilor":
        return f"Executive Council District {district}"
    if office == "State Senator":
        return f"State Senate District {district}"
    if office in ("State Representative", "Delegate to the State Convention"):
        return f"{county} District {district}"
    if office == "County Commissioner":
        return f"{county} Commissioner District {district}"
    if county:  # county-wide offices
        return f"{county}"
    return ""  # statewide (Governor, US Senate)


def place_races(cursor, municipality, election_ids):
    """Every race `municipality` votes on across the given elections."""
    qmarks = ",".join("?" * len(election_ids))
    cursor.execute(f"""
        SELECT r.id, r.election_id, e.party AS ballot_party, o.name AS office,
               COALESCE(r.county,'') AS county, COALESCE(r.district,'') AS district, r.seats
        FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o   ON r.office_id = o.id
        JOIN municipality_districts md
              ON md.office_id = r.office_id
             AND md.county   = COALESCE(r.county,'')
             AND md.district = COALESCE(r.district,'')
        WHERE r.election_id IN ({qmarks}) AND md.municipality = ?
        UNION
        SELECT r.id, r.election_id, e.party, o.name,
               COALESCE(r.county,''), COALESCE(r.district,''), r.seats
        FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o   ON r.office_id = o.id
        WHERE r.election_id IN ({qmarks})
          AND COALESCE(r.county,'') = '' AND COALESCE(r.district,'') = ''
    """, (*election_ids, municipality, *election_ids))
    races = [dict(r) for r in cursor.fetchall()]

    for race in races:
        cursor.execute("""
            SELECT rc.candidate_id, c.name, rc.party, rc.ballot_order, rc.is_incumbent,
                   (SELECT votes FROM results
                      WHERE race_id = rc.race_id AND candidate_id = rc.candidate_id
                        AND municipality = ?) AS votes
            FROM race_candidates rc
            JOIN candidates c ON rc.candidate_id = c.id
            WHERE rc.race_id = ?
            ORDER BY rc.ballot_order, c.name
        """, (municipality, race['id']))
        race['candidates'] = [dict(c) for c in cursor.fetchall()]
        race['label'] = race_label(race['office'], race['county'], race['district'])

    races.sort(key=lambda r: (r['ballot_party'] or '', OFFICE_RANK.get(r['office'], 99),
                              r['county'], _district_sort(r['district'])))
    return races


def _district_sort(d):
    try:
        return (0, int(d))
    except (ValueError, TypeError):
        return (1, d or '')


# ---------------------------------------------------------------------------
# Polling-place-centric entry
# ---------------------------------------------------------------------------

@entry_bp.route('/')
@login_required
def index():
    """Pick an entry event (year + type), with race counts."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT e.year, e.election_type,
               GROUP_CONCAT(DISTINCT e.party) AS parties,
               COUNT(r.id) AS race_count
        FROM elections e
        JOIN races r ON e.id = r.election_id
        WHERE e.year >= 2026 OR e.election_type LIKE '%special%'
        GROUP BY e.year, e.election_type
        ORDER BY e.year DESC, e.election_type
    """)
    events = cursor.fetchall()
    conn.close()
    return render_template('entry/index.html', events=events)


@entry_bp.route('/event/<int:year>/<election_type>/places')
@login_required
def places(year, election_type):
    """List polling places for an event with entry progress."""
    conn = get_db()
    cursor = conn.cursor()

    elections = event_elections(cursor, year, election_type)
    if not elections:
        flash('No election found for that event.', 'error')
        return redirect(url_for('entry.index'))
    election_ids = [e['id'] for e in elections]
    qmarks = ",".join("?" * len(election_ids))

    # Statewide race count (applies to every place).
    cursor.execute(f"""
        SELECT COUNT(*) AS n FROM races r
        WHERE r.election_id IN ({qmarks})
          AND COALESCE(r.county,'') = '' AND COALESCE(r.district,'') = ''
    """, election_ids)
    statewide = cursor.fetchone()['n']

    # District race count per municipality.
    cursor.execute(f"""
        SELECT md.municipality, COUNT(DISTINCT r.id) AS n
        FROM municipality_districts md
        JOIN races r ON r.office_id = md.office_id
             AND COALESCE(r.county,'') = md.county
             AND COALESCE(r.district,'') = md.district
        WHERE r.election_id IN ({qmarks})
        GROUP BY md.municipality
    """, election_ids)
    total_by_muni = {row['municipality']: row['n'] + statewide for row in cursor.fetchall()}

    # Entered (races with at least one saved result) per municipality.
    cursor.execute(f"""
        SELECT res.municipality, COUNT(DISTINCT res.race_id) AS n
        FROM results res
        JOIN races r ON res.race_id = r.id
        WHERE r.election_id IN ({qmarks})
        GROUP BY res.municipality
    """, election_ids)
    entered_by_muni = {row['municipality']: row['n'] for row in cursor.fetchall()}

    cursor.execute("SELECT municipality, county, polling_hours FROM polling_places ORDER BY county, municipality")
    rows = []
    for pp in cursor.fetchall():
        muni = pp['municipality']
        total = total_by_muni.get(muni, statewide)
        rows.append({
            'municipality': muni,
            'county': pp['county'] or '',
            'polling_hours': pp['polling_hours'],
            'total': total,
            'entered': min(entered_by_muni.get(muni, 0), total),
        })
    conn.close()

    return render_template('entry/places.html', year=year, election_type=election_type,
                           elections=elections, places=rows)


@entry_bp.route('/event/<int:year>/<election_type>/place/<path:municipality>')
@login_required
def place(year, election_type, municipality):
    """Enter every race a polling place votes on."""
    conn = get_db()
    cursor = conn.cursor()

    elections = event_elections(cursor, year, election_type)
    if not elections:
        abort(404)
    election_ids = [e['id'] for e in elections]

    cursor.execute("SELECT * FROM polling_places WHERE municipality = ?", (municipality,))
    place_info = cursor.fetchone()

    races = place_races(cursor, municipality, election_ids)

    # Ballots cast per party election.
    ballots = {}
    for e in elections:
        cursor.execute("SELECT ballots_cast FROM voter_registration WHERE election_id = ? AND municipality = ?",
                       (e['id'], municipality))
        row = cursor.fetchone()
        ballots[e['id']] = row['ballots_cast'] if row else None

    # Group races by ballot party for display.
    groups = {}
    for race in races:
        groups.setdefault(race['ballot_party'] or 'General', []).append(race)

    conn.close()
    return render_template('entry/place.html', year=year, election_type=election_type,
                           municipality=municipality, place_info=place_info,
                           elections=elections, groups=groups, ballots=ballots)


@entry_bp.route('/event/<int:year>/<election_type>/place/<path:municipality>/save', methods=['POST'])
@login_required
def place_save(year, election_type, municipality):
    """Save all results (and ballots cast) for one polling place."""
    conn = get_db()
    cursor = conn.cursor()

    elections = event_elections(cursor, year, election_type)
    if not elections:
        conn.close()
        return jsonify({'error': 'Event not found'}), 404
    valid_election_ids = {e['id'] for e in elections}

    data = request.get_json() or {}
    updated = 0

    # Guard: only accept races that belong to this event.
    for entry in data.get('results', []):
        race_id = entry.get('race_id')
        candidate_id = entry.get('candidate_id')
        votes = entry.get('votes', 0) or 0
        if not race_id or not candidate_id:
            continue
        cursor.execute("SELECT election_id FROM races WHERE id = ?", (race_id,))
        rr = cursor.fetchone()
        if not rr or rr['election_id'] not in valid_election_ids:
            continue

        cursor.execute("""SELECT votes FROM results
                          WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                       (race_id, candidate_id, municipality))
        old = cursor.fetchone()
        if old:
            if old['votes'] != votes:
                cursor.execute("""UPDATE results SET votes = ?
                                  WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                               (votes, race_id, candidate_id, municipality))
                log_audit(cursor, current_user.id, race_id, municipality, candidate_id,
                          'update', {'votes': old['votes']}, {'votes': votes})
                updated += 1
        else:
            cursor.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                              VALUES (?, ?, ?, ?)""", (race_id, candidate_id, municipality, votes))
            log_audit(cursor, current_user.id, race_id, municipality, candidate_id,
                      'create', None, {'votes': votes})
            updated += 1

    for entry in data.get('ballots', []):
        election_id = entry.get('election_id')
        ballots_cast = entry.get('ballots_cast')
        if election_id not in valid_election_ids or ballots_cast is None:
            continue
        cursor.execute("""SELECT id, ballots_cast FROM voter_registration
                          WHERE election_id = ? AND municipality = ?""", (election_id, municipality))
        existing = cursor.fetchone()
        if existing:
            if existing['ballots_cast'] != ballots_cast:
                cursor.execute("UPDATE voter_registration SET ballots_cast = ? WHERE id = ?",
                               (ballots_cast, existing['id']))
                updated += 1
        else:
            # county is NOT NULL in voter_registration; fill from polling_places.
            cursor.execute("SELECT county FROM polling_places WHERE municipality = ?", (municipality,))
            cr = cursor.fetchone()
            cursor.execute("""INSERT INTO voter_registration (election_id, county, municipality, ballots_cast)
                              VALUES (?, ?, ?, ?)""",
                           (election_id, (cr['county'] if cr else '') or '', municipality, ballots_cast))
            updated += 1

    conn.commit()
    conn.close()
    return jsonify({'success': True, 'updated': updated})


# ---------------------------------------------------------------------------
# Race-centric entry (recounts / cross-checks)
# ---------------------------------------------------------------------------

@entry_bp.route('/race/<int:race_id>')
@login_required
def race_entry(race_id):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT r.*, e.year, e.election_type, e.party AS election_party, o.name AS office_name
        FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o   ON r.office_id = o.id
        WHERE r.id = ?
    """, (race_id,))
    race = cursor.fetchone()
    if not race:
        flash('Race not found.', 'error')
        return redirect(url_for('entry.index'))

    # Roster from race_candidates (falls back to any candidates already in results).
    cursor.execute("""
        SELECT c.id, c.name, rc.party, rc.ballot_order
        FROM race_candidates rc JOIN candidates c ON rc.candidate_id = c.id
        WHERE rc.race_id = ?
        ORDER BY rc.party DESC, rc.ballot_order, c.name
    """, (race_id,))
    candidates = cursor.fetchall()

    # Towns that vote in this race, from the municipality_districts map.
    cursor.execute("""
        SELECT DISTINCT md.municipality
        FROM municipality_districts md
        WHERE md.office_id = ? AND md.county = ? AND md.district = ?
        ORDER BY md.municipality
    """, (race['office_id'], race['county'] or '', race['district'] or ''))
    towns = [r['municipality'] for r in cursor.fetchall()]
    if not towns:  # statewide
        cursor.execute("SELECT municipality FROM polling_places ORDER BY county, municipality")
        towns = [r['municipality'] for r in cursor.fetchall()]

    cursor.execute("SELECT municipality, candidate_id, votes FROM results WHERE race_id = ?", (race_id,))
    results = {}
    for r in cursor.fetchall():
        results.setdefault(r['municipality'], {})[r['candidate_id']] = r['votes']

    conn.close()
    return render_template('entry/race.html', race=race, candidates=candidates,
                           towns=towns, results=results)


@entry_bp.route('/race/<int:race_id>/save', methods=['POST'])
@login_required
def save_results(race_id):
    conn = get_db()
    cursor = conn.cursor()
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
        votes = entry.get('votes', 0) or 0
        if not town or not candidate_id:
            continue
        cursor.execute("""SELECT votes FROM results
                          WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                       (race_id, candidate_id, town))
        old_row = cursor.fetchone()
        if old_row:
            if old_row['votes'] != votes:
                cursor.execute("""UPDATE results SET votes = ?
                                  WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                               (votes, race_id, candidate_id, town))
                log_audit(cursor, current_user.id, race_id, town, candidate_id, 'update',
                          {'votes': old_row['votes']}, {'votes': votes})
                updated += 1
        else:
            cursor.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                              VALUES (?, ?, ?, ?)""", (race_id, candidate_id, town, votes))
            log_audit(cursor, current_user.id, race_id, town, candidate_id, 'create',
                      None, {'votes': votes})
            updated += 1

    conn.commit()
    conn.close()
    return jsonify({'success': True, 'updated': updated})
