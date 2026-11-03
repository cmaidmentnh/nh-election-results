"""Results entry routes.

Two entry modes share one data model (race_candidates rosters + results):

  * Polling-place-centric (primary workflow): pick an election event, pick a
    polling place, and enter every race that place votes on at once - matching
    how results actually arrive on election night (one tape per polling place).

  * Race-centric (recounts / cross-checks): the classic per-race grid of towns.
"""

import json
import re
import sqlite3
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, abort
from flask_login import login_required, current_user
from auth import get_db

entry_bp = Blueprint('entry', __name__, url_prefix='/entry')

DATABASE = 'nh_elections.db'
WRITE_IN_NAME = "Write-in"


def normalize_name(name):
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", "", (name or "").upper())).strip()


def ensure_writein_candidate(cursor):
    """Shared pseudo-candidate for the aggregate write-in line on every race."""
    cursor.execute("SELECT id FROM candidates WHERE name_normalized = 'WRITEIN' AND party IS NULL")
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO candidates (name, name_normalized, party) VALUES (?, 'WRITEIN', NULL)",
                   (WRITE_IN_NAME,))
    return cursor.lastrowid

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


def whole_count(value):
    """A vote or ballot count as a non-negative int, or None if it is not one."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if value.is_integer() and value >= 0 else None
    if isinstance(value, str) and value.strip().isascii() and value.strip().isdigit():
        return int(value.strip())
    return None


def _blank(value):
    return value is None or (isinstance(value, str) and value.strip() == '')


class _Reject(Exception):
    """Abort a save: roll back everything and answer with this status."""

    def __init__(self, status, payload):
        super().__init__(payload.get('error'))
        self.status = status
        self.payload = payload


STALE_PAGE = {'error': 'This page is out of date. Reload it before saving.', 'stale': True}


def _entries(data, key):
    """The list of cells under `key`, refusing anything that is not one."""
    rows = data.get(key) or []
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise _Reject(400, {'error': f'Bad request: "{key}" must be a list of entries.'})
    return rows


def _cell(entry, key='votes'):
    """(votes, orig) from one posted cell; votes None means 'blank - skip it'.

    A blank box is not a zero: the race grid used to send 0 for every empty
    town, which marked them all reported. `orig` is the value the page showed
    when it loaded (None = blank); a page that does not send it predates the
    lost-update check and is refused rather than allowed to overwrite.
    """
    raw = entry.get(key)
    if _blank(raw):
        return None, None
    votes = whole_count(raw)
    if votes is None:
        raise _Reject(400, {'error': f'"{raw}" is not a whole number of 0 or more.'})
    if 'orig' not in entry:
        raise _Reject(409, STALE_PAGE)
    orig = entry.get('orig')
    if _blank(orig):
        orig = None
    else:
        orig = whole_count(orig)
        if orig is None:
            raise _Reject(400, {'error': 'Bad original value.'})
    return votes, orig


def writein_on_roster(cursor, race_id, norm):
    """The race's own candidate with this normalised name, if it has one.

    A write-in of someone already in the race is that candidate. The lookup by
    party alone missed them in the general, where the election has no party, and
    created a second "Chris Pappas" that split his votes.
    """
    cursor.execute("""SELECT c.id FROM race_candidates rc
                        JOIN candidates c ON c.id = rc.candidate_id
                       WHERE rc.race_id = ? AND c.name_normalized = ?""", (race_id, norm))
    return cursor.fetchone()


def _vote_ceiling(cursor, race_id, municipality):
    """The most votes one candidate could get in one place, or None if unknown.

    The larger of ballots cast for this election (when on file) and twice the
    biggest single-candidate count the place gave in the 2024 general (floor
    100). Intake already refuses such counts; hand entry published them - a
    typo with an extra zero went straight to the live page.
    """
    cursor.execute("""SELECT vr.ballots_cast FROM races r
                      JOIN voter_registration vr ON vr.election_id = r.election_id
                     WHERE r.id = ? AND vr.municipality = ?""", (race_id, municipality))
    row = cursor.fetchone()
    cast = row['ballots_cast'] if row and row['ballots_cast'] else None
    cursor.execute("""SELECT MAX(res.votes) AS v FROM results res
                      JOIN races ra ON ra.id = res.race_id
                      JOIN elections e ON e.id = ra.election_id
                     WHERE e.year = 2024 AND e.election_type = 'general'
                       AND res.municipality = ?""", (municipality,))
    row = cursor.fetchone()
    past = max(100, 2 * row['v']) if row and row['v'] is not None else None
    # Whichever is larger: ballots cast may be one machine's tape so far.
    known = [n for n in (cast, past) if n]
    return max(known) if known else None


def _write_result(cursor, user_id, race_id, municipality, candidate_id, votes, orig, conflicts):
    """Record one candidate's votes in one town, through result_audit.

    Only written if what is stored is still what the editor's page showed
    (`orig`); otherwise someone else has changed it since, and saving would
    silently revert their correction - so it is reported as a conflict.
    Returns 1 if a row changed.
    """
    cursor.execute("""SELECT votes FROM results
                      WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                   (race_id, candidate_id, municipality))
    old = cursor.fetchone()
    stored = old['votes'] if old else None
    if stored == votes:
        return 0
    ceiling = _vote_ceiling(cursor, race_id, municipality)
    if ceiling is not None and votes > ceiling:
        raise _Reject(400, {'error': f'{votes:,} votes in {municipality} is more than '
                                     f'that place can cast ({ceiling:,}). Check for an extra digit.'})
    if stored != orig:
        conflicts.append({'race_id': race_id, 'candidate_id': candidate_id,
                          'municipality': municipality, 'stored': stored, 'orig': orig})
        return 0
    if old:
        cursor.execute("""UPDATE results SET votes = ?
                          WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                       (votes, race_id, candidate_id, municipality))
        log_audit(cursor, user_id, race_id, municipality, candidate_id,
                  'update', {'votes': old['votes']}, {'votes': votes})
    else:
        cursor.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                          VALUES (?, ?, ?, ?)""", (race_id, candidate_id, municipality, votes))
        log_audit(cursor, user_id, race_id, municipality, candidate_id,
                  'create', None, {'votes': votes})
    return 1


def _clear_town(cursor, user_id, race_id, municipality):
    """Un-report one town in one race: delete its rows, each one audited."""
    cursor.execute("""SELECT candidate_id, votes FROM results
                      WHERE race_id = ? AND municipality = ?""", (race_id, municipality))
    rows = cursor.fetchall()
    for r in rows:
        cursor.execute("""DELETE FROM results
                          WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                       (race_id, r['candidate_id'], municipality))
        log_audit(cursor, user_id, race_id, municipality, r['candidate_id'],
                  'delete', {'votes': r['votes']}, None)
    return len(rows)


def _conflict_response(conflicts):
    return {'error': 'Someone else changed this since you opened the page. '
                     'Reload to see their numbers - nothing was saved.',
            'conflict': True, 'conflicts': conflicts}


def race_towns(cursor, race):
    """Towns that vote in `race`, from the municipality_districts map."""
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
    return towns


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
    writein_id = ensure_writein_candidate(cursor)

    for race in races:
        # recruitment_filing_id = -1 marks a named write-in roster row.
        cursor.execute("""
            SELECT rc.candidate_id, c.name, rc.party, rc.ballot_order,
                   (rc.recruitment_filing_id = -1) AS is_writein,
                   (SELECT votes FROM results
                      WHERE race_id = rc.race_id AND candidate_id = rc.candidate_id
                        AND municipality = ?) AS votes
            FROM race_candidates rc
            JOIN candidates c ON rc.candidate_id = c.id
            WHERE rc.race_id = ? AND rc.candidate_id != ?
            ORDER BY is_writein, rc.ballot_order, c.name
        """, (municipality, race['id'], writein_id))
        race['candidates'] = [dict(c) for c in cursor.fetchall()]
        race['label'] = race_label(race['office'], race['county'], race['district'])

        # Aggregate write-in line (always present).
        cursor.execute("""SELECT votes FROM results
                          WHERE race_id = ? AND candidate_id = ? AND municipality = ?""",
                       (race['id'], writein_id, municipality))
        wr = cursor.fetchone()
        race['writein_id'] = writein_id
        race['writein_votes'] = wr['votes'] if wr else None

        race['filed_count'] = sum(1 for c in race['candidates'] if not c['is_writein'])
        race['under_filled'] = race['filed_count'] < (race['seats'] or 1)

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

    conn.commit()  # persist the write-in pseudo-candidate if it was just created
    conn.close()
    return render_template('entry/place.html', year=year, election_type=election_type,
                           municipality=municipality, place_info=place_info,
                           elections=elections, groups=groups, ballots=ballots)


@entry_bp.route('/event/<int:year>/<election_type>/place/<path:municipality>/save', methods=['POST'])
@login_required
def place_save(year, election_type, municipality):
    """Save results (and ballots cast) for one polling place.

    The page sends only the boxes the editor changed, each with the value it
    showed when it loaded. If any of those no longer matches what is stored,
    another editor got there first: nothing is saved and the page is told to
    reload (409), instead of quietly putting the old number back.
    """
    conn = get_db()
    cursor = conn.cursor()

    elections = event_elections(cursor, year, election_type)
    if not elections:
        conn.close()
        return jsonify({'error': 'Event not found'}), 404
    valid_election_ids = {e['id'] for e in elections}

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        conn.close()
        return jsonify({'error': 'Bad request.'}), 400
    updated = 0
    conflicts = []

    try:
        cursor.execute("BEGIN IMMEDIATE")  # the orig check and the write are one step

        # Same checks as the race grid: a real polling place, a race on its
        # ballot, a candidate on that race. Without them a save could put
        # Salem's votes in a Hillsborough race, or file a whole page under a
        # place that does not exist.
        cursor.execute("SELECT 1 FROM polling_places WHERE municipality = ?", (municipality,))
        if not cursor.fetchone():
            raise _Reject(404, {'error': f'{municipality} is not a polling place.'})
        allowed = {r['id']: {c['candidate_id'] for c in r['candidates']} | {r['writein_id']}
                   for r in place_races(cursor, municipality, sorted(valid_election_ids))}

        # Guard: only accept races that belong to this event.
        for entry in _entries(data, 'results'):
            race_id = entry.get('race_id')
            candidate_id = entry.get('candidate_id')
            if not race_id or not candidate_id:
                continue
            votes, orig = _cell(entry)
            if votes is None:
                continue
            cursor.execute("SELECT election_id FROM races WHERE id = ?", (race_id,))
            rr = cursor.fetchone()
            if not rr or rr['election_id'] not in valid_election_ids:
                continue
            if race_id not in allowed:
                raise _Reject(400, {'error': f'That race is not on the {municipality} ballot.'})
            if candidate_id not in allowed[race_id]:
                raise _Reject(400, {'error': 'That candidate is not in this race.'})
            updated += _write_result(cursor, current_user.id, race_id, municipality,
                                     candidate_id, votes, orig, conflicts)

        # Named write-ins: create the candidate + roster row, then record the votes.
        for entry in _entries(data, 'writeins'):
            race_id = entry.get('race_id')
            name = str(entry.get('name') or '').strip()
            if not race_id or not name:
                continue
            if race_id not in allowed:
                raise _Reject(400, {'error': f'That race is not on the {municipality} ballot.'})
            votes, orig = _cell(entry)
            if votes is None:
                continue
            cursor.execute("""SELECT r.election_id, e.party FROM races r
                              JOIN elections e ON r.election_id = e.id WHERE r.id = ?""", (race_id,))
            rr = cursor.fetchone()
            if not rr or rr['election_id'] not in valid_election_ids:
                continue
            norm = normalize_name(name)
            party = rr['party']
            crow = writein_on_roster(cursor, race_id, norm)
            if not crow:
                cursor.execute("SELECT id FROM candidates WHERE name_normalized = ? AND party IS ?",
                               (norm, party))
                crow = cursor.fetchone()
            if crow:
                candidate_id = crow['id']
            else:
                cursor.execute("INSERT INTO candidates (name, name_normalized, party) VALUES (?,?,?)",
                               (name, norm, party))
                candidate_id = cursor.lastrowid
            # Roster row flagged as a write-in (recruitment_filing_id = -1).
            cursor.execute("""INSERT OR IGNORE INTO race_candidates
                              (race_id, candidate_id, party, ballot_order, is_incumbent,
                               recruitment_candidate_id, recruitment_filing_id)
                              VALUES (?, ?, ?, 900, 0, NULL, -1)""", (race_id, candidate_id, party))
            updated += _write_result(cursor, current_user.id, race_id, municipality,
                                     candidate_id, votes, orig, conflicts)

        for entry in _entries(data, 'ballots'):
            election_id = entry.get('election_id')
            if election_id not in valid_election_ids:
                continue
            ballots_cast, orig = _cell(entry, 'ballots_cast')
            if ballots_cast is None:
                continue
            cursor.execute("""SELECT id, ballots_cast FROM voter_registration
                              WHERE election_id = ? AND municipality = ?""", (election_id, municipality))
            existing = cursor.fetchone()
            stored = existing['ballots_cast'] if existing else None
            if stored == ballots_cast:
                continue
            if stored != orig:
                conflicts.append({'election_id': election_id, 'municipality': municipality,
                                  'stored': stored, 'orig': orig})
                continue
            if existing:
                cursor.execute("UPDATE voter_registration SET ballots_cast = ? WHERE id = ?",
                               (ballots_cast, existing['id']))
            else:
                # county is NOT NULL in voter_registration; fill from polling_places.
                cursor.execute("SELECT county FROM polling_places WHERE municipality = ?", (municipality,))
                cr = cursor.fetchone()
                cursor.execute("""INSERT INTO voter_registration (election_id, county, municipality, ballots_cast)
                                  VALUES (?, ?, ?, ?)""",
                               (election_id, (cr['county'] if cr else '') or '', municipality, ballots_cast))
            updated += 1

        if conflicts:
            raise _Reject(409, _conflict_response(conflicts))
        conn.commit()
    except _Reject as r:
        conn.rollback()
        return jsonify(r.payload), r.status
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return jsonify({'success': True, 'updated': updated})


@entry_bp.route('/event/<int:year>/<election_type>/place/<path:municipality>/clear', methods=['POST'])
@login_required
def place_clear(year, election_type, municipality):
    """Un-report one race in this town (a result entered against the wrong town)."""
    data = request.get_json(silent=True) or {}
    race_id = data.get('race_id')
    conn = get_db()
    cursor = conn.cursor()
    try:
        elections = event_elections(cursor, year, election_type)
        cursor.execute("SELECT election_id FROM races WHERE id = ?", (race_id,))
        rr = cursor.fetchone()
        if not rr or rr['election_id'] not in {e['id'] for e in elections}:
            return jsonify({'error': 'Race not found for this event'}), 404
        cursor.execute("BEGIN IMMEDIATE")
        cleared = _clear_town(cursor, current_user.id, race_id, municipality)
        conn.commit()
        return jsonify({'success': True, 'cleared': cleared})
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
    towns = race_towns(cursor, race)

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
    """Save the race grid.

    A blank box is skipped, never written as 0; the town must be one that
    votes in this race and the candidate one on its roster; and, as on the
    polling-place screen, a box someone else has changed since the page loaded
    is a 409, not an overwrite.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, office_id, county, district FROM races WHERE id = ?", (race_id,))
    race = cursor.fetchone()
    if not race:
        conn.close()
        return jsonify({'error': 'Race not found'}), 404

    data = request.get_json(silent=True)
    if not isinstance(data, dict) or 'results' not in data:
        conn.close()
        return jsonify({'error': 'No results data'}), 400

    towns = set(race_towns(cursor, race))
    cursor.execute("SELECT candidate_id FROM race_candidates WHERE race_id = ?", (race_id,))
    roster = {r['candidate_id'] for r in cursor.fetchall()}

    updated = 0
    conflicts = []
    try:
        cursor.execute("BEGIN IMMEDIATE")
        for entry in _entries(data, 'results'):
            town = entry.get('town')
            candidate_id = entry.get('candidate_id')
            if not town or not candidate_id:
                continue
            votes, orig = _cell(entry)
            if votes is None:
                continue
            if town not in towns:
                raise _Reject(400, {'error': f'"{town}" does not vote in this race.'})
            if candidate_id not in roster:
                raise _Reject(400, {'error': f'Candidate {candidate_id} is not on this race.'})
            updated += _write_result(cursor, current_user.id, race_id, town,
                                     candidate_id, votes, orig, conflicts)
        if conflicts:
            raise _Reject(409, _conflict_response(conflicts))
        conn.commit()
    except _Reject as r:
        conn.rollback()
        return jsonify(r.payload), r.status
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return jsonify({'success': True, 'updated': updated})


@entry_bp.route('/race/<int:race_id>/clear', methods=['POST'])
@login_required
def race_clear(race_id):
    """Un-report one town in this race."""
    data = request.get_json(silent=True) or {}
    town = data.get('town')
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, office_id, county, district FROM races WHERE id = ?", (race_id,))
        race = cursor.fetchone()
        if not race:
            return jsonify({'error': 'Race not found'}), 404
        if not town or town not in set(race_towns(cursor, race)):
            return jsonify({'error': 'That town does not vote in this race.'}), 400
        cursor.execute("BEGIN IMMEDIATE")
        cleared = _clear_town(cursor, current_user.id, race_id, town)
        conn.commit()
        return jsonify({'success': True, 'cleared': cleared})
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
