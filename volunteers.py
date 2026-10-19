"""Election-night volunteers.

People sign up to cover a polling place, we approve them, and each approved
volunteer gets a personal link. On election night they open it on their phone,
photograph the posted results (or type them), and send. A report becomes an
intake_messages row with source 'web' and is parsed by the intake service
exactly like an email or a Signal post - same ballot roster, same checks, same
review queue. See intake/runner.py web_loop.

Only approved volunteers have a working link, so nobody can feed numbers in
just by finding the sign-up page.
"""

import ipaddress
import json
import logging
import os
import re
import secrets
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import requests
from flask import (Blueprint, Response, abort, flash, jsonify, redirect,
                   render_template, request, url_for)
from flask_limiter import Limiter
from flask_login import current_user

from auth import admin_required

log = logging.getLogger(__name__)

vol_bp = Blueprint('vol', __name__)

DATABASE = 'nh_elections.db'
BASE_DIR = Path(__file__).resolve().parent
ATTACH_DIR = Path(os.environ.get("INTAKE_ATTACH_DIR", str(BASE_DIR / "data" / "intake_attachments")))
PUBLIC_BASE_URL = os.environ.get('PUBLIC_BASE_URL', 'https://elections.nhhouse.gop').rstrip('/')

TURNSTILE_SITE_KEY = os.environ.get('VOLUNTEER_TURNSTILE_SITE_KEY', '')
TURNSTILE_SECRET_KEY = os.environ.get('VOLUNTEER_TURNSTILE_SECRET_KEY', '')

MAIL_FROM = os.environ.get('VOLUNTEER_MAIL_FROM',
                           '"NH House Republicans Election Night" <results@electhouserepublicans.com>')
MAIL_REPLY_TO = os.environ.get('VOLUNTEER_REPLY_TO', 'chris@electhouserepublicans.com')
REVIEW_EMAIL = os.environ.get('VOLUNTEER_REVIEW_EMAIL', 'chris@electhouserepublicans.com')

EVENT_YEAR = int(os.environ.get('VOLUNTEER_EVENT_YEAR', '2026'))
EVENT_TYPE = os.environ.get('VOLUNTEER_EVENT_TYPE', 'general')
ELECTION_DAY = os.environ.get('VOLUNTEER_ELECTION_DAY', 'Tuesday, November 3')

MAX_PLACES_PER_VOLUNTEER = 2
MAX_VOLUNTEERS_PER_PLACE = 3
MAX_PHOTO_BYTES = 8 * 1024 * 1024
MAX_PHOTOS_PER_REPORT = 8

TIER_LABELS = {1: 'Top priority', 2: 'Priority', 3: ''}


# ── Rate limiting ────────────────────────────────────────────────────────────
# The site sits behind Cloudflare, so the address nginx sees is a Cloudflare
# edge. Keying a limit on it would lump every visitor in a region together.
# Trust CF-Connecting-IP only when the request really came from Cloudflare.
_CLOUDFLARE_NETS = [ipaddress.ip_network(n) for n in (
    '173.245.48.0/20', '103.21.244.0/22', '103.22.200.0/22', '103.31.4.0/22',
    '141.101.64.0/18', '108.162.192.0/18', '190.93.240.0/20', '188.114.96.0/20',
    '197.234.240.0/22', '198.41.128.0/17', '162.158.0.0/15', '104.16.0.0/13',
    '104.24.0.0/14', '172.64.0.0/13', '131.0.72.0/22',
    '2400:cb00::/32', '2606:4700::/32', '2803:f800::/32', '2405:b500::/32',
    '2405:8100::/32', '2a06:98c0::/29', '2c0f:f248::/32')]


def client_ip():
    peer = request.headers.get('X-Real-IP') or request.remote_addr or ''
    cf = request.headers.get('CF-Connecting-IP', '').strip()
    if cf:
        try:
            if any(ipaddress.ip_address(peer) in n for n in _CLOUDFLARE_NETS):
                return cf
        except ValueError:
            pass
    return peer


limiter = Limiter(
    key_func=client_ip,
    default_limits=[],
    storage_uri=os.environ.get('RATELIMIT_STORAGE_URI', 'memory://'),
    in_memory_fallback_enabled=True,
)


def init_app(app):
    limiter.init_app(app)
    app.register_blueprint(vol_bp)
    try:
        conn = get_db()
        ensure_schema(conn)
        conn.close()
    except Exception:
        log.exception("could not apply volunteer schema")


# ── Database ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DATABASE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_schema(conn):
    conn.executescript((BASE_DIR / "migrations" / "005_volunteers.sql").read_text())
    conn.commit()


def place_tiers(cur):
    """municipality -> 1 (a swing/lean House district votes here), 2 (likely), 3."""
    cur.execute("""
        SELECT md.municipality, d.rating
          FROM municipality_districts md
          JOIN district_pvi_model d ON d.district_code = md.county || ' ' || md.district
         WHERE md.office_id = 7
    """)
    tiers = {}
    for r in cur.fetchall():
        rating = (r['rating'] or '').upper()
        t = 1 if (rating == 'SWING' or rating.startswith('LEAN')) else 2 if rating.startswith('LIKELY') else 3
        tiers[r['municipality']] = min(t, tiers.get(r['municipality'], 3))
    return tiers


def all_places(cur):
    """Every polling place with tier and coverage counts."""
    tiers = place_tiers(cur)
    cur.execute("""
        SELECT p.municipality, p.county, p.polling_place, p.polling_hours,
               SUM(CASE WHEN v.status = 'approved' THEN 1 ELSE 0 END) AS approved,
               SUM(CASE WHEN v.status = 'pending' THEN 1 ELSE 0 END) AS pending
          FROM polling_places p
          LEFT JOIN volunteer_assignments a ON a.municipality = p.municipality
          LEFT JOIN volunteers v ON v.id = a.volunteer_id AND v.status IN ('approved','pending')
         GROUP BY p.municipality
         ORDER BY p.county, p.municipality
    """)
    out = []
    for r in cur.fetchall():
        d = dict(r)
        d['approved'] = d['approved'] or 0
        d['pending'] = d['pending'] or 0
        d['tier'] = tiers.get(d['municipality'], 3)
        d['location'] = pretty_location(d['polling_place'])
        out.append(d)
    return out


def pretty_location(raw):
    """'ACWORTH TOWN HALL 13 TOWN HALL RD ACWORTH 03601' -> title case, ZIP kept."""
    if not raw or raw.strip().lower() == 'not applicable':
        return ''
    s = raw.strip().title()
    s = re.sub(r"\b(Nh|Us)\b", lambda m: m.group(1).upper(), s)
    s = re.sub(r"\b(\d+)(St|Nd|Rd|Th)\b", lambda m: m.group(1) + m.group(2).lower(), s)
    return s


def volunteer_by_token(cur, token):
    if not token or len(token) < 20:
        return None
    cur.execute("SELECT * FROM volunteers WHERE token = ?", (token,))
    return cur.fetchone()


def assignments_for(cur, volunteer_id):
    cur.execute("""
        SELECT a.municipality, p.county, p.polling_place, p.polling_hours
          FROM volunteer_assignments a
          LEFT JOIN polling_places p ON p.municipality = a.municipality
         WHERE a.volunteer_id = ? ORDER BY a.id
    """, (volunteer_id,))
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r['location'] = pretty_location(r['polling_place'])
    return rows


# ── Email ────────────────────────────────────────────────────────────────────

def send_email(to, subject, html, text, reply_to=None):
    """Send through SES. Returns True on success; never raises into a request."""
    try:
        import boto3
        ses = boto3.client('ses', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        ses.send_email(
            Source=MAIL_FROM,
            Destination={'ToAddresses': [to]},
            ReplyToAddresses=[reply_to or MAIL_REPLY_TO],
            Message={'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                     'Body': {'Html': {'Data': html, 'Charset': 'UTF-8'},
                              'Text': {'Data': text, 'Charset': 'UTF-8'}}},
        )
        return True
    except Exception:
        log.exception("volunteer email to %s failed", to)
        return False


def portal_url(token):
    return f"{PUBLIC_BASE_URL}/v/{token}"


def _places_text(places):
    return ", ".join(p['municipality'] for p in places) or "(none yet)"


def _wrap_html(inner):
    return ('<div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;'
            'font-size:15px;line-height:1.55;color:#1d2a22;max-width:560px">' + inner + '</div>')


def _button(href, label):
    return (f'<p style="margin:22px 0"><a href="{href}" style="background:#1f5a32;color:#fff;'
            f'text-decoration:none;padding:12px 20px;border-radius:6px;font-weight:600;'
            f'display:inline-block">{label}</a></p>')


def email_signup_received(v, places):
    first = v['name'].split()[0]
    places_list = "".join(f"<li>{p['municipality']}</li>" for p in places)
    html = _wrap_html(f"""
<p>Hi {first},</p>
<p>Thank you for signing up to report election results on {ELECTION_DAY}. You asked to cover:</p>
<ul>{places_list}</ul>
<p>We will confirm your spot shortly and send you a personal link. That link is how you
will send the results in from your phone on election night.</p>
<p>Questions? Just reply to this email.</p>
<p>Thank you,<br>Election Night Team<br>Committee to Elect House Republicans</p>""")
    text = (f"Hi {first},\n\nThank you for signing up to report election results on {ELECTION_DAY}. "
            f"You asked to cover: {_places_text(places)}.\n\nWe will confirm your spot shortly and send you a "
            "personal link. That link is how you will send the results in from your phone on election night.\n\n"
            "Questions? Just reply to this email.\n\nThank you,\nElection Night Team\n"
            "Committee to Elect House Republicans\n")
    return send_email(v['email'], "Thanks for signing up for election night", html, text)


def email_approved(v, places):
    first = v['name'].split()[0]
    url = portal_url(v['token'])
    rows = "".join(
        f"<li><b>{p['municipality']}</b>"
        + (f"<br><span style='color:#555'>{p['location']}</span>" if p.get('location') else "")
        + (f"<br><span style='color:#555'>Polls open {p['polling_hours']}</span>" if p.get('polling_hours') and 'applicable' not in p['polling_hours'].lower() else "")
        + "</li>" for p in places)
    html = _wrap_html(f"""
<p>Hi {first},</p>
<p>You are confirmed to report election results on {ELECTION_DAY}. Your polling place{'s' if len(places) != 1 else ''}:</p>
<ul>{rows}</ul>
<p><b>This is your personal link.</b> Save this email. On election night you will open the
link on your phone, take a photo of the posted results, and tap Send.</p>
{_button(url, 'Open my election night page')}
<p style="font-size:13px;color:#555">Please don't forward this link. It is tied to you and your polling place.</p>
<p>We will send step-by-step instructions before Election Day.</p>
<p>Thank you,<br>Election Night Team<br>Committee to Elect House Republicans</p>""")
    text = (f"Hi {first},\n\nYou are confirmed to report election results on {ELECTION_DAY} for: "
            f"{_places_text(places)}.\n\nThis is your personal link. Save this email. On election night open it "
            f"on your phone, take a photo of the posted results, and tap Send:\n{url}\n\n"
            "Please don't forward this link. It is tied to you and your polling place.\n\n"
            "We will send step-by-step instructions before Election Day.\n\nThank you,\nElection Night Team\n"
            "Committee to Elect House Republicans\n")
    return send_email(v['email'], "You're confirmed for election night - your personal link", html, text)


INSTRUCTIONS_STEPS = [
    "Polls close at 7:00 p.m. Plan to be at your polling place by 7:00 so you are there when counting finishes.",
    "Stay outside the guardrail and do not interfere with the count. You are there to watch, not to help.",
    "When counting is done, the moderator announces the results and posts them, usually on a wall or the door. That is public.",
    "Take a clear photo of every page of the posted results, close up, with the whole sheet in the frame. One photo per page.",
    "Open your personal link and pick your polling place. Add the photos and tap Send. That's it.",
    "If something goes wrong, email the photos to results@electhouserepublicans.com with your town in the subject.",
]


def email_instructions(v, places, reminder=False):
    first = v['name'].split()[0]
    url = portal_url(v['token'])
    steps_html = "".join(f"<li style='margin-bottom:8px'>{s}</li>" for s in INSTRUCTIONS_STEPS)
    steps_text = "\n".join(f"{i}. {s}" for i, s in enumerate(INSTRUCTIONS_STEPS, 1))
    lead = (f"Today is Election Day. Thank you for covering {_places_text(places)} tonight."
            if reminder else
            f"Here is everything you need for election night, {ELECTION_DAY}. You are covering {_places_text(places)}.")
    html = _wrap_html(f"""
<p>Hi {first},</p>
<p>{lead}</p>
<ol style="padding-left:20px">{steps_html}</ol>
{_button(url, 'Open my election night page')}
<p style="font-size:13px;color:#555">Your link is personal. Please don't forward it.</p>
<p>Thank you,<br>Election Night Team<br>Committee to Elect House Republicans</p>""")
    text = (f"Hi {first},\n\n{lead}\n\n{steps_text}\n\nYour personal link:\n{url}\n\n"
            "Please don't forward it.\n\nThank you,\nElection Night Team\nCommittee to Elect House Republicans\n")
    subject = ("Tonight: sending in your polling place results" if reminder
               else f"Election night instructions for {_places_text(places)}")
    return send_email(v['email'], subject, html, text)


def email_review(v, places, is_update):
    url = f"{PUBLIC_BASE_URL}/volunteers/admin?status=pending"
    rows = "".join(f"<li>{p['municipality']}</li>" for p in places)
    html = _wrap_html(f"""
<p><b>{'Updated' if is_update else 'New'} election-night volunteer</b></p>
<p>{v['name']}<br>{v['email']}<br>{v['phone'] or ''}<br>Home town: {v['home_town'] or '-'}<br>
With: {v['affiliation'] or '-'}</p>
<p>Polling places:</p><ul>{rows}</ul>
{_button(url, 'Review volunteers')}""")
    text = (f"{'Updated' if is_update else 'New'} election-night volunteer\n{v['name']}\n{v['email']}\n"
            f"{v['phone'] or ''}\nHome town: {v['home_town'] or '-'}\nWith: {v['affiliation'] or '-'}\n"
            f"Places: {_places_text(places)}\n\nReview: {url}\n")
    return send_email(REVIEW_EMAIL, f"Election night volunteer: {v['name']}", html, text,
                      reply_to=v['email'])


# ── Public sign-up ───────────────────────────────────────────────────────────

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


def _verify_turnstile(token):
    if not TURNSTILE_SECRET_KEY:
        # Not configured (local development). Production always has a key.
        return True
    if not token:
        return False
    try:
        r = requests.post('https://challenges.cloudflare.com/turnstile/v0/siteverify',
                          data={'secret': TURNSTILE_SECRET_KEY, 'response': token,
                                'remoteip': client_ip()}, timeout=10)
        return bool(r.json().get('success'))
    except Exception:
        log.exception("turnstile verify failed")
        return False


def _clean_phone(raw):
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


@vol_bp.route('/volunteer', methods=['GET'])
def signup():
    conn = get_db()
    places = all_places(conn.cursor())
    conn.close()
    open_count = sum(1 for p in places if p['approved'] + p['pending'] == 0)
    resp = Response(render_template(
        'volunteer/signup.html', places=places, open_count=open_count,
        site_key=TURNSTILE_SITE_KEY, max_places=MAX_PLACES_PER_VOLUNTEER,
        max_per_place=MAX_VOLUNTEERS_PER_PLACE, election_day=ELECTION_DAY,
        tier_labels=TIER_LABELS, form={}, error=None))
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@vol_bp.route('/volunteer', methods=['POST'])
@limiter.limit("6 per hour;20 per day")
def signup_post():
    f = request.form
    form = {k: (f.get(k) or '').strip() for k in ('name', 'email', 'phone', 'home_town', 'affiliation')}
    chosen = [m.strip() for m in f.getlist('places') if m.strip()]

    def fail(msg, code=400):
        conn = get_db()
        places = all_places(conn.cursor())
        conn.close()
        return render_template(
            'volunteer/signup.html', places=places,
            open_count=sum(1 for p in places if p['approved'] + p['pending'] == 0),
            site_key=TURNSTILE_SITE_KEY, max_places=MAX_PLACES_PER_VOLUNTEER,
            max_per_place=MAX_VOLUNTEERS_PER_PLACE, election_day=ELECTION_DAY,
            tier_labels=TIER_LABELS, form=form, chosen=chosen, error=msg), code

    # Honeypot: a field people never see. Bots fill it. Pretend it worked.
    if f.get('hp_ref'):
        log.info("volunteer honeypot tripped from %s", client_ip())
        return redirect(url_for('vol.signup_thanks'))

    if not _verify_turnstile(f.get('cf-turnstile-response')):
        return fail("Please complete the verification box and try again.")

    name, email = form['name'], form['email'].lower()
    if not (2 <= len(name) <= 100) or not re.search(r"[A-Za-z]", name):
        return fail("Please enter your full name.")
    if not EMAIL_RE.match(email) or len(email) > 200:
        return fail("Please enter a valid email address.")
    phone = _clean_phone(form['phone'])
    if not phone:
        return fail("Please enter a 10-digit cell phone number so we can reach you on election night.")
    if not chosen:
        return fail("Please pick at least one polling place.")
    chosen = list(dict.fromkeys(chosen))[:MAX_PLACES_PER_VOLUNTEER]
    for k in ('home_town', 'affiliation'):
        form[k] = form[k][:120]

    conn = get_db()
    cur = conn.cursor()
    places = {p['municipality']: p for p in all_places(cur)}
    bad = [m for m in chosen if m not in places]
    if bad:
        conn.close()
        return fail("One of the polling places you picked was not recognised. Please pick again.")

    cur.execute("SELECT * FROM volunteers WHERE email = ?", (email,))
    existing = cur.fetchone()
    now = datetime.now().isoformat(timespec='seconds')
    if existing:
        if existing['status'] == 'rejected':
            conn.close()
            return redirect(url_for('vol.signup_thanks'))
        vid = existing['id']
        if existing['status'] == 'pending':
            cur.execute("""UPDATE volunteers SET name=?, phone=?, home_town=?, affiliation=?, updated_at=?
                           WHERE id=?""", (name, phone, form['home_town'], form['affiliation'], now, vid))
    else:
        cur.execute("""INSERT INTO volunteers (name, email, phone, home_town, affiliation, token, signup_ip)
                       VALUES (?,?,?,?,?,?,?)""",
                    (name, email, phone, form['home_town'], form['affiliation'],
                     secrets.token_urlsafe(24), client_ip()))
        vid = cur.lastrowid

    cur.execute("SELECT municipality FROM volunteer_assignments WHERE volunteer_id = ?", (vid,))
    have = [r['municipality'] for r in cur.fetchall()]
    full = []
    for m in chosen:
        if m in have:
            continue
        if len(have) >= MAX_PLACES_PER_VOLUNTEER:
            break
        p = places[m]
        if p['approved'] + p['pending'] >= MAX_VOLUNTEERS_PER_PLACE:
            full.append(m)
            continue
        cur.execute("INSERT OR IGNORE INTO volunteer_assignments (volunteer_id, municipality) VALUES (?,?)",
                    (vid, m))
        have.append(m)
    conn.commit()

    if not have:
        conn.close()
        return fail(f"{', '.join(full)} already {'has' if len(full) == 1 else 'have'} enough volunteers. "
                    "Please pick another polling place.")

    cur.execute("SELECT * FROM volunteers WHERE id = ?", (vid,))
    v = cur.fetchone()
    mine = assignments_for(cur, vid)
    conn.close()

    if v['status'] == 'approved':
        email_approved(v, mine)
    else:
        email_signup_received(v, mine)
    email_review(v, mine, is_update=bool(existing))
    return redirect(url_for('vol.signup_thanks', full=','.join(full)) if full else url_for('vol.signup_thanks'))


@vol_bp.route('/volunteer/thanks')
def signup_thanks():
    full = [m for m in (request.args.get('full') or '').split(',') if m][:5]
    return render_template('volunteer/thanks.html', full=full, election_day=ELECTION_DAY)


# ── Volunteer's personal page ───────────────────────────────────────────────

def _submission_status(cur, external_id):
    cur.execute("SELECT id, status FROM intake_messages WHERE source = 'web' AND external_id = ?",
                (external_id,))
    m = cur.fetchone()
    if not m:
        return 'received'
    s = m['status']
    if s == 'applied':
        return 'posted'
    if s in ('queued', 'partial'):
        return 'being checked'
    if s == 'ignored':
        return 'no results found'
    if s == 'error':
        return 'being checked'
    return 'received'


@vol_bp.route('/v/<token>')
def portal(token):
    conn = get_db()
    cur = conn.cursor()
    v = volunteer_by_token(cur, token)
    if not v or v['status'] == 'rejected':
        conn.close()
        abort(404)
    places = assignments_for(cur, v['id'])
    cur.execute("SELECT * FROM volunteer_submissions WHERE volunteer_id = ? ORDER BY id DESC LIMIT 20",
                (v['id'],))
    subs = []
    for s in cur.fetchall():
        d = dict(s)
        d['n_photos'] = len(json.loads(d['attachments'] or '[]'))
        d['state'] = _submission_status(cur, d['external_id'])
        subs.append(d)
    conn.close()
    resp = Response(render_template('volunteer/portal.html', v=v, places=places, subs=subs,
                                    steps=INSTRUCTIONS_STEPS, election_day=ELECTION_DAY,
                                    max_photos=MAX_PHOTOS_PER_REPORT,
                                    sent=request.args.get('sent')))
    resp.headers['Cache-Control'] = 'no-store'
    resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
    resp.headers['Referrer-Policy'] = 'no-referrer'
    return resp


def _approved_volunteer_and_place(cur, token, municipality):
    v = volunteer_by_token(cur, token)
    if not v or v['status'] != 'approved':
        return None, None
    cur.execute("SELECT 1 FROM volunteer_assignments WHERE volunteer_id = ? AND municipality = ?",
                (v['id'], municipality))
    if not cur.fetchone():
        return v, None
    return v, municipality


def _token_key():
    """Limit volunteer uploads per personal link, not per IP: on election night
    many phones share one carrier-NAT address, and a per-IP cap would lock out
    every volunteer behind it at once."""
    return 'vtok:' + ((request.view_args or {}).get('token') or client_ip())


@vol_bp.route('/v/<token>/photo', methods=['POST'])
@limiter.limit("120 per hour", key_func=_token_key)
def portal_photo(token):
    conn = get_db()
    cur = conn.cursor()
    v, muni = _approved_volunteer_and_place(cur, token, (request.form.get('municipality') or '').strip())
    if not v or not muni:
        conn.close()
        return jsonify(ok=False, error="This link can't send results for that polling place."), 403
    f = request.files.get('photo')
    if not f:
        conn.close()
        return jsonify(ok=False, error="No photo received."), 400
    data = f.read(MAX_PHOTO_BYTES + 1)
    if not data or len(data) > MAX_PHOTO_BYTES:
        conn.close()
        return jsonify(ok=False, error="That photo is too large."), 400
    # Accept images and PDFs only; sniff, don't trust the filename.
    if data[:3] == b'\xff\xd8\xff':
        ext = 'jpg'
    elif data[:8] == b'\x89PNG\r\n\x1a\n':
        ext = 'png'
    elif data[:4] == b'%PDF':
        ext = 'pdf'
    elif data[4:8] == b'ftyp':
        ext = 'heic'
    elif data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        ext = 'webp'
    else:
        conn.close()
        return jsonify(ok=False, error="Please send a photo (JPG, PNG or HEIC) or a PDF."), 400
    ATTACH_DIR.mkdir(parents=True, exist_ok=True)
    # Named after the polling place: the intake parser reads the town from the
    # filename (runner._attachment_label), same as a clerk's emailed PDF.
    safe = re.sub(r"[^A-Za-z0-9]+", "_", muni).strip('_')
    path = ATTACH_DIR / f"{uuid.uuid4().hex[:8]}_{safe}.{ext}"
    path.write_bytes(data)
    cur.execute("INSERT INTO volunteer_uploads (volunteer_id, municipality, path) VALUES (?,?,?)",
                (v['id'], muni, str(path)))
    conn.commit()
    uid = cur.lastrowid
    conn.close()
    return jsonify(ok=True, id=uid)


@vol_bp.route('/v/<token>/send', methods=['POST'])
@limiter.limit("40 per hour", key_func=_token_key)
def portal_send(token):
    conn = get_db()
    cur = conn.cursor()
    muni_in = (request.form.get('municipality') or '').strip()
    v, muni = _approved_volunteer_and_place(cur, token, muni_in)
    if not v:
        conn.close()
        abort(404)
    if not muni:
        conn.close()
        flash("Pick your polling place first.", 'error')
        return redirect(url_for('vol.portal', token=token))
    note = (request.form.get('note') or '').strip()[:4000]
    ids = []
    for x in (request.form.get('upload_ids') or '').split(','):
        if x.strip().isdigit():
            ids.append(int(x))
    ids = ids[:MAX_PHOTOS_PER_REPORT]
    paths = []
    if ids:
        q = ",".join("?" * len(ids))
        cur.execute(f"""SELECT id, path FROM volunteer_uploads
                         WHERE id IN ({q}) AND volunteer_id = ? AND municipality = ? AND submission_id IS NULL""",
                    ids + [v['id'], muni])
        rows = cur.fetchall()
        paths = [r['path'] for r in rows]
        ids = [r['id'] for r in rows]
    if not paths and not re.search(r"\d", note):
        conn.close()
        flash("Add a photo of the results, or type the numbers, before sending.", 'error')
        return redirect(url_for('vol.portal', token=token))

    now = datetime.now().isoformat(timespec='seconds')
    cur.execute("""INSERT INTO volunteer_submissions (volunteer_id, municipality, note, attachments)
                   VALUES (?,?,?,?)""", (v['id'], muni, note, json.dumps(paths)))
    sid = cur.lastrowid
    ext_id = f"web-{sid}"
    cur.execute("UPDATE volunteer_submissions SET external_id = ? WHERE id = ?", (ext_id, sid))
    if ids:
        q = ",".join("?" * len(ids))
        cur.execute(f"UPDATE volunteer_uploads SET submission_id = ? WHERE id IN ({q})", [sid] + ids)
    # status 'web' = waiting for the intake service's web loop to claim it.
    cur.execute("""INSERT OR IGNORE INTO intake_messages
                       (source, external_id, sender, subject, body, attachments, received_at, status)
                   VALUES ('web', ?, ?, ?, ?, ?, ?, 'web')""",
                (ext_id, f"{v['name']} <{v['email']}> (volunteer #{v['id']}, {v['phone'] or ''})",
                 muni, note, json.dumps(paths), now))
    conn.commit()
    conn.close()
    return redirect(url_for('vol.portal', token=token, sent=sid))


# ── Admin ────────────────────────────────────────────────────────────────────

def _volunteer_rows(cur, status=None):
    sql = """SELECT v.*, GROUP_CONCAT(a.municipality, '|') AS places,
                    (SELECT COUNT(*) FROM volunteer_submissions s WHERE s.volunteer_id = v.id) AS n_reports
               FROM volunteers v
               LEFT JOIN volunteer_assignments a ON a.volunteer_id = v.id"""
    args = []
    if status:
        sql += " WHERE v.status = ?"
        args.append(status)
    sql += " GROUP BY v.id ORDER BY CASE v.status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END, v.created_at DESC"
    cur.execute(sql, args)
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        d['place_list'] = [p for p in (d['places'] or '').split('|') if p]
        rows.append(d)
    return rows


@vol_bp.route('/volunteers/admin')
@admin_required
def admin_list():
    status = request.args.get('status') or None
    if status not in (None, 'pending', 'approved', 'rejected'):
        status = None
    conn = get_db()
    cur = conn.cursor()
    places = all_places(cur)
    vols = _volunteer_rows(cur, status)
    cur.execute("SELECT status, COUNT(*) AS n FROM volunteers GROUP BY status")
    counts = {r['status']: r['n'] for r in cur.fetchall()}
    conn.close()

    def cov(pl):
        return {
            'total': len(pl),
            'covered': sum(1 for p in pl if p['approved']),
            'pending': sum(1 for p in pl if not p['approved'] and p['pending']),
            'open': sum(1 for p in pl if not p['approved'] and not p['pending']),
        }
    by_tier = {t: cov([p for p in places if p['tier'] == t]) for t in (1, 2, 3)}
    counties = sorted({p['county'] or 'Other' for p in places})
    by_county = {c: cov([p for p in places if (p['county'] or 'Other') == c]) for c in counties}
    return render_template('volunteer/admin.html', vols=vols, counts=counts, status=status,
                           total=cov(places), by_tier=by_tier, by_county=by_county,
                           tier_labels=TIER_LABELS, all_place_names=[p['municipality'] for p in places])


@vol_bp.route('/volunteers/admin/<int:vid>/<action>', methods=['POST'])
@admin_required
def admin_action(vid, action):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM volunteers WHERE id = ?", (vid,))
    v = cur.fetchone()
    if not v:
        conn.close()
        abort(404)
    now = datetime.now().isoformat(timespec='seconds')
    if action == 'approve':
        cur.execute("""UPDATE volunteers SET status='approved', approved_at=?, approved_by=?, updated_at=?
                       WHERE id=?""", (now, current_user.id, now, vid))
        conn.commit()
        cur.execute("SELECT * FROM volunteers WHERE id = ?", (vid,))
        v = cur.fetchone()
        if email_approved(v, assignments_for(cur, vid)):
            cur.execute("UPDATE volunteers SET link_sent_at=? WHERE id=?", (now, vid))
            flash(f"Approved {v['name']} and emailed their link.", 'success')
        else:
            flash(f"Approved {v['name']}, but the email failed. Use Resend link.", 'error')
    elif action == 'reject':
        cur.execute("UPDATE volunteers SET status='rejected', updated_at=? WHERE id=?", (now, vid))
        flash(f"Removed {v['name']}.", 'success')
    elif action == 'pending':
        cur.execute("UPDATE volunteers SET status='pending', updated_at=? WHERE id=?", (now, vid))
        flash(f"Moved {v['name']} back to pending.", 'success')
    elif action == 'resend':
        if v['status'] != 'approved':
            flash("Approve them first.", 'error')
        elif email_approved(v, assignments_for(cur, vid)):
            cur.execute("UPDATE volunteers SET link_sent_at=? WHERE id=?", (now, vid))
            flash(f"Resent {v['name']}'s link.", 'success')
        else:
            flash("Email failed.", 'error')
    elif action == 'newlink':
        cur.execute("UPDATE volunteers SET token=?, updated_at=? WHERE id=?",
                    (secrets.token_urlsafe(24), now, vid))
        flash(f"{v['name']}'s old link no longer works. Use Resend link to send the new one.", 'success')
    elif action == 'addplace':
        m = (request.form.get('municipality') or '').strip()
        cur.execute("SELECT 1 FROM polling_places WHERE municipality = ?", (m,))
        if cur.fetchone():
            cur.execute("INSERT OR IGNORE INTO volunteer_assignments (volunteer_id, municipality) VALUES (?,?)",
                        (vid, m))
            flash(f"Added {m} for {v['name']}.", 'success')
        else:
            flash("Unknown polling place.", 'error')
    elif action == 'removeplace':
        m = (request.form.get('municipality') or '').strip()
        cur.execute("DELETE FROM volunteer_assignments WHERE volunteer_id = ? AND municipality = ?", (vid, m))
        flash(f"Removed {m} from {v['name']}.", 'success')
    elif action == 'note':
        cur.execute("UPDATE volunteers SET admin_note=?, updated_at=? WHERE id=?",
                    ((request.form.get('note') or '').strip()[:500], now, vid))
    else:
        conn.close()
        abort(404)
    conn.commit()
    conn.close()
    return redirect(request.referrer or url_for('vol.admin_list'))


@vol_bp.route('/volunteers/admin/approve-bulk', methods=['POST'])
@admin_required
def admin_approve_bulk():
    ids = [int(x) for x in request.form.getlist('ids') if x.isdigit()]
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().isoformat(timespec='seconds')
    sent = failed = 0
    for vid in ids:
        cur.execute("SELECT * FROM volunteers WHERE id = ? AND status = 'pending'", (vid,))
        if not cur.fetchone():
            continue
        cur.execute("""UPDATE volunteers SET status='approved', approved_at=?, approved_by=?, updated_at=?
                       WHERE id=?""", (now, current_user.id, now, vid))
        conn.commit()
        cur.execute("SELECT * FROM volunteers WHERE id = ?", (vid,))
        v = cur.fetchone()
        if email_approved(v, assignments_for(cur, vid)):
            cur.execute("UPDATE volunteers SET link_sent_at=? WHERE id=?", (now, vid))
            sent += 1
        else:
            failed += 1
        conn.commit()
    conn.close()
    flash(f"Approved {sent + failed}; emailed {sent}" + (f", {failed} failed" if failed else "") + ".",
          'success' if not failed else 'error')
    return redirect(url_for('vol.admin_list', status='pending'))


@vol_bp.route('/volunteers/admin/export.csv')
@admin_required
def admin_export():
    import csv
    import io
    conn = get_db()
    cur = conn.cursor()
    tiers = place_tiers(cur)
    cur.execute("""SELECT v.id, v.name, v.email, v.phone, v.home_town, v.affiliation, v.status,
                          v.created_at, v.link_sent_at, a.municipality, p.county, p.polling_place
                     FROM volunteers v
                     LEFT JOIN volunteer_assignments a ON a.volunteer_id = v.id
                     LEFT JOIN polling_places p ON p.municipality = a.municipality
                    ORDER BY p.county, a.municipality, v.name""")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['volunteer_id', 'name', 'email', 'phone', 'home_town', 'with', 'status', 'signed_up',
                'link_sent', 'polling_place', 'county', 'priority', 'location'])
    for r in cur.fetchall():
        w.writerow([r['id'], r['name'], r['email'], r['phone'], r['home_town'], r['affiliation'], r['status'],
                    r['created_at'], r['link_sent_at'], r['municipality'], r['county'],
                    TIER_LABELS.get(tiers.get(r['municipality'], 3), '') if r['municipality'] else '',
                    pretty_location(r['polling_place'])])
    conn.close()
    return Response(buf.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=election_night_volunteers.csv'})


@vol_bp.route('/volunteers/admin/board')
@admin_required
def admin_board():
    """Election-night view: every polling place, who's on it, and whether it has reported."""
    conn = get_db()
    cur = conn.cursor()
    places = all_places(cur)

    cur.execute("SELECT id FROM elections WHERE year = ? AND election_type = ?", (EVENT_YEAR, EVENT_TYPE))
    eids = [r['id'] for r in cur.fetchall()]
    reported = {}
    if eids:
        q = ",".join("?" * len(eids))
        cur.execute(f"""SELECT res.municipality, COUNT(DISTINCT res.race_id) AS n
                          FROM results res JOIN races r ON r.id = res.race_id
                         WHERE r.election_id IN ({q}) GROUP BY res.municipality""", eids)
        reported = {r['municipality']: r['n'] for r in cur.fetchall()}

    cur.execute("""SELECT a.municipality, v.id, v.name, v.phone, v.email, v.status
                     FROM volunteer_assignments a JOIN volunteers v ON v.id = a.volunteer_id
                    WHERE v.status IN ('approved','pending') ORDER BY v.status, v.name""")
    vols = {}
    for r in cur.fetchall():
        vols.setdefault(r['municipality'], []).append(dict(r))

    cur.execute("""SELECT municipality, MAX(created_at) AS last, COUNT(*) AS n
                     FROM volunteer_submissions GROUP BY municipality""")
    subs = {r['municipality']: dict(r) for r in cur.fetchall()}
    cur.execute("""SELECT COUNT(*) AS n FROM intake_items WHERE status = 'pending'""")
    queue = cur.fetchone()['n']
    conn.close()

    for p in places:
        p['vols'] = vols.get(p['municipality'], [])
        p['races_in'] = reported.get(p['municipality'], 0)
        p['sub'] = subs.get(p['municipality'])
    n_total = len(places)
    show = request.args.get('show', 'all')
    if show == 'missing':
        places = [p for p in places if not p['races_in']]
    elif show == 'uncovered':
        places = [p for p in places if not p['approved']]
    places.sort(key=lambda p: (p['races_in'] > 0, p['tier'], p['county'] or '', p['municipality']))
    return render_template('volunteer/board.html', places=places, show=show, queue=queue,
                           tier_labels=TIER_LABELS, n_reported=sum(1 for v in reported.values() if v),
                           n_total=n_total)
