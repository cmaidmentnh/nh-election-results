"""Authentication module for election results entry."""

import sqlite3
import threading
import time
from functools import wraps
from flask import Blueprint, render_template, redirect, url_for, request, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

auth_bp = Blueprint('auth', __name__)
login_manager = LoginManager()

DATABASE = 'nh_elections.db'

def get_db():
    # Wait out another writer (intake, a second reviewer) instead of failing
    # with 'database is locked' the moment two requests overlap.
    conn = sqlite3.connect(DATABASE, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


class User(UserMixin):
    def __init__(self, id, username, role):
        self.id = id
        self.username = username
        self.role = role

    @staticmethod
    def get(user_id):
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, role FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return User(row['id'], row['username'], row['role'])
        return None

    @staticmethod
    def get_by_username(username):
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, password_hash, role FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()
        conn.close()
        return row

    def is_admin(self):
        return self.role == 'admin'


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)


def admin_required(f):
    """Decorator for admin-only routes."""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_admin():
            flash('Admin access required.', 'error')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function


# Failed logins per (address, username), kept per worker. Ten misses in ten
# minutes locks that pair out for the rest of the window. Keyed on the username
# too, so a room of reviewers behind one address can't lock each other out.
_FAILS = {}
_FAILS_LOCK = threading.Lock()
_FAIL_LIMIT, _FAIL_WINDOW = 10, 600


def _fail_key(username):
    ip = request.headers.get('X-Real-IP') or request.remote_addr or ''
    return (ip, username.lower())


def _locked_out(key):
    now = time.monotonic()
    with _FAILS_LOCK:
        hits = [t for t in _FAILS.get(key, []) if now - t < _FAIL_WINDOW]
        _FAILS[key] = hits
        return len(hits) >= _FAIL_LIMIT


def _record_fail(key):
    with _FAILS_LOCK:
        _FAILS.setdefault(key, []).append(time.monotonic())
        if len(_FAILS) > 10000:
            _FAILS.clear()


def _safe_next(target):
    """Only a path on this site: '/entry/', never 'https://evil' or '//evil'."""
    if not target or not target.startswith('/') or target.startswith('//') or '\\' in target:
        return None
    return target


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('admin.dashboard'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        key = _fail_key(username)
        if _locked_out(key):
            flash('Too many failed attempts. Wait ten minutes and try again.', 'error')
            return render_template('admin/login.html'), 429

        user_row = User.get_by_username(username)
        ok = bool(user_row and check_password_hash(user_row['password_hash'], password))
        if not ok:
            _record_fail(key)
        if ok:
            user = User(user_row['id'], user_row['username'], user_row['role'])
            login_user(user)

            # Update last login
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET last_login = ? WHERE id = ?",
                         (datetime.now(), user_row['id']))
            conn.commit()
            conn.close()

            next_page = _safe_next(request.args.get('next'))
            return redirect(next_page or url_for('admin.dashboard'))

        flash('Invalid username or password.', 'error')

    return render_template('admin/login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'success')
    return redirect(url_for('auth.login'))


# Helper functions for user management
def create_user(username, password, role='user'):
    """Create a new user. Returns user id or None if failed."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        password_hash = generate_password_hash(password)
        cursor.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
            (username, password_hash, role)
        )
        conn.commit()
        user_id = cursor.lastrowid
        conn.close()
        return user_id
    except sqlite3.IntegrityError:
        conn.close()
        return None


def change_password(user_id, new_password):
    """Change a user's password."""
    conn = get_db()
    cursor = conn.cursor()
    password_hash = generate_password_hash(new_password)
    cursor.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, user_id))
    conn.commit()
    conn.close()


def get_all_users():
    """Get all users."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, role, created_at, last_login FROM users ORDER BY username")
    users = cursor.fetchall()
    conn.close()
    return users


def delete_user(user_id):
    """Delete a user."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
