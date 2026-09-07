"""Database access for the intake service.

Every inbound message is recorded verbatim before it is parsed, so a bad parse
can be replayed against the original once the parser is fixed.
"""

import json
import sqlite3
from pathlib import Path

from intake import config


def connect():
    conn = sqlite3.connect(config.DATABASE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema(conn):
    """Apply the intake migration. Safe to run repeatedly."""
    sql = (Path(config.BASE_DIR) / "migrations" / "002_intake.sql").read_text()
    conn.executescript(sql)
    conn.commit()


def bot_user_id(conn):
    """The user id intake writes audit rows as, created on first use.

    The account gets an unusable password hash - it exists to attribute audit
    rows, and must never be able to log in.
    """
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username = ?", (config.BOT_USERNAME,))
    row = cur.fetchone()
    if row:
        return row["id"]
    # Several reports are handled at once, so two threads can reach this on a
    # cold database together. INSERT OR IGNORE plus a re-read is safe either way.
    cur.execute(
        "INSERT OR IGNORE INTO users (username, password_hash, role) VALUES (?, ?, ?)",
        (config.BOT_USERNAME, "!", "bot"),
    )
    conn.commit()
    cur.execute("SELECT id FROM users WHERE username = ?", (config.BOT_USERNAME,))
    return cur.fetchone()["id"]


def record_message(conn, source, external_id, sender, subject, body, attachments, received_at):
    """Store a raw inbound message. Returns (message_id, is_new)."""
    cur = conn.cursor()
    cur.execute(
        """INSERT OR IGNORE INTO intake_messages
               (source, external_id, sender, subject, body, attachments, received_at)
           VALUES (?,?,?,?,?,?,?)""",
        (source, external_id, sender, subject, body, json.dumps(attachments or []), received_at),
    )
    if cur.rowcount == 0:
        conn.commit()
        cur.execute(
            "SELECT id FROM intake_messages WHERE source = ? AND external_id = ?",
            (source, external_id),
        )
        return cur.fetchone()["id"], False
    conn.commit()
    return cur.lastrowid, True


def set_message_status(conn, message_id, status, parse_json=None, error=None):
    conn.execute(
        "UPDATE intake_messages SET status = ?, parse_json = COALESCE(?, parse_json), error = ? WHERE id = ?",
        (status, parse_json, error, message_id),
    )
    conn.commit()


def add_item(conn, message_id, **kw):
    # A column DEFAULT only applies when the column is omitted; passing an
    # explicit None still violates NOT NULL. Fill the defaults here so callers
    # that only care about, say, the reason cannot trip over it.
    kw.setdefault("kind", "result")
    kw.setdefault("status", "pending")
    cols = ("kind", "municipality", "municipality_text", "election_id", "race_id",
            "race_text", "candidate_id", "candidate_text", "votes", "old_votes",
            "confidence", "status", "reason")
    values = [kw.get(c) for c in cols]
    cur = conn.cursor()
    cur.execute(
        f"""INSERT INTO intake_items (message_id, {','.join(cols)})
            VALUES ({','.join('?' * (len(cols) + 1))})""",
        [message_id] + values,
    )
    conn.commit()
    return cur.lastrowid


def pending_items(conn, limit=500):
    cur = conn.cursor()
    cur.execute(
        """SELECT i.*, m.source, m.sender, m.subject, m.body, m.attachments,
                  m.received_at, m.created_at AS message_created_at
             FROM intake_items i
             JOIN intake_messages m ON i.message_id = m.id
            WHERE i.status = 'pending'
            ORDER BY m.received_at DESC, i.municipality, i.id
            LIMIT ?""",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


def recent_messages(conn, limit=100):
    cur = conn.cursor()
    cur.execute(
        """SELECT m.*,
                  (SELECT COUNT(*) FROM intake_items i
                    WHERE i.message_id = m.id AND i.status = 'applied') AS applied_count,
                  (SELECT COUNT(*) FROM intake_items i
                    WHERE i.message_id = m.id AND i.status = 'pending') AS pending_count
             FROM intake_messages m
            ORDER BY m.id DESC LIMIT ?""",
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]
