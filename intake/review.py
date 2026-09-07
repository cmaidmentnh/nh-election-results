"""Review queue for automated intake.

Everything the parser was not confident enough to publish lands here, with the
original message next to it, so a reviewer can see what the reporter actually
wrote before accepting a number.
"""

import json

from flask import Blueprint, jsonify, render_template, request
from flask_login import current_user, login_required

from auth import get_db
from entry import log_audit

intake_bp = Blueprint("intake", __name__, url_prefix="/entry/intake")


def _pending(cursor, limit=500):
    cursor.execute(
        """SELECT i.*, m.source, m.sender, m.subject, m.body, m.attachments, m.received_at,
                  c.name AS candidate_name, o.name AS office, r.county, r.district,
                  e.party AS ballot_party
             FROM intake_items i
             JOIN intake_messages m ON i.message_id = m.id
        LEFT JOIN candidates c  ON i.candidate_id = c.id
        LEFT JOIN races r       ON i.race_id = r.id
        LEFT JOIN offices o     ON r.office_id = o.id
        LEFT JOIN elections e   ON i.election_id = e.id
            WHERE i.status = 'pending'
            ORDER BY m.id DESC, i.municipality, i.id
            LIMIT ?""",
        (limit,),
    )
    return [dict(r) for r in cursor.fetchall()]


@intake_bp.route("/")
@login_required
def index():
    conn = get_db()
    cursor = conn.cursor()
    try:
        items = _pending(cursor)
        cursor.execute(
            """SELECT status, COUNT(*) AS n FROM intake_messages
                WHERE created_at > datetime('now', '-1 day') GROUP BY status"""
        )
        counts = {r["status"]: r["n"] for r in cursor.fetchall()}
        cursor.execute(
            """SELECT m.*,
                      (SELECT COUNT(*) FROM intake_items i
                        WHERE i.message_id = m.id AND i.status = 'applied') AS applied_count
                 FROM intake_messages m
                ORDER BY m.id DESC LIMIT 40"""
        )
        recent = [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()

    # Group the queue by the message it came from - a reviewer works a report
    # at a time, not a line at a time.
    groups = {}
    for it in items:
        groups.setdefault(it["message_id"], {"message": it, "items": []})["items"].append(it)

    return render_template("entry/intake.html", groups=list(groups.values()),
                           counts=counts, recent=recent)


@intake_bp.route("/item/<int:item_id>/<action>", methods=["POST"])
@login_required
def resolve(item_id, action):
    """Accept (optionally with an edited count) or reject one queued line."""
    if action not in ("accept", "reject"):
        return jsonify({"error": "unknown action"}), 400

    data = request.get_json(silent=True) or {}
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM intake_items WHERE id = ?", (item_id,))
        item = cursor.fetchone()
        if not item or item["status"] != "pending":
            return jsonify({"error": "not pending"}), 404

        if action == "reject":
            cursor.execute(
                "UPDATE intake_items SET status='rejected', reviewed_by=? WHERE id=?",
                (current_user.id, item_id),
            )
            conn.commit()
            return jsonify({"success": True, "status": "rejected"})

        votes = data.get("votes", item["votes"])
        race_id = data.get("race_id", item["race_id"])
        candidate_id = data.get("candidate_id", item["candidate_id"])
        votes = int(votes) if votes is not None else None

        if item["kind"] == "ballots":
            if not item["election_id"] or votes is None:
                return jsonify({"error": "needs an election and a count"}), 400
            cursor.execute(
                "SELECT id FROM voter_registration WHERE election_id=? AND municipality=?",
                (item["election_id"], item["municipality"]),
            )
            row = cursor.fetchone()
            if row:
                cursor.execute("UPDATE voter_registration SET ballots_cast=? WHERE id=?",
                               (votes, row["id"]))
            else:
                cursor.execute("SELECT county FROM polling_places WHERE municipality=?",
                               (item["municipality"],))
                cr = cursor.fetchone()
                cursor.execute(
                    """INSERT INTO voter_registration (election_id, county, municipality, ballots_cast)
                       VALUES (?,?,?,?)""",
                    (item["election_id"], (cr["county"] if cr else "") or "",
                     item["municipality"], votes),
                )
        else:
            if not (race_id and candidate_id and votes is not None and item["municipality"]):
                return jsonify({"error": "needs a race, a candidate and a count"}), 400
            cursor.execute(
                "SELECT votes FROM results WHERE race_id=? AND candidate_id=? AND municipality=?",
                (race_id, candidate_id, item["municipality"]),
            )
            old = cursor.fetchone()
            if old:
                cursor.execute(
                    "UPDATE results SET votes=? WHERE race_id=? AND candidate_id=? AND municipality=?",
                    (votes, race_id, candidate_id, item["municipality"]),
                )
                log_audit(cursor, current_user.id, race_id, item["municipality"], candidate_id,
                          "update", {"votes": old["votes"]}, {"votes": votes})
            else:
                cursor.execute(
                    "INSERT INTO results (race_id, candidate_id, municipality, votes) VALUES (?,?,?,?)",
                    (race_id, candidate_id, item["municipality"], votes),
                )
                log_audit(cursor, current_user.id, race_id, item["municipality"], candidate_id,
                          "create", None, {"votes": votes})

        cursor.execute(
            """UPDATE intake_items
                  SET status='applied', votes=?, race_id=?, candidate_id=?,
                      reviewed_by=?, applied_at=CURRENT_TIMESTAMP
                WHERE id=?""",
            (votes, race_id, candidate_id, current_user.id, item_id),
        )
        conn.commit()
        return jsonify({"success": True, "status": "applied", "votes": votes})
    finally:
        conn.close()


@intake_bp.route("/message/<int:message_id>")
@login_required
def message_detail(message_id):
    """The original report, for checking a parse against what was actually sent."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM intake_messages WHERE id = ?", (message_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        msg = dict(row)
        msg["attachments"] = json.loads(msg.get("attachments") or "[]")
        return jsonify(msg)
    finally:
        conn.close()
