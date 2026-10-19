"""Review queue for automated intake.

Everything the parser was not confident enough to publish lands here, with the
original message next to it, so a reviewer can see what the reporter actually
wrote before accepting a number.
"""

import json
import mimetypes
from pathlib import Path

from flask import Blueprint, abort, jsonify, render_template, request, send_file
from flask_login import current_user, login_required

from auth import get_db
from entry import log_audit, normalize_name

intake_bp = Blueprint("intake", __name__, url_prefix="/entry/intake")


def _attachments_for(cursor, message_id):
    cursor.execute("SELECT attachments FROM intake_messages WHERE id = ?", (message_id,))
    row = cursor.fetchone()
    if not row:
        return []
    try:
        return json.loads(row["attachments"] or "[]")
    except (TypeError, ValueError):
        return []


def _sniff(path):
    """Content type from magic bytes - signal-cli stores files with no suffix."""
    try:
        head = Path(path).open("rb").read(12)
    except OSError:
        return None
    if head[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    if head[:5] == b"%PDF-":
        return "application/pdf"
    if head[4:8] == b"ftyp":
        return "image/heic"
    return mimetypes.guess_type(str(path))[0]


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
        g = groups.setdefault(it["message_id"],
                              {"message": it, "items": [], "attachments": []})
        g["items"].append(it)
    for mid, g in groups.items():
        try:
            paths = json.loads(g["message"].get("attachments") or "[]")
        except (TypeError, ValueError):
            paths = []
        g["attachments"] = [
            {"index": i, "name": Path(p).name,
             "is_pdf": (_sniff(p) == "application/pdf") if Path(p).exists() else False}
            for i, p in enumerate(paths)
        ]

    return render_template("entry/intake.html", groups=list(groups.values()),
                           counts=counts, recent=recent)


def _apply_item(cursor, item, action, data, user_id):
    """Apply one queued line. Returns (status, error); the caller commits."""
    votes = None
    if action == "reject":
        cursor.execute(
            "UPDATE intake_items SET status='rejected', reviewed_by=? WHERE id=?",
            (user_id, item["id"]),
        )
        return "rejected", None

    votes = data.get("votes", item["votes"])
    race_id = data.get("race_id", item["race_id"])
    candidate_id = data.get("candidate_id", item["candidate_id"])
    votes = int(votes) if votes is not None else None

    # A town with several machines sends several tapes. "add" sums this
    # reading with what is recorded NOW, not with old_votes (the value when the
    # line was queued): with tapes 2 and 3 both waiting against tape 1, adding
    # both to the stale figure made the second Add overwrite the first, and the
    # town lost a machine.
    if action == "add" and votes is not None:
        if item["kind"] == "ballots":
            cursor.execute("SELECT ballots_cast FROM voter_registration WHERE election_id=? AND municipality=?",
                           (item["election_id"], item["municipality"]))
            cur_row = cursor.fetchone()
            votes += (cur_row["ballots_cast"] or 0) if cur_row else 0
        else:
            cursor.execute("SELECT votes FROM results WHERE race_id=? AND candidate_id=? AND municipality=?",
                           (race_id, candidate_id, item["municipality"]))
            cur_row = cursor.fetchone()
            votes += cur_row["votes"] if cur_row else 0

    # A name the ballot does not carry is a write-in. Record it the way the
    # hand-entry screen does: a candidate plus a roster row flagged as a
    # write-in (recruitment_filing_id = -1), so both paths agree.
    if action == "writein":
        name = (data.get("name") or item["candidate_text"] or "").strip()
        if not (race_id and name):
            return None, "needs a race and a name"
        cursor.execute("""SELECT r.election_id, e.party FROM races r
                          JOIN elections e ON r.election_id = e.id WHERE r.id = ?""",
                       (race_id,))
        rr = cursor.fetchone()
        if not rr:
            return None, "unknown race"
        party = rr["party"]
        norm = normalize_name(name)
        cursor.execute("SELECT id FROM candidates WHERE name_normalized = ? AND party IS ?",
                       (norm, party))
        crow = cursor.fetchone()
        if crow:
            candidate_id = crow["id"]
        else:
            cursor.execute(
                "INSERT INTO candidates (name, name_normalized, party) VALUES (?,?,?)",
                (name, norm, party))
            candidate_id = cursor.lastrowid
        cursor.execute("""INSERT OR IGNORE INTO race_candidates
                          (race_id, candidate_id, party, ballot_order, is_incumbent,
                           recruitment_candidate_id, recruitment_filing_id)
                          VALUES (?, ?, ?, 900, 0, NULL, -1)""",
                       (race_id, candidate_id, party))

    if item["kind"] == "ballots":
        if not item["election_id"] or votes is None:
            return None, "needs an election and a count"
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
            return None, "needs a race, a candidate and a count"
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
            log_audit(cursor, user_id, race_id, item["municipality"], candidate_id,
                      "update", {"votes": old["votes"]}, {"votes": votes})
        else:
            cursor.execute(
                "INSERT INTO results (race_id, candidate_id, municipality, votes) VALUES (?,?,?,?)",
                (race_id, candidate_id, item["municipality"], votes),
            )
            log_audit(cursor, user_id, race_id, item["municipality"], candidate_id,
                      "create", None, {"votes": votes})

    cursor.execute(
        """UPDATE intake_items
              SET status='applied', votes=?, race_id=?, candidate_id=?,
                  reviewed_by=?, applied_at=CURRENT_TIMESTAMP
            WHERE id=?""",
        (votes, race_id, candidate_id, user_id, item["id"]),
    )
    return "applied", votes


@intake_bp.route("/item/<int:item_id>/<action>", methods=["POST"])
@login_required
def resolve(item_id, action):
    """Accept (optionally with an edited count) or reject one queued line."""
    if action not in ("accept", "reject", "add", "writein"):
        return jsonify({"error": "unknown action"}), 400

    data = request.get_json(silent=True) or {}
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM intake_items WHERE id = ?", (item_id,))
        item = cursor.fetchone()
        if not item or item["status"] != "pending":
            return jsonify({"error": "not pending"}), 404
        status, result = _apply_item(cursor, item, action, data, current_user.id)
        if status is None:
            return jsonify({"error": result}), 400
        conn.commit()
        if status == "rejected":
            return jsonify({"success": True, "status": "rejected"})
        return jsonify({"success": True, "status": "applied", "votes": result})
    finally:
        conn.close()


@intake_bp.route("/message/<int:message_id>/bulk/<action>", methods=["POST"])
@login_required
def resolve_message(message_id, action):
    """One click for a whole report.

    A second machine's tape is fifty-odd lines, each held because the town
    already has a number - one click per line does not survive election night.
      add    - this is another machine's tape: lines with a number already on
               file are added to it, lines without one are recorded as read
      accept - record every line as read (replacing what is on file)
      reject - throw the whole report out
    Lines that need a human (no candidate matched) are skipped and left pending.
    """
    if action not in ("add", "accept", "reject"):
        return jsonify({"error": "unknown action"}), 400
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM intake_items WHERE message_id = ? AND status = 'pending' ORDER BY id",
                       (message_id,))
        items = cursor.fetchall()
        done = skipped = 0
        for item in items:
            # "add" on a line with nothing on file adds to zero, i.e. records it.
            status, _ = _apply_item(cursor, item, action, {}, current_user.id)
            if status is None:
                skipped += 1
            else:
                done += 1
        conn.commit()
        return jsonify({"success": True, "done": done, "skipped": skipped})
    finally:
        conn.close()


@intake_bp.route("/attachment/<int:message_id>/<int:index>")
@login_required
def attachment(message_id, index):
    """Serve one stored attachment so a reviewer can read the original.

    Only paths this message actually recorded are served, so the index cannot
    be used to reach anything else on disk. HEIC is converted on the way out,
    since browsers will not render it.
    """
    conn = get_db()
    try:
        paths = _attachments_for(conn.cursor(), message_id)
    finally:
        conn.close()
    if index < 0 or index >= len(paths):
        abort(404)

    path = Path(paths[index])
    if not path.exists():
        abort(404)

    media_type = _sniff(path) or "application/octet-stream"
    if media_type == "image/heic":
        try:
            import io

            import pillow_heif
            from PIL import Image
            pillow_heif.register_heif_opener()
            img = Image.open(path)
            img.load()
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=90)
            buf.seek(0)
            return send_file(buf, mimetype="image/jpeg")
        except Exception:
            abort(415)

    return send_file(str(path), mimetype=media_type)


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
