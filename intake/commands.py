"""Act on instructions sent to the bot in a private message.

The operator gets a message for every parse; this lets them answer it - "add
that to what's there", "Ayotte is 1204", "publish Bedford", "that's a write-in"
- instead of opening the review page on a phone.

Deliberately narrow: instructions can only resolve items already sitting in the
intake queue. There is no path from a message to arbitrary SQL, so the worst a
misread instruction can do is accept or reject a pending line, which the audit
log records and a human can undo.
"""

import json
import logging

from pydantic import BaseModel, Field

from entry import log_audit, normalize_name
from intake import config, parser, store

log = logging.getLogger("intake.commands")


class Action(BaseModel):
    item_id: int = Field(description="id of the pending intake item to act on")
    action: str = Field(description="one of: accept, add, reject, writein")
    votes: int = Field(description="vote count to use; 0 to keep the parsed value")


class Plan(BaseModel):
    understood: bool = Field(description="False if the instruction is unclear or not about the queue")
    reply: str = Field(description="One or two short sentences to send back")
    actions: list[Action]


SYSTEM = """You apply an election-night operator's instructions to a queue of \
vote lines awaiting review.

You are given the pending items, each with an id, town, race, candidate, the \
parsed count, and any count already on file. Decide which items the instruction \
refers to and what to do with each:

- accept  - record the parsed count (or the count the operator states in votes)
- add     - add the parsed count to what is already on file; use this when the \
operator says a report is another machine's tape, or says to add it
- reject  - discard the line
- writein - record the name as a named write-in for that race

Rules:
- Only act on items you were given. Never invent an item_id.
- If the operator states a number, put it in votes; otherwise set votes to 0 to \
keep what was parsed.
- If the instruction is ambiguous, or you cannot tell which item is meant, set \
understood=false, return no actions, and say briefly what you need. Doing \
nothing is always safer than acting on the wrong race.
- Match towns and candidates generously (surnames, common misspellings), but \
never across different towns.
- Keep reply short - it is read on a phone."""


def pending_for_prompt(conn, limit=120):
    cur = conn.cursor()
    cur.execute(
        """SELECT i.id, i.municipality, i.candidate_text, i.votes, i.old_votes,
                  i.reason, i.kind, c.name AS candidate_name, o.name AS office,
                  r.district, e.party
             FROM intake_items i
        LEFT JOIN candidates c ON i.candidate_id = c.id
        LEFT JOIN races r      ON i.race_id = r.id
        LEFT JOIN offices o    ON r.office_id = o.id
        LEFT JOIN elections e  ON i.election_id = e.id
            WHERE i.status = 'pending'
            ORDER BY i.id DESC LIMIT ?""",
        (limit,),
    )
    rows = []
    for r in cur.fetchall():
        rows.append({
            "id": r["id"], "town": r["municipality"], "kind": r["kind"],
            "party": r["party"], "office": r["office"], "district": r["district"],
            "candidate": r["candidate_name"] or r["candidate_text"],
            "parsed_votes": r["votes"], "already_on_file": r["old_votes"],
            "held_because": r["reason"],
        })
    return rows


def _apply_action(conn, act, user_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM intake_items WHERE id = ? AND status = 'pending'", (act.item_id,))
    item = cur.fetchone()
    if not item:
        return None

    if act.action == "reject":
        cur.execute("UPDATE intake_items SET status='rejected', reviewed_by=? WHERE id=?",
                    (user_id, act.item_id))
        return f"rejected {item['municipality']} {item['candidate_text'] or ''}".strip()

    votes = act.votes or item["votes"]
    if votes is None:
        return None
    race_id, candidate_id = item["race_id"], item["candidate_id"]

    if act.action == "add":
        votes = (item["old_votes"] or 0) + (act.votes or item["votes"] or 0)

    if act.action == "writein":
        name = (item["candidate_text"] or "").strip()
        if not (race_id and name):
            return None
        cur.execute("""SELECT r.election_id, e.party FROM races r
                       JOIN elections e ON r.election_id = e.id WHERE r.id = ?""", (race_id,))
        rr = cur.fetchone()
        if not rr:
            return None
        norm = normalize_name(name)
        cur.execute("SELECT id FROM candidates WHERE name_normalized = ? AND party IS ?",
                    (norm, rr["party"]))
        crow = cur.fetchone()
        if crow:
            candidate_id = crow["id"]
        else:
            cur.execute("INSERT INTO candidates (name, name_normalized, party) VALUES (?,?,?)",
                        (name, norm, rr["party"]))
            candidate_id = cur.lastrowid
        cur.execute("""INSERT OR IGNORE INTO race_candidates
                       (race_id, candidate_id, party, ballot_order, is_incumbent,
                        recruitment_candidate_id, recruitment_filing_id)
                       VALUES (?, ?, ?, 900, 0, NULL, -1)""",
                    (race_id, candidate_id, rr["party"]))

    if item["kind"] == "ballots":
        cur.execute("SELECT id FROM voter_registration WHERE election_id=? AND municipality=?",
                    (item["election_id"], item["municipality"]))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE voter_registration SET ballots_cast=? WHERE id=?", (votes, row["id"]))
        else:
            cur.execute("SELECT county FROM polling_places WHERE municipality=?",
                        (item["municipality"],))
            cr = cur.fetchone()
            cur.execute("""INSERT INTO voter_registration (election_id, county, municipality, ballots_cast)
                           VALUES (?,?,?,?)""",
                        (item["election_id"], (cr["county"] if cr else "") or "",
                         item["municipality"], votes))
    else:
        if not (race_id and candidate_id):
            return None
        cur.execute("SELECT votes FROM results WHERE race_id=? AND candidate_id=? AND municipality=?",
                    (race_id, candidate_id, item["municipality"]))
        old = cur.fetchone()
        if old:
            cur.execute("UPDATE results SET votes=? WHERE race_id=? AND candidate_id=? AND municipality=?",
                        (votes, race_id, candidate_id, item["municipality"]))
            log_audit(cur, user_id, race_id, item["municipality"], candidate_id,
                      "update", {"votes": old["votes"]}, {"votes": votes})
        else:
            cur.execute("INSERT INTO results (race_id, candidate_id, municipality, votes) VALUES (?,?,?,?)",
                        (race_id, candidate_id, item["municipality"], votes))
            log_audit(cur, user_id, race_id, item["municipality"], candidate_id,
                      "create", None, {"votes": votes})

    cur.execute("""UPDATE intake_items SET status='applied', votes=?, race_id=?, candidate_id=?,
                          reviewed_by=?, applied_at=CURRENT_TIMESTAMP WHERE id=?""",
                (votes, race_id, candidate_id, user_id, act.item_id))
    return f"{item['municipality']} {item['candidate_text'] or ''} = {votes:,}".strip()


def handle(conn, text):
    """Interpret one instruction and apply it. Returns the reply to send."""
    pending = pending_for_prompt(conn)
    if not pending:
        return "Nothing is waiting for review right now."

    resp = parser.client().messages.parse(
        model=config.MODEL,
        max_tokens=8000,
        system=SYSTEM,
        messages=[{"role": "user", "content":
                   f"PENDING ITEMS:\n{json.dumps(pending, indent=0)}\n\n"
                   f"OPERATOR INSTRUCTION:\n{text}"}],
        output_format=Plan,
    )
    plan = resp.parsed_output
    if not plan.understood or not plan.actions:
        return plan.reply or "I did not follow that - which town and race?"

    user_id = store.bot_user_id(conn)
    done = []
    for act in plan.actions:
        try:
            result = _apply_action(conn, act, user_id)
        except Exception:
            log.exception("could not apply %s", act)
            result = None
        if result:
            done.append(result)
    conn.commit()

    if not done:
        return "Nothing changed - those lines were already handled."
    body = "\n".join(f"- {d}" for d in done[:12])
    more = f"\n(+{len(done) - 12} more)" if len(done) > 12 else ""
    return f"{plan.reply}\n{body}{more}".strip()
