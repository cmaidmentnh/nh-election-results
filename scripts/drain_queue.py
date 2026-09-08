#!/usr/bin/env python3
"""Re-judge every pending intake item against the current rules and publish
whatever now passes.

Items are held with the reason that applied at the moment they were parsed. Fix
a rule - the reconciliation convention, the polls-close gate, a missing race -
and the old rows keep their stale reason and sit there forever. This re-runs the
checks against today's code and today's database, using the parse already stored
on the message, so nothing is re-read and no tokens are spent.

Reconciliation is a per-message, per-race property, so it is recomputed once per
message from `intake_messages.parse_json` rather than per item.

    python3 scripts/drain_queue.py            # report only
    python3 scripts/drain_queue.py --apply
"""
import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake import store, roster              # noqa: E402
from intake.apply import reconcile, _existing_votes   # noqa: E402
from intake.parser import Extraction          # noqa: E402
from entry import log_audit                   # noqa: E402


def unreconciled(cur, cache, message_id, parse_json, municipality):
    """Race ids in this message whose totals still fail to add up."""
    if message_id in cache:
        return cache[message_id]
    bad = set()
    if parse_json and municipality:
        try:
            extraction = Extraction.model_validate_json(parse_json)
            _text, index, _elections = roster.roster_for(cur, municipality)
            bad = set(reconcile(extraction, index))
        except Exception as exc:            # a parse we can no longer read
            print(f"  message {message_id}: cannot re-check ({exc})")
    cache[message_id] = bad
    return bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = store.connect()
    cur = conn.cursor()
    bot = store.bot_user_id(conn)

    cur.execute("""SELECT i.*, m.parse_json
                     FROM intake_items i
                     JOIN intake_messages m ON m.id = i.message_id
                    WHERE i.status = 'pending' AND i.kind = 'result'
                    ORDER BY i.message_id, i.id""")
    pending = [dict(r) for r in cur.fetchall()]

    cache, ready, held = {}, [], []
    # One town can report the same race twice (two tapes, or five parser reads
    # that each phrased the race differently). Publish the first, hold the rest.
    seen = set()
    for item in pending:
        if not (item["race_id"] and item["candidate_id"]
                and item["votes"] is not None and item["municipality"]):
            held.append((item, "no race or candidate match"))
            continue
        key = (item["race_id"], item["candidate_id"], item["municipality"])
        if key in seen:
            held.append((item, "duplicate of another pending item"))
            continue
        if item["race_id"] in unreconciled(cur, cache, item["message_id"],
                                           item["parse_json"], item["municipality"]):
            held.append((item, "still does not reconcile"))
            continue
        on_file = _existing_votes(cur, item["race_id"], item["candidate_id"],
                                  item["municipality"])
        if on_file is not None and on_file != item["votes"]:
            held.append((item, f"conflicts with {on_file} already on file"))
            continue
        seen.add(key)
        ready.append(item)

    print(f"ready to publish: {len(ready)}    still held: {len(held)}")
    for cause, n in Counter(c for _, c in held).most_common():
        print(f"    {n:4d}  {cause}")

    if not args.apply:
        print("(report only - pass --apply to publish)")
        return

    for item in ready:
        if _existing_votes(cur, item["race_id"], item["candidate_id"],
                           item["municipality"]) is None:
            cur.execute("""INSERT INTO results (race_id, candidate_id, municipality, votes)
                           VALUES (?,?,?,?)""",
                        (item["race_id"], item["candidate_id"],
                         item["municipality"], item["votes"]))
            log_audit(cur, bot, item["race_id"], item["municipality"],
                      item["candidate_id"], "create", None, {"votes": item["votes"]})
        cur.execute("""UPDATE intake_items
                          SET status = 'applied', applied_at = CURRENT_TIMESTAMP
                        WHERE id = ?""", (item["id"],))
    conn.commit()
    print(f"published {len(ready)}")


if __name__ == "__main__":
    main()
