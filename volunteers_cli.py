#!/usr/bin/env python3
"""Bulk emails to approved election-night volunteers.

DRY RUN by default: prints who would get it and sends nothing.

    python3 volunteers_cli.py instructions            # dry run
    python3 volunteers_cli.py instructions --send     # step-by-step email, once per volunteer
    python3 volunteers_cli.py reminder --send         # election-morning reminder, once per volunteer
    python3 volunteers_cli.py links --send            # anyone approved whose link never went out
    python3 volunteers_cli.py instructions --send --only someone@example.com   # test one

Each kind is sent at most once per volunteer (tracked in the volunteers table);
--force re-sends.
"""

import argparse
import os
import sys
import time
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

import volunteers as V  # noqa: E402

KINDS = {
    'instructions': 'instructions_sent_at',
    'reminder': 'reminder_sent_at',
    'links': 'link_sent_at',
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('kind', choices=sorted(KINDS))
    ap.add_argument('--send', action='store_true')
    ap.add_argument('--force', action='store_true', help='send even if already sent')
    ap.add_argument('--only', help='just this email address')
    args = ap.parse_args()

    col = KINDS[args.kind]
    conn = V.get_db()
    V.ensure_schema(conn)
    cur = conn.cursor()
    sql = f"SELECT * FROM volunteers WHERE status = 'approved'"
    params = []
    if not args.force:
        sql += f" AND {col} IS NULL"
    if args.only:
        sql += " AND email = ?"
        params.append(args.only.lower())
    cur.execute(sql + " ORDER BY id", params)
    vols = cur.fetchall()

    todo = []
    for v in vols:
        places = V.assignments_for(cur, v['id'])
        if not places:
            print(f"  skip {v['email']}: no polling place")
            continue
        todo.append((v, places))

    print(f"{args.kind}: {len(todo)} recipient(s){'' if args.send else ' (DRY RUN - add --send)'}")
    for v, places in todo:
        print(f"  {v['name']} <{v['email']}> - {', '.join(p['municipality'] for p in places)}")
    if not args.send:
        return

    ok = bad = 0
    for v, places in todo:
        if args.kind == 'links':
            sent = V.email_approved(v, places)
        else:
            sent = V.email_instructions(v, places, reminder=(args.kind == 'reminder'))
        if sent:
            cur.execute(f"UPDATE volunteers SET {col} = ? WHERE id = ?",
                        (datetime.now().isoformat(timespec='seconds'), v['id']))
            conn.commit()
            ok += 1
        else:
            bad += 1
            print(f"  FAILED {v['email']}")
        time.sleep(0.1)  # stay well under the SES send rate
    print(f"sent {ok}, failed {bad}")


if __name__ == '__main__':
    sys.exit(main())
