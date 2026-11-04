#!/usr/bin/env python3
"""Mark a town as partly reported, or clear it once every polling place is in.

A town that votes at more than one place (Goffstown: the high school and
Bartlett Elementary) can send in some of them first. Its votes count, but the
pages say "1 of 2 polling places reporting", leave it out of towns reported
and weight it by the share of its places in.

  python3 scripts/mark_partial.py Goffstown 1 2 --note "Bartlett Elementary not yet reported"
  python3 scripts/mark_partial.py --clear Goffstown
  python3 scripts/mark_partial.py --list

Election defaults to 31 (2026 general). Pages pick it up within a minute.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import queries  # noqa: E402

# Same table app.py creates at startup.
TOWN_PARTIAL_SQL = """CREATE TABLE IF NOT EXISTS town_partial (
    election_id INTEGER NOT NULL, municipality TEXT NOT NULL,
    reported_places INTEGER NOT NULL, total_places INTEGER NOT NULL,
    note TEXT, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (election_id, municipality))"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("town", nargs="?")
    ap.add_argument("reported", nargs="?", type=int)
    ap.add_argument("total", nargs="?", type=int)
    ap.add_argument("--note", default="")
    ap.add_argument("--election", type=int, default=31)
    ap.add_argument("--clear", metavar="TOWN")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    conn = queries.get_connection()
    conn.execute(TOWN_PARTIAL_SQL)
    if a.clear:
        n = conn.execute("DELETE FROM town_partial WHERE election_id = ? AND municipality = ?",
                         (a.election, a.clear)).rowcount
        print(f"cleared {a.clear}" if n else f"{a.clear} was not marked partial")
    elif a.town:
        if a.reported is None or not a.total or a.reported >= a.total:
            sys.exit("need: <town> <reported> <total> with reported < total (use --clear when all are in)")
        conn.execute("""INSERT OR REPLACE INTO town_partial
                        (election_id, municipality, reported_places, total_places, note)
                        VALUES (?, ?, ?, ?, ?)""",
                     (a.election, a.town, a.reported, a.total, a.note))
        print(f"{a.town}: {a.reported} of {a.total} polling places reporting")
    conn.commit()
    for r in conn.execute("SELECT * FROM town_partial WHERE election_id = ?", (a.election,)):
        print(" ", dict(r))


if __name__ == "__main__":
    main()
