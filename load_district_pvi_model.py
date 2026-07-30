#!/usr/bin/env python3
"""
Load the modelled 2026 district PVI into nh_elections.db.

Source: data/district_pvi_model.csv, produced by the PVI model (national-
referenced presidential lean in log-odds, pooled D-ward trend projected to
2026, plus a shrunk ticket-splitting term). Backtested at ~1.3 points of
mean absolute error on held-out cycles.

Columns
  pvi_national   district R share of the two-party presidential vote if the
                 nation split 50-50, minus 50. Same scale as the recruitment
                 database's districts.pvi column.
  nh_margin      predicted State House margin at an even statewide House vote.
                 This is what the rating is cut on, because NH targeting turns
                 on beating the state, not the nation.
  rating         SAFE GOP / LIKELY GOP / LEAN GOP / SWING / LEAN DEM /
                 LIKELY DEM / SAFE DEM, cut at +/-12, +/-6, +/-2 on nh_margin.
  pred_house_pct predicted R share of the two-party State Rep vote at an even
                 statewide House vote.

Idempotent: drops and rebuilds the table. Safe to re-run.
"""
import csv
import os
import sqlite3
import sys

DB = os.environ.get("ELECTIONS_DB", "nh_elections.db")
CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "data", "district_pvi_model.csv")


def main():
    if not os.path.exists(CSV):
        sys.exit(f"missing {CSV}")
    rows = list(csv.DictReader(open(CSV)))
    if not rows:
        sys.exit("no rows in csv")

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS district_pvi_model")
    cur.execute("""
        CREATE TABLE district_pvi_model (
            district_code   TEXT PRIMARY KEY,
            pvi_national    REAL,
            nh_margin       REAL,
            rating          TEXT,
            pred_house_pct  REAL
        )
    """)
    cur.executemany(
        "INSERT INTO district_pvi_model VALUES (?,?,?,?,?)",
        [(r["district_code"], float(r["pvi_national"]), float(r["nh_margin"]),
          r["rating"], float(r["pred_house_pct"])) for r in rows])
    conn.commit()

    n = cur.execute("SELECT COUNT(*) FROM district_pvi_model").fetchone()[0]
    print(f"loaded {n} districts into district_pvi_model ({DB})")
    for rating, c in cur.execute(
            "SELECT rating, COUNT(*) FROM district_pvi_model "
            "GROUP BY rating ORDER BY MIN(nh_margin) DESC"):
        print(f"   {rating:<11} {c:>3}")
    conn.close()


if __name__ == "__main__":
    main()
