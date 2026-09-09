#!/bin/sh
# Election-night autopilot.
#
# Three things have to keep happening while returns come in, and none of them
# should wait on a person: the wire feed has to be re-pulled as towns finish
# counting, the review queue has to be re-judged against the current rules
# (an item keeps the reason it was given when it was parsed, so fixing a rule
# strands the old rows until something re-checks them), and any race that went
# out with one candidate published and the rest of its field held has to be
# completed - a race showing one name at 100% is worse than an absent race.
#
# Every pass is idempotent. Stop it by creating /tmp/stop-nightloop.
cd /opt/nh-election-results || exit 1
set -a
. ./intake.env
set +a
P=/opt/nh-results-intake-venv/bin/python

while [ ! -f /tmp/stop-nightloop ]; do
  echo "=== pass $(date -u +%H:%M:%S) ==="
  $P scripts/import_feed.py --apply 2>&1 | tail -4
  $P scripts/drain_queue.py --apply 2>&1 | tail -2
  $P scripts/fix_partial_races.py --apply 2>&1 | tail -2
  sqlite3 nh_elections.db "SELECT 'towns=' || (SELECT count(DISTINCT municipality) FROM results r JOIN races ra ON ra.id = r.race_id WHERE ra.election_id IN (29,30)) || ' rows=' || (SELECT count(*) FROM results r JOIN races ra ON ra.id = r.race_id WHERE ra.election_id IN (29,30)) || ' pending=' || (SELECT count(*) FROM intake_items WHERE status = 'pending');"
  sleep 90
done
