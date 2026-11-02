#!/bin/sh
# Election-night autopilot for the general (election 31). See nightloop.sh,
# which is the primary's: same passes, minus the primary wire feed, scoped to
# the general's election id.
#
# Results that land before polls close are stored and held ("Held until polls
# close"); nothing else re-judges them, so the first pass after 7pm is what
# publishes them. Every pass is idempotent. Stop it by creating
# /tmp/stop-nightloop.
cd /opt/nh-election-results || exit 1
set -a
. ./intake.env
set +a
P=/opt/nh-results-intake-venv/bin/python
E=31

while [ ! -f /tmp/stop-nightloop ]; do
  echo "=== pass $(date -u +%H:%M:%S) ==="
  $P scripts/drain_queue.py --apply 2>&1 | tail -2
  $P scripts/fix_partial_races.py --apply 2>&1 | tail -2
  $P scripts/purge_non_candidates.py --apply --elections $E 2>&1 | tail -1
  $P scripts/fix_stale_ballots.py --apply --elections $E 2>&1 | tail -1
  $P scripts/anchor_check.py --elections $E 2>&1 | tail -6
  sqlite3 nh_elections.db "SELECT 'towns=' || (SELECT count(DISTINCT municipality) FROM results r JOIN races ra ON ra.id = r.race_id WHERE ra.election_id = $E) || ' rows=' || (SELECT count(*) FROM results r JOIN races ra ON ra.id = r.race_id WHERE ra.election_id = $E) || ' pending=' || (SELECT count(*) FROM intake_items WHERE status = 'pending');"
  sleep 90
done
