-- Six city wards were on the wrong county commissioner ballot.
--
-- commissioner_districts.json is keyed by TOWN, and build_municipality_districts
-- looked cities up by stripping "Ward N" off the name. That works right up until
-- a city is split between commissioner districts - and three are:
--
--   Rochester Wards 1, 5, 6 -> Strafford 1   (mapped as 3)
--   Dover    Wards 5, 6     -> Strafford 3   (mapped as 2)
--   Laconia  Ward  2        -> Belknap  3    (mapped as 1)
--
-- Rochester Ward 5's own VotingWorks tape says so on its face: the race is
-- headed "Wards 1, 5, 6 - For County Commissioner", and 393 for John Frank
-- Scruton + 5 write-ins + 53 undervotes = the 451 ballots it cast. Scruton is a
-- Strafford 1st District candidate. On the wrong ballot his 393 was held as
-- "Candidate not on this race's roster", and the ward's write-in totals were
-- filed into the 3rd District race, which its voters never saw.
--
-- The fix does not need a new source: the 2022 and 2024 general returns already
-- record what each ward voted on, ward by ward, and that is the authority here.
-- build_municipality_districts.py now prefers those results over the town-level
-- JSON for exactly this reason.
--
-- Idempotent. Only rows the general returns contradict are touched; a place with
-- no commissioner result in this cycle keeps whatever the JSON gave it.

-- Correlated NOT EXISTS, not a row-value NOT IN: SQLite evaluates the latter
-- against every municipality's results at once, which deletes by coincidence
-- rather than by evidence.
DELETE FROM municipality_districts AS md
 WHERE md.office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
   AND EXISTS (SELECT 1
                 FROM results res
                 JOIN races r     ON r.id = res.race_id
                 JOIN elections e ON e.id = r.election_id
                WHERE r.office_id = md.office_id
                  AND e.election_type = 'general' AND e.year >= 2022
                  AND res.municipality = md.municipality)
   AND NOT EXISTS (SELECT 1
                     FROM results res
                     JOIN races r     ON r.id = res.race_id
                     JOIN elections e ON e.id = r.election_id
                    WHERE r.office_id = md.office_id
                      AND e.election_type = 'general' AND e.year >= 2022
                      AND res.municipality = md.municipality
                      AND r.county   = md.county
                      AND r.district = md.district);

INSERT OR IGNORE INTO municipality_districts
       (municipality, office_id, county, district, redistricting_cycle, source)
SELECT res.municipality,
       (SELECT id FROM offices WHERE name = 'County Commissioner'),
       r.county, r.district, '2022-2030', 'results-general'
  FROM results res
  JOIN races r     ON r.id = res.race_id
  JOIN elections e ON e.id = r.election_id
 WHERE r.office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
   AND e.election_type = 'general' AND e.year >= 2022
 GROUP BY res.municipality, r.county, r.district;
