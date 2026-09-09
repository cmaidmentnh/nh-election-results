-- Two NH counties elect their county commissioners county-wide. Eight do not.
--
-- municipality_districts was built from commissioner_districts.json, which is a
-- RESIDENCY map: which district a town lies in, and therefore who may run. This
-- table, though, is what entry.place_races() joins on to decide what a town
-- VOTES ON, and for county commissioner the two questions come apart - but only
-- in some counties, so neither "one row per town" nor "every district for every
-- town" is right on its own.
--
-- The paper says it plainly, and says it both ways. Goshen's official Republican
-- Return of Votes prints "For County Commissioner, 1st District - Joe Osgood 53,
-- Undervotes 21" AND "2nd District - Bennie Nelson 53, Undervotes 21" against 74
-- Republican ballots cast: 53 + 21 = 74 in BOTH races, so every ballot is
-- accounted for in each one, which cannot happen in a race the town does not
-- vote in. Ossipee's Republican ballot and Chatham's Democratic ballot each
-- print all three Carroll districts, "Vote for not more than 1", and Chatham's
-- carries a write-in for Gene Chandler in the 3rd - a district Chatham is not
-- in. Against that, Rochester Ward 5's VotingWorks tape prints exactly ONE
-- commissioner race, headed "Wards 1, 5, 6", with 393 votes + 5 write-ins + 53
-- undervotes = its 451 ballots. Strafford elects by district; Rochester is even
-- split across two districts by ward.
--
-- So do not assume either way: derive it. The past two general elections are
-- already in this database, and a town with results in more than one
-- commissioner district of its county is a town that votes county-wide. For the
-- 2022-2030 cycle that selects Carroll and Sullivan, and nothing else.
--
-- On primary night the residency-only map held 76 real commissioner lines from
-- ten Carroll and Sullivan towns as "Race not identified on this town's ballot",
-- because the roster handed to the parser showed one commissioner race where the
-- ballot prints three.
--
-- Idempotent, and self-correcting: the DELETE removes county-wide rows for any
-- county the evidence does not support, so re-running after the evidence changes
-- converges rather than accumulating. Rows written by the residency map
-- (source 'commissioner-json') are never touched.

DELETE FROM municipality_districts
 WHERE office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
   AND source = 'commissioner-json(county-wide)'
   AND county NOT IN (
        SELECT county FROM (
            SELECT r.county AS county, res.municipality AS m,
                   COUNT(DISTINCT r.district) AS n
              FROM results res
              JOIN races r     ON r.id = res.race_id
              JOIN elections e ON e.id = r.election_id
             WHERE r.office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
               AND e.election_type = 'general' AND e.year >= 2022
             GROUP BY r.county, res.municipality)
         WHERE n > 1
         GROUP BY county);

INSERT OR IGNORE INTO municipality_districts
       (municipality, office_id, county, district, redistricting_cycle, source)
SELECT own.municipality, own.office_id, own.county, other.district,
       own.redistricting_cycle, 'commissioner-json(county-wide)'
  FROM municipality_districts own
  JOIN (SELECT DISTINCT office_id, county, district, redistricting_cycle
          FROM municipality_districts
         WHERE office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
       ) other
    ON other.office_id           = own.office_id
   AND other.county              = own.county
   AND other.redistricting_cycle = own.redistricting_cycle
 WHERE own.office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
   AND own.county IN (
        SELECT county FROM (
            SELECT r.county AS county, res.municipality AS m,
                   COUNT(DISTINCT r.district) AS n
              FROM results res
              JOIN races r     ON r.id = res.race_id
              JOIN elections e ON e.id = r.election_id
             WHERE r.office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
               AND e.election_type = 'general' AND e.year >= 2022
             GROUP BY r.county, res.municipality)
         WHERE n > 1
         GROUP BY county);
