-- County Commissioner is elected county-wide, not by the district.
--
-- municipality_districts was built from commissioner_districts.json, which is a
-- RESIDENCY map: which district a town lies in, and therefore who may run. This
-- table, though, is what entry.place_races() joins on to decide what a town
-- VOTES ON, and for county commissioner the two are different questions. A
-- candidate must live in the district; the whole county then elects all three
-- seats. One row per town understated every ballot in the state by two races.
--
-- The Return of Votes settles it. Goshen's official Republican return prints
-- "For County Commissioner, 1st District - Joe Osgood 53, Undervotes 21" AND
-- "2nd District - Bennie Nelson 53, Undervotes 21" against 74 Republican
-- ballots cast: 53 + 21 = 74 in BOTH races. Every ballot is accounted for in
-- each one, which cannot happen in a race the town does not vote in. Ossipee's
-- Republican ballot and Chatham's Democratic ballot likewise print all three
-- Carroll districts, each "Vote for not more than 1", and Chatham's carries a
-- write-in for Gene Chandler in the 3rd - a district Chatham does not live in.
--
-- On primary night this held 76 real commissioner lines from ten towns as
-- "Race not identified on this town's ballot": the roster handed to the parser
-- showed one commissioner race where the ballot has three.
--
-- Backfill only - INSERT OR IGNORE against the UNIQUE key, so it is safe to run
-- as often as you like and never touches a row that is already right.
-- build_municipality_districts.py now generates these directly, so a clean
-- rebuild produces the same map without this file.

INSERT OR IGNORE INTO municipality_districts
       (municipality, office_id, county, district, redistricting_cycle, source)
SELECT own.municipality, own.office_id, own.county, other.district,
       own.redistricting_cycle, 'commissioner-json(county-wide)'
  FROM municipality_districts own
  JOIN (SELECT DISTINCT office_id, county, district, redistricting_cycle
          FROM municipality_districts
         WHERE office_id = (SELECT id FROM offices WHERE name = 'County Commissioner')
       ) other
    ON other.office_id          = own.office_id
   AND other.county             = own.county
   AND other.redistricting_cycle = own.redistricting_cycle
 WHERE own.office_id = (SELECT id FROM offices WHERE name = 'County Commissioner');
