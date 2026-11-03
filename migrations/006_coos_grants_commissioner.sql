-- Official 2026 SoS general ballots: these Coos grants vote for County Commissioner District 3 (Ray Gorman).
INSERT OR IGNORE INTO municipality_districts (municipality, office_id, county, district, redistricting_cycle, source)
SELECT p.municipality, o.id, 'Coos', '3', '2022-2030', 'sos-ballot-2026'
  FROM polling_places p, offices o
 WHERE o.name = 'County Commissioner'
   AND p.municipality IN ('Chandlers Purchase','Crawfords Purchase','Cutts Grant','Dixs Grant','Ervings Location',
        'Greens Grant','Hadleys Purchase','Low and Burbanks Grant','Martins Location','Odell','Pinkhams Grant',
        'Sargents Purchase','Second College Grant','Success','Wentworths Location','Thompson and Meserves Purchase');
