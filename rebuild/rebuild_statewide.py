import sqlite3, sys, os, re
sys.path.insert(0, os.path.expanduser('~/nh-election-results/rebuild'))
import parse_lib as pl
os.chdir(os.path.expanduser('~/Downloads'))

ELECTION = {'2016':1,'2018':4,'2020':8,'2022':13,'2024':16}
OFFICE = {'President':1,'US Senator':2,'Governor':4}
# (office, year, official-name-substr, file globs, certified R, certified D) — certified for verification
JOBS = [
 ('President','2016',['2016-ge-president-*.xls*'],345790,348526),
 ('President','2020',['2020-president.xls'],365660,424937),
 ('President','2024',['2024-ge-president.xls'],395492,418496),
 ('US Senator','2016',['2016-ge-us-senator.xls'],353632,354649),
 ('US Senator','2020',['2020-ge-us-senator*.xls*','2020-us-senator*.xls*'],325033,450825),
 ('US Senator','2022',['2022-ge-us-senator_5.xls'],314215,344092),
 ('Governor','2016',['2016-ge-governor.xls'],354040,336975),
 ('Governor','2018',['2018-ge-governor.xls'],302782,233981),
 ('Governor','2020',['2020-governor.xls'],533240,264639),
 ('Governor','2022',['2022-ge-governor_2.xls'],342969,254051),
 ('Governor','2024',['2024-ge-governor.xls'],436700,360241),
]

def norm_name(n): return re.sub(r'\s+',' ',re.sub(r'[^A-Z0-9 ]','',n.upper())).strip()

def main(db, commit):
    conn=sqlite3.connect(db); cur=conn.cursor()
    print(f"{'OFFICE':14}{'YR':6}{'parsed R/D':22}{'certified R/D':22}{'match'}")
    for off,yr,globs,cR,cD in JOBS:
        town=pl.parse_statewide([g if os.path.exists(g.split('*')[0]) or True else g for g in globs])
        R,D,n=pl.totals(town)
        ok = (abs(R-cR)<=50 and abs(D-cD)<=50)
        print(f"{off:14}{yr:6}{f'{R:,}/{D:,}':22}{f'{cR:,}/{cD:,}':22}{'EXACT' if (R==cR and D==cD) else ('~ok' if ok else 'OFF ('+str(R-cR)+'/'+str(D-cD)+')')}")
        if commit:
            eid=ELECTION[yr]; oid=OFFICE[off]
            old=[r[0] for r in cur.execute("SELECT id FROM races WHERE election_id=? AND office_id=?",(eid,oid))]
            for rid in old:
                cur.execute("DELETE FROM results WHERE race_id=?",(rid,)); cur.execute("DELETE FROM races WHERE id=?",(rid,))
            cur.execute("INSERT INTO races(election_id,office_id,district,county,seats,is_official) VALUES(?,?,'','',1,1)",(eid,oid))
            rid=cur.lastrowid
            candcache={}
            def getc(nm,pty):
                k=(norm_name(nm),pty)
                if k in candcache: return candcache[k]
                row=cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND IFNULL(party,'')=?",k).fetchone()
                cid=row[0] if row else None
                if cid is None:
                    cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",(nm,k[0],pty)); cid=cur.lastrowid
                candcache[k]=cid; return cid
            for t,cv in town.items():
                for (nm,pty),v in cv.items():
                    cur.execute("INSERT INTO results(race_id,candidate_id,municipality,votes,votes_original) VALUES(?,?,?,?,?)",(rid,getc(nm,pty),t,v,v))
    if commit: conn.commit(); print("\nCOMMITTED to",db)
    conn.close()

if __name__=='__main__':
    main(sys.argv[1], '--commit' in sys.argv)
