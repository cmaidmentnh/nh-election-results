"""stdlib district applier: replace results for specific (office,year,district) races
from JSON. Backs up DB, round-trip verifies each race == expected R/D."""
import sqlite3, sys, json, re, shutil, time, os
def norm(n): return re.sub(r'\s+',' ',re.sub(r'[^A-Z0-9 ]','',n.upper())).strip()
def apply(db, jpath, commit):
    if commit:
        bak=f"{db}.bak-district-{time.strftime('%Y%m%d-%H%M%S')}"; shutil.copy2(db,bak); print("backup:",os.path.basename(bak))
    data=json.load(open(jpath)); conn=sqlite3.connect(db); cur=conn.cursor()
    allok=True; nfix=0; fails=[]
    for job in data:
        oid,yr,eid,dist=job['office_id'],job['year'],job['election_id'],str(job['district'])
        row=cur.execute("SELECT id FROM races WHERE election_id=? AND office_id=? AND CAST(district AS TEXT)=?",(eid,oid,dist)).fetchone()
        if not row:
            row=cur.execute("INSERT INTO races(election_id,office_id,district,county,seats,is_official) VALUES(?,?,?,'',1,1)",(eid,oid,dist)) or cur.execute("SELECT last_insert_rowid()").fetchone()
        rid=row[0] if row else cur.lastrowid
        # capture before
        bR=cur.execute("SELECT COALESCE(SUM(votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Republican'",(rid,)).fetchone()[0]
        cur.execute("DELETE FROM results WHERE race_id=?",(rid,))
        cand_id={}
        for c in job['candidates']:
            nn=norm(c['name'])
            r=cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND IFNULL(party,'')=?",(nn,c['party'])).fetchone()
            if r: cid=r[0]
            else: cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",(c['name'],nn,c['party'])); cid=cur.lastrowid
            cand_id[(c['name'],c['party'])]=cid
        for res in job['results']:
            cur.execute("INSERT OR REPLACE INTO results(race_id,candidate_id,municipality,votes,votes_original) VALUES(?,?,?,?,?)",
                        (rid,cand_id[(res['name'],res['party'])],res['muni'],res['votes'],res['votes']))
        sR=cur.execute("SELECT COALESCE(SUM(votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Republican'",(rid,)).fetchone()[0]
        sD=cur.execute("SELECT COALESCE(SUM(votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Democratic'",(rid,)).fetchone()[0]
        ok=(sR==job['R'] and sD==job['D']); allok=allok and ok
        if not ok: fails.append(f"off{oid} {yr} D{dist}: got {sR}/{sD} exp {job['R']}/{job['D']}")
        if bR!=sR or True: pass
        if bR!=sR: nfix+=1
    print(f"{len(data)} district-races applied; {nfix} changed R-total; verify {'ALL OK' if allok else 'FAILURES:'}")
    for f in fails: print("  FAIL",f)
    if commit and allok: conn.commit(); print("COMMITTED to",db)
    elif commit: print("NOT COMMITTED (failures)")
    conn.close(); return allok
if __name__=='__main__': apply(sys.argv[1],sys.argv[2],'--commit' in sys.argv)
