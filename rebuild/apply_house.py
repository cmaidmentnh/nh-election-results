"""stdlib house applier: replace results for (office7,year,county,district) races
from JSON. Backs up DB, round-trip verifies each race's R/D == source."""
import sqlite3, sys, json, re, shutil, time, os
def norm(n): return re.sub(r'\s+',' ',re.sub(r'[^A-Z0-9 ]','',n.upper())).strip()
def apply(db, jpath, commit):
    if commit:
        bak=f"{db}.bak-house-{time.strftime('%Y%m%d-%H%M%S')}"; shutil.copy2(db,bak); print("backup:",os.path.basename(bak))
    data=json.load(open(jpath)); conn=sqlite3.connect(db); cur=conn.cursor()
    allok=True; changed=0; fails=[]
    for j in data:
        eid,cty,dist=j['election_id'],j['county'],str(j['district'])
        row=cur.execute("SELECT id FROM races WHERE office_id=7 AND election_id=? AND county=? AND CAST(district AS TEXT)=?",(eid,cty,dist)).fetchone()
        if not row:
            cur.execute("INSERT INTO races(election_id,office_id,district,county,seats,is_official) VALUES(?,7,?,?,?,1)",(eid,dist,cty,j.get('seats',1)))
            rid=cur.lastrowid
        else: rid=row[0]
        bR=cur.execute("SELECT COALESCE(SUM(res.votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Republican'",(rid,)).fetchone()[0]
        bD=cur.execute("SELECT COALESCE(SUM(res.votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Democratic'",(rid,)).fetchone()[0]
        cur.execute("DELETE FROM results WHERE race_id=?",(rid,))
        cid={}
        for c in j['candidates']:
            nn=norm(c['name'])
            r=cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND IFNULL(party,'')=?",(nn,c['party'])).fetchone()
            if r: cid[(c['name'],c['party'])]=r[0]
            else:
                cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",(c['name'],nn,c['party']))
                cid[(c['name'],c['party'])]=cur.lastrowid
        for res in j['results']:
            cur.execute("INSERT OR REPLACE INTO results(race_id,candidate_id,municipality,votes,votes_original) VALUES(?,?,?,?,?)",
                        (rid,cid[(res['name'],res['party'])],res['muni'],res['votes'],res['votes']))
        sR=cur.execute("SELECT COALESCE(SUM(res.votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Republican'",(rid,)).fetchone()[0]
        sD=cur.execute("SELECT COALESCE(SUM(res.votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Democratic'",(rid,)).fetchone()[0]
        ok=(sR==j['R'] and sD==j['D']); allok=allok and ok
        if (bR,bD)!=(sR,sD): changed+=1
        if not ok: fails.append(f"{j['year']} {cty} D{dist}: got {sR}/{sD} exp {j['R']}/{j['D']}")
    print(f"{len(data)} house races applied; {changed} changed; verify {'ALL OK' if allok else str(len(fails))+' FAIL'}")
    for f in fails[:10]: print("  FAIL",f)
    if commit and allok: conn.commit(); print("COMMITTED to",db)
    elif commit: print("NOT COMMITTED (failures)")
    conn.close(); return allok
if __name__=='__main__': apply(sys.argv[1],sys.argv[2],'--commit' in sys.argv)
