"""Surgical: add ONLY genuinely-missing candidates to specific hand-verified
house races (DB dropped a real candidate). Does not touch existing DB data."""
import sqlite3, sys, json, re, shutil, time, os
def norm(n): return re.sub(r'\s+',' ',re.sub(r'[^A-Z0-9 ]','',n.upper())).strip()
# hand-verified races where DB is missing a candidate present in the clean source
TARGETS=[(2016,'Rockingham','31'),(2018,'Rockingham','31')]
def run(db, jpath, commit):
    if commit:
        bak=f"{db}.bak-housepatch-{time.strftime('%Y%m%d-%H%M%S')}"; shutil.copy2(db,bak); print("backup:",os.path.basename(bak))
    src={(j['year'],j['county'],str(j['district'])):j for j in json.load(open(jpath))}
    conn=sqlite3.connect(db); cur=conn.cursor()
    for yr,cty,dist in TARGETS:
        j=src.get((yr,cty,dist))
        if not j: print("no source for",yr,cty,dist); continue
        rid=cur.execute("SELECT id FROM races WHERE office_id=7 AND election_id=? AND county=? AND CAST(district AS TEXT)=?",(j['election_id'],cty,dist)).fetchone()[0]
        db_names={r[0] for r in cur.execute("SELECT DISTINCT c.name_normalized FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=?",(rid,))}
        added=[]
        for c in j['candidates']:
            nn=norm(c['name'])
            if nn in db_names: continue  # already present -> leave DB as-is
            row=cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND IFNULL(party,'')=?",(nn,c['party'])).fetchone()
            cid=row[0] if row else None
            if cid is None:
                cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",(c['name'],nn,c['party'])); cid=cur.lastrowid
            tot=0
            for res in j['results']:
                if res['name']==c['name'] and res['party']==c['party']:
                    cur.execute("INSERT OR IGNORE INTO results(race_id,candidate_id,municipality,votes,votes_original) VALUES(?,?,?,?,?)",
                                (rid,cid,res['muni'],res['votes'],res['votes'])); tot+=res['votes']
            added.append(f"{c['name']} ({c['party']}) +{tot}")
        print(f"{yr} {cty} D{dist} race {rid}: added {added if added else 'nothing (all present)'}")
    if commit: conn.commit(); print("COMMITTED")
    conn.close()
if __name__=='__main__': run(sys.argv[1],sys.argv[2],'--commit' in sys.argv)
