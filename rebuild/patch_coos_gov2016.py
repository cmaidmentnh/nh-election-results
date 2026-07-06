"""Governor 2016: the SoS file omits Coos town-level detail (confirmed absent from
every 2016 file). Add Coos's certified COUNTY total (from the file's own summary tab)
as a single clearly-labeled row so the statewide total is exact. Idempotent."""
import sqlite3, sys, re, shutil, time, os
def norm(n): return re.sub(r'\s+',' ',re.sub(r'[^A-Z0-9 ]','',n.upper())).strip()
MUNI="Coos County (no town detail)"
COOS=[("Chris Sununu","Republican",7424),("Colin Van Ostern","Democratic",7006),("Max Abramson","Libertarian",702)]
def run(db, commit):
    if commit:
        bak=f"{db}.bak-coos2016-{time.strftime('%Y%m%d-%H%M%S')}"; shutil.copy2(db,bak); print("backup:",os.path.basename(bak))
    conn=sqlite3.connect(db); cur=conn.cursor()
    rid=cur.execute("SELECT id FROM races WHERE office_id=4 AND election_id=1 AND IFNULL(district,'')='' AND IFNULL(county,'')=''").fetchone()[0]
    for nm,pty,v in COOS:
        row=cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND party=?",(norm(nm),pty)).fetchone()
        cid=row[0] if row else None
        if cid is None:
            cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",(nm,norm(nm),pty)); cid=cur.lastrowid
        cur.execute("INSERT OR REPLACE INTO results(race_id,candidate_id,municipality,votes,votes_original) VALUES(?,?,?,?,?)",(rid,cid,MUNI,v,v))
    R=cur.execute("SELECT SUM(votes) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Republican'",(rid,)).fetchone()[0]
    D=cur.execute("SELECT SUM(votes) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Democratic'",(rid,)).fetchone()[0]
    print(f"Governor 2016 statewide now R={R:,} D={D:,}  (certified 354,040 / 337,589) -> {'EXACT' if (R,D)==(354040,337589) else 'MISMATCH'}")
    if commit and (R,D)==(354040,337589): conn.commit(); print("COMMITTED")
    elif commit: print("NOT COMMITTED")
    conn.close()
if __name__=='__main__': run(sys.argv[1],'--commit' in sys.argv)
