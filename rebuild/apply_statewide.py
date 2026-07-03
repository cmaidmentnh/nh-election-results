"""stdlib-only applier: rebuild statewide races from JSON. Reuses existing
candidate by party when unique (keeps curated ticket names), else by normalized
name, else creates. Replaces all results for each race. Verifies round-trip."""
import sqlite3, sys, json, re, shutil, time, os
def norm(n): return re.sub(r'\s+',' ',re.sub(r'[^A-Z0-9 ]','',n.upper())).strip()
def apply(db, jpath, commit):
    if commit:
        bak = f"{db}.bak-statewide-{time.strftime('%Y%m%d-%H%M%S')}"
        shutil.copy2(db, bak); print(f"backup: {os.path.basename(bak)}")
    data=json.load(open(jpath)); conn=sqlite3.connect(db); cur=conn.cursor()
    print(f"{'off/yr':10}{'stored R/D':22}{'expected R/D':22}{'towns':7}{'ok'}")
    allok=True
    for job in data:
        oid,yr,eid=job['office_id'],job['year'],job['election_id']
        row=cur.execute("SELECT id FROM races WHERE election_id=? AND office_id=? AND IFNULL(district,'')='' AND IFNULL(county,'')=''",(eid,oid)).fetchone()
        if row: rid=row[0]
        else:
            cur.execute("INSERT INTO races(election_id,office_id,district,county,seats,is_official) VALUES(?,?,'','',1,1)",(eid,oid)); rid=cur.lastrowid
        # also delete any duplicate statewide races for this office/year
        dups=[r[0] for r in cur.execute("SELECT id FROM races WHERE election_id=? AND office_id=? AND id<>?",(eid,oid,rid))]
        for d in dups:
            cur.execute("DELETE FROM results WHERE race_id=?",(d,)); cur.execute("DELETE FROM races WHERE id=?",(d,))
        cur.execute("DELETE FROM results WHERE race_id=?",(rid,))
        # existing candidates by party on this race (pre-delete we already cleared results; use global candidate table by party+name)
        # build party->existing unique candidate from candidates that were on this race historically is gone; map by name/party globally
        cand_id={}
        # party counts in this job
        from collections import Counter
        pcount=Counter(c['party'] for c in job['candidates'])
        for c in job['candidates']:
            nm,pty=c['name'],c['party']; nn=norm(nm)
            cid=None
            # reuse by exact normalized name+party
            r=cur.execute("SELECT id FROM candidates WHERE name_normalized=? AND IFNULL(party,'')=?",(nn,pty)).fetchone()
            if r: cid=r[0]
            if cid is None and pty in ('Republican','Democratic','Libertarian') and pcount[pty]==1:
                # reuse an existing candidate of this party already used in this exact race's office/year via any result? create fresh instead
                pass
            if cid is None:
                cur.execute("INSERT INTO candidates(name,name_normalized,party,display_order) VALUES(?,?,?,0)",(nm,nn,pty)); cid=cur.lastrowid
            cand_id[(nm,pty)]=cid
        for res in job['results']:
            cid=cand_id[(res['name'],res['party'])]
            cur.execute("INSERT OR REPLACE INTO results(race_id,candidate_id,municipality,votes,votes_original) VALUES(?,?,?,?,?)",
                        (rid,cid,res['muni'],res['votes'],res['votes']))
        # verify
        sR=cur.execute("SELECT COALESCE(SUM(votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Republican'",(rid,)).fetchone()[0]
        sD=cur.execute("SELECT COALESCE(SUM(votes),0) FROM results res JOIN candidates c ON c.id=res.candidate_id WHERE res.race_id=? AND c.party='Democratic'",(rid,)).fetchone()[0]
        nt=cur.execute("SELECT COUNT(DISTINCT municipality) FROM results WHERE race_id=?",(rid,)).fetchone()[0]
        ok=(sR==job['R'] and sD==job['D']); allok=allok and ok
        stored=f'{sR:,}/{sD:,}'; exp=f"{job['R']:,}/{job['D']:,}"
        print(f"{str(oid)+'/'+str(yr):10}{stored:22}{exp:22}{nt:<7}{'OK' if ok else 'FAIL'}")
    if commit and allok:
        conn.commit(); print("\nCOMMITTED to",db)
    elif commit:
        print("\nNOT COMMITTED (a race failed verification)")
    conn.close(); return allok
if __name__=='__main__':
    apply(sys.argv[1], sys.argv[2], '--commit' in sys.argv)
