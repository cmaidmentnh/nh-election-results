import sys, os, re, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import district_lib as dl, parse_lib as pl
import sqlite3
os.chdir(os.path.expanduser('~/Downloads'))
DB=sys.argv[1]; conn=sqlite3.connect(DB); cur=conn.cursor()
EID={2016:1,2018:4,2020:8,2022:13,2024:16}
FILES={2016:glob.glob('2016-ge-state-senate-district*.xls'),
       2018:['2018-ge-state-senate-district-1-8.xls','2018-ge-state-senate-district-9-24.xls'],
       2020:['2020-state-senate-district-1-11.xls','2020-state-senate-district-12-24.xls'],
       2022:['2022-ge-state-senate-district-1-24_1.xls'],
       2024:['2024-ge-state-senate-district-1-24_0.xls']}
def db_dist(eid):
    d={}
    for dist,pty,v in cur.execute("""SELECT r.district,c.party,SUM(res.votes) FROM races r
       JOIN results res ON res.race_id=r.id JOIN candidates c ON c.id=res.candidate_id
       WHERE r.office_id=6 AND r.election_id=? GROUP BY r.district,c.party""",(eid,)):
        d.setdefault(str(dist),{})[pty]=v
    return d
problems=[]
for yr in sorted(FILES):
    dbd=db_dist(EID[yr]); seen={}
    for f in sorted(FILES[yr]):
        if not os.path.exists(f): print(f"{yr} MISSING {f}"); continue
        xl=pl.pd.ExcelFile(f)
        for sh in xl.sheet_names:
            if 'senate' not in sh.lower(): continue
            for dist,cands,towns,totals in dl.parse_blocks(xl.parse(sh,header=None)):
                if dist is None or not towns: continue
                tsR=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Republican')
                tsD=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Democratic')
                fR,fD=dl.rd(totals)
                seen[dist]=(tsR,tsD,fR,fD)
    for dist in sorted(seen):
        tsR,tsD,fR,fD=seen[dist]
        db=dbd.get(str(dist),{}); dR=db.get('Republican',0); dD=db.get('Democratic',0)
        okf=(tsR==fR and tsD==fD); okdb=(tsR==dR and tsD==dD)
        if not (okf and okdb):
            flag='file!=town' if not okf else 'DB-DIFF'
            problems.append(f"SEN {yr} D{dist}: town {tsR}/{tsD} file {fR}/{fD} DB {dR}/{dD} [{flag}]")
    print(f"SEN {yr}: {len(seen)} districts parsed, {sum(1 for d in seen if seen[d][0]==seen[d][2])}/{len(seen)} town==file")
print("\n=== PROBLEMS (town!=file or DB!=source) ===")
for p in problems: print(" ", p)
if not problems: print("  none — all senate districts match source AND DB")
