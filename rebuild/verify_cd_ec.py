import sys, os, re, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import district_lib as dl, parse_lib as pl
import sqlite3
os.chdir(os.path.expanduser('~/Downloads'))
DB=sys.argv[1]
conn=sqlite3.connect(DB); cur=conn.cursor()
def db_dist(office_id, eid):
    d={}
    for dist,pty,v in cur.execute("""SELECT r.district,c.party,SUM(res.votes) FROM races r
        JOIN results res ON res.race_id=r.id JOIN candidates c ON c.id=res.candidate_id
        WHERE r.office_id=? AND r.election_id=? GROUP BY r.district,c.party""",(office_id,eid)):
        d.setdefault(str(dist),{})[pty]=v
    return d
EID={2016:1,2018:4,2020:8,2022:13,2024:16}
# office, year -> list of (district, globs)
CONG={2016:'2016-ge-congressional-district-%d.xlsx',2018:'2018-ge-congressional-district-%d.xlsx',
      2020:'2020-congressional-district-%d.xlsx',2022:'2022-ge-congressional-district-%d.xlsx',
      2024:{1:'2024-ge-congressional-district-1_3.xlsx',2:'2024-ge-congressional-district-2_4.xlsx'}}
EC={2016:'2016-ge-executive-council-district-1-5.xls',2018:'2018-ge-executive-council-district-1-5.xls',
    2020:'2020-executive-council-district-1-5.xls',2022:'2022-executive-council-district-1-5_0.xls',
    2024:'2024-ge-executive-council-district-1-5.xls'}

def check(label, office_id, year, srcmap):
    dbd=db_dist(office_id, EID[year])
    for dist in sorted(srcmap, key=lambda x:int(x)):
        cands,towns,totals=srcmap[dist]
        tR,tD=dl.rd({k:sum(tv.get(k,0) for tv in towns.values()) for k in set().union(*[t.keys() for t in towns.values()])}) if towns else (0,0)
        # town-sum
        tsR=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Republican')
        tsD=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Democratic')
        fR,fD=dl.rd(totals)
        db=dbd.get(str(dist),{}); dR=db.get('Republican',0); dD=db.get('Democratic',0)
        okf = (tsR==fR and tsD==fD)
        okdb= (tsR==dR and tsD==dD)
        flag = 'OK' if (okf and okdb) else ('file!=town' if not okf else 'DB-DIFF')
        print(f"{label} {year} D{dist:<3} town R/D={tsR:>7,}/{tsD:<7,} file={fR:>7,}/{fD:<7,} DB={dR:>7,}/{dD:<7,} {flag}")

for year,pat in CONG.items():
    src={}
    for d in (1,2):
        f = pat[d] if isinstance(pat,dict) else pat%d
        if not os.path.exists(f): print(f"CONG {year} D{d} MISSING {f}"); continue
        xl=pl.pd.ExcelFile(f); r=dl.parse_district_tab(xl.parse(xl.sheet_names[0],header=None))
        if r: src[str(d)]=r
    check('CONG',3,year,src)
print()
for year,f in EC.items():
    if not os.path.exists(f): print(f"EC {year} MISSING {f}"); continue
    xl=pl.pd.ExcelFile(f); src={}
    for sh in xl.sheet_names:
        m=re.search(r'(\d)', sh)
        if not m: continue
        r=dl.parse_district_tab(xl.parse(sh,header=None))
        if r: src[m.group(1)]=r
    check('EC  ',5,year,src)
