"""State Rep verification: per-county files, multi-block, multi-member.
Aggregate R/D per (county,district) town-sum vs file TOTALS vs DB."""
import sys, os, re, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import district_lib as dl, parse_lib as pl
import sqlite3
os.chdir(os.path.expanduser('~/Downloads'))
DB=sys.argv[1]; conn=sqlite3.connect(DB); cur=conn.cursor()
EID={2016:1,2018:4,2020:8,2022:13,2024:16}
COUNTIES=['belknap','carroll','cheshire','coos','grafton','hillsborough','merrimack','rockingham','strafford','sullivan']
def county_of(fn):
    fl=fn.lower()
    for c in COUNTIES:
        if c in fl: return c.capitalize()
    return None
def db_dist(eid):
    d={}
    for county,dist,pty,v in cur.execute("""SELECT r.county,r.district,c.party,SUM(res.votes) FROM races r
       JOIN results res ON res.race_id=r.id JOIN candidates c ON c.id=res.candidate_id
       WHERE r.office_id=7 AND r.election_id=? GROUP BY r.county,r.district,c.party""",(eid,)):
        d.setdefault((str(county),str(dist)),{})[pty]=v
    return d
def parse_house_blocks(df):
    """like parse_blocks but district title 'District No. N (seats)'."""
    hrows=[i for i in range(len(df)) if any(isinstance(x,str) and re.search(r',\s*[rdl]\b',str(x),re.I) for x in df.iloc[i])]
    out=[]
    for hr in hrows:
        dist=None
        for r in range(hr,-1,-1):
            v=df.iat[r,0]
            if isinstance(v,str):
                m=re.search(r'district\s*(?:no\.?\s*)?(\d+)',v,re.I)
                if m: dist=int(m.group(1)); break
        cands=[]
        for c in range(df.shape[1]):
            v=df.iat[hr,c]
            if isinstance(v,str) and ',' in v and v.strip().lower() not in pl.NONCAND:
                nm,pty=pl.split_party(v)
                if nm and nm.lower() not in pl.NONCAND: cands.append((c,nm,pl.pf(pty)))
        towns={}; totals={}
        for r in range(hr+1,len(df)):
            m=df.iat[r,0]
            if not isinstance(m,str): continue
            t=pl.norm_town(m); low=t.lower()
            if low.startswith('total'):
                for c,nm,pty in cands:
                    try: totals[(nm,pty)]=totals.get((nm,pty),0)+int(float(df.iat[r,c]))
                    except (ValueError,TypeError): pass
                break
            if 'summary' in low or 'correction' in low or 'district' in low or not t: continue
            for c,nm,pty in cands:
                try: v=int(float(df.iat[r,c]))
                except (ValueError,TypeError): continue
                towns.setdefault(t,{})[(nm,pty)]=v
        if dist is not None and towns: out.append((dist,towns,totals))
    return out

for yr in (2016,2018,2020,2022,2024):
    files=[f for f in glob.glob(f'{yr}*house*') if county_of(f) and f.endswith(('.xls','.xlsx'))]
    dbd=db_dist(EID[yr])
    src={}  # (county,dist)-> town-sum R/D and file R/D
    for f in files:
        cty=county_of(f)
        try: xl=pl.pd.ExcelFile(f)
        except: continue
        for sh in xl.sheet_names:
            for dist,towns,totals in parse_house_blocks(xl.parse(sh,header=None)):
                tsR=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Republican')
                tsD=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Democratic')
                fR,fD=dl.rd(totals)
                key=(cty,str(dist))
                if (tsR,tsD)!=(fR,fD): continue
                if key in src: continue
                src[key]=(tsR,tsD)
    matched=okdb=badf=nomatch=0; probs=[]
    for key,(tsR,tsD) in src.items():
        db=dbd.get(key)
        if db is None: nomatch+=1; continue
        matched+=1
        dR=db.get('Republican',0); dD=db.get('Democratic',0)
        if (tsR,tsD)==(dR,dD): okdb+=1
        else: probs.append(f"{key} town {tsR}/{tsD} DB {dR}/{dD}")
    print(f"HOUSE {yr}: {len(files)} files, {len(src)} clean src districts, {okdb}/{matched} match DB, {nomatch} src-not-in-DB, {len(probs)} DIFF")
    for p in probs[:12]: print("    DIFF",p)
