"""Authoritative statewide verification: parsed town-sum vs the file's OWN
summary-tab statewide TOTALS row, AND vs the sum of per-county TOTALS rows.
The file's summary TOTALS is the SoS certification -- no hardcoded numbers."""
import sys, os, re, glob
import pandas as pd
sys.path.insert(0, os.path.expanduser('~/nh-election-results/rebuild'))
import parse_lib as pl
os.chdir(os.path.expanduser('~/Downloads'))

def cand_cols(df, hr):
    out=[]
    for c in range(df.shape[1]):
        v=df.iat[hr,c]
        if isinstance(v,str) and ',' in v and v.strip().lower() not in pl.NONCAND:
            nm,pty=pl.split_party(v)
            if nm and nm.lower() not in pl.NONCAND:
                out.append((c,nm,pl.pf(pty)))
    return out

def file_totals(globs):
    """Return (statewide_totals_by_party, county_totals_sum_by_party, ncounty_tabs, note)."""
    statewide=None; county_sum={}; ncty=0; notes=[]
    for f in pl.files_for(globs):
        try: xl=pd.ExcelFile(f)
        except Exception: continue
        for sh in xl.sheet_names:
            df=xl.parse(sh, header=None)
            p=pl.parse_tab(df)
            if not p: continue
            hr,cands,is_sum=p
            cols=cand_cols(df,hr)
            # all TOTALS rows in this tab, in order
            trows=[r for r in range(hr+1,len(df))
                   if isinstance(df.iat[r,0],str) and df.iat[r,0].strip().lower().startswith('total')]
            def rowvals(r):
                d={}
                for c,nm,pty in cols:
                    try: d[pty]=d.get(pty,0)+int(float(df.iat[r,c]))
                    except (ValueError,TypeError): pass
                return d
            if is_sum:
                # summary tab: FIRST totals row = statewide; last = this tab's own county (belknap)
                if trows:
                    statewide=rowvals(trows[0])
                if len(trows)>=2:  # belknap county total
                    for k,v in rowvals(trows[-1]).items(): county_sum[k]=county_sum.get(k,0)+v
                    ncty+=1
            else:
                # county tab(s): each TOTALS row is a county total (strafford+sullivan has 2)
                for r in trows:
                    for k,v in rowvals(r).items(): county_sum[k]=county_sum.get(k,0)+v
                    ncty+=1
    return statewide, county_sum, ncty

JOBS=[
 ('President 2016',['2016-ge-president-*.xls*']),
 ('President 2020',['2020-president.xls']),
 ('President 2024',['2024-ge-president.xls']),
 ('US Senator 2016',['2016-ge-us-senator.xls']),
 ('US Senator 2020',['2020-us-senator.xls']),
 ('US Senator 2022',['2022-ge-us-senator_5.xls']),
 ('Governor 2016',['2016-ge-governor.xls']),
 ('Governor 2018',['2018-ge-governor.xls']),
 ('Governor 2020',['2020-governor.xls']),
 ('Governor 2022',['2022-ge-governor_2.xls']),
 ('Governor 2024',['2024-ge-governor.xls']),
]
print(f"{'RACE':18}{'parsed R/D':20}{'summary-TOTALS R/D':22}{'countySum R/D':20}{'#cty':5}{'verdict'}")
for name,globs in JOBS:
    town=pl.parse_statewide(globs)
    pR=sum(v for tv in town.values() for (n,p),v in tv.items() if p=='Republican')
    pD=sum(v for tv in town.values() for (n,p),v in tv.items() if p=='Democratic')
    sw,cs,ncty=file_totals(globs)
    sR=sw.get('Republican') if sw else None; sD=sw.get('Democratic') if sw else None
    cR=cs.get('Republican'); cD=cs.get('Democratic')
    match_sw = sw and pR==sR and pD==sD
    match_cs = pR==cR and pD==cD
    v = 'PERFECT' if (match_sw and match_cs) else ('sw-ok' if match_sw else ('cty-ok' if match_cs else 'MISMATCH'))
    print(f"{name:18}{f'{pR:,}/{pD:,}':20}{(f'{sR:,}/{sD:,}' if sw else 'n/a'):22}{f'{cR:,}/{cD:,}':20}{ncty:<5}{v}")
