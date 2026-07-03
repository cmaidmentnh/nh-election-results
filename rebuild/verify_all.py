import glob, re, warnings, sys
import pandas as pd
import parse_lib as pl
warnings.simplefilter("ignore")

def dedupe_files(globs):
    fs = pl.files_for(globs)
    # drop " (N)" download duplicates, keep one per logical name
    seen, out = set(), []
    for f in fs:
        base = re.sub(r' \(\d+\)', '', f)
        if base in seen: continue
        seen.add(base); out.append(f)
    return out

def verify(name, globs):
    files = dedupe_files(globs)
    tab_ok = tab_bad = 0; badlist=[]; counties=set(); tabs=0
    seen_tabs=set()
    for f in files:
        try: xl = pd.ExcelFile(f)
        except Exception: continue
        for sh in xl.sheet_names:
            if sh in seen_tabs: continue
            seen_tabs.add(sh)
            df = xl.parse(sh, header=None)
            p = pl.parse_tab(df)
            if not p: continue
            hr, cands, is_sum = p
            if is_sum: continue    # skip summary tabs for the per-tab town check
            tabs+=1
            for cty in pl.COUNTIES:
                if cty[:5] in sh.lower().replace(' ',''): counties.add(cty)
            townR=townD=0; totR=totD=None
            for r in range(hr+1,len(df)):
                m=df.iat[r,0]
                if not isinstance(m,str):continue
                t=pl.norm_town(m); low=t.lower().replace(' county','')
                if not t or 'summary' in low or 'correction' in low:continue
                def val(c):
                    try:return int(float(df.iat[r,c]))
                    except:return 0
                if low.startswith('total'):
                    totR=sum(val(c) for c,n,pt in cands if pt=='Republican'); totD=sum(val(c) for c,n,pt in cands if pt=='Democratic')
                else:
                    townR+=sum(val(c) for c,n,pt in cands if pt=='Republican'); townD+=sum(val(c) for c,n,pt in cands if pt=='Democratic')
            if totR is None:  # no TOTALS row in tab
                continue
            if abs(townR-totR)<=3 and abs(townD-totD)<=3: tab_ok+=1
            else: tab_bad+=1; badlist.append(f"{sh}(town {townR}/{townD} vs TOTALS {totR}/{totD})")
    miss = sorted(pl.COUNTIES - counties)
    status = "OK" if (tab_bad==0 and not miss) else "CHECK"
    print(f"  [{status}] {name:18} tabs={tabs} pass={tab_ok} fail={tab_bad}"
          + (f"  MISSING COUNTIES: {miss}" if miss else "")
          + (f"  MISMATCH: {badlist[:2]}" if badlist else ""))

JOBS = [
 ('President 2016', ['2016-ge-president-*.xls*']),
 ('President 2020', ['2020-president.xls','2020-ge-president*.xls*']),
 ('President 2024', ['2024-ge-president.xls']),
 ('Governor 2016', ['2016-ge-governor.xls']),
 ('Governor 2018', ['2018-ge-governor.xls']),
 ('Governor 2020', ['2020-governor.xls']),
 ('Governor 2022', ['2022-ge-governor_2.xls']),
 ('Governor 2024', ['2024-ge-governor.xls']),
 ('US Senate 2016', ['2016-ge-us-senator.xls']),
 ('US Senate 2020', ['2020-ge-us-senator*.xls*','2020-us-senator*.xls*']),
 ('US Senate 2022', ['2022-ge-us-senator_5.xls']),
 ('US House 2016', ['2016-ge-congress*.xls*']),
 ('US House 2018', ['2018-ge-congress*.xls*']),
 ('US House 2020', ['2020-ge-congress*.xls*']),
 ('US House 2022', ['2022-ge-congress*.xls*']),
 ('US House 2024', ['2024-ge-congress*.xls*']),
 ('Exec Council 2016', ['2016-ge-executive-council*.xls*']),
 ('Exec Council 2024', ['2024-ge-executive-council*.xls*']),
 ('State Senate 2016', ['2016-ge-state-senate*.xls*']),
 ('State Senate 2024', ['2024-ge-state-senate*.xls*']),
 ('State House 2016', ['2016-ge-house-*.xls*']),
 ('State House 2024', ['2024-ge-house-*.xls*']),
]
import os
os.chdir(os.path.expanduser('~/Downloads'))
for name, globs in JOBS:
    verify(name, globs)
