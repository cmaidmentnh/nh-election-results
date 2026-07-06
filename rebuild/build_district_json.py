"""Build per-district rebuild JSON for cleanly-structured district files
(Exec Council, single-block Congress, well-formed multi-block Senate years).
Uses parse_blocks; merges summary/short name variants by surname within district."""
import sys, os, json, re, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import district_lib as dl, parse_lib as pl
from collections import defaultdict
os.chdir(os.path.expanduser('~/Downloads'))
EID={2016:1,2018:4,2020:8,2022:13,2024:16}

def canon_map(pairs):
    byp=defaultdict(list)
    for nm,pty in pairs: byp[pty].append(nm)
    m={}
    for pty,names in byp.items():
        cl=[]
        for nm in names:
            sur=nm.split()[-1].upper().strip(',.') if nm.split() else nm.upper()
            for c in cl:
                if c['s']==sur: c['n'].append(nm); break
            else: cl.append({'s':sur,'n':[nm]})
        for c in cl:
            canon=max(c['n'], key=lambda s:len(s.split()))
            for nm in c['n']: m[(nm,pty)]=canon
    return m

# SPECS: (office_id, year, [files], expected_districts)
def senate_files(yr):
    return {2016:glob.glob('2016-ge-state-senate-district*.xls'),
            2018:['2018-ge-state-senate-district-1-8.xls','2018-ge-state-senate-district-9-24.xls'],
            2020:['2020-state-senate-district-1-11.xls','2020-state-senate-district-12-24.xls'],
            2022:['2022-ge-state-senate-district-1-24_1.xls']}[yr]
EC_F={2016:'2016-ge-executive-council-district-1-5.xls',2018:'2018-ge-executive-council-district-1-5.xls',
      2020:'2020-executive-council-district-1-5.xls',2022:'2022-executive-council-district-1-5_0.xls',
      2024:'2024-ge-executive-council-district-1-5.xls'}

def collect(office_id, year, files, tabfilter):
    """Return {district:int -> {'towns':{t:{(nm,pty):v}}, 'totals':{(nm,pty):v}}}, keeping FIRST clean block per district (town-sum==totals)."""
    out={}
    for f in files:
        if not os.path.exists(f): continue
        xl=pl.pd.ExcelFile(f)
        for sh in xl.sheet_names:
            if not tabfilter(sh): continue
            for dist,cands,towns,totals in dl.parse_blocks(xl.parse(sh,header=None)):
                if dist is None or not towns: continue
                tsR=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Republican')
                tsD=sum(v for tv in towns.values() for (n,p),v in tv.items() if p=='Democratic')
                fR,fD=dl.rd(totals)
                if (tsR,tsD)!=(fR,fD): continue   # only trust blocks that self-verify
                if dist in out: continue           # first clean block wins (guards appended junk)
                out[dist]={'towns':towns,'totals':totals}
    return out

jobs=[]
# Exec Council all years
for yr,f in EC_F.items():
    jobs.append((5,yr,[f],lambda sh:'council' in sh.lower() or 'exe' in sh.lower()))
# Senate clean years (skip 2024 messy)
for yr in (2016,2018,2020,2022):
    jobs.append((6,yr,senate_files(yr),lambda sh:'senate' in sh.lower()))

out=[]
for oid,yr,files,tf in jobs:
    dmap=collect(oid,yr,files,tf)
    for dist,dd in sorted(dmap.items()):
        towns=dd['towns']
        cmap=canon_map({k for tv in towns.values() for k in tv})
        results=[]; cands={}
        for t,tv in towns.items():
            merged=defaultdict(int)
            for (nm,pty),v in tv.items(): merged[(cmap[(nm,pty)],pty)]+=v
            for (nm,pty),v in merged.items():
                cands[(nm,pty)]=True; results.append({'muni':t,'name':nm,'party':pty,'votes':v})
        R=sum(x['votes'] for x in results if x['party']=='Republican')
        D=sum(x['votes'] for x in results if x['party']=='Democratic')
        out.append({'office_id':oid,'year':yr,'election_id':EID[yr],'district':str(dist),
                    'candidates':[{'name':n,'party':p} for (n,p) in cands],'results':results,'R':R,'D':D})
    print(f"office {oid} {yr}: {len(dmap)} districts")
json.dump(out, open(sys.argv[1],'w'))
print("wrote",sys.argv[1],"-",len(out),"district-races")
