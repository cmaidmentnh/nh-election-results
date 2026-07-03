import sys, os, json
sys.path.insert(0, os.path.expanduser('~/nh-election-results/rebuild'))
import parse_lib as pl
os.chdir(os.path.expanduser('~/Downloads'))
ELECTION={2016:1,2018:4,2020:8,2022:13,2024:16}
# office_id, year, globs, note
JOBS=[
 (1,2016,['2016-ge-president-*.xls*'],''),
 (1,2020,['2020-president.xls'],''),
 (1,2024,['2024-ge-president.xls'],''),
 (2,2016,['2016-ge-us-senator.xls'],''),
 (2,2020,['2020-us-senator.xls'],''),
 (2,2022,['2022-ge-us-senator_5.xls'],'SoS summary page has a 2,922-vote internal quirk vs town detail; town detail stored'),
 (4,2016,['2016-ge-governor.xls'],'Coos town-detail missing from SoS file (9 counties present)'),
 (4,2018,['2018-ge-governor.xls'],''),
 (4,2020,['2020-governor.xls'],''),
 (4,2022,['2022-ge-governor_2.xls'],''),
 (4,2024,['2024-ge-governor_3.xls'],''),
]
from collections import defaultdict
def canon_map(pairs):
    """Merge same-person name variants (summary full name 'Kelly Ayotte' vs county
    short name 'Ayotte') by surname within party. Distinct surnames stay separate."""
    byp=defaultdict(list)
    for nm,pty in pairs: byp[pty].append(nm)
    m={}
    for pty,names in byp.items():
        clusters=[]
        for nm in names:
            sur=nm.split()[-1].upper().strip(',.') if nm.split() else nm.upper()
            for cl in clusters:
                if cl['sur']==sur: cl['names'].append(nm); break
            else: clusters.append({'sur':sur,'names':[nm]})
        for cl in clusters:
            canon=max(cl['names'], key=lambda s:len(s.split()))
            for nm in cl['names']: m[(nm,pty)]=canon
    return m

out=[]
for oid,yr,globs,note in JOBS:
    town=pl.parse_statewide(globs)
    cmap=canon_map({(nm,pty) for cv in town.values() for (nm,pty) in cv})
    cands={}
    results=[]
    for t,cv in town.items():
        merged=defaultdict(int)
        for (nm,pty),v in cv.items():
            merged[(cmap[(nm,pty)],pty)] += v
        for (nm,pty),v in merged.items():
            cands[(nm,pty)]=True
            results.append({'muni':t,'name':nm,'party':pty,'votes':v})
    R=sum(x['votes'] for x in results if x['party']=='Republican')
    D=sum(x['votes'] for x in results if x['party']=='Democratic')
    out.append({'office_id':oid,'year':yr,'election_id':ELECTION[yr],
                'candidates':[{'name':n,'party':p} for (n,p) in cands],
                'results':results,'R':R,'D':D,'ntowns':len(town),'note':note})
    print(f"office {oid} {yr}: {len(town)} towns, {len(results)} rows, R={R:,} D={D:,} {('['+note+']') if note else ''}")
json.dump(out, open(sys.argv[1],'w'))
print("wrote", sys.argv[1])
