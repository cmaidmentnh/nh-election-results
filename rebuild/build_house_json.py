"""Full State Rep rebuild from per-county SoS files. Parses every base+floterial
district block (multi-member), captures ALL candidates + per-town votes, self-verifies
town-sum==file TOTALS. district key=(county,number). Floterials share numbering."""
import sys, os, re, glob, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parse_lib as pl, district_lib as dl
os.chdir(os.path.expanduser('~/Downloads'))
EID={2016:1,2018:4,2020:8,2022:13,2024:16}
COUNTIES=['belknap','carroll','cheshire','coos','grafton','hillsborough','merrimack','rockingham','strafford','sullivan']
def county_of(fn):
    fl=os.path.basename(fn).lower()
    for c in COUNTIES:
        if c in fl: return c.capitalize()
    return None

_TITLE=re.compile(r'^\s*district\s*(?:no\.?)?\s*\d', re.I)
def _hdr_cands(df, r):
    out=[]
    for c in range(1, df.shape[1]):
        v=df.iat[r,c]
        if isinstance(v,str) and ',' in v and v.strip().lower() not in pl.NONCAND:
            nm,pty=pl.split_party(v)
            if nm and nm.lower() not in pl.NONCAND: out.append((c,nm,pl.pf(pty)))
    return out

def house_blocks(df):
    """Title-delimited district blocks. A district spans from its 'District No. N'
    title row to the next title. Candidates are split across stacked sub-blocks
    (same towns repeated, different candidates); merge them. Accumulate TOTALS rows
    for self-verification. Small (1-town) districts may have no TOTALS row."""
    tr=[r for r in range(len(df)) if isinstance(df.iat[r,0],str) and _TITLE.match(df.iat[r,0])]
    tr.append(len(df))
    out=[]
    for i in range(len(tr)-1):
        r0,r1=tr[i],tr[i+1]
        title=df.iat[r0,0]
        dm=re.search(r'district\s*(?:no\.?\s*)?(\d+)',title,re.I)
        if not dm: continue
        dist=int(dm.group(1))
        ms=re.search(r'\((\d+)\)',title); seats=int(ms.group(1)) if ms else 1
        isfl='fl' in title.lower().replace(' ','')
        # fusion candidate (won both primaries): 'Name, d/r' — my party mapping is unreliable
        # for these; DB uses the real party, so exclude such districts from any rebuild.
        has_fusion=any(isinstance(df.iat[r,c],str) and re.search(r',\s*[dr]\s*/\s*[dr]\b',df.iat[r,c],re.I)
                       for r in range(r0,r1) for c in range(df.shape[1]))
        towns={}; totals={}; cur=None; last_town=None; had_recount=False
        for r in range(r0, r1):
            hc=_hdr_cands(df, r)
            if hc: cur=hc; last_town=None; continue
            m=df.iat[r,0]
            if not isinstance(m,str): continue
            t=pl.norm_town(m); low=t.lower()
            if low.startswith('total'):
                if cur:
                    for c,nm,pty in cur:
                        try: totals[(nm,pty)]=totals.get((nm,pty),0)+int(float(df.iat[r,c]))
                        except (ValueError,TypeError): pass
                continue
            if low=='recount' or low.startswith('recount'):
                had_recount=True
                # recount restatement of the previous town -> official count; replace, don't add
                if last_town is not None and cur:
                    for c,nm,pty in cur:
                        try: towns[last_town][(nm,pty)]=int(float(df.iat[r,c]))
                        except (ValueError,TypeError): pass
                continue
            if 'summary' in low or 'correction' in low or not t or _TITLE.match(m): continue
            if not cur: continue
            last_town=t
            for c,nm,pty in cur:
                try: v=int(float(df.iat[r,c]))
                except (ValueError,TypeError): continue
                towns.setdefault(t,{})[(nm,pty)]=v
        cands=[(None,n,p) for (n,p) in sorted({k for tv in towns.values() for k in tv})]
        out.append((dist,seats,isfl,cands,towns,totals,had_recount,has_fusion))
    return out

def build(years, filesfor):
    out=[]; report=[]
    for yr in years:
        got={}; rejected=[]; trust={}
        for f in filesfor(yr):
            cty=county_of(f)
            if not cty: continue
            try: xl=pl.pd.ExcelFile(f)
            except: continue
            for sh in xl.sheet_names:
                for dist,seats,isfl,cands,towns,totals,had_recount,has_fusion in house_blocks(xl.parse(sh,header=None)):
                    if not towns: continue
                    # self-verify town-sum==TOTALS, but recount districts legitimately
                    # differ from the election-day TOTALS row, so skip the check there
                    ok=True
                    if not had_recount:
                        for k in totals:
                            ts=sum(tv.get(k,0) for tv in towns.values())
                            if ts!=totals[k]: ok=False; break
                    key=(cty,str(dist))
                    if not ok:
                        rejected.append(key); trust[key]=False; continue
                    # trusted block: verified against a real TOTALS row, no recount column,
                    # no fusion (d/r) candidate -> my party mapping is unambiguous here
                    block_trusted = bool(totals) and (not had_recount) and (not has_fusion)
                    trust[key]=trust.get(key,True) and block_trusted
                    if key in got:
                        seats0,towns0=got[key]
                        for t,tv in towns.items():
                            towns0.setdefault(t,{}).update(tv)
                        got[key]=(max(seats0,seats),towns0)
                    else:
                        got[key]=(seats,dict((t,dict(tv)) for t,tv in towns.items()))
        for (cty,dist),(seats,towns) in got.items():
            cands={}; results=[]
            for t,tv in towns.items():
                for (nm,pty),v in tv.items():
                    cands[(nm,pty)]=True
                    results.append({'muni':t,'name':nm,'party':pty,'votes':v})
            R=sum(x['votes'] for x in results if x['party']=='Republican')
            D=sum(x['votes'] for x in results if x['party']=='Democratic')
            out.append({'office_id':7,'year':yr,'election_id':EID[yr],'county':cty,'district':dist,
                        'seats':seats,'candidates':[{'name':n,'party':p} for (n,p) in cands],
                        'results':results,'R':R,'D':D,'trusted':trust.get((cty,str(dist)),False)})
        report.append((yr,len(got),len(set(rejected))))
    return out, report

def files_2016_2022(yr):
    return [f for f in glob.glob(f'{yr}*house*') if county_of(f) and f.endswith(('.xls','.xlsx')) and 'senate' not in f.lower()]

if __name__=='__main__':
    out,report=build([2016,2018,2020,2022], files_2016_2022)
    for yr,n,rej in report: print(f"HOUSE {yr}: {n} districts parsed clean, {rej} rejected(town!=file)")
    json.dump(out, open(sys.argv[1],'w'))
    print("wrote",sys.argv[1],"-",len(out),"district-races")
