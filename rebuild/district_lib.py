"""Parser for one-tab-per-district SoS files (Congress, Exec Council, State Senate).
Each tab = one district: header candidates, town rows, a TOTALS row (district total)."""
import glob, re, sys, os
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import parse_lib as pl

def parse_district_tab(df):
    p = pl.parse_tab(df)
    if not p: return None
    hr, cands, is_sum = p
    towns={}; totals={}
    for r in range(hr+1, len(df)):
        m=df.iat[r,0]
        if not isinstance(m,str): continue
        t=pl.norm_town(m); low=t.lower().replace(' county','')
        if not t or 'summary' in low or 'correction' in low: continue
        if low.startswith('total'):
            for c,nm,pty in cands:
                try: totals[(nm,pty)]=totals.get((nm,pty),0)+int(float(df.iat[r,c]))
                except (ValueError,TypeError): pass
            continue
        if low in pl.COUNTIES and is_sum: continue
        for c,nm,pty in cands:
            try: v=int(float(df.iat[r,c]))
            except (ValueError,TypeError): continue
            towns.setdefault(t,{})[(nm,pty)]=v
    return cands, towns, totals

def rd(d):
    return (sum(v for (n,p),v in d.items() if p=='Republican'),
            sum(v for (n,p),v in d.items() if p=='Democratic'))

def parse_blocks(df):
    """Multi-district tab -> [(district:int, cands, towns{t:{(nm,pty):v}}, totals{(nm,pty):v})].
    Each block = candidate header row; district from nearest preceding 'District N' title."""
    hrows=[i for i in range(len(df))
           if any(isinstance(x,str) and re.search(r',\s*[rdl]\b',str(x),re.I) for x in df.iloc[i])]
    blocks=[]
    for hr in hrows:
        dist=None
        for r in range(hr,-1,-1):
            for c in range(df.shape[1]):
                v=df.iat[r,c]
                if isinstance(v,str):
                    m=re.search(r'district\s*(?:no\.?\s*)?(\d+)',v,re.I)
                    if m: dist=int(m.group(1)); break
            if dist is not None: break
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
        blocks.append((dist,cands,towns,totals))
    return blocks

def expand_tabname(sh):
    """'senate 12-15'->[12,13,14,15]; 'senate 10 and 11'->[10,11]; 'senate 3'->[3]."""
    nums=[int(x) for x in re.findall(r'\d+', sh)]
    if len(nums)==2 and ('-' in sh or 'and' not in sh.lower()) and nums[1]>nums[0]+1 and '-' in sh.replace(' ',''):
        return list(range(nums[0], nums[1]+1))
    return nums
