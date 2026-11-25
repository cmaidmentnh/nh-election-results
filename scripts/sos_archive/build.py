"""Build data/sos_archive/general_<year>.json.gz from the downloaded SoS pages."""
import re,os,sys,json,gzip,glob,collections
UNL=[]
from parse import parse
from pdfgrid import parse_pdf
COUNTY_PREFIX={'belk':'Belknap','carr':'Carroll','ches':'Cheshire','coos':'Coos','graf':'Grafton','hill':'Hillsborough','merr':'Merrimack','rock':'Rockingham','stra':'Strafford','sull':'Sullivan'}
PARTY={'u':'Independent','rtc':'Republican','im':'Independent','r':'Republican','d':'Democratic','i':'Independent','ind':'Independent','und':'Independent','gr':'Green','ci':'Independent','ib':'Independent','l':'Libertarian','lib':'Libertarian','g':'Green','grn':'Green','c':'Constitution','con':'Constitution'}
ALIAS={"at. & gil ac. grant":"Atkinson & Gilmanton Academy Grant","atkinson and gilmanton ac. grant":"Atkinson & Gilmanton Academy Grant",
       "thomp. and mess purchase":"Thompson and Meserves Purchase","thompson & meserve's purchase":"Thompson and Meserves Purchase","thompson & meserves purchase":"Thompson and Meserves Purchase",
       "atkinson & gilmanton academy grant":"Atkinson & Gilmanton Academy Grant","at. & gil. academy grant":"Atkinson & Gilmanton Academy Grant",
       "atk. & gilm. ac. gt.":"Atkinson & Gilmanton Academy Grant","low & burbank's grant":"Low and Burbanks Grant","thompson & meserve's purchase":"Thompson and Meserves Purchase"}
def office_of(fn):
    f=fn.lower()
    if 'sum' in f or re.search(r'(names|ballots|preswins|concon|county|index|after)',f) or re.match(r'^(con)(belk|carr|ches|coos|graf|hill|merr|rock|stra|sull|sum)',f): return None
    if 'rep' in f and not 'pres' in f: return 'State Representative'
    if re.match(r'^(senate|sen)',f): return 'State Senator'
    if re.match(r'^(council|exe)',f): return 'Executive Councilor'
    if re.match(r'^(congress|con)\d',f): return 'Representative in Congress'
    if re.match(r'^(belk|carr|ches|coos|graf|hill|hills|merr|rock|stra|sull)\.htm',f): return None   # 1996 county-office pages
    if 'gov' in f: return 'Governor'
    if 'uss' in f: return 'United States Senator'
    if 'pres' in f: return 'President of the United States'
    return None
def county_of(fn):
    f=fn.lower()
    for k,v in COUNTY_PREFIX.items():
        if f.startswith(k): return v
    return None
def split_party(raw):
    raw=re.sub(r",\s*i\.m\.$",", im",raw.strip())         # "Hall, i.m." (independent moderate)
    raw=re.sub(r"\s*\(dec'?d\)\s*$","",raw.strip())      # "Carlson, r (dec'd)": died before election day
    raw=re.sub(r'\bind mod$','ind',raw.strip())    # "ind mod" (independent moderate) -> Independent
    m=re.match(r'^(.*?),\s*([rd])\s+([A-Z][a-z]+)$',raw.strip())   # "Frost, d Donald"
    if m: return f"{m.group(3)} {m.group(1)}",PARTY[m.group(2).lower()]
    m=re.search(r"(?:\s*,\s*|\s+)\(?([a-z]{1,3}(?:\s*[&/-]\s*[a-z]{1,3})?)\)?[.`'\s]*$",raw,re.I)
    if not m or m.group(1).lower().split('&')[0].split('/')[0].split('-')[0].strip() not in PARTY: return raw.strip(),''
    first=re.split(r'\s*[&/-]\s*',m.group(1).lower())[0]
    return raw[:m.start()].strip().rstrip(','),PARTY[first]
def clean_name(n):
    n=re.sub(r'\s*\((w-in|write-in)\)|\s+w-in$','',n,flags=re.I).strip()
    if ' and ' in n and re.search(r'\band\b',n): return n           # "Bush and Cheney"
    parts=[p.strip() for p in n.split(',')]
    if len(parts)>=2:
        sfx=[p for p in parts[1:] if re.match(r'^(jr|sr|ii|iii|iv)\.?$',p,re.I)]
        rest=[p for p in parts[1:] if p not in sfx]
        n=' '.join(rest+[parts[0]]+sfx)
    return re.sub(r'\s+',' ',n).strip()
def muni(m):
    m=re.sub(r'\*+|\s*\(.*?\)','',m).strip();m=re.sub(r'\s+',' ',m)
    m=re.sub(r'^((Republican|Democratic)\s+)+','',m)            # 2002 sheets: party caption run into the town cell
    m=re.sub(r'\bward\b','Ward',m);m=re.sub(r'\s+-\s+Ward\b',' Ward',m);m=re.sub(r'^BeansPurchase$','Beans Purchase',m)
    if m.lower() in ALIAS: return ALIAS[m.lower()]
    m=re.sub(r'\bWd\.?\s*(\d)',r'Ward \1',m)
    m=re.sub(r'\bLoc\.?$','Location',m);m=re.sub(r'\bGt\.?$','Grant',m);m=re.sub(r'\bPur\.?$','Purchase',m)
    m=m.replace('’','').replace("'",'')
    # one spelling for the two unincorporated places every sheet abbreviates differently
    k=re.sub(r'[^a-z]','',m.lower().replace('&','and'))
    if k.startswith(('atandgil','atkandgilm','atkinsonandgilmanton')): return 'Atkinson & Gilmanton Academy Grant'
    if k.startswith(('thompandmes','thompsonandmeserve')): return 'Thompson and Meserves Purchase'
    return m
def cms_meta(fn):
    """2012/2014 files are named <page>_<link text>_<id>: office, county, first district"""
    f=fn.lower()
    if 'recount' in f or 'winners' in f: return None,None,None
    for k,off in [('repgen','State Representative'),('state_representative','State Representative'),('sengen','State Senator'),('state_senate','State Senator'),
                  ('congen','Representative in Congress'),('representative_in_congress','Representative in Congress'),('exegen','Executive Councilor'),
                  ('executive_council','Executive Councilor'),('govgen','Governor'),('governor','Governor'),('united_states_senator','United States Senator'),
                  ('ussgen','United States Senator'),('presgen','President of the United States')]:
        if f.startswith(k) or f.startswith('2012'+k) or f.startswith('2014'+k):
            cty=None
            if off=='State Representative':
                m=re.search(r'house-([a-z]+)',f);cty=m.group(1).title() if m else None
            m=re.search(r'district[s]?_(\d+)',f);dist=m.group(1) if m and off in('State Senator','Representative in Congress','Executive Councilor') else None
            return off,cty,dist
    return None,None,None
def is_sheet(path):
    with open(path,'rb') as fh: h=fh.read(8)
    return h.startswith(b'PK') or h.startswith(b'\xd0\xcf\x11\xe0')
def build(year,folder):
    out=[];probs=[];unk=[];global UNL;UNL=[]
    cms=year>=2012
    for f in sorted(glob.glob(f'raw/{folder}/*')):
        fn=os.path.basename(f)
        if cms:
            off,cty_c,dist_c=cms_meta(fn)
            if not off or not is_sheet(f): unk.append(fn);continue   # spreadsheets only; the PDFs duplicate them
        else: off=office_of(fn)
        if not off:
            unk.append(fn);continue
        cty=(cty_c if cms else county_of(fn)) if off=='State Representative' else None
        dist=dist_c if cms else None
        m=None if cms else re.match(r'^(?:congress|con|council|exe)\D*(\d)',fn,re.I)
        if m and off in('Representative in Congress','Executive Councilor'): dist=m.group(1)
        m=None if cms else re.match(r'^(?:senate|sen)\D*(\d+)',fn,re.I)
        if m and off=='State Senator': dist=m.group(1)   # first district on the page; later headings move it on
        recs,p=(parse_pdf if fn.lower().endswith('.pdf') else parse)(f,off,cty,district=dist,rep=(off=='State Representative'))
        probs+=[(fn,)+tuple(x) for x in p if x[2]!='unlabeled column']
        unl={x[1] for x in p if x[2]=='unlabeled column'}
        if unl: UNL.append((fn,off,dist,sorted(unl)))
        for r in recs: r['_file']=fn
        for r in recs:
            raw=r['candidate']
            if re.match(r'^(scatt+er|scattering|others?|write-?ins?)$',raw,re.I): name,party='Write-Ins',''
            else:
                name,party=split_party(raw);name=clean_name(name)
            out.append({'office':off,'county':cty,'district':r['district'] if off not in('Governor','United States Senator','President of the United States') else None,
                        'seats':r['seats'],'municipality':muni(r['municipality']),'candidate':name,'party':party,'votes':r['votes'],'raw':raw,'_file':r['_file']})
    # fill unlabeled columns from a sibling page of the same race that names that column
    fixed=[]
    for fn,off,dist,cols in UNL:
        for col in cols:
            i=int(col[5:])
            hdr=collections.Counter()
            for g in sorted(glob.glob(f'raw/{folder}/*')):
                g2=os.path.basename(g)
                if g2==fn or office_of(g2)!=off: continue
                from grid import rows as _rows
                for row in _rows(g):
                    if len(row)>i and row[i] and not re.match(r'^[\d,]+$',row[i]) and re.search(r',\s*[a-z]',row[i]): hdr[row[i]]+=1;break
            if hdr and off in('Governor','United States Senator','President of the United States','Representative in Congress','Executive Councilor'):
                rawname=hdr.most_common(1)[0][0];name,party=split_party(rawname);name=clean_name(name)
                for r in out:
                    if r['_file']==fn and r['raw']==col: r['candidate'],r['party'],r['raw']=name,party,rawname
                fixed.append((fn,col,rawname))
            else:
                # a candidate whose name is blank on the sheet: keep the votes, say so in the name
                for r in out:
                    if r['_file']==fn and r['raw']==col: r['candidate'],r['party']='Unnamed candidate (blank on SoS sheet)',''
                probs.append((fn,dist,col,'UNLABELED COLUMN - kept as Unnamed candidate'))
    print('  filled unlabeled columns:',fixed)
    for r in out: r.pop('_file',None)
    # two people with the same surname and party in one race (Phyllis and George Katsakiores, Derry 2000):
    # the sheet prints surnames only, so the second column becomes "Name (2)"
    seen=collections.Counter()
    for r in out:
        k=(r['office'],r['county'],r['district'],r['candidate'].upper(),r['party'],r['municipality'])
        seen[k]+=1
        if seen[k]>1 and r['candidate']!='Write-Ins': r['candidate']=f"{r['candidate']} ({seen[k]})"
    # 'Scatter' and 'Others' columns on one sheet are both write-ins: one line per town
    merged={};final=[]
    for r in out:
        if r['candidate']=='Write-Ins':
            k=(r['office'],r['county'],r['district'],r['municipality'])
            if k in merged: merged[k]['votes']+=r['votes'];continue
            merged[k]=r
        final.append(r)
    return final,probs,unk
if __name__=='__main__':
    year=int(sys.argv[1]);folder=sys.argv[2]
    recs,probs,unk=build(year,folder)
    print(year,len(recs),'records; total-check problems',len(probs),'; skipped files',unk)
    for p in probs[:30]: print('  PROB',p)
    json.dump(recs,open(f'out_{year}.json','w'))
