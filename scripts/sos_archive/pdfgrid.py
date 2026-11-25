"""Rebuild the rows/columns of an SoS results PDF from word positions. Values go to
columns by x-position (a blank cell stays blank); candidate names go to columns in
order. A table runs across page breaks until its TOTALS line, and is checked
against it."""
import re,pdfplumber
NUMT=re.compile(r'^(\d[\d,]*|-+)$')
PARTYTOK=re.compile(r"^(r|d|i|l|g|c|lib|ind|con|grn|[rdi]&[rdi]|[rdi]/[rdi]),?$")   # lowercase only: "L." is a middle initial
COUNTIES=['Belknap','Carroll','Cheshire','Coos','Grafton','Hillsborough','Merrimack','Rockingham','Strafford','Sullivan']
DISTRE=re.compile(r'Dist\w*ct\s*(?:No\.?)?\s*(\d+)\s*(?:\((\d+)\))?',re.I)
TITLE=re.compile(r'GENERAL ELECTION|NEW HAMPSHIRE|November|^Page|STATE REPRESENTATIVES|GOVERNOR|UNITED STATES|PRESIDENT|EXECUTIVE COUNCIL|CONGRESSIONAL|State Senate|COUNTY OFFICES',re.I)
def lines_of(page):
    words=page.extract_words(x_tolerance=3)
    L=[]
    for w in sorted(words,key=lambda w:(round(w['top']),w['x0'])):
        if L and abs(L[-1][0]-w['top'])<=2.5: L[-1][1].append(w)
        else: L.append([w['top'],[w]])
    out=[]
    for _,ws in L:
        ws=sorted(ws,key=lambda w:w['x0']);m=[]
        for w in ws:
            # the PDF stores some numbers as touching pieces ("1" ",701"): join them
            if m and re.match(r'^[\d,]+$',w['text']) and re.match(r'^[\d,]+$',m[-1]['text']) and w['x0']-m[-1]['x1']<1.0:
                m[-1]=dict(m[-1],text=m[-1]['text']+w['text'],x1=w['x1'])
            else: m.append(dict(w))
        out.append(m)
    return out
def split_label(ws):
    i=0
    while i<len(ws):
        t=ws[i]['text']
        if NUMT.match(t) and not (i>0 and ws[i-1]['text'].lower() in('ward','wd','no.','district')): break
        i+=1
    return ws[:i],ws[i:]
def num(t): return None if t.startswith('-') else int(t.replace(',',''))
def cluster(xs,tol=14):
    cl=[]
    for x in sorted(xs):
        if cl and x-cl[-1][-1]<=tol: cl[-1].append(x)
        else: cl.append([x])
    return [sum(c)/len(c) for c in cl]
def stacks(hlines):
    """header words grouped into vertical stacks: words that overlap horizontally across lines,
    or sit within a few points of each other on one line, name the same candidate"""
    items=[]
    for li,ws in enumerate(hlines):
        for w in ws: items.append([w['x0'],w['x1'],[(li,w['x0'],w['text'])]])
    items.sort()
    out=[]
    for it in items:
        if out:
            p=out[-1]
            same_line_close=any(l==it[2][0][0] for l,_,_ in p[2]) and it[0]-p[1]<=3.5
            if it[0]<p[1]-0.5 or same_line_close:
                p[1]=max(p[1],it[1]);p[2]+=it[2];continue
        out.append(it)
    return [' '.join(t for _,_,t in sorted(s[2])) for s in out]
def anchor_chunks(hlines):
    """split header lines into chunks (gap > 4pt); an anchor chunk ends in a party code or Scatter.
    Non-anchor lines also break where an anchor chunk starts, so two names that touch split apart."""
    def is_anchor(ch):
        last=ch[-1]['text']
        return bool(PARTYTOK.match(last)) or last.lower() in('scatter','recount','others','other') or bool(re.search(r',\s*(r|d|lib|ind)$',last,re.I))
    def split(ws,starts=()):
        out=[];cur=[]
        for w in ws:
            if cur and (w['x0']-cur[-1]['x1']>4 or any(abs(w['x0']-s)<=2 for s in starts)): out.append(cur);cur=[]
            cur.append(w)
        if cur: out.append(cur)
        return out
    first=[(li,ch) for li,ws in enumerate(hlines) for ch in split(ws)]
    anchors=[c for c in first if is_anchor(c[1])]
    starts=[ch[0]['x0'] for _,ch in anchors]
    chunks=[]
    for li,ws in enumerate(hlines):
        if any(l==li for l,_ in anchors): chunks+=[c for c in first if c[0]==li]
        else: chunks+=[(li,ch) for ch in split(ws,starts)]
    return anchors,chunks
SUFFIX=re.compile(r'^(Jr|Sr|II|III|IV)\.?,?$')
PARTYW=re.compile(r'^(r|d|i|l|g|c|lib|ind|und|con|grn|mod|[rdi]&[rdi]|[rdi]/[rdi]),?$')
def text_names(tokens):
    """House headers: candidate names in reading order. A name ends at its party code
    ('ind mod' is one), at 'Recount'/'Scatter', or at a surname printed with its party cut off."""
    out=[];cur=[]
    for i,t in enumerate(tokens):
        nxt=tokens[i+1] if i+1<len(tokens) else None
        if t.lower() in('w/i','w-in','(w-in)') and cur:
            out.append(' '.join(cur)+' w-in');cur=[];continue
        if t in('Recount','Scatter','Others','Other') and not cur:
            out.append(t);continue
        cur.append(t)
        if PARTYW.match(t) and not (t=='ind' and nxt=='mod'):
            out.append(' '.join(cur));cur=[];continue
        if t.endswith(',') and not SUFFIX.match(t) and nxt is not None and (nxt in('Recount','Scatter') or (nxt[:1].isupper() and not SUFFIX.match(nxt) and len(cur)>=1 and not (len(cur)==1 and nxt.endswith(',') is False and not PARTYW.match(tokens[i+2] if i+2<len(tokens) else '')))):
            # "Ingbretson," then a new name: the party fell off this one
            if nxt in('Recount','Scatter') or nxt.endswith(','):
                out.append(' '.join(cur));cur=[];continue
    if cur: out.append(' '.join(cur))
    return out
def group_names(hlines,ncol):
    """candidate names, left to right, from header lines (the bottom line carries the parties)"""
    words=[w for ws in hlines for w in ws]
    if not words: return None
    anchors,chunks=anchor_chunks(hlines)
    if ncol and len(anchors)==ncol:
        cen=lambda ch:(ch[0]['x0']+ch[-1]['x1'])/2
        rng=lambda ch:(ch[0]['x0'],ch[-1]['x1'])
        anchors.sort(key=lambda c:cen(c[1]))
        parts=[[c] for c in anchors]
        for c in chunks:
            if any(c is a for a in anchors): continue
            a0,a1=rng(c[1])
            ov=[max(0,min(a1,rng(a[1])[1])-max(a0,rng(a[1])[0])) for a in anchors]
            k=max(range(len(anchors)),key=lambda k:ov[k]) if max(ov)>0 else min(range(len(anchors)),key=lambda k:abs(cen(c[1])-cen(anchors[k][1])))
            parts[k].append(c)
        return [' '.join(w['text'] for _,ch in sorted(p,key=lambda c:(c[0],c[1][0]['x0'])) for w in ch) for p in parts]
    bottom=max(hlines,key=lambda ws:sum(1 for w in ws if PARTYTOK.match(w['text']) or w['text'].lower() in('scatter','recount')))
    groups=[];cur=[]
    for w in bottom:
        cur.append(w)
        if PARTYTOK.match(w['text']) or w['text'].lower() in('scatter','recount','others','other') or re.search(r',\s*(r|d|lib)$',w['text'],re.I):
            groups.append(cur);cur=[]
    if cur: groups.append(cur)
    if ncol and len(groups)!=ncol:
        # fall back to splitting the bottom line at its ncol-1 widest gaps
        gaps=sorted(range(1,len(bottom)),key=lambda i:bottom[i]['x0']-bottom[i-1]['x1'],reverse=True)[:ncol-1]
        if len(bottom)<ncol: return None
        cuts=sorted(gaps);groups=[];prev=0
        for c in cuts+[len(bottom)]: groups.append(bottom[prev:c]);prev=c
    # first-name lines above: split each into phrases at wide gaps, attach each phrase to the nearest group
    extra=[[] for _ in groups]
    for ws in hlines:
        if ws is bottom: continue
        ph=[]
        for w in ws:
            if ph and w['x0']-ph[-1][-1]['x1']<=8: ph[-1].append(w)
            else: ph.append([w])
        for p in ph:
            cx=(p[0]['x0']+p[-1]['x1'])/2
            k=min(range(len(groups)),key=lambda k:abs(cx-(groups[k][0]['x0']+groups[k][-1]['x1'])/2))
            extra[k]+=[w['text'] for w in p]
    return [' '.join(extra[k]+[w['text'] for w in g]) for k,g in enumerate(groups)]
def parse_pdf(path,office,county=None,district=None,seats=1,rep=False):
    recs=[];problems=[];st={'d':district,'s':seats,'c':county,'names':None,'cols':None,'rows':[],'tot':None,'rep':rep}
    def close():
        if st['names'] and st['rows']:
            names=st['names'];rows=st['rows']
            if st['tot']:
                for j in range(len(names)):
                    s=sum((r[1].get(j) or 0) for r in rows)
                    if st['tot'].get(j) is not None and s!=st['tot'][j]: problems.append((st['d'],names[j],'sum',s,'total',st['tot'][j]))
            for lab,v in rows:
                final={}
                for j,nm in enumerate(names):
                    if nm.lower()=='recount' and j>0:
                        if v.get(j) is not None: final[j-1]=v[j]
                    elif j not in final: final[j]=v.get(j)
                for j,val in final.items():
                    if val is not None:
                        recs.append({'office':office,'county':lab[1],'district':st['d'],'seats':st['s'],'municipality':lab[0],'candidate':names[j],'votes':val})
        st['rows']=[];st['tot']=None
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            hdr=[]
            for ws in lines_of(page):
                ws=[w for w in ws if not re.match(r'^[+-]\d+$',w['text'])]   # '473 -1' correction notes
                lab,vals=split_label(ws)
                text=' '.join(w['text'] for w in ws)
                if re.match(r'^Ballot Law Commission',text,re.I): continue   # district-total override line, not a town
                isdata=lab and vals and all(NUMT.match(v['text']) for v in vals) and not TITLE.search(text)
                if not lab and vals and all(NUMT.match(v['text']) for v in vals) and hdr:
                    # numbers with no town: the town was printed at the start of the header line above
                    # ("Manchester Ward 5   Rokas, d   Recount ...")
                    hl=hdr[-1];k=next((i for i,w in enumerate(hl) if w['text'].endswith(',')),None)
                    if k and k>=1:
                        lab=hl[:k-1];hdr[-1]=hl[k-1:];isdata=True
                        ws=lab+ws
                if not isdata:
                    if st.get('buf'): flush(st,problems,close)   # finish the table above before a new heading
                    m=DISTRE.search(text)
                    m0=re.search(r'Dist\w*ct\s*No\.?\s*\((\d+)\)',text,re.I) if rep and not m else None
                    if m0:   # "District No. (2)": the number fell off the sheet; it is the next district
                        close();st['names']=None;st['d']=str(int(st['d'] or 0)+1);st['s']=int(m0.group(1))
                    if m and (rep or re.search(r'senate|council|congress',text,re.I)) and (rep or str(int(m.group(1)))!=st['d'] or not st['names']):
                        # House: every 'District No.' line opens a new table; elsewhere only a new district does
                        # (Council and Congress repeat their heading on every page)
                        close();st['names']=None
                        st['d']=str(int(m.group(1)))
                        if rep and m.group(2): st['s']=int(m.group(2))
                    cm=[c for c in COUNTIES if re.search(rf'\b{c}\s+County\b',text,re.I)]
                    if cm: st['c']=cm[0]
                    # keep only candidate words: drop titles, district/county captions
                    keep=[w for w in ws if not TITLE.search(text) or re.search(r',\s*[a-z]',text)]
                    keep=[w for w in keep if not re.match(r'^(District|No\.?|\(\d+\)|\d+|County|November|Page)$',w['text'],re.I) and w['text'] not in COUNTIES and not TITLE.search(w['text'])]
                    if keep: hdr.append(keep)
                    continue
                # a data line
                if hdr:
                    # header lines seen since the last data line describe the table that starts here
                    cols=None
                    st['pending_hdr']=hdr;hdr=[]
                labtxt=re.sub(r'\*+','',' '.join(w['text'] for w in lab)).strip()
                st.setdefault('buf',[]).append((labtxt,[(v['x0'],v['x1'],v['text']) for v in vals]))
                if labtxt.upper().startswith('TOTAL'):
                    flush(st,problems,close)
            flush(st,problems,close)
    flush(st,problems,close);close()
    return recs,problems
def flush(st,problems,close):
    buf=st.pop('buf',[]);hdr=st.pop('pending_hdr',None)
    if not buf: return
    xs=[x1 for _,vals in buf for x0,x1,t in vals if not t.startswith('-')]
    cols=cluster(xs)
    want=len(st['names']) if st['names'] else None
    if hdr:
        na=len(anchor_chunks(hdr)[0])
        if na and na<len(cols) and not st.get('rep'): want=na
    if want and len(cols)>want:
        # some rows sit a few points off the others: merge the closest neighbouring columns
        cnt={c:sum(1 for x in xs if abs(x-c)<=14) for c in cols}
        cols=[[c,cnt[c]] for c in cols]
        while len(cols)>want:
            i=min(range(1,len(cols)),key=lambda i:cols[i][0]-cols[i-1][0])
            a,b2=cols[i-1],cols[i];n=a[1]+b2[1]
            cols[i-1:i+1]=[[(a[0]*a[1]+b2[0]*b2[1])/max(n,1),n]]
        cols=[c for c,_ in cols]
    if hdr and st['names'] and st.get('rep') and any(PARTYW.match(w['text'].split(',')[-1] or 'x') or w['text']=='Scatter' for ws in hdr for w in ws):
        # House: a new candidate header (even with no TOTALS line before it) starts a new group
        close();st['names']=None;st['cols']=None
    if hdr and not st['names']:     # a repeated page header inside a running table changes nothing
        if st.get('rep'):
            def toks_of(ws):
                out=[]
                for w in ws:
                    m=re.match(r'^(.*,)([a-z]{1,3})$',w['text'])      # "Tilton,d" / "Sr.,r"
                    if m: out+=[dict(w,text=m.group(1),x1=w['x0']+(w['x1']-w['x0'])*.8),dict(w,text=m.group(2),x0=w['x1']-4)]
                    else: out.append(w)
                return [w for w in out if not re.match(r'^(Manchester|Nashua|Concord|Lebanon|Ward|\d+(\.\d)?)$',w['text'])]
            lines=[toks_of(ws) for ws in hdr]
            main=max(lines,key=len)
            names=text_names([w['text'] for w in main])
            # where each name sits, to place words wrapped onto other header lines
            spans=[];i=0
            for n in names:
                k=len(n.replace(' w-in','').split());ws=main[i:i+k];i+=k
                spans.append((ws[0]['x0'],ws[-1]['x1']) if ws else (0,0))
            for ln in lines:
                if ln is main: continue
                for w in ln:
                    cx=(w['x0']+w['x1'])/2
                    j=min(range(len(names)),key=lambda j:0 if spans[j][0]-3<=cx<=spans[j][1]+12 else min(abs(cx-spans[j][0]),abs(cx-spans[j][1])))
                    if PARTYW.match(w['text']): names[j]=names[j]+' '+w['text']
                    else: names[j]=w['text']+' '+names[j]
            def party_of(n):
                m=re.search(r'(?:,\s*|\s)([a-z]{1,3}(?:\s+mod)?)$',n);return m.group(1) if m and PARTYW.match(m.group(1).split()[0]) else None
            for j,n in enumerate(names):
                if n.endswith(','):
                    order=sorted([k for k in range(len(names)) if k!=j and names[k] not in('Scatter','Recount')],key=lambda k:(abs(k-j),-k))
                    nb=[party_of(names[k]) for k in order]
                    nb=[p for p in nb if p]
                    if nb: names[j]=n+' '+nb[0]
            if names and len(names)==len(cols)+1 and names[-1]=='Scatter': names=names[:-1]
        else:
            names=group_names(hdr,len(cols))
        if names and len(names)==len(cols):
            # a repeated page header keeps the table going; only a new heading (district) starts a new one
            if not st['names'] or len(st['names'])!=len(names): st['names']=names
        elif names:
            problems.append((st['d'],'|'.join(names),'header/column count',len(names),'cols',len(cols)))
    if not st['names']: return
    if len(cols)!=len(st['names']):
        # a page whose rows leave a column blank: place by the previous page's positions
        if st.get('cols') and len(st['cols'])==len(st['names']): cols=st['cols']
        else:
            problems.append((st['d'],'|'.join(st['names']),'columns',len(cols),'names',len(st['names'])));return
    st['cols']=cols
    for lab,vals in buf:
        v={}
        for x0,x1,t in vals:
            j=min(range(len(cols)),key=lambda k:abs(cols[k]-x1))
            v[j]=num(t)
        if lab.upper().startswith('TOTAL'):
            st['tot']=v;close()
            if st.get('rep'): st['names']=None;st['cols']=None
        elif lab.lower()=='recount' and st['rows']: st['rows'][-1]=(st['rows'][-1][0],v)
        else: st['rows'].append(((lab,st['c']),v))
