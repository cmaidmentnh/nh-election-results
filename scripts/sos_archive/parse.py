"""Parse archived NH Secretary of State general-election result pages into
per-town candidate vote records, self-checking every block against its TOTALS row."""
import re,json,sys,collections
from grid import rows
NUM=re.compile(r'^-?\d[\d,]*$')
PARTY=re.compile(r"(?:,\s*|\s+)\(?([rdilgc]|lib|ind|con|grn|[rdi]\s*[&/-]\s*[rdi])\)?[.`'\s]*$|,\s*[rd]\s+[A-Z][a-z]+$",re.I)
NONCAND=re.compile(r'^(scatter|scattering|others?|write-?ins?|recount|blc recount|blc|undervotes?|overvotes?)$',re.I)
COUNTIES=['Belknap','Carroll','Cheshire','Coos','Grafton','Hillsborough','Merrimack','Rockingham','Strafford','Sullivan']
DIST=re.compile(r'Dist\w*ct\s*(?:No\.?)?\s*(\d*)\s*\((\d+)\)',re.I)
def is_name(c):
    c=re.sub(r'^Recount\s+\S.*$','Recount',c.strip())
    return bool(c) and not NUM.match(c) and (bool(PARTY.search(c)) or bool(NONCAND.match(c)) or 'w-in' in c.lower() or 'write-in' in c.lower())
def num(c): return int(c.replace(',','')) if NUM.match(c.strip()) else None
SIDE=re.compile(r'(?:SENATE|COUNCIL)\D{0,25}?DISTRICT\s*(?:NO\.?)?\s*\d+\s*$',re.I)
def unside(R):
    # one race printed as two side-by-side half-lists (1998 Council 2): the header repeats its names
    for k,r in enumerate(R):
        nm=[(i,x) for i,x in enumerate(r) if i>0 and is_name(x)]
        txt=[x for _,x in nm]
        if len(txt)>=2 and txt[0] in txt[1:]:
            j=[i for i,x in nm if x==txt[0]][1]-1
            left=[r2[:j] for r2 in R[k:]];rightrows=[[r2[j] if len(r2)>j else '']+r2[j+1:] for r2 in R[k:] if len(r2)>j+1]
            return R[:k]+left+[['']+r[j+1:]]+rightrows[1:]
    out=[];right=[];j=None
    for r in R:
        hits=[i for i,x in enumerate(r) if i>1 and SIDE.search(x) and any(is_name(y) or NUM.match(y) for y in r[1:i])]
        if hits: j=hits[0]
        if j and len(r)>j:
            out.append(r[:j]);right.append(r[j:])
        else:
            if right: out+=right;right=[]
            j=None if not hits else j
            out.append(r)
    return out+right
def parse(path,office,county=None,district=None,seats=1,rep=False):
    R=rows(path);recs=[];checks=[];problems=[]
    if not rep: R=unside(R)
    cols=None;block=[];total=None;cur_county=county;cur_dist=district;cur_seats=seats;lastnum=0;prev_noname=False
    def close():
        nonlocal block,total
        if cols and block:
            # self-check
            if total:
                for i,nm in cols.items():
                    if total.get(i) is None: continue
                    s=sum((r[2].get(i) or 0) for r in block if not r[3])
                    rec_rows=[r for r in block]
                    if s!=total[i]: problems.append((cur_dist,nm,'sum',s,'total',total[i]))
            for lab,cty,vals,superseded in block:
                if superseded: continue
                # Recount column: value in col i+1 named 'Recount' replaces col i
                final={}
                for i,nm in sorted(cols.items()):
                    if nm.lower() in('recount','blc recount','blc'):
                        j=max(k for k in cols if k<i and cols[k].lower() not in('recount','blc recount','blc'))
                        if vals.get(i) is not None: final[j]=vals[i]
                    elif i not in final: final[i]=vals.get(i)
                for i,v in final.items():
                    nm=cols[i]
                    if v is None or NONCAND.match(nm) and nm.lower()!='recount' and nm.lower() not in('scatter','others','other','write-ins','write-in'): continue
                    recs.append({'office':office,'county':cty,'district':cur_dist,'seats':cur_seats,'municipality':lab,'candidate':nm,'votes':v})
        block=[];total=None
    prev_text=None
    for r in R:
        r=[re.sub(r'^Recount\s+\S.*$','Recount',x) for x in r]
        c0=r[0].strip() if r else ''
        pt,prev_text=prev_text,(r if r and not any(NUM.match(x) for x in r) else None)
        prev_text_for_row=pt
        m=DIST.search(c0) if rep else None
        if rep and not m:
            m2=re.search(r'County\s+District\s*(?:No\.?)?\s*(\d+)\s*$',c0,re.I)
            if m2:
                # no "(seats)" printed: one candidate per seat in the first party's list
                n_named=sum(1 for x in r[1:] if is_name(x) and not NONCAND.match(x.strip()))
                class M:
                    def __init__(s,a,b): s.g=(a,b)
                    def group(s,i): return s.g[i-1]
                m=M(m2.group(1),str(max(1,n_named)))
        if m:
            close()
            n=int(m.group(1)) if m.group(1) else None
            if n is None: n=lastnum if prev_noname and cur_seats==int(m.group(2)) else lastnum+1
            prev_noname=not m.group(1)
            cur_dist=str(n);lastnum=n;cur_seats=int(m.group(2))
            names={i:x for i,x in enumerate(r) if i>0 and is_name(x)}
            if names: names={i:x for i,x in enumerate(r) if i>0 and x and not NUM.match(x) and not x.lower().startswith('page ')}
            elif prev_text_for_row and any(is_name(x) for x in prev_text_for_row[1:]):
                # the names row was printed just above the "District No." line
                names={i:x for i,x in enumerate(prev_text_for_row) if i>0 and x and not NUM.match(x)}
            cols=names or None
            continue
        if not rep:
            hd=[re.search(r'(?:SENATE|COUNCIL|CONGRESSIONAL)\D{0,25}?(?:DISTRICT|DIST\.)\s*(?:NO\.?)?\s*(\d+)\b(?!\s*,)',x,re.I) for x in r]
            hd=[h for h in hd if h] if not any(NUM.match(x) for x in r) else []
            if hd:
                close();cur_dist=str(int(hd[0].group(1)));cols=None
                r=['' if (h and h.string==x) else x for x,h in zip(r,[re.search(r'(?:SENATE|COUNCIL|CONGRESSIONAL)\D{0,25}?(?:DISTRICT|DIST\.)\s*(?:NO\.?)?\s*(\d+)\s*$',x,re.I) for x in r])]
                if not any(is_name(x) for x in r): continue
                c0=r[0]
        cty=[x for x in r if re.sub(r'\s+',' ',x).strip().upper() in [f'{y.upper()} COUNTY' for y in COUNTIES]]
        if cty and not any(NUM.match(x) for x in r):
            close();cur_county=cty[0].split()[0].title()
            if not any(is_name(x) for x in r): continue
            r=['' if x in cty else x for x in r];c0=r[0]
        r=[re.sub(r'^Recount\s+\S.*$','Recount',x) for x in r]
        names={i:x for i,x in enumerate(r) if is_name(x)}
        nums=[i for i,x in enumerate(r) if NUM.match(x)]
        if names and not nums:
            close()
            # every text cell of a header row is a candidate, even one printed without a party (an independent)
            names={i:x for i,x in enumerate(r) if x and not NUM.match(x) and not x.lower().startswith('page ') and (i>0 or is_name(x))}
            if prev_text_for_row:
                names={i:(prev_text_for_row[i]+' '+x if i<len(prev_text_for_row) and re.search(r'\band$',prev_text_for_row[i]) else x) for i,x in names.items()}
            if min(names)==0: names={i+1:x for i,x in names.items()}
            cols=names;continue
        if cols is None or not nums: continue
        lab=re.sub(r'\*+','',c0).strip()
        lab=re.sub(r'^.*?Page \d+ of \d+\s*','',lab)
        lab=re.sub(r'^State Senate\s*-?\s*District\s*(No\.?)?\s*\d+\s*','',lab,flags=re.I)                       # 'State Senate District No. 1 Republica n Page 2 of 2 Stewartstown'
        lab=re.sub(r'^((Republica\s*n|Republican|Democratic)\s+)+','',lab).strip()
        if re.match(r'^Recount Figures$',lab,re.I): lab='Recount'
        vals={i:num(x) for i,x in enumerate(r) if i>0 and NUM.match(x)}
        if not lab or NUM.match(lab): continue
        if lab.upper().startswith('TOTAL'): total=vals;continue
        if re.match(r'^(blc results?|ballot law commission|after ballot law)',lab,re.I): continue
        if re.match(r'^(recount|blc recount)$',lab,re.I) and block:
            block[-1]=(block[-1][0],block[-1][1],vals,False);continue
        # a number under a column with no candidate name: keep it as __COL<i> and flag it
        named=[k for k,v in cols.items() if not v.startswith('__COL')]
        for i in vals:
            if i not in cols and named and i>max(named) and i==max(vals):
                cols[i]='Scatter'   # trailing unheaded column = scattered write-ins
            elif i not in cols and cols.get(i+1,'').lower() in('scatter','others','other'):
                cols[i]=cols.pop(i+1)   # the header cell sits one column right of its numbers
            elif i not in cols:
                cols[i]=f'__COL{i}';problems.append((cur_dist,f'__COL{i}','unlabeled column',lab,vals[i],None))
        block.append((lab,cur_county,vals,False))
    close()
    return recs,problems
if __name__=='__main__':
    recs,probs=parse(sys.argv[1],'State Representative',sys.argv[2],rep=True)
    print(len(recs),'records; problems',len(probs));[print(' ',p) for p in probs[:20]]
