"""Compare statewide totals with the SoS's own statewide summary pages (sum*)."""
import json,sys,glob,collections,re,os
from parse import parse
from pdfgrid import parse_pdf
year=int(sys.argv[1]);folder=sys.argv[2]
R=json.load(open(f'out_{year}.json'))
mine=collections.defaultdict(collections.Counter)
for r in R:
    if r['office'] in('Governor','United States Senator','President of the United States'): mine[r['office']][r['raw'].split(',')[0].strip()]+=r['votes']
for f in sorted(glob.glob(f'raw/{folder}/*sum*')):
    fn=os.path.basename(f).lower()
    off='Governor' if 'gov' in fn else 'United States Senator' if 'uss' in fn else 'President of the United States' if 'pres' in fn else None
    if not off: continue
    recs,_=(parse_pdf if fn.endswith('.pdf') else parse)(f,off,None)
    s=collections.Counter()
    for r in recs:
        if not re.search(r'county',r['municipality'],re.I) or True: s[r['candidate'].split(',')[0].strip()]+=r['votes']
    for cand,v in s.most_common(4):
        m=mine[off].get(cand)
        print(f'  {year} {off[:22]:22} {cand:28} summary {v:>9,}  mine {m if m is None else format(m,",")}  {"OK" if m==v else "DIFF"}')
