import json,sys,collections,re
year=int(sys.argv[1]);R=json.load(open(f'out_{year}.json'))
known={l.strip().lower() for l in open('known_munis.txt')}
races=collections.defaultdict(collections.Counter);seats={}
for r in R:
    k=(r['office'],r['county'],r['district']);seats[k]=r['seats']
    races[k][(r['candidate'],r['party'])]+=r['votes']
def winners(k):
    c=[x for x in races[k].most_common() if x[0][0]!='Write-Ins'];return c[:seats[k]]
summ=collections.defaultdict(collections.Counter);cnt=collections.Counter()
for k in races:
    cnt[k[0]]+=1
    for (n,p),v in winners(k): summ[k[0]][p or '?']+=1
print(f'== {year}')
for off in sorted(cnt):
    s=sum(seats[k] for k in races if k[0]==off)
    print(f'  {off}: {cnt[off]} races, {s} seats; winners by party {dict(summ[off])}')
    if off in('Governor','United States Senator','President of the United States'):
        k=[k for k in races if k[0]==off][0];print('     ',[(n,p,v) for (n,p),v in races[k].most_common(4)])
    if off in ('Representative in Congress','Executive Councilor','State Senator') and cnt[off]<=5:
        for k in sorted(races,key=lambda k:tuple(str(x) for x in k)):
            if k[0]==off: print('     ',k[2],[(n,p,v) for (n,p),v in races[k].most_common(3)])
unk=collections.Counter(r['municipality'] for r in R if r['municipality'].lower() not in known)
print('  unknown municipality names:',dict(unk))
nop=collections.Counter(r['raw'] for r in R if not r['party'] and r['candidate']!='Write-Ins')
print('  candidates with no party:',dict(nop))
