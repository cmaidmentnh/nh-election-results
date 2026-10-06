"""Rebuild static/data/nh-house-floterial-districts.geojson from the base districts.

Every 2022 base district lies wholly inside at most one floterial, so a floterial is exactly
the union of its member base districts. Membership comes from the SoS official 2026 general
ballots via nh-voter-api data/townward_districts.csv (town/ward -> base, floterial).
The previous file (from the GRANIT layer) gave floterials to ~95 town/wards that have none.

    python3 scripts/build_floterial_geojson.py [path/to/townward_districts.csv]
"""
import csv, json, sys, collections, os
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

D = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'static', 'data')
MAPCSV = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser('~/nh-voter-api/data/townward_districts.csv')
AB = {'Belknap': 'BE', 'Carroll': 'CA', 'Cheshire': 'CH', 'Coos': 'CO', 'Grafton': 'GR',
      'Hillsborough': 'HI', 'Merrimack': 'ME', 'Rockingham': 'RO', 'Strafford': 'ST', 'Sullivan': 'SU'}
def code(d):  # 'Carroll 7' -> 'CA7'
    c, n = d.rsplit(' ', 1); return AB[c] + str(int(n))

base_to_flot = {}
for r in csv.DictReader(open(MAPCSV)):
    b, f = code(r['house_base']), (code(r['house_flot']) if r['house_flot'] else ' ')
    if base_to_flot.setdefault(b, f) != f:
        sys.exit('base %s maps to two floterials (%s, %s); union-of-bases is invalid' % (b, base_to_flot[b], f))

bases = json.load(open(os.path.join(D, 'nh-house-base-districts.geojson')))
old = json.load(open(os.path.join(D, 'nh-house-floterial-districts.geojson')))
oldprops = {f['properties']['floathse22']: f['properties'] for f in old['features']}
groups = collections.defaultdict(list)
import re
def ncode(c):  # 'HI03' -> 'HI3' (the base layer zero-pads some codes)
    m = re.match(r'([A-Z]+)0*(\d+)$', c.strip()); return m.group(1) + m.group(2)
for f in bases['features']:
    b = ncode(f['properties']['basehse22'])
    if b not in base_to_flot: sys.exit('base %s not in membership map' % b)
    groups[base_to_flot[b]].append(shape(f['geometry']))
missing = set(base_to_flot) - {ncode(f['properties']['basehse22']) for f in bases['features']}
if missing: sys.exit('bases in map but not in geometry: %s' % sorted(missing))

feats = []
for i, fc in enumerate(sorted(groups, key=lambda c: (c != ' ', c)), 1):
    geom = unary_union(groups[fc]).buffer(0)
    props = dict(oldprops.get(fc, {}))
    props.update({'fid': i, 'floathse22': fc, 'objectid': i,
                  'member_bases': ','.join(sorted(b for b, f in base_to_flot.items() if f == fc)) if fc != ' ' else ''})
    for k in ('SHAPE__Length', 'shape_leng', 'SHAPE__Area'): props.pop(k, None)
    g = mapping(geom)
    feats.append({'type': 'Feature', 'properties': props, 'geometry': g})
out = {'type': 'FeatureCollection', 'name': old.get('name', 'nh-house-floterial-districts'), 'crs': old.get('crs'), 'features': feats}
json.dump(out, open(os.path.join(D, 'nh-house-floterial-districts.geojson'), 'w'), separators=(',', ':'))
print('wrote %d features (%d floterials + no-floterial remainder)' % (len(feats), len(feats) - 1))
