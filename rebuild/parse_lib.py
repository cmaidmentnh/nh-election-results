"""Corrected universal parser for NH SoS result spreadsheets.
Key fixes: read ALL tabs; KEEP towns named after counties (Merrimack, Hillsborough,
Grafton, Strafford); skip county-total rows only inside summary tabs; dedupe towns
across overlapping tabs (SET, not add); strip footnote asterisks."""
import glob, re, warnings
import pandas as pd
warnings.simplefilter("ignore")

COUNTIES = {'belknap','carroll','cheshire','coos','grafton','hillsborough','merrimack','rockingham','strafford','sullivan'}
NONCAND = {'undervotes','overvotes','scatter','scattering','write-ins','write-in','nan','total votes','','no election','recount'}

def split_party(s):
    p = s.rsplit(',', 1)
    return (p[0].strip(), p[1].strip().lower()) if len(p) == 2 and len(p[1].strip()) <= 6 else (s.strip(), '')

def pf(p):
    p = p.lower()
    if 'r' in p and 'd' in p: return 'Republican'
    if p.startswith('r'): return 'Republican'
    if p.startswith('d'): return 'Democratic'
    if p.startswith('l'): return 'Libertarian'
    if p.startswith('i'): return 'Independent'
    return 'Other'

def norm_town(m):
    return re.sub(r'\s+', ' ', re.sub(r'\*+$', '', m)).strip()

def files_for(globs):
    return sorted(set(sum([glob.glob(g) for g in globs], [])))

def parse_tab(df):
    """Return (header_row_index, [(col,name,party)], is_summary_tab) or None."""
    hr = next((i for i in range(min(8, len(df)))
               if any(isinstance(x, str) and re.search(r',\s*[rdl]', str(x), re.I) for x in df.iloc[i])), None)
    if hr is None:
        return None
    col0 = [str(x).strip().lower().replace(' county', '') for x in df.iloc[hr+1:, 0].tolist()]
    is_summary = sum(1 for x in col0 if x in COUNTIES) >= 5
    cands = []
    for c in range(df.shape[1]):
        v = df.iat[hr, c]
        if isinstance(v, str) and ',' in v and v.strip().lower() not in NONCAND:
            nm, pty = split_party(v)
            if nm and nm.lower() not in NONCAND:
                cands.append((c, nm, pf(pty)))
    return hr, cands, is_summary

def town_rows(df, hr, is_summary):
    """Yield (town, row_index) for real town rows (keeps county-named towns)."""
    for r in range(hr+1, len(df)):
        m = df.iat[r, 0]
        if not isinstance(m, str):
            continue
        t = norm_town(m); low = t.lower().replace(' county', '')
        if not t or low.startswith('total') or 'summary' in low or 'correction' in low:
            continue
        if is_summary and low in COUNTIES:      # county-total row inside a summary tab
            continue
        yield t, r

def parse_statewide(globs):
    """{town: {(candidate,party): votes}} deduped; for statewide single-race offices."""
    town = {}
    for f in files_for(globs):
        try: xl = pd.ExcelFile(f)
        except Exception: continue
        for sh in xl.sheet_names:
            df = xl.parse(sh, header=None)
            p = parse_tab(df)
            if not p: continue
            hr, cands, is_sum = p
            for t, r in town_rows(df, hr, is_sum):
                for c, nm, pty in cands:
                    try: v = int(float(df.iat[r, c]))
                    except (ValueError, TypeError): continue
                    town.setdefault(t, {})[(nm, pty)] = v   # SET = dedupe overlapping tabs
    return town

def totals(town):
    R = sum(v for tv in town.values() for (n, p), v in tv.items() if p == 'Republican')
    D = sum(v for tv in town.values() for (n, p), v in tv.items() if p == 'Democratic')
    return R, D, len(town)


def parse_with_totals(globs):
    """Returns (town_dict, totals_R, totals_D, counties_seen, missing_note).
    Reads BOTH town rows AND the file's own TOTALS rows per tab, so we can verify
    town-sum against the SoS-provided totals and detect missing counties/towns."""
    town = {}
    tot_R = tot_D = 0
    seen_tabs = set()
    counties = set()
    for f in files_for(globs):
        try: xl = pd.ExcelFile(f)
        except Exception: continue
        for sh in xl.sheet_names:
            key = (sh, )  # dedupe identical tab names across duplicate files
            df = xl.parse(sh, header=None)
            p = parse_tab(df)
            if not p: continue
            hr, cands, is_sum = p
            # county from tab name
            for cty in COUNTIES:
                if cty[:4] in sh.lower().replace(' ', ''):
                    counties.add(cty)
            if sh in seen_tabs:   # same tab name already processed (duplicate file) -> skip totals, dedupe towns by SET below
                dup = True
            else:
                dup = False; seen_tabs.add(sh)
            for r in range(hr+1, len(df)):
                m = df.iat[r, 0]
                if not isinstance(m, str): continue
                t = norm_town(m); low = t.lower().replace(' county', '')
                if not t or 'summary' in low or 'correction' in low: continue
                if low.startswith('total'):
                    if not dup:
                        for c, nm, pty in cands:
                            try: v = int(float(df.iat[r, c]))
                            except (ValueError, TypeError): continue
                            if pty == 'Republican': tot_R += v
                            elif pty == 'Democratic': tot_D += v
                    continue
                if is_sum and low in COUNTIES: continue
                for c, nm, pty in cands:
                    try: v = int(float(df.iat[r, c]))
                    except (ValueError, TypeError): continue
                    town.setdefault(t, {})[(nm, pty)] = v
    return town, tot_R, tot_D, counties
