"""
Shared derivation of the canonical voting-municipality list and town->county
map for the 2026 entry system.

Single source of truth: the 2024 general election results. Every House seat is
up every cycle, so coverage is complete; names are ward-correct; and it avoids
the duplicate / empty-county / asterisk junk in district_compositions.
"""

import re

TEMPLATE_YEAR = 2024
TEMPLATE_TYPE = "general"


def town_county_rows(cur):
    """Yield (municipality, county, district) for State Rep in the template election."""
    cur.execute(
        """SELECT DISTINCT res.municipality, r.county, r.district
               FROM results res
               JOIN races r     ON res.race_id = r.id
               JOIN offices o    ON r.office_id = o.id
               JOIN elections e  ON r.election_id = e.id
               WHERE o.name = 'State Representative'
                 AND e.year = ? AND e.election_type = ?""",
        (TEMPLATE_YEAR, TEMPLATE_TYPE),
    )
    return cur.fetchall()


def normkey(s):
    """Normalize a municipality name for fuzzy matching (ward number un-padded)."""
    s = (s or "").upper().strip().rstrip("*")
    s = re.sub(r"WARD\s+0*(\d+)", r"WARD\1", s)
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


def town_county_map(cur):
    """UPPER base-town and normkey -> county."""
    m = {}
    for muni, county, _ in town_county_rows(cur):
        base = re.sub(r"\s+Ward\s+\d+\*?$", "", muni).strip().upper()
        m.setdefault(base, county)
        m.setdefault(normkey(muni), county)
    return m


def canonical_map(cur):
    """normkey -> canonical municipality name (the 2024-results spelling)."""
    canon = {}
    for muni, _, _ in town_county_rows(cur):
        canon.setdefault(normkey(muni), muni)
    return canon
