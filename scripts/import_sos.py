#!/usr/bin/env python3
"""Read the Secretary of State's certified town-by-town returns.

These are the real thing - every town, signed off by the state - and they
supersede anything we assembled from wire feeds, machine tapes or photographs
of a clerk's return. They appear a few files at a time over the days after an
election, so this is written to be run again and again: it re-reads whatever is
on the page now and only writes what differs.

Three things in the layout will catch you out.

A zero is not printed at all. Albany's row carries fourteen figures and
Wolfeboro's thirteen, and Amherst's carries nine with the gaps in the middle,
because an empty column is simply left blank. Counting fields left to right
therefore shifts every column, so each figure is placed by the x co-ordinate it
is printed at, against the column anchors taken from the TOTALS row - the one
row that always carries every column.

The x co-ordinates have to come from the PDF itself, not from a rendered text
dump. pdftotext -layout fits each page to its own widest line, so a two-page
county comes out with the columns in one place on page one and somewhere else
on page two, and the TOTALS row that anchors them is only on the last page.
-bbox-layout gives the real points, and those agree across pages.

And the header cannot be read positionally at all. The figures are right-
aligned to their column and the names are not, and the names wrap over five
lines, so matching a fragment to a column by where it ends scatters the names
across the wrong candidates. The order is regular instead - every candidate on
one party's ballot by surname, then the other party's, then Write-Ins - so the
names come from the ballot we already hold, and that inference is then checked
twice before anything is written: the surnames in the header must appear left
to right in that same order, and every column's printed county total must sit
sensibly against our own sum for the candidate it was given. A file that fails
either check is refused, not guessed at.

    python3 scripts/import_sos.py --list
    python3 scripts/import_sos.py --url <pdf-url>            # dry run
    python3 scripts/import_sos.py --url <pdf-url> --apply
"""
import argparse
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PAGES = {
    "Republican": "https://www.sos.nh.gov/2026-republican-state-primary",
    "Democratic": "https://www.sos.nh.gov/2026-democratic-state-primary",
}

PARTY_ELECTION = {"REPUBLICAN": 29, "DEMOCRATIC": 30}

# sos.nh.gov sits behind Akamai and refuses anything that does not look like a
# browser. It is not enough to set a User-Agent: the Sec-Fetch-* set has to be
# present too, and a HEAD is refused outright whatever you send.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

SUFFIXES = {"JR", "SR", "II", "III", "IV"}


def surname(name):
    parts = [p for p in re.sub(r"[^A-Za-z ]", " ", name.upper()).split()
             if p not in SUFFIXES]
    return parts[-1] if parts else name.upper()


def fetch(url, referer=None):
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    req = urllib.request.Request(url, headers=headers)
    return urllib.request.urlopen(req, timeout=60).read()


def list_documents():
    """Every result PDF currently linked off the two party pages."""
    found = []
    for party, page in PAGES.items():
        try:
            html = fetch(page).decode("utf-8", "replace")
        except Exception as exc:
            print(f"  {party}: page unreachable ({exc})")
            continue
        for href in re.findall(r'href="([^"]+\.(?:pdf|xlsx))"', html):
            url = href if href.startswith("http") else "https://www.sos.nh.gov" + href
            if url not in {u for _p, _g, u in found}:
                found.append((party, page, url))
    return found


# ---------------------------------------------------------------- the PDF ---

WORD_RE = re.compile(r'\s*<word xMin="([\d.]+)" yMin="([\d.]+)" '
                     r'xMax="([\d.]+)" yMax="([\d.]+)">(.*)</word>')


def pdf_words(path):
    """Every word in the PDF with the point co-ordinates it is printed at."""
    xml = subprocess.run(["pdftotext", "-bbox-layout", str(path), "-"],
                         capture_output=True, text=True).stdout
    page, out = 0, []
    for line in xml.split("\n"):
        if "<page " in line:
            page += 1
        m = WORD_RE.match(line)
        if m:
            x0, y0, x1, y1 = (float(m.group(i)) for i in (1, 2, 3, 4))
            text = (m.group(5).replace("&amp;", "&").replace("&apos;", "'")
                    .replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">"))
            out.append({"page": page, "x0": x0, "x1": x1,
                        "y": (y0 + y1) / 2, "text": text})
    return out


def group_rows(words):
    """Words gathered into printed rows.

    A town's label and its figures are set in slightly different boxes, so
    their y co-ordinates differ by a point or two; the rows are 13 points
    apart, so anything within four points of the running mean is one row.
    """
    rows = []
    for w in sorted(words, key=lambda w: (w["page"], w["y"], w["x0"])):
        if rows and rows[-1][0]["page"] == w["page"] and \
                abs(w["y"] - rows[-1][0]["y"]) < 4:
            rows[-1].append(w)
        else:
            rows.append([w])
    return [sorted(r, key=lambda w: w["x0"]) for r in rows]


def is_number(text):
    return bool(re.fullmatch(r"\d[\d,]*", text))


def column_anchors(rows):
    """The right-hand edge of every figure column, off the TOTALS row.

    Every other row leaves its zeros blank, so TOTALS is the only row that can
    be trusted to show where all the columns are.
    """
    for row in rows:
        if row and row[0]["text"].upper().startswith("TOTALS"):
            return sorted(w["x1"] for w in row if is_number(w["text"]))
    return None


def split_row(row, cols):
    """A row's label and its figures.

    The figures are right-aligned hard against a column anchor, so a number is
    a figure when its right edge is on one and part of the label otherwise -
    which is what keeps the 12 of "Manchester Ward 12" out of the first column.
    """
    label, figures = [], []
    for w in row:
        if is_number(w["text"]):
            near = min(cols, key=lambda c: abs(c - w["x1"]))
            if abs(near - w["x1"]) < 3:
                figures.append((cols.index(near), int(w["text"].replace(",", ""))))
                continue
        label.append(w["text"])
    return " ".join(label).strip(), figures


# ----------------------------------------------------------- the ballot -----

def race_clause(district, county=None):
    """Narrow to one race. State Representative districts are numbered per
    county - Carroll 4 and Strafford 4 are different races - so the county has
    to travel with the district."""
    sql, args = "", []
    if district not in (None, ""):
        sql += " AND ra.district = ?"; args.append(str(district))
    if county not in (None, ""):
        sql += " AND ra.county = ?"; args.append(county)
    return sql, tuple(args)


def district_clause(district):
    """An office can be one race or several. Governor and U.S. Senator are one,
    so the office name alone finds them; Representative in Congress is two, and
    without the district CD-1's roster and CD-2's answer to the same query.
    Nashua is in the second district and Dover in the first, and a surname that
    appears on both ballots would resolve against the wrong one."""
    if district in (None, ""):
        return "", ()
    return " AND ra.district = ?", (str(district),)


def ballot_roster(office, election_id, district=None):
    """Everyone actually printed on one party's ballot for an office.

    race_candidates also carries every name anybody wrote in - Vermin Supreme,
    "any pedophile" - filed against the same race, and those are not columns on
    the Secretary of State's sheet. The filed candidates are the ones carrying
    a recruitment filing id; the write-ins are parked at ballot_order 900.
    """
    dclause, dargs = district_clause(district)
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT c.name FROM race_candidates rc
                     JOIN candidates c ON c.id = rc.candidate_id
                     JOIN races ra ON ra.id = rc.race_id
                     JOIN offices o ON o.id = ra.office_id
                    WHERE ra.election_id = ? AND o.name = ?
                      AND IFNULL(rc.recruitment_filing_id, -1) > 0
                      AND IFNULL(rc.ballot_order, 900) < 900""" + dclause,
                (election_id, office, *dargs))
    names = [r["name"] for r in cur.fetchall()]
    conn.close()
    return sorted(names, key=surname)


def roster_order(office, election_ids):
    """The column order the sheet prints, taken from the ballot we hold.

    One party's ballot by surname, then the other's, then the aggregate
    write-in column. Which party leads is not assumed - both orders are offered
    and the header decides.
    """
    out = []
    for eid in election_ids:
        out.extend(ballot_roster(office, eid))
    return out + ["Write-in"]


def header_agrees(rows, first_data, names):
    """Do the surnames printed in the header run left to right in this order?

    The header is the one independent witness to the column order, and it can
    be read for order even though it cannot be read for position. Each surname
    is looked for in the header text and taken at the leftmost place it appears
    that is still to the right of the previous name - "Maxwell" appears twice,
    once as Mary Maxwell's surname and once as Maxwell Saal's first name, and
    the second is only reached when it is Saal's turn.
    """
    spots = []
    for row in rows[:first_data]:
        for w in row:
            token = re.sub(r"[^A-Za-z]", "", w["text"]).upper()
            if token:
                spots.append((w["x0"], token))
    spots.sort()
    at = -1.0
    placed = []
    for name in names:
        if name.lower().startswith("write"):
            continue
        want = surname(name)
        hit = next((x for x, t in spots if t == want and x > at), None)
        if hit is None:
            return False, f"{name}: the header does not carry {want} " \
                          f"after x={at:.0f}"
        at = hit
        placed.append((want, hit))
    return True, placed


def parse(pdf_path, election_ids, office_hint=None):
    words = pdf_words(pdf_path)
    rows = group_rows(words)
    office = office_hint
    if not office:
        for row in rows[:6]:
            # the date is printed at the left margin on the same line as the
            # office, so it lands in the same row and has to be taken off first
            line = re.sub(r"^[A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4}\s*", "",
                          " ".join(w["text"] for w in row).strip())
            m = re.search(r"^(.+?)\s*-\s*(Republican|Democratic)$", line)
            if m:
                office = m.group(1).strip()
                break
    if not office:
        return {"error": "no office title on the sheet"}
    cols = column_anchors(rows)
    if not cols:
        return {"error": "no TOTALS row - the column anchors cannot be read"}

    first_data = len(rows)
    for i, row in enumerate(rows):
        label, figures = split_row(row, cols)
        up = label.upper()
        if figures and label and "COUNTY" not in up and not up.startswith("TOTALS"):
            first_data = i
            break

    orders = [election_ids, election_ids[::-1]]
    chosen = why = None
    for order in orders:
        names = roster_order(office, order)
        if len(names) != len(cols):
            why = (f"the ballot has {len(names)} columns "
                   f"({', '.join(names)}) and the sheet prints {len(cols)}")
            continue
        ok, detail = header_agrees(rows, first_data, names)
        if ok:
            chosen = names
            break
        why = detail
    if not chosen:
        return {"error": f"the column order could not be confirmed: {why}"}

    town_rows, totals = [], {}
    for row in rows:
        label, figures = split_row(row, cols)
        if not figures:
            continue
        up = label.upper()
        if up.startswith("TOTALS"):
            totals = {chosen[i]: v for i, v in figures}
            continue
        if not label or "COUNTY" in up or "STATE OF NEW HAMPSHIRE" in up:
            continue
        town_rows.append((label, {chosen[i]: v for i, v in figures}))
    return {"office": office, "candidates": chosen,
            "rows": town_rows, "totals": totals}


# ------------------------------------------------------- the spreadsheet ----

def parse_xlsx(path, source=None):
    """Read a county sheet published as a workbook instead of a PDF.

    None of the PDF's trouble applies here. A cell is a cell, so an empty one
    is a plain zero and needs no column arithmetic, and the candidate names sit
    in a header row and can simply be read - which is strictly better than
    inferring the order from the ballot, so that inference is not used at all
    on this path. The county totals are still checked against our own.
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[wb.sheetnames[0]]
    grid = [list(r) for r in ws.iter_rows(values_only=True)]

    office, district = "", None
    for row in grid[:5]:
        for cell in row:
            if not isinstance(cell, str):
                continue
            m = re.search(r"^(.+?)\s*-\s*(Republican|Democratic)$", cell.strip())
            if m and "Primary Election" not in m.group(1):
                office = m.group(1).strip()
            # The congressional sheets title themselves differently from the
            # county ones - "Congressional District 1 Republican", with no
            # dash - and they carry a district, which the statewide offices do
            # not. Getting that number wrong would write CD-1's towns into
            # CD-2's race, so it is taken from the title and then made to agree
            # with the tab name and the file name before it is used.
            m2 = re.match(r"^Congressional District\s+(\d)\s+"
                          r"(Republican|Democratic)$", cell.strip(), re.I)
            if m2:
                office, district = "Representative in Congress", m2.group(1)
    if office == "Representative in Congress":
        for label, found in (("sheet tab", re.search(r"con\s*(\d)", ws.title, re.I)),
                             ("file name", re.search(r"district[-_ ]?(\d)", str(source or path), re.I))):
            if not found:
                return {"error": f"congressional sheet with no district in its {label}"}
            if found.group(1) != district:
                return {"error": f"the title says district {district} and the "
                                 f"{label} says {found.group(1)}"}
    if not office:
        return {"error": "no office title on the sheet"}

    head = next((i for i, r in enumerate(grid)
                 if isinstance(r[0], str)
                 and ("COUNTY" in r[0].upper() or "SUMMARY" in r[0].upper())), None)
    if head is None:
        # The congressional sheets label the first column with the election
        # date rather than the county. Find the candidate row instead: every
        # name on these sheets carries its party as a trailing ", r" or ", d".
        head = next((i for i, r in enumerate(grid[:8])
                     if sum(1 for c in r[1:] if isinstance(c, str)
                            and re.search(r",\s*[rd]\s*$", c.strip())) >= 2), None)
    if head is None:
        return {"error": "no header row - nothing says which column is whose"}

    names, cols = [], []
    for j, cell in enumerate(grid[head][1:], start=1):
        if not isinstance(cell, str) or not cell.strip():
            continue
        # the header prints the party as a trailing ", r" or ", d"
        name = re.sub(r",\s*[rd]\s*$", "", cell.strip()).strip()
        names.append("Write-in" if name.lower().startswith("write") else name)
        cols.append(j)

    town_rows, totals = [], {}
    for row in grid[head + 1:]:
        label = row[0]
        if not isinstance(label, str) or not label.strip():
            continue
        figures = {}
        for name, j in zip(names, cols):
            v = row[j] if j < len(row) else None
            if isinstance(v, (int, float)):
                figures[name] = int(v)
            elif isinstance(v, str) and re.fullmatch(r"[\d,]+", v.strip()):
                figures[name] = int(v.replace(",", ""))
            else:
                figures[name] = 0
        if label.strip().upper().startswith("TOTAL"):
            totals = figures
            continue
        town_rows.append((label.strip(), figures))
    return {"office": office, "district": district, "candidates": names,
            "rows": town_rows, "totals": totals}


# ---------------------------------------------------- the House sheets ------

HOUSE_BLOCK = re.compile(r"^District No\.\s*(\d+)\s*\((\d+)\)\s*(F)?\s*$", re.I)


def parse_house_xlsx(path, source=None):
    """One county's State Representative returns.

    Unlike every other sheet, this one is not a single race. It is a stack of
    them: a "District No. 4 (2)" header carrying that district's own candidate
    columns, the towns that vote in it, then a Totals row, then the next
    district. A trailing F marks a floterial. Districts are numbered per
    county, so the county comes from the file name and travels with them.
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[wb.sheetnames[0]]
    grid = [list(r) for r in ws.iter_rows(values_only=True)]

    m = re.search(r"house-([a-z]+)-(republican|democratic)",
                  str(source or path), re.I)
    if not m:
        return {"error": "no county in the file name"}
    county = m.group(1).capitalize()
    if county == "Coos":
        county = "Coos"

    blocks, cur = [], None
    for row in grid:
        label = row[0] if row and isinstance(row[0], str) else None
        head = HOUSE_BLOCK.match(label.strip()) if label else None
        if head:
            names, cols = [], []
            for j, cell in enumerate(row[1:], start=1):
                if not isinstance(cell, str) or not cell.strip():
                    continue
                nm = re.sub(r",\s*[rd]\s*$", "", cell.strip()).strip()
                names.append("Write-in" if nm.lower().startswith("write") else nm)
                cols.append(j)
            cur = {"district": head.group(1), "seats": int(head.group(2)),
                   "floterial": bool(head.group(3)), "candidates": names,
                   "cols": cols, "rows": [], "totals": {}}
            blocks.append(cur)
            continue
        if cur is None or not label or not label.strip():
            continue
        figures = {}
        for nm, j in zip(cur["candidates"], cur["cols"]):
            v = row[j] if j < len(row) else None
            if isinstance(v, (int, float)):
                figures[nm] = int(v)
            elif isinstance(v, str) and re.fullmatch(r"[\d,]+", v.strip()):
                figures[nm] = int(v.replace(",", ""))
            else:
                figures[nm] = 0
        if label.strip().lower().startswith("total"):
            cur["totals"] = figures
        else:
            cur["rows"].append((label.strip(), figures))
    if not blocks:
        return {"error": "no district blocks on the sheet"}
    # Derry prints three times in Rockingham 13, one row per polling place, and
    # we hold Derry as one municipality. Left alone, each row would be applied
    # in turn and the last would win, leaving the town with a third of its
    # votes. Add them up instead.
    for b in blocks:
        merged, order = {}, []
        for town, figures in b["rows"]:
            if town not in merged:
                merged[town] = dict(figures)
                order.append(town)
            else:
                for k, v in figures.items():
                    merged[town][k] = merged[town].get(k, 0) + v
        b["rows"] = [(t, merged[t]) for t in order]
    return {"office": "State Representative", "county": county, "blocks": blocks}


def house_votes(county, district, election_id, towns):
    """What we hold for one House district, by candidate and municipality."""
    clause, args = race_clause(district, county)
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    got = {}
    for i in range(0, len(towns), 400):
        chunk = towns[i:i + 400]
        marks = ",".join("?" for _ in chunk)
        cur.execute(f"""SELECT c.name, r.municipality, r.votes
                          FROM results r
                          JOIN races ra ON ra.id = r.race_id
                          JOIN offices o ON o.id = ra.office_id
                          JOIN candidates c ON c.id = r.candidate_id
                         WHERE ra.election_id = ? AND o.name = 'State Representative'
                           AND r.municipality IN ({marks})""" + clause,
                    (election_id, *chunk, *args))
        for row in cur.fetchall():
            got.setdefault(row["municipality"], {})[row["name"]] = row["votes"]
    conn.close()
    return got


# -------------------------------------------------- the county sheets -------

COUNTY_OFFICE = {
    "SHERIFF": "County Sheriff",
    "ATTORNEY": "County Attorney",
    "TREASURER": "County Treasurer",
    "REGISTER OF DEEDS": "Register of Deeds",
    "REGISTER OF PROBATE": "Register of Probate",
    "COUNTY COMMISSIONERS": "County Commissioner",
    "COUNTY COMMISSIONER": "County Commissioner",
    "DELEGATES TO THE STATE CONVENTION": "Delegate to the State Convention",
    "DELEGATE TO THE STATE CONVENTION": "Delegate to the State Convention",
}


def _office_of(label):
    up = re.sub(r"\s+", " ", str(label or "")).strip().upper().rstrip(":")
    return COUNTY_OFFICE.get(up)


def parse_county_offices_xlsx(path, source=None):
    """One county's county-office returns.

    A third shape again. Offices run side by side across the page - Sheriff in
    one span of columns, Attorney in the next - and those spans are stacked
    down the sheet in blocks, each with its own header, towns and Totals row.

    The only thing separating one race from its neighbour is the Write-Ins
    column that ends it, and that is what this splits on. It matters most for
    the commissioners, where the header says "County Commissioners" once and
    the three districts that follow are unlabelled: cutting at the write-in
    column is what tells Chandler's race from McGee's from Parker's.
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[wb.sheetnames[0]]
    grid = [list(r) for r in ws.iter_rows(values_only=True)]

    m = re.search(r"county-offices-([a-z]+)-(republican|democratic)",
                  str(source or path), re.I)
    if not m:
        return {"error": "no county in the file name"}
    county = m.group(1).capitalize()

    races, i = [], 0
    while i < len(grid):
        row = grid[i]
        labels = [(j, _office_of(c)) for j, c in enumerate(row)
                  if j and _office_of(c)]
        if not labels:
            i += 1
            continue
        # the candidate row usually follows the office row, but the
        # commissioners block puts a spacer between them
        head, hrow = None, None
        for k in range(i + 1, min(i + 4, len(grid))):
            if any(isinstance(c, str) and (re.search(r",\s*[rd]\s*$", c.strip())
                                           or c.strip().lower().startswith("write"))
                   for c in grid[k]):
                head, hrow = grid[k], k
                break
        if head is None:
            i += 1
            continue

        # cut the header into one race per write-in column
        runs, start = [], None
        for j, cell in enumerate(head):
            if not isinstance(cell, str) or not cell.strip():
                continue
            if start is None:
                start = j
            if cell.strip().lower().startswith("write"):
                runs.append((start, j))
                start = None
        if start is not None:
            runs.append((start, len(head) - 1))

        # the towns for this block
        rows, k = [], hrow + 1
        while k < len(grid):
            label = grid[k][0]
            if isinstance(label, str) and label.strip():
                if label.strip().upper().startswith("TOTAL"):
                    k += 1
                    break
                rows.append(grid[k])
            elif _office_of(grid[k][1] if len(grid[k]) > 1 else None):
                break
            k += 1

        for lo, hi in runs:
            office = None
            for j, name in labels:
                if j <= lo:
                    office = name
            if not office:
                continue
            names, cols = [], []
            for j in range(lo, hi + 1):
                cell = head[j] if j < len(head) else None
                if not isinstance(cell, str) or not cell.strip():
                    continue
                nm = re.sub(r",\s*[rd]\s*$", "", cell.strip()).strip()
                names.append("Write-in" if nm.lower().startswith("write") else nm)
                cols.append(j)
            if len(names) < 2:      # a write-in column on its own is not a race
                continue
            town_rows = []
            for r in rows:
                figures = {}
                for nm, j in zip(names, cols):
                    v = r[j] if j < len(r) else None
                    if isinstance(v, (int, float)):
                        figures[nm] = int(v)
                    elif isinstance(v, str) and re.fullmatch(r"[\d,]+", v.strip()):
                        figures[nm] = int(v.replace(",", ""))
                    else:
                        figures[nm] = 0
                town_rows.append((str(r[0]).strip(), figures))
            races.append({"office": office, "candidates": names, "rows": town_rows})
        i = k

    if not races:
        return {"error": "no office blocks on the sheet"}
    return {"county": county, "races": races}


SENATE_TITLE = re.compile(r"^State Senate District\s+(\d+)\s+"
                          r"(Republican|Democratic)\s*$", re.I)


def parse_senate_xlsx(path, source=None):
    """State Senate returns: the congressional shape, but stacked.

    One file can hold two or three districts - 10 and 11 share a sheet - each
    with its own title, header, towns and Totals row.
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[wb.sheetnames[0]]
    grid = [list(r) for r in ws.iter_rows(values_only=True)]

    blocks, i = [], 0
    while i < len(grid):
        title = None
        for cell in grid[i]:
            if isinstance(cell, str) and SENATE_TITLE.match(cell.strip()):
                title = SENATE_TITLE.match(cell.strip())
                break
        if not title:
            i += 1
            continue
        head, hrow = None, None
        for k in range(i + 1, min(i + 4, len(grid))):
            if any(isinstance(c, str) and (re.search(r",\s*[rd]\s*$", c.strip())
                                           or c.strip().lower().startswith("write"))
                   for c in grid[k]):
                head, hrow = grid[k], k
                break
        if head is None:
            i += 1
            continue
        names, cols = [], []
        for j, cell in enumerate(head[1:], start=1):
            if not isinstance(cell, str) or not cell.strip():
                continue
            nm = re.sub(r",\s*[rd]\s*$", "", cell.strip()).strip()
            names.append("Write-in" if nm.lower().startswith("write") else nm)
            cols.append(j)
        rows, totals, k = [], {}, hrow + 1
        while k < len(grid):
            label = grid[k][0]
            if isinstance(label, str) and SENATE_TITLE.match(label.strip()):
                break
            if isinstance(label, str) and label.strip():
                figures = {}
                for nm, j in zip(names, cols):
                    v = grid[k][j] if j < len(grid[k]) else None
                    if isinstance(v, (int, float)):
                        figures[nm] = int(v)
                    elif isinstance(v, str) and re.fullmatch(r"[\d,]+", v.strip()):
                        figures[nm] = int(v.replace(",", ""))
                    else:
                        figures[nm] = 0
                if label.strip().upper().startswith("TOTAL"):
                    totals = figures
                    k += 1
                    break
                rows.append((label.strip(), figures))
            k += 1
        blocks.append({"district": title.group(1), "candidates": names,
                       "rows": rows, "totals": totals})
        i = k

    if not blocks:
        return {"error": "no district titles on the sheet"}
    return {"office": "State Senator", "blocks": blocks}


def column_trouble(pairs):
    """Does any column look like it belongs to a different candidate?

    The original test - certified within half to double what we hold - was
    built for the U.S. Senate sheets, where we already had every town. It is
    the wrong test here, and it failed in both directions: Rochester filed a
    whole floterial on Ward 1, so our ward figure was four times the certified
    one, and Derry never sent its second page, so ours is half.

    Neither is a mapping fault. A column on the wrong candidate moves one
    figure and leaves the rest, so what gives it away is a candidate whose
    ratio disagrees with everybody else's - not the size of the ratio. Small
    figures are ignored: a write-in going from 6 to 1 says nothing.

    `pairs` is (name, ours, certified). Returns a list of complaints.
    """
    usable = [(n, o, c) for n, o, c in pairs if o >= 25 and c >= 25]
    if len(usable) < 2:
        return []
    ratios = sorted(c / o for _n, o, c in usable)
    mid = ratios[len(ratios) // 2]
    if mid <= 0:
        return []
    out = []
    for n, o, c in usable:
        r = c / o
        if r > mid * 1.6 or r < mid / 1.6:
            out.append(f"{n}: ours {o}, certified {c} "
                       f"({r:.2f}x against {mid:.2f}x for the rest)")
    return out


# ------------------------------------------------------------ the check -----

def municipalities():
    """Every municipality we hold, keyed by a squashed form of its name.

    The Secretary of State writes Hale's Location and we write Hales Location.
    """
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT municipality FROM municipality_districts")
    got = {}
    for row in cur.fetchall():
        got[re.sub(r"[^A-Za-z0-9]", "", row["municipality"]).upper()] = row["municipality"]
    conn.close()
    return got


def race_roll(office, election_id, district=None):
    """Every name on this race, ballot or written in, with its race id."""
    dclause, dargs = district_clause(district)
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("""SELECT ra.id AS race_id, c.name,
                          IFNULL(rc.recruitment_filing_id, -1) AS filed
                     FROM race_candidates rc
                     JOIN candidates c ON c.id = rc.candidate_id
                     JOIN races ra ON ra.id = rc.race_id
                     JOIN offices o ON o.id = ra.office_id
                    WHERE ra.election_id = ? AND o.name = ?""" + dclause,
                (election_id, office, *dargs))
    rows = [(r["race_id"], r["name"], r["filed"]) for r in cur.fetchall()]
    conn.close()
    return rows


def norm(name):
    return " ".join(p for p in re.sub(r"[^A-Za-z ]", " ", name.upper()).split()
                    if p not in SUFFIXES)


def resolve_names(office, election_id, sheet_names, district=None):
    """Match the sheet's spelling of a name to the one we file it under.

    The ward reader looks a name up exactly and then by surname, and gives up
    on the whole race if either is ambiguous. Both aliases the feed recorded for
    the senator - "John Sununu" and a bare "Sununu" - answer to the surname, so
    a certified "John E. Sununu, r" on the Democratic sheet would take the whole
    town down with it. Resolving here instead, on how much of the name actually
    agrees, keeps that from happening and hands the reader our own spelling.
    """
    roll = race_roll(office, election_id, district)
    out = {}
    for sheet in sheet_names:
        if sheet.lower().startswith("write"):
            out[sheet] = sheet
            continue
        want = norm(sheet)
        exact = [n for _r, n, _f in roll if norm(n) == want]
        if exact:
            out[sheet] = exact[0]
            continue
        same = [(n, f) for _r, n, f in roll if surname(n) == surname(sheet)]
        if not same:
            out[sheet] = None
            continue
        wanted = set(want.split())

        def score(pair):
            n, filed = pair
            return (len(wanted & set(norm(n).split())), filed > 0, -len(norm(n)))

        same.sort(key=score, reverse=True)
        if len(same) > 1 and score(same[0]) == score(same[1]):
            out[sheet] = None
        else:
            out[sheet] = same[0][0]
    return out


def our_votes(office, election_id, towns, district=None):  # district: one race of several
    """What we currently hold for this race, by candidate and municipality."""
    dclause, dargs = district_clause(district)
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    got = {}
    for i in range(0, len(towns), 400):
        chunk = towns[i:i + 400]
        marks = ",".join("?" for _ in chunk)
        cur.execute(f"""SELECT c.name, r.municipality, r.votes
                          FROM results r
                          JOIN races ra ON ra.id = r.race_id
                          JOIN offices o ON o.id = ra.office_id
                          JOIN candidates c ON c.id = r.candidate_id
                         WHERE ra.election_id = ? AND o.name = ?
                           AND r.municipality IN ({marks})""" + dclause,
                    (election_id, office, *chunk, *dargs))
        for row in cur.fetchall():
            got.setdefault(row["municipality"], {})[row["name"]] = row["votes"]
    conn.close()
    return got


def validate(got, election_id, mapped, resolved):
    """Check the column mapping against our own figures before writing.

    Only towns we already hold something for are compared: a town we have never
    seen would look like a shortfall in every column at once and say nothing
    about the mapping.

    What is being tested here is which candidate each column belongs to, not
    whether every figure agrees. A column on the wrong candidate is off by a
    multiple - Sununu's 20,000 landed on a man with 200 - so the test is one of
    magnitude, and small disagreements are reported rather than refused. The
    write-in columns are counted as one bucket, because the sheet splits
    declared write-ins out by name where we lumped them together.
    """
    office, dist = got["office"], got.get("district")
    ballot = set(ballot_roster(office, election_id, dist))
    towns = sorted(mapped.values())
    ours = our_votes(office, election_id, towns, dist)
    seen = {t for t in towns if ours.get(t)}
    problems, moves = [], []

    for name in got["candidates"]:
        db = resolved.get(name)
        if db not in ballot:
            continue
        sos = sum(fig.get(name, 0) for pdf, fig in got["rows"]
                  if mapped.get(pdf) in seen)
        mine = sum(v.get(db, 0) for t, v in ours.items() if t in seen)
        moves.append((db, mine, sos))
        if mine > 1.25 * sos + 15:
            problems.append(f"{db}: we hold {mine} across {len(seen)} towns "
                            f"where the sheet certifies {sos} - far too high "
                            f"to be a counting difference, so the column is on "
                            f"the wrong candidate")
        elif sos >= 40 and mine < 0.75 * sos:
            problems.append(f"{db}: we hold {mine} against a certified {sos} "
                            f"in the same {len(seen)} towns - too far apart to "
                            f"be the undercount, so the column is suspect")

    bucket_sos = sum(v for pdf, fig in got["rows"] if mapped.get(pdf) in seen
                     for k, v in fig.items() if resolved.get(k) not in ballot)
    bucket_mine = sum(v for t, fig in ours.items() if t in seen
                      for k, v in fig.items() if k not in ballot)
    moves.append(("(write-ins, all together)", bucket_mine, bucket_sos))
    if bucket_mine > 2 * bucket_sos + 40:
        problems.append(f"write-ins: we hold {bucket_mine} against a certified "
                        f"{bucket_sos} - too far apart to write")
    return problems, moves, ours


def town_differences(got, mapped, resolved, ours, ballot):
    """Every town whose certified figure is not the one we hold.

    The known bug runs one way: the wire feed wrote VotingWorks' scanned column
    and left the hand-entered ballots out, so our figure should come up, not
    down. A town where ours is the higher one is the opposite of that and is
    listed separately, because it needs an explanation rather than an import.
    """
    up, down = [], []
    for pdf_town, figures in got["rows"]:
        town = mapped.get(pdf_town)
        if not town:
            continue
        held = ours.get(town, {})
        for name, votes in figures.items():
            db = resolved.get(name)
            if db not in ballot:
                continue
            old = held.get(db)
            if old is None or old == votes:
                continue
            (up if votes > old else down).append((town, db, old, votes))
    return up, down


def ward_check(got, resolved, ballot, known, election_id):
    """Cities the sheet totals but we hold ward by ward.

    Their ward rows are the finer record and nothing else has them, so they are
    never painted over with a single city figure. Comparing the two is worth
    doing all the same: our wards should add up to the certified city, and where
    they do not, the gap is the undercount showing itself.
    """
    dclause, dargs = district_clause(got.get("district"))
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    report = []
    for pdf_town, figures in got["rows"]:
        key = re.sub(r"[^A-Za-z0-9]", "", pdf_town).upper()
        if key in known:
            continue
        wards = sorted(v for k, v in known.items() if k.startswith(key + "WARD"))
        if not wards:
            continue
        marks = ",".join("?" for _ in wards)
        cur.execute(f"""SELECT c.name, SUM(r.votes) AS v
                          FROM results r
                          JOIN races ra ON ra.id = r.race_id
                          JOIN offices o ON o.id = ra.office_id
                          JOIN candidates c ON c.id = r.candidate_id
                         WHERE ra.election_id = ? AND o.name = ?
                           AND r.municipality IN ({marks})""" + dclause + """
                         GROUP BY c.name""",
                    (election_id, got["office"], *wards, *dargs))
        mine = {r["name"]: r["v"] for r in cur.fetchall()}
        for name, votes in figures.items():
            db = resolved.get(name)
            if db not in ballot:
                continue
            have = mine.get(db, 0)
            if have != votes:
                report.append((pdf_town, len(wards), db, have, votes))
    conn.close()
    return report


# ------------------------------------------------------------- applying -----

def apply_county(got, party, mapped, resolved, ours, ballot, apply):
    """Hand each town to the ward reader, one town at a time."""
    here = Path(__file__).resolve().parent
    done = 0
    for pdf_town, figures in got["rows"]:
        town = mapped.get(pdf_town)
        if not town:
            continue
        held = ours.get(town, {})
        lines = []
        for name, votes in figures.items():
            db = resolved.get(name)
            if db is None:
                continue
            # a write-in nobody got is not a figure - unless we are already
            # carrying one for them, in which case the certified zero is the
            # correction
            if votes == 0 and db not in ballot and not held.get(db):
                continue
            lines.append(f"0\t{party}\t{got['office']}\t{db}\t{votes}")
        if not lines:
            continue
        with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as fh:
            fh.write("\n".join(lines) + "\n")
            path = fh.name
        cmd = [sys.executable, str(here / "apply_ward_report.py"),
               "--file", path, "--city", town]
        if apply:
            cmd.append("--apply")
        out = subprocess.run(cmd, capture_output=True, text=True)
        sys.stdout.write(out.stdout)
        sys.stderr.write(out.stderr)
        Path(path).unlink(missing_ok=True)
        done += 1
    return done


def add_missing(office, election_id, names, party, apply, district=None):
    """Put a certified name we do not carry onto the race, so it can be read."""
    here = Path(__file__).resolve().parent
    roll = race_roll(office, election_id, district)
    if not roll:
        return
    race_id = roll[0][0]
    for name in names:
        cmd = [sys.executable, str(here / "add_ballot_candidate.py"),
               "--race", str(race_id), "--name", name, "--party", party.title()]
        if apply:
            cmd.append("--apply")
        out = subprocess.run(cmd, capture_output=True, text=True)
        sys.stdout.write("  " + out.stdout.replace("\n", "\n  ").rstrip() + "\n")


def handle(source, path, party, apply, out_tsv=None):
    election_id = PARTY_ELECTION[party]
    other = 30 if election_id == 29 else 29
    name = Path(source).name

    if "state-senate" in Path(source).name.lower():
        return handle_senate(source, path, party, apply, out_tsv)

    if "county-offices" in Path(source).name.lower():
        return handle_county_offices(source, path, party, apply, out_tsv)

    if "house" in Path(source).name.lower():
        return handle_house(source, path, party, apply, out_tsv)

    if str(path).lower().endswith(".xlsx"):
        # the download lands in a temp file, so the published name - which
        # carries the district - has to come from the source
        got = parse_xlsx(path, source)
    else:
        got = parse(path, [election_id, other])
    if got.get("error"):
        print(f"REFUSED {name}: {got['error']}")
        return False

    where = f" district {got['district']}" if got.get("district") else ""
    print(f"\n=== {name}: {got['office']}{where} - {party.title()}")
    print(f"columns: {got['candidates']}")

    known = municipalities()
    mapped, missing, warded = {}, [], []
    for pdf_town, _fig in got["rows"]:
        key = re.sub(r"[^A-Za-z0-9]", "", pdf_town).upper()
        if key in known:
            mapped[pdf_town] = known[key]
        elif any(k.startswith(key + "WARD") for k in known):
            warded.append(pdf_town)
        else:
            missing.append(pdf_town)
    print(f"{len(got['rows'])} rows on the sheet, {len(mapped)} of them "
          f"municipalities we hold")

    dist = got.get("district")
    resolved = resolve_names(got["office"], election_id, got["candidates"], dist)
    ballot = set(ballot_roster(got["office"], election_id, dist))
    unknown = [n for n, db in resolved.items() if db is None]
    if unknown:
        totals = {n: sum(f.get(n, 0) for _t, f in got["rows"]) for n in unknown}
        print("names on the sheet that this race does not carry: " +
              ", ".join(f"{n} ({totals[n]})" for n in unknown))

    problems, moves, ours = validate(got, election_id, mapped, resolved)
    held = len([t for t in mapped.values() if ours.get(t)])
    print(f"\ncolumn check, over the {held} municipalities we already hold:")
    for cand, mine, sos in moves:
        print(f"  {cand:<28} ours {mine:>7}   certified {sos:>7}")
    if problems:
        print(f"\nREFUSED {name} - the column mapping did not stand up:")
        for p in problems:
            print("  " + p)
        return False
    print("  mapping accepted")

    # only now, with the mapping proved, is a certified name we do not carry
    # put on the race - otherwise a refused sheet would still leave its mark
    wanted = [n for n in unknown
              if sum(f.get(n, 0) for _t, f in got["rows"]) > 0]
    if wanted:
        add_missing(got["office"], election_id, wanted, party, apply, dist)
        if apply:
            resolved = resolve_names(got["office"], election_id,
                                     got["candidates"], dist)

    if warded:
        gaps = ward_check(got, resolved, ballot, known, election_id)
        print(f"\n{len(warded)} city total(s) the sheet gives whole and we hold "
              f"ward by ward - not written over, compared instead:")
        for city in warded:
            rows = [g for g in gaps if g[0] == city]
            if not rows:
                print(f"  {city}: our wards agree with the certified city")
            for _c, nwards, cand, mine, sos in rows:
                print(f"  {city} ({nwards} wards): {cand} ours {mine} "
                      f"certified {sos}  ({sos - mine:+d})")
    if missing:
        print(f"\nnot a municipality we hold, skipped: {', '.join(missing)}")

    up, down = town_differences(got, mapped, resolved, ours, ballot)
    if down:
        print(f"\n{len(down)} figure(s) where OURS IS HIGHER than the certified "
              f"return - the opposite of the known undercount:")
        for town, cand, old, new in down:
            print(f"  {town}: {cand} ours {old} certified {new} ({new - old:+d})")
    print(f"\n{len(up)} figure(s) where the certified return is higher than ours")

    if out_tsv:
        with open(out_tsv, "w") as fh:
            for pdf_town, figures in got["rows"]:
                town = mapped.get(pdf_town, pdf_town)
                for cand, votes in figures.items():
                    fh.write(f"{town}\t{party}\t{got['office']}\t"
                             f"{resolved.get(cand) or cand}\t{votes}\n")
        print(f"wrote {out_tsv}")
        return True

    print()
    done = apply_county(got, party, mapped, resolved, ours, ballot, apply)
    print(f"{done} municipality/ies {'applied' if apply else 'dry run'}")
    return True


def handle_house(source, path, party, apply, out_tsv=None):
    """Apply one county's House sheet, district by district.

    Each district is handed to the ward reader as its own report, because that
    is the unit it can check: the candidates in a block all belong to one race,
    so a name it cannot place, or one that could belong to two races, stops
    that district and leaves the rest alone.
    """
    election_id = PARTY_ELECTION[party]
    got = parse_house_xlsx(path, source)
    name = Path(source).name
    if got.get("error"):
        print(f"REFUSED {name}: {got['error']}")
        return False

    county = got["county"]
    print(f"\n=== {name}: {county} County State Representatives - {party.title()}")
    print(f"{len(got['blocks'])} district block(s)")

    known = municipalities()
    here = Path(__file__).resolve().parent
    done = held = 0
    for b in got["blocks"]:
        tag = f"{county} {b['district']}" + (" (floterial)" if b["floterial"] else "")
        # the sheet's own arithmetic first: the town rows have to make the
        # Totals row the sheet prints, or the columns are not what they say
        # Only against figures the sheet actually printed. Hillsborough 2, 12
        # and 13 leave every candidate cell of their Totals row empty and fill
        # in only the write-in column, and an empty cell is not a claim that
        # the district cast no votes. Strafford 3 still fails this, because
        # there the printed total is a real number and the rows do not reach
        # it.
        if b["totals"] and any(b["totals"].get(n, 0) for n in b["candidates"]):
            bad = [(n, sum(f.get(n, 0) for _t, f in b["rows"]), b["totals"].get(n, 0))
                   for n in b["candidates"]
                   if b["totals"].get(n, 0)
                   and sum(f.get(n, 0) for _t, f in b["rows"]) != b["totals"][n]]
            if bad:
                print(f"  REFUSED {tag}: town rows do not make the printed total: {bad}")
                held += 1
                continue

        mapped = {}
        for town, _f in b["rows"]:
            key = re.sub(r"[^A-Za-z0-9]", "", town).upper()
            if key in known:
                mapped[town] = known[key]
        ours = house_votes(county, b["district"], election_id,
                           sorted(mapped.values()))

        # a column on the wrong candidate is off by a multiple, so compare
        # what we already hold against what the sheet says for the same towns
        # district totals, not town by town: a city that filed its whole
        # district on one ward still totals the same across the district
        moves, pairs = [], []
        for n in b["candidates"]:
            if n.lower().startswith("write"):
                continue
            mine = sum(v.get(n, 0) for t, v in ours.items())
            sos = sum(f.get(n, 0) for _t, f in b["rows"])
            if mine:
                moves.append((n, mine, sos))
                pairs.append((n, mine, sos))
        problems = column_trouble(pairs)
        if problems:
            print(f"  REFUSED {tag}: " + "; ".join(problems))
            held += 1
            continue

        wrote = 0
        for town, figures in b["rows"]:
            db_town = mapped.get(town)
            if not db_town:
                continue
            lines = [f"0\t{party}\tState Representative\t{n}\t{v}"
                     for n, v in figures.items()]
            with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as fh:
                fh.write("\n".join(lines) + "\n")
                tsv = fh.name
            cmd = [sys.executable, str(here / "apply_ward_report.py"),
                   "--file", tsv, "--city", db_town]
            if apply:
                cmd.append("--apply")
            out = subprocess.run(cmd, capture_output=True, text=True)
            if "held back" in out.stdout or "refus" in out.stdout.lower():
                sys.stdout.write(f"  {db_town}: " + out.stdout.split("held back")[-1][:160] + "\n")
            Path(tsv).unlink(missing_ok=True)
            wrote += 1
        print(f"  {tag}: {len(b['candidates'])} columns, {wrote} town(s) "
              f"{'applied' if apply else 'dry run'}"
              + (f"   [{', '.join(f'{n} {m}->{x}' for n, m, x in moves[:3])}]" if moves else ""))
        done += 1
    print(f"{done} district(s) {'applied' if apply else 'checked'}, {held} held back")
    return held == 0


def handle_county_offices(source, path, party, apply, out_tsv=None):
    """Apply one county's county-office sheet, one race at a time."""
    election_id = PARTY_ELECTION[party]
    got = parse_county_offices_xlsx(path, source)
    name = Path(source).name
    if got.get("error"):
        print(f"REFUSED {name}: {got['error']}")
        return False

    county = got["county"]
    print(f"\n=== {name}: {county} County offices - {party.title()}")
    known = municipalities()
    here = Path(__file__).resolve().parent
    done = held = 0
    seen, per_town = {}, {}
    for race in got["races"]:
        office = race["office"]
        seen[office] = seen.get(office, 0) + 1
        tag = office + (f" #{seen[office]}" if office == "County Commissioner" else "")

        mapped = {}
        for town, _f in race["rows"]:
            key = re.sub(r"[^A-Za-z0-9]", "", town).upper()
            if key in known:
                mapped[town] = known[key]
        ours = our_votes(office, election_id, sorted(mapped.values()))

        moves, pairs = [], []
        for n in race["candidates"]:
            if n.lower().startswith("write"):
                continue
            mine = sum(v.get(n, 0) for v in ours.values())
            sos = sum(f.get(n, 0) for _t, f in race["rows"])
            if mine:
                moves.append((n, mine, sos))
                pairs.append((n, mine, sos))
        problems = column_trouble(pairs)
        if problems:
            print(f"  REFUSED {tag}: " + "; ".join(problems))
            held += 1
            continue

        # the heading carries the district, so the three commissioner races can
        # travel in the same report as the rest without being confused for one
        # another - one call per town instead of one per town per race
        heading = office + (f" District {seen[office]}"
                            if office == "County Commissioner" else "")
        for town, figures in race["rows"]:
            db_town = mapped.get(town)
            if not db_town:
                continue
            per_town.setdefault(db_town, []).extend(
                f"0\t{party}\t{heading}\t{n}\t{v}" for n, v in figures.items())
        print(f"  {tag}: {len(mapped)} town(s) queued"
              + (f"   [{', '.join(f'{n} {m}->{x}' for n, m, x in moves[:2])}]" if moves else ""))
        done += 1

    for db_town, lines in sorted(per_town.items()):
        with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as fh:
            fh.write("\n".join(lines) + "\n")
            tsv = fh.name
        cmd = [sys.executable, str(here / "apply_ward_report.py"),
               "--file", tsv, "--city", db_town]
        if apply:
            cmd.append("--apply")
        out = subprocess.run(cmd, capture_output=True, text=True)
        if "held back" in out.stdout:
            sys.stdout.write(f"  {db_town}: "
                             + out.stdout.split("held back")[-1].strip()[:200] + "\n")
        Path(tsv).unlink(missing_ok=True)
    print(f"{done} race(s) {'applied' if apply else 'checked'} over "
          f"{len(per_town)} town(s), {held} held back")
    return held == 0


def handle_senate(source, path, party, apply, out_tsv=None):
    """Apply one State Senate sheet, district by district."""
    election_id = PARTY_ELECTION[party]
    got = parse_senate_xlsx(path, source)
    name = Path(source).name
    if got.get("error"):
        print(f"REFUSED {name}: {got['error']}")
        return False

    print(f"\n=== {name}: State Senate - {party.title()}")
    known = municipalities()
    here = Path(__file__).resolve().parent
    done = held = 0
    for b in got["blocks"]:
        tag = f"District {b['district']}"
        # Only against figures the sheet actually printed. Some sheets leave every candidate cell of their Totals row empty and fill
        # in only the write-in column, and an empty cell is not a claim that
        # the district cast no votes. Strafford 3 still fails this, because
        # there the printed total is a real number and the rows do not reach
        # it.
        if b["totals"] and any(b["totals"].get(n, 0) for n in b["candidates"]):
            bad = [(n, sum(f.get(n, 0) for _t, f in b["rows"]), b["totals"].get(n, 0))
                   for n in b["candidates"]
                   if b["totals"].get(n, 0)
                   and sum(f.get(n, 0) for _t, f in b["rows"]) != b["totals"][n]]
            if bad:
                print(f"  REFUSED {tag}: town rows do not make the printed total: {bad}")
                held += 1
                continue

        mapped = {}
        for town, _f in b["rows"]:
            key = re.sub(r"[^A-Za-z0-9]", "", town).upper()
            if key in known:
                mapped[town] = known[key]
        ours = our_votes("State Senator", election_id, sorted(mapped.values()),
                         b["district"])
        pairs, moves = [], []
        for n in b["candidates"]:
            if n.lower().startswith("write"):
                continue
            mine = sum(v.get(n, 0) for v in ours.values())
            sos = sum(f.get(n, 0) for _t, f in b["rows"])
            if mine:
                moves.append((n, mine, sos))
                pairs.append((n, mine, sos))
        problems = column_trouble(pairs)
        if problems:
            print(f"  REFUSED {tag}: " + "; ".join(problems))
            held += 1
            continue

        heading = f"State Senator District {b['district']}"
        wrote = 0
        for town, figures in b["rows"]:
            db_town = mapped.get(town)
            if not db_town:
                continue
            lines = [f"0\t{party}\t{heading}\t{n}\t{v}" for n, v in figures.items()]
            with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as fh:
                fh.write("\n".join(lines) + "\n")
                tsv = fh.name
            cmd = [sys.executable, str(here / "apply_ward_report.py"),
                   "--file", tsv, "--city", db_town]
            if apply:
                cmd.append("--apply")
            out = subprocess.run(cmd, capture_output=True, text=True)
            if "held back" in out.stdout:
                sys.stdout.write(f"  {db_town} {tag}: "
                                 + out.stdout.split("held back")[-1].strip()[:140] + "\n")
            Path(tsv).unlink(missing_ok=True)
            wrote += 1
        print(f"  {tag}: {wrote} town(s) {'applied' if apply else 'dry run'}"
              + (f"   [{', '.join(f'{n} {m}->{x}' for n, m, x in moves[:2])}]" if moves else ""))
        done += 1
    print(f"{done} district(s) {'applied' if apply else 'checked'}, {held} held back")
    return held == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--all", action="store_true",
                    help="every county sheet currently linked")
    ap.add_argument("--url")
    ap.add_argument("--file", help="a sheet already on disk")
    ap.add_argument("--party", help="REPUBLICAN or DEMOCRATIC, if the file "
                                    "name does not say")
    ap.add_argument("--out", help="write a TSV instead of applying")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    if args.list:
        docs = list_documents()
        print(f"{len(docs)} document(s) linked:")
        for party, _page, url in docs:
            print(f"  [{party}] {url.rsplit('/', 1)[-1]}")
        return

    jobs = []
    if args.all:
        for party, page, url in list_documents():
            if "summary" in url.lower():
                continue
            jobs.append((party.upper()[:10], page, url))
    elif args.url:
        party = (args.party or
                 ("REPUBLICAN" if "republican" in args.url.lower()
                  else "DEMOCRATIC")).upper()
        jobs.append((party, PAGES[party.title()], args.url))
    elif args.file:
        party = (args.party or
                 ("REPUBLICAN" if "republican" in args.file.lower()
                  else "DEMOCRATIC")).upper()
        jobs.append((party, None, args.file))
    else:
        ap.error("pass --list, --all, --url or --file")

    ok = bad = 0
    for party, page, source in jobs:
        party = "REPUBLICAN" if party.startswith("REPUB") else "DEMOCRATIC"
        if page:
            suffix = ".xlsx" if source.lower().endswith(".xlsx") else ".pdf"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as fh:
                fh.write(fetch(source, page))
                path = fh.name
        else:
            path = source
        try:
            if handle(source, path, party, args.apply, args.out):
                ok += 1
            else:
                bad += 1
        finally:
            if page:
                Path(path).unlink(missing_ok=True)
    print(f"\n{ok} sheet(s) taken, {bad} refused")
    if bad:
        sys.exit(2)


if __name__ == "__main__":
    main()
