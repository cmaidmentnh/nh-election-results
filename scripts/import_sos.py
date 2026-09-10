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

def ballot_roster(office, election_id):
    """Everyone actually printed on one party's ballot for an office.

    race_candidates also carries every name anybody wrote in - Vermin Supreme,
    "any pedophile" - filed against the same race, and those are not columns on
    the Secretary of State's sheet. The filed candidates are the ones carrying
    a recruitment filing id; the write-ins are parked at ballot_order 900.
    """
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT c.name FROM race_candidates rc
                     JOIN candidates c ON c.id = rc.candidate_id
                     JOIN races ra ON ra.id = rc.race_id
                     JOIN offices o ON o.id = ra.office_id
                    WHERE ra.election_id = ? AND o.name = ?
                      AND IFNULL(rc.recruitment_filing_id, -1) > 0
                      AND IFNULL(rc.ballot_order, 900) < 900""", (election_id, office))
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

def parse_xlsx(path):
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

    office = ""
    for row in grid[:5]:
        for cell in row:
            if not isinstance(cell, str):
                continue
            m = re.search(r"^(.+?)\s*-\s*(Republican|Democratic)$", cell.strip())
            if m and "Primary Election" not in m.group(1):
                office = m.group(1).strip()
    if not office:
        return {"error": "no office title on the sheet"}

    head = next((i for i, r in enumerate(grid)
                 if isinstance(r[0], str)
                 and ("COUNTY" in r[0].upper() or "SUMMARY" in r[0].upper())), None)
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
    return {"office": office, "candidates": names,
            "rows": town_rows, "totals": totals}


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


def race_roll(office, election_id):
    """Every name on this race, ballot or written in, with its race id."""
    from intake import store
    conn = store.connect()
    cur = conn.cursor()
    cur.execute("""SELECT ra.id AS race_id, c.name,
                          IFNULL(rc.recruitment_filing_id, -1) AS filed
                     FROM race_candidates rc
                     JOIN candidates c ON c.id = rc.candidate_id
                     JOIN races ra ON ra.id = rc.race_id
                     JOIN offices o ON o.id = ra.office_id
                    WHERE ra.election_id = ? AND o.name = ?""",
                (election_id, office))
    rows = [(r["race_id"], r["name"], r["filed"]) for r in cur.fetchall()]
    conn.close()
    return rows


def norm(name):
    return " ".join(p for p in re.sub(r"[^A-Za-z ]", " ", name.upper()).split()
                    if p not in SUFFIXES)


def resolve_names(office, election_id, sheet_names):
    """Match the sheet's spelling of a name to the one we file it under.

    The ward reader looks a name up exactly and then by surname, and gives up
    on the whole race if either is ambiguous. Both aliases the feed recorded for
    the senator - "John Sununu" and a bare "Sununu" - answer to the surname, so
    a certified "John E. Sununu, r" on the Democratic sheet would take the whole
    town down with it. Resolving here instead, on how much of the name actually
    agrees, keeps that from happening and hands the reader our own spelling.
    """
    roll = race_roll(office, election_id)
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


def our_votes(office, election_id, towns):
    """What we currently hold for this race, by candidate and municipality."""
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
                           AND r.municipality IN ({marks})""",
                    (election_id, office, *chunk))
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
    office = got["office"]
    ballot = set(ballot_roster(office, election_id))
    towns = sorted(mapped.values())
    ours = our_votes(office, election_id, towns)
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
                           AND r.municipality IN ({marks})
                         GROUP BY c.name""",
                    (election_id, got["office"], *wards))
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


def add_missing(office, election_id, names, party, apply):
    """Put a certified name we do not carry onto the race, so it can be read."""
    here = Path(__file__).resolve().parent
    roll = race_roll(office, election_id)
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

    if str(path).lower().endswith(".xlsx"):
        got = parse_xlsx(path)
    else:
        got = parse(path, [election_id, other])
    if got.get("error"):
        print(f"REFUSED {name}: {got['error']}")
        return False

    print(f"\n=== {name}: {got['office']} - {party.title()}")
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

    resolved = resolve_names(got["office"], election_id, got["candidates"])
    ballot = set(ballot_roster(got["office"], election_id))
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
        add_missing(got["office"], election_id, wanted, party, apply)
        if apply:
            resolved = resolve_names(got["office"], election_id,
                                     got["candidates"])

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
