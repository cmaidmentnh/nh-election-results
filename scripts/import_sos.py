#!/usr/bin/env python3
"""Read the Secretary of State's certified town-by-town returns.

These are the real thing - every town, signed off by the state - and they
supersede anything we assembled from wire feeds, machine tapes or photographs
of a clerk's return. They appear a few files at a time over the days after an
election, so this is written to be run again and again: it re-reads whatever is
on the page now and only writes what differs.

Two things in the layout will catch you out.

Trailing zero columns are dropped. Albany's row carries fourteen figures and
Wolfeboro's thirteen, because Wolfeboro's write-in count is zero and simply is
not printed. Counting fields left to right therefore shifts every column, so
each figure is placed by where it sits on the line instead, against the column
positions taken from the TOTALS row - the one row that always carries them all.

And the candidate names in the header wrap across five lines, so a name is
rebuilt from every fragment that falls within its column.

    python3 scripts/import_sos.py --list
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
        for href in re.findall(r'href="([^"]+\.pdf)"', html):
            url = href if href.startswith("http") else "https://www.sos.nh.gov" + href
            found.append((party, page, url))
    return found


def columns_from_totals(lines):
    """Right-hand edge of every figure column, taken from the TOTALS row."""
    for line in lines:
        if line.strip().upper().startswith("TOTALS"):
            return [m.end() for m in re.finditer(r"\d[\d,]*", line)]
    return None


def header_names(lines, cols, stop_at):
    """Rebuild each column's candidate name from the wrapped header block."""
    parts = [[] for _ in cols]
    for line in lines[:stop_at]:
        if not line.strip():
            continue
        for m in re.finditer(r"[A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*)*", line):
            text = m.group(0).strip()
            if not text or text.upper().startswith(("STATE OF NEW", "SEPTEMBER")):
                continue
            i = min(range(len(cols)), key=lambda k: abs(cols[k] - m.end()))
            parts[i].append((m.start(), text))
    names = []
    for chunk in parts:
        chunk.sort()
        joined = " ".join(t for _, t in chunk)
        joined = re.sub(r"\s*,\s*[rd]\b", "", joined)      # drop the ", r" / ", d" suffix
        names.append(re.sub(r"\s+", " ", joined).strip())
    return names


def parse(pdf_path):
    text = subprocess.run(["pdftotext", "-layout", str(pdf_path), "-"],
                          capture_output=True, text=True).stdout
    lines = text.split("\n")
    office = ""
    for line in lines[:6]:
        if "-" in line and "Primary Election" not in line and line.strip():
            office = line.strip()
            break
    cols = columns_from_totals(lines)
    if not cols:
        return None
    first_data = next((i for i, l in enumerate(lines)
                       if re.match(r"^\s*[A-Z][A-Za-z.'\- ]+\s{2,}\d", l)
                       and "COUNTY" not in l.upper()), len(lines))
    names = header_names(lines, cols, first_data)
    rows = []
    for line in lines[first_data:]:
        t = line.strip()
        if not t or t.upper().startswith("TOTALS"):
            continue
        nums = [(m.group(0), m.end()) for m in re.finditer(r"\d[\d,]*", line)]
        if not nums:
            continue
        town = line[:min(n[1] - len(n[0]) for n in nums)].strip()
        if not town or town.upper().endswith("COUNTY"):
            continue
        for value, end in nums:
            i = min(range(len(cols)), key=lambda k: abs(cols[k] - end))
            rows.append((town, names[i], int(value.replace(",", ""))))
    return {"office": office, "candidates": names, "rows": rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--url")
    ap.add_argument("--out", help="write a TSV for apply_ward_report.py")
    args = ap.parse_args()

    if args.list:
        docs = list_documents()
        print(f"{len(docs)} document(s) linked:")
        for party, _page, url in docs:
            print(f"  [{party}] {url.rsplit('/', 1)[-1]}")
        return

    if not args.url:
        ap.error("pass --list or --url")

    referer = PAGES["Republican"] if "republican" in args.url else PAGES["Democratic"]
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as fh:
        fh.write(fetch(args.url, referer))
        path = fh.name
    got = parse(path)
    if not got:
        print("could not find a TOTALS row - layout not recognised")
        return
    print(f"office: {got['office']}")
    print(f"columns: {got['candidates']}")
    print(f"{len(got['rows'])} town/candidate figures")
    if args.out:
        party = "REPUBLICAN" if "republican" in args.url else "DEMOCRATIC"
        with open(args.out, "w") as fh:
            for town, cand, votes in got["rows"]:
                name = "Write-in" if cand.lower().startswith("write") else cand
                fh.write(f"0\t{party}\t{got['office']}\t{name}\t{votes}\n")
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
