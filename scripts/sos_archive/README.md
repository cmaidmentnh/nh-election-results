# 1998-2010 general elections, rebuilt from archived SoS pages

The Secretary of State's own result pages for 1996-2010 are gone from sos.nh.gov but the
Internet Archive kept them (`want.txt` lists every page used; `fetch1.sh` pulls the
snapshot nearest 2012, i.e. after recounts).

- `grid.py` / `parse.py`: the Excel-exported HTML pages, read by column position so a blank
  cell stays blank. Every district block is checked against the TOTALS row printed under it.
- `pdfgrid.py`: 2010 was published only as PDF; same idea from word positions.
- `build.py <year> <folder>`: writes `out_<year>.json`; `validate.py` and `sumcheck.py` print
  seat counts and compare statewide totals with the SoS statewide summary pages.
- `../../import_sos_archive.py` loads `data/sos_archive/general_<year>.json.gz`.

Checks at import time: every Governor / US Senate / President total matches the SoS summary
page to the vote; State Senate 24 districts each year; House 400 seats each year 2000-2010.
The House sheets print surnames only. Not covered: 1996 (archived pages too damaged) and the
1998 State House (no pages archived). Known oddities kept as printed: a few SoS TOTALS rows
that disagree with their own town rows by a handful of votes; Belknap 2 (2000), settled by the
Ballot Law Commission; Senate 10 (1998), one candidate's name blank on the sheet.
