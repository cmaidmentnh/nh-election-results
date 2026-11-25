# 1998-2014 general elections, rebuilt from archived SoS files

The Secretary of State's result files for these years are gone from sos.nh.gov, but the
Internet Archive kept them. 1998-2010: HTML pages (`want.txt`; 2010 is PDF only). 2012 and
2014: the Excel workbooks behind sos.nh.gov/WorkArea/DownloadAsset.aspx (`dl2012.txt`,
`dl2014.txt`). `fetch1.sh` pulls a snapshot from after the recounts.

- `grid.py` / `parse.py`: HTML pages read by column position (a blank cell stays blank);
  `xlgrid.py` feeds .xls/.xlsx workbooks to the same parser; `pdfgrid.py` does 2010 from
  PDF word positions. Every district block is checked against its printed TOTALS row, and a
  candidate header with no vote rows under it is reported (a dropped block passes a TOTALS
  check - that is how Lebanon 2010 and Exeter 2014 were missed at first).
- `build.py <year> <folder>` writes `out_<year>.json`; `validate.py` prints seat counts;
  `sumcheck.py` compares statewide totals with the SoS statewide summaries.
- `../../import_sos_archive.py` loads `data/sos_archive/general_<year>.json.gz` (years <= 2014 only).

House seat splits against the published counts (Ballotpedia/Dubin):
2010 298-102, 2012 221-179 D, 2014 239-160-1 match exactly. 2002-2008 are within 1-2 seats:
both-party nominees ("r&d") are counted by their first-listed party. 2000 is 144-254 against
a published 140-256 and is not yet explained.

Not covered: 1996 (archived pages too damaged), 1998 State House (never archived), 2012
Executive Council and 2014 State Senate (files never archived; 3 of 24 2014 Senate districts
exist and were left out rather than load a partial year). House sheets print surnames only.
Kept as printed: a few SoS TOTALS rows that disagree with their own town rows by a handful of
votes; Belknap 2 (2000), settled by the Ballot Law Commission; Senate 10 (1998), one name blank.
