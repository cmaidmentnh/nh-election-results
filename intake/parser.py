"""Claude-backed extraction of vote totals from free-form reports.

Two calls per message:

  1. identify_town - which polling place is this? (skipped when the town name
     matched exactly in Python, which is the common case)
  2. extract      - given that town's exact ballot, attach numbers to race ids
                    and candidate ids

The model is never asked to invent a race or a candidate. It picks from the
ids it is given, and anything it cannot place comes back unmatched so a human
sees it rather than the parser guessing.
"""

import base64
import logging
import re
import mimetypes
from pathlib import Path

import os

import anthropic
from pydantic import BaseModel, Field

from intake import config

log = logging.getLogger("intake.parser")

_client = None
READ_TIMEOUT_SUBSCRIPTION = float(os.environ.get("INTAKE_READ_TIMEOUT", "240"))
# Every subscription read is its own Claude Code CLI process (~230 MB). WORKERS
# x MAX_READS could start 50+ at once, and the box ran out of memory during the
# dry runs. Cap the reads in flight across the whole service.
import threading as _threading
_READ_SLOTS = _threading.BoundedSemaphore(int(os.environ.get("INTAKE_MAX_CONCURRENT_READS", "10")))
READ_TIMEOUT_API = float(os.environ.get("INTAKE_API_TIMEOUT", "300"))

if config.LLM_BACKEND == "subscription":
    # Claude Code prefers ANTHROPIC_API_KEY over the plan's OAuth token, and the
    # Agent SDK hands the whole process environment to it - so leaving the key
    # in the environment would quietly bill the API for every "subscription"
    # read. The key stays in config for the fallback client only.
    os.environ.pop("ANTHROPIC_API_KEY", None)
    os.environ.pop("ANTHROPIC_AUTH_TOKEN", None)


def client():
    global _client
    if _client is None:
        # A read that never returns held a worker forever; with 16 workers a
        # handful of hung reads stops the whole night. One retry, then fail
        # loudly to the review queue (runner.process).
        _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY or None,
                                      timeout=READ_TIMEOUT_API, max_retries=1)
    return _client


def _via_subscription(system, content, schema):
    """One read on the Claude plan through the Agent SDK. Returns the parsed JSON."""
    import asyncio
    from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query

    async def prompt():
        yield {"type": "user", "message": {"role": "user", "content": content},
               "parent_tool_use_id": None, "session_id": ""}

    async def run():
        opts = ClaudeAgentOptions(
            model=config.MODEL, system_prompt=system, tools=[], max_turns=4,
            output_format={"type": "json_schema", "schema": schema},
            setting_sources=[], cwd=str(config.ATTACH_DIR),
        )
        result = None
        # Drain the stream rather than returning from inside it: leaving the
        # generator mid-iteration makes the SDK log a close error every read.
        async for m in query(prompt=prompt(), options=opts):
            if isinstance(m, ResultMessage):
                result = m
        if result is None:
            raise RuntimeError("subscription read returned no result")
        if result.is_error or result.structured_output is None:
            raise RuntimeError(f"subscription read failed: {result.subtype} {str(result.result)[:200]}")
        return result.structured_output

    async def bounded():
        return await asyncio.wait_for(run(), timeout=READ_TIMEOUT_SUBSCRIPTION)

    try:
        return asyncio.run(bounded())
    except asyncio.TimeoutError:
        raise RuntimeError(f"subscription read timed out after {READ_TIMEOUT_SUBSCRIPTION:.0f}s")


def _json_call(system, content, model_cls, api_call):
    """Run a read on the configured plan; fall back to the API key if the plan refuses."""
    with _READ_SLOTS:
        if config.LLM_BACKEND == "subscription":
            try:
                data = _via_subscription(system, content, _strict_schema(model_cls.model_json_schema()))
                return model_cls.model_validate(data)
            except Exception as exc:
                if not config.ANTHROPIC_API_KEY:
                    raise
                log.warning("subscription read failed (%s); using the API key for this one",
                            str(exc)[:160])
        return api_call()


class TownGuess(BaseModel):
    municipality: str = Field(description="Exact polling place name from the list, or empty string if unclear")
    confidence: float = Field(description="0.0 to 1.0")
    reasoning: str


class VoteLine(BaseModel):
    race_id: int = Field(description="race_id from the ballot, or 0 if none fits")
    candidate_id: int = Field(description="candidate_id from the ballot, or 0 if none fits")
    candidate_text: str = Field(description="The name exactly as the reporter wrote it")
    race_text: str = Field(description="The race exactly as the reporter wrote it, if stated")
    votes: int
    is_writein: bool = Field(
        description="True when the sheet lists this name in a write-in section or "
                    "marks it as a write-in. False for a name printed on the ballot.")
    confidence: float = Field(description="0.0 to 1.0 for this one line")


class BallotsLine(BaseModel):
    election_id: int = Field(description="election_id whose ballots these are, or 0 if unclear")
    ballots_cast: int
    confidence: float


class RaceCheck(BaseModel):
    race_id: int = Field(description="race_id this checksum belongs to")
    stated_total: int = Field(description="The TOTAL VOTES CAST printed for this race, or 0 if the sheet does not print one")
    blanks: int = Field(description="BLANKS or UNDERVOTES printed for this race, or 0")
    overvotes: int = Field(description="OVERVOTES printed for this race, or 0")


class Extraction(BaseModel):
    municipality_text: str = Field(description="The place name as the reporter wrote it")
    contains_results: bool = Field(description="False for chatter, questions, acknowledgements")
    notes: str = Field(description="Anything a human reviewer should know; empty if nothing")
    report_scope: str = Field(
        description="One of: 'machine_tape' (one machine's tape, part of the town), "
                    "'town_total' (the town's combined total for these races), "
                    "'unknown'. Judge from the sheet itself; do not guess.")
    device_label: str = Field(
        description="Any machine/device/tape identifier printed on the report "
                    "(e.g. 'Machine 1 of 2', 'AccuVote #3', 'District 2 tape'), "
                    "empty string if none. Never invent one.")
    header_place: str = Field(
        default="",
        description="The town, city and/or ward printed ON THE SHEET ITSELF (tape header, "
                    "form title), copied exactly. Never taken from the email subject, the "
                    "message text or the polling place you were told. Empty if the sheet "
                    "names no place.")
    election_header: str = Field(
        default="",
        description="The election name and date printed on the sheet, copied exactly "
                    "(e.g. 'STATE GENERAL ELECTION NOVEMBER 3, 2026', 'State Primary "
                    "Election September 8, 2026', 'REPUBLICAN BALLOT'). Empty if none.")
    tapes_not_entered: int = Field(
        default=0,
        description="How many OTHER machine tapes or tally sheets for this polling place "
                    "appear in the attachments but were NOT entered in lines (because only "
                    "one tape is entered per report). 0 if every tape shown was entered.")
    lines: list[VoteLine]
    ballots: list[BallotsLine]
    checks: list[RaceCheck] = Field(
        description="One entry per race whose sheet prints a total, for arithmetic verification")


TOWN_SYSTEM = """You identify which New Hampshire polling place an election-night \
report is about.

Reply with the polling place name copied EXACTLY from the list you are given, or \
an empty string if you cannot tell. Never invent a name or return one that is not \
on the list.

New Hampshire towns are often confused with each other. Take care with:
- Ward-split cities (Manchester, Nashua, Concord, Dover, Rochester, Keene, Laconia, \
Portsmouth, Somersworth, Berlin, Claremont, Franklin, Lebanon). A report from one of \
these must name the ward. Without a ward number, return an empty string.
- Similar names: Hampton / Hampton Falls / North Hampton / East Kingston / Kingston, \
Newport / Newington / Newmarket / New Ipswich / Newton, Lyme / Lyman / Lyndeborough."""

# The ballot rules depend on the event. A state primary is two party ballots
# (R + D) on the same day; a general (election_type 'general' in the elections
# table, as in 2024) is one ballot per polling place. Matched exactly against
# config.EVENT_TYPE (INTAKE_ELECTION_TYPE), not by substring.
PRIMARY_EVENT_TYPES = ("state_primary",)

PRIMARY_BALLOT_RULES = """THIS IS A PRIMARY. THERE ARE TWO SEPARATE BALLOTS.
The Republican ballot and the Democratic ballot are different elections held the \
same day. The same office appears on BOTH, with a DIFFERENT race_id and a \
different set of candidates. Governor on the Republican ballot and Governor on the \
Democratic ballot are not the same race. Choosing the wrong one files a \
candidate's votes under the wrong party, which is worse than not filing them at \
all. So:
- A report usually states its party once, at the top ("Republican primary", "GOP", \
  "R side", "Dem results"). That party applies to every line in the report until \
  the report says otherwise.
- If the party is never stated, use the ballot the reported candidates actually \
  appear on - the two candidate lists do not overlap.
- If a line could sit on either ballot and nothing resolves it, return race_id=0 \
  and let a human decide. Never guess between the two."""

GENERAL_BALLOT_RULES = """THIS IS A GENERAL ELECTION. Each polling place has one ballot, and each \
office on it has one race_id. A party label printed beside a candidate's name identifies the \
candidate; it does not mean a separate ballot. If a line cannot be matched to a race on the \
ballot, return race_id=0 and let a human decide."""

PRIMARY_BALLOTS_NOTE = """ A primary reports \
ballots cast per party, so "1,420 Republican ballots" uses the Republican \
election_id."""

# Photo-reading: a primary tally sheet can carry R and D side by side. A general
# has one ballot, so there is no second ballot to split columns into.
PRIMARY_BALLOT_LABEL_NOTE = """- The sheet usually says which ballot it is - "REPUBLICAN", "DEMOCRATIC", "REP", \
"DEM", often in the header or as a column heading. Use it. If one sheet has \
separate Republican and Democratic columns, read each into its own ballot's \
race_id.
"""

IS_PRIMARY_EVENT = config.EVENT_TYPE in PRIMARY_EVENT_TYPES

EXTRACT_SYSTEM = """You read election-night vote reports from New Hampshire town \
clerks and party volunteers and turn them into structured vote totals.

You are given the EXACT ballot for one polling place: every race with its race_id, \
and every candidate with its candidate_id. Your only job is to attach the reported \
numbers to those ids.

{ballot_rules}

Every race listed in the ballot above IS on this town's ballot. If the report names \
an office you can see above - including abbreviations like "Exec Council D4", "EC4", \
"CD1", "Sheriff", "Reg of Deeds" - use that race_id. Do not claim a race is absent \
when it is listed.

Other rules:
- Only ever use a race_id and candidate_id that appear in the ballot above. Never \
invent one.
- If a reported name does not match any candidate on that ballot, still return the \
line with candidate_id=0 and the name in candidate_text. A human will resolve it. \
Do NOT force it onto the nearest name.
- Match on surname where the ballot is unambiguous; if two candidates in the same \
race share a surname, only match when the report distinguishes them, otherwise \
return candidate_id=0.
- Votes written as "412" and "412 votes" and "412 (52%)" are all 412. Never derive a \
vote count from a percentage.
- A total ballots cast / turnout figure is NOT a candidate vote. Put it in ballots, \
using the election_id shown in that ballot's header above.{ballots_note}
- For every race where the sheet prints a TOTAL VOTES CAST (and/or BLANKS,
UNDERVOTES, OVERVOTES), copy those figures into checks for that race_id. Copy
them exactly as printed; do not compute them yourself and do not infer a total
that is not on the page. They are used to verify the candidate numbers add up,
so a guessed total defeats the check. Leave a figure as 0 when the sheet does
not print it.
- Set confidence per line. Use a low value when handwriting is unclear, when a digit \
is ambiguous, or when the name match was a stretch.
- If the message contains no vote totals at all (a question, a greeting, "on my way"), \
set contains_results=false and return no lines.
- When the attachments show more than one machine tape, enter one and set \
tapes_not_entered to how many others you left out.
- header_place is the place printed on the sheet itself and election_header the \
election printed on it - copy both from the sheet only, never from the message.
REPORTS ARRIVE IN TWO SHAPES, AND WHICH ONE MATTERS.
A town may send each machine's tape first and a combined town total afterwards.
Set report_scope to 'machine_tape' when the sheet is one device's output (it
names a machine, device or tape number, or is plainly a printer tape), and
'town_total' when it is the town's combined return for those races (a Return of
Votes form, a sheet headed with the town name and totals, or one that reconciles
to the town's ballots cast). Use 'unknown' when the sheet does not make it clear
- do not guess, because the two are combined differently downstream.

MANY TOWNS COUNT ON SEVERAL MACHINES AND SEND ONE TAPE PER MACHINE.
A tape may therefore hold only part of a town's vote. If the report names a
machine, device, or tape number, put it in device_label exactly as printed.
Never add tapes together yourself and never scale a number up to what you think
the town total should be - report only what this sheet shows.

PHOTOGRAPHS AND SCANNED TALLY SHEETS ARE THE NORMAL CASE.
Most reports are a phone photo of the machine tape or the hand-tally sheet, or a
scanned PDF of it, with little or no typed text. Read them carefully:
- Work down the sheet race by race. A New Hampshire tally sheet lists the office, \
then each candidate with a vote total beside or beneath the name.
{ballot_label_note}- These lines are NOT candidates and must never be returned as candidate votes: \
TOTAL, TOTAL VOTES CAST, BLANKS, BLANK, UNDERVOTES, OVERVOTES, SCATTERING, \
SCATTERED, VOID, SPOILED, ABSENTEE (as a column heading), REGISTERED VOTERS. \
A "TOTAL BALLOTS CAST" or "BALLOTS CAST" figure goes in ballots, not lines.
- "SCATTERING" or a plain "WRITE-IN" total is the aggregate write-in line - use \
the "Write-in (aggregate line)" candidate_id for that race.
- A HAND-COUNT TOWN LISTS EACH WRITTEN-IN NAME SEPARATELY, under a "write-in" \
heading or beside a "W/I" mark. Report every one: set is_writein=true, put the \
person's name in candidate_text, candidate_id=0, and use that race's race_id. \
Copy the name exactly as it is written, letter for letter, however odd it looks \
and even if it is not a person ("NOTA", "None of the above") - it will be matched \
downstream, and a name you tidied up cannot be matched to the same name read off \
another line. Leave candidate_id=0 unless the name is already listed for that \
race - including one tagged "(write-in)", which an earlier town in the same \
district has already reported - in which case use that candidate_id.
- A VotingWorks "Write-In Adjudication Report" is a breakdown of the tally, not \
extra votes. A name it lists under "Official Candidates" is ALREADY counted in that \
candidate's own line on the Tally Report - do not return it at all. Names under \
"Write-In Candidates" break down that race's Write-In line; return them as write-ins.
- On a hand-count sheet, a vote written in for a candidate who is also printed on \
the ballot is a separate line; return it as its own is_writein=true line and never \
add it into the printed line yourself.
- Set is_writein=false on every line that is not a write-in.
- Multi-seat State Representative races list many candidates at once; report every \
one you can read.
- Numbers are often handwritten. Distinguish carefully between 1/7, 3/8, 5/6, and \
0/6/8. If a digit is genuinely ambiguous, lower the confidence for that line - do \
not silently pick one.
- Read only what is printed. If a number is cut off, smudged, obscured by glare, or \
outside the frame, omit that line and say so in notes rather than guessing.
- If the photograph is too blurry or too dark to read at all, set \
contains_results=false and say so in notes."""

EXTRACT_SYSTEM = (EXTRACT_SYSTEM
                  .replace("{ballot_rules}",
                           PRIMARY_BALLOT_RULES if IS_PRIMARY_EVENT else GENERAL_BALLOT_RULES)
                  .replace("{ballots_note}",
                           PRIMARY_BALLOTS_NOTE if IS_PRIMARY_EVENT else "")
                  .replace("{ballot_label_note}",
                           PRIMARY_BALLOT_LABEL_NOTE if IS_PRIMARY_EVENT else ""))


# Claude Opus 5 reads up to 2576px on the long edge. Tally tapes are dense
# columns of small digits, so send at that ceiling rather than downscaling
# further - and no larger, which would only cost tokens.
MAX_EDGE = 2576
# Primary night: one Hudson report carried 29 tape photos and the old cap of 12
# dropped 17 of them without a word. 30 covers a busy town; past that the rest
# are listed for a human (runner queues a note) instead of vanishing.
MAX_ATTACHMENTS = 30
# The API takes at most 100 images a request, and above 20 every image must be
# within 2000px. Past BIG_REQUEST_IMAGES the tiles are cut at 2000px instead.
MAX_IMAGES = 100
BIG_REQUEST_IMAGES = 20
BIG_REQUEST_EDGE = 2000
# Whole request stays well under the 32MB request ceiling.
MAX_REQUEST_BYTES = 24 * 1024 * 1024
# A long tape becomes several tiles; cap the total so one photo cannot
# swamp a request.
MAX_TILES = 6
MAX_PDF_BYTES = 20 * 1024 * 1024


def _sniff(data):
    """Content type from magic bytes.

    Filenames cannot be trusted here: signal-cli stores attachments under a
    bare id with no extension, so guessing by suffix skips every photo posted
    to the Signal group.
    """
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:5] == b"%PDF-":
        return "application/pdf"
    if data[4:8] == b"ftyp" and data[8:12] in (b"heic", b"heix", b"hevc", b"mif1", b"msf1"):
        return "image/heic"
    if data[:4] == b"PK\x03\x04":
        # A zip container. .xlsx is the one that matters: at least one town sent
        # its results as a spreadsheet in 2024.
        return "application/zip"
    return None


def _prepare_image(data, media_type, max_edge=None):
    """Re-encode to JPEG within the model's resolution ceiling.

    Returns a LIST of images: a long tally tape shrunk to fit 2576px on its
    long edge would have its digits squashed into illegibility, so tall images
    are cut into overlapping tiles at full readable width instead. The overlap
    keeps a row that falls on a seam readable in one tile or the other.

    Phone photos arrive at 12MP and, from iPhones, often as HEIC, which the API
    does not accept; both are handled here so the caller does not care.
    """
    try:
        import io

        from PIL import Image
        try:  # iPhone photos
            import pillow_heif
            pillow_heif.register_heif_opener()
        except ImportError:
            if media_type == "image/heic":
                return []

        from PIL import ImageOps
        img = Image.open(io.BytesIO(data))
        img.load()
        # Phones store a portrait photo sideways with an Orientation tag; the
        # model is sent pixels, not tags, so turn it upright first.
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        # Scale by WIDTH so text keeps its size, then tile down the length.
        edge = max_edge or MAX_EDGE
        if img.width > edge:
            ratio = edge / img.width
            img = img.resize((edge, max(1, int(img.height * ratio))), Image.LANCZOS)

        # More tiles than the cap used to cut the bottom of a long tape off
        # without a word (the last races). Shrink until it fits, and say so.
        step_ = edge - 220
        need = 1 + max(0, -(-(img.height - edge) // step_))
        if need > MAX_TILES:
            target_h = edge + step_ * (MAX_TILES - 1)
            ratio = target_h / img.height
            log.warning("Tall image %dx%d needs %d tiles; shrinking to fit %d",
                        img.width, img.height, need, MAX_TILES)
            img = img.resize((max(1, int(img.width * ratio)), target_h), Image.LANCZOS)

        pieces = []
        if img.height <= edge:
            pieces.append(img)
        else:
            overlap = 220
            step = edge - overlap
            top = 0
            while top < img.height and len(pieces) < MAX_TILES:
                bottom = min(top + edge, img.height)
                pieces.append(img.crop((0, top, img.width, bottom)))
                if bottom >= img.height:
                    break
                top += step

        out = []
        for piece in pieces:
            buf = io.BytesIO()
            piece.save(buf, format="JPEG", quality=88, optimize=True)
            out.append((buf.getvalue(), "image/jpeg"))
        return out
    except Exception:
        log.exception("Could not re-encode an attachment")
        if media_type in ("image/jpeg", "image/png", "image/gif", "image/webp"):
            return [(data, media_type)]
        return []


def _spreadsheet_text(path):
    """An .xlsx of results rendered as plain rows the model can read."""
    try:
        from openpyxl import load_workbook
    except ImportError:
        log.warning("openpyxl not installed; cannot read %s", path)
        return ""
    try:
        wb = load_workbook(path, read_only=True, data_only=True)
    except Exception:
        log.exception("Could not open spreadsheet %s", path)
        return ""
    out = []
    for ws in wb.worksheets:
        out.append(f"--- sheet: {ws.title} ---")
        for row in ws.iter_rows(values_only=True):
            cells = ["" if c is None else str(c).strip() for c in row]
            if any(cells):
                out.append(" | ".join(cells).rstrip(" |"))
            if len(out) > 4000:
                out.append("(truncated)")
                break
    wb.close()
    return "\n".join(out)


def _xls_text(path):
    """A legacy Excel 97-2003 .xls (Claremont's tally sheet) as plain rows.
    Needs xlrd in the intake venv; without it the message is queued as
    unreadable (runner) rather than dropped."""
    try:
        import xlrd
    except ImportError:
        log.warning("xlrd not installed; cannot read legacy .xls %s", path)
        return ""
    try:
        book = xlrd.open_workbook(str(path))
    except Exception:
        log.exception("Could not open .xls %s", path)
        return ""
    out = []
    for sh in book.sheets():
        out.append(f"--- sheet: {sh.name} ---")
        for r in range(min(sh.nrows, 4000)):
            cells = ["" if v is None else str(v).strip() for v in sh.row_values(r)]
            if any(cells):
                out.append(" | ".join(cells).rstrip(" |"))
    return "\n".join(out)


def readable_content(paths):
    """Whether any attachment produced something the model can actually see."""
    return bool(_attachment_blocks(paths) or spreadsheet_texts(paths))


def _document_text(path):
    """A .docx of results as plain text.

    A Word file is a zip like .xlsx, so it passed the container check and was
    then handed to openpyxl, which raised, logged, and returned nothing. The
    message read as chatter and was dropped without a trace - Gorham sent its
    whole town that way and published zero rows. Read the document part
    directly rather than adding a dependency mid-count.
    """
    import re
    import zipfile
    try:
        with zipfile.ZipFile(path) as zf:
            names = [n for n in ("word/document.xml",) if n in zf.namelist()]
            if not names:
                return ""
            xml = zf.read(names[0]).decode("utf-8", "replace")
    except Exception:
        log.exception("Could not open document %s", path)
        return ""
    # Paragraph and row breaks first, so a table of votes keeps its lines.
    xml = re.sub(r"</w:(p|tr)>", "\n", xml)
    xml = re.sub(r"</w:tc>", " | ", xml)
    text = re.sub(r"<[^>]+>", "", xml)
    lines = [ln.strip(" |\t") for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


def spreadsheet_texts(paths):
    """Text extracted from spreadsheet or word-processor attachments.

    Clerks send whatever their office produces - a tape photo, a scanned PDF,
    a spreadsheet, or a Word document. Anything that carries numbers has to
    reach the model, because a report we cannot read is indistinguishable from
    a town that never reported.
    """
    chunks = []
    for p in (paths or []):
        path = Path(p)
        try:
            if not path.exists():
                continue
            head = path.open("rb").read(8)
        except OSError:
            continue
        suffix = path.suffix.lower()
        if suffix in (".csv", ".tsv", ".txt"):
            try:
                text = path.read_text("utf-8", errors="replace")[:200000]
            except OSError:
                text = ""
            label = "text attachment"
        elif head[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" or suffix == ".xls":
            text = _xls_text(path)
            label = "spreadsheet attachment"
        elif suffix in (".docx", ".docm"):
            text = _document_text(path)
            label = "document attachment"
        elif head[:4] == b"PK\x03\x04" or suffix in (".xlsx", ".xlsm"):
            text = _spreadsheet_text(path)
            label = "spreadsheet attachment"
        else:
            continue
        if text:
            chunks.append(f"[{label}: {path.name}]\n{text}")
    return "\n\n".join(chunks)


def unread_attachments(paths):
    """Attachments this report carries that the reader will not be shown."""
    return _attachment_plan(paths)[1]


_plan_cache = {}


def _attachment_plan(paths):
    """(blocks, unread_paths) within the per-request image, size and count limits."""
    key = tuple(paths or [])
    if key in _plan_cache:
        return _plan_cache[key]
    blocks, unread = _build_blocks(paths, MAX_EDGE)
    if sum(1 for b in blocks if b["type"] == "image") > BIG_REQUEST_IMAGES:
        blocks, unread = _build_blocks(paths, BIG_REQUEST_EDGE)
    if len(_plan_cache) > 256:
        _plan_cache.clear()
    _plan_cache[key] = (blocks, unread)
    return blocks, unread


def _attachment_blocks(paths, limit=MAX_ATTACHMENTS):
    """Photos and PDFs of tally sheets as content blocks.

    Most reports on election night are a picture of the tape or a scanned
    printout rather than typed numbers, so this is the main input path.
    """
    blocks, _unread = _attachment_plan(paths)
    return [dict(b) for b in blocks]


def _build_blocks(paths, max_edge):
    paths = list(paths or [])
    unread = paths[MAX_ATTACHMENTS:]
    blocks = []
    size = 0
    for p in paths[:MAX_ATTACHMENTS]:
        path = Path(p)
        try:
            if not path.exists() or path.stat().st_size == 0:
                continue
            data = path.read_bytes()
        except OSError:
            log.exception("Could not read attachment %s", p)
            continue

        media_type = _sniff(data) or mimetypes.guess_type(str(path))[0]

        if media_type == "application/pdf":
            if len(data) > MAX_PDF_BYTES:
                log.warning("Skipping oversized PDF %s (%.1f MB)", p, len(data) / 1e6)
                unread.append(p)
                continue
            if size + len(data) * 4 // 3 > MAX_REQUEST_BYTES:
                unread.append(p)
                continue
            size += len(data) * 4 // 3
            blocks.append({
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf",
                           "data": base64.standard_b64encode(data).decode()},
            })
            continue

        if not media_type or not media_type.startswith("image/"):
            continue

        pieces = _prepare_image(data, media_type, max_edge)
        n_images = sum(1 for b in blocks if b["type"] == "image")
        piece_bytes = sum(len(x) * 4 // 3 for x, _t in pieces)
        if (n_images + len(pieces) > MAX_IMAGES
                or size + piece_bytes > MAX_REQUEST_BYTES):
            unread.append(p)
            continue
        size += piece_bytes
        for prepared, out_type in pieces:
            blocks.append({
                "type": "image",
                "source": {"type": "base64", "media_type": out_type,
                           "data": base64.standard_b64encode(prepared).decode()},
            })
    if unread:
        log.warning("%d attachment(s) over the per-report limit, not read: %s",
                    len(unread), ", ".join(Path(u).name for u in unread[:5]))
    return blocks, unread


def identify_town(town_list, body, subject="", sender="", attachments=None):
    content = _attachment_blocks(attachments)
    content.append({
        "type": "text",
        "text": (
            f"Polling places:\n{town_list}\n\n"
            f"--- REPORT ---\nFrom: {sender}\nSubject: {subject}\n\n{body or '(no text; see image)'}"
        ),
    })
    def api_call():
        resp = client().messages.parse(
            model=config.MODEL,
            max_tokens=4000,
            system=TOWN_SYSTEM,
            messages=[{"role": "user", "content": content}],
            output_format=TownGuess,
            output_config={"effort": "low"},
        )
        return resp.parsed_output
    return _json_call(TOWN_SYSTEM, content, TownGuess, api_call)


def extract(municipality, roster_text, body, subject="", sender="", attachments=None,
            max_tokens=None, _retry=True):
    content = _attachment_blocks(attachments)
    content.append({
        "type": "text",
        "text": (
            f"POLLING PLACE: {municipality}\n"
            f"BALLOT FOR THIS POLLING PLACE:\n{roster_text}\n\n"
            f"--- REPORT ---\nFrom: {sender}\nSubject: {subject}\n\n"
            f"{body or '(no text; results are in the attached file)'}"
        ),
    })
    sheets = spreadsheet_texts(attachments)
    if sheets:
        content.append({"type": "text", "text": f"\n--- ATTACHED SPREADSHEET ---\n{sheets}"})
    # Stream every read at the full ceiling rather than trying a small
    # non-streaming call first. A town's return-of-votes sheet routinely runs
    # past 16k output tokens and comes back as truncated JSON; the retry then
    # re-read the whole sheet from scratch, so the biggest towns - the ones
    # people are waiting on - were the slowest to publish. Streaming is the
    # only way the SDK allows a ceiling this high, and one read at 48k beats
    # a read at 16k plus a second read at 48k.
    try:
        return _extract_streamed(content, max_tokens=max_tokens)
    except Exception as exc:
        if _retry:
            log.warning("%s: extraction failed (%s); one more try",
                        municipality, str(exc)[:140])
            return _extract_streamed(content, max_tokens=max_tokens)
        raise


def _strict_schema(node):
    """Pydantic's JSON schema, with additionalProperties pinned false throughout.

    The API rejects an object schema that does not say so explicitly, and the
    rejection only shows up on the streamed retry - which is the path a report
    with six tape photos takes. Goffstown's tapes died on it with a 400.
    """
    if isinstance(node, dict):
        if node.get("type") == "object" or "properties" in node:
            node["additionalProperties"] = False
            if "properties" in node:
                node["required"] = list(node["properties"])
                for prop in node["properties"].values():
                    if isinstance(prop, dict):
                        prop.pop("default", None)
        for value in node.values():
            _strict_schema(value)
    elif isinstance(node, list):
        for value in node:
            _strict_schema(value)
    return node


def _extract_streamed(content, max_tokens=None):
    """One extraction with a large output ceiling, which requires streaming."""
    return _json_call(EXTRACT_SYSTEM, content, Extraction,
                      lambda: _extract_streamed_api(content, max_tokens))


def _extract_streamed_api(content, max_tokens=None):
    with client().messages.stream(
        model=config.MODEL,
        max_tokens=max_tokens or config.MAX_OUTPUT_TOKENS_RETRY,
        system=EXTRACT_SYSTEM,
        thinking={"type": "adaptive"},
        messages=[{"role": "user", "content": content}],
        output_config={"format": {"type": "json_schema",
                                  "schema": _strict_schema(Extraction.model_json_schema())}},
    ) as stream:
        message = stream.get_final_message()
    text = next((b.text for b in message.content if b.type == "text"), None)
    if text is None:
        # A bare StopIteration here used to surface as an error with no text at
        # all, so nobody could tell a refusal from a crash.
        raise ValueError(f"no text in the model's reply (stop_reason={message.stop_reason})")
    return Extraction.model_validate_json(text)


def line_key(line):
    """Identity of one reported line across independent reads.

    Matched lines are identified by the ids they resolved to. Unmatched lines
    have race_id=0 and candidate_id=0, so keying on ids alone would collapse
    every unmatched line in a report into a single bucket - and then a value
    agreed for one of them would be written onto all of them. Fall back to the
    text the reporter actually wrote, which is what distinguishes them.
    """
    if line.race_id and line.candidate_id:
        return ("id", line.race_id, line.candidate_id)
    # Normalise hard: two reads of the same page often differ only in
    # punctuation ("Donald J. Trump" vs "Donald J Trump"), and treating those
    # as different lines would report a disagreement that is not one.
    def norm(t):
        return re.sub(r"[^a-z0-9]+", "", (t or "").lower())

    # Once the race is resolved to an id, that id IS the race - what the
    # reporter called it is not part of the line's identity any more. Keying on
    # the text held Carroll's write-ins for a second time after they were
    # matched: five reads had all found "NOTA, 1" in the same race, but one had
    # copied the office down as "State Representative" and another as "STATE
    # REP CARROLL 1", so they counted as five different lines seen once each
    # and none of them ever agreed with anything.
    race = ("race", line.race_id) if line.race_id else ("text", norm(line.race_text))
    return ("text", race, norm(line.candidate_text))


def _occ_keys(lines):
    """line_key plus its occurrence number within one read.

    A message carrying four tabulator tapes has four Ayotte lines - four
    numbers, not one number said four times. Keyed on (race, candidate) alone,
    the reads collapsed onto the first tape: Derry's four tapes published
    Ayotte 698 against a true ~2,174 for 51 minutes on primary night."""
    seen, out = {}, []
    for l in lines:
        k = line_key(l)
        n = seen.get(k, 0)
        seen[k] = n + 1
        out.append(k + (n,))
    return out


def _tally(reads):
    """Per (race, candidate, occurrence), what each read said. Missing counts against it."""
    votes = {}
    per_read = [dict(zip(_occ_keys(r.lines), (l.votes for l in r.lines))) for r in reads]
    for d in per_read:
        for key in d:
            votes.setdefault(key, [])
    for key in votes:
        for d in per_read:
            votes[key].append(d.get(key))
    return votes


def _verdict(values):
    """(winning_value, agreed?) for one line's readings across every pass.

    A supermajority is required, never a bare majority. Repeated reading fixes
    RANDOM misreads - a smudged digit read differently each time - but it does
    not fix SYSTEMATIC ones. If the model consistently reads the wrong column of
    a layout it will do so every pass, and a bare 3-2 majority would then
    manufacture false confidence in a wrong number. So a close split is treated
    as unresolved and sent to a human, which is the honest answer.
    """
    present = [v for v in values if v is not None]
    if not present:
        return None, False
    counts = {}
    for v in present:
        counts[v] = counts.get(v, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: -kv[1])
    top_value, top_count = ranked[0]
    runner_up = ranked[1][1] if len(ranked) > 1 else 0
    n = len(values)

    if n <= 2:
        return top_value, top_count == n            # both reads, same answer
    if top_count >= 4:
        return top_value, True                      # 4+ of 5 agree
    if top_count >= 3 and runner_up <= 1:
        return top_value, True                      # 3-1-1, one clear answer
    return top_value, False                         # 3-2 or 2-2-1: genuinely split


def extract_consensus(municipality, roster_text, body, subject="", sender="",
                      attachments=None, max_reads=None, settle_names=None):
    """Read the report until the readings settle, then report what is solid.

    Back-testing against real 2024 clerk PDFs showed the model reporting high
    confidence on numbers that were badly wrong - Hollis County Sheriff read as
    2,571 against an actual 5,337. Self-reported confidence therefore cannot be
    the only gate.

    Two reads are taken first. If every line agrees, that is the answer. If any
    line disagrees the whole report is read again, up to max_reads, and each
    line is decided by supermajority - see _verdict for why a bare majority is
    not enough. The winning value replaces whatever the first read said, so a
    line that lost 1-4 publishes the number the other four saw.

    Returns (extraction, agreed_keys, disagreements) where disagreements maps a
    key to the full list of readings, for the review queue to show.

    settle_names, if given, is handed every read before they are compared. A
    line nobody could match to a candidate id is identified by the text the
    reporter wrote, so a written-in name that five reads spelled five ways is
    five single-sighting lines that agree on nothing - see
    roster.canonicalise_writeins, which puts them back together first.
    """
    from concurrent.futures import ThreadPoolExecutor

    def read_once():
        return extract(municipality, roster_text, body, subject, sender, attachments)

    def settle(rs):
        if settle_names:
            settle_names(rs)

    # The two baseline reads are independent, so pay for one round trip, not
    # two. On election night latency per report is what decides whether the
    # board keeps up with the towns.
    failures = []

    def gather(futures):
        out = []
        for f in futures:
            try:
                out.append(f.result())
            except Exception as exc:          # one bad read must not sink the rest
                log.warning("%s: one read failed (%s)", municipality, str(exc)[:160])
                failures.append(exc)
        return out

    with ThreadPoolExecutor(max_workers=2) as pool:
        reads = gather([pool.submit(read_once) for _ in range(2)])
    if len(reads) < 2:
        # Never let a single surviving read publish on its own: agreement needs
        # two. Read again to make up the pair.
        with ThreadPoolExecutor(max_workers=2) as pool:
            reads += gather([pool.submit(read_once) for _ in range(2 - len(reads))])
    if not reads:
        raise failures[-1]
    settle(reads)

    first = reads[0]
    if not first.contains_results or not first.lines:
        return first, set(), {}

    def unsettled(rs):
        return any(not _verdict(v)[1] for v in _tally(rs).values())

    # Escalate in one parallel batch, not one read at a time. Reading serially
    # cost most of a minute per extra read and put an unsettled report two and a
    # half minutes behind the towns; the verdict is the same either way, so pay
    # for one more round trip instead of three.
    limit = max_reads or config.MAX_READS
    if len(reads) < limit and unsettled(reads):
        extra = limit - len(reads)
        with ThreadPoolExecutor(max_workers=extra) as pool:
            reads.extend(gather([pool.submit(read_once) for _ in range(extra)]))
        settle(reads)

    tally = _tally(reads)
    agreed, disagreements = set(), {}
    # Two DIFFERENT numbers for one key inside a single read are two lines on
    # the sheet that resolved to one candidate - a name printed in two party
    # columns that the reader did not sum (the roster asks it to: see
    # roster.cross_filed), or a write-in for a printed candidate. _tally keeps
    # only the first of them and the overwrite below then gives both lines the
    # same value, so apply.py's "two lines for this candidate" guard saw a
    # harmless duplicate and published one column. Hold them instead.
    collided = {}
    for r in reads:
        seen_here = {}
        for l in r.lines:
            k = line_key(l)
            if k in seen_here and seen_here[k] != l.votes:
                collided.setdefault(k, f"{seen_here[k]:,} and {l.votes:,} on two lines of the "
                                       f"sheet for one candidate - add them, or is one a correction?")
            seen_here.setdefault(k, l.votes)
    for key, values in tally.items():
        winner, ok = _verdict(values)
        if len(reads) < 2:
            ok = False                         # one read is an opinion, not a check
        if key[:-1] in collided:
            ok, winner = False, None
        if ok:
            agreed.add(key)
        else:
            disagreements[key] = [collided[key[:-1]]] if key[:-1] in collided else values

        # Publish what the reads actually settled on, not the first attempt.
        if winner is not None:
            for line, k in zip(first.lines, _occ_keys(first.lines)):
                if k == key:
                    line.votes = winner

    # A line only later reads saw at all still belongs in the report.
    seen = set(_occ_keys(first.lines))
    for r in reads[1:]:
        for l, key in zip(r.lines, _occ_keys(r.lines)):
            if key not in seen and key in tally:
                winner, ok = _verdict(tally[key])
                if winner is not None:
                    l.votes = winner
                    first.lines.append(l)
                    seen.add(key)

    log.info("%s: %d reads, %d settled, %d split",
             municipality, len(reads), len(agreed), len(disagreements))
    return first, agreed, disagreements
