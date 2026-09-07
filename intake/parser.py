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
import mimetypes
from pathlib import Path

import anthropic
from pydantic import BaseModel, Field

from intake import config

_client = None


def client():
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY or None)
    return _client


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
    confidence: float = Field(description="0.0 to 1.0 for this one line")


class BallotsLine(BaseModel):
    election_id: int = Field(description="election_id whose ballots these are, or 0 if unclear")
    ballots_cast: int
    confidence: float


class Extraction(BaseModel):
    municipality_text: str = Field(description="The place name as the reporter wrote it")
    contains_results: bool = Field(description="False for chatter, questions, acknowledgements")
    notes: str = Field(description="Anything a human reviewer should know; empty if nothing")
    lines: list[VoteLine]
    ballots: list[BallotsLine]


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

EXTRACT_SYSTEM = """You read election-night vote reports from New Hampshire town \
clerks and party volunteers and turn them into structured vote totals.

You are given the EXACT ballot for one polling place: every race with its race_id, \
and every candidate with its candidate_id. Your only job is to attach the reported \
numbers to those ids.

THIS IS A PRIMARY. THERE ARE TWO SEPARATE BALLOTS.
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
  and let a human decide. Never guess between the two.

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
using the election_id shown in that ballot's header above. A primary reports \
ballots cast per party, so "1,420 Republican ballots" uses the Republican \
election_id.
- Set confidence per line. Use a low value when handwriting is unclear, when a digit \
is ambiguous, or when the name match was a stretch.
- If the message contains no vote totals at all (a question, a greeting, "on my way"), \
set contains_results=false and return no lines.
- Photographs of tally tapes: read only what is printed. If a number is cut off or \
illegible, omit that line and say so in notes rather than guessing."""


def _image_blocks(paths, limit=8):
    """Attachment files as image content blocks. Non-images are skipped."""
    blocks = []
    for p in (paths or [])[:limit]:
        path = Path(p)
        if not path.exists():
            continue
        media_type, _ = mimetypes.guess_type(str(path))
        if not media_type or not media_type.startswith("image/"):
            continue
        if path.stat().st_size > 5 * 1024 * 1024:
            continue
        blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.standard_b64encode(path.read_bytes()).decode(),
            },
        })
    return blocks


def identify_town(town_list, body, subject="", sender="", attachments=None):
    content = _image_blocks(attachments)
    content.append({
        "type": "text",
        "text": (
            f"Polling places:\n{town_list}\n\n"
            f"--- REPORT ---\nFrom: {sender}\nSubject: {subject}\n\n{body or '(no text; see image)'}"
        ),
    })
    resp = client().messages.parse(
        model=config.MODEL,
        max_tokens=4000,
        system=TOWN_SYSTEM,
        messages=[{"role": "user", "content": content}],
        output_format=TownGuess,
    )
    return resp.parsed_output


def extract(municipality, roster_text, body, subject="", sender="", attachments=None):
    content = _image_blocks(attachments)
    content.append({
        "type": "text",
        "text": (
            f"POLLING PLACE: {municipality}\n"
            f"BALLOT FOR THIS POLLING PLACE:\n{roster_text}\n\n"
            f"--- REPORT ---\nFrom: {sender}\nSubject: {subject}\n\n"
            f"{body or '(no text; results are in the attached image)'}"
        ),
    })
    resp = client().messages.parse(
        model=config.MODEL,
        max_tokens=16000,
        system=EXTRACT_SYSTEM,
        thinking={"type": "adaptive"},
        messages=[{"role": "user", "content": content}],
        output_format=Extraction,
    )
    return resp.parsed_output
