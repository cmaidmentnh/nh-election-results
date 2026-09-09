"""Gmail API feed for results@electhouserepublicans.com.

Preferred over IMAP because the mailbox is already authorised - the stored
OAuth token is self-contained (it carries its own client id, secret and refresh
token), so no app password has to be minted or rotated.

Scope discipline: that token also carries gmail.send and calendar, which this
service has no business using. Everything here is read-only apart from stamping
each message we take with our own label (and clearing UNREAD alongside it), and
every message is filtered to the results group before it is touched. Mail in
the mailbox that is not addressed to the group is never fetched.
"""

import base64
import logging
import re
import uuid
from pathlib import Path

from intake import config

log = logging.getLogger("intake.gmail")

_service = None
_label_id = None


def available():
    return bool(config.GMAIL_TOKEN_PATH and Path(config.GMAIL_TOKEN_PATH).exists())


def service():
    global _service
    if _service is None:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_authorized_user_file(config.GMAIL_TOKEN_PATH)
        if not creds.valid:
            if not (creds.expired and creds.refresh_token):
                raise RuntimeError(
                    f"Gmail token at {config.GMAIL_TOKEN_PATH} cannot be refreshed; "
                    "re-authorise it and copy the token back to the server."
                )
            creds.refresh(Request())
            Path(config.GMAIL_TOKEN_PATH).write_text(creds.to_json())
        _service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    return _service


def _headers(payload):
    return {h["name"].lower(): h["value"] for h in (payload.get("headers") or [])}


def _addressed_to_group(hdrs):
    # x-forwarded-to is checked because a clerk who mails a colleague and has
    # it forwarded on to us leaves our address in no other header.
    for key in ("to", "cc", "bcc", "delivered-to", "x-original-to",
                "x-forwarded-to", "list-id", "list-post"):
        value = (hdrs.get(key) or "").lower()
        if any(address in value for address in config.ACCEPT_ADDRESSES):
            return True
    return False


def _ingested_label_id(users):
    """Id of the label we stamp on mail we have already handed to the pipeline.

    Read state was the wrong bookmark, because it is shared with the operator.
    On primary night the inbox was being triaged by hand while the poller ran,
    so a clerk report a human opened first went read before the 20-second poll
    reached it and `is:unread` then skipped it forever. This label is only ever
    set by the code below, so nothing anyone does in Gmail can lose a town.
    """
    global _label_id
    if _label_id is None:
        existing = users.labels().list(userId="me").execute().get("labels") or []
        match = [l["id"] for l in existing if l["name"] == config.GMAIL_INGESTED_LABEL]
        if match:
            _label_id = match[0]
        else:
            _label_id = users.labels().create(userId="me", body={
                "name": config.GMAIL_INGESTED_LABEL,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            }).execute()["id"]
    return _label_id


def _walk(part, out_text, out_html, attachments):
    body = part.get("body") or {}
    filename = part.get("filename") or ""

    if part.get("parts"):
        for p in part["parts"]:
            _walk(p, out_text, out_html, attachments)
        return

    if filename and body.get("attachmentId"):
        attachments.append((filename, body["attachmentId"]))
        return

    data = body.get("data")
    if not data:
        return
    text = base64.urlsafe_b64decode(data.encode()).decode("utf-8", errors="replace")
    mime = part.get("mimeType") or ""
    if mime == "text/plain":
        out_text.append(text)
    elif mime == "text/html":
        out_html.append(text)


def _candidate_ids(users):
    """Every message addressed to us in the window that we have not taken yet.

    Deliberately not scoped to `in:inbox`. Mail to the group address is the
    address we actually publish to clerks, and Gmail files group traffic under
    CATEGORY_FORUMS with no INBOX label at all - the one group message in the
    mailbox proves it - so `in:inbox` made every clerk who used the published
    address invisible to the poller. Spam is searched for the same reason: a
    town clerk mailing from an unfamiliar municipal domain for the first time
    is exactly the profile Gmail files as spam.

    Trash, drafts and our own sent replies are dropped instead, since none of
    them is an incoming return.
    """
    terms = " OR ".join(f"to:{a} OR cc:{a} OR deliveredto:{a} OR list:{a}"
                        for a in config.ACCEPT_ADDRESSES)
    query = (f"({terms}) newer_than:{config.GMAIL_LOOKBACK} "
             f"-label:{config.GMAIL_INGESTED_LABEL} -in:trash -in:drafts -in:chats")

    ids, token = [], None
    while len(ids) < config.GMAIL_MAX_FETCH:
        resp = users.messages().list(
            userId="me", q=query, maxResults=100, pageToken=token,
            includeSpamTrash=True).execute()
        ids += [m["id"] for m in (resp.get("messages") or [])]
        token = resp.get("nextPageToken")
        if not token:
            break
    return ids[:config.GMAIL_MAX_FETCH]


def fetch_new():
    """Yield dicts for group mail we have not taken, labelling each as we go."""
    config.ATTACH_DIR.mkdir(parents=True, exist_ok=True)
    users = service().users()
    label_id = _ingested_label_id(users)

    for ref in [{"id": i} for i in _candidate_ids(users)]:
        msg = users.messages().get(userId="me", id=ref["id"], format="full").execute()
        payload = msg.get("payload") or {}
        hdrs = _headers(payload)
        labels = set(msg.get("labelIds") or [])
        if labels & {"TRASH", "DRAFT", "SENT", "CHAT"}:
            continue  # our own mail, or mail a human threw away
        if not _addressed_to_group(hdrs):
            continue  # left alone; not ours to touch

        texts, htmls, atts = [], [], []
        _walk(payload, texts, htmls, atts)

        body = "\n".join(texts).strip()
        if not body and htmls:
            body = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", "\n".join(htmls))).strip()

        paths = []
        for filename, att_id in atts[:12]:
            try:
                a = users.messages().attachments().get(
                    userId="me", messageId=ref["id"], id=att_id).execute()
                raw = base64.urlsafe_b64decode(a["data"].encode())
            except Exception:
                log.exception("Could not download attachment %s", filename)
                continue
            safe = re.sub(r"[^A-Za-z0-9._-]", "_", filename) or uuid.uuid4().hex
            p = config.ATTACH_DIR / f"{uuid.uuid4().hex[:8]}_{safe}"
            p.write_bytes(raw)
            paths.append(str(p))

        yield {
            "source": "email",
            "external_id": hdrs.get("message-id") or ref["id"],
            "sender": hdrs.get("from", ""),
            "subject": hdrs.get("subject", ""),
            "body": body,
            "attachments": paths,
            "received_at": hdrs.get("date"),
        }

        # The label is the bookmark; clearing UNREAD only keeps the operator's
        # inbox badge honest. Runner.process dedupes on Message-ID anyway, so a
        # message that slips through twice is stored once.
        users.messages().modify(
            userId="me", id=ref["id"],
            body={"addLabelIds": [label_id], "removeLabelIds": ["UNREAD"]}
        ).execute()
