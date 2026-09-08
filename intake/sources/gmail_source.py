"""Gmail API feed for results@electhouserepublicans.com.

Preferred over IMAP because the mailbox is already authorised - the stored
OAuth token is self-contained (it carries its own client id, secret and refresh
token), so no app password has to be minted or rotated.

Scope discipline: that token also carries gmail.send and calendar, which this
service has no business using. Everything here is read-plus-mark-read only, and
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
    for key in ("to", "cc", "delivered-to", "x-original-to", "list-id", "list-post"):
        value = (hdrs.get(key) or "").lower()
        if any(address in value for address in config.ACCEPT_ADDRESSES):
            return True
    return False


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


def fetch_new():
    """Yield dicts for unread group mail, marking each read as it is handed over."""
    config.ATTACH_DIR.mkdir(parents=True, exist_ok=True)
    users = service().users()

    terms = " OR ".join(f"to:{a} OR deliveredto:{a} OR list:{a}"
                        for a in config.ACCEPT_ADDRESSES)
    query = f"is:unread in:inbox ({terms})"
    resp = users.messages().list(userId="me", q=query, maxResults=50).execute()

    for ref in resp.get("messages", []) or []:
        msg = users.messages().get(userId="me", id=ref["id"], format="full").execute()
        payload = msg.get("payload") or {}
        hdrs = _headers(payload)
        if not _addressed_to_group(hdrs):
            continue  # left unread; not ours to touch

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

        users.messages().modify(
            userId="me", id=ref["id"], body={"removeLabelIds": ["UNREAD"]}
        ).execute()
