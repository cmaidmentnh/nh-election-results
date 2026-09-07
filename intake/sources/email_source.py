"""IMAP feed for results@electhouserepublicans.com.

The address is a Google Group; the configured mailbox is a member of it, so
every clerk report lands there. Only messages actually addressed to the group
are read - the service ignores the rest of the mailbox entirely.
"""

import email
import imaplib
import re
import uuid
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime

from intake import config


def _decode(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def _addressed_to_group(msg):
    """True if this message came through the results group."""
    group = config.GROUP_ADDRESS.lower()
    fields = [msg.get_all(h) or [] for h in
              ("To", "Cc", "Delivered-To", "X-Original-To", "List-Id", "List-Post")]
    for values in fields:
        for v in values:
            if group in (v or "").lower():
                return True
    return False


def _body_and_attachments(msg, attach_dir):
    """Plain-text body plus any image/PDF attachments written to disk."""
    body_parts, html_parts, attachments = [], [], []

    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        disp = (part.get("Content-Disposition") or "").lower()
        ctype = part.get_content_type()
        filename = _decode(part.get_filename())

        if "attachment" in disp or filename:
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            safe = re.sub(r"[^A-Za-z0-9._-]", "_", filename or f"{uuid.uuid4().hex}")
            path = attach_dir / f"{uuid.uuid4().hex[:8]}_{safe}"
            path.write_bytes(payload)
            attachments.append(str(path))
            continue

        payload = part.get_payload(decode=True)
        if not payload:
            continue
        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="replace")
        except LookupError:
            text = payload.decode("utf-8", errors="replace")
        if ctype == "text/plain":
            body_parts.append(text)
        elif ctype == "text/html":
            html_parts.append(text)

    body = "\n".join(body_parts).strip()
    if not body and html_parts:
        body = re.sub(r"<[^>]+>", " ", "\n".join(html_parts))
        body = re.sub(r"\s+", " ", body).strip()
    return body, attachments


def fetch_new():
    """Yield dicts for unread group mail, marking each read as it is handed over."""
    config.ATTACH_DIR.mkdir(parents=True, exist_ok=True)

    conn = imaplib.IMAP4_SSL(config.IMAP_HOST)
    try:
        conn.login(config.IMAP_USER, config.IMAP_PASSWORD)
        conn.select(config.IMAP_FOLDER)
        typ, data = conn.search(None, "UNSEEN")
        if typ != "OK":
            return
        for num in (data[0].split() if data and data[0] else []):
            typ, raw = conn.fetch(num, "(RFC822)")
            if typ != "OK" or not raw or not raw[0]:
                continue
            msg = email.message_from_bytes(raw[0][1])
            if not _addressed_to_group(msg):
                continue  # left unread; not ours to touch

            body, attachments = _body_and_attachments(msg, config.ATTACH_DIR)
            try:
                received = parsedate_to_datetime(msg.get("Date")).isoformat()
            except Exception:
                received = None

            yield {
                "source": "email",
                "external_id": msg.get("Message-ID") or f"uid-{num.decode()}",
                "sender": _decode(msg.get("From")),
                "subject": _decode(msg.get("Subject")),
                "body": body,
                "attachments": attachments,
                "received_at": received,
            }
            conn.store(num, "+FLAGS", "\\Seen")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        conn.logout()
