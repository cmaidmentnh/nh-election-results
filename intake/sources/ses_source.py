"""Dedicated results inbox: results@nhhouse.gop, received by Amazon SES.

SES stores every message to that address as a raw .eml object in S3 (receipt
rule "results-intake-rule", bucket INTAKE_S3_BUCKET, prefix INTAKE_S3_PREFIX).
This polls the prefix, hands each message to the pipeline, and moves the
object under done/ so it is never listed again.

Unlike the Gmail feed, nothing here is anyone's personal mailbox: everything
sent to this address is meant as a results report, so there is no Jev gate
and no triage - and Chris's own inboxes are never touched by it.

Message-ID is the dedupe key (runner.process), so a clerk who copies both this
address and chris@electhouserepublicans.com is still counted once.
"""

import email
import logging
from email.utils import parsedate_to_datetime

from intake import config
from intake.sources.email_source import _body_and_attachments, _decode

log = logging.getLogger("intake.ses")

_client = None


def _s3():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client("s3", region_name=config.S3_REGION)
    return _client


def available():
    return bool(config.S3_BUCKET)


def _move(key, dest_prefix):
    s3 = _s3()
    name = key[len(config.S3_PREFIX):]
    s3.copy_object(Bucket=config.S3_BUCKET, Key=config.S3_PREFIX + dest_prefix + name,
                   CopySource={"Bucket": config.S3_BUCKET, "Key": key})
    s3.delete_object(Bucket=config.S3_BUCKET, Key=key)


def fetch_new():
    """Yield one dict per new message in the results inbox, oldest first."""
    if not available():
        return
    config.ATTACH_DIR.mkdir(parents=True, exist_ok=True)
    s3 = _s3()
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=config.S3_BUCKET, Prefix=config.S3_PREFIX):
        for obj in page.get("Contents", []):
            rest = obj["Key"][len(config.S3_PREFIX):]
            if not rest or "/" in rest:          # done/, spam/ and the like
                continue
            keys.append((obj["LastModified"], obj["Key"]))
    for _, key in sorted(keys):
        try:
            raw = s3.get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()
            msg = email.message_from_bytes(raw)
            if rest_is_setup(key, msg):
                _move(key, "done/")
                continue
            verdicts = " ".join((msg.get(h) or "") for h in ("X-SES-Spam-Verdict", "X-SES-Virus-Verdict")).upper()
            if "FAIL" in verdicts:
                log.warning("results inbox: %s flagged spam/virus (%s), set aside", key, verdicts)
                _move(key, "spam/")
                continue
            body, attachments = _body_and_attachments(msg, config.ATTACH_DIR)
            try:
                received = parsedate_to_datetime(msg.get("Date")).isoformat()
            except Exception:
                received = None
            item = {
                "source": "email",
                "external_id": msg.get("Message-ID") or f"s3-{key}",
                "sender": _decode(msg.get("From")),
                "subject": _decode(msg.get("Subject")),
                "body": body,
                "attachments": attachments,
                "received_at": received,
            }
            # Moved before it is handed over, like Gmail's label: a crash
            # mid-parse leaves the stored intake_messages row for the stranded
            # sweep, never a second copy of the message.
            _move(key, "done/")
            log.info("results inbox: %s from %s (%d attachment(s))", item["subject"][:60],
                     item["sender"][:60], len(attachments))
            yield item
        except Exception:
            log.exception("results inbox: could not read %s", key)


def rest_is_setup(key, msg):
    """SES drops a one-off AMAZON_SES_SETUP_NOTIFICATION object when the rule is created."""
    return key.endswith("AMAZON_SES_SETUP_NOTIFICATION")
