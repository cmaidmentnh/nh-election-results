#!/usr/bin/env python3
"""
Scheduled follow-up to the two clerks who asked to be re-contacted closer to the
2026 State Primary (Sept 8):
  * Lyman   — Bethany Carignan asked to be emailed "the week before the election".
  * Chatham — Patricia Pitman asked to "request this information closer to election day".

Sent via AWS SES from the verified electhouserepublicans.com domain (same as the
original outreach). Intended to run from cron on 2026-09-01. DRY RUN by default;
--send actually sends. A sentinel file prevents an accidental double-send.
"""

import argparse
import os
import sys
import time

SENDER = '"Chris Maidment" <chris@electhouserepublicans.com>'
REPLY_TO = "chris@electhouserepublicans.com"
SUBJECT = "Following up: unofficial election results for the September 8 State Primary"
SENTINEL = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".clerk_followup_sent")

# (email, greeting name, town, the specific reason they asked us to follow up)
RECIPIENTS = [
    ("tctx@lymannh.gov", "Bethany Carignan", "Lyman",
     "When I first wrote in June you asked me to follow up the week before the election"),
    ("townclerk@chathamnh.org", "Patricia Pitman", "Chatham",
     "When I first wrote in June you asked me to reach out closer to election day"),
]


def text_body(name, town, reason):
    return f"""\
Dear {name},

{reason}, so here I am — the 2026 State Primary is Tuesday, September 8.

Whenever {town}'s unofficial results are available that night (or the next
morning, whatever is normal for your office), I would be grateful if you would
send them to chris@electhouserepublicans.com -- the same distribution you already
do, in whatever format you normally use. I'm interested in every race on the
ballot, all the way down to State Representative, county, and local contests, not
just the top of the ticket.

I'll plan to send a short reminder again before the November 3 General Election.
Thank you very much for your help, and for all the work you do running {town}'s
elections.

With appreciation,

Chris Maidment
chris@electhouserepublicans.com
"""


def html_body(name, town, reason):
    return f"""\
<html><body style="font-family:Georgia,serif;font-size:15px;color:#1a1a1a;line-height:1.5;">
<p>Dear {name},</p>
<p>{reason}, so here I am &mdash; the 2026 <b>State Primary is Tuesday, September&nbsp;8</b>.</p>
<p>Whenever {town}'s unofficial results are available that night (or the next morning,
whatever is normal for your office), I would be grateful if you would send them to
<b>chris@electhouserepublicans.com</b> &mdash; the same distribution you already do, in
whatever format you normally use. I'm interested in <b>every race on the ballot</b>, all
the way down to State Representative, county, and local contests.</p>
<p>I'll plan to send a short reminder again before the November&nbsp;3 General Election.
Thank you very much for your help, and for all the work you do running {town}'s elections.</p>
<p>With appreciation,<br><br>Chris Maidment<br>
<a href="mailto:chris@electhouserepublicans.com">chris@electhouserepublicans.com</a></p>
</body></html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="actually send (default is dry run)")
    ap.add_argument("--force", action="store_true", help="ignore the already-sent sentinel")
    args = ap.parse_args()

    if args.send and os.path.exists(SENTINEL) and not args.force:
        print(f"Already sent (sentinel {SENTINEL} exists). Use --force to override.")
        return

    print(f"{len(RECIPIENTS)} recipient(s): " + ", ".join(f"{t} <{e}>" for e, _, t, _ in RECIPIENTS))
    if not args.send:
        print("DRY RUN — nothing sent. Re-run with --send to send.")
        return

    import boto3
    from botocore.exceptions import ClientError
    ses = boto3.client("ses", region_name=os.environ.get("AWS_REGION", "us-east-1"),
                       aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                       aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    sent = failed = 0
    for email, name, town, reason in RECIPIENTS:
        try:
            ses.send_email(Source=SENDER, Destination={"ToAddresses": [email]},
                           ReplyToAddresses=[REPLY_TO],
                           Message={"Subject": {"Data": SUBJECT, "Charset": "UTF-8"},
                                    "Body": {"Text": {"Data": text_body(name, town, reason), "Charset": "UTF-8"},
                                             "Html": {"Data": html_body(name, town, reason), "Charset": "UTF-8"}}})
            sent += 1
            print(f"  sent -> {town} <{email}>")
            time.sleep(0.2)
        except ClientError as ex:
            failed += 1
            print(f"  FAIL -> {email}: {ex.response['Error']['Message']}", file=sys.stderr)
    if sent and not failed:
        open(SENTINEL, "w").write(time.strftime("%Y-%m-%d %H:%M:%S"))
    print(f"\nDone. sent={sent} failed={failed}")


if __name__ == "__main__":
    main()
