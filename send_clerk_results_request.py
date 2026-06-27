#!/usr/bin/env python3
"""
Email every town/city clerk (from the imported polling-place list) asking them
to send unofficial election results to chris@electhouserepublicans.com as part
of their normal distribution, for the 2026 primary and general.

Sends via AWS SES from the verified electhouserepublicans.com domain. Reads the
same AWS creds the recruitment app uses (env: AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY / AWS_REGION).

DRY RUN by default — prints the recipient list and sends nothing. Add --send to
actually send. --limit N caps recipients (handy for a test to yourself).

    python3 send_clerk_results_request.py                 # dry run
    python3 send_clerk_results_request.py --send          # send to all clerks
    python3 send_clerk_results_request.py --send --only you@example.com  # test one
"""

import argparse
import os
import sqlite3
import sys
import time

SENDER = '"Chris Maidment" <chris@electhouserepublicans.com>'
REPLY_TO = "chris@electhouserepublicans.com"
SUBJECT = "Request to Include Us in Your Unofficial Election Results Distribution"


def text_body(greeting_name):
    return f"""\
Dear {greeting_name},

My name is Chris Maidment. I help compile and publish timely New Hampshire
election results for the public, and I also work for the Committee to Elect
House Republicans -- so I have a strong interest in receiving election results
as promptly as possible.

As you distribute unofficial election-night results for the 2026 State Primary
(September 8) and the 2026 General Election (November 3), I would be grateful if
you would add chris@electhouserepublicans.com to your normal distribution list
and send those unofficial results to that address as soon as they are
practicably available on election night.

I am interested in the results for every race on your ballot -- all the way down
the ticket to State Representative, county offices, and any local contests --
not just the top-of-the-ticket races.

To be clear, I am only asking to be included in the same distribution you
already do -- whatever format you normally use is perfectly fine. There is no
special form to complete and nothing extra is required of your office.

Having these unofficial numbers promptly allows us to provide fast, accurate
public reporting while the official results work their way through
certification. Thank you for the essential work you and your team do to run
New Hampshire's elections.

With appreciation,

Chris Maidment
chris@electhouserepublicans.com
"""


def html_body(greeting_name):
    return f"""\
<html><body style="font-family:Georgia,serif;font-size:15px;color:#1a1a1a;line-height:1.5;">
<p>Dear {greeting_name},</p>
<p>My name is Chris Maidment. I help compile and publish timely New Hampshire
election results for the public, and I also work for the <b>Committee to Elect
House Republicans</b> &mdash; so I have a strong interest in receiving election
results as promptly as possible.</p>
<p>As you distribute <b>unofficial election-night results</b> for the 2026 State
Primary (September&nbsp;8) and the 2026 General Election (November&nbsp;3), I would
be grateful if you would add <b>chris@electhouserepublicans.com</b> to your normal
distribution list and send those unofficial results to that address <b>as soon as
they are practicably available</b> on election night.</p>
<p>I am interested in the results for <b>every race on your ballot</b> &mdash; all the
way down the ticket to State Representative, county offices, and any local contests
&mdash; not just the top-of-the-ticket races.</p>
<p>To be clear, I am only asking to be included in the same distribution you already
do &mdash; whatever format you normally use is perfectly fine. There is no special
form to complete and nothing extra is required of your office.</p>
<p>Having these unofficial numbers promptly allows us to provide fast, accurate public
reporting while the official results work their way through certification. Thank you
for the essential work you and your team do to run New Hampshire's elections.</p>
<p>With appreciation,<br><br>
Chris Maidment<br>
<a href="mailto:chris@electhouserepublicans.com">chris@electhouserepublicans.com</a></p>
</body></html>
"""


def _greeting_name(clerk):
    """Title-case the clerk's name for the greeting, or a generic fallback."""
    if not clerk or not clerk.strip():
        return "Town/City Clerk"
    name = " ".join(w.capitalize() for w in clerk.split())
    return name


def recipients(db_path):
    """Return [(email, greeting_name)], one per distinct email."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT lower(trim(email)) AS email, MIN(clerk) AS clerk
        FROM polling_places
        WHERE email LIKE '%_@_%.__%'
        GROUP BY lower(trim(email))
        ORDER BY 1
    """)
    rows = [(r[0], _greeting_name(r[1])) for r in cur.fetchall() if r[0] and "@" in r[0]]
    conn.close()
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="nh_elections.db")
    ap.add_argument("--send", action="store_true", help="actually send (default is dry run)")
    ap.add_argument("--only", help="send only to this address (test)")
    ap.add_argument("--name", help="greeting name to use with --only (test personalization)")
    ap.add_argument("--limit", type=int, help="cap number of recipients")
    args = ap.parse_args()

    if args.only:
        to_list = [(args.only, args.name or "Town/City Clerk")]
    else:
        to_list = recipients(args.db)
    if args.limit:
        to_list = to_list[:args.limit]

    print(f"{len(to_list)} recipient(s).")
    if not args.send:
        for e, name in to_list:
            print(f"   {e}  ->  Dear {name},")
        print("\nDRY RUN — nothing sent. Re-run with --send to send.")
        return

    import boto3
    from botocore.exceptions import ClientError
    ses = boto3.client(
        "ses",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    sent = failed = 0
    for e, name in to_list:
        try:
            ses.send_email(
                Source=SENDER,
                Destination={"ToAddresses": [e]},
                ReplyToAddresses=[REPLY_TO],
                Message={
                    "Subject": {"Data": SUBJECT, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": text_body(name), "Charset": "UTF-8"},
                        "Html": {"Data": html_body(name), "Charset": "UTF-8"},
                    },
                },
            )
            sent += 1
            print(f"  sent -> {e}")
            time.sleep(0.1)  # stay well under the SES rate limit
        except ClientError as ex:
            failed += 1
            print(f"  FAIL -> {e}: {ex.response['Error']['Message']}", file=sys.stderr)

    print(f"\nDone. sent={sent} failed={failed}")


if __name__ == "__main__":
    main()
