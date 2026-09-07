# Results intake

Turns election-night reports emailed to **results@electhouserepublicans.com** or
posted in the results Signal group into rows in `results`, automatically.

## How it works

Both feeds run in one service and share one parser:

1. **Store first.** Every inbound message is written to `intake_messages`
   verbatim, before anything is parsed. A bad parse can always be replayed
   against the original with `--replay <id>`.
2. **Resolve the town.** Exact name, then a scan for a place name inside the
   text, then the sender's address against the clerk list, then the model.
3. **Hand over that town's exact ballot.** Every race id and candidate id the
   town votes on, on both the Republican and Democratic ballots, goes into the
   prompt. The model only attaches numbers to those ids - it is never asked to
   invent a race or a candidate, so a name that is not on the town's ballot
   cannot be matched to one by accident.
4. **Validate, then write or queue.**

## What publishes automatically

A line goes straight to the live page only if **all** of these hold:

- the town resolved to a real polling place
- the race is on that town's ballot
- the candidate is on that race's roster
- the count is plausible, and not more than the town's ballots cast
- the model's confidence is at least `INTAKE_MIN_CONFIDENCE` (default 0.85)
- it does not contradict a value already on file

Anything else goes to **/entry/intake** with the reason and the original report
beside it. Nothing is ever dropped. Every write goes through `result_audit`
under the `intake-bot` user, so the existing audit trail and undo path apply.

A second report that repeats a number already on file changes nothing. A second
report that *contradicts* one is queued rather than applied - the first number
stays up until a human picks.

## The two ward conventions

New Hampshire reports results two different ways, and the resolver handles both:

- **Towns with several polling locations** (Salem, Merrimack, Derry, Hudson,
  Goffstown, Walpole, Farmington, Berlin) report one combined town total. A ward
  suffix collapses: `Salem Ward 2` -> `Salem`.
- **Cities with wards** (Manchester, Nashua, Concord, Dover, Rochester, Keene,
  Laconia, Portsmouth, Somersworth, Claremont, Franklin, Lebanon) report per
  ward. The ward is kept, and a bare `Manchester` is treated as ambiguous and
  sent to review rather than guessed.

## Running it

```bash
# service
systemctl status nh-results-intake
journalctl -u nh-results-intake -f

# drain the mailbox once, without starting the service
/opt/nh-results-intake-venv/bin/python -m intake.runner --once

# re-parse a stored message after fixing something
/opt/nh-results-intake-venv/bin/python -m intake.runner --replay 42
```

Config lives in `/opt/nh-election-results/intake.env` (see
`deploy/intake.env.example`). The service has its own virtualenv at
`/opt/nh-results-intake-venv` so upgrading the Anthropic SDK here cannot
disturb the Flask app or the Signal bots on the same box.

## Turning it off

`systemctl stop nh-results-intake`. The hand-entry screens are unaffected -
intake writes through the same tables they do.

To keep it running but publish nothing automatically, set
`INTAKE_AUTO_APPLY=0` and restart: every line then goes to the review queue.
