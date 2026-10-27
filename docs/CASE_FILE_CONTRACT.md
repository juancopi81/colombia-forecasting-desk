# Running stories

The eight YAML files under `cases/` carry the desk's evolving interpretation of
important stories. The shared `case_file.v1` validator checks the required fields,
reader text, dates, word caps and source links. Structured identifiers are kept
separate from reader text. A bill identity includes chamber and year; bare number
matches are insufficient.

```bash
./.venv/bin/python scripts/validate_cases.py
./.venv/bin/python scripts/update_cases.py --date 2026-10-27
```

The second command writes `runs/<date>/case_updates.json`. Its `cases` list has a
case_id, plain_title, current_view, status, candidate evidence, days_since_update
and suggested_status for every case. Each candidate carries its input artifact,
locator, matched_on fields, official source URL and excerpt. Keyword matches are
weaker review prompts, not assertions of identity or a changed outcome. Matching
does not edit story text, declare a vote result or create a forecast.

The model reads this evidence, checks its context and edits the live YAML only
when the evidence supports a changed view. Existing updates remain in the list;
new updates carry a date and official source link. Do not carry dates forward
merely because a source repeats. A parked suggestion is advisory: it needs the
configured period without a match and complete matcher inputs. Missing inputs
are recorded as coverage_gaps, never evidence that a story ended.

After editing and validation, freeze the day's state:

```bash
./.venv/bin/python scripts/snapshot_cases.py --date 2026-10-27
```

The snapshot lives in `runs/<date>/cases_snapshot/`, including a deterministic
SHA-256 manifest. Repeating the command with identical files is safe. A different
snapshot is refused, and future-dated views or updates cannot enter a past run.
Weekly reporting reads only the cutoff run's snapshot; it never uses the live
`cases/` folder to reconstruct history. The historical October 19–23 weekly demo
must acknowledge that these snapshots did not yet exist.

Both commands accept `--runs-dir` and `--cases-dir` for isolated runs and tests.
Validation accepts `--cases-dir`. None of these commands fetches the network or
changes a forecast ledger.

The matcher also accepts successful official HTML follow-ups preserved in the
same run with a `sources/*_receipt.json`, verified TLS, and a matching SHA-256.
This keeps a continuing case visible when the bounded daily ticker has dropped
its older bill. A missing, outside-run or altered capture cannot supply evidence.

The seed views are dated October 27. Sources include the current run's official
indicators and legislative material, the October 23 full-edition budget follow-up,
the October 26 food-tax forecast evidence and the October 9 official pension-rule
excerpt. The energy case is an explicit research watch, not a confirmed shortage.
