# Colombia Forecasting Desk

Experimental agent-assisted forecasting project focused on Colombian political, economic, regulatory, and institutional events. See [`PROJECT_SPEC.md`](PROJECT_SPEC.md) for the full vision and milestones.

## Quickstart

Requires Python 3.12 and [`uv`](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest -q
uv run python scripts/scan_metasources.py
```

The pipeline produces a dated run folder under `runs/YYYY-MM-DD/` containing:

- `raw_items.json` — every item fetched from each enabled metasource
- `cleaned_items.json` — items after HTML stripping, normalization, filtering, and dedupe
- `clusters.json` — clusters of related items, ranked by simple heuristics
- `metasource_brief.md` — the human-readable daily brief
- `source_failures.json` — per-source errors (run never crashes on a single source)
- `run_summary.json` — counts and timestamps for the run

### Optional flags

```bash
uv run python scripts/scan_metasources.py --date 2026-04-27 --config config/metasources.yaml
```

## Project layout

```
colombia_forecasting_desk/   # core package (config, fetchers, cleaner, dedupe, cluster, ranker, brief, pipeline)
config/metasources.yaml      # registry of public sources (enabled/disabled, fetch_method, priority, trust_role)
scripts/scan_metasources.py  # M1 entry point
prompts/                     # placeholder prompts (used in later milestones)
runs/YYYY-MM-DD/             # generated run artifacts (gitignored content)
forecasts/                   # forecast log (used in later milestones)
tests/                       # pytest suite
```

## Status

Currently at **M1 — Metasource Pipeline**. See [`docs/M1_METASOURCE_PIPELINE.md`](docs/M1_METASOURCE_PIPELINE.md) for the detailed plan and [`PROJECT_SPEC.md`](PROJECT_SPEC.md) for upcoming milestones (M2 question discovery, M3 evidence packs, M4 public X experiment).
