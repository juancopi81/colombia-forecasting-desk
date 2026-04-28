from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .brief import render_brief
from .cleaner import clean
from .cluster import cluster as cluster_items
from .cluster import topic_keywords
from .config_loader import load_metasources
from .dedupe import dedupe
from .fetchers import fetch_all
from .models import (
    CleanedItem,
    Cluster,
    Metasource,
    RawItem,
    RunSummary,
    SourceFailure,
)
from .ranker import parse_iso, rank

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/metasources.yaml")
RUNS_DIR = Path("runs")
MAX_AGE_DAYS = 14


@dataclass(frozen=True)
class PipelineResult:
    run_dir: Path
    raw_items: list[RawItem]
    cleaned_items: list[CleanedItem]
    clusters: list[Cluster]
    failures: list[SourceFailure]
    summary: RunSummary


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_clock(date: str | None = None) -> datetime:
    if date is None:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("--date must use YYYY-MM-DD format") from exc
    return parsed.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)


def _write_json(path: Path, payload) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _drop_too_old(
    items: Iterable[CleanedItem],
    now: datetime | None = None,
) -> list[CleanedItem]:
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=MAX_AGE_DAYS)
    kept: list[CleanedItem] = []
    dropped = 0
    for it in items:
        if not it.published_at:
            kept.append(it)
            continue
        dt = parse_iso(it.published_at)
        if dt is None:
            kept.append(it)
            continue
        if cutoff <= dt <= current:
            kept.append(it)
        else:
            dropped += 1
    if dropped:
        logger.info("Dropped %d items older than %d days", dropped, MAX_AGE_DAYS)
    return kept


def _rankable_items(items: Iterable[CleanedItem]) -> list[CleanedItem]:
    rankable = []
    skipped = 0
    for it in items:
        if it.quality_notes or not it.published_at:
            skipped += 1
            continue
        rankable.append(it)
    if skipped:
        logger.info("Excluded %d low-quality/undated items from clustering", skipped)
    return rankable


def _drop_empty(items: Iterable[CleanedItem]) -> list[CleanedItem]:
    return [it for it in items if it.title or it.clean_text]


def _setup_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def run(
    date: str | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    runs_root: str | Path = RUNS_DIR,
) -> PipelineResult:
    _setup_logging()
    started_at = _now_iso()
    current = _run_clock(date)
    run_date = date or current.strftime("%Y-%m-%d")

    sources: list[Metasource] = load_metasources(config_path)
    logger.info("Loaded %d enabled sources from %s", len(sources), config_path)

    raw_items, failures = fetch_all(sources)
    logger.info(
        "Fetched %d raw items; %d source failures", len(raw_items), len(failures)
    )

    by_source: dict[str, Metasource] = {s.id: s for s in sources}
    cleaned: list[CleanedItem] = []
    for raw in raw_items:
        source = by_source.get(raw.source_id)
        if source is None:
            continue
        cleaned.append(clean(raw, source))

    cleaned = _drop_empty(cleaned)
    cleaned = _drop_too_old(cleaned, now=current)
    cleaned = dedupe(cleaned)
    logger.info("Retained %d cleaned items after filter+dedupe", len(cleaned))

    rankable = _rankable_items(cleaned)
    clusters = cluster_items(rankable)
    ranked = rank(clusters, now=current)
    logger.info("Built %d clusters", len(ranked))

    keywords = topic_keywords(rankable, top_n=5)

    finished_at = _now_iso()
    summary = RunSummary(
        run_date=run_date,
        started_at=started_at,
        finished_at=finished_at,
        sources_checked=len(sources),
        sources_failed=len(failures),
        raw_items=len(raw_items),
        cleaned_items=len(cleaned),
        clusters=len(ranked),
    )

    run_dir = Path(runs_root) / run_date
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "raw_items.json", [asdict(r) for r in raw_items])
    _write_json(run_dir / "cleaned_items.json", [asdict(c) for c in cleaned])
    _write_json(run_dir / "clusters.json", [asdict(c) for c in ranked])
    _write_json(
        run_dir / "source_failures.json", [asdict(f) for f in failures]
    )
    brief_text = render_brief(summary, ranked, failures, cleaned, keywords)
    (run_dir / "metasource_brief.md").write_text(brief_text, encoding="utf-8")
    _write_json(run_dir / "run_summary.json", asdict(summary))

    logger.info("Wrote artifacts to %s", run_dir)
    return PipelineResult(
        run_dir=run_dir,
        raw_items=raw_items,
        cleaned_items=cleaned,
        clusters=ranked,
        failures=failures,
        summary=summary,
    )
