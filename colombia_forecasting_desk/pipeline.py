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
    SourceHealth,
)
from .ranker import parse_iso, rank

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/metasources.yaml")
RUNS_DIR = Path("runs")
SANDBOX_DIR_NAME = "sandbox"
MAX_AGE_DAYS = 14


@dataclass(frozen=True)
class PipelineResult:
    run_dir: Path
    raw_items: list[RawItem]
    cleaned_items: list[CleanedItem]
    clusters: list[Cluster]
    failures: list[SourceFailure]
    source_health: list[SourceHealth]
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


def _derive_status(
    raw: int, rankable: int, failure_count: int
) -> str:
    if failure_count > 0:
        return "failed"
    if raw == 0:
        return "no_raw"
    if rankable == 0:
        return "no_rankable"
    return "ok"


def build_source_health(
    sources: list[Metasource],
    raw_items: list[RawItem],
    cleaned_items: list[CleanedItem],
    rankable_items: list[CleanedItem],
    failures: list[SourceFailure],
) -> list[SourceHealth]:
    raw_counts: dict[str, int] = {}
    cleaned_counts: dict[str, int] = {}
    dated_counts: dict[str, int] = {}
    rankable_counts: dict[str, int] = {}
    failures_by_source: dict[str, list[SourceFailure]] = {}

    for item in raw_items:
        raw_counts[item.source_id] = raw_counts.get(item.source_id, 0) + 1
        if item.published_at:
            dated_counts[item.source_id] = dated_counts.get(item.source_id, 0) + 1
    for item in cleaned_items:
        cleaned_counts[item.source_id] = cleaned_counts.get(item.source_id, 0) + 1
    for item in rankable_items:
        rankable_counts[item.source_id] = rankable_counts.get(item.source_id, 0) + 1
    for failure in failures:
        failures_by_source.setdefault(failure.source_id, []).append(failure)

    health: list[SourceHealth] = []
    for source in sources:
        source_failures = failures_by_source.get(source.id, [])
        raw_count = raw_counts.get(source.id, 0)
        rankable_count = rankable_counts.get(source.id, 0)
        status = _derive_status(raw_count, rankable_count, len(source_failures))
        health.append(
            SourceHealth(
                source_id=source.id,
                source_name=source.name,
                url=source.url,
                raw_count=raw_count,
                cleaned_count=cleaned_counts.get(source.id, 0),
                dated_count=dated_counts.get(source.id, 0),
                rankable_count=rankable_count,
                failure_count=len(source_failures),
                failures=[
                    f"{f.error_class}: {f.error_message}" for f in source_failures
                ],
                onboarding_status=source.onboarding_status,
                status=status,
            )
        )
    return health


def _drop_empty(items: Iterable[CleanedItem]) -> list[CleanedItem]:
    return [it for it in items if it.title or it.clean_text]


def _setup_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _select_source(
    sources: list[Metasource], source_id: str
) -> Metasource:
    for source in sources:
        if source.id == source_id:
            return source
    available = ", ".join(sorted(s.id for s in sources))
    raise ValueError(
        f"source_id={source_id!r} not found among enabled sources. "
        f"Enabled: {available or '(none)'}"
    )


def run_single_source(
    source_id: str,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    runs_root: str | Path = RUNS_DIR,
    date: str | None = None,
) -> PipelineResult:
    """Fetch+clean+rank a single source and write artifacts to runs/sandbox/<source_id>/.

    Used by the M1.2 onboarding loop to iterate on a parser without polluting
    the dated run folders. Each invocation overwrites the previous sandbox
    artifacts for that source.
    """
    _setup_logging()
    started_at = _now_iso()
    current = _run_clock(date)
    run_date = date or current.strftime("%Y-%m-%d")

    sources = load_metasources(config_path)
    source = _select_source(sources, source_id)
    logger.info("Sandbox run for source: %s", source.id)

    raw_items, failures = fetch_all([source])

    cleaned = [clean(raw, source) for raw in raw_items]
    cleaned = _drop_empty(cleaned)
    cleaned = _drop_too_old(cleaned, now=current)
    cleaned = dedupe(cleaned)
    rankable = _rankable_items(cleaned)
    clusters = cluster_items(rankable)
    ranked = rank(clusters, now=current)

    source_health = build_source_health(
        [source], raw_items, cleaned, rankable, failures
    )

    finished_at = _now_iso()
    summary = RunSummary(
        run_date=run_date,
        started_at=started_at,
        finished_at=finished_at,
        sources_checked=1,
        sources_failed=len(failures),
        raw_items=len(raw_items),
        cleaned_items=len(cleaned),
        clusters=len(ranked),
    )

    run_dir = Path(runs_root) / SANDBOX_DIR_NAME / source.id
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "raw_items.json", [asdict(r) for r in raw_items])
    _write_json(run_dir / "cleaned_items.json", [asdict(c) for c in cleaned])
    _write_json(run_dir / "clusters.json", [asdict(c) for c in ranked])
    _write_json(run_dir / "source_failures.json", [asdict(f) for f in failures])
    _write_json(run_dir / "source_health.json", [asdict(h) for h in source_health])
    _write_json(run_dir / "run_summary.json", asdict(summary))

    logger.info("Wrote sandbox artifacts to %s", run_dir)
    return PipelineResult(
        run_dir=run_dir,
        raw_items=raw_items,
        cleaned_items=cleaned,
        clusters=ranked,
        failures=failures,
        source_health=source_health,
        summary=summary,
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
    source_health = build_source_health(
        sources, raw_items, cleaned, rankable, failures
    )

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
    _write_json(run_dir / "source_health.json", [asdict(h) for h in source_health])
    brief_text = render_brief(
        summary, ranked, failures, cleaned, keywords, source_health=source_health
    )
    (run_dir / "metasource_brief.md").write_text(brief_text, encoding="utf-8")
    _write_json(run_dir / "run_summary.json", asdict(summary))

    logger.info("Wrote artifacts to %s", run_dir)
    return PipelineResult(
        run_dir=run_dir,
        raw_items=raw_items,
        cleaned_items=cleaned,
        clusters=ranked,
        failures=failures,
        source_health=source_health,
        summary=summary,
    )
