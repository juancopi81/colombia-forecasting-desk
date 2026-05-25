from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

from .acceptance import build_acceptance_report
from .analyst_leads import build_analyst_leads, render_analyst_leads
from .brief import render_brief, render_m2_handoff
from .candidates import build_m1_candidates
from .cleaner import clean
from .cluster import cluster as cluster_items
from .cluster import topic_keywords
from .config_loader import load_metasources
from .dedupe import dedupe
from .decision_records import link_legislative_followups, link_official_legal_records
from .fetchers import fetch_all
from .indicator_watch import (
    build_indicator_watch,
    fetch_structured_indicator_observations,
)
from .indicator_tension_cards import (
    build_indicator_tension_cards,
    render_indicator_tension_cards,
)
from .legislative_reconciler import build_legislative_reconciliations
from .m2_review_packet import build_m2_review_packet, render_m2_review_packet
from .m2_ranker import build_legislative_m2_ranking
from .manifest import build_run_manifest
from .models import (
    CleanedItem,
    Cluster,
    IndicatorObservation,
    Metasource,
    RawItem,
    RunSummary,
    SourceFailure,
    SourceHealth,
)
from .observability import RunTrace
from .procurement_leads import build_procurement_concentration_leads
from .ranker import parse_iso, rank
from .registry_changes import add_mincit_zonas_francas_change_events

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/metasources.yaml")
RUNS_DIR = Path("runs")
SANDBOX_DIR_NAME = "sandbox"
MAX_AGE_DAYS = 14
MAX_FUTURE_CALENDAR_DAYS = 400
PDF_EXTENSIONS = (".pdf",)
SPREADSHEET_EXTENSIONS = (".xlsx", ".xls", ".csv", ".ods")
DOCUMENT_EXTENSIONS = (".doc", ".docx", ".ppt", ".pptx", ".zip")


@dataclass(frozen=True)
class PipelineResult:
    run_dir: Path
    raw_items: list[RawItem]
    cleaned_items: list[CleanedItem]
    clusters: list[Cluster]
    failures: list[SourceFailure]
    source_health: list[SourceHealth]
    indicator_watch: list[IndicatorObservation]
    indicator_tension_cards: list[dict]
    legislative_reconciliations: list[dict]
    m2_ranked_questions: dict
    m2_review_packet: dict
    analyst_leads: dict
    m1_candidates: dict
    acceptance_report: dict
    run_manifest: dict
    run_trace: dict
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
    future_calendar_cutoff = current + timedelta(days=MAX_FUTURE_CALENDAR_DAYS)
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
        if (
            it.source_type == "calendar"
            and current < dt <= future_calendar_cutoff
        ):
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


def _raw_item_content_kind(item: RawItem) -> str:
    metadata = item.metadata or {}
    if metadata.get("content_extraction") or metadata.get("parsed_content"):
        return "parsed_content"
    if metadata.get("extraction") == "imprenta_nacional_jsf_table":
        return "document_link"

    path = urlsplit(item.url).path.lower()
    if any(ext in path for ext in SPREADSHEET_EXTENSIONS):
        return "spreadsheet_link"
    if any(ext in path for ext in PDF_EXTENSIONS):
        return "pdf_link"
    if any(ext in path for ext in DOCUMENT_EXTENSIONS):
        return "document_link"
    return "html_or_api"


def _derive_content_mode(
    source_items: list[RawItem], failure_count: int
) -> tuple[str, int, int]:
    if not source_items:
        return ("failed" if failure_count else "no_items", 0, 0)

    kinds = [_raw_item_content_kind(item) for item in source_items]
    kind_set = set(kinds)
    document_kinds = {"pdf_link", "spreadsheet_link", "document_link"}
    document_link_count = sum(1 for kind in kinds if kind in document_kinds)
    parsed_content_count = sum(1 for kind in kinds if kind == "parsed_content")

    if kind_set == {"parsed_content"}:
        mode = "parsed_content"
    elif "parsed_content" in kind_set:
        mode = "mixed_with_parsed_content"
    elif kind_set <= document_kinds:
        if kind_set == {"pdf_link"}:
            mode = "pdf_links_only"
        elif kind_set == {"spreadsheet_link"}:
            mode = "spreadsheet_links_only"
        else:
            mode = "document_links_only"
    elif document_link_count:
        mode = "mixed_document_and_html_links"
    else:
        mode = "html_or_api"

    return mode, document_link_count, parsed_content_count


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
    raw_by_source: dict[str, list[RawItem]] = {}
    tagged_counts: dict[str, int] = {}
    untagged_rankable_counts: dict[str, int] = {}

    for item in raw_items:
        raw_counts[item.source_id] = raw_counts.get(item.source_id, 0) + 1
        raw_by_source.setdefault(item.source_id, []).append(item)
        if item.published_at:
            dated_counts[item.source_id] = dated_counts.get(item.source_id, 0) + 1
    for item in cleaned_items:
        cleaned_counts[item.source_id] = cleaned_counts.get(item.source_id, 0) + 1
    for item in rankable_items:
        rankable_counts[item.source_id] = rankable_counts.get(item.source_id, 0) + 1
        if item.detected_entities or item.detected_topics:
            tagged_counts[item.source_id] = tagged_counts.get(item.source_id, 0) + 1
        else:
            untagged_rankable_counts[item.source_id] = (
                untagged_rankable_counts.get(item.source_id, 0) + 1
            )
    for failure in failures:
        failures_by_source.setdefault(failure.source_id, []).append(failure)

    health: list[SourceHealth] = []
    for source in sources:
        source_failures = failures_by_source.get(source.id, [])
        raw_count = raw_counts.get(source.id, 0)
        rankable_count = rankable_counts.get(source.id, 0)
        status = _derive_status(raw_count, rankable_count, len(source_failures))
        content_mode, document_link_count, parsed_content_count = _derive_content_mode(
            raw_by_source.get(source.id, []), len(source_failures)
        )
        tagged_count = tagged_counts.get(source.id, 0)
        untagged_rankable_count = untagged_rankable_counts.get(source.id, 0)
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
                content_mode=content_mode,
                document_link_count=document_link_count,
                parsed_content_count=parsed_content_count,
                tagged_count=tagged_count,
                untagged_rankable_count=untagged_rankable_count,
                acceptance_status=_source_acceptance_status(
                    status,
                    document_link_count,
                    parsed_content_count,
                    tagged_count,
                    untagged_rankable_count,
                ),
            )
        )
    return health


def _source_acceptance_status(
    status: str,
    document_link_count: int,
    parsed_content_count: int,
    tagged_count: int,
    untagged_rankable_count: int,
) -> str:
    if status == "failed":
        return "failed"
    if document_link_count > 0 and parsed_content_count == 0:
        return "document_unparsed"
    if status in {"no_raw", "no_rankable"}:
        return status
    if untagged_rankable_count > 0 and tagged_count == 0:
        return "untagged"
    return "ok"


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
    strict_requested: bool = False,
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
    trace = RunTrace(
        run_date=run_date,
        mode="sandbox",
        metadata={
            "config_path": str(config_path),
            "source_id": source_id,
            "strict_requested": strict_requested,
        },
    )

    with trace.span("load_metasources") as span:
        sources = load_metasources(config_path)
        source = _select_source(sources, source_id)
        span.set_counts(enabled_sources=len(sources))
    logger.info("Sandbox run for source: %s", source.id)

    with trace.span("fetch_sources", metadata={"source_count": 1}) as span:
        raw_items, failures = fetch_all([source], trace=trace)
        span.set_counts(raw_items=len(raw_items), source_failures=len(failures))
    with trace.span("build_legislative_reconciliations") as span:
        legislative_reconciliations = build_legislative_reconciliations(raw_items)
        span.set_counts(records=len(legislative_reconciliations))

    with trace.span("clean_and_rank_items") as span:
        cleaned = [clean(raw, source) for raw in raw_items]
        cleaned = _drop_empty(cleaned)
        cleaned = _drop_too_old(cleaned, now=current)
        cleaned = dedupe(cleaned)
        rankable = _rankable_items(cleaned)
        clusters = cluster_items(rankable)
        ranked = rank(clusters, now=current)
        span.set_counts(
            cleaned_items=len(cleaned),
            rankable_items=len(rankable),
            clusters=len(ranked),
        )

    with trace.span("build_source_health") as span:
        source_health = build_source_health(
            [source], raw_items, cleaned, rankable, failures
        )
        span.set_counts(source_health_records=len(source_health))
    with trace.span("build_indicator_watch") as span:
        indicator_watch = build_indicator_watch(raw_items, cleaned, now=current)
        span.set_counts(
            indicators=len(indicator_watch),
            observed_indicators=sum(
                1 for indicator in indicator_watch if indicator.status == "observed"
            ),
        )
    with trace.span("build_indicator_tension_cards") as span:
        indicator_tension_cards = build_indicator_tension_cards(indicator_watch)
        span.set_counts(cards=len(indicator_tension_cards))

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
    keywords = topic_keywords(rankable, top_n=5)
    with trace.span("build_m1_candidates") as span:
        m1_candidates = build_m1_candidates(
            summary,
            ranked,
            failures,
            keywords,
            source_health=source_health,
            indicator_watch=indicator_watch,
            legislative_reconciliations=legislative_reconciliations,
        )
        span.set_counts(
            candidates=len(m1_candidates.get("candidates") or []),
            rejected=len(m1_candidates.get("rejected") or []),
            source_caveats=len(m1_candidates.get("source_caveats") or []),
        )
    with trace.span("build_m2_artifacts") as span:
        m2_ranked_questions = build_legislative_m2_ranking(
            legislative_reconciliations,
            summary,
            generated_at=finished_at,
        )
        m2_review_packet = build_m2_review_packet(
            summary,
            raw_items,
            cleaned,
            m1_candidates,
            m2_ranked_questions,
            legislative_reconciliations,
            source_health,
            indicator_watch,
            indicator_tension_cards,
            generated_at=finished_at,
        )
        span.set_counts(
            ranked_questions=len(m2_ranked_questions.get("ranked_questions") or []),
            review_items=len(m2_review_packet.get("review_items") or []),
        )
    with trace.span("build_analyst_leads") as span:
        procurement_concentration_leads = build_procurement_concentration_leads(
            raw_items,
            cleaned,
        )
        analyst_leads = build_analyst_leads(
            summary,
            m2_review_packet,
            indicator_tension_cards,
            procurement_concentration_leads,
            generated_at=finished_at,
        )
        analyst_summary = analyst_leads.get("summary") or {}
        span.set_counts(
            leads=analyst_summary.get("lead_count"),
            forecast_questions=analyst_summary.get("forecast_question_count"),
            analyst_insights=analyst_summary.get("analyst_insight_count"),
            investigation_leads=analyst_summary.get("investigation_lead_count"),
            procurement_concentration_leads=len(procurement_concentration_leads),
        )
    with trace.span("build_acceptance_report") as span:
        acceptance_report = build_acceptance_report(
            summary,
            m1_candidates,
            source_health,
            failures,
            cleaned,
            indicator_watch,
        )
        span.set_metadata(
            acceptance_status=acceptance_report.get("status"),
            strict_pass=acceptance_report.get("strict_pass"),
        )
        span.set_counts(
            errors=acceptance_report.get("error_count"),
            warnings=acceptance_report.get("warning_count"),
        )

    run_dir = Path(runs_root) / SANDBOX_DIR_NAME / source.id
    with trace.span("write_artifacts", metadata={"run_dir": str(run_dir)}) as span:
        run_dir.mkdir(parents=True, exist_ok=True)
        _write_json(run_dir / "raw_items.json", [asdict(r) for r in raw_items])
        _write_json(run_dir / "cleaned_items.json", [asdict(c) for c in cleaned])
        _write_json(run_dir / "clusters.json", [asdict(c) for c in ranked])
        _write_json(
            run_dir / "indicator_watch.json", [asdict(i) for i in indicator_watch]
        )
        _write_json(run_dir / "indicator_tension_cards.json", indicator_tension_cards)
        (run_dir / "indicator_tension_cards.md").write_text(
            render_indicator_tension_cards(
                indicator_tension_cards,
                run_date=run_date,
            ),
            encoding="utf-8",
        )
        _write_json(run_dir / "source_failures.json", [asdict(f) for f in failures])
        _write_json(run_dir / "source_health.json", [asdict(h) for h in source_health])
        _write_json(
            run_dir / "legislative_reconciler.json", legislative_reconciliations
        )
        _write_json(run_dir / "m2_ranked_questions.json", m2_ranked_questions)
        _write_json(run_dir / "m2_review_packet.json", m2_review_packet)
        (run_dir / "m2_review_packet.md").write_text(
            render_m2_review_packet(m2_review_packet),
            encoding="utf-8",
        )
        _write_json(run_dir / "analyst_leads.json", analyst_leads)
        (run_dir / "analyst_leads.md").write_text(
            render_analyst_leads(analyst_leads),
            encoding="utf-8",
        )
        _write_json(run_dir / "m1_candidates.json", m1_candidates)
        _write_json(run_dir / "acceptance_report.json", acceptance_report)
        _write_json(run_dir / "run_summary.json", asdict(summary))
        span.set_counts(artifacts_written=17)
    run_trace = trace.to_dict()
    _write_json(run_dir / "run_trace.json", run_trace)
    run_manifest = build_run_manifest(
        run_dir,
        summary,
        config_path=config_path,
        strict_requested=strict_requested,
        acceptance_report=acceptance_report,
        m1_candidates=m1_candidates,
        legislative_reconciliations=legislative_reconciliations,
        m2_ranked_questions=m2_ranked_questions,
        m2_review_packet=m2_review_packet,
        indicator_tension_cards=indicator_tension_cards,
        analyst_leads=analyst_leads,
    )
    _write_json(run_dir / "run_manifest.json", run_manifest)

    logger.info("Wrote sandbox artifacts to %s", run_dir)
    return PipelineResult(
        run_dir=run_dir,
        raw_items=raw_items,
        cleaned_items=cleaned,
        clusters=ranked,
        failures=failures,
        source_health=source_health,
        indicator_watch=indicator_watch,
        indicator_tension_cards=indicator_tension_cards,
        legislative_reconciliations=legislative_reconciliations,
        m2_ranked_questions=m2_ranked_questions,
        m2_review_packet=m2_review_packet,
        analyst_leads=analyst_leads,
        m1_candidates=m1_candidates,
        acceptance_report=acceptance_report,
        run_manifest=run_manifest,
        run_trace=run_trace,
        summary=summary,
    )


def run(
    date: str | None = None,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    runs_root: str | Path = RUNS_DIR,
    strict_requested: bool = False,
) -> PipelineResult:
    _setup_logging()
    started_at = _now_iso()
    current = _run_clock(date)
    run_date = date or current.strftime("%Y-%m-%d")
    trace = RunTrace(
        run_date=run_date,
        mode="daily",
        metadata={
            "config_path": str(config_path),
            "strict_requested": strict_requested,
        },
    )

    with trace.span("load_metasources") as span:
        sources: list[Metasource] = load_metasources(config_path)
        span.set_counts(enabled_sources=len(sources))
    logger.info("Loaded %d enabled sources from %s", len(sources), config_path)

    with trace.span("fetch_sources", metadata={"source_count": len(sources)}) as span:
        raw_items, failures = fetch_all(sources, trace=trace)
        span.set_counts(raw_items=len(raw_items), source_failures=len(failures))
    with trace.span("enrich_raw_items") as span:
        raw_items = add_mincit_zonas_francas_change_events(
            raw_items,
            runs_root=runs_root,
            run_date=run_date,
            now=current,
        )
        raw_items = link_legislative_followups(raw_items)
        raw_items = link_official_legal_records(raw_items)
        span.set_counts(raw_items=len(raw_items))
    with trace.span("build_legislative_reconciliations") as span:
        legislative_reconciliations = build_legislative_reconciliations(raw_items)
        span.set_counts(records=len(legislative_reconciliations))
    logger.info(
        "Fetched %d raw items; %d source failures", len(raw_items), len(failures)
    )

    with trace.span("clean_and_rank_items") as span:
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
        span.set_counts(
            cleaned_items=len(cleaned),
            rankable_items=len(rankable),
            clusters=len(ranked),
        )
    logger.info("Built %d clusters", len(ranked))

    keywords = topic_keywords(rankable, top_n=5)
    with trace.span("build_source_health") as span:
        source_health = build_source_health(
            sources, raw_items, cleaned, rankable, failures
        )
        span.set_counts(source_health_records=len(source_health))
    with trace.span("build_indicator_watch") as span:
        structured_indicators = fetch_structured_indicator_observations()
        indicator_watch = build_indicator_watch(
            raw_items, cleaned, structured_indicators, now=current
        )
        span.set_counts(
            indicators=len(indicator_watch),
            observed_indicators=sum(
                1 for indicator in indicator_watch if indicator.status == "observed"
            ),
        )
    with trace.span("build_indicator_tension_cards") as span:
        indicator_tension_cards = build_indicator_tension_cards(indicator_watch)
        span.set_counts(cards=len(indicator_tension_cards))

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
    with trace.span("build_m1_candidates") as span:
        m1_candidates = build_m1_candidates(
            summary,
            ranked,
            failures,
            keywords,
            source_health=source_health,
            indicator_watch=indicator_watch,
            legislative_reconciliations=legislative_reconciliations,
        )
        span.set_counts(
            candidates=len(m1_candidates.get("candidates") or []),
            rejected=len(m1_candidates.get("rejected") or []),
            source_caveats=len(m1_candidates.get("source_caveats") or []),
        )
    with trace.span("build_m2_artifacts") as span:
        m2_ranked_questions = build_legislative_m2_ranking(
            legislative_reconciliations,
            summary,
            generated_at=finished_at,
        )
        m2_review_packet = build_m2_review_packet(
            summary,
            raw_items,
            cleaned,
            m1_candidates,
            m2_ranked_questions,
            legislative_reconciliations,
            source_health,
            indicator_watch,
            indicator_tension_cards,
            generated_at=finished_at,
        )
        span.set_counts(
            ranked_questions=len(m2_ranked_questions.get("ranked_questions") or []),
            review_items=len(m2_review_packet.get("review_items") or []),
        )
    with trace.span("build_analyst_leads") as span:
        procurement_concentration_leads = build_procurement_concentration_leads(
            raw_items,
            cleaned,
        )
        analyst_leads = build_analyst_leads(
            summary,
            m2_review_packet,
            indicator_tension_cards,
            procurement_concentration_leads,
            generated_at=finished_at,
        )
        analyst_summary = analyst_leads.get("summary") or {}
        span.set_counts(
            leads=analyst_summary.get("lead_count"),
            forecast_questions=analyst_summary.get("forecast_question_count"),
            analyst_insights=analyst_summary.get("analyst_insight_count"),
            investigation_leads=analyst_summary.get("investigation_lead_count"),
            procurement_concentration_leads=len(procurement_concentration_leads),
        )
    with trace.span("build_acceptance_report") as span:
        acceptance_report = build_acceptance_report(
            summary,
            m1_candidates,
            source_health,
            failures,
            cleaned,
            indicator_watch,
        )
        span.set_metadata(
            acceptance_status=acceptance_report.get("status"),
            strict_pass=acceptance_report.get("strict_pass"),
        )
        span.set_counts(
            errors=acceptance_report.get("error_count"),
            warnings=acceptance_report.get("warning_count"),
        )

    run_dir = Path(runs_root) / run_date
    with trace.span("write_artifacts", metadata={"run_dir": str(run_dir)}) as span:
        run_dir.mkdir(parents=True, exist_ok=True)
        _write_json(run_dir / "raw_items.json", [asdict(r) for r in raw_items])
        _write_json(run_dir / "cleaned_items.json", [asdict(c) for c in cleaned])
        _write_json(run_dir / "clusters.json", [asdict(c) for c in ranked])
        _write_json(
            run_dir / "indicator_watch.json", [asdict(i) for i in indicator_watch]
        )
        _write_json(run_dir / "indicator_tension_cards.json", indicator_tension_cards)
        (run_dir / "indicator_tension_cards.md").write_text(
            render_indicator_tension_cards(
                indicator_tension_cards,
                run_date=run_date,
            ),
            encoding="utf-8",
        )
        _write_json(
            run_dir / "source_failures.json", [asdict(f) for f in failures]
        )
        _write_json(run_dir / "source_health.json", [asdict(h) for h in source_health])
        _write_json(
            run_dir / "legislative_reconciler.json", legislative_reconciliations
        )
        _write_json(run_dir / "m2_ranked_questions.json", m2_ranked_questions)
        _write_json(run_dir / "m2_review_packet.json", m2_review_packet)
        (run_dir / "m2_review_packet.md").write_text(
            render_m2_review_packet(m2_review_packet),
            encoding="utf-8",
        )
        _write_json(run_dir / "analyst_leads.json", analyst_leads)
        (run_dir / "analyst_leads.md").write_text(
            render_analyst_leads(analyst_leads),
            encoding="utf-8",
        )
        _write_json(run_dir / "m1_candidates.json", m1_candidates)
        _write_json(run_dir / "acceptance_report.json", acceptance_report)
        brief_text = render_brief(
            summary,
            ranked,
            failures,
            cleaned,
            keywords,
            source_health=source_health,
            indicator_watch=indicator_watch,
            m1_candidates=m1_candidates,
            acceptance_report=acceptance_report,
            m2_ranked_questions=m2_ranked_questions,
            m2_review_packet=m2_review_packet,
        )
        (run_dir / "metasource_brief.md").write_text(brief_text, encoding="utf-8")
        handoff_text = render_m2_handoff(
            summary,
            ranked,
            failures,
            keywords,
            source_health=source_health,
            indicator_watch=indicator_watch,
            m1_candidates=m1_candidates,
            acceptance_report=acceptance_report,
            m2_ranked_questions=m2_ranked_questions,
            m2_review_packet=m2_review_packet,
        )
        (run_dir / "m2_handoff.md").write_text(handoff_text, encoding="utf-8")
        _write_json(run_dir / "run_summary.json", asdict(summary))
        span.set_counts(artifacts_written=19)
    run_trace = trace.to_dict()
    _write_json(run_dir / "run_trace.json", run_trace)
    run_manifest = build_run_manifest(
        run_dir,
        summary,
        config_path=config_path,
        strict_requested=strict_requested,
        acceptance_report=acceptance_report,
        m1_candidates=m1_candidates,
        legislative_reconciliations=legislative_reconciliations,
        m2_ranked_questions=m2_ranked_questions,
        m2_review_packet=m2_review_packet,
        indicator_tension_cards=indicator_tension_cards,
        analyst_leads=analyst_leads,
    )
    _write_json(run_dir / "run_manifest.json", run_manifest)

    logger.info("Wrote artifacts to %s", run_dir)
    return PipelineResult(
        run_dir=run_dir,
        raw_items=raw_items,
        cleaned_items=cleaned,
        clusters=ranked,
        failures=failures,
        source_health=source_health,
        indicator_watch=indicator_watch,
        indicator_tension_cards=indicator_tension_cards,
        legislative_reconciliations=legislative_reconciliations,
        m2_ranked_questions=m2_ranked_questions,
        m2_review_packet=m2_review_packet,
        analyst_leads=analyst_leads,
        m1_candidates=m1_candidates,
        acceptance_report=acceptance_report,
        run_manifest=run_manifest,
        run_trace=run_trace,
        summary=summary,
    )
