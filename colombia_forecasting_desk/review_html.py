"""Deterministic HTML review surface for daily runs and recent-run trends.

This module renders a human-friendly review surface from the structured run
artifacts that the M1/M2 pipeline already writes. It is intentionally a *pure
renderer*: it never runs an LLM, never touches the network, and adds no new
dependency. HTML is hand-built with :func:`html.escape`, mirroring the way
``brief.py`` builds Markdown.

Two surfaces are produced:

* a per-run "daily review" (``runs/YYYY-MM-DD/review.html``) that makes a
  monitor/no-post day feel informative, and
* a recent-runs index (``runs/review_index.html``) that surfaces patterns
  across runs, most importantly forecast-question droughts and recurring
  themes.

Determinism note: every timestamp shown comes from the artifacts themselves
(``finished_at`` / ``generated_at``), never ``datetime.now()``. Given the same
artifacts the output is byte-stable, so re-rendering is a no-op and the smoke
tests stay meaningful. These HTML files are derived purely from artifacts that
``check_artifact_parity.py`` already compares, so they are deliberately left out
of that guard (CSS tweaks must not trip a behavior-parity check).

The renderer never promotes, scores, or reinterprets anything. Tension cards,
market-pricing rows, and co-occurrence bundles are shown with their own
advisory/context labels and caveats so the surface cannot be mistaken for a
probability input.
"""
from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "review_html.v1"
DEFAULT_WINDOW = 14
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Per-card display caps keep the page scannable; full detail lives in the
# linked source artifacts.
INSIGHT_LIMIT = 8
INVESTIGATION_LIMIT = 8
EVIDENCE_LIMIT = 4
CAVEAT_LIMIT = 3
BUNDLE_LIMIT = 6
MONITOR_QUEUE_LIMIT = 8
RECURRING_LIMIT = 12

# JSON artifacts the renderer reads. Missing files load as ``None`` so old runs
# (which may lack newer artifacts) render without crashing.
JSON_ARTIFACTS = (
    "run_summary.json",
    "run_manifest.json",
    "acceptance_report.json",
    "analyst_leads.json",
    "m2_ranked_questions.json",
    "indicator_watch.json",
    "indicator_tension_cards.json",
    "market_pricing_watch.json",
    "cooccurrence_bundles.json",
    "source_health.json",
)

# Ordered list of files the daily view links back to, when present.
LINK_ARTIFACTS: tuple[tuple[str, str], ...] = (
    ("metasource_brief.md", "Daily brief (human-readable)"),
    ("analyst_leads.md", "Analyst leads"),
    ("m2_review_packet.md", "M2 review packet"),
    ("m2_handoff.md", "M2 handoff packet"),
    ("indicator_tension_cards.md", "Indicator tension cards"),
    ("market_pricing_watch.md", "Market-pricing watch"),
    ("cooccurrence_bundles.md", "Co-occurrence bundles"),
    ("candidate_questions.md", "Candidate questions"),
    ("analyst_leads.json", "Analyst leads (JSON)"),
    ("m1_candidates.json", "M1 candidates (JSON)"),
    ("indicator_watch.json", "Indicator watch (JSON)"),
    ("source_health.json", "Source health (JSON)"),
    ("acceptance_report.json", "Acceptance report (JSON)"),
    ("run_manifest.json", "Run manifest (JSON)"),
    ("run_trace.json", "Run trace (JSON)"),
)

# Hand-written narrative files the human/LLM may add to a run. They are linked,
# never embedded, so the HTML stays deterministic.
HUMAN_NOTE_FILES: tuple[tuple[str, str], ...] = (
    ("human_decisions.md", "Human decisions"),
    ("daily_comparison.md", "Daily comparison"),
)

HIGH_IMPACT_SOURCE_TERMS = {
    "registraduria",
    "registraduría",
    "cne",
    "dian",
    "minhacienda",
    "banrep",
    "dane",
    "congreso",
    "senado",
    "camara",
    "cámara",
    "gacetas",
    "diario_oficial",
}
INDICATOR_COVERAGE_TERMS = {
    "indicator",
    "current result",
    "labor",
    "mercado_laboral",
    "geih",
    "empleo",
    "ipc",
    "inflation",
    "icoced",
    "tax_collection",
    "recaudo",
}
EXECUTION_ENVIRONMENT_FAILURE_TERMS = {
    "connecterror",
    "dns",
    "host allowlist",
    "name resolution",
    "network is unreachable",
    "nodename nor servname",
    "sandbox",
    "targetclosederror",
    "temporary failure",
}

SOURCE_RELIABILITY_BUCKETS: tuple[dict[str, str], ...] = (
    {
        "id": "high_impact_failures",
        "label": "High-impact source failures",
        "note": "Priority sources that failed or degraded, reducing decision confidence.",
        "variant": "alert",
    },
    {
        "id": "decision_relevant_parser_gaps",
        "label": "Decision-relevant parser gaps",
        "note": "Link-only or document-unparsed sources that can block top-lead review.",
        "variant": "watch",
    },
    {
        "id": "indicator_coverage_gaps",
        "label": "Indicator coverage gaps",
        "note": "Indicator-specific failed, stale, or unparsed coverage.",
        "variant": "watch",
    },
    {
        "id": "execution_environment_failures",
        "label": "Execution environment failures",
        "note": "Sandbox, DNS, or network-wide failures; rerun before treating as source health.",
        "variant": "muted",
    },
    {
        "id": "background_parser_debt",
        "label": "Background parser debt",
        "note": "Lower-priority parser or link-only debt to keep visible but de-emphasized.",
        "variant": "muted",
    },
)
SOURCE_RELIABILITY_BUCKET_BY_ID = {
    bucket["id"]: bucket for bucket in SOURCE_RELIABILITY_BUCKETS
}


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None


def _extract_recorded_decision(run_dir: Path, present: set[str]) -> dict[str, str] | None:
    """Best-effort, tolerant parse of the recorded human decision.

    Reads only two known single-value lines from ``human_decisions.md``. If the
    file is absent or the prose drifts, this quietly returns ``None`` and the
    daily view falls back to the artifact-derived status plus a plain link.
    """
    if "human_decisions.md" not in present:
        return None
    try:
        text = (run_dir / "human_decisions.md").read_text(encoding="utf-8")
    except OSError:
        return None
    out: dict[str, str] = {}
    decision = re.search(r"Decision:\s*`?([A-Za-z0-9_]+)`?", text)
    if decision:
        out["decision"] = decision.group(1)
    post = re.search(r"Post today:\s*([A-Za-z]+)", text)
    if post:
        out["post_today"] = post.group(1).lower()
    return out or None


def _extract_human_monitor_queue(run_dir: Path, present: set[str]) -> list[dict[str, str]]:
    """Parse a numbered human follow-up queue from ``human_decisions.md``.

    The daily workflow often records a short editorial queue under headings like
    ``## Monitor Queue`` or ``## Election And Market Follow-Up Queue``. This
    parser intentionally reads only numbered Markdown list items from queue-ish
    sections and ignores all other prose, so drift elsewhere in the file cannot
    affect the generated review surface.
    """
    if "human_decisions.md" not in present:
        return []
    try:
        lines = (run_dir / "human_decisions.md").read_text(encoding="utf-8").splitlines()
    except OSError:
        return []

    items: list[dict[str, str]] = []
    active_heading = ""
    current: str | None = None
    item_re = re.compile(r"^\s*\d+[.)]\s+(.+?)\s*$")

    def flush() -> None:
        nonlocal current
        if current:
            items.append(
                {
                    "label": normalize_markdown_text(current),
                    "kind": "human priority",
                    "note": normalize_markdown_text(active_heading),
                }
            )
        current = None

    for line in lines:
        heading = re.match(r"^#{2,6}\s+(.+?)\s*$", line)
        if heading:
            flush()
            label = heading.group(1)
            active_heading = label if _is_queue_heading(label) else ""
            continue
        if not active_heading:
            continue
        match = item_re.match(line)
        if match:
            flush()
            current = match.group(1)
            continue
        if current and line.startswith((" ", "\t")) and line.strip():
            current = f"{current} {line.strip()}"
    flush()
    return items[:MONITOR_QUEUE_LIMIT]


def _is_queue_heading(text: str) -> bool:
    folded = text.lower()
    return "queue" in folded or "follow-up" in folded or "follow up" in folded


def normalize_markdown_text(text: str) -> str:
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    return " ".join(text.split())


def load_run_artifacts(run_dir: Path) -> dict[str, Any]:
    """Load a run directory's artifacts into a plain dict for pure rendering.

    Keys prefixed with ``_`` are renderer metadata (run date, present files,
    recorded human decision). All render functions operate on this dict, so they
    can be unit-tested with a hand-built dict and never hit the filesystem.
    """
    art: dict[str, Any] = {name: _read_json(run_dir / name) for name in JSON_ARTIFACTS}
    present = (
        {p.name for p in run_dir.iterdir() if p.is_file()} if run_dir.exists() else set()
    )
    art["_run_date"] = run_dir.name
    art["_present"] = present
    art["_human_decision"] = _extract_recorded_decision(run_dir, present)
    art["_human_monitor_queue"] = _extract_human_monitor_queue(run_dir, present)
    return art


def find_run_dirs(runs_root: Path, window: int | None = DEFAULT_WINDOW) -> list[Path]:
    """Return dated run directories sorted ascending, limited to the last ``window``."""
    if not runs_root.exists():
        return []
    dirs = sorted(
        (p for p in runs_root.iterdir() if p.is_dir() and DATE_RE.match(p.name)),
        key=lambda p: p.name,
    )
    if window and window > 0:
        return dirs[-window:]
    return dirs


# --------------------------------------------------------------------------- #
# Derivations (deterministic, no rendering)
# --------------------------------------------------------------------------- #
def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class Decision:
    status: str  # "monitor_no_post" | "review_for_post"
    label: str
    headline: str
    facts: list[str]
    m3_ready: bool
    recorded_human_decision: dict[str, str] | None = None


def derive_decision(art: dict[str, Any]) -> Decision:
    """Derive the post/monitor status from structured artifacts only.

    A run is "review for post" only when an artifact actually carries an
    M3-ready signal: a ``forecast_question`` lead, or a non-empty
    ``ready_for_m3`` M2 bucket. Otherwise it is a monitoring run *by design* —
    surfaced calmly, not as an error. This mirrors the M3 gate; it never loosens
    it.
    """
    leads = art.get("analyst_leads.json") or {}
    summary = leads.get("summary", {}) or {}
    fq = _as_int(summary.get("forecast_question_count"))
    insights = _as_int(summary.get("analyst_insight_count"))
    investigations = _as_int(summary.get("investigation_lead_count"))
    review_items = _as_int(summary.get("review_item_count"))

    ranked = art.get("m2_ranked_questions.json") or {}
    buckets = ranked.get("bucket_counts", {}) or {}
    ready = _as_int(buckets.get("ready_for_m3"))

    m3_ready = fq > 0 or ready > 0

    bucket_text = (
        ", ".join(
            f"{name.replace('_', ' ')} {count}"
            for name, count in sorted(buckets.items())
        )
        or "none recorded"
    )
    facts = [
        f"{fq} forecast-question lead(s) promoted in analyst_leads.json.",
        f"M2 legislative triage produced no ready_for_m3 bucket (buckets: {bucket_text})."
        if not ready
        else f"M2 triage flagged {ready} ready_for_m3 record(s) (buckets: {bucket_text}).",
        f"{insights} analyst insight(s) and {investigations} investigation lead(s) recorded.",
        f"{review_items} item(s) waiting in the M2 review queue for sampling.",
    ]

    if m3_ready:
        return Decision(
            status="review_for_post",
            label="Review for possible forecast",
            headline=(
                "An artifact carries an M3-ready signal today. Open the M2 review "
                "packet and the analyst leads before deciding to post."
            ),
            facts=facts,
            m3_ready=True,
            recorded_human_decision=art.get("_human_decision"),
        )
    return Decision(
        status="monitor_no_post",
        label="Monitoring — no new forecast",
        headline=(
            "No M3-ready forecast question in today's artifacts. This is a "
            "monitoring run by design — the desk recorded insights and leads, "
            "not a publishable forecast."
        ),
        facts=facts,
        m3_ready=False,
        recorded_human_decision=art.get("_human_decision"),
    )


@dataclass
class RunRow:
    date: str
    raw_items: int
    cleaned_items: int
    clusters: int
    candidates: int
    analyst_leads: int
    forecast_questions: int
    analyst_insights: int
    investigation_leads: int
    tension_cards: int
    bundles: int
    market_observed: int
    sources_checked: int
    sources_failed: int
    acceptance_status: str
    acceptance_warnings: int
    acceptance_errors: int
    review_items: int
    m2_buckets: dict[str, int]
    m3_ready: bool
    finished_at: str


def summarize_run(art: dict[str, Any]) -> RunRow:
    """Reduce one run's artifacts to a flat row of comparable counts."""
    summary = art.get("run_summary.json") or {}
    manifest = art.get("run_manifest.json") or {}
    manifest_counts = manifest.get("counts", {}) or {}
    leads = art.get("analyst_leads.json") or {}
    lead_summary = leads.get("summary", {}) or {}
    ranked = art.get("m2_ranked_questions.json") or {}
    buckets = {k: _as_int(v) for k, v in (ranked.get("bucket_counts", {}) or {}).items()}
    acceptance = art.get("acceptance_report.json") or {}
    markets = art.get("market_pricing_watch.json") or []
    bundles = art.get("cooccurrence_bundles.json") or []

    decision = derive_decision(art)

    return RunRow(
        date=art.get("_run_date", summary.get("run_date", "")),
        raw_items=_as_int(summary.get("raw_items", manifest_counts.get("raw_items"))),
        cleaned_items=_as_int(
            summary.get("cleaned_items", manifest_counts.get("cleaned_items"))
        ),
        clusters=_as_int(summary.get("clusters", manifest_counts.get("clusters"))),
        candidates=_as_int(manifest_counts.get("m1_candidates")),
        analyst_leads=_as_int(lead_summary.get("lead_count")),
        forecast_questions=_as_int(lead_summary.get("forecast_question_count")),
        analyst_insights=_as_int(lead_summary.get("analyst_insight_count")),
        investigation_leads=_as_int(lead_summary.get("investigation_lead_count")),
        tension_cards=_as_int(lead_summary.get("indicator_tension_card_count")),
        bundles=len(bundles) if isinstance(bundles, list) else 0,
        market_observed=sum(
            1
            for row in (markets if isinstance(markets, list) else [])
            if (row or {}).get("status") == "observed"
        ),
        sources_checked=_as_int(summary.get("sources_checked")),
        sources_failed=_as_int(summary.get("sources_failed")),
        acceptance_status=str(acceptance.get("status", "unknown")),
        acceptance_warnings=_as_int(acceptance.get("warning_count")),
        acceptance_errors=_as_int(acceptance.get("error_count")),
        review_items=_as_int(lead_summary.get("review_item_count")),
        m2_buckets=buckets,
        m3_ready=decision.m3_ready,
        finished_at=str(summary.get("finished_at") or manifest.get("generated_at") or ""),
    )


def _leads_of_type(art: dict[str, Any], lead_type: str) -> list[dict[str, Any]]:
    leads = art.get("analyst_leads.json") or {}
    return [
        lead
        for lead in (leads.get("leads", []) or [])
        if isinstance(lead, dict) and lead.get("lead_type") == lead_type
    ]


def collect_source_caveats(art: dict[str, Any]) -> list[dict[str, Any]]:
    """List sources whose health makes silence unreliable today.

    This deliberately flags only *genuine visibility gaps*, not every non-ok
    status. A source that parsed content but produced no rankable candidate is
    working fine (a registry or agenda source often does this), so it is not a
    caveat. A source is flagged when:

    * it failed to fetch (``failure_count`` > 0 or ``status`` == ``failed``);
    * it has no parser yet (``onboarding_status`` == ``needs_parser``) and so
      returned nothing rankable/raw — we cannot see it, so silence is unreliable;
    * it returned document links it could not parse into content.

    Each entry carries a short reason and any failure messages so the reader
    knows whether "no signal" means "nothing happened" or "we could not see".
    Caveats also carry a deterministic reliability bucket for daily review and
    recent-run aggregation; the bucket changes presentation only, never M1/M2/M3
    decisions.
    """
    health = art.get("source_health.json") or []
    execution_environment_source_ids = _execution_environment_failure_source_ids(
        health, art.get("acceptance_report.json") or {}
    )
    current_lead_source_ids = _current_lead_source_ids(art)
    caveats: list[dict[str, Any]] = []
    for record in health if isinstance(health, list) else []:
        if not isinstance(record, dict):
            continue
        reasons: list[str] = []
        failure_count = _as_int(record.get("failure_count"))
        status = str(record.get("status", ""))
        onboarding = str(record.get("onboarding_status", ""))
        doc_links = _as_int(record.get("document_link_count"))
        parsed = _as_int(record.get("parsed_content_count"))
        rankable = _as_int(record.get("rankable_count"))
        if failure_count > 0:
            reasons.append(f"{failure_count} fetch failure(s)")
        elif status == "failed":
            reasons.append("fetch failed")
        elif status in ("stale", "unparsed"):
            reasons.append(status.replace("_", " "))
        if onboarding == "needs_parser" and status in ("no_raw", "no_rankable"):
            reasons.append(f"no parser yet ({status})")
        if doc_links > 0 and parsed == 0 and rankable == 0:
            reasons.append(f"{doc_links} document link(s) but no parsed content")
        if not reasons:
            continue
        bucket = _source_caveat_bucket(
            record,
            is_execution_environment_failure=(
                str(record.get("source_id", "")) in execution_environment_source_ids
            ),
            is_current_lead_source=(
                str(record.get("source_id", "")) in current_lead_source_ids
            ),
        )
        caveats.append(
            {
                "source_id": record.get("source_id", ""),
                "source_name": record.get("source_name", record.get("source_id", "")),
                "bucket": bucket,
                "reasons": reasons,
                "messages": list(record.get("failures", []) or [])[:2],
            }
        )
    for indicator in _indicator_coverage_caveats(art):
        caveats.append(indicator)
    caveats.sort(key=lambda c: c["source_id"])
    return caveats


def _indicator_coverage_caveats(art: dict[str, Any]) -> list[dict[str, Any]]:
    rows = art.get("indicator_watch.json") or []
    caveats: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", ""))
        freshness = str(row.get("freshness_status", ""))
        if status == "observed" and freshness not in {"failed", "stale", "unparsed"}:
            continue
        indicator_id = str(row.get("indicator_id") or row.get("name") or "")
        if not indicator_id:
            continue
        reason = f"indicator {status or freshness or 'not observed'}"
        if freshness and freshness != status:
            reason = f"{reason}; freshness {freshness}"
        messages = [
            str(value)
            for value in [
                row.get("headline"),
                row.get("error_message"),
                row.get("next_step"),
            ]
            if value
        ][:2]
        caveats.append(
            {
                "source_id": f"indicator:{indicator_id}",
                "source_name": row.get("name") or row.get("source_name") or indicator_id,
                "bucket": "indicator_coverage_gaps",
                "reasons": [reason],
                "messages": messages,
            }
        )
    return caveats


def _execution_environment_failure_source_ids(
    health: Any, acceptance: dict[str, Any]
) -> set[str]:
    records = [r for r in health if isinstance(r, dict)] if isinstance(health, list) else []
    failed = [
        record
        for record in records
        if _as_int(record.get("failure_count")) > 0
        or str(record.get("status", "")) == "failed"
    ]
    env_failed = [
        record for record in failed if _has_execution_environment_failure_message(record)
    ]
    issue_codes = {
        str(issue.get("code", ""))
        for issue in (acceptance.get("issues", []) or [])
        if isinstance(issue, dict)
    }
    mass_failure = len(env_failed) >= 3 or (
        "operational_source_failure_share_too_high" in issue_codes
        and len(env_failed) >= 2
    )
    if not mass_failure:
        return set()
    return {str(record.get("source_id", "")) for record in env_failed}


def _has_execution_environment_failure_message(record: dict[str, Any]) -> bool:
    messages = " ".join(str(message) for message in (record.get("failures", []) or []))
    haystack = messages.lower()
    return any(term in haystack for term in EXECUTION_ENVIRONMENT_FAILURE_TERMS)


def _current_lead_source_ids(art: dict[str, Any]) -> set[str]:
    source_ids: set[str] = set()

    def add(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            source_ids.add(value.strip())
        elif isinstance(value, list):
            for item in value:
                add(item)

    leads = (art.get("analyst_leads.json") or {}).get("leads", []) or []
    for lead in leads if isinstance(leads, list) else []:
        if not isinstance(lead, dict):
            continue
        add(lead.get("source_id"))
        add(lead.get("source_ids"))
        for evidence in lead.get("evidence", []) or []:
            if not isinstance(evidence, dict):
                continue
            add(evidence.get("source_id"))
            add(evidence.get("source_ids"))

    ranked = art.get("m2_ranked_questions.json") or {}
    for entry in ranked.get("review_queue", []) or []:
        if not isinstance(entry, dict):
            continue
        add(entry.get("source_id"))
        add(entry.get("source_ids"))

    return source_ids


def _source_caveat_bucket(
    record: dict[str, Any],
    *,
    is_execution_environment_failure: bool = False,
    is_current_lead_source: bool = False,
) -> str:
    if is_execution_environment_failure:
        return "execution_environment_failures"
    source_id = str(record.get("source_id", ""))
    source_name = str(record.get("source_name", ""))
    haystack = f"{source_id} {source_name}".lower()
    is_high_impact = any(term in haystack for term in HIGH_IMPACT_SOURCE_TERMS)
    is_indicator_gap = any(term in haystack for term in INDICATOR_COVERAGE_TERMS)
    if is_indicator_gap:
        return "indicator_coverage_gaps"
    if _as_int(record.get("failure_count")) > 0 or str(record.get("status", "")) == "failed":
        if is_high_impact:
            return "high_impact_failures"
    if (
        _as_int(record.get("document_link_count")) > 0
        and _as_int(record.get("parsed_content_count")) == 0
        and _as_int(record.get("rankable_count")) == 0
        and (is_high_impact or is_current_lead_source)
    ):
        return "decision_relevant_parser_gaps"
    return "background_parser_debt"


def derive_monitor_queue(
    art: dict[str, Any], limit: int = MONITOR_QUEUE_LIMIT
) -> list[dict[str, str]]:
    """Build a deterministic "what to sample next" queue from artifacts.

    Prefer the explicit numbered queue from ``human_decisions.md`` when present.
    Otherwise fall back to a proxy that combines today's investigation leads
    with the top of the M2 review queue. It does not promote anything; it just
    collects what the artifacts already flagged for follow-up.
    """
    human_queue = [
        item for item in art.get("_human_monitor_queue") or [] if isinstance(item, dict)
    ]
    if human_queue:
        return human_queue[:limit]

    items: list[dict[str, str]] = []
    seen: set[str] = set()

    for lead in _leads_of_type(art, "investigation_lead"):
        label = (lead.get("title") or lead.get("claim_or_question") or "").strip()
        if not label or label in seen:
            continue
        seen.add(label)
        items.append(
            {
                "label": label,
                "kind": "investigation lead",
                "note": (lead.get("next_check") or "").strip(),
            }
        )

    ranked = art.get("m2_ranked_questions.json") or {}
    for entry in ranked.get("review_queue", []) or []:
        if not isinstance(entry, dict):
            continue
        label = (entry.get("question_seed") or entry.get("canonical_bill_id") or "").strip()
        if not label or label in seen:
            continue
        seen.add(label)
        items.append(
            {
                "label": label,
                "kind": f"M2 {entry.get('bucket', 'review')}",
                "note": str(entry.get("canonical_bill_id") or ""),
            }
        )

    return items[:limit]


def count_forecast_drought(rows: list[RunRow]) -> int:
    """Count trailing consecutive runs with no M3-ready signal (rows ascending)."""
    streak = 0
    for row in reversed(rows):
        if row.m3_ready:
            break
        streak += 1
    return streak


@dataclass
class ThemeCount:
    label: str
    family: str
    days: int
    dates: list[str] = field(default_factory=list)


def _aggregate(
    per_run: list[tuple[str, dict[str, Any]]],
    extract: Any,
) -> list[ThemeCount]:
    counts: dict[str, dict[str, Any]] = {}
    for date, art in per_run:
        seen: set[str] = set()
        for label, family in extract(art):
            label = (label or "").strip()
            if not label or label in seen:
                continue
            seen.add(label)
            bucket = counts.setdefault(label, {"family": family or "", "dates": []})
            bucket["dates"].append(date)
    out = [
        ThemeCount(label=label, family=v["family"], days=len(v["dates"]), dates=sorted(v["dates"]))
        for label, v in counts.items()
    ]
    out.sort(key=lambda t: (-t.days, t.label))
    return out


def aggregate_recurring_insights(
    per_run: list[tuple[str, dict[str, Any]]]
) -> list[ThemeCount]:
    def extract(art: dict[str, Any]):
        for lead in _leads_of_type(art, "analyst_insight"):
            ctx = lead.get("review_context") or {}
            yield lead.get("title") or lead.get("claim_or_question", "")[:80], ctx.get("family", "")

    return _aggregate(per_run, extract)


def aggregate_tension_cards(
    per_run: list[tuple[str, dict[str, Any]]]
) -> list[ThemeCount]:
    def extract(art: dict[str, Any]):
        cards = art.get("indicator_tension_cards.json") or []
        for card in cards if isinstance(cards, list) else []:
            if isinstance(card, dict):
                yield card.get("title") or card.get("card_id", ""), card.get("family", "")

    return _aggregate(per_run, extract)


def aggregate_source_issues(
    per_run: list[tuple[str, dict[str, Any]]]
) -> list[ThemeCount]:
    def extract(art: dict[str, Any]):
        for caveat in collect_source_caveats(art):
            label = caveat.get("source_name") or caveat.get("source_id", "")
            bucket = SOURCE_RELIABILITY_BUCKET_BY_ID.get(str(caveat.get("bucket", "")))
            yield label, (bucket or {}).get("label") or "; ".join(
                caveat.get("reasons", [])
            )

    return _aggregate(per_run, extract)


# --------------------------------------------------------------------------- #
# HTML helpers
# --------------------------------------------------------------------------- #
def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _attr(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _safe_url(url: Any) -> str:
    """Return ``url`` only if it is a plausible http(s) link.

    Guards against malformed artifact data (e.g. a stringified dict that slipped
    into a ``url`` field) and against unsafe schemes such as ``javascript:``.
    Anything else degrades to plain text instead of a broken/unsafe href.
    """
    if isinstance(url, str) and (url.startswith("http://") or url.startswith("https://")):
        return url
    return ""


def _truncate(items: list[Any], limit: int) -> tuple[list[Any], int]:
    if len(items) <= limit:
        return items, 0
    return items[:limit], len(items) - limit


def _pill(text: str, variant: str = "") -> str:
    cls = "pill" + (f" pill--{variant}" if variant else "")
    return f'<span class="{cls}">{_esc(text)}</span>'


def _disposition_variant(disposition: str) -> str:
    return {
        "select_for_evidence_pack": "post",
        "monitor_or_research": "watch",
        "research_more_before_m3": "muted",
    }.get(disposition, "muted")


def _stat(value: Any, label: str, variant: str = "") -> str:
    cls = "stat__value" + (f" stat__value--{variant}" if variant else "")
    return (
        '<div class="stat">'
        f'<div class="{cls}">{_esc(value)}</div>'
        f'<div class="stat__label">{_esc(label)}</div>'
        "</div>"
    )


def _section(label: str, body: str, note: str = "", index: int = 0) -> str:
    note_html = f'<p class="section__note">{_esc(note)}</p>' if note else ""
    return (
        f'<section class="section" style="--i:{index}">'
        f'<div class="section__head"><h2 class="section__label">{_esc(label)}</h2>{note_html}</div>'
        f"{body}"
        "</section>"
    )


def _evidence_dl(evidence: list[Any]) -> str:
    shown, extra = _truncate([e for e in evidence if isinstance(e, dict)], EVIDENCE_LIMIT)
    if not shown:
        return ""
    rows: list[str] = []
    for item in shown:
        label = item.get("label") or item.get("source") or "evidence"
        value = item.get("value") or item.get("headline") or ""
        source = item.get("source") or ""
        url = _safe_url(item.get("url"))
        term = f'<a href="{_attr(url)}">{_esc(label)}</a>' if url else _esc(label)
        meta = f' <span class="dl__src">{_esc(source)}</span>' if source else ""
        rows.append(
            f'<div class="dl__row"><dt>{term}</dt>'
            f'<dd>{_esc(value)}{meta}</dd></div>'
        )
    extra_html = f'<p class="more">+{extra} more in source artifact</p>' if extra else ""
    return f'<dl class="dl">{"".join(rows)}</dl>{extra_html}'


def _caveats_list(caveats: list[Any]) -> str:
    shown, extra = _truncate([str(c) for c in caveats], CAVEAT_LIMIT)
    if not shown:
        return ""
    lis = "".join(f"<li>{_esc(c)}</li>" for c in shown)
    extra_html = f"<li class='more'>+{extra} more</li>" if extra else ""
    return f'<ul class="caveats">{lis}{extra_html}</ul>'


# --------------------------------------------------------------------------- #
# Daily view
# --------------------------------------------------------------------------- #
def _render_lead_card(lead: dict[str, Any], lead_type_variant: str) -> str:
    title = lead.get("title") or lead.get("claim_or_question") or "(untitled lead)"
    claim = lead.get("claim_or_question") or ""
    disposition = lead.get("disposition") or ""
    next_check = lead.get("next_check") or ""
    ctx = lead.get("review_context") or {}
    family = ctx.get("family") or ""

    tags = [_pill(lead.get("lead_type", "lead").replace("_", " "), lead_type_variant)]
    if disposition:
        tags.append(_pill(disposition.replace("_", " "), _disposition_variant(disposition)))
    if family:
        tags.append(_pill(family.replace("_", " ")))

    claim_html = (
        f'<p class="card__claim">{_esc(claim)}</p>'
        if claim and claim != title
        else ""
    )
    evidence_html = _evidence_dl(lead.get("evidence", []) or [])
    caveats_html = _caveats_list(lead.get("caveats", []) or [])
    next_html = (
        f'<p class="card__next"><span class="card__next-label">Next check</span>{_esc(next_check)}</p>'
        if next_check
        else ""
    )
    return (
        '<article class="card">'
        f'<div class="tags">{"".join(tags)}</div>'
        f'<h3 class="card__title">{_esc(title)}</h3>'
        f"{claim_html}"
        f"{evidence_html}"
        f"{caveats_html}"
        f"{next_html}"
        "</article>"
    )


def _render_tension_cards(art: dict[str, Any], index: int) -> str:
    cards = art.get("indicator_tension_cards.json") or []
    if not isinstance(cards, list) or not cards:
        return ""
    blocks: list[str] = []
    for card in cards:
        if not isinstance(card, dict):
            continue
        tags = [_pill("tension card", "watch")]
        if card.get("family"):
            tags.append(_pill(str(card["family"]).replace("_", " ")))
        if card.get("severity"):
            tags.append(_pill(str(card["severity"])))
        evidence_html = _evidence_dl(card.get("evidence", []) or [])
        caveats_html = _caveats_list(card.get("caveats", []) or [])
        blocks.append(
            '<article class="card">'
            f'<div class="tags">{"".join(tags)}</div>'
            f'<h3 class="card__title">{_esc(card.get("title", card.get("card_id", "")))}</h3>'
            f'<p class="card__claim">{_esc(card.get("trigger", ""))}</p>'
            f'<p class="card__why">{_esc(card.get("why_it_matters", ""))}</p>'
            f"{evidence_html}"
            f"{caveats_html}"
            "</article>"
        )
    body = f'<div class="cards">{"".join(blocks)}</div>'
    return _section(
        "Indicator tension cards",
        body,
        note="Advisory cross-indicator screens only — review prompts, never probability inputs or conclusions.",
        index=index,
    )


def _render_market_pricing(art: dict[str, Any], index: int) -> str:
    rows = art.get("market_pricing_watch.json") or []
    if not isinstance(rows, list) or not rows:
        return ""
    blocks: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        freshness = str(row.get("freshness_status", ""))
        fresh_variant = "ok" if freshness == "current" else "watch"
        status = str(row.get("status", ""))
        tags = []
        if status:
            tags.append(_pill(status, "ok" if status == "observed" else "alert"))
        if freshness:
            tags.append(_pill(freshness, fresh_variant))
        value = row.get("latest_close")
        unit = (row.get("values", {}) or {}).get("unit") or row.get("currency") or ""
        value_html = (
            f'<div class="market__value">{_esc(value)} <span class="market__unit">{_esc(unit)}</span></div>'
            if value is not None
            else ""
        )
        caveats_html = _caveats_list(row.get("caveats", []) or [])
        blocks.append(
            '<article class="card card--market">'
            f'<div class="tags">{"".join(tags)}</div>'
            f'<h3 class="card__title">{_esc(row.get("name", row.get("market_id", "")))}</h3>'
            f"{value_html}"
            f'<p class="card__claim">{_esc(row.get("headline", ""))}</p>'
            f"{caveats_html}"
            "</article>"
        )
    body = f'<div class="cards cards--market">{"".join(blocks)}</div>'
    return _section(
        "Market-pricing context",
        body,
        note="Experimental, fail-closed context only. Not advice, not a ranking signal, not a probability input.",
        index=index,
    )


def _render_bundles(art: dict[str, Any], index: int) -> str:
    bundles = art.get("cooccurrence_bundles.json") or []
    if not isinstance(bundles, list) or not bundles:
        return ""
    shown, extra = _truncate([b for b in bundles if isinstance(b, dict)], BUNDLE_LIMIT)
    blocks: list[str] = []
    for bundle in shown:
        inputs = bundle.get("inputs", []) or []
        input_titles, more_inputs = _truncate(
            [str((i or {}).get("title", "")) for i in inputs if isinstance(i, dict)], 5
        )
        chips = "".join(_pill(t) for t in input_titles if t)
        more_html = f'<span class="more">+{more_inputs} more</span>' if more_inputs else ""
        blocks.append(
            '<article class="card">'
            f'<h3 class="card__title">{_esc(bundle.get("title", bundle.get("bundle_id", "")))}</h3>'
            f'<p class="card__claim">{_esc(bundle.get("description", ""))}</p>'
            f'<div class="tags tags--wrap">{chips}{more_html}</div>'
            "</article>"
        )
    extra_html = f'<p class="more">+{extra} more bundle(s) in cooccurrence_bundles.md</p>' if extra else ""
    body = f'<div class="cards">{"".join(blocks)}</div>{extra_html}'
    return _section(
        "Co-occurrence bundles",
        body,
        note="Neutral routing aids — ingredients that co-occurred today. Not a thesis, not causality.",
        index=index,
    )


def _render_source_caveats(art: dict[str, Any], index: int) -> str:
    caveats = collect_source_caveats(art)
    acceptance = art.get("acceptance_report.json") or {}
    issues = acceptance.get("issues", []) or []

    if not caveats and not issues:
        body = '<p class="empty">No source-health caveats today — coverage looks clean.</p>'
        return _section("Source-health caveats", body, index=index)

    caveat_html = _render_source_reliability_buckets(caveats)

    issue_rows: list[str] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        severity = str(issue.get("severity", ""))
        variant = "alert" if severity == "error" else "watch"
        issue_rows.append(
            '<li class="caveat">'
            f'{_pill(severity or "issue", variant)} '
            f'<span class="caveat__reason">{_esc(issue.get("message", issue.get("code", "")))}</span>'
            "</li>"
        )
    issue_html = (
        f'<h3 class="subhead">Acceptance issues</h3><ul class="caveat-list">{"".join(issue_rows)}</ul>'
        if issue_rows
        else ""
    )
    return _section(
        "Source-health caveats",
        caveat_html + issue_html,
        note="Where silence may mean 'we could not see', not 'nothing happened'.",
        index=index,
    )


def _render_source_reliability_buckets(caveats: list[dict[str, Any]]) -> str:
    blocks: list[str] = []
    for bucket in SOURCE_RELIABILITY_BUCKETS:
        bucket_id = bucket["id"]
        bucket_caveats = [c for c in caveats if c.get("bucket") == bucket_id]
        if not bucket_caveats:
            continue
        rows: list[str] = []
        for caveat in bucket_caveats:
            reason = "; ".join(caveat["reasons"])
            messages = "".join(
                f'<div class="caveat__msg">{_esc(m)}</div>'
                for m in caveat["messages"]
            )
            rows.append(
                f'<li class="caveat caveat--{_attr(bucket["variant"])}">'
                f'<div class="caveat__src">{_esc(caveat["source_name"])} '
                f'<code>{_esc(caveat["source_id"])}</code></div>'
                f'<div class="caveat__reason">{_esc(reason)}</div>'
                f"{messages}"
                "</li>"
            )
        blocks.append(
            '<div class="caveat-bucket">'
            f'<h3 class="subhead">{_esc(bucket["label"])} <code>{_esc(bucket_id)}</code></h3>'
            f'<p class="bucket__note">{_esc(bucket["note"])}</p>'
            f'<ul class="caveat-list caveat-list--{_attr(bucket_id)}">{"".join(rows)}</ul>'
            "</div>"
        )
    return "".join(blocks)


def _render_banner(decision: Decision) -> str:
    variant = "post" if decision.m3_ready else "monitor"
    facts = "".join(f"<li>{_esc(f)}</li>" for f in decision.facts)
    recorded = decision.recorded_human_decision
    recorded_html = ""
    if recorded:
        parts = []
        if recorded.get("decision"):
            parts.append(f'recorded decision <code>{_esc(recorded["decision"])}</code>')
        if recorded.get("post_today"):
            parts.append(f'post today: <strong>{_esc(recorded["post_today"])}</strong>')
        if parts:
            recorded_html = (
                f'<p class="banner__recorded">Human notes — {", ".join(parts)} '
                "(see human_decisions.md).</p>"
            )
    return (
        f'<div class="banner banner--{variant}" style="--i:0">'
        f'<div class="banner__status">{_esc(decision.label)}</div>'
        f'<p class="banner__headline">{_esc(decision.headline)}</p>'
        f'<ul class="banner__facts">{facts}</ul>'
        f"{recorded_html}"
        "</div>"
    )


def _render_links(art: dict[str, Any], index: int) -> str:
    present = art.get("_present", set())
    artifact_links = [
        f'<li><a href="{_attr(name)}">{_esc(label)}</a> <code>{_esc(name)}</code></li>'
        for name, label in LINK_ARTIFACTS
        if name in present
    ]
    note_links = [
        f'<li><a href="{_attr(name)}">{_esc(label)}</a> <code>{_esc(name)}</code></li>'
        for name, label in HUMAN_NOTE_FILES
        if name in present
    ]
    blocks = ""
    if artifact_links:
        blocks += f'<ul class="linklist">{"".join(artifact_links)}</ul>'
    if note_links:
        blocks += (
            '<h3 class="subhead">Human notes (hand-written, not part of this generated view)</h3>'
            f'<ul class="linklist">{"".join(note_links)}</ul>'
        )
    return _section("Source artifacts", blocks, index=index)


def render_daily_review_html(art: dict[str, Any]) -> str:
    """Render the per-run daily review HTML from a loaded artifact dict."""
    run_date = art.get("_run_date", "")
    row = summarize_run(art)
    decision = derive_decision(art)

    masthead = (
        '<header class="masthead">'
        '<p class="kicker">Colombia Forecasting Desk</p>'
        '<h1 class="title">Daily Review</h1>'
        '<div class="masthead__meta">'
        f"<span>{_esc(run_date)}</span>"
        f"<span>{_esc(row.sources_checked)} sources checked · {_esc(row.sources_failed)} failed</span>"
        f"<span>acceptance: {_esc(row.acceptance_status)}</span>"
        f"<span>generated {_esc(row.finished_at or 'n/a')}</span>"
        "</div>"
        "</header>"
    )

    stats = (
        '<div class="stats" style="--i:1">'
        + _stat(row.raw_items, "raw items")
        + _stat(row.cleaned_items, "cleaned")
        + _stat(row.clusters, "clusters")
        + _stat(row.candidates, "M1 candidates")
        + _stat(
            row.forecast_questions,
            "forecast questions",
            "post" if row.forecast_questions else "muted",
        )
        + _stat(row.analyst_insights, "analyst insights")
        + _stat(row.investigation_leads, "investigation leads")
        + _stat(row.tension_cards, "tension cards")
        + _stat(row.bundles, "bundles")
        + _stat(row.market_observed, "market rows")
        + "</div>"
    )

    why_facts = "".join(f"<li>{_esc(f)}</li>" for f in decision.facts)
    why_body = f'<ul class="why">{why_facts}</ul>'
    why_section = _section(
        "Why no M3 today" if not decision.m3_ready else "M3 readiness",
        why_body,
        index=2,
    )

    insights = _leads_of_type(art, "analyst_insight")
    shown_insights, extra_insights = _truncate(insights, INSIGHT_LIMIT)
    insight_cards = "".join(_render_lead_card(lead, "insight") for lead in shown_insights)
    insight_extra = (
        f'<p class="more">+{extra_insights} more in analyst_leads.md</p>'
        if extra_insights
        else ""
    )
    insight_body = (
        f'<div class="cards">{insight_cards}</div>{insight_extra}'
        if shown_insights
        else '<p class="empty">No analyst insights recorded today.</p>'
    )
    insight_section = _section(
        "Top analyst insights",
        insight_body,
        note="Source-backed findings. These are not forecasts and carry no probability.",
        index=3,
    )

    investigations = _leads_of_type(art, "investigation_lead")
    shown_inv, extra_inv = _truncate(investigations, INVESTIGATION_LIMIT)
    inv_cards = "".join(_render_lead_card(lead, "watch") for lead in shown_inv)
    inv_extra = (
        f'<p class="more">+{extra_inv} more in analyst_leads.md</p>' if extra_inv else ""
    )
    inv_body = (
        f'<div class="cards">{inv_cards}</div>{inv_extra}'
        if shown_inv
        else '<p class="empty">No investigation leads recorded today.</p>'
    )
    inv_section = _section(
        "Top investigation leads",
        inv_body,
        note="Plausible leads that need more research before they could become insights or forecasts.",
        index=4,
    )

    queue = derive_monitor_queue(art)
    if queue:
        is_human_queue = bool(art.get("_human_monitor_queue"))
        queue_items = "".join(
            '<li class="queue__item">'
            f'<div class="queue__label">{_esc(item["label"])}</div>'
            f'<div class="queue__meta">{_pill(item["kind"])}'
            f'<span class="queue__note">{_esc(item["note"])}</span></div>'
            "</li>"
            for item in queue
        )
        queue_section = _section(
            "Monitor queue (human)" if is_human_queue else "Monitor queue (derived)",
            f'<ol class="queue">{queue_items}</ol>',
            note=(
                "Parsed from human_decisions.md — recorded editorial priorities, not a promotion."
                if is_human_queue
                else "Derived from today's investigation leads and M2 review queue — what to sample next, not a promotion."
            ),
            index=5,
        )
    else:
        queue_section = ""

    body = (
        masthead
        + _render_banner(decision)
        + stats
        + why_section
        + insight_section
        + inv_section
        + queue_section
        + _render_source_caveats(art, index=6)
        + _render_tension_cards(art, index=7)
        + _render_market_pricing(art, index=8)
        + _render_bundles(art, index=9)
        + _render_links(art, index=10)
        + _footer()
    )
    return _page(f"Daily Review — {run_date}", body)


# --------------------------------------------------------------------------- #
# Global index view
# --------------------------------------------------------------------------- #
def _bar(days: int, total: int, label: str, sub: str = "") -> str:
    pct = int(round((days / total) * 100)) if total else 0
    sub_html = f'<span class="bar__sub">{_esc(sub)}</span>' if sub else ""
    return (
        '<li class="bar-row">'
        f'<div class="bar__label">{_esc(label)}{sub_html}</div>'
        '<div class="bar__track">'
        f'<div class="bar__fill" style="width:{pct}%"></div>'
        "</div>"
        f'<div class="bar__count">{days}/{total}</div>'
        "</li>"
    )


def _render_counts_table(rows: list[RunRow]) -> str:
    head = (
        "<thead><tr>"
        "<th>Date</th><th>Status</th><th class='num'>Raw</th><th class='num'>Clean</th>"
        "<th class='num'>Clusters</th><th class='num'>Leads</th><th class='num'>FQ</th>"
        "<th class='num'>Tension</th><th class='num'>Bundles</th>"
        "<th class='num'>Src fail</th><th>Accept.</th>"
        "</tr></thead>"
    )
    body_rows: list[str] = []
    for row in reversed(rows):  # newest first
        status_pill = (
            _pill("review", "post") if row.m3_ready else _pill("monitor", "watch")
        )
        fq_cls = "num num--accent" if row.forecast_questions else "num num--zero"
        fail_cls = "num num--alert" if row.sources_failed else "num"
        accept_pill = (
            _pill(row.acceptance_status, "ok")
            if row.acceptance_status == "pass"
            else _pill(row.acceptance_status, "alert")
        )
        body_rows.append(
            "<tr>"
            f'<td><a href="{_attr(row.date + "/review.html")}">{_esc(row.date)}</a></td>'
            f"<td>{status_pill}</td>"
            f'<td class="num">{row.raw_items}</td>'
            f'<td class="num">{row.cleaned_items}</td>'
            f'<td class="num">{row.clusters}</td>'
            f'<td class="num">{row.analyst_leads}</td>'
            f'<td class="{fq_cls}">{row.forecast_questions}</td>'
            f'<td class="num">{row.tension_cards}</td>'
            f'<td class="num">{row.bundles}</td>'
            f'<td class="{fail_cls}">{row.sources_failed}</td>'
            f"<td>{accept_pill}</td>"
            "</tr>"
        )
    return f'<table class="table">{head}<tbody>{"".join(body_rows)}</tbody></table>'


def _render_recurring(title: str, items: list[ThemeCount], total: int, index: int, note: str) -> str:
    if not items:
        return ""
    shown, _ = _truncate(items, RECURRING_LIMIT)
    bars = "".join(_bar(t.days, total, t.label, t.family) for t in shown)
    return _section(title, f'<ul class="bars">{bars}</ul>', note=note, index=index)


def render_runs_index_html(run_dirs: list[Path]) -> str:
    """Render the recent-runs index from a list of run directories."""
    per_run = [(p.name, load_run_artifacts(p)) for p in run_dirs]
    rows = [summarize_run(art) for _, art in per_run]
    rows.sort(key=lambda r: r.date)

    if not rows:
        body = (
            '<header class="masthead"><p class="kicker">Colombia Forecasting Desk</p>'
            '<h1 class="title">Recent Runs</h1></header>'
            '<p class="empty">No dated run folders found.</p>' + _footer()
        )
        return _page("Recent Runs", body)

    latest = rows[-1]
    drought = count_forecast_drought(rows)
    total = len(rows)
    span = f"{rows[0].date} → {rows[-1].date}"

    masthead = (
        '<header class="masthead">'
        '<p class="kicker">Colombia Forecasting Desk</p>'
        '<h1 class="title">Recent Runs</h1>'
        '<div class="masthead__meta">'
        f"<span>{total} runs · {_esc(span)}</span>"
        f'<span><a href="{_attr(latest.date + "/review.html")}">latest daily review →</a></span>'
        "</div>"
        "</header>"
    )

    drought_variant = "monitor" if drought else "post"
    drought_headline = (
        f"{drought} consecutive monitoring run(s) with no M3-ready forecast question."
        if drought
        else "The most recent run carries an M3-ready signal."
    )
    banner = (
        f'<div class="banner banner--{drought_variant}" style="--i:0">'
        '<div class="banner__status">Forecast-question drought</div>'
        f'<p class="banner__headline">{_esc(drought_headline)}</p>'
        '<ul class="banner__facts">'
        f"<li>Window: last {total} run(s), {_esc(span)}.</li>"
        f"<li>Latest run recorded {latest.analyst_insights} insight(s) and "
        f"{latest.investigation_leads} investigation lead(s).</li>"
        "<li>A drought is expected: the desk only posts when an artifact clears the M3 gate.</li>"
        "</ul>"
        "</div>"
    )

    counts_section = _section(
        "Counts over time",
        _render_counts_table(rows),
        note="Newest first. FQ = forecast-question leads. A column of zeros is the drought, by design.",
        index=1,
    )

    recurring_insights = _render_recurring(
        "Recurring analyst insights",
        aggregate_recurring_insights(per_run),
        total,
        index=2,
        note="How often each insight recurred across the window. Recurrence is a monitoring signal, not a forecast.",
    )
    recurring_tension = _render_recurring(
        "Repeated tension cards",
        aggregate_tension_cards(per_run),
        total,
        index=3,
        note="Advisory screens that keep firing. Persistent ≠ resolvable; still not a probability input.",
    )
    source_issues = _render_recurring(
        "Source reliability issues",
        aggregate_source_issues(per_run),
        total,
        index=4,
        note="Sources whose health repeatedly made silence unreliable across the window.",
    )

    queue = derive_monitor_queue(per_run[-1][1])
    if queue:
        is_human_queue = bool(per_run[-1][1].get("_human_monitor_queue"))
        queue_items = "".join(
            '<li class="queue__item">'
            f'<div class="queue__label">{_esc(item["label"])}</div>'
            f'<div class="queue__meta">{_pill(item["kind"])}'
            f'<span class="queue__note">{_esc(item["note"])}</span></div>'
            "</li>"
            for item in queue
        )
        queue_section = _section(
            "Active monitor queue (latest run)",
            f'<ol class="queue">{queue_items}</ol>',
            note=(
                "Parsed from the latest run's human_decisions.md."
                if is_human_queue
                else "Derived from the latest run's investigation leads and M2 review queue."
            ),
            index=5,
        )
    else:
        queue_section = ""

    run_links = "".join(
        '<li class="queue__item">'
        f'<div class="queue__label"><a href="{_attr(row.date + "/review.html")}">{_esc(row.date)}</a></div>'
        f'<div class="queue__meta">{(_pill("review", "post") if row.m3_ready else _pill("monitor", "watch"))}'
        f'<span class="queue__note">{row.analyst_leads} leads · {row.tension_cards} tension cards</span></div>'
        "</li>"
        for row in reversed(rows)
    )
    links_section = _section(
        "Per-run reviews",
        f'<ol class="queue">{run_links}</ol>',
        index=6,
    )

    body = (
        masthead
        + banner
        + counts_section
        + recurring_insights
        + recurring_tension
        + source_issues
        + queue_section
        + links_section
        + _footer()
    )
    return _page("Recent Runs — Colombia Forecasting Desk", body)


# --------------------------------------------------------------------------- #
# Page scaffold
# --------------------------------------------------------------------------- #
def _footer() -> str:
    return (
        '<footer class="foot">'
        f"Generated deterministically by <code>scripts/render_review.py</code> "
        f"({SCHEMA_VERSION}) from run artifacts. No LLM, no network. "
        "Tension cards, market-pricing rows, and bundles are advisory context, "
        "never probability inputs."
        "</footer>"
    )


def _page(title: str, body: str) -> str:
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n'
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{_esc(title)}</title>\n"
        f"<style>{CSS}</style>\n"
        "</head>\n"
        '<body>\n<div class="wrap">\n'
        f"{body}\n"
        "</div>\n</body>\n</html>\n"
    )


CSS = """
:root{
  --paper:#f2ede1; --card:#fbf8f0; --card-2:#f6f0e2;
  --ink:#211e18; --ink-soft:#5c5749; --ink-faint:#928c7c;
  --rule:#ddd5c2; --rule-strong:#cabfa6;
  --accent:#9c6611; --accent-soft:#f0e3c6;
  --link:#3a5a78; --alert:#963226; --alert-soft:#f1ddd5;
  --ok:#4e6b3d; --ok-soft:#e3ebd8; --watch:#9c6611; --watch-soft:#f3e8cf;
  --serif:"Charter","Iowan Old Style","Palatino Linotype",Palatino,Georgia,ui-serif,serif;
  --sans:ui-sans-serif,-apple-system,"Helvetica Neue","Segoe UI",system-ui,sans-serif;
  --mono:"SF Mono",ui-monospace,"JetBrains Mono",Menlo,Consolas,monospace;
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{
  margin:0; background:var(--paper); color:var(--ink);
  font-family:var(--serif); font-size:17px; line-height:1.6;
  background-image:radial-gradient(circle at 1px 1px, rgba(33,30,24,.035) 1px, transparent 0);
  background-size:22px 22px;
}
.wrap{max-width:1040px; margin:0 auto; padding:54px 28px 96px}
a{color:var(--link); text-decoration:none; border-bottom:1px solid rgba(58,90,120,.28)}
a:hover{border-bottom-color:var(--link)}
code{font-family:var(--mono); font-size:.82em; color:var(--ink-soft);
  background:rgba(33,30,24,.05); padding:.05em .35em; border-radius:3px}

/* Masthead */
.masthead{border-bottom:3px double var(--ink); padding-bottom:18px; margin-bottom:30px}
.kicker{font-family:var(--mono); text-transform:uppercase; letter-spacing:.22em;
  font-size:11.5px; color:var(--accent); margin:0 0 8px}
.title{font-family:var(--serif); font-weight:600; font-size:46px; line-height:1.02;
  letter-spacing:-.015em; margin:0}
.masthead__meta{display:flex; flex-wrap:wrap; gap:6px 20px; margin-top:14px;
  font-family:var(--mono); font-size:12px; color:var(--ink-faint)}
.masthead__meta a{color:var(--accent); border-bottom-color:rgba(156,102,17,.3)}

/* Banner */
.banner{background:var(--card); border:1px solid var(--rule); border-left:5px solid var(--accent);
  border-radius:4px; padding:22px 26px; margin-bottom:34px; box-shadow:0 1px 0 rgba(33,30,24,.04)}
.banner--monitor{border-left-color:var(--watch); background:linear-gradient(180deg,var(--watch-soft),var(--card) 70%)}
.banner--post{border-left-color:var(--ok); background:linear-gradient(180deg,var(--ok-soft),var(--card) 70%)}
.banner__status{font-family:var(--mono); text-transform:uppercase; letter-spacing:.16em;
  font-size:12px; font-weight:600; color:var(--ink)}
.banner--monitor .banner__status{color:var(--accent)}
.banner--post .banner__status{color:var(--ok)}
.banner__headline{font-size:21px; line-height:1.4; margin:8px 0 14px; max-width:60ch}
.banner__facts{margin:0; padding:0; list-style:none; display:grid; gap:6px}
.banner__facts li{font-family:var(--sans); font-size:14px; color:var(--ink-soft);
  padding-left:18px; position:relative}
.banner__facts li::before{content:"›"; position:absolute; left:2px; color:var(--accent)}
.banner__recorded{font-family:var(--sans); font-size:13.5px; color:var(--ink-soft);
  margin:14px 0 0; padding-top:12px; border-top:1px solid var(--rule)}

/* Stats */
.stats{display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:1px;
  background:var(--rule); border:1px solid var(--rule); border-radius:4px; overflow:hidden;
  margin-bottom:14px}
.stat{background:var(--card); padding:14px 12px; text-align:center}
.stat__value{font-family:var(--mono); font-size:26px; font-weight:600; line-height:1; letter-spacing:-.02em}
.stat__value--muted{color:var(--ink-faint)}
.stat__value--post{color:var(--ok)}
.stat__label{font-family:var(--sans); font-size:11px; text-transform:uppercase;
  letter-spacing:.08em; color:var(--ink-faint); margin-top:7px}

/* Sections */
.section{margin-top:42px}
.section__head{display:flex; align-items:baseline; gap:16px; flex-wrap:wrap;
  border-bottom:1px solid var(--rule); padding-bottom:10px; margin-bottom:20px}
.section__label{font-family:var(--mono); text-transform:uppercase; letter-spacing:.16em;
  font-size:13px; font-weight:600; color:var(--ink); margin:0; white-space:nowrap}
.section__note{font-style:italic; font-size:13.5px; color:var(--ink-faint); margin:0; flex:1}
.subhead{font-family:var(--mono); text-transform:uppercase; letter-spacing:.12em;
  font-size:11.5px; color:var(--ink-faint); margin:24px 0 10px}

/* Cards */
.cards{display:grid; gap:14px}
.cards--market{grid-template-columns:repeat(auto-fit,minmax(240px,1fr))}
.card{background:var(--card); border:1px solid var(--rule); border-left:3px solid var(--rule-strong);
  border-radius:4px; padding:16px 18px}
.card--market{border-left-color:var(--link)}
.card__title{font-family:var(--serif); font-weight:600; font-size:18px; line-height:1.3;
  margin:8px 0 6px; letter-spacing:-.01em}
.card__claim{font-size:15px; color:var(--ink-soft); margin:0 0 10px}
.card__why{font-size:14px; color:var(--ink-faint); font-style:italic; margin:0 0 10px}
.card__next{font-family:var(--sans); font-size:13.5px; color:var(--ink-soft); margin:10px 0 0;
  padding-top:10px; border-top:1px dotted var(--rule)}
.card__next-label{font-family:var(--mono); text-transform:uppercase; letter-spacing:.1em;
  font-size:10px; color:var(--accent); display:block; margin-bottom:3px}
.market__value{font-family:var(--mono); font-size:24px; font-weight:600; margin:4px 0 8px}
.market__unit{font-size:13px; color:var(--ink-faint); font-weight:400}

/* Definition list (evidence) */
.dl{margin:10px 0 0; display:grid; gap:6px}
.dl__row{display:grid; grid-template-columns:minmax(120px,40%) 1fr; gap:12px;
  font-size:13.5px; padding:5px 0; border-top:1px solid rgba(33,30,24,.06)}
.dl dt{font-family:var(--sans); font-weight:600; color:var(--ink)}
.dl dd{margin:0; color:var(--ink-soft)}
.dl__src{color:var(--ink-faint); font-style:italic}

/* Caveats */
.caveats{margin:10px 0 0; padding:0 0 0 16px; list-style:none}
.caveats li{font-family:var(--sans); font-size:12.5px; color:var(--ink-faint);
  position:relative; padding:2px 0}
.caveats li::before{content:"–"; position:absolute; left:-14px; color:var(--accent)}

/* Pills */
.tags{display:flex; gap:6px; flex-wrap:wrap; align-items:center}
.tags--wrap{margin-top:10px}
.pill{font-family:var(--mono); text-transform:uppercase; letter-spacing:.07em;
  font-size:10px; font-weight:600; padding:3px 8px; border-radius:999px;
  background:rgba(33,30,24,.06); color:var(--ink-soft); white-space:nowrap}
.pill--post{background:var(--ok-soft); color:var(--ok)}
.pill--watch{background:var(--watch-soft); color:var(--accent)}
.pill--alert{background:var(--alert-soft); color:var(--alert)}
.pill--ok{background:var(--ok-soft); color:var(--ok)}
.pill--insight{background:#e2e9ef; color:var(--link)}
.pill--muted{background:rgba(33,30,24,.05); color:var(--ink-faint)}

/* why / lists */
.why{margin:0; padding:0; list-style:none; display:grid; gap:8px}
.why li{font-family:var(--sans); font-size:15px; color:var(--ink-soft);
  padding-left:20px; position:relative}
.why li::before{content:"■"; position:absolute; left:0; font-size:9px; top:6px; color:var(--accent)}
.empty{font-style:italic; color:var(--ink-faint)}
.more{font-family:var(--sans); font-size:12.5px; color:var(--ink-faint); font-style:italic; margin:8px 0 0}

/* Source caveats */
.caveat-list{margin:0; padding:0; list-style:none; display:grid; gap:10px}
.caveat-bucket + .caveat-bucket{margin-top:18px}
.bucket__note{font-family:var(--sans); font-size:12.5px; color:var(--ink-faint);
  margin:-4px 0 10px}
.caveat{background:var(--card); border:1px solid var(--rule); border-radius:4px; padding:12px 14px}
.caveat--alert{border-left:3px solid var(--alert)}
.caveat--watch{border-left:3px solid var(--watch)}
.caveat--muted{background:var(--card-2); border-style:dashed}
.caveat__src{font-family:var(--sans); font-weight:600; font-size:14px}
.caveat__reason{font-family:var(--sans); font-size:13.5px; color:var(--alert); margin-top:3px}
.caveat--watch .caveat__reason{color:var(--watch)}
.caveat--muted .caveat__reason{color:var(--ink-faint)}
.caveat__msg{font-family:var(--mono); font-size:11.5px; color:var(--ink-faint); margin-top:5px;
  word-break:break-word}

/* Queue */
.queue{margin:0; padding:0; list-style:none; counter-reset:q; display:grid; gap:10px}
.queue__item{display:grid; gap:4px; padding:12px 14px 12px 44px; position:relative;
  background:var(--card); border:1px solid var(--rule); border-radius:4px}
.queue__item::before{counter-increment:q; content:counter(q); position:absolute; left:14px; top:12px;
  font-family:var(--mono); font-size:13px; font-weight:600; color:var(--accent)}
.queue__label{font-size:15px; line-height:1.4}
.queue__meta{display:flex; gap:10px; align-items:center; flex-wrap:wrap}
.queue__note{font-family:var(--mono); font-size:11.5px; color:var(--ink-faint)}

/* Bars */
.bars{margin:0; padding:0; list-style:none; display:grid; gap:9px}
.bar-row{display:grid; grid-template-columns:1fr 160px auto; align-items:center; gap:14px}
.bar__label{font-size:14.5px}
.bar__sub{font-family:var(--mono); font-size:11px; color:var(--ink-faint); margin-left:8px;
  text-transform:uppercase; letter-spacing:.06em}
.bar__track{height:9px; background:rgba(33,30,24,.07); border-radius:999px; overflow:hidden}
.bar__fill{height:100%; background:linear-gradient(90deg,var(--accent),#c08a2e); border-radius:999px}
.bar__count{font-family:var(--mono); font-size:12.5px; color:var(--ink-soft); text-align:right}

/* Table */
.table{width:100%; border-collapse:collapse; font-size:13.5px}
.table th,.table td{padding:9px 10px; text-align:left; border-bottom:1px solid var(--rule)}
.table th{font-family:var(--mono); text-transform:uppercase; letter-spacing:.06em;
  font-size:10.5px; color:var(--ink-faint); font-weight:600; border-bottom:2px solid var(--rule-strong)}
.table .num{text-align:right; font-family:var(--mono)}
.table td.num{color:var(--ink-soft)}
.table .num--accent{color:var(--ok); font-weight:600}
.table .num--zero{color:var(--ink-faint)}
.table .num--alert{color:var(--alert); font-weight:600}
.table tbody tr:hover{background:var(--card-2)}

/* Footer */
.foot{margin-top:56px; padding-top:18px; border-top:1px solid var(--rule);
  font-family:var(--sans); font-size:12px; color:var(--ink-faint); line-height:1.6}

/* Motion */
@media (prefers-reduced-motion: no-preference){
  .banner,.stats,.section{opacity:0; transform:translateY(10px);
    animation:rise .5s cubic-bezier(.2,.7,.3,1) forwards; animation-delay:calc(var(--i,0) * 45ms)}
  @keyframes rise{to{opacity:1; transform:none}}
}
@media (max-width:680px){
  .title{font-size:34px}
  .stats{grid-template-columns:repeat(2,1fr)}
  .dl__row{grid-template-columns:1fr}
  .bar-row{grid-template-columns:1fr 90px auto; gap:10px}
  .table{font-size:12px}
  .table th:nth-child(3),.table td:nth-child(3),
  .table th:nth-child(4),.table td:nth-child(4){display:none}
}
"""
