from __future__ import annotations

import hashlib
from dataclasses import asdict
from typing import Any
from urllib.parse import urldefrag

from .cleaner import normalize_whitespace
from .models import (
    CleanedItem,
    IndicatorObservation,
    RawItem,
    RunSummary,
    SourceHealth,
)

SCHEMA_VERSION = "m2_review_packet.v1"
MAX_REVIEW_ITEMS = 24
MAX_EXCERPTS_PER_ITEM = 4
OFFICIAL_EXCERPT_CHARS = 2600
MEDIA_EXCERPT_CHARS = 1000
STRUCTURED_EXCERPT_CHARS = 1800
NEWS_SOURCE_TYPES = {"news"}


def build_m2_review_packet(
    run_summary: RunSummary,
    raw_items: list[RawItem],
    cleaned_items: list[CleanedItem],
    m1_candidates: dict[str, Any],
    m2_ranked_questions: dict[str, Any],
    legislative_reconciliations: list[dict[str, Any]],
    source_health: list[SourceHealth],
    indicator_watch: list[IndicatorObservation],
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build an evidence-rich packet for LLM/human M2 review.

    The packet packages source excerpts and structured evidence. It is not a
    scoring authority; it is meant to let the LLM challenge M1/M2 heuristics.
    """
    indexes = _EvidenceIndexes(raw_items, cleaned_items)
    legislative_by_id = {
        str(record.get("canonical_bill_id") or ""): record
        for record in legislative_reconciliations
        if isinstance(record, dict)
    }
    indicator_by_id = {
        indicator.indicator_id: indicator for indicator in indicator_watch
    }

    review_items: list[dict[str, Any]] = []
    seen: set[str] = set()

    for ranked in _ranked_review_items(m2_ranked_questions):
        item = _review_item_from_ranked(
            ranked,
            legislative_by_id,
            indexes,
        )
        _append_review_item(review_items, seen, item)

    for candidate in m1_candidates.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        item = _review_item_from_candidate(candidate, indicator_by_id, indexes)
        _append_review_item(review_items, seen, item)

    review_items = review_items[:MAX_REVIEW_ITEMS]
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": run_summary.run_date,
        "generated_at": generated_at or run_summary.finished_at,
        "policy": {
            "purpose": (
                "Give the LLM and human reviewer enough source content to "
                "challenge deterministic M1/M2 heuristics."
            ),
            "not_allowed": [
                "Do not reject solely because a deterministic score is low.",
                "Do not estimate probability unless evidence is sufficient.",
                "Do not treat summaries as a substitute for the source excerpts.",
            ],
            "review_instruction": (
                "Read the excerpts first, then decide whether the heuristic "
                "over-ranked, under-ranked, or missed the public-interest angle."
            ),
        },
        "inputs": {
            "m1_candidates_artifact": "m1_candidates.json",
            "m2_ranked_questions_artifact": "m2_ranked_questions.json",
            "legislative_reconciler_artifact": "legislative_reconciler.json",
            "raw_items_artifact": "raw_items.json",
            "cleaned_items_artifact": "cleaned_items.json",
            "indicator_watch_artifact": "indicator_watch.json",
            "source_health_artifact": "source_health.json",
        },
        "summary": {
            "review_item_count": len(review_items),
            "items_with_source_excerpts": sum(
                1 for item in review_items if item.get("source_excerpts")
            ),
            "heuristic_challenge_count": sum(
                1 for item in review_items if item.get("heuristic_risk_flags")
            ),
            "source_caveat_count": len(_source_caveats(source_health)),
        },
        "source_caveats": _source_caveats(source_health),
        "review_items": review_items,
    }


def render_m2_review_packet(packet: dict[str, Any]) -> str:
    """Render the M2 review packet as a paste-friendly Markdown artifact."""
    summary = packet.get("summary") if isinstance(packet.get("summary"), dict) else {}
    lines = [
        f"# M2 Review Packet - {packet.get('run_date', '')}",
        "",
        "## Review Policy",
        "",
        str((packet.get("policy") or {}).get("review_instruction") or ""),
        "",
        "- This packet is content-first: read the source excerpts before "
        "trusting scores.",
        "- Heuristic labels are advisory and should be challenged when the "
        "evidence suggests a better question.",
        "- Do not estimate probability unless the packet has enough evidence "
        "for resolution criteria.",
        "",
        "## Summary",
        "",
        f"- Review items: {summary.get('review_item_count', 0)}",
        f"- Items with source excerpts: {summary.get('items_with_source_excerpts', 0)}",
        f"- Heuristic challenge items: {summary.get('heuristic_challenge_count', 0)}",
        f"- Source caveats: {summary.get('source_caveat_count', 0)}",
        "",
    ]
    caveats = packet.get("source_caveats") or []
    if caveats:
        lines.extend(["## Source Caveats", ""])
        for caveat in caveats[:8]:
            if not isinstance(caveat, dict):
                continue
            lines.append(
                f"- `{caveat.get('source_id', '')}`: {caveat.get('reason', '')}"
            )
        lines.append("")

    lines.extend(["## Review Items", ""])
    for index, item in enumerate(packet.get("review_items") or [], 1):
        if not isinstance(item, dict):
            continue
        lines.extend(_render_review_item(index, item))
    return "\n".join(lines).rstrip() + "\n"


class _EvidenceIndexes:
    def __init__(
        self,
        raw_items: list[RawItem],
        cleaned_items: list[CleanedItem],
    ) -> None:
        self.raw_by_id = {item.id: item for item in raw_items}
        self.cleaned_by_id = {item.id: item for item in cleaned_items}
        self.ids_by_url: dict[str, list[str]] = {}
        for item in raw_items:
            for key in _url_keys(item.url):
                self.ids_by_url.setdefault(key, []).append(item.id)
        for item in cleaned_items:
            for key in _url_keys(item.url):
                self.ids_by_url.setdefault(key, []).append(item.id)

    def by_item_ids(
        self,
        item_ids: list[str],
    ) -> list[tuple[RawItem | None, CleanedItem | None]]:
        pairs: list[tuple[RawItem | None, CleanedItem | None]] = []
        seen: set[str] = set()
        for item_id in item_ids:
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            pairs.append(
                (self.raw_by_id.get(item_id), self.cleaned_by_id.get(item_id))
            )
        return pairs

    def by_urls(
        self,
        urls: list[str],
    ) -> list[tuple[RawItem | None, CleanedItem | None]]:
        item_ids: list[str] = []
        for url in urls:
            for key in _url_keys(url):
                item_ids.extend(self.ids_by_url.get(key, []))
        return self.by_item_ids(item_ids)


def _ranked_review_items(
    m2_ranked_questions: dict[str, Any],
) -> list[dict[str, Any]]:
    ranked = [
        item
        for item in m2_ranked_questions.get("ranked_questions") or []
        if isinstance(item, dict)
    ]
    audit_refs = {
        str(item.get("rank_id") or "")
        for key in ("possible_false_negatives", "possible_false_positives")
        for item in (m2_ranked_questions.get("heuristic_audit") or {}).get(key, [])
        if isinstance(item, dict)
    }
    selected: list[dict[str, Any]] = []
    for item in ranked:
        if item.get("bucket") == "ready_for_m3":
            selected.append(item)
    for item in ranked:
        if str(item.get("rank_id") or "") in audit_refs:
            selected.append(item)
    for item in ranked:
        if item.get("bucket") in {"public_interest_but_unready", "watchlist"}:
            selected.append(item)
    return _unique_ranked(selected)


def _review_item_from_ranked(
    ranked: dict[str, Any],
    legislative_by_id: dict[str, dict[str, Any]],
    indexes: _EvidenceIndexes,
) -> dict[str, Any]:
    canonical_id = str(ranked.get("canonical_bill_id") or "")
    record = legislative_by_id.get(canonical_id, {})
    source_evidence = [
        item for item in record.get("source_evidence") or [] if isinstance(item, dict)
    ]
    urls = [str(item.get("url") or "") for item in source_evidence]
    excerpts = _source_excerpts(indexes.by_urls(urls), preferred_urls=urls)
    if not excerpts and source_evidence:
        excerpts = [
            _summary_excerpt(item)
            for item in source_evidence[:MAX_EXCERPTS_PER_ITEM]
        ]
    return {
        "packet_item_id": _packet_item_id(
            "ranked",
            ranked.get("rank_id") or canonical_id,
        ),
        "item_type": "legislative_ranked_record",
        "origin_id": canonical_id,
        "question_seed": ranked.get("question_seed") or "",
        "recommendation": ranked.get("recommendation") or "",
        "bucket": ranked.get("bucket") or "",
        "heuristic_score": ranked.get("overall_score"),
        "heuristic_reasons": list(ranked.get("score_reasons") or []),
        "heuristic_penalties": list(ranked.get("penalties") or []),
        "heuristic_risk_flags": list(ranked.get("heuristic_risk_flags") or []),
        "llm_review_hint": ranked.get("llm_review_hint") or "",
        "missing_evidence": list(ranked.get("missing_evidence") or []),
        "source_ids": list(ranked.get("source_ids") or []),
        "source_urls": _unique_preserve_order(urls),
        "source_excerpts": excerpts,
        "structured_context": {
            "ranked_record": _compact_dict(ranked),
            "legislative_reconciler_record": _compact_dict(record),
        },
    }


def _review_item_from_candidate(
    candidate: dict[str, Any],
    indicator_by_id: dict[str, IndicatorObservation],
    indexes: _EvidenceIndexes,
) -> dict[str, Any]:
    evidence = (
        candidate.get("evidence")
        if isinstance(candidate.get("evidence"), dict)
        else {}
    )
    item_ids = [str(item_id) for item_id in evidence.get("item_ids") or []]
    links = [link for link in evidence.get("links") or [] if isinstance(link, dict)]
    urls = [str(link.get("url") or "") for link in links]
    excerpts = _source_excerpts(
        indexes.by_item_ids(item_ids) + indexes.by_urls(urls),
        preferred_urls=urls,
    )
    structured_context: dict[str, Any] = {"candidate": _compact_dict(candidate)}
    indicator = indicator_by_id.get(str(candidate.get("origin_id") or ""))
    if indicator is not None:
        structured_context["indicator_observation"] = asdict(indicator)
        excerpts.append(_indicator_excerpt(indicator))
    return {
        "packet_item_id": _packet_item_id(
            "candidate",
            candidate.get("candidate_id") or candidate.get("origin_id") or "",
        ),
        "item_type": str(candidate.get("candidate_type") or "candidate"),
        "origin_id": str(candidate.get("origin_id") or ""),
        "question_seed": str(candidate.get("question_seed") or ""),
        "recommendation": str(candidate.get("decision_hint") or "candidate"),
        "bucket": "m1_candidate",
        "heuristic_score": (candidate.get("m1_scores") or {}).get(
            "forecastability_score"
        ),
        "heuristic_reasons": list(candidate.get("reasons") or []),
        "heuristic_penalties": list(candidate.get("noise_reasons") or []),
        "heuristic_risk_flags": [],
        "llm_review_hint": (
            "Read the source excerpts and decide whether this M1 candidate is "
            "actually public-interest and forecastable."
        ),
        "missing_evidence": list(candidate.get("missing_evidence") or []),
        "source_ids": list(candidate.get("source_ids") or []),
        "source_urls": _unique_preserve_order(urls),
        "source_excerpts": excerpts[:MAX_EXCERPTS_PER_ITEM],
        "structured_context": structured_context,
    }


def _append_review_item(
    output: list[dict[str, Any]],
    seen: set[str],
    item: dict[str, Any],
) -> None:
    if len(output) >= MAX_REVIEW_ITEMS:
        return
    keys = _review_item_keys(item)
    if not keys or any(key in seen for key in keys):
        return
    seen.update(keys)
    output.append(item)


def _review_item_keys(item: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    packet_id = str(item.get("packet_item_id") or "")
    origin_id = str(item.get("origin_id") or "")
    question_seed = normalize_whitespace(str(item.get("question_seed") or "")).lower()
    if packet_id:
        keys.add(f"packet:{packet_id}")
    if origin_id:
        keys.add(f"origin:{origin_id}")
    if question_seed:
        keys.add(f"question:{question_seed}")
    return keys


def _source_excerpts(
    pairs: list[tuple[RawItem | None, CleanedItem | None]],
    preferred_urls: list[str],
) -> list[dict[str, Any]]:
    excerpts: list[dict[str, Any]] = []
    seen: set[str] = set()
    preferred = {_defrag(url) for url in preferred_urls if url}
    sorted_pairs = sorted(
        pairs,
        key=lambda pair: (
            0 if _pair_url(pair) in preferred else 1,
            _pair_source_id(pair),
            _pair_title(pair),
        ),
    )
    for raw, cleaned in sorted_pairs:
        item_id = (
            raw.id
            if raw is not None
            else cleaned.id
            if cleaned is not None
            else ""
        )
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        excerpt = _item_excerpt(raw, cleaned)
        if excerpt:
            excerpts.append(excerpt)
        if len(excerpts) >= MAX_EXCERPTS_PER_ITEM:
            break
    return excerpts


def _item_excerpt(raw: RawItem | None, cleaned: CleanedItem | None) -> dict[str, Any]:
    item = raw or cleaned
    if item is None:
        return {}
    metadata = dict((raw.metadata if raw is not None else cleaned.metadata) or {})
    clean_text = cleaned.clean_text if cleaned is not None else ""
    raw_text = raw.raw_text if raw is not None else ""
    source_type = item.source_type
    text = clean_text or raw_text or item.title
    max_chars = _excerpt_limit(source_type, metadata)
    excerpt, truncated = _clip_text(text, max_chars)
    return {
        "item_id": item.id,
        "source_id": item.source_id,
        "source_name": item.source_name,
        "source_type": source_type,
        "title": item.title,
        "url": item.url,
        "published_at": item.published_at,
        "content_kind": _content_kind(item.url, metadata),
        "metadata_hints": _metadata_hints(metadata),
        "char_count": len(text),
        "excerpt_char_count": len(excerpt),
        "truncated": truncated,
        "excerpt": excerpt,
    }


def _summary_excerpt(evidence: dict[str, Any]) -> dict[str, Any]:
    summary = normalize_whitespace(str(evidence.get("summary") or ""))
    excerpt, truncated = _clip_text(summary, STRUCTURED_EXCERPT_CHARS)
    return {
        "item_id": "",
        "source_id": str(evidence.get("source_id") or ""),
        "source_name": str(evidence.get("source_id") or ""),
        "source_type": "structured_summary",
        "title": str(evidence.get("role") or "source evidence"),
        "url": str(evidence.get("url") or ""),
        "published_at": str(evidence.get("date") or ""),
        "content_kind": "structured_summary",
        "metadata_hints": {"role": str(evidence.get("role") or "")},
        "char_count": len(summary),
        "excerpt_char_count": len(excerpt),
        "truncated": truncated,
        "excerpt": excerpt,
    }


def _indicator_excerpt(indicator: IndicatorObservation) -> dict[str, Any]:
    text = "\n".join(
        [
            indicator.headline,
            f"Period: {indicator.period}",
            f"Values: {indicator.values}",
            f"Why it matters: {indicator.why_it_matters}",
            f"Next step: {indicator.next_step}",
        ]
    )
    excerpt, truncated = _clip_text(text, STRUCTURED_EXCERPT_CHARS)
    return {
        "item_id": indicator.indicator_id,
        "source_id": indicator.indicator_id,
        "source_name": indicator.source_name,
        "source_type": "structured_indicator",
        "title": indicator.name,
        "url": indicator.source_url,
        "published_at": indicator.release_date,
        "content_kind": "structured_indicator",
        "metadata_hints": {"period": indicator.period, "status": indicator.status},
        "char_count": len(text),
        "excerpt_char_count": len(excerpt),
        "truncated": truncated,
        "excerpt": excerpt,
    }


def _excerpt_limit(source_type: str, metadata: dict[str, Any]) -> int:
    if source_type in NEWS_SOURCE_TYPES:
        return MEDIA_EXCERPT_CHARS
    if metadata.get("content_extraction") or metadata.get("parsed_content"):
        return OFFICIAL_EXCERPT_CHARS
    return STRUCTURED_EXCERPT_CHARS


def _content_kind(url: str, metadata: dict[str, Any]) -> str:
    if metadata.get("content_extraction") or metadata.get("parsed_content"):
        return "parsed_content"
    if metadata.get("extraction"):
        return str(metadata.get("extraction"))
    lower = url.lower()
    if ".pdf" in lower:
        return "pdf_link"
    if any(ext in lower for ext in (".xlsx", ".xls", ".csv")):
        return "spreadsheet_link"
    return "html_or_api"


def _metadata_hints(metadata: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "content_extraction",
        "parsed_content",
        "extraction",
        "document_title",
        "agenda_action_type",
        "project_label",
        "status",
        "edition_number",
    ]
    return {key: metadata[key] for key in keys if key in metadata and metadata[key]}


def _source_caveats(source_health: list[SourceHealth]) -> list[dict[str, str]]:
    caveats: list[dict[str, str]] = []
    for health in source_health:
        if health.failure_count:
            caveats.append(
                {
                    "source_id": health.source_id,
                    "reason": "source failed; silence is not evidence of no activity",
                    "content_mode": health.content_mode,
                }
            )
        elif health.document_link_count and health.parsed_content_count == 0:
            caveats.append(
                {
                    "source_id": health.source_id,
                    "reason": "document links were found but content was not parsed",
                    "content_mode": health.content_mode,
                }
            )
    return caveats


def _render_review_item(index: int, item: dict[str, Any]) -> list[str]:
    lines = [
        f"### {index}. {item.get('question_seed', item.get('origin_id', ''))}",
        "",
        f"- Type: `{item.get('item_type', '')}`",
        f"- Bucket: `{item.get('bucket', '')}`",
        f"- Recommendation: `{item.get('recommendation', '')}`",
        f"- Heuristic score: `{item.get('heuristic_score', 'n/a')}`",
        f"- Risk flags: "
        f"{', '.join(item.get('heuristic_risk_flags') or []) or 'none'}",
        f"- LLM review hint: {item.get('llm_review_hint', '')}",
        "",
    ]
    if item.get("heuristic_reasons"):
        lines.append("Reasons surfaced:")
        lines.extend(f"- {reason}" for reason in item["heuristic_reasons"])
        lines.append("")
    if item.get("heuristic_penalties"):
        lines.append("Reasons to be skeptical:")
        lines.extend(f"- {reason}" for reason in item["heuristic_penalties"])
        lines.append("")
    if item.get("missing_evidence"):
        lines.append("Missing evidence:")
        lines.extend(f"- {reason}" for reason in item["missing_evidence"])
        lines.append("")
    excerpts = [ex for ex in item.get("source_excerpts") or [] if isinstance(ex, dict)]
    if excerpts:
        lines.append("Source excerpts:")
        lines.append("")
        for excerpt in excerpts:
            lines.extend(_render_excerpt(excerpt))
    else:
        lines.extend(
            [
                "Source excerpts:",
                "",
                "- _No parsed source excerpt matched this item._",
                "",
            ]
        )
    return lines


def _render_excerpt(excerpt: dict[str, Any]) -> list[str]:
    title = str(excerpt.get("title") or excerpt.get("source_id") or "source")
    url = str(excerpt.get("url") or "")
    suffix = " (truncated)" if excerpt.get("truncated") else ""
    lines = [
        f"#### {title}",
        "",
        f"- Source: `{excerpt.get('source_id', '')}`",
        f"- Published: `{excerpt.get('published_at', '')}`",
        f"- Content kind: `{excerpt.get('content_kind', '')}`{suffix}",
    ]
    if url:
        lines.append(f"- URL: {url}")
    lines.extend(["", "```text", str(excerpt.get("excerpt") or ""), "```", ""])
    return lines


def _clip_text(text: str, max_chars: int) -> tuple[str, bool]:
    clean = normalize_whitespace(text)
    if len(clean) <= max_chars:
        return clean, False
    return clean[: max_chars - 20].rstrip() + " ... [truncated]", True


def _url_keys(url: str) -> list[str]:
    clean = str(url or "").strip()
    if not clean:
        return []
    no_fragment = _defrag(clean)
    return _unique_preserve_order([clean, no_fragment])


def _defrag(url: str) -> str:
    return urldefrag(str(url or "").strip()).url


def _pair_url(pair: tuple[RawItem | None, CleanedItem | None]) -> str:
    raw, cleaned = pair
    item = raw or cleaned
    return _defrag(item.url if item is not None else "")


def _pair_source_id(pair: tuple[RawItem | None, CleanedItem | None]) -> str:
    raw, cleaned = pair
    item = raw or cleaned
    return item.source_id if item is not None else ""


def _pair_title(pair: tuple[RawItem | None, CleanedItem | None]) -> str:
    raw, cleaned = pair
    item = raw or cleaned
    return item.title if item is not None else ""


def _packet_item_id(kind: str, seed: object) -> str:
    digest = hashlib.sha1(f"{kind}\x1f{seed}".encode("utf-8")).hexdigest()[:12]
    return f"m2pkt_{digest}"


def _unique_ranked(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = str(item.get("rank_id") or item.get("canonical_bill_id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _unique_preserve_order(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, item in value.items():
        if key == "source_excerpts":
            continue
        if item is None or item == "" or item == [] or item == {}:
            continue
        output[key] = item
    return output
