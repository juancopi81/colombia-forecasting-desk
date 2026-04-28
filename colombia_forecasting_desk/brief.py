from __future__ import annotations

from .models import CleanedItem, Cluster, RunSummary, SourceFailure

TOP_SIGNALS_LIMIT = 10
LOW_QUALITY_LIMIT = 10
ERROR_MSG_TRUNCATE = 200


def _bullet_or_none(items: list[str], empty_text: str = "_None._") -> str:
    if not items:
        return empty_text
    return "\n".join(f"- {x}" for x in items)


def _max_priority(priorities: list[str]) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    if not priorities:
        return "low"
    return max(priorities, key=lambda p: order.get(p, 0))


def _render_cluster(idx: int, cluster: Cluster) -> str:
    priority = _max_priority(cluster.priorities)
    source_types = ", ".join(cluster.source_types) or "n/a"
    latest = cluster.latest_published_at or "n/a"
    questions = (
        "\n".join(f"- {q}" for q in cluster.possible_questions)
        if cluster.possible_questions
        else "- _(populated in M2)_"
    )
    missing = _bullet_or_none(cluster.missing_evidence, "- _(populated in M2)_")
    next_sources = _bullet_or_none(
        cluster.recommended_next_sources, "- _(populated in M2)_"
    )
    seen: set[str] = set()
    link_lines: list[str] = []
    for url, title, src in zip(
        cluster.member_urls, cluster.member_titles, cluster.member_source_names
    ):
        if url in seen:
            continue
        seen.add(url)
        title_safe = title.strip() or "(no title)"
        link_lines.append(f"- [{title_safe}]({url}) — {src}")
    links_block = "\n".join(link_lines) if link_lines else "- _None._"

    why = cluster.why_it_matters or "_TBD by analyst (populated in M2)._"

    return (
        f"### {idx}. {cluster.title}\n\n"
        f"- Priority: {priority}\n"
        f"- Confidence: {cluster.confidence}\n"
        f"- Source count: {cluster.source_count}\n"
        f"- Source types: {source_types}\n"
        f"- Signal types: {', '.join(cluster.signal_types) or 'unknown'}\n"
        f"- Latest update: {latest}\n"
        f"- Score: {cluster.score}\n\n"
        f"**Summary:** {cluster.summary or '_(no summary)_'}\n\n"
        f"**Why it may matter:** {why}\n\n"
        f"**Possible forecastable questions:**\n{questions}\n\n"
        f"**Missing evidence:**\n{missing}\n\n"
        f"**Recommended next sources:**\n{next_sources}\n\n"
        f"**Links:**\n{links_block}\n"
    )


def _render_failures(failures: list[SourceFailure]) -> str:
    if not failures:
        return "_No source failures during this run._"
    lines = []
    for f in failures:
        msg = f.error_message[:ERROR_MSG_TRUNCATE]
        lines.append(f"- `{f.source_id}`: {f.error_class}: {msg}")
    return "\n".join(lines)


def _render_low_quality(items: list[CleanedItem]) -> str:
    flagged = [it for it in items if it.quality_notes]
    if not flagged:
        return "_None flagged._"
    lines = []
    for it in flagged[:LOW_QUALITY_LIMIT]:
        lines.append(
            f"- `{it.source_id}`: {it.title or '(no title)'} — {it.quality_notes}"
        )
    return "\n".join(lines)


def render_brief(
    run_summary: RunSummary,
    ranked_clusters: list[Cluster],
    failures: list[SourceFailure],
    cleaned_items: list[CleanedItem],
    topic_keywords: list[str],
) -> str:
    top = ranked_clusters[:TOP_SIGNALS_LIMIT]
    top_blocks = [
        _render_cluster(i + 1, c) for i, c in enumerate(top)
    ]
    top_section = "\n---\n\n".join(top_blocks) if top_blocks else "_No clusters._"

    keywords_section = (
        "\n".join(f"- {w}" for w in topic_keywords) if topic_keywords else "_None._"
    )

    return (
        f"# Metasource Brief — {run_summary.run_date}\n\n"
        "## Run Summary\n\n"
        f"- Run date: {run_summary.run_date}\n"
        f"- Started at: {run_summary.started_at}\n"
        f"- Finished at: {run_summary.finished_at}\n"
        f"- Sources checked: {run_summary.sources_checked}\n"
        f"- Sources failed: {run_summary.sources_failed}\n"
        f"- Raw items collected: {run_summary.raw_items}\n"
        f"- Cleaned items retained: {run_summary.cleaned_items}\n"
        f"- Clusters created: {run_summary.clusters}\n\n"
        "## Top Signals\n\n"
        f"{top_section}\n\n"
        "## Emerging Questions\n\n"
        "- _(populated in M2)_\n\n"
        "## Topics to Monitor\n\n"
        f"{keywords_section}\n\n"
        "## Noisy / Low-Confidence Items\n\n"
        f"{_render_low_quality(cleaned_items)}\n\n"
        "## Source Failures\n\n"
        f"{_render_failures(failures)}\n\n"
        "## Suggested Next Step\n\n"
        "- Review top 3 clusters; propose forecastable questions in M2.\n"
    )
