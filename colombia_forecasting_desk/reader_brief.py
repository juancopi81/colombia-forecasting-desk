"""Validated, deterministic reader-facing daily briefs. No network or LLM work."""
from __future__ import annotations

import html
import re
from typing import Any, Iterable
from urllib.parse import quote

from .plain_language import validate_plain_text, word_count
from .schemas.reader_brief import validate as validate_schema


def _text(value: str) -> str:
    return " ".join(value.split())


def _md(value: str) -> str:
    return re.sub(r"([\\`*_{}\[\]()#+!<>])", r"\\\1", _text(value))


def _percent(value: float) -> str:
    return f"{value * 100:g}%"


def _quiet(brief: dict[str, Any]) -> bool:
    return not brief["needs_you"]["required"] and not any(
        brief[key] for key in ("changes", "coming_up", "open_forecasts", "data_problems")
    )


def _markdown(brief: dict[str, Any]) -> str:
    if _quiet(brief):
        return "Nothing needs you today.\n"
    needs = brief["needs_you"]
    if needs["required"]:
        due = f" By {needs['by']}." if needs.get("by") else ""
        lines = [f"**Needs you:** {_md(needs['action'])}{due}"]
    else:
        lines = ["**Nothing needs you today.**"]
    lines.extend(["", _md(brief["headline"])])
    for item in brief["changes"]:
        url = quote(item["source_url"], safe=":/?#[]@!$&'*+,;=%")
        confidence = "" if item["confidence"] == "confirmed" else f" ({item['confidence']})"
        lines.extend(["", f"- **{_md(item['title'])}{confidence}:** {_md(item['what_happened'])} "
                      f"{_md(item['why_it_matters'])} Affected: {_md(item['who_is_affected'])} "
                      f"[Source]({url})."])
    if brief["coming_up"]:
        lines.extend(["", "**Coming up**"])
        for item in brief["coming_up"]:
            call = item.get("our_call")
            view = (f"Our call: {_percent(call['probability'])} — {_md(call['outcome'])}."
                    if call else _md(item["no_call_reason"]))
            lines.append(f"- {item['date']} — {_md(item['event'])}. {view}")
    if brief["open_forecasts"]:
        lines.extend(["", "**Open forecasts**"])
        for item in brief["open_forecasts"]:
            lines.append(f"- {_md(item['plain_question'])} {_percent(item['probability'])}; "
                         f"official record due {item['resolves_by']}.")
    if brief["data_problems"]:
        lines.extend(["", "**Data problems**"])
        lines.extend(f"- {_md(problem)}" for problem in brief["data_problems"])
    return "\n".join(lines) + "\n"


def validate_reader_brief(
    brief: Any,
    *,
    run_date: str | None = None,
    forecast_rows: Iterable[dict[str, Any]] | None = None,
    source_urls: set[str] | None = None,
) -> list[str]:
    """Validate the shared shape, complete rendered text and optional provenance.

    Providing ledger rows checks IDs and probabilities. Providing source URLs
    checks each change against evidence already captured in the run.
    """
    issues = validate_schema(brief)
    if issues:
        return issues
    if run_date is not None and brief["run_date"] != run_date:
        issues.append("reader brief run_date does not match the run folder")
    fields = [brief["headline"], *brief["data_problems"]]
    if brief["needs_you"]["required"]:
        fields.append(brief["needs_you"]["action"])
    for item in brief["changes"]:
        fields.extend(item[key] for key in ("title", "what_happened", "why_it_matters", "who_is_affected"))
        if any(char.isspace() for char in item["source_url"]) or any(char in item["source_url"] for char in '<>"'):
            issues.append("changes.source_url contains unsafe URL characters")
        if source_urls is not None and item["source_url"] not in source_urls:
            issues.append(f"change source is not captured in the run: {item['source_url']}")
    for item in brief["coming_up"]:
        fields.append(item["event"])
        if item.get("our_call"):
            fields.append(item["our_call"]["outcome"])
        else:
            reason = item["no_call_reason"]
            fields.append(reason)
            if len(re.findall(r"[.!?](?:\s|$)", reason)) > 1 or "\n" in reason:
                issues.append("no_call_reason must be one sentence")
    fields.extend(item["plain_question"] for item in brief["open_forecasts"])
    if any(re.search(r"<[^>]+>", field) for field in fields):
        issues.append("reader fields must be plain text, without HTML tags")
    for field in fields:
        issues.extend(validate_plain_text(field))
    rendered = _markdown(brief)
    count = word_count(rendered)
    if count > 250:
        issues.append(f"rendered reader brief has {count} words; maximum is 250")
    if forecast_rows is not None:
        rows = {row.get("forecast_id"): row for row in forecast_rows}
        calls = [item["our_call"] for item in brief["coming_up"] if item.get("our_call")]
        calls.extend(brief["open_forecasts"])
        for call in calls:
            row = rows.get(call["forecast_id"])
            if row is None:
                issues.append(f"unknown forecast_id: {call['forecast_id']}")
                continue
            if row.get("probability") != call["probability"]:
                issues.append(f"probability differs from recorded forecast: {call['forecast_id']}")
            if row.get("status") == "void":
                issues.append(f"void forecast cannot be presented as a call: {call['forecast_id']}")
            if str(row.get("run_date", "")) > brief["run_date"]:
                issues.append(f"forecast postdates the brief: {call['forecast_id']}")
        for call in brief["open_forecasts"]:
            row = rows.get(call["forecast_id"])
            if row is not None:
                if row.get("status") != "open":
                    issues.append(f"open_forecasts contains a closed forecast: {call['forecast_id']}")
                due = row.get("resolution_check_window_end") or row.get("resolution_deadline")
                if due != call["resolves_by"]:
                    issues.append(f"record-check date differs from ledger: {call['forecast_id']}")
    return list(dict.fromkeys(issues))


def render_reader_brief(brief: dict[str, Any]) -> str:
    issues = validate_reader_brief(brief)
    if issues:
        raise ValueError("; ".join(issues))
    return _markdown(brief)


def render_reader_brief_html(brief: dict[str, Any]) -> str:
    """Render the same content as Markdown without interpreting authored markup."""
    issues = validate_reader_brief(brief)
    if issues:
        raise ValueError("; ".join(issues))
    esc = lambda value: html.escape(_text(str(value)), quote=True)
    start = '<section class="reader-brief" aria-label="Daily brief">'
    if _quiet(brief):
        return start + '<p class="reader-brief__attention">Nothing needs you today.</p></section>'
    needs = brief["needs_you"]
    attention = (f"Needs you: {esc(needs['action'])}" if needs["required"] else "Nothing needs you today.")
    if needs["required"] and needs.get("by"):
        attention += f" By {esc(needs['by'])}."
    parts = [start, f'<p class="reader-brief__attention">{attention}</p>',
             f'<p class="kicker">Colombia Forecasting Desk · {esc(brief["run_date"])}</p>',
             f'<h1>{esc(brief["headline"])}</h1>']
    if brief["changes"]:
        parts.append('<ul class="reader-brief__changes">')
        for item in brief["changes"]:
            confidence = "" if item["confidence"] == "confirmed" else f" ({esc(item['confidence'])})"
            parts.append(f'<li><strong>{esc(item["title"])}{confidence}:</strong> '
                         f'{esc(item["what_happened"])} {esc(item["why_it_matters"])} '
                         f'Affected: {esc(item["who_is_affected"])} '
                         f'<a href="{esc(item["source_url"])}">Source</a>.</li>')
        parts.append("</ul>")
    if brief["coming_up"]:
        parts.append("<h2>Coming up</h2><ul>")
        for item in brief["coming_up"]:
            call = item.get("our_call")
            view = (f"Our call: {_percent(call['probability'])} — {esc(call['outcome'])}."
                    if call else esc(item["no_call_reason"]))
            parts.append(f'<li>{esc(item["date"])} — {esc(item["event"])}. {view}</li>')
        parts.append("</ul>")
    if brief["open_forecasts"]:
        parts.append("<h2>Open forecasts</h2><ul>")
        for item in brief["open_forecasts"]:
            parts.append(f'<li>{esc(item["plain_question"])} {_percent(item["probability"])}; '
                         f'official record due {esc(item["resolves_by"])}.</li>')
        parts.append("</ul>")
    if brief["data_problems"]:
        parts.append("<h2>Data problems</h2><ul>")
        parts.extend(f"<li>{esc(problem)}</li>" for problem in brief["data_problems"])
        parts.append("</ul>")
    return "".join(parts) + "</section>"
