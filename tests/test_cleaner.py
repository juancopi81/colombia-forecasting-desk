from __future__ import annotations

from colombia_forecasting_desk.cleaner import (
    SUMMARY_MAX_CHARS,
    clean,
    normalize_whitespace,
    signal_type_for,
    strip_html,
    truncate_summary,
)
from colombia_forecasting_desk.models import Metasource


def _source(**over) -> Metasource:
    base = dict(
        id="x", name="X", url="https://x", type="news",
        country_relevance="high", access_status="rss_public", fetch_method="rss",
        priority="high", update_frequency="daily", trust_role="media_signal",
        parsing_difficulty="easy", enabled=True, notes="",
    )
    base.update(over)
    return Metasource(**base)


def test_strip_html_removes_tags() -> None:
    assert strip_html("<p>Hola <b>mundo</b></p>") == "Hola mundo"


def test_strip_html_handles_plain_text() -> None:
    assert strip_html("Just text") == "Just text"


def test_normalize_whitespace_collapses() -> None:
    assert normalize_whitespace("  a\n\tb  c\n") == "a b c"


def test_truncate_summary_short_pass_through() -> None:
    assert truncate_summary("hola") == "hola"


def test_truncate_summary_word_boundary() -> None:
    text = "palabra " * 60
    out = truncate_summary(text)
    assert len(out) <= SUMMARY_MAX_CHARS + 1
    assert out.endswith("…")
    assert " " not in out[-2:]


def test_signal_type_mapping_all_roles() -> None:
    assert signal_type_for(_source(trust_role="official_signal")) == "official_update"
    assert signal_type_for(_source(trust_role="media_signal")) == "media_narrative"
    assert signal_type_for(_source(trust_role="polling_signal")) == "poll"
    assert signal_type_for(_source(trust_role="agenda_signal")) == "calendar_event"
    assert signal_type_for(_source(trust_role="resolution_source")) == "official_update"
    assert signal_type_for(_source(trust_role="civic_signal")) == "civic_event"
    assert signal_type_for(_source(trust_role="weird_role")) == "unknown"


def test_clean_assigns_civic_event_for_secop_like_source(make_raw) -> None:
    """Regression test for the M1.5 review finding: SECOP datasets use
    trust_role=civic_signal, and items must end up classified as
    `civic_event` rather than the catch-all `unknown` so briefs surface
    them with a meaningful signal type.
    """
    secop_source = _source(
        id="secop_ii_contratos",
        type="dataset",
        trust_role="civic_signal",
        fetch_method="api",
        access_status="api_public",
    )
    cleaned = clean(make_raw(title="SECOP II Contrato — algo"), secop_source)
    assert cleaned.signal_type == "civic_event"


def test_clean_strips_html_and_sets_signal(make_raw, sample_source) -> None:
    raw = make_raw(raw_text="<p>Hola  <b>mundo</b>!</p>", title="  Título  ")
    cleaned = clean(raw, sample_source)
    assert cleaned.title == "Título"
    assert cleaned.clean_text == "Hola mundo !"
    assert cleaned.signal_type == "official_update"
    assert cleaned.country_relevance == "high"


def test_clean_flags_short_text(make_raw, sample_source) -> None:
    raw = make_raw(raw_text="x", title="title")
    cleaned = clean(raw, sample_source)
    assert "low_quality:short_text" in cleaned.quality_notes


def test_clean_flags_no_title(make_raw, sample_source) -> None:
    raw = make_raw(title="")
    cleaned = clean(raw, sample_source)
    assert "low_quality:no_title" in cleaned.quality_notes


def test_clean_falls_back_to_title_when_no_text(make_raw, sample_source) -> None:
    raw = make_raw(raw_text="", title="A useful long enough title for fallback path")
    cleaned = clean(raw, sample_source)
    assert cleaned.clean_text == raw.title.strip()
