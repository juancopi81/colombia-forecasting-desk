from __future__ import annotations

from colombia_forecasting_desk.dedupe import canonicalize_url, dedupe


def test_canonicalize_strips_fragment_and_trailing_slash() -> None:
    assert canonicalize_url("https://Example.com/foo/#frag") == "https://example.com/foo"


def test_canonicalize_strips_utm_and_fbclid() -> None:
    url = "https://example.com/a?utm_source=x&fbclid=abc&id=42"
    assert canonicalize_url(url) == "https://example.com/a?id=42"


def test_canonicalize_root_keeps_slash() -> None:
    assert canonicalize_url("https://example.com/") == "https://example.com/"


def test_dedupe_keeps_official_over_media(make_cleaned) -> None:
    media = make_cleaned(
        id="m1",
        source_id="eltiempo",
        url="https://example.com/article?utm_source=x",
        trust_role="media_signal",
    )
    official = make_cleaned(
        id="o1",
        source_id="banrep",
        url="https://example.com/article",
        trust_role="official_signal",
    )
    out = dedupe([media, official])
    assert len(out) == 1
    assert out[0].source_id == "banrep"


def test_dedupe_keeps_first_when_same_trust(make_cleaned) -> None:
    a = make_cleaned(id="a", source_id="x", url="https://e.com/a")
    b = make_cleaned(id="b", source_id="y", url="https://e.com/a/")
    out = dedupe([a, b])
    assert len(out) == 1
    assert out[0].id == "a"


def test_dedupe_drops_within_source_same_title(make_cleaned) -> None:
    a = make_cleaned(id="a", source_id="x", url="https://e.com/a", title="Hola Mundo")
    b = make_cleaned(id="b", source_id="x", url="https://e.com/b", title="hola mundo")
    out = dedupe([a, b])
    assert len(out) == 1


def test_dedupe_keeps_different_sources_same_title(make_cleaned) -> None:
    a = make_cleaned(id="a", source_id="x", url="https://e1.com/a", title="Hola")
    b = make_cleaned(id="b", source_id="y", url="https://e2.com/b", title="Hola")
    out = dedupe([a, b])
    assert len(out) == 2


def test_dedupe_preserves_semantic_document_fragments(make_cleaned) -> None:
    first = make_cleaned(
        id="decreto-500",
        source_id="diario_oficial",
        url="https://example.com/diario?edicion=53.491#act-decreto-500-de-2026",
        title="Diario Oficial 53.491 — Decreto 500 de 2026",
        metadata={"document_row_type": "diario_legal_act"},
    )
    second = make_cleaned(
        id="decreto-502",
        source_id="diario_oficial",
        url="https://example.com/diario?edicion=53.491#act-decreto-502-de-2026",
        title="Diario Oficial 53.491 — Decreto 502 de 2026",
        metadata={"document_row_type": "diario_legal_act"},
    )

    out = dedupe([first, second])

    assert [item.id for item in out] == ["decreto-500", "decreto-502"]


def test_dedupe_preserves_camara_agenda_project_fragments(make_cleaned) -> None:
    first = make_cleaned(
        id="camara-project-1",
        source_id="camara_agenda_consolidada",
        url="https://example.com/agenda.pdf#project-1",
        title="Cámara agenda — Proyecto de Ley 429 de 2025 Cámara",
        metadata={"document_row_type": "camara_agenda_item"},
    )
    second = make_cleaned(
        id="camara-project-2",
        source_id="camara_agenda_consolidada",
        url="https://example.com/agenda.pdf#project-2",
        title="Cámara agenda — Proyecto de Ley 430 de 2025 Cámara",
        metadata={"document_row_type": "camara_agenda_item"},
    )

    out = dedupe([first, second])

    assert [item.id for item in out] == ["camara-project-1", "camara-project-2"]
