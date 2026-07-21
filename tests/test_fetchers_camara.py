from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import httpx

from colombia_forecasting_desk.cleaner import clean
from colombia_forecasting_desk.fetchers import (
    _enrich_camara_agenda_pdfs,
    _extract_camara_agenda_entries_from_text,
    _extract_camara_agenda_pdf_links,
    fetch_html,
)
from tests.fetcher_helpers import _FakeBinaryResponse, _minimal_text_pdf

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
CAMARA_AGENDA_URL = "https://www.camara.gov.co/agenda-consolidada/"
EXPECTED_PDF_URL = (
    "https://www.camara.gov.co/wp-content/uploads/2026/04/"
    "AGENDA-LEGISLATIVA-DEL-27-AL-30-DE-ABRIL-DE-2026-MIERCOLES.pdf"
)


def _camara_source(sample_source):
    return replace(
        sample_source,
        id="camara_agenda_consolidada",
        name="Cámara — Agenda consolidada",
        type="calendar",
        url=CAMARA_AGENDA_URL,
        fetch_method="html",
    )


def _camara_agenda_pdf_text() -> str:
    return (
        "MIERCOLES 29 de abril TEMA: debate y votacion del "
        "Proyecto de Ley No. 562 de 2025 Camara, "
        "\"POR MEDIO DE LA CUAL SE FORTALECE LA POLITICA DE SALUD PUBLICA\". "
        "Autores: Representantes. La agenda contiene informacion suficiente "
        "para el analista y para el seguimiento oficial del Congreso."
    )


def test_extract_camara_agenda_pdf_link_from_embedpress_fixture(
    sample_source,
) -> None:
    source = _camara_source(sample_source)
    html = (
        FIXTURE_DIR / "camara_agenda_consolidada" / "2026-04-29.html"
    ).read_text(encoding="utf-8")

    items = _extract_camara_agenda_pdf_links(
        html,
        CAMARA_AGENDA_URL,
        source,
        "2026-04-29T00:00:00Z",
    )

    assert len(items) == 1
    item = items[0]
    assert item.url == EXPECTED_PDF_URL
    assert item.published_at == "2026-04-27T00:00:00Z"
    assert item.title.startswith("Cámara agenda PDF — AGENDA LEGISLATIVA")
    assert "27 AL 30 DE ABRIL DE 2026" in item.title
    assert "MIÉRCOLES" in item.title
    assert item.raw_text.endswith("PDF body not parsed.")
    assert item.metadata["extraction"] == "camara_agenda_embedpress_pdf_link"
    assert item.metadata["document_link_type"] == "agenda_pdf"
    assert (
        item.metadata["pdf_discovery"]
        == "embedpress_iframe_data_src_file_param"
    )
    assert item.metadata["source_page_url"] == CAMARA_AGENDA_URL
    assert item.metadata["embedpress_viewer_url"].startswith(
        "https://www.camara.gov.co/wp-admin/admin-ajax.php?action=get_viewer"
    )
    assert item.metadata["agenda_title"].startswith("AGENDA LEGISLATIVA")
    assert item.metadata["pdf_parse_status"] == "not_parsed"
    assert "content_extraction" not in item.metadata
    assert "low_quality:unparsed_pdf_link" in clean(item, source).quality_notes


def test_extract_camara_agenda_entries_from_pdf_text(sample_source) -> None:
    source = _camara_source(sample_source)
    item = _extract_camara_agenda_pdf_links(
        (
            FIXTURE_DIR / "camara_agenda_consolidada" / "2026-04-29.html"
        ).read_text(encoding="utf-8"),
        CAMARA_AGENDA_URL,
        source,
        "2026-04-29T00:00:00Z",
    )[0]

    entries = _extract_camara_agenda_entries_from_text(
        item,
        _camara_agenda_pdf_text(),
    )

    assert len(entries) == 1
    entry = entries[0]
    assert entry.published_at == "2026-04-29T00:00:00Z"
    assert "Proyecto de Ley 562 de 2025 Cámara" in entry.title
    assert entry.url == f"{EXPECTED_PDF_URL}#project-1"
    assert "official Cámara agenda PDF" in entry.raw_text
    assert entry.metadata["content_extraction"] == "camara_agenda_pdf"
    assert entry.metadata["extraction"] == "camara_agenda_pdf_entry"
    assert entry.metadata["agenda_source_url"] == CAMARA_AGENDA_URL
    assert entry.metadata["source_pdf_url"] == EXPECTED_PDF_URL
    assert entry.metadata["scheduled_date"] == "2026-04-29T00:00:00Z"
    assert entry.metadata["agenda_action_type"] == "votacion"
    assert entry.metadata["project_records"] == [
        {
            "kind": "Ley",
            "number": "562",
            "year": "2025",
            "chamber": "Cámara",
        }
    ]
    assert entry.metadata["document_title"] == (
        "POR MEDIO DE LA CUAL SE FORTALECE LA POLITICA DE SALUD PUBLICA"
    )
    assert entry.metadata["project_identity_status"] == "clean_project_identity"
    assert entry.metadata["has_clean_project_identity"] is True
    assert entry.metadata["follow_up_sources"][0]["source_id"] == (
        "camara_proyectos_ley_registry"
    )
    assert "pdf_parse_status" not in entry.metadata


def test_extract_camara_agenda_entries_from_live_plenary_text_shape(
    sample_source,
) -> None:
    source = _camara_source(sample_source)
    item = _extract_camara_agenda_pdf_links(
        (
            FIXTURE_DIR / "camara_agenda_consolidada" / "2026-04-29.html"
        ).read_text(encoding="utf-8"),
        CAMARA_AGENDA_URL,
        source,
        "2026-06-19T00:00:00Z",
    )[0]
    item = replace(
        item,
        title="Cámara agenda PDF — Plenaria-Orden del Dia-Proyectos (2026-06-17)",
        published_at="2026-06-17T00:00:00Z",
    )
    text = (
        "Para la Sesión Ordinaria del día miércoles 17 de junio de 2026. "
        "IV INFORMES DE CONCILIACIÓN Proyecto de Ley N° 058 de 2024 Cámara - "
        "077 de 2025 Senado “Por la cual se dictan normas para garantizar "
        "el derecho a la seguridad, integridad y vida de las personas en las "
        "vías de Colombia”. Publicado en la Gaceta del Congreso No. 695 de 2026. "
        "Anuncio: junio 16 de 2026. Proyecto de Ley N° 429 de 2025 Cámara - "
        "244 de 2024 Senado “Por medio de la cual se crea la política pública "
        "de estado de Familias Guardabosques”."
    )

    entries = _extract_camara_agenda_entries_from_text(item, text)

    assert len(entries) == 2
    assert entries[0].published_at == "2026-06-17T00:00:00Z"
    assert "Proyecto de Ley 058 de 2024 Cámara" in entries[0].title
    assert entries[0].metadata["project_records"] == [
        {
            "kind": "Ley",
            "number": "058",
            "year": "2024",
            "chamber": "Cámara",
        },
        {
            "kind": "Ley",
            "number": "077",
            "year": "2025",
            "chamber": "Senado",
        },
    ]
    assert entries[0].metadata["document_title"].startswith(
        "Por la cual se dictan normas"
    )
    assert entries[1].metadata["project_records"][0]["number"] == "429"


def test_fetch_camara_agenda_enriches_embedpress_pdf_entry(
    sample_source,
) -> None:
    source = _camara_source(sample_source)
    html = (
        FIXTURE_DIR / "camara_agenda_consolidada" / "2026-04-29.html"
    ).read_text(encoding="utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == EXPECTED_PDF_URL:
            return httpx.Response(
                200,
                content=_minimal_text_pdf(_camara_agenda_pdf_text()),
                request=request,
            )
        return httpx.Response(200, text=html, request=request)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    assert items[0].metadata["content_extraction"] == "camara_agenda_pdf"
    assert items[0].metadata["source_pdf_url"] == EXPECTED_PDF_URL
    assert "content_extraction_error" not in items[0].metadata


def test_enrich_camara_agenda_pdf_marks_readable_non_bill_document_as_parsed(
    sample_source,
) -> None:
    source = _camara_source(sample_source)
    items = _extract_camara_agenda_pdf_links(
        (
            FIXTURE_DIR / "camara_agenda_consolidada" / "2026-04-29.html"
        ).read_text(encoding="utf-8"),
        CAMARA_AGENDA_URL,
        source,
        "2026-04-29T00:00:00Z",
    )

    class NoProjectPdfClient:
        def get(self, url, params=None):  # noqa: ANN001 - mirrors httpx.Client.get
            text = (
                "La agenda legislativa contiene informacion general de la "
                "Camara para una sesion con datos de control politico, "
                "pero sin un numero de proyecto de ley recuperable."
            )
            return _FakeBinaryResponse(_minimal_text_pdf(text), url=url)

    enriched = _enrich_camara_agenda_pdfs(items, NoProjectPdfClient(), max_items=1)

    assert len(enriched) == 1
    item = enriched[0]
    assert item.url == EXPECTED_PDF_URL
    assert item.metadata["content_extraction"] == "camara_agenda_pdf"
    assert item.metadata["pdf_parse_status"] == "parsed_no_legislative_entries"
    assert item.metadata["document_row_type"] == "camara_agenda_document"
    assert "content_extraction_error" not in item.metadata
    assert item.metadata["pdf_text_chars"] > 0
    assert "PDF body parsed" in item.raw_text
    assert "low_quality:unparsed_pdf_link" not in clean(item, source).quality_notes


def test_fetch_camara_agenda_preserves_fail_closed_empty_result(
    sample_source,
) -> None:
    source = _camara_source(sample_source)
    html = """
    <html><body><main>
      <a href="/agenda-consolidada/">AGENDA LEGISLATIVA</a>
      <a href="/orden-del-dia/">Orden del dia comision primera</a>
    </main></body></html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, request=request)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == []
