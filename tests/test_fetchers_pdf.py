from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_extract_pdf_text_reads_literal_text_stream() -> None:
    text = (
        "El DANE inicia la tercera entrega de resultados del Censo Economico "
        "Nacional Urbano con informacion suficiente para el analista."
    )
    assert "Censo Economico Nacional" in _extract_pdf_text(_minimal_text_pdf(text))


def test_extract_pdf_text_reads_split_tj_fragments() -> None:
    pdf = _minimal_operator_pdf(
        b"BT\n"
        b"[(El )4(Proyecto de Ley No. )-3(312 del 2025 Senado )"
        b"(sera discutido en primer debate por la comision.)] TJ\n"
        b"[(La agenda contiene informacion suficiente para el analista.)] TJ\n"
        b"ET"
    )

    text = _extract_pdf_text(pdf)

    assert "Proyecto de Ley No. 312 del 2025 Senado" in text
    assert "primer debate" in text


def test_extract_pdf_text_decodes_octal_escapes() -> None:
    text = _extract_pdf_text(
        _minimal_text_pdf(
            "Gaceta del Congreso. PROYECTO DE LEY N\\332MERO 550 DE 2026 "
            "C\\301MARA Y SENADO por la cual se adiciona el Presupuesto "
            "General de la Naci\\363n."
        )
    )

    assert "NÚMERO 550 DE 2026 CÁMARA Y SENADO" in text
    assert "Nación" in text


def test_enrich_pdf_text_marks_pdf_item_as_parsed_content() -> None:
    item = RawItem(
        id="dane-pdf-1",
        source_id="dane_comunicados_prensa",
        source_name="DANE",
        source_type="official_updates",
        url="https://www.dane.gov.co/files/prensa/comunicados/cp-demo.pdf",
        title="DANE publica comunicado tecnico",
        fetched_at="2026-05-06T00:00:00Z",
        published_at="2026-05-05T00:00:00Z",
        raw_text="DANE publica comunicado tecnico 05/05/2026 PDF Descargar",
        metadata={"extraction": "dane_comunicados_table"},
    )

    enriched = _enrich_pdf_text([item], _FakePdfClient(), max_items=1)[0]

    assert enriched.metadata["content_extraction"] == "pdf_text_best_effort"
    assert "PDF text excerpt" in enriched.raw_text
    assert "resultados economicos nacionales" in enriched.raw_text


def test_enrich_pdf_text_handles_pdf_aspx_attachment_urls() -> None:
    item = RawItem(
        id="mincit-pdf-1",
        source_id="mincit_zonas_francas",
        source_name="MinCIT",
        source_type="regulatory",
        url="https://zf.mincit.gov.co/getattachment/estadisticas/zonas.pdf.aspx",
        title="Zonas Francas aprobadas",
        fetched_at="2026-05-06T00:00:00Z",
        published_at="2026-02-18T00:00:00Z",
        raw_text="Zonas Francas aprobadas Fecha de actualización: 18 de febrero de 2026",
        metadata={"extraction": "anchor"},
    )

    enriched = _enrich_pdf_text([item], _FakePdfClient(), max_items=1)[0]

    assert enriched.metadata["content_extraction"] == "pdf_text_best_effort"
    assert "PDF text excerpt" in enriched.raw_text
