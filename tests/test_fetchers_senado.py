from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_extract_senado_agenda_entries_from_pdf_text() -> None:
    item = RawItem(
        id="senado-agenda-1",
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        source_type="calendar",
        url=(
            "https://www.senado.gov.co/index.php/documentos/senado-prensa/"
            "agenda-legislativa-actual/9373-agenda-legislativa-del-11-al-15-"
            "de-mayo-de-2026/file"
        ),
        title="Agenda Legislativa del 11 al 15 de mayo de 2026 ( pdf, 944 KB )",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-05-11T00:00:00Z",
        raw_text="Agenda Legislativa del 11 al 15 de mayo de 2026",
        metadata={"extraction": "anchor"},
    )
    text = (
        "MARTES 12 de mayo TEMA: la presentacion en primer debate del "
        "Proyecto de Ley No. 312 del 2025 Senado 463 del 2025 Camara, "
        "\"POR MEDIO DE LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO\". "
        "Autores: Ministro de Hacienda. "
        "MIERCOLES 13 de mayo TEMA: ponencia del Proyecto de Ley No. "
        "550 de 2026 Camara, 369 de 2026 Senado."
    )

    entries = _extract_senado_agenda_entries_from_text(item, text)

    assert len(entries) == 2
    assert entries[0].published_at == "2026-05-12T00:00:00Z"
    assert "Proyecto de Ley 312 de 2025 Senado" in entries[0].title
    assert entries[0].metadata["content_extraction"] == "senado_agenda_pdf"
    assert entries[0].metadata["scheduled_date"] == "2026-05-12T00:00:00Z"
    assert entries[0].metadata["agenda_action_type"] == "primer debate"
    assert entries[0].metadata["project_records"] == [
        {
            "kind": "Ley",
            "number": "312",
            "year": "2025",
            "chamber": "Senado",
        },
        {
            "kind": "Ley",
            "number": "463",
            "year": "2025",
            "chamber": "Cámara",
        },
    ]
    assert entries[0].metadata["document_title"] == (
        "POR MEDIO DE LA CUAL SE MODIFICA EL REGIMEN TRIBUTARIO"
    )
    assert entries[0].metadata["project_identity_status"] == "clean_project_identity"
    assert entries[0].metadata["has_clean_project_identity"] is True
    assert entries[0].metadata["follow_up_sources"][0]["source_id"] == (
        "gacetas_congreso"
    )
    assert entries[0].url.endswith("#project-1")
    assert "official Senado agenda PDF" in entries[0].raw_text
    assert "Follow-up sources: Gacetas del Congreso" in entries[0].raw_text


def test_extract_senado_agenda_keeps_loose_title_as_research_lead() -> None:
    item = RawItem(
        id="senado-agenda-1",
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        source_type="calendar",
        url="https://www.senado.gov.co/index.php/documentos/agenda/file",
        title="Agenda Legislativa del 11 al 15 de mayo de 2026 ( pdf, 944 KB )",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-05-11T00:00:00Z",
        raw_text="Agenda Legislativa del 11 al 15 de mayo de 2026",
        metadata={"extraction": "anchor"},
    )
    text = (
        "LUNES 11 de mayo TEMA: ponencia: Proyecto de Ley Senado "
        "elacual se modificael articulo de laleydeyse Dictan "
        "otrasdisposiciones. Autores: Senadores."
    )

    entries = _extract_senado_agenda_entries_from_text(item, text)

    assert len(entries) == 1
    assert "el cual se modifica el articulo" in entries[0].title
    assert entries[0].metadata["project_label"] == "Proyecto de Ley Senado"
    assert entries[0].metadata["project_records"] == []
    assert entries[0].metadata["project_identity_status"] == "missing_project_number"
    assert entries[0].metadata["has_clean_project_identity"] is False
    assert entries[0].metadata["follow_up_sources"][0]["source_id"] == (
        "gacetas_congreso"
    )


def test_enrich_senado_agenda_pdfs_replaces_pdf_link_with_entries() -> None:
    item = RawItem(
        id="senado-agenda-1",
        source_id="senado_agenda_legislativa",
        source_name="Senado — Agenda Legislativa Actual",
        source_type="calendar",
        url="https://www.senado.gov.co/index.php/documentos/agenda/file",
        title="Agenda Legislativa del 11 al 15 de mayo de 2026 ( pdf, 944 KB )",
        fetched_at="2026-05-15T15:10:00Z",
        published_at="2026-05-11T00:00:00Z",
        raw_text="Agenda Legislativa del 11 al 15 de mayo de 2026",
        metadata={"extraction": "anchor"},
    )

    enriched = _enrich_senado_agenda_pdfs(
        [item], _FakeSenadoPdfClient(), max_items=1
    )

    assert len(enriched) == 1
    assert enriched[0].metadata["extraction"] == "senado_agenda_pdf_entry"
    assert enriched[0].metadata["content_extraction"] == "senado_agenda_pdf"
    assert enriched[0].url.endswith("#project-1")
    assert "Agenda Legislativa" not in enriched[0].title
