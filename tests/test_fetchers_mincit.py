from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_extract_mincit_zonas_francas_approved_rows_from_pdf_text() -> None:
    rows = _extract_mincit_zonas_francas_approved_rows_from_text(
        _mincit_zf_item(),
        MINCIT_ZF_SAMPLE_TEXT,
    )

    assert len(rows) == 2
    first = rows[0].metadata
    assert first["registry"] == "mincit_zonas_francas_aprobadas"
    assert first["content_extraction"] == "mincit_zonas_francas_approved_pdf"
    assert first["nit"] == "800178052"
    assert first["zona_franca_name"] == (
        "Zona Franca Industrial de Bienes y Servicios La Candelaria"
    )
    assert first["zone_class"] == "Permanente"
    assert first["user_type"] == "Usuario Operador"
    assert first["department"] == "Bolívar"
    assert first["municipality"] == "Cartagena"
    assert first["declaratory_resolution"] == "Res. 95 de 10 de febrero de 1993"
    assert first["extension_resolution"] == (
        "Res. 1311 de 1 de diciembre de 2021"
    )
    assert first["ciiu"] == "7020"
    assert first["snapshot_date"] == "2025-12-31T00:00:00Z"
    assert first["source_report_date"] == "2026-01-29T00:00:00Z"
    assert first["follow_up_sources"][1]["source_id"] == "diario_oficial"

    second = rows[1].metadata
    assert second["nit"] == "901911193"
    assert second["zone_class"] == "Permanente Especial"
    assert second["extension_resolution"] == "Vacía"


def test_extract_mincit_zonas_francas_handles_repeated_location_terms() -> None:
    text = (
        "ZONAS FRANCAS FECHA: 31 DE DICIEMBRE DE 2025 "
        "NIT NOMBRE ZONA FRANCA CLASE DE ZONA FRANCA TIPO DE USUARIO "
        "DEPARTAMENTO MUNICIPIO Resolución de declaratoria Resolución de prorroga CIIU "
        "800185347 Zona Franca de Bogotá Permanente Usuario Operador Bogotá Bogotá "
        "Res. 934 de 06 de agosto de 1993 Res. 888 de 26 de agosto de 2020 7020 "
        "900162578 Zona Franca de Las Américas S.A.S. Permanente Permanente "
        "Magdalena Santa Marta Res. 5657 de 27 de Junio de 2008 "
        "Res. 232 de 9 de febrero de 2022 6820 "
        "90191119 3 Zona Franca Permanente Especial De Servicios Rionegro MRO "
        "Permanente especial Servicios Antioquia Rionegro "
        "Res. No. 2118 del 26 de diciembre de 2025 Vacía 3315 "
        "RESUMEN ZONAS FRANCAS."
    )

    rows = _extract_mincit_zonas_francas_approved_rows_from_text(
        _mincit_zf_item(),
        text,
    )

    assert len(rows) == 3
    assert rows[0].metadata["department"] == "Bogotá"
    assert rows[0].metadata["municipality"] == "Bogotá"
    assert rows[1].metadata["user_type"] == "Permanente"
    assert rows[1].metadata["department"] == "Magdalena"
    assert rows[2].metadata["registry_key"] == "901911193"


def test_enrich_mincit_zonas_francas_expands_approved_pdf_to_registry_rows() -> None:
    enriched = _enrich_mincit_zonas_francas(
        [_mincit_zf_item()],
        _FakeMinCITPdfClient(),
    )

    assert [item.metadata["registry_key"] for item in enriched] == [
        "800178052",
        "901911193",
    ]
    assert all(
        item.metadata["content_extraction"] == "mincit_zonas_francas_approved_pdf"
        for item in enriched
    )
    assert "MinCIT Zonas Francas aprobadas" in enriched[0].title
