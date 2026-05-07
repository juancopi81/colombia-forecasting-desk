from __future__ import annotations

from colombia_forecasting_desk.tagger import tag_item


def test_tag_item_matches_aliases_with_accent_folding() -> None:
    entities, topics = tag_item(
        "Banco de la República mantiene la tasa de interés",
        "La Junta Directiva dejó sin cambios la política monetaria.",
    )

    assert entities == ["banrep"]
    assert "monetary_policy" in topics


def test_tag_item_matches_multiple_official_aliases() -> None:
    entities, topics = tag_item(
        "Registraduría y Consejo Nacional Electoral actualizan calendario",
        "La DIAN revisa medidas tributarias y de aduanas.",
    )

    assert entities == ["dian", "registraduria", "cne"]
    assert topics == ["fiscal_tax", "external_trade", "electoral"]


def test_tag_item_uses_metadata_context() -> None:
    entities, topics = tag_item(
        "Boletín mensual",
        "Resultados de producción de gas natural.",
        metadata={"source_id": "anh_estadisticas", "source_name": "ANH"},
    )

    assert entities == ["anh"]
    assert topics == ["energy", "hydrocarbons"]


def test_tag_item_does_not_match_substrings() -> None:
    entities, topics = tag_item(
        "Medianas, banrepublica y secopiloto no son entidades",
        "La ciudadanía comenta cámaras fotográficas y presidenciales.",
    )

    assert entities == []
    assert topics == []
