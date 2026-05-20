from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_extract_dane_comunicados_reads_dated_table(sample_source) -> None:
    source = replace(sample_source, id="dane_comunicados_prensa")
    html = """
    <table>
      <tr>
        <th>Documento</th><th>Fecha de publicación</th><th>Formato</th>
      </tr>
      <tr>
        <td>Boletín técnico mercado laboral nacional</td>
        <td>27/04/2026</td>
        <td><a href="/files/boletin.pdf">PDF</a></td>
      </tr>
    </table>
    """
    items = _extract_dane_comunicados(
        html,
        "https://www.dane.gov.co/index.php/sala-de-prensa",
        source,
        "2026-04-27T12:00:00Z",
    )
    assert len(items) == 1
    assert items[0].title == "Boletín técnico mercado laboral nacional"
    assert items[0].published_at == "2026-04-27T00:00:00Z"
    assert items[0].metadata["extraction"] == "dane_comunicados_table"


def test_parse_dane_icoced_xlsx_extracts_headline_metrics() -> None:
    parsed = _parse_dane_icoced_xlsx(
        _minimal_icoced_xlsx(),
        year=2026,
        month=3,
    )

    assert parsed is not None
    assert parsed["metrics"]["total"] == {
        "index": 135.44,
        "monthly_variation_pct": 0.75,
        "year_to_date_variation_pct": 6.47,
        "annual_variation_pct": 6.33,
    }
    assert parsed["metrics"]["residential"]["monthly_variation_pct"] == 0.77
    assert parsed["metrics"]["non_residential"]["monthly_variation_pct"] == 0.72
    assert "variación mensual de 0,75%" in parsed["headline"]
    assert "residenciales 0,77%" in parsed["headline"]


def test_enrich_dane_icoced_xlsx_marks_item_as_parsed_content() -> None:
    item = RawItem(
        id="icoced-1",
        source_id="dane_icoced",
        source_name="DANE ICOCED",
        source_type="economic_indicator",
        url="https://example.com/anex-ICOCED-mar2026.xlsx",
        title="DANE ICOCED — Anexo marzo 2026",
        fetched_at="2026-05-04T00:00:00Z",
        published_at="2026-04-30T00:00:00Z",
        raw_text="Link-level text",
        metadata={"period_year": 2026, "period_month": 3},
    )

    enriched = _enrich_dane_icoced_xlsx([item], _FakeBinaryClient())[0]

    assert enriched.metadata["content_extraction"] == "dane_icoced_xlsx"
    assert (
        enriched.metadata["headline_metrics"]["total"]["monthly_variation_pct"]
        == 0.75
    )
    assert "ICOCED total registró una variación mensual de 0,75%" in enriched.raw_text
