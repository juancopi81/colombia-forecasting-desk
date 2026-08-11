from __future__ import annotations

import io
import json
from pathlib import Path
import zipfile

from colombia_forecasting_desk.config_loader import load_metasources
from tests.fetcher_helpers import *  # noqa: F403


BANREP_EME_FIXTURE_DIR = (
    Path(__file__).resolve().parent / "fixtures" / "banrep_eme"
)


def _banrep_eme_fixture_xlsx() -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr(
            "xl/workbook.xml",
            (BANREP_EME_FIXTURE_DIR / "workbook.xml").read_text(encoding="utf-8"),
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            (BANREP_EME_FIXTURE_DIR / "workbook.xml.rels").read_text(
                encoding="utf-8"
            ),
        )
        for sheet in ("resumen", "trm", "tasa_interv"):
            zf.writestr(
                f"xl/worksheets/{sheet}.xml",
                (BANREP_EME_FIXTURE_DIR / f"{sheet}.xml").read_text(
                    encoding="utf-8"
                ),
            )
    return out.getvalue()


def _banrep_eme_source(sample_source):
    return replace(
        sample_source,
        id="banrep_eme_expectations",
        name="BanRep EME",
        type="economic_indicator",
        url="https://www.banrep.gov.co/es/taxonomy/term/3996",
        fetch_method="html",
        update_frequency="monthly",
        trust_role="official_signal",
        max_items=1,
    )


def _banrep_eme_transport(*, detail_html: str | None = None, workbook: bytes | None = None):
    listing = (BANREP_EME_FIXTURE_DIR / "listing.html").read_text(encoding="utf-8")
    detail = detail_html or (BANREP_EME_FIXTURE_DIR / "detail.html").read_text(
        encoding="utf-8"
    )
    workbook_bytes = workbook if workbook is not None else _banrep_eme_fixture_xlsx()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/es/taxonomy/term/3996":
            return httpx.Response(200, text=listing, request=request)
        if "resultado-encuesta-mensual" in request.url.path:
            return httpx.Response(200, text=detail, request=request)
        if request.url.path.endswith("res_inf_jun2026.xlsx"):
            return httpx.Response(
                200,
                content=workbook_bytes,
                headers={
                    "content-type": (
                        "application/vnd.openxmlformats-officedocument."
                        "spreadsheetml.sheet"
                    )
                },
                request=request,
            )
        return httpx.Response(404, request=request)

    return httpx.MockTransport(handler)


def test_extract_banrep_eme_archive_and_detail_discovers_latest_official_xlsx() -> None:
    listing = (BANREP_EME_FIXTURE_DIR / "listing.html").read_text(encoding="utf-8")
    entries = fetchers._extract_banrep_eme_archive_entries(
        listing,
        "https://www.banrep.gov.co/es/taxonomy/term/3996",
    )

    assert [entry["period_month"] for entry in entries] == [6, 5]
    assert entries[0]["published_at"] == "2026-06-17T00:00:00Z"
    assert entries[0]["url"].endswith("eme-junio-2026")

    detail = fetchers._extract_banrep_eme_detail(
        (BANREP_EME_FIXTURE_DIR / "detail.html").read_text(encoding="utf-8"),
        entries[0]["url"],
    )
    assert detail["release_date"] == "2026-06-17T00:00:00Z"
    assert detail["workbook_url"].endswith("res_inf_jun2026.xlsx")


def test_parse_banrep_eme_xlsx_extracts_compact_consensus_fields() -> None:
    parsed = fetchers._parse_banrep_eme_xlsx(
        _banrep_eme_fixture_xlsx(),
        release_date="2026-06-17T00:00:00Z",
    )

    assert parsed is not None
    assert parsed["fieldwork_start"] == "2026-06-09"
    assert parsed["fieldwork_end"] == "2026-06-11"
    expectations = parsed["expectations"]
    assert expectations["policy_rate"] == {
        "horizon_date": "2026-06-30",
        "mean_pct": 11.76,
        "median_pct": 11.75,
        "mode_pct": 11.75,
        "min_pct": 11.25,
        "max_pct": 13.0,
        "participants": 40,
    }
    assert expectations["trm"]["nearest"]["median_cop_per_usd"] == 3514.9
    assert expectations["trm"]["year_end"]["median_cop_per_usd"] == 3663.0
    assert expectations["inflation"]["year_end"]["mean_pct"] == 6.52
    assert expectations["inflation"]["twelve_month"]["mean_pct"] == 5.53


def test_fetch_banrep_eme_emits_one_stale_baseline_not_a_conclusion(
    sample_source,
    monkeypatch,
) -> None:
    source = _banrep_eme_source(sample_source)
    monkeypatch.setattr(fetchers, "_now_iso", lambda: "2026-08-11T15:00:00Z")

    with httpx.Client(
        transport=_banrep_eme_transport(),
        follow_redirects=True,
    ) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    item = items[0]
    assert item.source_type == "economic_indicator"
    assert item.published_at == "2026-06-17T00:00:00Z"
    assert "baseline, not a conclusion" in item.raw_text
    assert item.metadata["baseline_role"] == "consensus_only"
    assert item.metadata["freshness_status"] == "stale"
    assert item.metadata["days_stale"] == 55
    assert item.metadata["is_stale"] is True
    assert item.metadata["participant_counts"] == {
        "policy_rate": 40,
        "trm": 38,
        "inflation": 41,
    }


def test_fetch_banrep_eme_uses_browser_for_bot_blocked_detail(
    sample_source,
    monkeypatch,
) -> None:
    source = _banrep_eme_source(sample_source)
    listing = (BANREP_EME_FIXTURE_DIR / "listing.html").read_text(encoding="utf-8")
    detail = (BANREP_EME_FIXTURE_DIR / "detail.html").read_text(encoding="utf-8")
    workbook = _banrep_eme_fixture_xlsx()
    browser_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/es/taxonomy/term/3996":
            return httpx.Response(200, text=listing, request=request)
        if "resultado-encuesta-mensual" in request.url.path:
            return httpx.Response(
                200,
                text="<html>Radware Bot Manager</html>",
                request=request,
            )
        if request.url.path.endswith("res_inf_jun2026.xlsx"):
            return httpx.Response(200, content=workbook, request=request)
        return httpx.Response(404, request=request)

    def fake_browser_detail(url: str) -> tuple[str, str]:
        browser_calls.append(url)
        return detail, url

    monkeypatch.setattr(
        dane_fetchers,
        "_fetch_banrep_eme_detail_with_browser",
        fake_browser_detail,
    )
    with httpx.Client(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    ) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    assert browser_calls == [
        "https://www.banrep.gov.co/es/"
        "resultado-encuesta-mensual-expectativas-analistas-economicos-eme-junio-2026"
    ]


def test_fetch_banrep_eme_missing_workbook_fails_closed(sample_source) -> None:
    source = _banrep_eme_source(sample_source)
    transport = _banrep_eme_transport(detail_html="<html><body>No XLSX</body></html>")

    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items, failures = fetchers.fetch_all([source], client=client)

    assert items == []
    assert len(failures) == 1
    assert failures[0].source_id == "banrep_eme_expectations"
    assert "did not expose an official XLSX" in failures[0].error_message


def test_fetch_banrep_eme_malformed_workbook_fails_closed(sample_source) -> None:
    source = _banrep_eme_source(sample_source)
    transport = _banrep_eme_transport(workbook=b"not an xlsx archive")

    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items, failures = fetchers.fetch_all([source], client=client)

    assert items == []
    assert len(failures) == 1
    assert "missing required consensus fields" in failures[0].error_message


def test_banrep_eme_metasource_is_enabled_monthly_official_baseline() -> None:
    sources = load_metasources(
        Path(__file__).resolve().parents[1] / "config" / "metasources.yaml"
    )
    source = next(item for item in sources if item.id == "banrep_eme_expectations")

    assert source.enabled is True
    assert source.priority == "high"
    assert source.update_frequency == "monthly"
    assert source.trust_role == "official_signal"
    assert source.max_items == 1


def _banrep_calendar_html() -> str:
    calendar_options = {
        "timeZone": "America/Bogota",
        "events": [
            {
                "title": (
                    "Reunión de la Junta Directiva de julio de 2026 - "
                    "Decisión sobre la tasa de interés de intervención"
                ),
                "start": "2026-07-31T08:30:00",
                "url": (
                    "/es/noticias/calendario-actividades-junta-directiva/"
                    "reunion-julio-2026"
                ),
            },
            {
                "title": (
                    "Publicación de las minutas de la reunión de la Junta "
                    "Directiva de julio de 2026"
                ),
                "start": "2026-08-05T17:00:00",
                "url": (
                    "/es/noticias/calendario-actividades-junta-directiva/"
                    "publicacion-minutas-julio-2026"
                ),
            },
        ],
    }
    settings = {
        "fullCalendarView": [
            {"calendar_options": json.dumps(calendar_options, ensure_ascii=False)}
        ]
    }
    return (
        '<html><body><script type="application/json" '
        'data-drupal-selector="drupal-settings-json">'
        f"{json.dumps(settings, ensure_ascii=False)}"
        "</script></body></html>"
    )


def test_extract_banrep_calendar_keeps_only_policy_rate_decisions(
    sample_source,
) -> None:
    source = replace(
        sample_source,
        id="banrep_junta_calendar",
        name="BanRep Junta calendar",
        type="calendar",
        url="https://www.banrep.gov.co/es/calendario-junta-directiva",
        trust_role="agenda_signal",
    )

    items = fetchers._extract_banrep_junta_calendar(
        _banrep_calendar_html(),
        source.url,
        source,
        "2026-07-30T12:00:00Z",
    )

    assert len(items) == 1
    item = items[0]
    assert item.title.startswith("Reunión de la Junta Directiva de julio de 2026")
    assert item.url.endswith("/reunion-julio-2026")
    assert item.published_at == "2026-07-31T08:30:00-05:00"
    assert item.metadata == {
        "content_extraction": "banrep_junta_calendar",
        "event_type": "banrep_policy_rate_decision",
        "scheduled_date": "2026-07-31",
        "scheduled_at_local": "2026-07-31T08:30:00-05:00",
        "timezone": "America/Bogota",
    }


def test_fetch_banrep_calendar_uses_structured_calendar_parser(
    sample_source,
) -> None:
    source = replace(
        sample_source,
        id="banrep_junta_calendar",
        name="BanRep Junta calendar",
        type="calendar",
        url="https://www.banrep.gov.co/es/calendario-junta-directiva",
        trust_role="agenda_signal",
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            text=_banrep_calendar_html(),
            request=request,
        )
    )

    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert len(items) == 1
    assert items[0].metadata["scheduled_date"] == "2026-07-31"


def test_fetch_banrep_calendar_uses_browser_on_bot_block(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="banrep_junta_calendar",
        name="BanRep Junta calendar",
        type="calendar",
        url="https://www.banrep.gov.co/es/calendario-junta-directiva",
        trust_role="agenda_signal",
    )
    browser_item = RawItem(
        id="banrep-calendar-browser-item",
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url=(
            "https://www.banrep.gov.co/es/noticias/"
            "calendario-actividades-junta-directiva/reunion-julio-2026"
        ),
        title="Reunión Junta Directiva julio 2026 - decisión tasa",
        fetched_at="2026-07-30T12:00:00Z",
        published_at="2026-07-31T08:30:00-05:00",
        raw_text="Official BanRep calendar event.",
        metadata={
            "content_extraction": "banrep_junta_calendar",
            "event_type": "banrep_policy_rate_decision",
            "scheduled_date": "2026-07-31",
        },
    )
    calls: list[str] = []

    def fake_browser_fetch(source_arg, fetched_at):
        calls.append(source_arg.id)
        return [browser_item]

    monkeypatch.setattr(
        fetchers,
        "_fetch_banrep_calendar_with_browser",
        fake_browser_fetch,
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>Radware Bot Manager</html>")
    )

    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == [browser_item]
    assert calls == ["banrep_junta_calendar"]


def test_fetch_banrep_junta_uses_browser_on_bot_block(
    sample_source,
    monkeypatch,
) -> None:
    source = replace(
        sample_source,
        id="banrep_junta_comunicados",
        name="BanRep Junta",
        type="official_updates",
        url="https://www.banrep.gov.co/es/comunicados-junta",
        fetch_method="html",
    )
    browser_item = RawItem(
        id="banrep-browser-item",
        source_id=source.id,
        source_name=source.name,
        source_type=source.type,
        url="https://www.banrep.gov.co/es/minutas",
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-06T00:00:00Z",
        raw_text="Official BanRep minutas.",
        metadata={"content_extraction": "banrep_minutas_html"},
    )
    calls: list[str] = []

    def fake_browser_fetch(source_arg, fetched_at):
        calls.append(source_arg.id)
        return [browser_item]

    monkeypatch.setattr(
        fetchers,
        "_fetch_banrep_junta_with_browser",
        fake_browser_fetch,
    )

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>Radware Bot Manager</html>")
    )
    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items = fetch_html(source, client)

    assert items == [browser_item]
    assert calls == ["banrep_junta_comunicados"]


def test_extract_banrep_minutas_metadata_reads_policy_body() -> None:
    metadata = _extract_banrep_minutas_metadata(
        BANREP_MINUTAS_DETAIL_HTML,
        "https://www.banrep.gov.co/es/noticias/minutas-banrep-marzo-2026",
    )

    assert metadata["content_extraction"] == "banrep_minutas_html"
    assert metadata["decision_action"] == "hike"
    assert metadata["rate_change_bps"] == 100
    assert metadata["policy_rate_pct"] == "11.25"
    assert metadata["publication_date"] == "2026-04-07T00:00:00Z"
    assert metadata["vote_result"] == "majority"
    assert "Cuatro directores votaron" in metadata["vote_summary"]
    assert len(metadata["key_bullets"]) == 2
    assert "incrementar la tasa" in metadata["board_blocs"]["majority"]
    assert "reducción de 50 pbs" in metadata["board_blocs"]["rate_cut_bloc"]
    assert "mantener inalterada" in metadata["board_blocs"]["hold_bloc"]
    assert metadata["official_links"][0]["url"].endswith("/es/print/pdf/node/65818")
    assert "ABR 30" in metadata["next_meeting_context"]


def test_extract_banrep_minutas_metadata_reads_drupal_node_body() -> None:
    metadata = _extract_banrep_minutas_metadata(
        BANREP_MINUTAS_DRUPAL_DETAIL_HTML,
        "https://www.banrep.gov.co/es/noticias/minutas-banrep-abril-2026",
    )

    assert metadata["content_extraction"] == "banrep_minutas_html"
    assert metadata["decision_action"] == "hold"
    assert metadata["policy_rate_pct"] == "11.25"
    assert metadata["publication_date"] == "2026-05-06T00:00:00Z"
    assert metadata["vote_result"] == "unanimous"
    assert "consenso" in metadata["vote_summary"]
    assert len(metadata["key_bullets"]) == 2
    assert "cuatro directores" in metadata["board_blocs"]["hawkish_bloc"]
    assert "dos directores" in metadata["board_blocs"]["dovish_bloc"]
    assert "Otro miembro" in metadata["board_blocs"]["single_member_bloc"]
    assert "30 de junio" in metadata["next_meeting_context"]
    assert metadata["official_links"][0]["url"].startswith(
        "https://d1b4gd4m8561gs.cloudfront.net/"
    )


def test_enrich_banrep_minutas_html_keeps_listing_item_on_detail_failure() -> None:
    minutas = RawItem(
        id="banrep-minutas-1",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-marzo-2026",
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-04-07T00:00:00Z",
        raw_text="07/04/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )
    comunicado = RawItem(
        id="banrep-comunicado-1",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/junta-directiva-marzo-2026",
        title="La Junta Directiva decidió incrementar la tasa",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-03-31T00:00:00Z",
        raw_text="31/03/2026 Comunicado Junta",
        metadata={"extraction": "anchor"},
    )
    missing = RawItem(
        id="banrep-minutas-2",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-enero-2026",
        title="Minutas BanRep: decisión anterior",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-02-04T00:00:00Z",
        raw_text="04/02/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )

    enriched = _enrich_banrep_minutas_html(
        [minutas, comunicado, missing],
        _FakeBanrepMinutasClient(),
        max_items=3,
    )

    assert enriched[0].metadata["content_extraction"] == "banrep_minutas_html"
    assert "BanRep minutas detail" in enriched[0].raw_text
    assert enriched[1] == comunicado
    assert enriched[2] == missing
    assert enriched[2].metadata == {"extraction": "anchor"}


def test_enrich_banrep_minutas_html_uses_browser_when_detail_is_bot_blocked(
    monkeypatch,
) -> None:
    minutas = RawItem(
        id="banrep-minutas-1",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url="https://www.banrep.gov.co/es/noticias/minutas-banrep-marzo-2026",
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-04-29T00:00:00Z",
        published_at="2026-04-07T00:00:00Z",
        raw_text="07/04/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )
    browser_item = RawItem(
        id=minutas.id,
        source_id=minutas.source_id,
        source_name=minutas.source_name,
        source_type=minutas.source_type,
        url=minutas.url,
        title=minutas.title,
        fetched_at=minutas.fetched_at,
        published_at=minutas.published_at,
        raw_text="Browser parsed BanRep minutas detail.",
        metadata={"content_extraction": "banrep_minutas_html"},
    )
    calls: list[int] = []

    def fake_browser_enrich(items, *, max_items):
        calls.append(max_items)
        assert items == [minutas]
        return [browser_item]

    monkeypatch.setattr(
        dane_fetchers,
        "_enrich_banrep_minutas_html_with_browser_session",
        fake_browser_enrich,
    )

    enriched = _enrich_banrep_minutas_html(
        [minutas],
        _FakeBanrepBotBlockClient(),
        max_items=1,
    )

    assert enriched == [browser_item]
    assert calls == [1]


def test_enrich_banrep_minutas_html_with_browser_uses_detail_parser() -> None:
    minutas_url = "https://www.banrep.gov.co/es/minutas"
    minutas = RawItem(
        id="banrep-minutas-browser",
        source_id="banrep_junta_comunicados",
        source_name="BanRep Junta",
        source_type="official_updates",
        url=minutas_url,
        title="Minutas BanRep: decisión de política monetaria",
        fetched_at="2026-05-19T00:00:00Z",
        published_at="2026-05-06T00:00:00Z",
        raw_text="06/05/2026 Minutas BanRep",
        metadata={"extraction": "anchor"},
    )
    page = _FakeBanrepBrowserPage({minutas_url: BANREP_MINUTAS_DETAIL_HTML})

    enriched = _enrich_banrep_minutas_html_with_browser(
        [minutas],
        page,
        max_items=1,
    )

    assert enriched[0].metadata["content_extraction"] == "banrep_minutas_html"
    assert enriched[0].metadata["vote_result"] == "majority"
    assert "BanRep minutas detail" in enriched[0].raw_text
