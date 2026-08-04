from __future__ import annotations

import json

from tests.fetcher_helpers import *  # noqa: F403


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
