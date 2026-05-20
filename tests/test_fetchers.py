from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_struct_time_to_iso_handles_none() -> None:
    assert _struct_time_to_iso(None) is None


def test_struct_time_to_iso_formats() -> None:
    st = time.strptime("2026-04-27T11:00:00", "%Y-%m-%dT%H:%M:%S")
    assert _struct_time_to_iso(st) == "2026-04-27T11:00:00Z"


def test_parse_date_text_to_iso_handles_spanish_dates() -> None:
    assert _parse_date_text_to_iso("Abril 09 de 2026 04:35 PM") == (
        "2026-04-09T00:00:00Z"
    )
    assert _parse_date_text_to_iso("27/04/2026") == "2026-04-27T00:00:00Z"
    assert _parse_date_text_to_iso("17.05.2026") == "2026-05-17T00:00:00Z"
    assert _parse_date_text_to_iso("Agenda Legislativa del 20 al 24 de abril de 2026") == (
        "2026-04-20T00:00:00Z"
    )


def test_extract_anchors_filters_nav_and_short() -> None:
    html = """
    <html><body><main>
      <a href="/news/article-one">Junta del Banco mantiene tasa de interés en 9.5%</a>
      <a href="https://other.com/x">Junta del Banco mantiene tasa de interés en 9.5%</a>
      <a href="/menu">Menú</a>
      <a href="#">Inicio</a>
      <a href="/news/article-two">DANE publica cifras de inflación de marzo</a>
      <a href="/news/article-one?utm_source=foo">Junta del Banco mantiene tasa de interés en 9.5%</a>
    </main></body></html>
    """
    anchors = _extract_anchors(html, "https://example.com/")
    urls = [u for _, u in anchors]
    # canonical URL dedupe: only one article-one anchor (utm variant collapsed)
    assert sum(1 for u in urls if "article-one" in u) == 1
    assert any("article-two" in u for u in urls)
    # cross-domain dropped
    assert not any("other.com" in u for u in urls)
    # nav/short dropped
    assert not any(text.lower() in {"menú", "inicio"} for text, _ in anchors)


def test_extract_anchors_caps_at_30() -> None:
    body = "".join(
        f'<a href="/n/{i}">Title number {i:03d} long enough to keep</a>'
        for i in range(60)
    )
    html = f"<html><body><main>{body}</main></body></html>"
    anchors = _extract_anchors(html, "https://example.com/")
    assert len(anchors) == 30


def test_fetch_all_records_source_trace_events(sample_source) -> None:
    source = replace(
        sample_source,
        id="trace_rss",
        name="Trace RSS",
        url="https://example.com/rss.xml",
        fetch_method="rss",
    )
    rss = """
    <rss><channel>
      <item>
        <title>Banco de la Republica anuncia nueva decision de tasas</title>
        <link>https://example.com/item-1</link>
        <pubDate>Mon, 18 May 2026 11:34:48 -0500</pubDate>
        <description>Comunicado con suficiente texto para trazabilidad.</description>
      </item>
    </channel></rss>
    """

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text=rss, request=request)
    )
    trace = RunTrace(run_date="2026-05-18", mode="daily")

    with httpx.Client(transport=transport, follow_redirects=True) as client:
        items, failures = fetchers.fetch_all([source], client=client, trace=trace)

    assert len(items) == 1
    assert failures == []
    source_events = [
        event
        for event in trace.to_dict()["events"]
        if event["name"] == "fetch_source"
    ]
    assert len(source_events) == 1
    event = source_events[0]
    assert event["status"] == "ok"
    assert event["metadata"]["source_id"] == "trace_rss"
    assert event["metadata"]["fetch_method"] == "rss"
    assert event["counts"]["raw_items"] == 1


def test_fetch_all_records_failed_source_trace_event(sample_source) -> None:
    source = replace(sample_source, id="bad_source", fetch_method="unsupported")
    trace = RunTrace(run_date="2026-05-18", mode="daily")

    items, failures = fetchers.fetch_all([source], trace=trace)

    assert items == []
    assert len(failures) == 1
    source_events = [
        event
        for event in trace.to_dict()["events"]
        if event["name"] == "fetch_source"
    ]
    assert source_events[0]["status"] == "error"
    assert source_events[0]["metadata"]["source_id"] == "bad_source"
    assert source_events[0]["error_class"] == "ValueError"


def test_cap_items_uses_source_max_items(sample_source, make_raw) -> None:
    source = replace(sample_source, max_items=2)
    items = [
        make_raw(id="1", url="https://example.com/1"),
        make_raw(id="2", url="https://example.com/2"),
        make_raw(id="3", url="https://example.com/3"),
    ]
    capped = _cap_items(source, items)
    assert [it.id for it in capped] == ["1", "2"]
