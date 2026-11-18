from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from colombia_forecasting_desk import indicator_watch as watch


HTML = (Path(__file__).parent / "fixtures/dane_trade_balance/2026-11-18.html").read_text()
EXPORTS_HTML = """
<h2>Información septiembre de 2026</h2>
<p>Las ventas externas del país fueron US$5.410,7 millones FOB y presentaron
un crecimiento de 16,4%; aumento del 20,0% en las ventas externas del grupo de
Manufacturas.</p><p>Información actualizada el 4 de noviembre de 2026</p>
"""
IMPORTS_HTML = """
<h2>Información septiembre 2026</h2>
<p>Las importaciones fueron US$8.192,8 millones CIF y presentaron un crecimiento
de 33,7%; aumento de 40,0% en el grupo de Manufacturas.</p>
<p>Información actualizada el 18 de noviembre de 2026</p>
"""


def components():
    exports = watch.exports_component_from_html(EXPORTS_HTML)
    imports = watch.imports_component_from_html(IMPORTS_HTML)
    balance = watch.trade_balance_component_from_html(HTML)
    assert exports is not None and imports is not None and balance is not None
    return exports, imports, balance


def test_official_monthly_balance_retains_signed_values_and_provenance():
    balance = watch.trade_balance_component_from_html(HTML)
    assert balance is not None
    assert balance.period == "2026-09"
    assert balance.release_date == "2026-11-18T00:00:00Z"
    assert balance.source_url == watch.TRADE_BALANCE_URL
    assert balance.values["goods_trade_balance_usd_millions"] == -2268.2
    assert balance.values["prior_year_goods_trade_balance_usd_millions"] == -1135.1
    assert balance.values["prior_year_period"] == "2025-09"
    assert balance.values["valuation"] == "FOB"
    assert balance.values["frequency"] == "monthly"
    assert "se registro un deficit" in balance.values["source_text"]


@pytest.mark.parametrize("current,prior,expected", [
    ("superávit", "déficit", (2268.2, -1135.1)),
    ("déficit", "superávit", (-2268.2, 1135.1)),
])
def test_balance_sign_comes_from_each_published_label(current, prior, expected):
    html = HTML.replace("un déficit en", f"un {current} en").replace(
        "un déficit de", f"un {prior} de"
    )
    balance = watch.trade_balance_component_from_html(html)
    assert balance is not None
    assert (
        balance.values["goods_trade_balance_usd_millions"],
        balance.values["prior_year_goods_trade_balance_usd_millions"],
    ) == expected


@pytest.mark.parametrize("old,new", [
    ("Información septiembre 2026", "Información agosto 2026"),
    ("Información septiembre 2026", "Información septiembre 2025"),
    ("En septiembre de 2026", "En agosto de 2026"),
    ("En septiembre de 2026", "En septiembre de 2025"),
    ("en septiembre de 2025", "en agosto de 2025"),
    ("en septiembre de 2025", "en septiembre de 2024"),
    ("En septiembre de 2026", "En enero-septiembre de 2026"),
    ("FOB", "CIF"),
    ("2.268,2", "2.26,8,2"),
    ("2.268,2", "-2.268,2"),
    ("2.268,2", "N/D"),
    ("2.268,2", "2,268.2"),
    ("Información septiembre 2026", "Información histórica"),
    ("Información actualizada el 18 de noviembre de 2026", "Sin fecha"),
    ("18 de noviembre de 2026", "31 de noviembre de 2026"),
    ("18 de noviembre de 2026", "18 de septiembre de 2026"),
])
def test_balance_rejects_unmatched_periods_nonmonthly_valuation_or_bad_values(old, new):
    assert watch.trade_balance_component_from_html(HTML.replace(old, new)) is None


def test_balance_rejects_ambiguous_current_results():
    assert watch.trade_balance_component_from_html(
        HTML + HTML.replace("2.268,2", "3.000,0")
    ) is None


def test_bundle_uses_the_official_balance_instead_of_mixed_valuation_arithmetic():
    exports, imports, balance = components()
    bundle = watch.external_trade_observation_from_components([exports, imports, balance])
    assert bundle is not None
    assert bundle.period == "2026-09"
    assert bundle.values["observed_components"] == 3
    assert bundle.values["total_components"] == 3
    assert bundle.values["goods_trade_balance_status"] == "observed_official_fob"
    assert bundle.values["goods_trade_balance_usd_millions"] == -2268.2
    assert bundle.values["goods_trade_balance_usd_millions"] != (
        exports.values["exports_usd_millions_fob"]
        - imports.values["imports_usd_millions_cif"]
    )
    assert bundle.values["goods_trade_balance_source_url"] == balance.source_url
    assert "official balance US$-2,268.2m FOB" in bundle.headline
    assert "not a subtraction" in bundle.values["goods_trade_balance_caveat"]
    assert bundle.components == [exports, imports, balance]


@pytest.mark.parametrize("period", ["2026-08", "2025-09", "2026-10"])
def test_balance_for_a_different_period_stays_separate(period):
    exports, imports, balance = components()
    balance = replace(balance, period=period, release_date="2026-11-19T00:00:00Z")
    bundle = watch.external_trade_observation_from_components([exports, imports, balance])
    assert bundle is not None
    assert bundle.period == "2026-09"
    assert bundle.release_date == "2026-11-18T00:00:00Z"
    assert bundle.values["goods_trade_balance_status"] == "unavailable_period_mismatch"
    assert "goods_trade_balance_usd_millions" not in bundle.values
    assert period in bundle.values["goods_trade_balance_caveat"]
    assert bundle.values["components"]["trade_balance"] == balance.values
    assert "-2,268.2" not in bundle.headline


@pytest.mark.parametrize("missing_exports", [False, True])
def test_no_combined_balance_with_missing_or_different_period_exports(missing_exports):
    exports, imports, balance = components()
    given = [imports, balance]
    if not missing_exports:
        given.insert(0, replace(exports, period="2026-10"))
    bundle = watch.external_trade_observation_from_components(given)
    assert bundle is not None and bundle.status == "observed"
    assert bundle.values["goods_trade_balance_status"] == "unavailable_matched_components"
    assert "goods_trade_balance_usd_millions" not in bundle.values
    assert bundle.values["components"]["trade_balance"] == balance.values


@pytest.mark.parametrize("balance_response", ["ok", "unparseable", "http_error", "timeout"])
def test_structured_fetch_collects_balance_and_isolates_its_failures(monkeypatch, balance_response):
    requests = []

    def handler(request):
        url = str(request.url)
        requests.append(url)
        if url == watch.EXPORTS_URL:
            return httpx.Response(200, text=EXPORTS_HTML)
        if url == watch.IMPORTS_URL:
            return httpx.Response(200, text=IMPORTS_HTML)
        if url == watch.TRADE_BALANCE_URL:
            if balance_response == "timeout":
                raise httpx.ReadTimeout("balance timeout", request=request)
            if balance_response == "http_error":
                return httpx.Response(503)
            return httpx.Response(200, text=HTML if balance_response == "ok" else "<p>Unavailable</p>")
        return httpx.Response(503)

    client_class = httpx.Client
    monkeypatch.setattr(watch.httpx, "Client", lambda **kwargs: client_class(
        transport=httpx.MockTransport(handler), **kwargs
    ))
    observations = watch.fetch_structured_indicator_observations(
        now=datetime(2026, 11, 18, tzinfo=timezone.utc)
    )
    trade = next(item for item in observations if item.indicator_id == "external_trade")
    assert requests.count(watch.TRADE_BALANCE_URL) == 1
    assert trade.status == "observed"
    assert trade.period == "2026-09"
    assert [part.status for part in trade.components[:2]] == ["observed", "observed"]
    if balance_response == "ok":
        assert trade.components[2].status == "observed"
        assert trade.values["goods_trade_balance_usd_millions"] == -2268.2
    else:
        assert trade.components[2].status == "failed"
        assert trade.values["goods_trade_balance_status"] == "unavailable_official_balance"
        assert "goods_trade_balance_usd_millions" not in trade.values
