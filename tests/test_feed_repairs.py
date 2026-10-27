from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import ssl

import httpx
import pytest

from colombia_forecasting_desk.banrep_tls import banrep_ssl_context
from colombia_forecasting_desk.config_loader import load_metasources
from colombia_forecasting_desk.indicator_watch import (
    _fetch_policy_rate_ibr_observation,
    labor_market_observation_from_html,
    policy_rate_ibr_observation_from_rows,
)
from colombia_forecasting_desk.source_fetching.dane import _fetch_banrep_eme_portal

FIXTURES = Path(__file__).parent / "fixtures" / "feed_repairs_20261027"
NOW = datetime(2026, 10, 27, 17, tzinfo=timezone.utc)


def test_current_geih_words_and_geih_release_do_not_borrow_relab_date():
    observation = labor_market_observation_from_html((FIXTURES / "labor.html").read_text())
    assert observation is not None
    assert observation.status == "observed"
    assert observation.period == "2026-08"
    assert observation.release_date == "2026-09-30T00:00:00Z"
    assert observation.values["national_unemployment_rate_pct"] == 9.4
    assert observation.values["national_unemployment_previous_year_pct"] == 8.6
    assert observation.values["national_unemployment_previous_year_period"] == "2025-08"
    assert observation.values["national_unemployment_annual_change_pp"] == 0.8
    assert observation.values["national_participation_previous_year_pct"] == 63.9
    assert observation.values["national_occupation_previous_year_pct"] == 58.4
    assert observation.values["series_variant"] == "national_total_original"
    assert all("/GEIH/" in doc["url"] for doc in observation.values["official_documents"])


def test_geih_missing_unemployment_comparison_does_not_use_occupation_comparison():
    html = (FIXTURES / "labor.html").read_text().replace(
        ", mientras que en el mismo mes de 2025 fue 8,6%", ""
    )
    observation = labor_market_observation_from_html(html)
    assert observation is not None
    assert observation.values.get("national_unemployment_previous_year_pct") is None
    assert observation.values.get("national_unemployment_annual_change_pp") is None
    assert "national_unemployment_previous_year_period" not in observation.values


def test_verified_chain_context_keeps_certificate_and_hostname_checks():
    context = banrep_ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert not (context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN)


def test_live_suameca_nested_rows_are_filtered_by_actual_as_of_date():
    policy = json.loads((FIXTURES / "policy.json").read_text())
    ibr = json.loads((FIXTURES / "ibr.json").read_text())
    current = policy_rate_ibr_observation_from_rows(policy, ibr, NOW)
    assert current is not None
    assert current.values["policy_rate_pct"] == 12.25
    assert current.values["ibr_overnight_nominal_pct"] == 11.412
    prior = policy_rate_ibr_observation_from_rows(
        policy, ibr, datetime(2026, 9, 30, 17, tzinfo=timezone.utc)
    )
    assert prior is not None
    assert prior.values["policy_rate_pct"] == 12.0
    assert prior.values["policy_rate_date"] == "2026-09-30"


def test_live_policy_and_ibr_fetch_uses_both_series():
    def handler(request):
        series = request.url.params["idSerie"]
        name = "policy" if series == "59" else "ibr"
        return httpx.Response(200, json=json.loads((FIXTURES / f"{name}.json").read_text()))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = _fetch_policy_rate_ibr_observation(client, as_of=NOW)
    assert result.status == "observed"
    assert result.values["policy_rate_pct"] == 12.25


def _eme_source():
    return next(source for source in load_metasources("config/metasources.yaml") if source.id == "banrep_eme_expectations")


def _eme_client(menu=None):
    def handler(request):
        if request.url.path.endswith("consultaMenuXopcion"):
            return httpx.Response(200, json=menu if menu is not None else json.loads((FIXTURES / "eme-menu.json").read_text()))
        if request.url.path.endswith("res_inf_oct2026.xlsx"):
            return httpx.Response(200, content=(FIXTURES / "eme-oct2026.xlsx").read_bytes(), headers={"last-modified": "Wed, 21 Oct 2026 12:00:00 GMT"})
        raise AssertionError(f"Unexpected endpoint: {request.url}")

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_current_eme_portal_preserves_unknown_publication_date_and_point_estimates():
    with _eme_client() as client:
        rows = _fetch_banrep_eme_portal(_eme_source(), client, "2026-10-27T17:00:00Z")
    assert len(rows) == 1
    row = rows[0]
    assert row.published_at is None  # the portal's 2024 migration date is not this release
    assert row.metadata["publication_date_known"] is False
    assert row.metadata["release_date"] is None
    assert row.metadata["freshness_basis"] == "fieldwork_end"
    assert row.metadata["fieldwork_start"] == "2026-10-08"
    assert row.metadata["fieldwork_end"] == "2026-10-13"
    assert row.metadata["survey_period"] == "2026-10"
    assert row.metadata["freshness_status"] == "current"
    policy = row.metadata["expectations"]["policy_rate"]
    assert policy["median_pct"] == 12.5
    assert policy["horizon_date"] == "2026-10-31"
    assert policy["participants"] == 42
    assert "probability" not in policy
    assert row.metadata["expectations"]["inflation"]["year_end"]["mean_pct"] == 6.83


def test_eme_portal_rejects_missing_and_nonofficial_current_workbooks():
    bad_menu = [{"enlace_pagina_externa": "17000", "listaDocumento": [{"nombreAdjunto": "https://other.example/res_inf_oct2026.xlsx"}]}]
    with _eme_client(bad_menu) as client, pytest.raises(ValueError, match="no official dated"):
        _fetch_banrep_eme_portal(_eme_source(), client, "2026-10-27T17:00:00Z")


def test_eme_portal_rejects_future_fieldwork():
    with _eme_client() as client, pytest.raises(ValueError, match="non-future"):
        _fetch_banrep_eme_portal(_eme_source(), client, "2026-10-10T17:00:00Z")
