from __future__ import annotations

from pathlib import Path

from colombia_forecasting_desk import review_html as rh


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #
def _art(**overrides) -> dict:
    """Minimal loaded-artifact dict for a clean monitor day."""
    art = {
        "run_summary.json": {
            "run_date": "2026-05-29",
            "raw_items": 504,
            "cleaned_items": 202,
            "clusters": 42,
            "sources_checked": 30,
            "sources_failed": 1,
            "finished_at": "2026-05-29T15:45:33Z",
        },
        "run_manifest.json": {"counts": {"m1_candidates": 9}},
        "acceptance_report.json": {
            "status": "pass",
            "warning_count": 3,
            "error_count": 0,
            "issues": [],
        },
        "analyst_leads.json": {
            "summary": {
                "lead_count": 9,
                "forecast_question_count": 0,
                "analyst_insight_count": 5,
                "investigation_lead_count": 4,
                "indicator_tension_card_count": 4,
                "review_item_count": 20,
            },
            "leads": [],
        },
        "m2_ranked_questions.json": {
            "bucket_counts": {"watchlist": 31, "research_more": 49, "blocked_or_resolved": 11},
            "review_queue": [],
        },
        "indicator_tension_cards.json": [],
        "market_pricing_watch.json": [],
        "cooccurrence_bundles.json": [],
        "source_health.json": [],
        "_run_date": "2026-05-29",
        "_present": set(),
        "_human_decision": None,
        "_human_monitor_queue": [],
        "_candidate_monitor_queue": [],
    }
    art.update(overrides)
    return art


def _row(date: str, m3_ready: bool = False) -> rh.RunRow:
    return rh.RunRow(
        date=date,
        raw_items=0,
        cleaned_items=0,
        clusters=0,
        candidates=0,
        analyst_leads=0,
        forecast_questions=0,
        analyst_insights=0,
        investigation_leads=0,
        tension_cards=0,
        bundles=0,
        market_observed=0,
        sources_checked=0,
        sources_failed=0,
        acceptance_status="pass",
        acceptance_warnings=0,
        acceptance_errors=0,
        review_items=0,
        m2_buckets={},
        m3_ready=m3_ready,
        finished_at="",
    )


def _insight(title: str, family: str = "sovereign_funding", **overrides) -> dict:
    lead = {
        "lead_type": "analyst_insight",
        "title": title,
        "claim_or_question": f"{title} claim.",
        "disposition": "monitor_or_research",
        "next_check": "Inspect the underlying series.",
        "evidence": [{"label": "Policy rate", "value": "11.25%", "source": "BanRep", "url": ""}],
        "caveats": ["Advisory screen only."],
        "review_context": {"family": family},
    }
    lead.update(overrides)
    return lead


# --------------------------------------------------------------------------- #
# derive_decision
# --------------------------------------------------------------------------- #
def test_derive_decision_is_monitor_when_no_m3_signal() -> None:
    decision = rh.derive_decision(_art())
    assert decision.status == "monitor_no_post"
    assert decision.m3_ready is False
    assert "monitoring run by design" in decision.headline


def test_derive_decision_is_review_when_forecast_question_present() -> None:
    art = _art()
    art["analyst_leads.json"]["summary"]["forecast_question_count"] = 1
    decision = rh.derive_decision(art)
    assert decision.status == "review_for_post"
    assert decision.m3_ready is True


def test_derive_decision_is_review_when_ready_for_m3_bucket_present() -> None:
    art = _art()
    art["m2_ranked_questions.json"]["bucket_counts"]["ready_for_m3"] = 1
    decision = rh.derive_decision(art)
    assert decision.m3_ready is True


def test_derive_decision_surfaces_recorded_human_decision() -> None:
    art = _art(_human_decision={"decision": "monitor_no_new_m3", "post_today": "no"})
    decision = rh.derive_decision(art)
    assert decision.recorded_human_decision == {
        "decision": "monitor_no_new_m3",
        "post_today": "no",
    }


def test_derive_decision_blocks_all_zero_source_failure_run() -> None:
    art = _art(
        **{
            "run_summary.json": {
                "run_date": "2026-06-03",
                "raw_items": 0,
                "cleaned_items": 0,
                "clusters": 0,
                "sources_checked": 30,
                "sources_failed": 30,
                "finished_at": "2026-06-03T15:33:13Z",
            },
            "run_manifest.json": {"counts": {"m1_candidates": 0}},
            "acceptance_report.json": {
                "status": "fail",
                "warning_count": 12,
                "error_count": 6,
                "issues": [],
            },
            "_human_decision": {
                "decision": "blocked_network_not_decision_grade",
                "post_today": "no",
            },
        }
    )

    decision = rh.derive_decision(art)
    assert decision.status == "blocked_network_not_decision_grade"
    assert decision.m3_ready is False
    assert "Rerun with live network access" in decision.headline

    html_out = rh.render_daily_review_html(art)
    assert "Blocked - not decision-grade" in html_out
    assert "Monitoring — no new forecast" not in html_out
    assert "recorded decision <code>blocked_network_not_decision_grade</code>" in html_out


# --------------------------------------------------------------------------- #
# summarize_run / drought
# --------------------------------------------------------------------------- #
def test_summarize_run_pulls_counts_and_m3_flag() -> None:
    row = rh.summarize_run(_art())
    assert row.date == "2026-05-29"
    assert row.raw_items == 504
    assert row.candidates == 9
    assert row.forecast_questions == 0
    assert row.acceptance_status == "pass"
    assert row.m3_ready is False
    assert row.finished_at == "2026-05-29T15:45:33Z"


def test_count_forecast_drought_counts_trailing_monitor_runs() -> None:
    rows = [
        _row("2026-05-18", m3_ready=True),
        _row("2026-05-19", m3_ready=True),
        _row("2026-05-20", m3_ready=False),
        _row("2026-05-21", m3_ready=False),
        _row("2026-05-22", m3_ready=False),
    ]
    assert rh.count_forecast_drought(rows) == 3


def test_count_forecast_drought_is_zero_when_latest_is_ready() -> None:
    rows = [_row("2026-05-20", m3_ready=False), _row("2026-05-21", m3_ready=True)]
    assert rh.count_forecast_drought(rows) == 0


# --------------------------------------------------------------------------- #
# source caveats
# --------------------------------------------------------------------------- #
def test_collect_source_caveats_flags_only_genuine_visibility_gaps() -> None:
    art = _art(
        **{
            "source_health.json": [
                # genuine failure
                {
                    "source_id": "registraduria_noticias",
                    "source_name": "Registraduría",
                    "status": "failed",
                    "onboarding_status": "needs_parser",
                    "failure_count": 1,
                    "failures": ["HTTPStatusError: 404"],
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
                # no parser yet -> unreliable silence
                {
                    "source_id": "moe_observatorio",
                    "source_name": "MOE",
                    "status": "no_raw",
                    "onboarding_status": "needs_parser",
                    "failure_count": 0,
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
                # document links but nothing parsed
                {
                    "source_id": "minhacienda_proyectos_decreto",
                    "source_name": "MinHacienda",
                    "status": "no_rankable",
                    "onboarding_status": "working",
                    "failure_count": 0,
                    "document_link_count": 17,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
                # WORKING source with parsed content but no rankable candidate:
                # this is healthy and must NOT be flagged.
                {
                    "source_id": "mincit_zonas_francas",
                    "source_name": "MinCIT",
                    "status": "no_rankable",
                    "onboarding_status": "working",
                    "failure_count": 0,
                    "document_link_count": 0,
                    "parsed_content_count": 116,
                    "rankable_count": 0,
                },
                # fully ok source: not flagged
                {
                    "source_id": "eltiempo_colombia",
                    "source_name": "El Tiempo",
                    "status": "ok",
                    "onboarding_status": "working",
                    "failure_count": 0,
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 21,
                },
            ]
        }
    )
    flagged = {c["source_id"] for c in rh.collect_source_caveats(art)}
    assert flagged == {
        "registraduria_noticias",
        "moe_observatorio",
        "minhacienda_proyectos_decreto",
    }


# --------------------------------------------------------------------------- #
# aggregation
# --------------------------------------------------------------------------- #
def test_aggregate_recurring_insights_counts_sorts_and_dedups() -> None:
    per_run = [
        ("2026-05-27", _art(**{"analyst_leads.json": {"summary": {}, "leads": [
            _insight("Construction cost vs IPC squeeze", "construction_cost"),
            _insight("TES-policy spread tension"),
            # duplicate within the same day must count once
            _insight("TES-policy spread tension"),
        ]}})),
        ("2026-05-28", _art(**{"analyst_leads.json": {"summary": {}, "leads": [
            _insight("Construction cost vs IPC squeeze", "construction_cost"),
        ]}})),
        ("2026-05-29", _art(**{"analyst_leads.json": {"summary": {}, "leads": [
            _insight("Construction cost vs IPC squeeze", "construction_cost"),
        ]}})),
    ]
    themes = rh.aggregate_recurring_insights(per_run)
    # sorted by days desc, then label asc
    assert themes[0].label == "Construction cost vs IPC squeeze"
    assert themes[0].days == 3
    assert themes[0].family == "construction_cost"
    assert themes[1].label == "TES-policy spread tension"
    assert themes[1].days == 1
    assert themes[1].dates == ["2026-05-27"]


def test_aggregate_source_issues_counts_recurrence() -> None:
    bad = {
        "source_id": "moe",
        "source_name": "MOE",
        "status": "no_raw",
        "onboarding_status": "needs_parser",
        "failure_count": 0,
        "document_link_count": 0,
        "parsed_content_count": 0,
        "rankable_count": 0,
    }
    per_run = [
        ("2026-05-28", _art(**{"source_health.json": [bad]})),
        ("2026-05-29", _art(**{"source_health.json": [bad]})),
    ]
    issues = rh.aggregate_source_issues(per_run)
    assert issues[0].label == "MOE"
    assert issues[0].days == 2


# --------------------------------------------------------------------------- #
# monitor queue
# --------------------------------------------------------------------------- #
def test_derive_monitor_queue_combines_leads_and_review_queue_and_dedups() -> None:
    art = _art(
        **{
            "analyst_leads.json": {
                "summary": {},
                "leads": [
                    {
                        "lead_type": "investigation_lead",
                        "title": "PL 564 subsidy traceability",
                        "claim_or_question": "...",
                        "next_check": "Check committee agenda.",
                    }
                ],
            },
            "m2_ranked_questions.json": {
                "bucket_counts": {},
                "review_queue": [
                    {"question_seed": "PL 564 subsidy traceability", "bucket": "watchlist",
                     "canonical_bill_id": "bill:2026:camara:564"},  # dup label, dropped
                    {"question_seed": "PL 565 pension adjustment", "bucket": "watchlist",
                     "canonical_bill_id": "bill:2026:camara:565"},
                ],
            },
        }
    )
    queue = rh.derive_monitor_queue(art)
    labels = [item["label"] for item in queue]
    assert labels == ["PL 564 subsidy traceability", "PL 565 pension adjustment"]
    assert queue[0]["kind"] == "investigation lead"


def test_derive_monitor_queue_prefers_human_priorities() -> None:
    art = _art(
        _human_monitor_queue=[
            {
                "label": "Confirm official scrutiny status.",
                "kind": "human priority",
                "note": "Election Follow-Up Queue",
            }
        ],
        **{
            "analyst_leads.json": {
                "summary": {},
                "leads": [
                    {
                        "lead_type": "investigation_lead",
                        "title": "Machine-derived fallback",
                        "next_check": "Check committee agenda.",
                    }
                ],
            }
        },
    )
    assert rh.derive_monitor_queue(art) == [
        {
            "label": "Confirm official scrutiny status.",
            "kind": "human priority",
            "note": "Election Follow-Up Queue",
        }
    ]


def test_derive_monitor_queue_uses_candidate_questions_before_derived() -> None:
    art = _art(
        _candidate_monitor_queue=[
            {
                "label": "Re-check official Registraduria/CNE result status.",
                "kind": "candidate review",
                "note": "Monitor Queue",
            }
        ],
        **{
            "analyst_leads.json": {
                "summary": {},
                "leads": [
                    {
                        "lead_type": "investigation_lead",
                        "title": "Machine-derived fallback",
                        "next_check": "Check committee agenda.",
                    }
                ],
            }
        },
    )
    assert rh.derive_monitor_queue(art) == [
        {
            "label": "Re-check official Registraduria/CNE result status.",
            "kind": "candidate review",
            "note": "Monitor Queue",
        }
    ]


def test_derive_monitor_queue_prefers_human_over_candidate_questions() -> None:
    art = _art(
        _human_monitor_queue=[
            {
                "label": "Human queue wins.",
                "kind": "human priority",
                "note": "Monitor Queue",
            }
        ],
        _candidate_monitor_queue=[
            {
                "label": "Candidate fallback.",
                "kind": "candidate review",
                "note": "Monitor Queue",
            }
        ],
    )
    assert [item["label"] for item in rh.derive_monitor_queue(art)] == [
        "Human queue wins."
    ]


def test_derive_monitor_queue_compacts_long_legislative_titles() -> None:
    art = _art(
        **{
            "m2_ranked_questions.json": {
                "bucket_counts": {},
                "review_queue": [
                    {
                        "bucket": "watchlist",
                        "canonical_bill_id": "bill:2026:camara:556",
                        "question_seed": (
                            "Could Proyecto de Ley 556 de 2026 Cámara - Por medio de "
                            "la cual se establece el régimen jurídico del boxeo "
                            "profesional en Colombia, se crea la Dirección Nacional "
                            "de Boxeo Profesional como autoridad especializada y "
                            "organismo rector del boxeo profesional, se garantiza la "
                            "separación estructural, funcional, competencial e "
                            "institucional entre el deporte amateur y el deporte "
                            "profesional, se adoptan medidas de integridad, "
                            "transparencia, control y protección de los boxeadores y "
                            "boxeadoras profesionales y campeones del país, y se "
                            "dictan otras disposiciones. become a forecastable "
                            "unresolved legislative decision?"
                        ),
                    }
                ],
            }
        }
    )
    queue = rh.derive_monitor_queue(art)
    assert queue[0]["label"] == "PL 556/2026 Cámara"
    assert "régimen jurídico del boxeo profesional" in queue[0]["detail"]


# --------------------------------------------------------------------------- #
# url safety + loader
# --------------------------------------------------------------------------- #
def test_safe_url_accepts_only_http_strings() -> None:
    assert rh._safe_url("https://example.com") == "https://example.com"
    assert rh._safe_url("http://example.com") == "http://example.com"
    assert rh._safe_url("javascript:alert(1)") == ""
    assert rh._safe_url({"url": "https://example.com"}) == ""
    assert rh._safe_url(None) == ""


def test_find_run_dirs_filters_dated_dirs_and_windows(tmp_path: Path) -> None:
    for name in ["2026-05-27", "2026-05-28", "2026-05-29", "notes", "review_index.html"]:
        target = tmp_path / name
        if name.endswith(".html"):
            target.write_text("x", encoding="utf-8")
        else:
            target.mkdir()
    dirs = rh.find_run_dirs(tmp_path, window=2)
    assert [d.name for d in dirs] == ["2026-05-28", "2026-05-29"]
    assert [d.name for d in rh.find_run_dirs(tmp_path, window=None)] == [
        "2026-05-27",
        "2026-05-28",
        "2026-05-29",
    ]


def test_load_run_artifacts_is_tolerant_and_extracts_human_decision(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-05-29"
    run_dir.mkdir()
    (run_dir / "run_summary.json").write_text('{"raw_items": 5}', encoding="utf-8")
    (run_dir / "indicator_watch.json").write_text(
        '[{"indicator_id": "labor_market", "status": "failed"}]',
        encoding="utf-8",
    )
    (run_dir / "human_decisions.md").write_text(
        (
            "# Human Decisions\n\n"
            "Decision: `monitor_no_new_m3`.\n\n"
            "## Election And Market Follow-Up Queue\n\n"
            "1. Confirm official first-round result status and scrutiny timeline.\n"
            "2. Capture Monday closing USD/COP and TES curve/yields.\n\n"
            "## Post Decision\n\n"
            "Post today: no.\n"
        ),
        encoding="utf-8",
    )
    art = rh.load_run_artifacts(run_dir)
    assert art["run_summary.json"] == {"raw_items": 5}
    assert art["indicator_watch.json"] == [
        {"indicator_id": "labor_market", "status": "failed"}
    ]
    assert art["analyst_leads.json"] is None  # missing file -> None, no crash
    assert art["_human_decision"] == {"decision": "monitor_no_new_m3", "post_today": "no"}
    assert art["_human_monitor_queue"] == [
        {
            "label": "Confirm official first-round result status and scrutiny timeline.",
            "kind": "human priority",
            "note": "Election And Market Follow-Up Queue",
        },
        {
            "label": "Capture Monday closing USD/COP and TES curve/yields.",
            "kind": "human priority",
            "note": "Election And Market Follow-Up Queue",
        },
    ]
    assert "human_decisions.md" in art["_present"]


def test_load_run_artifacts_extracts_candidate_questions_monitor_queue(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-02"
    run_dir.mkdir()
    (run_dir / "candidate_questions.md").write_text(
        (
            "# Candidate Questions\n\n"
            "## Candidates Reviewed\n\n"
            "1. This numbered list is not a queue.\n\n"
            "## Monitor Queue\n\n"
            "1. Re-check official Registraduria/CNE result and scrutiny status with an access\n"
            "   path that is not blocked by Cloudflare.\n"
            "2. Collect local TES and official TRM context.\n"
        ),
        encoding="utf-8",
    )
    art = rh.load_run_artifacts(run_dir)
    assert art["_candidate_monitor_queue"] == [
        {
            "label": (
                "Re-check official Registraduria/CNE result and scrutiny status "
                "with an access path that is not blocked by Cloudflare."
            ),
            "kind": "candidate review",
            "note": "Monitor Queue",
        },
        {
            "label": "Collect local TES and official TRM context.",
            "kind": "candidate review",
            "note": "Monitor Queue",
        },
    ]


# --------------------------------------------------------------------------- #
# rendering: structure, safety, determinism
# --------------------------------------------------------------------------- #
def test_render_daily_returns_complete_deterministic_document() -> None:
    art = _art(
        _present={"analyst_leads.md", "metasource_brief.md", "human_decisions.md"},
        _human_decision={"decision": "monitor_no_new_m3", "post_today": "no"},
        **{"analyst_leads.json": {
            "summary": {
                "forecast_question_count": 0,
                "analyst_insight_count": 1,
                "investigation_lead_count": 0,
                "indicator_tension_card_count": 0,
                "review_item_count": 20,
                "lead_count": 1,
            },
            "leads": [_insight("TES-policy spread tension")],
        }},
    )
    html_out = rh.render_daily_review_html(art)
    assert html_out.count("<!DOCTYPE html>") == 1
    assert html_out.count("<html") == 1 and html_out.count("</html>") == 1
    assert "<title>Daily Review — 2026-05-29</title>" in html_out
    assert "Monitoring — no new forecast" in html_out
    assert "TES-policy spread tension" in html_out
    assert "human_decisions.md" in html_out
    # advisory framing is present so the surface cannot read as a probability input
    assert "never probability inputs" in html_out
    # deterministic: identical input renders identical output
    assert html_out == rh.render_daily_review_html(art)


def test_render_daily_links_sampling_decision_artifacts() -> None:
    art = _art(
        _present={
            "candidate_questions.md",
            "m2_sampling_decisions.md",
            "m2_sampling_decisions.json",
        },
    )
    html_out = rh.render_daily_review_html(art)
    assert 'href="m2_sampling_decisions.md"' in html_out
    assert 'href="m2_sampling_decisions.json"' in html_out
    assert "M2 sampling decisions" in html_out


def test_render_daily_labels_human_monitor_queue() -> None:
    art = _art(
        _human_monitor_queue=[
            {
                "label": "Confirm official scrutiny status.",
                "kind": "human priority",
                "note": "Election And Market Follow-Up Queue",
            }
        ],
    )
    html_out = rh.render_daily_review_html(art)
    assert "Monitor queue (human)" in html_out
    assert "Parsed from human_decisions.md" in html_out
    assert "Confirm official scrutiny status." in html_out
    assert "Machine-derived fallback" not in html_out


def test_render_daily_labels_candidate_questions_monitor_queue() -> None:
    art = _art(
        _candidate_monitor_queue=[
            {
                "label": "Collect local TES and official TRM context.",
                "kind": "candidate review",
                "note": "Monitor Queue",
            }
        ],
    )
    html_out = rh.render_daily_review_html(art)
    assert "Monitor queue (candidate_questions)" in html_out
    assert "Parsed from candidate_questions.md" in html_out
    assert "Collect local TES and official TRM context." in html_out
    assert "candidate review" in html_out


def test_render_daily_marks_observed_market_rows_lagged_or_stale() -> None:
    art = _art(
        _run_date="2026-06-02",
        **{
            "market_pricing_watch.json": [
                {
                    "status": "observed",
                    "freshness_status": "current",
                    "observed_date": "2026-06-01",
                    "latest_close": 41.74,
                    "currency": "USD",
                    "name": "Global X MSCI Colombia ETF",
                    "headline": "COLO latest daily close was 41.74 USD on 2026-06-01.",
                    "source_name": "Nasdaq public historical endpoint",
                    "caveats": ["Advisory context only."],
                },
                {
                    "status": "observed",
                    "freshness_status": "current",
                    "observed_date": "2026-05-26",
                    "latest_close": 102.75,
                    "currency": "USD/barrel",
                    "name": "Brent crude spot price",
                    "headline": "Brent spot latest daily close was 102.75 USD/barrel on 2026-05-26.",
                    "source_name": "FRED / EIA",
                    "caveats": ["Publication lags can occur."],
                },
            ],
        },
    )
    html_out = rh.render_daily_review_html(art)
    assert ">observed<" in html_out
    assert ">lagged<" in html_out
    assert ">stale<" in html_out
    assert "observed 2026-06-01" in html_out
    assert "observed 2026-05-26" in html_out
    assert ">current<" not in html_out


def test_render_daily_dedupes_stale_market_status_and_freshness_pills() -> None:
    art = _art(
        **{
            "market_pricing_watch.json": [
                {
                    "status": "stale",
                    "freshness_status": "stale",
                    "observed_date": "2026-05-26",
                    "latest_close": 3920.25,
                    "currency": "COP/USD",
                    "name": "USD/COP spot",
                    "headline": "USD/COP latest available close is stale.",
                    "source_name": "Market data snapshot",
                    "caveats": ["Advisory context only."],
                },
            ],
        },
    )
    html_out = rh.render_daily_review_html(art)
    assert html_out.count(">stale<") == 1
    assert "USD/COP spot" in html_out


def test_render_daily_includes_official_indicator_moves_before_insights() -> None:
    art = _art(
        _run_date="2026-06-05",
        **{
            "indicator_watch.json": [
                {
                    "indicator_id": "trm_usd_cop",
                    "name": "TRM / USD-COP",
                    "category": "markets",
                    "status": "observed",
                    "frequency": "daily",
                    "freshness_status": "current",
                    "period": "2026-06-05",
                    "release_date": "2026-06-05T00:00:00Z",
                    "headline": (
                        "TRM vigente desde 2026-06-05: 3565.32 COP/USD. "
                        "Seven-day move: -81.26 COP (-2.23%), peso appreciation."
                    ),
                    "values": {
                        "trm_cop_per_usd": 3565.32,
                        "seven_day_change_pct": -2.23,
                    },
                    "source_name": "Superintendencia Financiera de Colombia",
                    "source_url": "https://www.datos.gov.co/trm",
                    "next_step": "Observed from datos.gov.co TRM dataset.",
                }
            ],
        },
    )
    html_out = rh.render_daily_review_html(art)
    assert "Official indicator moves" in html_out
    assert "TRM / USD-COP" in html_out
    assert "material move" in html_out
    assert "3565.32" in html_out
    assert "Source: <a href=\"https://www.datos.gov.co/trm\"" in html_out
    assert html_out.index("Why no M3 today") < html_out.index("Official indicator moves")
    assert html_out.index("Official indicator moves") < html_out.index(
        "Top analyst insights"
    )


def test_render_daily_keeps_indicator_moves_separate_from_market_pricing() -> None:
    art = _art(
        _run_date="2026-06-05",
        **{
            "indicator_watch.json": [
                {
                    "indicator_id": "trm_usd_cop",
                    "name": "TRM / USD-COP",
                    "category": "markets",
                    "status": "observed",
                    "frequency": "daily",
                    "freshness_status": "current",
                    "period": "2026-06-05",
                    "headline": "TRM seven-day move was -2.23%.",
                    "values": {"seven_day_change_pct": -2.23},
                    "source_name": "Superfinanciera",
                    "source_url": "https://www.datos.gov.co/trm",
                }
            ],
            "market_pricing_watch.json": [
                {
                    "status": "observed",
                    "freshness_status": "current",
                    "observed_date": "2026-06-05",
                    "latest_close": 41.74,
                    "currency": "USD",
                    "name": "Global X MSCI Colombia ETF",
                    "headline": "COLO latest daily close was 41.74 USD.",
                    "source_name": "Nasdaq public historical endpoint",
                    "caveats": ["Advisory context only."],
                }
            ],
        },
    )
    html_out = rh.render_daily_review_html(art)
    market_section = html_out[html_out.index("Market-pricing context") :]
    assert "Global X MSCI Colombia ETF" in market_section
    assert "TRM / USD-COP" not in market_section
    assert "Experimental, fail-closed context only" in market_section


def test_render_daily_omits_stale_indicator_moves_from_official_moves() -> None:
    art = _art(
        _run_date="2026-06-05",
        **{
            "indicator_watch.json": [
                {
                    "indicator_id": "oil_gas_production",
                    "name": "Oil and gas production",
                    "status": "observed",
                    "frequency": "monthly",
                    "freshness_status": "stale",
                    "period": "2025-01",
                    "headline": "Older oil and gas production period.",
                    "values": {},
                }
            ],
        },
    )
    html_out = rh.render_daily_review_html(art)
    assert "Official indicator moves" not in html_out
    assert "Indicator coverage gaps" in html_out
    assert "Oil and gas production" in html_out


def test_render_daily_keeps_full_long_legislative_title_in_details() -> None:
    long_title = (
        "Proyecto de Ley 564 de 2026 Cámara - Por medio de la cual se crea el "
        "Sistema Único de Trazabilidad, Validación y No Duplicidad de Subsidios "
        "Estatales, se fortalece la interoperabilidad de la oferta social del "
        "Estado y se dictan otras disposiciones."
    )
    art = _art(
        **{
            "analyst_leads.json": {
                "summary": {
                    "forecast_question_count": 0,
                    "analyst_insight_count": 0,
                    "investigation_lead_count": 1,
                    "indicator_tension_card_count": 0,
                    "review_item_count": 0,
                    "lead_count": 1,
                },
                "leads": [
                    {
                        "lead_type": "investigation_lead",
                        "title": long_title,
                        "claim_or_question": (
                            f"Should {long_title} be reviewed alongside Fiscal / "
                            "tax pulse because subsidy legislation can interact "
                            "with fiscal cost?"
                        ),
                        "evidence": [
                            {
                                "label": (
                                    "Cámara registry — Proyecto de Ley 564 de "
                                    "2026 Cámara — SUBSIDIOS TRANSPARENTES"
                                ),
                                "value": "Full registry value.",
                                "source": "Cámara",
                            }
                        ],
                        "source_refs": {
                            "artifact_refs": [
                                {
                                    "artifact": "legislative_reconciler.json",
                                    "key": "canonical_bill_id",
                                    "value": "bill:2026:camara:564",
                                }
                            ]
                        },
                    }
                ],
            }
        },
    )
    html_out = rh.render_daily_review_html(art)
    assert "PL 564/2026 Cámara - SUBSIDIOS TRANSPARENTES" in html_out
    assert "Should PL 564/2026 Cámara - SUBSIDIOS TRANSPARENTES be reviewed" in html_out
    assert "<summary>Full title</summary>" in html_out
    assert "Sistema Único de Trazabilidad" in html_out


def test_render_daily_groups_source_reliability_by_impact_bucket() -> None:
    art = _art(
        **{
            "source_health.json": [
                {
                    "source_id": "registraduria_noticias",
                    "source_name": "Registraduría Noticias",
                    "status": "failed",
                    "onboarding_status": "working",
                    "failure_count": 1,
                    "failures": ["HTTPStatusError: 403"],
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
                {
                    "source_id": "moe_observatorio",
                    "source_name": "MOE Observatorio",
                    "status": "no_raw",
                    "onboarding_status": "needs_parser",
                    "failure_count": 0,
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
            ]
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "High-impact source failures" in html_out
    assert "high_impact_failures" in html_out
    assert "Background parser debt" in html_out
    assert "background_parser_debt" in html_out
    assert html_out.index("High-impact source failures") < html_out.index(
        "Background parser debt"
    )
    assert "Registraduría Noticias" in html_out
    assert "MOE Observatorio" in html_out


def test_render_daily_marks_priority_document_gaps_as_decision_relevant() -> None:
    art = _art(
        **{
            "source_health.json": [
                {
                    "source_id": "minhacienda_proyectos_decreto",
                    "source_name": "MinHacienda proyectos de decreto",
                    "status": "no_rankable",
                    "onboarding_status": "working",
                    "failure_count": 0,
                    "document_link_count": 17,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
            ]
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "Decision-relevant parser gaps" in html_out
    assert "decision_relevant_parser_gaps" in html_out
    assert "MinHacienda proyectos de decreto" in html_out
    assert "Background parser debt" not in html_out


def test_render_daily_marks_lead_referenced_document_gaps_as_decision_relevant() -> None:
    art = _art(
        **{
            "analyst_leads.json": {
                "summary": {
                    "forecast_question_count": 0,
                    "analyst_insight_count": 1,
                    "investigation_lead_count": 0,
                    "indicator_tension_card_count": 0,
                    "review_item_count": 0,
                    "lead_count": 1,
                },
                "leads": [
                    _insight(
                        "Municipal procurement decree watch",
                        evidence=[
                            {
                                "label": "decree index",
                                "value": "3 linked documents",
                                "source": "Municipal source",
                                "source_id": "municipal_decree_index",
                            }
                        ],
                    )
                ],
            },
            "source_health.json": [
                {
                    "source_id": "municipal_decree_index",
                    "source_name": "Municipal decree index",
                    "status": "no_rankable",
                    "onboarding_status": "working",
                    "failure_count": 0,
                    "document_link_count": 3,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
            ],
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "Decision-relevant parser gaps" in html_out
    assert "decision_relevant_parser_gaps" in html_out
    assert "Municipal decree index" in html_out
    assert "Background parser debt" not in html_out


def test_render_daily_marks_indicator_parse_failures_as_coverage_gaps() -> None:
    art = _art(
        **{
            "source_health.json": [
                {
                    "source_id": "dane_geih_labor_market",
                    "source_name": "DANE labor-market current result",
                    "status": "failed",
                    "onboarding_status": "working",
                    "failure_count": 1,
                    "failures": ["ValueError: current result parse failed"],
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
            ]
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "Indicator coverage gaps" in html_out
    assert "indicator_coverage_gaps" in html_out
    assert "DANE labor-market current result" in html_out
    assert "High-impact source failures" not in html_out


def test_render_daily_includes_indicator_watch_failures_as_coverage_gaps() -> None:
    art = _art(
        **{
            "indicator_watch.json": [
                {
                    "indicator_id": "labor_market",
                    "name": "Labor market",
                    "status": "failed",
                    "freshness_status": "failed",
                    "headline": "DANE headline fetch returned no parseable current-result text.",
                    "next_step": "Add GEIH annex details when needed.",
                }
            ],
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "Indicator coverage gaps" in html_out
    assert "indicator_coverage_gaps" in html_out
    assert "Labor market" in html_out
    assert "DANE headline fetch returned no parseable current-result text." in html_out


def test_render_daily_marks_stale_indicator_sources_as_coverage_gaps() -> None:
    art = _art(
        **{
            "source_health.json": [
                {
                    "source_id": "labor_market_current_result",
                    "source_name": "Labor-market current result",
                    "status": "stale",
                    "onboarding_status": "working",
                    "failure_count": 0,
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
            ]
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "Indicator coverage gaps" in html_out
    assert "indicator_coverage_gaps" in html_out
    assert "stale" in html_out


def test_render_daily_labels_mass_dns_failures_as_execution_environment() -> None:
    art = _art(
        **{
            "acceptance_report.json": {
                "status": "fail",
                "warning_count": 0,
                "error_count": 1,
                "issues": [
                    {
                        "code": "operational_source_failure_share_too_high",
                        "severity": "error",
                        "message": "Too many sources failed for a full M1 run to be operational.",
                    }
                ],
            },
            "source_health.json": [
                {
                    "source_id": "registraduria_noticias",
                    "source_name": "Registraduría Noticias",
                    "status": "failed",
                    "onboarding_status": "working",
                    "failure_count": 1,
                    "failures": ["ConnectError: DNS failed in sandbox"],
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
                {
                    "source_id": "gacetas_congreso",
                    "source_name": "Gacetas del Congreso",
                    "status": "failed",
                    "onboarding_status": "working",
                    "failure_count": 1,
                    "failures": ["ConnectError: DNS failed in sandbox"],
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
                {
                    "source_id": "minhacienda_noticias",
                    "source_name": "MinHacienda noticias",
                    "status": "failed",
                    "onboarding_status": "working",
                    "failure_count": 1,
                    "failures": ["ConnectError: DNS failed in sandbox"],
                    "document_link_count": 0,
                    "parsed_content_count": 0,
                    "rankable_count": 0,
                },
            ],
        }
    )
    html_out = rh.render_daily_review_html(art)
    assert "Execution environment failures" in html_out
    assert "execution_environment_failures" in html_out
    assert "rerun before treating as source health" in html_out
    assert "High-impact source failures" not in html_out


def test_render_daily_escapes_html_and_never_emits_unsafe_href() -> None:
    malicious = _insight("<script>alert('xss')</script>")
    # malformed url field (a stringified dict, as seen in procurement leads)
    malicious["evidence"] = [
        {"label": "evil", "value": "x", "source": "s", "url": {"url": "https://e.com"}},
        {"label": "ok", "value": "y", "source": "s", "url": "https://good.example"},
    ]
    art = _art(**{"analyst_leads.json": {
        "summary": {
            "forecast_question_count": 0,
            "analyst_insight_count": 1,
            "investigation_lead_count": 0,
            "indicator_tension_card_count": 0,
            "review_item_count": 0,
            "lead_count": 1,
        },
        "leads": [malicious],
    }})
    html_out = rh.render_daily_review_html(art)
    assert "<script>alert" not in html_out
    assert "&lt;script&gt;" in html_out
    # the dict url must not become an href; the valid url still does
    assert 'href="{' not in html_out
    assert 'href="https://good.example"' in html_out


def test_render_index_shows_drought_and_per_run_links(tmp_path: Path) -> None:
    for name in ["2026-05-28", "2026-05-29"]:
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / "run_summary.json").write_text(
            f'{{"run_date": "{name}", "raw_items": 10, "sources_checked": 5, "sources_failed": 0}}',
            encoding="utf-8",
        )
        (run_dir / "analyst_leads.json").write_text(
            '{"summary": {"forecast_question_count": 0, "lead_count": 2}, "leads": []}',
            encoding="utf-8",
        )
        (run_dir / "m2_ranked_questions.json").write_text(
            '{"bucket_counts": {"watchlist": 3}, "review_queue": []}', encoding="utf-8"
        )
    html_out = rh.render_runs_index_html(rh.find_run_dirs(tmp_path, window=14))
    assert "<title>Recent Runs — Colombia Forecasting Desk</title>" in html_out
    assert "2 consecutive monitoring run(s)" in html_out
    assert 'href="2026-05-29/review.html"' in html_out
    assert html_out == rh.render_runs_index_html(rh.find_run_dirs(tmp_path, window=14))


def test_render_index_uses_latest_human_monitor_queue(tmp_path: Path) -> None:
    for name in ["2026-05-28", "2026-05-29"]:
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / "run_summary.json").write_text(
            f'{{"run_date": "{name}", "raw_items": 10}}',
            encoding="utf-8",
        )
        (run_dir / "analyst_leads.json").write_text(
            '{"summary": {"forecast_question_count": 0, "lead_count": 0}, "leads": []}',
            encoding="utf-8",
        )
        (run_dir / "m2_ranked_questions.json").write_text(
            '{"bucket_counts": {}, "review_queue": []}', encoding="utf-8"
        )
    (tmp_path / "2026-05-29" / "human_decisions.md").write_text(
        (
            "# Human Decisions\n\n"
            "## Monitor Queue\n\n"
            "1. Check the official committee agenda.\n"
        ),
        encoding="utf-8",
    )
    html_out = rh.render_runs_index_html(rh.find_run_dirs(tmp_path, window=14))
    assert "Parsed from the latest run&#x27;s human_decisions.md." in html_out
    assert "Check the official committee agenda." in html_out
    assert "human priority" in html_out


def test_render_index_uses_latest_candidate_questions_monitor_queue(tmp_path: Path) -> None:
    for name in ["2026-05-28", "2026-05-29"]:
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / "run_summary.json").write_text(
            f'{{"run_date": "{name}", "raw_items": 10}}',
            encoding="utf-8",
        )
        (run_dir / "analyst_leads.json").write_text(
            '{"summary": {"forecast_question_count": 0, "lead_count": 0}, "leads": []}',
            encoding="utf-8",
        )
        (run_dir / "m2_ranked_questions.json").write_text(
            '{"bucket_counts": {}, "review_queue": []}', encoding="utf-8"
        )
    (tmp_path / "2026-05-29" / "candidate_questions.md").write_text(
        (
            "# Candidate Questions\n\n"
            "## Monitor Queue\n\n"
            "1. Re-check official Registraduria/CNE result status.\n"
        ),
        encoding="utf-8",
    )
    html_out = rh.render_runs_index_html(rh.find_run_dirs(tmp_path, window=14))
    assert "Parsed from the latest run&#x27;s candidate_questions.md." in html_out
    assert "Re-check official Registraduria/CNE result status." in html_out
    assert "candidate review" in html_out


def test_render_index_aggregates_source_reliability_bucket_labels(tmp_path: Path) -> None:
    source_health = """
[
  {
    "source_id": "registraduria_noticias",
    "source_name": "Registraduría Noticias",
    "status": "failed",
    "onboarding_status": "working",
    "failure_count": 1,
    "failures": ["HTTPStatusError: 403"],
    "document_link_count": 0,
    "parsed_content_count": 0,
    "rankable_count": 0
  }
]
""".strip()
    for name in ["2026-05-28", "2026-05-29"]:
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / "run_summary.json").write_text(
            f'{{"run_date": "{name}", "raw_items": 10, "sources_checked": 5, "sources_failed": 1}}',
            encoding="utf-8",
        )
        (run_dir / "analyst_leads.json").write_text(
            '{"summary": {"forecast_question_count": 0, "lead_count": 0}, "leads": []}',
            encoding="utf-8",
        )
        (run_dir / "m2_ranked_questions.json").write_text(
            '{"bucket_counts": {}, "review_queue": []}', encoding="utf-8"
        )
        (run_dir / "source_health.json").write_text(source_health, encoding="utf-8")
    html_out = rh.render_runs_index_html(rh.find_run_dirs(tmp_path, window=14))
    assert "Source reliability issues" in html_out
    assert "Registraduría Noticias" in html_out
    assert "High-impact source failures" in html_out
