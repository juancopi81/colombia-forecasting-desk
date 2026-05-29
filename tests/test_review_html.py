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
    (run_dir / "human_decisions.md").write_text(
        "# Human Decisions\n\nDecision: `monitor_no_new_m3`.\n\nPost today: no.\n",
        encoding="utf-8",
    )
    art = rh.load_run_artifacts(run_dir)
    assert art["run_summary.json"] == {"raw_items": 5}
    assert art["analyst_leads.json"] is None  # missing file -> None, no crash
    assert art["_human_decision"] == {"decision": "monitor_no_new_m3", "post_today": "no"}
    assert "human_decisions.md" in art["_present"]


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
