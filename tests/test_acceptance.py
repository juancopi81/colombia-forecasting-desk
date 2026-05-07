from __future__ import annotations

from colombia_forecasting_desk.acceptance import build_acceptance_report
from colombia_forecasting_desk.models import RunSummary, SourceFailure, SourceHealth


def _summary(**overrides) -> RunSummary:
    base = dict(
        run_date="2026-05-06",
        started_at="2026-05-06T12:00:00Z",
        finished_at="2026-05-06T12:00:30Z",
        sources_checked=2,
        sources_failed=0,
        raw_items=10,
        cleaned_items=5,
        clusters=2,
    )
    base.update(overrides)
    return RunSummary(**base)


def _candidate(**overrides) -> dict:
    base = {
        "candidate_id": "m1c_event_abc123",
        "candidate_type": "event_signal",
        "resolution_source": "Primary official source.",
        "noise_reasons": [],
        "evidence": {
            "source_ids": ["banrep_rss"],
            "links": [{"title": "Source", "url": "https://example.com"}],
            "starting_evidence": "Official signal.",
        },
    }
    base.update(overrides)
    return base


def test_acceptance_passes_for_well_formed_candidate(make_cleaned) -> None:
    report = build_acceptance_report(
        _summary(),
        {"candidates": [_candidate()]},
        [
            SourceHealth(
                source_id="banrep_rss",
                source_name="BanRep",
                url="https://example.com",
                raw_count=1,
                cleaned_count=1,
                dated_count=1,
                rankable_count=1,
                failure_count=0,
            )
        ],
        [],
        [
            make_cleaned(
                detected_entities=["banrep"],
                detected_topics=["monetary_policy"],
            )
        ],
    )

    assert report["status"] == "pass"
    assert report["strict_pass"] is True
    assert report["error_count"] == 0


def test_acceptance_errors_on_candidate_without_resolution_or_evidence() -> None:
    report = build_acceptance_report(
        _summary(),
        {
            "candidates": [
                _candidate(
                    resolution_source="",
                    evidence={"source_ids": ["dane_comunicados_prensa"]},
                )
            ]
        },
        [],
        [],
        [],
    )

    codes = {issue["code"] for issue in report["issues"]}
    assert report["status"] == "fail"
    assert "candidate_missing_resolution_source" in codes
    assert "candidate_missing_evidence" in codes


def test_acceptance_errors_when_candidate_uses_link_only_source() -> None:
    report = build_acceptance_report(
        _summary(),
        {
            "candidates": [
                _candidate(
                    evidence={
                        "source_ids": ["gacetas_congreso"],
                        "links": [{"url": "https://example.com"}],
                    }
                )
            ]
        },
        [
            SourceHealth(
                source_id="gacetas_congreso",
                source_name="Gacetas",
                url="https://example.com",
                raw_count=3,
                cleaned_count=0,
                dated_count=3,
                rankable_count=0,
                failure_count=0,
                content_mode="document_links_only",
                document_link_count=3,
                parsed_content_count=0,
            )
        ],
        [],
        [],
    )

    codes = {issue["code"] for issue in report["issues"]}
    assert "candidate_missing_evidence" in codes
    assert "candidate_from_link_only_source" in codes
    assert "document_source_unparsed" in codes
    assert report["strict_pass"] is False


def test_acceptance_warns_on_high_impact_source_failure() -> None:
    report = build_acceptance_report(
        _summary(sources_failed=1),
        {"candidates": [_candidate()]},
        [],
        [
            SourceFailure(
                source_id="registraduria_noticias",
                source_name="Registraduria",
                url="https://example.com",
                error_class="HTTPStatusError",
                error_message="403",
                occurred_at="2026-05-06T12:00:00Z",
            )
        ],
        [],
    )

    assert report["strict_pass"] is True
    assert "high_impact_source_failed" in {
        issue["code"] for issue in report["issues"]
    }


def test_acceptance_errors_on_nonempty_run_without_candidates() -> None:
    report = build_acceptance_report(
        _summary(raw_items=3),
        {"candidates": []},
        [],
        [],
        [],
    )

    assert report["status"] == "fail"
    assert report["issues"][0]["code"] == "no_candidates_from_nonempty_run"
