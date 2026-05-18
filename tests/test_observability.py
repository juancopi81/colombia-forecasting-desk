from __future__ import annotations

import pytest

from colombia_forecasting_desk.observability import RunTrace


def test_run_trace_records_successful_span_counts_and_metadata() -> None:
    trace = RunTrace(
        run_date="2026-05-18",
        mode="daily",
        metadata={"config_path": "config/metasources.yaml", "empty": None},
    )

    with trace.span(
        "fetch_sources",
        category="source_fetch",
        metadata={"source_count": 2, "ignored": None},
    ) as span:
        span.set_counts(raw_items=10, skipped=None)
        span.set_metadata(status="ok", ignored=None)

    payload = trace.to_dict()

    assert payload["schema_version"] == "run_trace.v1"
    assert payload["run_date"] == "2026-05-18"
    assert payload["mode"] == "daily"
    assert payload["metadata"] == {"config_path": "config/metasources.yaml"}
    assert len(payload["events"]) == 1
    event = payload["events"][0]
    assert event["name"] == "fetch_sources"
    assert event["category"] == "source_fetch"
    assert event["status"] == "ok"
    assert event["counts"] == {"raw_items": 10}
    assert event["metadata"] == {"source_count": 2, "status": "ok"}
    assert event["duration_ms"] >= 0


def test_run_trace_records_failed_span_without_swallowing_error() -> None:
    trace = RunTrace(run_date="2026-05-18", mode="daily")

    with pytest.raises(RuntimeError, match="fetch failed"):
        with trace.span("fetch_source", category="source_fetch"):
            raise RuntimeError("fetch failed")

    payload = trace.to_dict()
    event = payload["events"][0]
    assert event["name"] == "fetch_source"
    assert event["status"] == "error"
    assert event["error_class"] == "RuntimeError"
    assert event["error_message"] == "fetch failed"
