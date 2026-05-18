from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_artifact_parity.py"
SPEC = importlib.util.spec_from_file_location("check_artifact_parity", SCRIPT_PATH)
assert SPEC and SPEC.loader
parity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(parity)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_artifact_parity_ignores_volatile_run_metadata(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    baseline.mkdir()
    candidate.mkdir()
    _write_json(
        baseline / "run_summary.json",
        {
            "run_date": "2026-05-18",
            "started_at": "2026-05-18T10:00:00Z",
            "finished_at": "2026-05-18T10:01:00Z",
            "raw_items": 1,
        },
    )
    _write_json(
        candidate / "run_summary.json",
        {
            "run_date": "2026-05-18",
            "started_at": "2026-05-18T11:00:00Z",
            "finished_at": "2026-05-18T11:01:00Z",
            "raw_items": 1,
        },
    )

    assert parity.check_parity(
        baseline, candidate, artifacts=("run_summary.json",)
    ) == []


def test_artifact_parity_ignores_volatile_run_trace_timing(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    baseline.mkdir()
    candidate.mkdir()
    common = {
        "schema_version": "run_trace.v1",
        "run_date": "2026-05-18",
        "mode": "daily",
        "events": [
            {
                "name": "fetch_sources",
                "category": "pipeline",
                "status": "ok",
                "counts": {"raw_items": 10},
            }
        ],
    }
    _write_json(
        baseline / "run_trace.json",
        {
            **common,
            "started_at": "2026-05-18T10:00:00Z",
            "finished_at": "2026-05-18T10:01:00Z",
            "duration_ms": 10.1,
            "events": [
                {
                    **common["events"][0],
                    "started_at": "2026-05-18T10:00:01Z",
                    "finished_at": "2026-05-18T10:00:02Z",
                    "duration_ms": 1.2,
                }
            ],
        },
    )
    _write_json(
        candidate / "run_trace.json",
        {
            **common,
            "started_at": "2026-05-18T11:00:00Z",
            "finished_at": "2026-05-18T11:01:00Z",
            "duration_ms": 20.2,
            "events": [
                {
                    **common["events"][0],
                    "started_at": "2026-05-18T11:00:01Z",
                    "finished_at": "2026-05-18T11:00:02Z",
                    "duration_ms": 2.3,
                }
            ],
        },
    )

    assert parity.check_parity(
        baseline, candidate, artifacts=("run_trace.json",)
    ) == []


def test_artifact_parity_reports_stable_content_drift(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    baseline.mkdir()
    candidate.mkdir()
    _write_json(baseline / "m1_candidates.json", {"candidates": [{"id": "a"}]})
    _write_json(candidate / "m1_candidates.json", {"candidates": [{"id": "b"}]})

    failures = parity.check_parity(
        baseline, candidate, artifacts=("m1_candidates.json",)
    )

    assert failures
    assert failures[0] == "m1_candidates.json: content differs"


def test_artifact_parity_allows_artifacts_missing_from_both_sides(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    baseline.mkdir()
    candidate.mkdir()

    assert parity.check_parity(
        baseline, candidate, artifacts=("m2_review_packet.json",)
    ) == []
