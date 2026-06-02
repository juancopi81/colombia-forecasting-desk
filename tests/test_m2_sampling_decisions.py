from __future__ import annotations

import json
from pathlib import Path

import pytest

from colombia_forecasting_desk.m2_sampling_decisions import (
    MissingCandidateQuestionsError,
    build_m2_sampling_decisions,
    write_m2_sampling_decisions,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _ranked_row(**overrides) -> dict:
    row = {
        "rank_id": "m2leg_564",
        "canonical_bill_id": "bill:2026:camara:564",
        "display_title": "Proyecto de Ley 564 de 2026 Cámara - Subsidios Transparentes",
        "question_seed": (
            "Could Proyecto de Ley 564 de 2026 Cámara become a forecastable "
            "unresolved legislative decision?"
        ),
        "bucket": "watchlist",
        "recommendation": "monitor",
        "overall_score": 0.668,
        "heuristic_risk_flags": ["possible_false_negative_structurally_strong"],
        "missing_evidence": ["exact forecast deadline/window"],
        "source_ids": ["camara_proyectos_ley_registry"],
        "penalties": ["Decision window is unclear or latest movement is registry-only."],
        "score_reasons": ["Clean bill number, chamber, and year are present."],
    }
    row.update(overrides)
    return row


def test_build_sampling_decisions_parses_candidates_and_links_m2_rows(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "2026-06-02"
    run_dir.mkdir()
    (run_dir / "candidate_questions.md").write_text(
        """# Candidate Questions - 2026-06-02

## Decision Summary

Overall decision: `monitor_no_new_m3`.

## Candidates Reviewed

### 1. PL 564 Subsidios Transparentes

- Decision: `research_more_before_m3`
- Question considered: Will Proyecto de Ley 564 de 2026 Camara advance beyond
  committee-stage registry status within a dated official decision window?
- Resolution source: Camara registry, committee agenda, Gacetas del Congreso,
  Senado registry if transferred, and Diario Oficial.
- Deadline/window: Not ready. Need committee agenda, ponencia status, debate
  date, or transfer milestone.
- Evidence available now: Top M2 watchlist item; active `Tramite en Comision`
  status; public-resource/subsidy traceability hook.
- Missing evidence: Exact procedural deadline, current agenda or ponencia event.
- Why not M3: Strong public-interest hook, but no dated resolution criteria.

### 2. PL 560 GLP subsidy

- Decision: `research_more_before_m3`
- Question considered: Will Proyecto de Ley 560 de 2025 Camara advance?
- Resolution source: Camara/Senado registries and Gacetas del Congreso.
- Deadline/window: Needs official agenda date.
- Evidence available now: Clean official bill identity.
- Missing evidence: Current agenda.
- Why not M3: Possible duplicate of an active forecast-log item.
""",
        encoding="utf-8",
    )
    _write_json(
        run_dir / "m2_ranked_questions.json",
        {
            "schema_version": "m2_legislative_ranking.v1",
            "ranked_questions": [
                _ranked_row(),
                _ranked_row(
                    rank_id="m2leg_560",
                    canonical_bill_id="bill:2025:camara:560",
                    penalties=["Likely duplicate of an active forecast-log item."],
                ),
            ],
            "review_queue": [],
        },
    )

    artifact = build_m2_sampling_decisions(run_dir)

    assert artifact["schema_version"] == "m2_sampling_decisions.v1"
    assert artifact["overall_decision"] == "monitor_no_new_m3"
    assert artifact["candidate_count"] == 2
    assert artifact["decision_counts"] == {"research_more_before_m3": 2}

    first = artifact["candidates"][0]
    assert first["title"] == "PL 564 Subsidios Transparentes"
    assert first["decision"] == "research_more_before_m3"
    assert "dated official decision window" in first["question_considered"]
    assert first["canonical_bill_ids_detected"] == ["bill:2026:camara:564"]
    assert first["m2_ranked_refs"][0]["rank_id"] == "m2leg_564"
    assert (
        first["m2_ranked_refs"][0]["match_method"]
        == "canonical_bill_id_from_candidate_text"
    )
    assert first["duplicate_relationship"]["status"] == "not_recorded"
    assert "deadline_or_window" in first["missing_m3_fields"]
    assert "duplicate_check" in first["missing_m3_fields"]
    assert "### 1. PL 564 Subsidios Transparentes" in first["raw_markdown"]

    second = artifact["candidates"][1]
    assert second["m2_ranked_refs"][0]["rank_id"] == "m2leg_560"
    assert second["duplicate_relationship"]["status"] == "possible_duplicate"


def test_sampling_decisions_detect_existing_draft_lane_text(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-02"
    run_dir.mkdir()
    (run_dir / "candidate_questions.md").write_text(
        """# Candidate Questions - 2026-06-02

## Candidates Reviewed

### 1. ICOCED construction costs above IPC

- Decision: `monitor_no_new_m3`
- Question considered: Will the next DANE ICOCED release remain above the latest
  annual IPC rate?
- Resolution source: DANE ICOCED and DANE IPC.
- Deadline/window: Next DANE monthly release.
- Evidence available now: ICOCED annual variation 6.45% versus IPC 5.68%.
- Missing evidence: Release calendar.
- Why not M3: Adjacent to an existing draft lane and not the strongest
  public-interest candidate today.
""",
        encoding="utf-8",
    )

    artifact = build_m2_sampling_decisions(run_dir)

    duplicate = artifact["candidates"][0]["duplicate_relationship"]
    assert duplicate["status"] == "possible_duplicate"
    assert "adjacent draft" in duplicate["notes"]


def test_sampling_decisions_do_not_fuzzy_match_ranked_rows(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-02"
    run_dir.mkdir()
    (run_dir / "candidate_questions.md").write_text(
        """# Candidate Questions - 2026-06-02

## Candidates Reviewed

### Subsidios Transparentes bill

- Decision: `research_more_before_m3`
- Question considered: Will the subsidies bill advance?
- Resolution source: Camara registry.
- Deadline/window: Not ready.
- Evidence available now: Registry context.
- Missing evidence: Exact bill identity.
""",
        encoding="utf-8",
    )
    _write_json(
        run_dir / "m2_ranked_questions.json",
        {
            "schema_version": "m2_legislative_ranking.v1",
            "ranked_questions": [_ranked_row()],
            "review_queue": [],
        },
    )

    artifact = build_m2_sampling_decisions(run_dir)

    candidate = artifact["candidates"][0]
    assert candidate["canonical_bill_ids_detected"] == []
    assert candidate["m2_ranked_refs"] == []
    assert candidate["duplicate_relationship"]["status"] == "unknown"


def test_build_sampling_decisions_requires_candidate_questions(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "2026-06-02"
    run_dir.mkdir()

    with pytest.raises(MissingCandidateQuestionsError):
        build_m2_sampling_decisions(run_dir)


def test_write_sampling_decisions_writes_json_and_markdown(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-02"
    run_dir.mkdir()
    (run_dir / "candidate_questions.md").write_text(
        """# Candidate Questions - 2026-06-02

## Candidates Reviewed

### 1. TES auction funding-cost signal

- Decision: `monitor_no_new_m3`
- Question considered: Will the next official COP TES auction report show a
  maximum cutoff rate of at least 14.0%?
- Resolution source: MinHacienda / IRC TES auction reports.
- Deadline/window: Next official COP TES auction report.
- Evidence available now: Latest COP TES auction max cutoff was 14.79%.
- Missing evidence: Next auction date and tenor mix.
- Why not M3: Good calibration lead, but still needs tenor/calendar framing.
""",
        encoding="utf-8",
    )

    artifact, json_path, markdown_path = write_m2_sampling_decisions(run_dir)

    assert artifact["candidate_count"] == 1
    assert json_path.exists()
    assert markdown_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["candidate_count"] == 1
    rendered = markdown_path.read_text(encoding="utf-8")
    assert "# M2 Sampling Decisions - 2026-06-02" in rendered
    assert "TES auction funding-cost signal" in rendered
    assert "M2 ranked refs: none" in rendered
