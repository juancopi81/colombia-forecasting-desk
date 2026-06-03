from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from colombia_forecasting_desk.m3_case_file import (
    ALLOWED_DUPLICATE_STATUSES,
    extract_m3_case_file,
    validate_evidence_pack_markdown,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = REPO_ROOT / "scripts" / "scaffold_m3_case_file.py"

_spec = importlib.util.spec_from_file_location("scaffold_m3_case_file", _SCRIPT)
assert _spec and _spec.loader
scaffold = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(scaffold)


def _candidate(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "index": 1,
        "title": "PL 564 Subsidios Transparentes",
        "decision": "research_more_before_m3",
        "question_considered": (
            "Will Proyecto de Ley 564 de 2026 Camara advance beyond committee "
            "stage within a dated official window?"
        ),
        "resolution_source": "Camara registry, committee agenda, Gacetas del Congreso.",
        "resolution_criteria": "not_recorded",
        "deadline_or_window": "Not ready. Need committee agenda or debate date.",
        "missing_evidence": "Exact procedural deadline and current agenda event.",
        "missing_m3_fields": ["resolution_criteria", "deadline_or_window", "duplicate_check"],
        "why_not_m3": "Strong public-interest hook, but no dated resolution criteria.",
        "evidence_available_now": "Top M2 watchlist item; active Tramite en Comision status.",
        "duplicate_relationship": {
            "status": "not_recorded",
            "notes": "Matched M2 row did not record a duplicate signal.",
            "source": "m2_ranked_questions.json",
        },
        "canonical_bill_ids_detected": ["bill:2026:camara:564"],
        "m2_ranked_refs": [
            {
                "artifact": "m2_ranked_questions.json",
                "rank_id": "m2leg_5812b57d7886",
                "canonical_bill_id": "bill:2026:camara:564",
                "display_title": "Proyecto de Ley 564 de 2026 Cámara - Subsidios Transparentes",
                "source_ids": ["camara_proyectos_ley_registry"],
                "heuristic_risk_flags": ["possible_false_negative_structurally_strong"],
                "missing_evidence": ["exact forecast deadline/window"],
            }
        ],
    }
    base.update(overrides)
    return base


def _sampling(*candidates: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "m2_sampling_decisions.v1",
        "run_date": "2026-06-03",
        "status": "recorded",
        "candidate_count": len(candidates),
        "candidates": list(candidates),
    }


def _write_sampling(run_dir: Path, payload: dict[str, Any]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "m2_sampling_decisions.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def test_scaffold_output_is_validator_clean_research_more(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-03"
    _write_sampling(run_dir, _sampling(_candidate()))

    code = scaffold.main(
        ["--date", "2026-06-03", "--candidate-index", "1", "--runs-dir", str(tmp_path)]
    )
    assert code == 0

    draft = run_dir / "evidence_packs" / "01_pl_564_subsidios_transparentes.draft.md"
    assert draft.is_file()
    text = draft.read_text(encoding="utf-8")

    case_file = extract_m3_case_file(text)
    assert case_file is not None
    assert case_file["schema_version"] == "m3_case_file.v1"
    assert case_file["m3_gate"] == "research_more"
    # The real contract validator must find zero issues.
    assert validate_evidence_pack_markdown(text) == []


def test_gate_never_ready_for_m3(tmp_path: Path) -> None:
    assert scaffold.gate_for_decision("monitor_no_new_m3") == "research_more"
    assert scaffold.gate_for_decision("select_for_evidence_pack") == "research_more"
    assert scaffold.gate_for_decision("totally_unknown") == "research_more"
    assert scaffold.gate_for_decision("reject") == "reject"

    case_file = scaffold.build_case_file(
        _candidate(decision="select_for_evidence_pack"), "2026-06-03"
    )
    assert case_file["m3_gate"] == "research_more"


def test_missing_evidence_never_empty_for_research_more(tmp_path: Path) -> None:
    candidate = _candidate(missing_evidence="", missing_m3_fields=[], m2_ranked_refs=[])
    case_file = scaffold.build_case_file(candidate, "2026-06-03")
    assert case_file["missing_evidence"]  # non-empty -> research_more stays valid
    text = scaffold.build_scaffold_markdown(candidate, "2026-06-03")
    assert validate_evidence_pack_markdown(text) == []


@pytest.mark.parametrize(
    "raw_status", ["unknown", "not_recorded", "possible_duplicate", "duplicate", "weird"]
)
def test_duplicate_status_mapped_into_allowed_set(raw_status: str) -> None:
    candidate = _candidate(duplicate_relationship={"status": raw_status, "notes": "x"})
    status = scaffold.build_case_file(candidate, "2026-06-03")["duplicate_check"]["status"]
    assert status in ALLOWED_DUPLICATE_STATUSES


def test_unknown_duplicate_status_becomes_not_checked() -> None:
    candidate = _candidate(duplicate_relationship={"status": "unknown"})
    case_file = scaffold.build_case_file(candidate, "2026-06-03")
    # Never silently claims no_active_duplicate (which could unlock ready_for_m3).
    assert case_file["duplicate_check"]["status"] == "not_checked"


def test_refuses_to_overwrite_existing_draft_and_preserves_hand_authored_pack(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "2026-06-03"
    _write_sampling(run_dir, _sampling(_candidate()))
    packs = run_dir / "evidence_packs"
    packs.mkdir(parents=True)
    human = packs / "pl_564_subsidios_transparentes.md"
    human.write_text("# human pack\n", encoding="utf-8")

    args = ["--date", "2026-06-03", "--candidate-index", "1", "--runs-dir", str(tmp_path)]
    assert scaffold.main(args) == 0
    draft = packs / "01_pl_564_subsidios_transparentes.draft.md"
    assert draft.is_file()
    original = draft.read_text(encoding="utf-8")
    # The hand-authored .md is a different path and is left untouched.
    assert human.read_text(encoding="utf-8") == "# human pack\n"

    # Re-running refuses to clobber the existing draft (no --force escape hatch).
    assert scaffold.main(args) == 1
    assert draft.read_text(encoding="utf-8") == original


def test_force_flag_is_not_supported(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-03"
    _write_sampling(run_dir, _sampling(_candidate()))
    with pytest.raises(SystemExit):
        scaffold.main(
            [
                "--date",
                "2026-06-03",
                "--candidate-index",
                "1",
                "--runs-dir",
                str(tmp_path),
                "--force",
            ]
        )


def test_select_by_rank_id(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-03"
    other = _candidate(
        index=2,
        title="TES auction signal",
        m2_ranked_refs=[{"rank_id": "m2leg_zzz", "source_ids": []}],
    )
    _write_sampling(run_dir, _sampling(_candidate(), other))

    code = scaffold.main(
        ["--date", "2026-06-03", "--rank-id", "m2leg_zzz", "--runs-dir", str(tmp_path)]
    )
    assert code == 0
    assert (run_dir / "evidence_packs" / "02_tes_auction_signal.draft.md").is_file()


def test_select_candidate_raises_on_ambiguous_rank_id() -> None:
    shared = "m2leg_dup"
    sampling = _sampling(
        _candidate(index=1, m2_ranked_refs=[{"rank_id": shared, "source_ids": []}]),
        _candidate(
            index=2, title="Second", m2_ranked_refs=[{"rank_id": shared, "source_ids": []}]
        ),
    )
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.select_candidate(sampling, rank_id=shared)


def test_rank_id_ambiguity_fails_via_cli_and_writes_nothing(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-03"
    shared = "m2leg_dup"
    _write_sampling(
        run_dir,
        _sampling(
            _candidate(index=1, m2_ranked_refs=[{"rank_id": shared, "source_ids": []}]),
            _candidate(
                index=2, title="Second", m2_ranked_refs=[{"rank_id": shared, "source_ids": []}]
            ),
        ),
    )
    code = scaffold.main(
        ["--date", "2026-06-03", "--rank-id", shared, "--runs-dir", str(tmp_path)]
    )
    assert code == 1
    assert not (run_dir / "evidence_packs").exists()


def test_list_mode_prints_candidates(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run_dir = tmp_path / "2026-06-03"
    _write_sampling(run_dir, _sampling(_candidate()))

    code = scaffold.main(["--date", "2026-06-03", "--list", "--runs-dir", str(tmp_path)])
    assert code == 0
    out = capsys.readouterr().out
    assert "PL 564 Subsidios Transparentes" in out
    assert "research_more" in out


def test_requires_exactly_one_selector(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-06-03"
    _write_sampling(run_dir, _sampling(_candidate()))
    base = ["--date", "2026-06-03", "--runs-dir", str(tmp_path)]
    assert scaffold.main(base) == 1  # no selector
    assert scaffold.main(base + ["--candidate-index", "1", "--rank-id", "x"]) == 1  # both


def test_missing_sampling_artifact_errors(tmp_path: Path) -> None:
    code = scaffold.main(
        ["--date", "2026-06-03", "--candidate-index", "1", "--runs-dir", str(tmp_path)]
    )
    assert code == 1


def test_output_is_deterministic_and_draft_suffixed(tmp_path: Path) -> None:
    candidate = _candidate()
    first = scaffold.build_scaffold_markdown(candidate, "2026-06-03")
    second = scaffold.build_scaffold_markdown(candidate, "2026-06-03")
    assert first == second
    assert scaffold.scaffold_path(tmp_path, candidate).name.endswith(".draft.md")
