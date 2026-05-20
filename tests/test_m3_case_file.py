from __future__ import annotations

from colombia_forecasting_desk.m3_case_file import (
    extract_m3_case_file,
    validate_evidence_pack_markdown,
    validate_m3_case_file,
)


def _valid_case_file(**overrides) -> dict:
    base = {
        "schema_version": "m3_case_file.v1",
        "question": (
            "Will Proyecto de Ley 372 de 2026 Senado advance beyond "
            "`PENDIENTE DE ENVIAR A COMISION` by 2026-06-30?"
        ),
        "resolution_source": (
            "Senado registry, Senado agenda, or Gacetas del Congreso."
        ),
        "resolution_criteria": [
            "Resolve YES if an official record shows the bill sent to commission.",
            "Resolve NO if no qualifying official movement appears by the check window.",
        ],
        "deadline_or_window": "2026-06-30, with check window through 2026-07-03.",
        "source_excerpts": [
            {
                "source_id": "senado_leyes_registry",
                "source_name": "Senado - Seccion de Leyes",
                "url": "https://leyes.senado.gov.co/api/get_detalle_pdly.php?id=9915",
                "date": "2026-05-12",
                "excerpt": (
                    "Proyecto de Ley 372 de 2026 Senado. Estado: "
                    "PENDIENTE DE ENVIAR A COMISION."
                ),
            }
        ],
        "missing_evidence": [
            "Whether the next Senado agenda after 2026-05-15 lists the bill."
        ],
        "duplicate_check": {
            "status": "no_active_duplicate",
            "matched_forecast_ids": [],
            "notes": "No active forecast exists for this project.",
        },
        "m3_gate": "ready_for_m3",
        "gate_reason": "Question has official identity, deadline, and resolution path.",
        "artifact_refs": [
            {
                "artifact": "m2_review_packet.json",
                "key": "packet_item_id",
                "value": "m2pkt_soat",
            }
        ],
    }
    base.update(overrides)
    return base


def _pack_markdown(case_yaml: str) -> str:
    return f"""# Evidence Pack - Senado SOAT Advancement

## M3 Case File

```yaml
{case_yaml}
```

## Relevant Evidence

- Source-backed evidence follows the readiness case file.
"""


def test_validate_ready_m3_case_file_accepts_soat_style_case() -> None:
    issues = validate_m3_case_file(_valid_case_file())

    assert issues == []


def test_ready_m3_case_file_rejects_missing_resolution_and_deadline() -> None:
    case_file = _valid_case_file(
        resolution_source="",
        deadline_or_window="",
        m3_gate="ready_for_m3",
    )

    issues = validate_m3_case_file(case_file)
    codes = {issue.code for issue in issues}

    assert "ready_missing_resolution_source" in codes
    assert "ready_missing_deadline_or_window" in codes


def test_research_more_can_hold_missing_resolution_details() -> None:
    case_file = _valid_case_file(
        resolution_source="",
        deadline_or_window="",
        m3_gate="research_more",
        missing_evidence=[
            "Official resolution source and deadline are not known yet."
        ],
        duplicate_check={"status": "not_checked", "matched_forecast_ids": []},
    )

    assert validate_m3_case_file(case_file) == []


def test_ready_m3_case_file_requires_clear_duplicate_check() -> None:
    case_file = _valid_case_file(
        duplicate_check={
            "status": "possible_duplicate",
            "matched_forecast_ids": ["fcst_20260515_senado_soat_bill_advancement"],
        }
    )

    issues = validate_m3_case_file(case_file)

    assert any(issue.code == "ready_duplicate_check_not_clear" for issue in issues)


def test_extract_and_validate_case_file_from_evidence_pack_markdown() -> None:
    markdown = _pack_markdown(
        """
schema_version: m3_case_file.v1
question: Will Proyecto de Ley 372 de 2026 Senado advance by 2026-06-30?
resolution_source: Senado registry and Gacetas del Congreso.
resolution_criteria:
  - Resolve YES on official movement beyond the current registry status.
deadline_or_window: 2026-06-30, check through 2026-07-03.
source_excerpts:
  - source_id: senado_leyes_registry
    url: https://leyes.senado.gov.co/api/get_detalle_pdly.php?id=9915
    excerpt: Proyecto de Ley 372 de 2026 Senado. Estado pendiente.
missing_evidence: []
duplicate_check:
  status: no_active_duplicate
  matched_forecast_ids: []
m3_gate: ready_for_m3
gate_reason: Ready for evidence-pack probability work.
""".strip()
    )

    case_file = extract_m3_case_file(markdown)

    assert case_file is not None
    assert case_file["schema_version"] == "m3_case_file.v1"
    assert validate_evidence_pack_markdown(markdown) == []


def test_legacy_evidence_pack_shape_requires_m3_case_file_first() -> None:
    legacy_pack = """# Evidence Pack - GLP San Andres Bill Advancement

## Candidate Question

Will the GLP transport subsidy bill advance by 2026-06-30?

## Candidate Resolution Criteria

- Resolve YES on official movement.
"""

    issues = validate_evidence_pack_markdown(legacy_pack)
    codes = {issue.code for issue in issues}

    assert "m3_case_file_not_first" in codes
    assert "missing_m3_case_file" in codes
