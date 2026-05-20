from __future__ import annotations

from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "validate_m3_case_file.py"


def _write_valid_pack(path: Path) -> None:
    path.write_text(
        """# Evidence Pack - DANE ICOCED

## M3 Case File

```yaml
schema_version: m3_case_file.v1
question: Will the next DANE ICOCED release stay above IPC?
resolution_source: DANE ICOCED and DANE IPC official releases.
resolution_criteria:
  - Resolve YES if annual ICOCED is above annual IPC.
deadline_or_window: 2026-06-05
source_excerpts:
  - source_id: dane_icoced
    url: https://www.dane.gov.co/icoced
    excerpt: ICOCED annual variation was 6.33%.
missing_evidence: []
duplicate_check:
  status: no_active_duplicate
  matched_forecast_ids: []
m3_gate: ready_for_m3
gate_reason: Official numeric sources and deadline are present.
```

## Relevant Evidence

Evidence body follows.
""",
        encoding="utf-8",
    )


def _run_cli(*paths: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *(str(path) for path in paths)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_validate_m3_case_file_cli_accepts_valid_pack(tmp_path: Path) -> None:
    pack = tmp_path / "icoced.md"
    _write_valid_pack(pack)

    result = _run_cli(pack)

    assert result.returncode == 0
    assert str(pack) in result.stdout
    assert "gate: ready_for_m3" in result.stdout
    assert "issues: 0" in result.stdout
    assert result.stderr == ""


def test_validate_m3_case_file_cli_rejects_legacy_pack(tmp_path: Path) -> None:
    pack = tmp_path / "legacy.md"
    pack.write_text(
        """# Evidence Pack - Legacy

## Forecast Question

Will the bill advance?
""",
        encoding="utf-8",
    )

    result = _run_cli(pack)

    assert result.returncode == 1
    assert "gate: missing" in result.stdout
    assert "m3_case_file_not_first" in result.stdout
    assert "missing_m3_case_file" in result.stdout
    assert result.stderr == ""
