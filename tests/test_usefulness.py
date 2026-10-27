from datetime import date, timedelta
import importlib.util
import json
from pathlib import Path
import pytest

from scripts.finalize_reader_brief import finalize

SCRIPT = Path(__file__).parent.parent / "scripts/check_usefulness.py"
spec = importlib.util.spec_from_file_location("usefulness_check", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_ten_run_check_counts_valid_weekdays_and_excludes_later_knowledge(tmp_path):
    first = date(2026, 10, 27)
    days = [first + timedelta(days=n) for n in range(20) if (first + timedelta(days=n)).weekday() < 5][:11]
    for day in days:
        folder = tmp_path / day.isoformat()
        folder.mkdir()
        brief = {"schema_version": "reader_brief.v1", "run_date": day.isoformat(),
                 "needs_you": {"required": False, "action": None, "by": None},
                 "headline": "Nothing needs you today.", "changes": [], "coming_up": [],
                 "open_forecasts": [], "data_problems": []}
        (folder / "reader_brief.json").write_text(json.dumps(brief))
        (folder / "acceptance_report.json").write_text(json.dumps({"status": "pass", "strict_pass": True}))
        finalize(run_date=day.isoformat(), runs_dir=tmp_path)
    assert module.check_usefulness(tmp_path, days[8].isoformat())["status"] == "collecting"
    result = module.check_usefulness(tmp_path, days[9].isoformat())
    assert result["status"] == "review_due"
    assert result["decision_grade_reader_runs"] == 10
    assert days[10].isoformat() not in result["run_dates"]
    (tmp_path / days[0].isoformat() / "acceptance_report.json").write_text('{"status":"fail","strict_pass":false}')
    assert module.check_usefulness(tmp_path, days[9].isoformat())["status"] == "collecting"


def test_an_unrenderable_brief_does_not_count_as_a_useful_run(tmp_path):
    folder = tmp_path / "2026-10-27"
    folder.mkdir()
    (folder / "reader_brief.json").write_text('{"schema_version":"reader_brief.v1"}')
    (folder / "acceptance_report.json").write_text('{"status":"pass","strict_pass":true}')
    assert module.check_usefulness(tmp_path, "2026-10-27")["decision_grade_reader_runs"] == 0


def test_failed_provenance_and_edited_outputs_cannot_use_an_old_receipt(tmp_path):
    folder = tmp_path / "2026-10-27"
    folder.mkdir()
    brief = json.loads((Path(__file__).parent / "fixtures/shared_interfaces/reader_brief.valid.json").read_text())
    brief.update(changes=[], coming_up=[], open_forecasts=[], data_problems=[])
    (folder / "reader_brief.json").write_text(json.dumps(brief))
    (folder / "acceptance_report.json").write_text('{"status":"pass","strict_pass":true}')
    finalize(run_date="2026-10-27", runs_dir=tmp_path)
    assert module.check_usefulness(tmp_path, "2026-10-27")["decision_grade_reader_runs"] == 1
    original = (folder / "reader_brief.json").read_bytes()
    brief["changes"] = [{"title": "Unverified change", "what_happened": "An unsupported event appeared.",
                         "why_it_matters": "It could matter.", "who_is_affected": "Readers.",
                         "confidence": "confirmed", "source_url": "https://uncaptured.gov.co/event"}]
    (folder / "reader_brief.json").write_text(json.dumps(brief))
    with pytest.raises(ValueError, match="not captured"):
        finalize(run_date="2026-10-27", runs_dir=tmp_path)
    assert module.check_usefulness(tmp_path, "2026-10-27")["decision_grade_reader_runs"] == 0
    (folder / "reader_brief.json").write_bytes(original)
    (folder / "reader_brief.md").write_text("An edited but unvalidated output")
    assert module.check_usefulness(tmp_path, "2026-10-27")["decision_grade_reader_runs"] == 0
