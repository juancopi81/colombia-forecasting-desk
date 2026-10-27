from copy import deepcopy
import json
import hashlib
from pathlib import Path

import pytest
import yaml

from colombia_forecasting_desk.cases import ARTIFACTS, build_case_updates, load_cases, match_case, snapshot_cases

FIXTURE = Path(__file__).parent / "fixtures/shared_interfaces/case_file.valid.json"


def case():
    return json.loads(FIXTURE.read_text())


def write_cases(tmp_path, value):
    folder = tmp_path / "cases"
    folder.mkdir(exist_ok=True)
    (folder / f"{value['case_id']}.yaml").write_text(yaml.safe_dump(value))
    return folder


def make_run(tmp_path, day="2026-10-27"):
    folder = tmp_path / day
    folder.mkdir()
    for name, key in ARTIFACTS.items():
        (folder / name).write_text(json.dumps({key: []} if key else []))
    return folder


def test_structured_bill_identity_matches_only_the_correct_year_and_chamber():
    value = case()
    value["identifiers"].update(bills=["PL 012/2026 Cámara"], indicator_ids=[], keywords=[])
    record = {"origin_project": {"number": "12", "year": "2026", "chamber": "camara"}}
    assert match_case(value, record) == ["bill:PL 012/2026 Cámara"]
    record["origin_project"]["chamber"] = "senado"
    assert not match_case(value, record)
    record["origin_project"].update(chamber="camara", year="2025")
    assert not match_case(value, record)


def test_number_substrings_do_not_match_other_decrees():
    value = case()
    value["identifiers"].update(decrees=["Decreto 1599/2026"], indicator_ids=[], keywords=[])
    assert not match_case(value, {"title": "Decreto 15991 de 2026"})
    assert match_case(value, {"title": "DECRETO NÚMERO 1599 DE 2026"})


def test_candidate_evidence_is_official_and_does_not_edit_the_story(tmp_path):
    value = case()
    original = deepcopy(value)
    folder = make_run(tmp_path)
    (folder / "indicator_watch.json").write_text(json.dumps([
        {"indicator_id": "ipc_inflation", "headline": "Inflation increased.", "source_url": "https://www.dane.gov.co/ipc"},
        {"indicator_id": "ipc_inflation", "headline": "A newspaper claim.", "source_url": "https://news.example.com/ipc"},
    ]))
    updates = build_case_updates(folder, [value])
    candidates = updates["cases"][0]["candidates"]
    assert len(candidates) == 1
    assert candidates[0]["artifact"] == "indicator_watch.json"
    assert candidates[0]["source_url"] == "https://www.dane.gov.co/ipc"
    assert value == original


def test_parking_is_only_suggested_after_the_wait_and_with_complete_inputs(tmp_path):
    value = case()
    value["current_view"]["date"] = "2026-10-01"
    value["updates"] = []
    folder = make_run(tmp_path)
    assert build_case_updates(folder, [value])["cases"][0]["suggested_status"] == "parked"
    (folder / "indicator_watch.json").unlink()
    result = build_case_updates(folder, [value])
    assert result["coverage_gaps"]
    assert result["cases"][0]["suggested_status"] == "active"
    assert value["status"] == "active"


def test_snapshot_is_frozen_idempotent_and_rejects_future_knowledge(tmp_path):
    value = case()
    folder = write_cases(tmp_path, value)
    run = tmp_path / "2026-10-27"
    snapshot = snapshot_cases(folder, run)
    assert snapshot_cases(folder, run) == snapshot
    assert json.loads((snapshot / "manifest.json").read_text())["files"]
    before = (snapshot / f"{value['case_id']}.yaml").read_bytes()
    value["current_view"]["text"] = "New information is available."
    write_cases(tmp_path, value)
    with pytest.raises(ValueError, match="frozen"):
        snapshot_cases(folder, run)
    assert (snapshot / f"{value['case_id']}.yaml").read_bytes() == before
    with pytest.raises(ValueError, match="later knowledge"):
        snapshot_cases(folder, tmp_path / "2026-10-23")


def test_all_seed_cases_validate():
    values = load_cases(Path(__file__).parent.parent / "cases")
    assert len(values) == 8
    assert all(value["updates"][0]["source_url"].startswith("https://") for value in values)


def test_verified_official_followup_keeps_a_case_visible_when_ticker_omits_it(tmp_path):
    value = case()
    value["identifiers"].update(keywords=["ultraprocesados"], indicator_ids=[])
    folder = make_run(tmp_path)
    sources = folder / "sources"
    sources.mkdir()
    raw = b"<html><title>ULTRAPROCESADOS</title><main>Official bill record.</main></html>"
    (sources / "official.html").write_bytes(raw)
    receipt = {"status": 200, "tls_verification": True, "url": "https://www.camara.gov.co/ultraprocesados/", "source_id": "declared_resolver", "sha256": hashlib.sha256(raw).hexdigest(), "artifact": "runs/2026-10-27/sources/official.html"}
    (sources / "official_receipt.json").write_text(json.dumps(receipt))
    assert len(build_case_updates(folder, [value])["cases"][0]["candidates"]) == 1
    (sources / "official.html").write_text("Replaced content")
    result = build_case_updates(folder, [value])
    assert not result["cases"][0]["candidates"]
    assert result["coverage_gaps"]


def test_invalid_story_cannot_be_snapshotted(tmp_path):
    value = case()
    value["plain_summary"] = "M3 packet"
    folder = write_cases(tmp_path, value)
    with pytest.raises(ValueError, match="internal token"):
        snapshot_cases(folder, tmp_path / "2026-10-27")
    assert not (tmp_path / "2026-10-27/cases_snapshot").exists()
