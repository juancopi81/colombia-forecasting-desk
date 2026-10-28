from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from colombia_forecasting_desk.entities import (
    build_watchlist_hits, load_watchlist, match_record, nit_check_digit,
    nit_variants, normalize_divipola, normalize_name, normalize_nit, render_watchlist_hits,
)


def entry(kind="company"):
    return {"watch_id": "example-company", "kind": kind, "label": "Example Company", "why": "Synthetic test", "identifiers": {"nit": "900123456", "names": ["Example Company SAS"]}}


def test_nit_explicit_dv_and_known_joined_alias_without_guessing():
    base = "900123456"
    dv = nit_check_digit(base)
    assert normalize_nit(f"900.123.456-{dv}") == base
    assert normalize_nit(base + dv) is None
    assert normalize_nit(base + dv, expected_base=base) == base
    assert normalize_nit(base + "-" + str((int(dv) + 1) % 10)) is None
    assert base + dv in nit_variants(base)
    assert normalize_nit("CC 900123456") is None


def test_names_and_divipola_preserve_unambiguous_structure():
    assert normalize_name("Ácme S.A.S.") == "acme"
    assert normalize_name("Example S.A. E.S.P. BIC") == "example"
    assert normalize_divipola("05001") == "05001"
    assert normalize_divipola(5001) == "05001"
    assert normalize_divipola("05") == "05"
    assert normalize_divipola("city 05") is None


def test_exact_identifiers_precede_name_candidates_and_cedula_is_not_company_nit():
    watch = entry()
    base = watch["identifiers"]["nit"]
    record = {"title": "Example Company SAS", "metadata": {"socrata_fields": {"documento_proveedor": base + nit_check_digit(base), "tipodocproveedor": "NIT"}}}
    assert match_record(watch, record) == "nit"
    record["metadata"]["socrata_fields"]["tipodocproveedor"] = "Cédula de Ciudadanía"
    assert match_record(watch, record) == "name_candidate"
    watch["kind"] = "public_entity"
    record["metadata"]["nit_entidad"] = base
    assert match_record(watch, record) == "nit"  # buyer identity is independent of supplier type


def test_legal_identity_includes_year_chamber_and_whole_decree_number():
    watch = entry("bill")
    watch["identifiers"] = {"bills": ["PL 012/2026 Cámara"]}
    assert match_record(watch, {"title": "Proyecto de Ley 12 de 2026 Cámara"}) == "bill"
    assert match_record(watch, {"title": "Proyecto de Ley 12 de 2026 Senado"}) is None
    watch["identifiers"] = {"decrees": ["Decreto 1599/2026"]}
    assert match_record(watch, {"title": "Decreto 1599 de 2026"}) == "decree"
    assert match_record(watch, {"title": "Decreto 15991 de 2026"}) is None


def test_place_codes_are_exact_and_names_are_candidates():
    watch = entry("place")
    watch["identifiers"] = {"divipola": ["05001"]}
    catalog = [{"cod_mpio": "05001", "nom_mpio": "Example Town", "cod_dpto": "05", "dpto": "Example Department"}]
    assert match_record(watch, {"metadata": {"cod_mpio": "05001"}}, divipola=catalog) == "divipola"
    assert match_record(watch, {"title": "Example Town has a new approved zone"}, divipola=catalog) == "name_candidate"


def test_public_entity_does_not_inherit_its_department_as_an_identity():
    watch = entry("public_entity")
    watch["identifiers"].update(names=["Departamento de Antioquia", "Gobernación de Antioquia"], divipola=["05"])
    catalog = [{"cod_mpio": "05001", "nom_mpio": "Medellín", "cod_dpto": "05", "dpto": "Antioquia"}]
    for record in ({"title": "Universidad de Antioquia"}, {"title": "Antioquia free-trade zone", "cod_dpto": "05"}):
        assert match_record(watch, record, divipola=catalog) is None
    assert match_record(watch, {"title": "Gobernación de Antioquia approves a project"}, divipola=catalog) == "name_candidate"
    place = deepcopy(watch)
    place.update(kind="place", identifiers={"divipola": ["05"]})
    assert match_record(place, {"title": "Universidad de Antioquia"}, divipola=catalog) == "name_candidate"


def test_watchlist_rejects_person_kinds_and_ambiguous_nit(tmp_path):
    path = tmp_path / "watchlist.yaml"
    path.write_text("entries: [{watch_id: example, kind: person, label: Example, why: Test, identifiers: {}}]")
    with pytest.raises(ValueError, match="kind"):
        load_watchlist(path)
    assert len(load_watchlist(Path("config/watchlist.yaml"))["entries"]) == 2


def test_matcher_reads_raw_zone_fragments_and_scrubs_procurement_text(tmp_path):
    run = tmp_path / "2026-10-27"; run.mkdir()
    cache = tmp_path / "cache"; cache.mkdir()
    watch = entry("topic"); watch["identifiers"] = {"keywords": ["synthetic place"]}
    rows = [
        {"id": "zone-a", "source_id": "mincit_zonas_francas", "title": "Synthetic Place zone A", "url": "https://example.gov.co/zones.pdf#zf-1"},
        {"id": "zone-b", "source_id": "mincit_zonas_francas", "title": "Synthetic Place zone B", "url": "https://example.gov.co/zones.pdf#zf-2"},
        {"id": "contract", "source_id": "secop_ii_contratos", "title": "private-identity-token in synthetic place", "url": "https://example.gov.co/contract", "metadata": {"documento_proveedor": "12345678", "tipodocproveedor": "CC"}},
    ]
    (run / "raw_items.json").write_text(json.dumps(rows))
    result = build_watchlist_hits(run, {"entries": [watch]}, cache_dir=cache)
    assert len(result["hits"]) == 3
    assert "private-identity-token" not in json.dumps(result)
    assert "12345678" not in json.dumps(result)
    assert "No new matching records" not in render_watchlist_hits(result)


def test_future_history_is_excluded_and_missing_sources_are_visible(tmp_path):
    run = tmp_path / "2026-10-23"; run.mkdir()
    cache = tmp_path / "cache"; cache.mkdir()
    history = {"as_of": "2026-10-27", "fetched_at": "2026-10-27T17:00:00Z", "patterns": [{"secret": "future"}]}
    (cache / "example-company.json").write_text(json.dumps(history))
    result = build_watchlist_hits(run, {"entries": [entry()]}, cache_dir=cache)
    assert result["patterns"] == []
    assert any("later history" in text for text in result["coverage_gaps"])
    assert any("raw_items.json unavailable" in text for text in result["coverage_gaps"])


def test_prior_reported_urls_are_suppressed_without_reading_same_day_or_future(tmp_path):
    cache = tmp_path / "cache"; cache.mkdir()
    watch = entry("topic"); watch["identifiers"] = {"keywords": ["example"]}
    old = {"schema_version": "watchlist_hits.v1", "run_date": "2026-10-26", "hits": [{"watch_id": watch["watch_id"], "kind": "topic", "source_id": "official_record", "title": "Example old event", "url": "https://example.gov.co/event#old", "summary": "Previously reported", "matched_on": "keyword_candidate"}], "patterns": []}
    previous = tmp_path / old["run_date"]; previous.mkdir()
    (previous / "watchlist_hits.json").write_text(json.dumps(old))
    run = tmp_path / "2026-10-27"; run.mkdir()
    rows = [{"title": "Example event", "url": "https://example.gov.co/event#" + fragment} for fragment in ("old", "new")]
    (run / "raw_items.json").write_text(json.dumps(rows))
    future = tmp_path / "2026-10-28"; future.mkdir()
    (future / "watchlist_hits.json").write_text("invalid future report must not be read")
    result = build_watchlist_hits(run, {"entries": [watch]}, cache_dir=cache)
    assert [hit["url"] for hit in result["hits"]] == [rows[1]["url"]]
    assert result["previously_reported_hits"] == 1
    (run / "watchlist_hits.json").write_text(json.dumps(result))
    assert build_watchlist_hits(run, {"entries": [watch]}, cache_dir=cache) == result
    (previous / "watchlist_hits.json").write_text("corrupt")
    with pytest.raises(ValueError):
        build_watchlist_hits(run, {"entries": [watch]}, cache_dir=cache)


def test_unchanged_history_pattern_is_not_reported_again_but_changed_numbers_are(tmp_path):
    cache = tmp_path / "cache"; cache.mkdir()
    run = tmp_path / "2026-10-27"; run.mkdir()
    pattern = {"watch_id": "example-company", "pattern_id": "repeat", "kind": "repeat_direct_award", "plain_text": "Example organization received twelve direct awards.", "caveat": "This is a prompt for review.", "numbers": {"contracts": 12}, "evidence_urls": ["https://example.gov.co/awards"]}
    previous = tmp_path / "2026-10-26"; previous.mkdir()
    (previous / "watchlist_hits.json").write_text(json.dumps({"schema_version": "watchlist_hits.v1", "run_date": previous.name, "hits": [], "patterns": [pattern]}))
    history = {"as_of": run.name, "fetched_at": run.name + "T16:00:00Z", "patterns": [pattern]}
    path = cache / "example-company.json"; path.write_text(json.dumps(history))
    result = build_watchlist_hits(run, {"entries": [entry()]}, cache_dir=cache)
    assert result["patterns"] == [] and result["previously_reported_patterns"] == 1
    history["patterns"][0]["numbers"]["contracts"] = 13
    path.write_text(json.dumps(history))
    assert len(build_watchlist_hits(run, {"entries": [entry()]}, cache_dir=cache)["patterns"]) == 1


def test_watchlist_plain_language_is_validated_and_codes_expand_only_in_visible_text(tmp_path):
    run = tmp_path / "2026-10-27"; run.mkdir()
    result = build_watchlist_hits(run, {"entries": [entry()]}, cache_dir=tmp_path)
    result["coverage_gaps"] = ["jbjy-vk9h: supplier coverage is partial; see https://www.datos.gov.co/resource/jbjy-vk9h.json"]
    rendered = render_watchlist_hits(result)
    assert "SECOP II contracts: supplier coverage" in rendered
    assert "https://www.datos.gov.co/resource/jbjy-vk9h.json" in rendered
    result["coverage_gaps"] = ["M3 packet is unavailable"]
    with pytest.raises(ValueError, match="internal token"):
        render_watchlist_hits(result)
