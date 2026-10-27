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
    assert "No matching records" not in render_watchlist_hits(result)


def test_future_history_is_excluded_and_missing_sources_are_visible(tmp_path):
    run = tmp_path / "2026-10-23"; run.mkdir()
    cache = tmp_path / "cache"; cache.mkdir()
    history = {"as_of": "2026-10-27", "fetched_at": "2026-10-27T17:00:00Z", "patterns": [{"secret": "future"}]}
    (cache / "example-company.json").write_text(json.dumps(history))
    result = build_watchlist_hits(run, {"entries": [entry()]}, cache_dir=cache)
    assert result["patterns"] == []
    assert any("later history" in text for text in result["coverage_gaps"])
    assert any("raw_items.json unavailable" in text for text in result["coverage_gaps"])
