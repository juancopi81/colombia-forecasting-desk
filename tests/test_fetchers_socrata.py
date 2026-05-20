from __future__ import annotations

from tests.fetcher_helpers import *  # noqa: F403


def test_parse_socrata_date_handles_iso_with_millis() -> None:
    assert _parse_socrata_date("2026-04-10T00:00:00.000") == "2026-04-10T00:00:00Z"
    assert _parse_socrata_date("2026-01-15T13:45:30.123") == "2026-01-15T13:45:30Z"


def test_parse_socrata_date_returns_none_on_garbage() -> None:
    assert _parse_socrata_date(None) is None
    assert _parse_socrata_date("") is None
    assert _parse_socrata_date("not-a-date") is None
    assert _parse_socrata_date(12345) is None
    # Calendar-invalid date.
    assert _parse_socrata_date("2026-02-31T00:00:00.000") is None


def test_socrata_params_compose_query() -> None:
    adapter = SocrataAdapter(
        date_field="fecha_de_publicacion_del",
        title_field="nombre_del_procedimiento",
        id_field="id_del_proceso",
        entity_field="entidad",
        label="SECOP II Proceso",
    )
    cutoff = datetime(2026, 4, 16, 0, 0, 0, tzinfo=timezone.utc)
    params = _socrata_params(adapter, cutoff=cutoff, limit=30)
    assert params["$where"] == (
        "fecha_de_publicacion_del >= '2026-04-16T00:00:00.000'"
    )
    assert params["$order"] == "fecha_de_publicacion_del DESC"
    assert params["$limit"] == "30"
    selected = set(params["$select"].split(","))
    assert selected == {
        "fecha_de_publicacion_del",
        "nombre_del_procedimiento",
        "id_del_proceso",
        "entidad",
    }


def test_socrata_row_to_item_synthesizes_url_and_title(sample_source) -> None:
    source = replace(
        sample_source,
        id="secop_ii_procesos",
        type="dataset",
        url="https://www.datos.gov.co/resource/p6dx-8zbt.json",
        trust_role="civic_signal",
    )
    adapter = SOCRATA_ADAPTERS["secop_ii_procesos"]
    row = {
        "fecha_de_publicacion_del": "2026-04-25T00:00:00.000",
        "nombre_del_procedimiento": "ADQUISICIÓN DE EQUIPOS DE COMPUTO",
        "id_del_proceso": "CO1.REQ.10337260",
        "entidad": "MUNICIPIO DE SUCRE",
    }
    item = _socrata_row_to_item(row, source, "2026-04-30T12:00:00Z", adapter)
    assert item is not None
    assert item.published_at == "2026-04-25T00:00:00Z"
    assert item.title.startswith("SECOP II Proceso — ADQUISICIÓN")
    assert "MUNICIPIO DE SUCRE" in item.title
    assert item.url == (
        "https://www.datos.gov.co/resource/p6dx-8zbt.json"
        "?id=CO1.REQ.10337260"
    )
    assert item.metadata["extraction"] == "socrata_api"
    assert item.metadata["id_value"] == "CO1.REQ.10337260"


def test_socrata_row_to_item_skips_rows_missing_required_fields(sample_source) -> None:
    source = replace(
        sample_source,
        id="secop_ii_procesos",
        type="dataset",
        url="https://www.datos.gov.co/resource/p6dx-8zbt.json",
    )
    adapter = SOCRATA_ADAPTERS["secop_ii_procesos"]
    # Missing date.
    assert _socrata_row_to_item(
        {
            "nombre_del_procedimiento": "X",
            "id_del_proceso": "abc",
        },
        source,
        "2026-04-30T12:00:00Z",
        adapter,
    ) is None
    # Missing title.
    assert _socrata_row_to_item(
        {
            "fecha_de_publicacion_del": "2026-04-25T00:00:00.000",
            "id_del_proceso": "abc",
        },
        source,
        "2026-04-30T12:00:00Z",
        adapter,
    ) is None
    # Missing id.
    assert _socrata_row_to_item(
        {
            "fecha_de_publicacion_del": "2026-04-25T00:00:00.000",
            "nombre_del_procedimiento": "X",
        },
        source,
        "2026-04-30T12:00:00Z",
        adapter,
    ) is None


def test_fetch_api_calls_socrata_with_expected_params(sample_source) -> None:
    source = replace(
        sample_source,
        id="secop_ii_procesos",
        type="dataset",
        url="https://www.datos.gov.co/resource/p6dx-8zbt.json",
        fetch_method="api",
        trust_role="civic_signal",
        max_items=5,
    )
    captured: dict = {}
    payload = [
        {
            "fecha_de_publicacion_del": "2026-04-28T00:00:00.000",
            "nombre_del_procedimiento": "PRESTACION DE SERVICIOS",
            "id_del_proceso": "CO1.REQ.111",
            "entidad": "ENT A",
        },
        {
            "fecha_de_publicacion_del": "2026-04-27T00:00:00.000",
            "nombre_del_procedimiento": "OBRA PUBLICA",
            "id_del_proceso": "CO1.REQ.222",
            "entidad": "ENT B",
        },
        # Duplicate id -> deduped.
        {
            "fecha_de_publicacion_del": "2026-04-26T00:00:00.000",
            "nombre_del_procedimiento": "PRESTACION DE SERVICIOS",
            "id_del_proceso": "CO1.REQ.111",
            "entidad": "ENT A",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        items = fetch_api(source, client)

    assert len(items) == 2
    assert items[0].title.startswith("SECOP II Proceso — PRESTACION DE SERVICIOS")
    assert items[1].title.startswith("SECOP II Proceso — OBRA PUBLICA")
    assert all(it.url.startswith(source.url + "?id=") for it in items)
    assert captured["params"]["$limit"] == "5"
    assert captured["params"]["$order"] == "fecha_de_publicacion_del DESC"
    assert captured["params"]["$where"].startswith(
        "fecha_de_publicacion_del >= '"
    )


def test_socrata_adapter_registry_covers_yaml_sources() -> None:
    """Every fetch_method=api source in the YAML must have an adapter."""
    from pathlib import Path

    import yaml

    config_path = Path(__file__).resolve().parents[1] / "config" / "metasources.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    enabled_api_ids = {
        entry["id"]
        for entry in raw["metasources"]
        if entry.get("enabled") and entry.get("fetch_method") == "api"
    }
    missing = enabled_api_ids - set(SOCRATA_ADAPTERS)
    assert not missing, (
        f"enabled api sources without SOCRATA_ADAPTERS entry: {sorted(missing)}"
    )
