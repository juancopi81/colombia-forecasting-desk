from copy import deepcopy
import json
from pathlib import Path

import pytest

from colombia_forecasting_desk.plain_language import validate_plain_text, word_count
from colombia_forecasting_desk.schemas import SCHEMAS, require_valid, validate_artifact

FIXTURES = Path(__file__).parent / "fixtures" / "shared_interfaces"


@pytest.mark.parametrize("schema", SCHEMAS)
def test_shared_valid_and_invalid_examples(schema):
    valid = json.loads((FIXTURES / f"{schema}.valid.json").read_text())
    invalid = json.loads((FIXTURES / f"{schema}.invalid.json").read_text())
    assert validate_artifact(valid) == []
    assert validate_artifact(invalid)
    require_valid(valid)
    with pytest.raises(ValueError):
        require_valid(invalid)


@pytest.mark.parametrize("payload", [None, [], "reader_brief.v1", {}, {"schema_version": []}])
def test_non_artifacts_are_rejected(payload):
    assert validate_artifact(payload)


def test_reader_call_or_skip_is_exclusive_and_probabilities_are_numbers():
    payload = json.loads((FIXTURES / "reader_brief.valid.json").read_text())
    payload["coming_up"][0]["our_call"] = {
        "probability": True, "forecast_id": "test", "outcome": "rates rise"
    }
    assert validate_artifact(payload)
    payload["coming_up"][0]["our_call"]["probability"] = 0.55
    payload["coming_up"][0]["no_call_reason"] = None
    assert not validate_artifact(payload)


def test_family_skip_cannot_hide_a_prediction():
    payload = json.loads((FIXTURES / "family_forecasts.valid.json").read_text())
    payload["entries"][0].update(decision="skip", skip_reason="No current release", probability=0.5)
    assert validate_artifact(payload)


def test_later_optional_fields_do_not_break_shared_consumers():
    for schema in SCHEMAS:
        payload = json.loads((FIXTURES / f"{schema}.valid.json").read_text())
        original = deepcopy(payload)
        payload["extension"] = {"source_hash": "example"}
        assert not validate_artifact(payload)
        assert original == {key: value for key, value in payload.items() if key != "extension"}


@pytest.mark.parametrize("bad", ["M3 is ready", "Use the shadow_track lane", "Decreto 1599", "x" * 30])
def test_internal_tokens_codes_and_squashed_text_are_rejected(bad):
    assert validate_plain_text(bad)


def test_reader_words_are_counted_without_markdown_targets():
    assert word_count("Read [the source](https://example.gov.co/a-very-long-url).") == 3
    assert not validate_plain_text("Read https://example.gov.co/" + "x" * 60)
    assert not validate_plain_text("The budget decree sets next year's spending ceiling for public services (Decreto 1599).")
