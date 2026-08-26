from __future__ import annotations

from colombia_forecasting_desk.court_rulings import (
    court_text_signals,
    is_written_court_ruling_url,
)


def test_court_text_signals_detects_slash_year_references_and_is_repeatable() -> None:
    text = (
        "Sentencia C-259/26. La Corte ordeno corregir la implementacion "
        "dentro de treinta dias."
    )

    first = court_text_signals(text)
    second = court_text_signals(text)

    assert first == second
    assert first["decision_references"] == ["Sentencia C-259/26"]
    assert first["implementation_or_correction_signal"] is True
    assert first["clock_language_signal"] is True


def test_written_ruling_url_excludes_communication_pdf() -> None:
    assert is_written_court_ruling_url(
        "https://www.corteconstitucional.gov.co/relatoria/2026/C-259-26.htm"
    )
    assert not is_written_court_ruling_url(
        "https://www.corteconstitucional.gov.co/comunicados/26.pdf"
    )
