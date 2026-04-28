from __future__ import annotations

from colombia_forecasting_desk.dedupe import canonicalize_url, dedupe


def test_canonicalize_strips_fragment_and_trailing_slash() -> None:
    assert canonicalize_url("https://Example.com/foo/#frag") == "https://example.com/foo"


def test_canonicalize_strips_utm_and_fbclid() -> None:
    url = "https://example.com/a?utm_source=x&fbclid=abc&id=42"
    assert canonicalize_url(url) == "https://example.com/a?id=42"


def test_canonicalize_root_keeps_slash() -> None:
    assert canonicalize_url("https://example.com/") == "https://example.com/"


def test_dedupe_keeps_official_over_media(make_cleaned) -> None:
    media = make_cleaned(
        id="m1",
        source_id="eltiempo",
        url="https://example.com/article?utm_source=x",
        trust_role="media_signal",
    )
    official = make_cleaned(
        id="o1",
        source_id="banrep",
        url="https://example.com/article",
        trust_role="official_signal",
    )
    out = dedupe([media, official])
    assert len(out) == 1
    assert out[0].source_id == "banrep"


def test_dedupe_keeps_first_when_same_trust(make_cleaned) -> None:
    a = make_cleaned(id="a", source_id="x", url="https://e.com/a")
    b = make_cleaned(id="b", source_id="y", url="https://e.com/a/")
    out = dedupe([a, b])
    assert len(out) == 1
    assert out[0].id == "a"


def test_dedupe_drops_within_source_same_title(make_cleaned) -> None:
    a = make_cleaned(id="a", source_id="x", url="https://e.com/a", title="Hola Mundo")
    b = make_cleaned(id="b", source_id="x", url="https://e.com/b", title="hola mundo")
    out = dedupe([a, b])
    assert len(out) == 1


def test_dedupe_keeps_different_sources_same_title(make_cleaned) -> None:
    a = make_cleaned(id="a", source_id="x", url="https://e1.com/a", title="Hola")
    b = make_cleaned(id="b", source_id="y", url="https://e2.com/b", title="Hola")
    out = dedupe([a, b])
    assert len(out) == 2
