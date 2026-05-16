from __future__ import annotations

from colombia_forecasting_desk.cluster import (
    cluster,
    jaccard,
    tokenize_title,
    topic_keywords,
)


def test_tokenize_drops_stopwords_and_short_tokens() -> None:
    tokens = tokenize_title("La junta del Banco de la República mantiene la tasa")
    assert "junta" in tokens
    assert "banco" in tokens
    assert "republica" in tokens
    assert "tasa" in tokens
    # stopwords and short tokens removed
    for sw in ("la", "del", "de"):
        assert sw not in tokens


def test_jaccard_basic() -> None:
    assert jaccard({"a", "b"}, {"a", "b"}) == 1.0
    assert jaccard({"a", "b"}, {"c"}) == 0.0
    assert jaccard(set(), set()) == 0.0


def test_clusters_near_identical_titles(make_cleaned) -> None:
    a = make_cleaned(
        id="a",
        source_id="banrep",
        title="Banco de la República mantiene la tasa de interés en 9.5%",
    )
    b = make_cleaned(
        id="b",
        source_id="eltiempo",
        title="BanRep mantiene tasa de interés en 9.5%, dice junta del Banco República",
    )
    c = make_cleaned(
        id="c",
        source_id="dane",
        title="DANE publica cifras de inflación de marzo",
    )
    clusters = cluster([a, b, c])
    sizes = sorted(len(cl.items) for cl in clusters)
    assert sizes == [1, 2]


def test_clusters_unrelated_stay_separate(make_cleaned) -> None:
    a = make_cleaned(id="a", title="Inflación de marzo bajó al 5.2%")
    b = make_cleaned(id="b", title="Senado aprueba reforma pensional")
    clusters = cluster([a, b])
    assert len(clusters) == 2


def test_generic_imprenta_editions_do_not_cluster_by_shared_boilerplate(
    make_cleaned,
) -> None:
    a = make_cleaned(
        id="a",
        source_id="gacetas_congreso",
        source_type="legal",
        title="Gaceta del Congreso 398 — Senado de la República",
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )
    b = make_cleaned(
        id="b",
        source_id="gacetas_congreso",
        source_type="legal",
        title="Gaceta del Congreso 399 — Senado de la República",
        detected_entities=["congreso"],
        detected_topics=["legislative"],
    )

    clusters = cluster([a, b])

    assert len(clusters) == 2


def test_cluster_aggregates_detected_tags(make_cleaned) -> None:
    a = make_cleaned(
        id="a",
        title="Inflación marzo cifras DANE",
        detected_entities=["dane"],
        detected_topics=["inflation"],
    )
    b = make_cleaned(
        id="b",
        title="Inflación marzo cifras BanRep",
        detected_entities=["banrep"],
        detected_topics=["monetary_policy", "inflation"],
    )

    [grouped] = cluster([a, b])

    assert grouped.detected_entities == ["banrep", "dane"]
    assert grouped.detected_topics == ["monetary_policy", "inflation"]


def test_cluster_preserves_member_metadata(make_cleaned) -> None:
    item = make_cleaned(
        id="senado-1",
        source_id="senado_agenda_legislativa",
        metadata={"official_followup_match_count": 1},
    )

    [grouped] = cluster([item])

    assert grouped.member_metadata == [{"official_followup_match_count": 1}]


def test_clusters_cross_source_strong_shared_tags_when_titles_differ(
    make_cleaned,
) -> None:
    a = make_cleaned(
        id="a",
        source_id="banrep_comunicados",
        title="Junta mantiene tasa de interes",
        detected_entities=["banrep"],
        detected_topics=["monetary_policy"],
    )
    b = make_cleaned(
        id="b",
        source_id="eltiempo_colombia",
        title="Emisor deja politica monetaria sin cambios",
        detected_entities=["banrep"],
        detected_topics=["monetary_policy"],
    )

    clusters = cluster([a, b])

    assert sorted(len(group.items) for group in clusters) == [2]


def test_same_source_strong_shared_tags_do_not_force_cluster(make_cleaned) -> None:
    a = make_cleaned(
        id="a",
        source_id="banrep_comunicados",
        title="Junta mantiene tasa de interes",
        detected_entities=["banrep"],
        detected_topics=["monetary_policy"],
    )
    b = make_cleaned(
        id="b",
        source_id="banrep_comunicados",
        title="Emisor deja politica monetaria sin cambios",
        detected_entities=["banrep"],
        detected_topics=["monetary_policy"],
    )

    clusters = cluster([a, b])

    assert len(clusters) == 2


def test_cluster_summary_matches_title_member(make_cleaned) -> None:
    short = make_cleaned(
        id="a",
        title="Gaceta Congreso",
        summary="393 | Senado de la República | 29/04/2026",
    )
    long = make_cleaned(
        id="b",
        title="Gaceta del Congreso 418 — Senado de la República",
        summary="418 | Senado de la República | 05/05/2026",
    )
    [grouped] = cluster([short, long], threshold=0.0)
    assert grouped.title == long.title
    assert grouped.summary == long.summary


def test_cluster_ids_stable_across_calls(make_cleaned) -> None:
    items = [
        make_cleaned(id="a", title="Inflación marzo cifras"),
        make_cleaned(id="b", title="Inflación marzo cifras alta"),
    ]
    c1 = cluster(items)
    c2 = cluster(items)
    assert [c.cluster_id for c in c1] == [c.cluster_id for c in c2]


def test_topic_keywords_returns_top_n(make_cleaned) -> None:
    items = [
        make_cleaned(id=f"i{i}", title="Inflación marzo cifras DANE")
        for i in range(3)
    ]
    items.append(make_cleaned(id="x", title="Senado pensional reforma"))
    keywords = topic_keywords(items, top_n=3)
    assert "inflacion" in keywords
    assert len(keywords) <= 3
