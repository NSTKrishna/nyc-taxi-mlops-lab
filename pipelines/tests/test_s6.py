"""S6: what the wiki branch decides without a database: chunk ids, re-chunk triggers, the index, the defs."""

import dagster as dg
import pytest

from labpipes.defs import rag

PAGE = """Intro about the Harbor Line.

## Pricing

The Harbor Line surcharge is $4.50 for trips ending at Pier 76.

## Hours

Service runs 06:00 to 23:00.
"""
POLICY = {"max_tokens": 120, "overlap_tokens": 20}


def test_chunk_ids_are_content_hashes_so_unchanged_text_keeps_its_vector():
    first = rag.plan_chunks("harbor-line", "Harbor Line", PAGE, POLICY)
    edited = rag.plan_chunks("harbor-line", "Harbor Line", PAGE.replace("23:00", "midnight"), POLICY)
    ids = lambda rows: {r[0]: r for r in rows}  # noqa: E731
    pricing = [r for r in first if r[3] == "Pricing"][0]
    assert pricing[0] in ids(edited)  # the Pricing section didn't change: same id, no new embedding
    hours_before = [r for r in first if r[3] == "Hours"][0][0]
    assert hours_before not in ids(edited)  # the Hours text changed: a new chunk to embed
    assert all(r[1] == "harbor-line" and r[4].startswith("Harbor Line") for r in first)


def test_a_change_in_chunk_settings_re_chunks_every_page():
    assert rag.chunker_id({"max_tokens": 120, "overlap_tokens": 20}) != rag.chunker_id({"max_tokens": 200, "overlap_tokens": 20})


def test_index_sql_follows_decision_016():
    assert rag.index_sql(None, None) is None
    assert "using hnsw (embedding vector_cosine_ops)" in rag.index_sql("hnsw", None)
    assert "with (lists = 4)" in rag.index_sql("ivfflat", 4)
    with pytest.raises(ValueError):
        rag.index_sql("ivfflat", None)
    with pytest.raises(ValueError):
        rag.index_sql("flat", None)


def test_vectors_are_pgvector_literals():
    assert rag.vector_literal([0.5, -1.0, 1e-9]) == "[0.5,-1,1e-09]"


def test_undecided_chunking_fails_closed(monkeypatch):
    monkeypatch.setattr(rag, "CHUNKING", {"max_tokens": None, "overlap_tokens": None})

    class NoRag:  # never reached: the asset fails before it connects
        def connect(self):
            raise AssertionError("connected before checking the decision")

    with pytest.raises(dg.Failure, match="decision 015"):
        rag.wiki_chunks(dg.build_asset_context(), rag=NoRag())


def test_the_job_check_and_daily_schedule_are_defined():
    from labpipes.definitions import defs

    job = defs.resolve_job_def("ingest_wiki")
    assert {k.to_user_string() for k in job.asset_layer.executable_asset_keys} >= {"wiki_pages", "wiki_chunks", "wiki_embeddings"}
    assert next(iter(rag.wiki_embeddings_complete.check_specs)).blocking
    assert rag.ingest_wiki_daily.cron_schedule == "0 5 * * *"
