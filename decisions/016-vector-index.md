# 016: HNSW or IVFFlat

Stage: S6 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

An **HNSW** index on `rag.chunks.embedding` with cosine distance (`INDEX = "hnsw"` in `pipelines/src/labpipes/defs/rag.py`): `create index ... using hnsw (embedding vector_cosine_ops)`, with pgvector's default build parameters (`m = 16`, `ef_construction = 64`). `wiki_embeddings` creates it if it doesn't exist. I would pick HNSW at a million chunks too.

## Rejected alternative

**IVFFlat**: k-means clusters the vectors into `lists` when the index is built, and each query scans only the nearest few lists (`ivfflat.probes`). It builds faster and uses less memory than HNSW. But it needs the data first, because the clusters are trained on the rows that exist at build time, and `lists` has to follow the row count (about rows / 1000). Rows inserted later go into clusters that were trained without them, so recall drifts as the wiki changes, until someone rebuilds the index.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
The wiki changes, and the pipeline keeps it current: pages are edited, `wiki_chunks` deletes and inserts chunks, and `wiki_embeddings` adds vectors every day. HNSW is a graph that takes each new vector as it's inserted, with no training step and no rebuild, so recall stays where it was as the content moves. IVFFlat would also be awkward today: with 16 chunks there is nothing to cluster (`lists` would be 1, a plain scan).

At a million chunks: HNSW gives the best speed and recall for queries, and keeps them under inserts, at the cost of memory (roughly a few GB for 768-dimension vectors plus the graph) and a build that takes minutes to hours. That fits a db.t4g.medium only barely, so at that size the instance would grow. I'd pick IVFFlat at a million only if memory or build time were the constraint and the data changed rarely (rebuilt in bulk). For today's 16 vectors either index is faster than the network, and the planner may well scan anyway.

## What would show this was wrong

Retrieval missing chunks that an exact scan finds (compare the top 3 with `set enable_indexscan = off`); index builds or inserts becoming slow, or memory pressure on RDS as the wiki grows; or a wiki that changes only in rare bulk loads, where IVFFlat's cheaper build would win.
