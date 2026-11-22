"""S6: the wiki RAG branch, in the `rag` database on RDS (pgvector), on a daily schedule.

- wiki_pages: the published pages from the ops wiki (resource `wiki`: labpipes/wiki.py). Store each
  page's ETag and send it next time: an unchanged page then costs a 304 and no body.
- wiki_chunks: split changed pages with labpipes.chunking.chunk_markdown (chunk size and overlap are
  decision 015) and store the chunks; use Chunk.content_hash as the id so unchanged text keeps it.
- wiki_embeddings: embed only the chunks that have no vector yet (resource `embed`: nomic-embed-text,
  768 dimensions) and store them as pgvector `vector(768)`, with an index (HNSW or IVFFlat: decision 016).
- A blocking check that every chunk has a vector, and a schedule (and a job you can run by hand with
  scripts/dagster-run.sh) for the three.
The RDS admin connection comes from the `rag` resource (RagStore.connect); `create extension vector`
works there. Copy asset and check shapes from pipelines/reference/patterns.py.

State lives in the database, not in Dagster: each step reads what the one before it left there, so a
second run with nothing changed fetches nothing (all 304s), chunks nothing and embeds nothing.

  rag.pages   id, title, updated_at, etag, body, chunked_with: the ETag and chunker a page was last chunked at
  rag.chunks  id (content hash), page_id, idx, heading, content, tokens, embedding vector(768)
"""

import dagster as dg

from ..chunking import chunk_markdown
from ..resources import LlmEmbed, RagStore, WikiApi

UNDECIDED = None

# Decision 015: chunk size and overlap, in estimated tokens (labpipes/chunking.py). Until decided,
# wiki_chunks fails rather than guess. Changing them re-chunks every page on the next run.
CHUNKING: dict = {"max_tokens": UNDECIDED, "overlap_tokens": UNDECIDED}

# Decision 016: the vector index, "hnsw" or "ivfflat" (with IVFFLAT_LISTS). Until decided there is no
# index: retrieval still works, as an exact scan over every chunk, which is fine at wiki size.
INDEX: str | None = UNDECIDED
IVFFLAT_LISTS = UNDECIDED  # ivfflat only: how many lists (rule of thumb: rows / 1000, at least 1)

DIMENSIONS = 768

SCHEMA = [
    "create extension if not exists vector",
    "create schema if not exists rag",
    """create table if not exists rag.pages (
         id text primary key, title text not null, updated_at text, etag text not null, body text not null,
         chunked_with text, fetched_at timestamptz not null default now())""",
    f"""create table if not exists rag.chunks (
         id text primary key, page_id text not null references rag.pages (id) on delete cascade,
         idx integer not null, heading text not null, content text not null, tokens integer not null,
         embedding vector({DIMENSIONS}))""",
    "create index if not exists chunks_page on rag.chunks (page_id)",
]


def chunker_id(policy: dict) -> str:
    """What a page was chunked with: its ETag plus the chunk settings, so a change in either re-chunks it."""
    return f"{policy['max_tokens']}:{policy['overlap_tokens']}"


def _undecided(policy: dict) -> list[str]:
    return [k for k, v in policy.items() if v is UNDECIDED]


def ensure_schema(conn) -> None:
    with conn, conn.cursor() as cur:
        for sql in SCHEMA:
            cur.execute(sql)


def vector_literal(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.7g}" for x in v) + "]"


def index_sql(kind: str | None, lists) -> str | None:
    """The CREATE INDEX for decision 016, or None while it's undecided."""
    if kind is UNDECIDED:
        return None
    if kind == "hnsw":
        return "create index if not exists chunks_embedding_hnsw on rag.chunks using hnsw (embedding vector_cosine_ops)"
    if kind == "ivfflat":
        if lists is UNDECIDED or int(lists) < 1:
            raise ValueError("ivfflat needs IVFFLAT_LISTS (decision 016)")
        return f"create index if not exists chunks_embedding_ivfflat on rag.chunks using ivfflat (embedding vector_cosine_ops) with (lists = {int(lists)})"
    raise ValueError(f"INDEX must be 'hnsw' or 'ivfflat', got {kind!r}")


@dg.asset(group_name="rag", kinds={"postgres"})
def wiki_pages(context: dg.AssetExecutionContext, wiki: WikiApi, rag: RagStore) -> dg.MaterializeResult:
    """Every published page, by conditional GET: an unchanged page is a 304 and costs no body."""
    client = wiki.client()
    conn = rag.connect()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("select id, etag from rag.pages")
            known = dict(cur.fetchall())
        refs = client.pages()
        fetched = not_modified = 0
        for ref in refs:
            page = client.page(ref.id, known.get(ref.id))
            if page is None:
                not_modified += 1
                continue
            fetched += 1
            with conn, conn.cursor() as cur:
                cur.execute(
                    """insert into rag.pages (id, title, updated_at, etag, body) values (%s, %s, %s, %s, %s)
                       on conflict (id) do update set title = excluded.title, updated_at = excluded.updated_at,
                         etag = excluded.etag, body = excluded.body, fetched_at = now()""",
                    (page.id, page.title, page.updated_at, page.etag, page.body))
        listed = {r.id for r in refs}
        gone = sorted(set(known) - listed)
        if gone:  # unpublished: its chunks go with it (on delete cascade)
            with conn, conn.cursor() as cur:
                cur.execute("delete from rag.pages where id = any(%s)", (gone,))
        context.log.info(f"{len(refs)} pages: {fetched} fetched, {not_modified} not modified (304), {len(gone)} removed")
        return dg.MaterializeResult(metadata={
            "pages": dg.MetadataValue.int(len(refs)),
            "fetched": dg.MetadataValue.int(fetched),
            "not_modified": dg.MetadataValue.int(not_modified),
            "removed": dg.MetadataValue.int(len(gone)),
        })
    finally:
        conn.close()


def plan_chunks(page_id: str, title: str, body: str, policy: dict) -> list[tuple]:
    """Rows for rag.chunks: (id, page_id, idx, heading, content, tokens). The id is the content hash, so
    text that didn't change keeps its id, and with it its embedding."""
    return [(c.content_hash(page_id), page_id, c.index, c.heading, c.text, c.tokens)
            for c in chunk_markdown(title, body, int(policy["max_tokens"]), int(policy["overlap_tokens"]))]


@dg.asset(deps=[wiki_pages], group_name="rag", kinds={"postgres"})
def wiki_chunks(context: dg.AssetExecutionContext, rag: RagStore) -> dg.MaterializeResult:
    """Re-chunk only the pages whose ETag (or the chunk settings) changed since they were last chunked."""
    if _undecided(CHUNKING):
        raise dg.Failure(f"chunking not decided: {', '.join(_undecided(CHUNKING))} (rag.CHUNKING, decision 015)")
    settings = chunker_id(CHUNKING)
    conn = rag.connect()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("select id, title, body, etag from rag.pages where chunked_with is distinct from etag || '@' || %s",
                        (settings,))
            todo = cur.fetchall()
        added = removed = 0
        for page_id, title, body, etag in todo:
            rows = plan_chunks(page_id, title, body, CHUNKING)
            ids = [r[0] for r in rows]
            with conn, conn.cursor() as cur:  # one transaction per page: its chunks are never half replaced
                cur.execute("delete from rag.chunks where page_id = %s and not (id = any(%s))", (page_id, ids))
                removed += cur.rowcount
                for row in rows:
                    # An unchanged chunk keeps its row (and its vector); only its position may move.
                    # xmax = 0 only for a row this statement inserted, not for one it updated.
                    cur.execute(
                        """insert into rag.chunks (id, page_id, idx, heading, content, tokens) values (%s, %s, %s, %s, %s, %s)
                           on conflict (id) do update set idx = excluded.idx, heading = excluded.heading
                           returning (xmax = 0)""", row)
                    added += 1 if cur.fetchone()[0] else 0
                cur.execute("update rag.pages set chunked_with = %s || '@' || %s where id = %s", (etag, settings, page_id))
        with conn.cursor() as cur:
            cur.execute("select count(*), coalesce(sum(tokens), 0), coalesce(max(tokens), 0) from rag.chunks")
            total, tokens, biggest = cur.fetchone()
        context.log.info(f"{len(todo)} page(s) re-chunked: {added} new chunk(s), {removed} removed; {total} in all")
        return dg.MaterializeResult(metadata={
            "pages_chunked": dg.MetadataValue.int(len(todo)),
            "chunks_new": dg.MetadataValue.int(added),
            "chunks_removed": dg.MetadataValue.int(removed),
            "chunks_total": dg.MetadataValue.int(int(total)),
            "tokens_total": dg.MetadataValue.int(int(tokens)),
            "chunk_tokens_max": dg.MetadataValue.int(int(biggest)),
            "chunking": settings,
        })
    finally:
        conn.close()


@dg.asset(deps=[wiki_chunks], group_name="rag", kinds={"postgres"})
def wiki_embeddings(context: dg.AssetExecutionContext, rag: RagStore, embed: LlmEmbed) -> dg.MaterializeResult:
    """Embed only the chunks without a vector, then make sure decision 016's index exists."""
    conn = rag.connect()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("select id, content from rag.chunks where embedding is null order by page_id, idx")
            todo = cur.fetchall()
        for i in range(0, len(todo), 32):
            batch = todo[i : i + 32]
            vectors = embed.embed_documents([content for _, content in batch])  # adds "search_document: "
            with conn, conn.cursor() as cur:
                for (chunk_id, _), v in zip(batch, vectors, strict=True):
                    cur.execute("update rag.chunks set embedding = %s::vector where id = %s", (vector_literal(v), chunk_id))
        index = index_sql(INDEX, IVFFLAT_LISTS)
        if index:
            with conn, conn.cursor() as cur:
                cur.execute(index)
        context.log.info(f"{len(todo)} chunk(s) embedded; index: {INDEX or 'none yet (decision 016)'}")
        return dg.MaterializeResult(metadata={
            "chunks_embedded": dg.MetadataValue.int(len(todo)),
            "index": INDEX or "none (exact scan; decision 016 undecided)",
        })
    finally:
        conn.close()


@dg.asset_check(asset=wiki_embeddings, blocking=True, description="Every chunk has a 768-dimension vector.")
def wiki_embeddings_complete(context: dg.AssetCheckExecutionContext, rag: RagStore) -> dg.AssetCheckResult:
    conn = rag.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""select count(*), count(*) filter (where embedding is null),
                                   count(*) filter (where embedding is not null and vector_dims(embedding) <> {DIMENSIONS}),
                                   (select count(*) from rag.pages)
                            from rag.chunks""")
            total, missing, wrong_dims, pages = cur.fetchone()
    finally:
        conn.close()
    passed = total > 0 and missing == 0 and wrong_dims == 0
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(f"{total} chunks from {pages} pages, all embedded" if passed else
                     f"{missing} of {total} chunks without a vector, {wrong_dims} with the wrong size" if total else "no chunks at all"),
        metadata={"chunks": dg.MetadataValue.int(int(total)), "missing": dg.MetadataValue.int(int(missing)),
                  "wrong_dimensions": dg.MetadataValue.int(int(wrong_dims)), "pages": dg.MetadataValue.int(int(pages))},
    )


ingest_wiki = dg.define_asset_job(
    "ingest_wiki",
    selection=dg.AssetSelection.assets(wiki_pages, wiki_chunks, wiki_embeddings),
    description="Re-read the wiki (conditional GETs), re-chunk what changed, embed what has no vector.",
)

# Daily, before the morning: the wiki is re-read once a day, and an unchanged wiki costs only 304s.
ingest_wiki_daily = dg.ScheduleDefinition(
    name="ingest_wiki_daily", job=ingest_wiki, cron_schedule="0 5 * * *", execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
