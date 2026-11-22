# 015: Chunk size, overlap and top-k

Stage: S6 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Chunks of at most **256 tokens with 32 tokens of overlap** (`CHUNKING` in `pipelines/src/labpipes/defs/rag.py`), and **top-k 3** (`TOP_K` in `app/src/rideapp/assistant.py`): each answer's prompt carries the 3 chunks closest to the question by cosine distance. With today's wiki, every page is exactly one chunk.

## Rejected alternative

128 tokens with 16 overlap: 19 chunks, with the three longest pages (`eta-quoting`, `how-etas-are-computed`, `eta-complaints`) split in two. The chunks are smaller and each match is more precise, but a fact can land in one half while its condition or exception is in the other.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
Measured on the wiki (2026-11-22): 16 pages, 1,546 tokens in all, median 91 tokens per page, the largest 181. Every page is a single section under its title heading.

| max tokens | chunks | median | largest |
| --- | --- | --- | --- |
| 64 | 32 | 62 | 81 |
| 128 | 19 | 88 | 133 |
| 256 (chosen) | 16 | 94 | 188 |

The pages are already the size of a good chunk and about one topic each, so cutting them only separates facts from their context. The Harbor Line page, for example, gives the amount ($4.25), where it applies (Pier 76, the Hudson Yards ferry landing), the receipt code (HL3) and that it doesn't change the ETA, all in 79 tokens. "An answer is only as good as the chunk that holds it": one page per chunk keeps all of that together. The 32 tokens of overlap cost nothing today, because no page is split, and they protect the boundary if a page grows past 256.

Top-k 3 is about 33 s of prompt processing on the LLM host (about 11 s per chunk). 2 would save about 11 s but leaves no room when a lookalike page ranks first (`tlc-surcharges` and `harbor-line` are both about surcharges). 4 would be a quarter of the whole wiki in every prompt, for about 44 s. The system prompt stays fixed and cached, so only the excerpts and the question are new tokens on each request.

Result on a wiki-only question (2026-11-22, "How much is the Harbor Line surcharge, and when does it apply?"): "a flat $4.25 ... trips that end at Pier 76 or the Hudson Yards ferry landing ... code HL3 on the trip receipt", correct in every detail. Without the wiki the same question got "I don't know". Sources: `harbor-line` 0.870, then the lookalikes `tlc-surcharges` 0.697 and `tlc-airports` 0.663. Generation 44.5 s, 624 prompt tokens (Langfuse trace `b6a2fbb0...`). Ingest: 16 pages, 16 chunks, 1,616 tokens; a second run fetched 0 (16 not modified), chunked 0, embedded 0.

## What would show this was wrong

The right page in the wiki but not among the 3 sources (raise top-k, or improve the chunks' text); answers mixing up two lookalike pages (lower top-k, or tighten the prompt); pages growing so that a fact and its exception land in different chunks (then smaller chunks with overlap); or the LLM's prompt time climbing past what riders will wait for.
