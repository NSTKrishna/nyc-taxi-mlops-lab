# 018: Database role scope

Stage: S7 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

The MCP server queries Redshift as `lab_ro` (`MCP_DB_USER`), never `labadmin`. `lab_ro` has a disabled password (Data API temporary credentials only), `USAGE` on schema `marts` and `SELECT` on `marts.zone_hour_stats`, and **nothing else**. `CREATE` on schema `public` is revoked from `PUBLIC`, so `lab_ro` can't create a table anywhere. All of this is in `pipelines/src/labpipes/defs/mcp_reader.py`. The `mcp_reader` asset runs right after `zone_hour_stats` in every `ingest_month` run and re-applies the grants, and a blocking check reads the privileges back from Redshift.

## Rejected alternative

`SELECT` on every table in `marts`, plus default privileges so future `marts` tables are readable too. No new grant would be needed as the mart grows, but anything put in `marts` later (a staging copy, a table with rider-level data) would be readable by the MCP server without anyone deciding it should be.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
`query_zone_stats` reads one table, so the user reads one table. If the tool layer or the prompt is ever fooled, the database itself still refuses: no rows from `ingest` or `staging`, no writes, nothing created. That limit holds whatever the SQL says.

How I know it can't do more, from Redshift itself, not from my reading of the grants:
- `mcp_reader_least_privilege` (blocking) checks `has_table_privilege` and `has_schema_privilege` for `lab_ro`: select on the mart = true; insert, delete, create in marts, usage on ingest and staging, create in public = false. It passed on 2026-11-22 (run `7548af32`).
- The status artifact (the MCP server's `/healthz?db=1`, asked **as `lab_ro`**) shows the same: `read_marts: true`, `write_marts: false`, `use_ingest: false`, `create_in_marts: false`.

The refresh can't undo it: `zone_hour_stats` is rebuilt with `delete` + `insert` and never dropped, so its grant stays, and `mcp_reader` re-applies it after every refresh anyway in case that ever changes.

The cost: a new mart table needs its own grant and a decision, and `revoke create on schema public from public` applies to every non-superuser on the cluster.

## What would show this was wrong

The status artifact showing `write_marts` or `use_ingest` true; `query_zone_stats` failing with a permission error after a mart change (the grant was lost: then the re-grant isn't running); or a second mart table the assistant should read, which then needs a deliberate grant.
