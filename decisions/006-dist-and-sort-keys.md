# 006: Dist and sort keys for the trip tables

Stage: S1 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

`ingest.raw_trips` uses Redshift's automatic table optimization (`DISTSTYLE AUTO`, `SORTKEY AUTO`): `TABLE_KEYS = ""` in `pipelines/src/labpipes/defs/raw_trips.py`. Redshift picks the distribution and sort keys and changes them over time from how the table is queried. The downstream tables keep the keys from the given SQL: `staging.stg_trips` is sorted by `pickup_at`, and `staging.trip_features` by `trip_month`.

## Rejected alternative

`DISTSTYLE EVEN SORTKEY (source_file)`: rows spread evenly over the slices, stored in month order. Every query on `raw_trips` filters by month (the load's `delete ... where source_file = ...` and `stg_trips`'s `select ... where source_file = ...`), so a month sort key would let those queries skip the other months' blocks, predictably and from the first load.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
`raw_trips` is a landing table: it's written once per month and read once per month by `stg_trips`, and nothing joins or groups on it directly. Joins and aggregations happen on `stg_trips` and `trip_features`, which already have sort keys. At four months (about 12.9M rows) on a single `ra3.large` node, the difference between AUTO and a hand-picked key is small, and AUTO adapts if the access pattern changes, with nothing to maintain. A load takes about 3 minutes per month end to end.

The cost: what AUTO picks isn't visible in the code, so I can't predict the layout, and early queries run before Redshift has learned anything.

## What would show this was wrong

The monthly delete or the `stg_trips` select on `raw_trips` taking a growing share of a load as months accumulate; `svv_table_info` showing a high `unsorted` percentage or skew on `raw_trips`; or AUTO choosing a key that doesn't match the month filter. Then set `DISTSTYLE EVEN SORTKEY (source_file)` with `ALTER TABLE ... ALTER DISTSTYLE / ALTER SORTKEY`.
