# 004: COPY vs Spectrum

Stage: S1 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Load each month into Redshift with `COPY` (`pipelines/src/labpipes/defs/raw_trips.py`). One transaction COPYs the month's Parquet file into a temp table, deletes that month's earlier rows from `ingest.raw_trips` (by `source_file`), and inserts the new ones. Everything downstream (`stg_trips`, `zone_hour_stats`, `trip_features`) reads the copy in Redshift, not the file in S3.

## Rejected alternative

Redshift Spectrum: an external table over `s3://nst-mlops-2026/trips/`, queried in place, with `ingest.raw_trips` as a view over it. There's no load step and nothing to duplicate on a rerun, a new file is visible the moment it lands, and the data isn't stored twice.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
With COPY, data enters the warehouse only through the pipeline, behind the blocking `schema_contract` check: a file whose schema moved is never copied. With Spectrum, the file in S3 *is* the raw table. If a file is replaced by a bad one (gameday), every query over raw data changes at that instant, before any check runs. COPY also keeps the trip tables local, so sort keys apply and every downstream step is a local query. The contract names `ingest.raw_trips`, and `ingest` already holds local tables (`taxi_zones`), so Spectrum would also need a separate external schema plus a view.

The cost is the reload logic (delete and insert in one transaction, so a rerun never duplicates a month) and storing four months twice, about 220 MB of Parquet. That's small. A load takes about 3 minutes per month on the platform host (2024-01: 2.6 min, 2024-02: 3.2 min).

## What would show this was wrong

Months growing to the point where storing them twice or loading them is slow or costly; needing a file visible within seconds of landing; or queries that only ever scan a month once (then Spectrum's pay-per-scan beats loading it).
