# 005: Blocking vs warning, and every threshold

Stage: S1 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Every data-quality check on `stg_trips` blocks (`POLICY` in `pipelines/src/labpipes/defs/quality.py`), so a month that fails any of them never reaches `zone_hour_stats` or `trip_features`. `schema_contract` on `trip_files` blocks too, because the contract requires it.

| Check | Fails when | Measured on 2024-01..04 |
|---|---|---|
| `stg_trips_values`: dropoff at or before pickup | rate > 0.05% | 0.031% |
| `stg_trips_values`: duration over 3 h | rate > 0.1% | 0.067% |
| `stg_trips_values`: `passenger_count` null | rate > 18% | 11.9% (4.7% in January) |
| `stg_trips_values`: zone not in `taxi_zones` | rate > 0.01% | 0 |
| `stg_trips_duplicates`: exact duplicate trips | rate > 0.01% | 1 row in 3.0M |
| `stg_trips_complete`: rows in `stg_trips` vs the file's footer | not equal | equal |
| `stg_trips_volume`: rows vs the median of the other months | ratio < 0.7 or > 1.4 | 0.84 .. 1.19 |

## Rejected alternative

Warn-only for the noisy value checks (null passengers, over-3-hour trips), blocking only `schema_contract` and `stg_trips_complete`. Ingest would then never stop because some trips are messy. But a month with a real problem, say half its passenger counts missing, would flow into the mart and the training data with only a yellow mark in Dagster.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
Each threshold sits above what four real months showed and below what a broken month would show, so a normal month passes with room to spare, and a corrupt one (gameday) stops before the mart. Blocking costs little here: a blocked month only delays that month's data, while a bad month that gets through skews `zone_hour_stats` and every model trained on it. `stg_trips_complete` must be exact, because `stg_trips` is unfiltered, so any difference means rows were lost in the load.

The null-passenger threshold is the loosest (18% against 11.9% measured). January's 4.7% shows the rate moves from month to month, so a tighter limit would block a normal month.

## What would show this was wrong

A normal month blocked (a threshold too tight, most likely null passengers or volume in a holiday month); or a gameday corrupt month that passes every check (a threshold too loose, or a kind of corruption no check measures, for example shifted timestamps with valid durations).
