"""S1: data-quality checks on stg_trips (nulls, ranges, duplicates, row counts).

Decision points: which checks block downstream and which only warn, and every threshold. Base them on
what real months look like (scripts/verify/results/tlc-stats-2024-01-04.md is the instructor's
baseline; measure your own in Redshift) and on what gameday's corrupt month would look like.

Every number and every blocking flag lives in POLICY below; the checks only measure and compare. A value
left as None is undecided: the check then fails (and blocks), saying so, rather than pass on a guess.
"""

import statistics

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import DataBucket, RedshiftData
from .staging import stg_trips

UNDECIDED = None

# Decision: blocking vs warning, and thresholds (decisions/, S1). Rates are fractions of the month's rows.
# Student's choice: "tight, all block", about 1.5x the worst of 2024-01..04 (TLC files, measured
# 2026-11-21; worst month in brackets). Re-measure in Redshift and revisit if a good month trips one.
POLICY: dict = {
    "stg_trips_values": {
        "blocking": True,
        "max_rate": {
            "nonpositive_duration": 0.0005,  # dropoff at or before pickup        [0.031%]
            "over_3h": 0.001,                # duration over 180 minutes          [0.067%]
            "null_passengers": 0.18,         # passenger_count is null            [11.9%; 4.7% in Jan]
            "unknown_zone": 0.0001,          # id not in ingest.taxi_zones        [0]
        },
    },
    "stg_trips_duplicates": {"blocking": True, "max_rate": 0.0001},  # [1 row in 3.0M]
    # stg_trips is unfiltered, so its rows should equal the file's footer row count exactly.
    "stg_trips_complete": {"blocking": True},
    # This month's rows over the median of the other loaded months.                  [0.84..1.19]
    "stg_trips_volume": {"blocking": True, "min_ratio": 0.7, "max_ratio": 1.4},
}


def _blocking(name: str) -> bool:
    """Undecided blocks: a check nobody has set must not wave a month through to the mart."""
    b = POLICY[name]["blocking"]
    return True if b is UNDECIDED else bool(b)


def _result(name: str, passed: bool, undecided: list[str], description: str, metadata: dict) -> dg.AssetCheckResult:
    if undecided:
        passed = False
        description = f"threshold not decided: {', '.join(undecided)} (quality.POLICY). {description}"
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR if _blocking(name) else dg.AssetCheckSeverity.WARN,
        description=description,
        metadata=metadata,
    )


def _rate(n, rows) -> float:
    return int(n or 0) / int(rows) if int(rows or 0) else 0.0


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=_blocking("stg_trips_values"),
                description="Nulls and out-of-range values stay under their rates.")
def stg_trips_values(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = redshift.query(
        "select count(*) as rows, "
        "sum(case when s.duration_min <= 0 then 1 else 0 end) as nonpositive_duration, "
        "sum(case when s.duration_min > 180 then 1 else 0 end) as over_3h, "
        "sum(case when s.passenger_count is null then 1 else 0 end) as null_passengers, "
        "sum(case when pz.location_id is null or dz.location_id is null then 1 else 0 end) as unknown_zone "
        "from staging.stg_trips s "
        "left join ingest.taxi_zones pz on pz.location_id = s.pickup_zone_id "
        "left join ingest.taxi_zones dz on dz.location_id = s.dropoff_zone_id "
        f"where s.trip_month = '{month}'"
    )[0]
    limits = POLICY["stg_trips_values"]["max_rate"]
    rates = {k: _rate(r[k], r["rows"]) for k in limits}
    undecided = [k for k, v in limits.items() if v is UNDECIDED]
    over = [k for k, v in limits.items() if v is not UNDECIDED and rates[k] > v]
    return _result(
        "stg_trips_values", not over, undecided,
        "all rates within limits" if not over else "over limit: " + ", ".join(f"{k} {rates[k]:.4%}" for k in over),
        {"rows": dg.MetadataValue.int(int(r["rows"])),
         **{f"{k}_rate": dg.MetadataValue.float(rates[k]) for k in limits},
         **{k: dg.MetadataValue.int(int(r[k] or 0)) for k in limits}},
    )


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=_blocking("stg_trips_duplicates"),
                description="Exact duplicate trips stay under their rate.")
def stg_trips_duplicates(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    r = redshift.query(
        f"select (select count(*) from staging.stg_trips where trip_month = '{month}') as rows, "
        "coalesce(sum(n - 1), 0) as extra from ("
        f"select count(*) as n from staging.stg_trips where trip_month = '{month}' "
        "group by vendor_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id, trip_distance, total_amount "
        "having count(*) > 1)"
    )[0]
    limit = POLICY["stg_trips_duplicates"]["max_rate"]
    rate = _rate(r["extra"], r["rows"])
    passed = limit is UNDECIDED or rate <= limit
    return _result(
        "stg_trips_duplicates", passed, ["max_rate"] if limit is UNDECIDED else [],
        f"{int(r['extra'] or 0)} extra copies ({rate:.4%})",
        {"rows": dg.MetadataValue.int(int(r["rows"])), "extra": dg.MetadataValue.int(int(r["extra"] or 0)),
         "extra_rate": dg.MetadataValue.float(rate)},
    )


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=_blocking("stg_trips_complete"),
                description="Every row of the file reached stg_trips (row count equals the Parquet footer's).")
def stg_trips_complete(context: dg.AssetCheckExecutionContext, redshift: RedshiftData,
                       data_bucket: DataBucket) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    expected = data_bucket.footer(month).num_rows
    loaded = int(redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'") or 0)
    return _result(
        "stg_trips_complete", loaded == expected, [],
        f"{loaded} rows loaded, file has {expected}",
        {"loaded": dg.MetadataValue.int(loaded), "file_rows": dg.MetadataValue.int(expected)},
    )


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=_blocking("stg_trips_volume"),
                description="The month's row count is in line with the other loaded months.")
def stg_trips_volume(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    counts = {r["trip_month"]: int(r["n"]) for r in
              redshift.query("select trip_month, count(*) as n from staging.stg_trips group by trip_month")}
    this, others = counts.get(month, 0), [n for m, n in counts.items() if m != month]
    p = POLICY["stg_trips_volume"]
    undecided = [k for k in ("min_ratio", "max_ratio") if p[k] is UNDECIDED]
    if not others:
        return _result("stg_trips_volume", True, undecided, f"{this} rows; no other months to compare",
                       {"rows": dg.MetadataValue.int(this)})
    ratio = this / statistics.median(others)
    passed = (p["min_ratio"] is UNDECIDED or ratio >= p["min_ratio"]) and (p["max_ratio"] is UNDECIDED or ratio <= p["max_ratio"])
    return _result(
        "stg_trips_volume", passed, undecided,
        f"{this} rows, {ratio:.2f}x the median of {len(others)} other month(s)",
        {"rows": dg.MetadataValue.int(this), "ratio_to_median": dg.MetadataValue.float(ratio),
         "other_months": dg.MetadataValue.int(len(others))},
    )
