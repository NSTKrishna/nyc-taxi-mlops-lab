"""S1: the S3 sensor (and the job it starts).

Watch trips/ in the data bucket (DataBucket.trip_files). When a month's file appears or changes, add
the month to the trip_months partitions and request a run that goes from trip_files to zone_hour_stats.
Patterns: pipelines/reference/patterns.py.

The cursor maps month -> the ETag last requested, so a changed file (a new ETag) is seen as new. The
run_key includes the ETag too: Dagster drops a repeated request for the same file version, and a
re-landed file gets a fresh run.
"""

import json

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import DataBucket

# zone_hour_stats and everything upstream of it: taxi_zones, trip_files, raw_trips, stg_trips, and the
# checks on them. The mart is in the same run, so a blocking check that fails upstream keeps the month out.
ingest_month = dg.define_asset_job(
    "ingest_month",
    selection=dg.AssetSelection.assets("zone_hour_stats").upstream(),  # partitioning inferred: trip_months
    description="One month from its landed file to the zone_hour_stats mart.",
)


@dg.sensor(job=ingest_month, minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.STOPPED)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    seen: dict[str, str] = json.loads(context.cursor or "{}")
    found = {f.month: f.etag for f in data_bucket.trip_files()}
    changed = sorted(m for m, etag in found.items() if seen.get(m) != etag)
    if not changed:
        return dg.SkipReason(f"no new or changed files ({len(found)} under trips/)")
    for m in changed:
        check_month(m)  # partition keys end up in SQL text
    new = [m for m in changed if m not in seen]
    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=m, run_key=f"{m}:{found[m]}", tags={"etag": found[m]}) for m in changed],
        dynamic_partitions_requests=[trip_months.build_add_request(new)] if new else [],
        cursor=json.dumps({**seen, **{m: found[m] for m in changed}}, sort_keys=True),
    )
