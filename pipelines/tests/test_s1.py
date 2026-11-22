"""S1: schema contract, idempotent COPY, quality checks and the sensor, against the fakes (no AWS)."""

import json

import dagster as dg
import pytest
from fakes import LOG, fake_bucket, feature_redshift, schema_2023, schema_2025

from labpipes.contracts import trips_schema
from labpipes.defs import quality
from labpipes.defs.marts import zone_hour_stats
from labpipes.defs.feature_check import feature_contract
from labpipes.defs.features import trip_features
from labpipes.defs.quality import stg_trips_complete, stg_trips_duplicates, stg_trips_values, stg_trips_volume
from labpipes.defs.raw_trips import load_statements, raw_trips
from labpipes.defs.schema_check import schema_contract, schema_differences
from labpipes.defs.sensor import ingest_month, trip_files_sensor
from labpipes.defs.staging import stg_trips
from labpipes.defs.trip_files import trip_files
from labpipes.defs.zones import taxi_zones
from labpipes.partitions import source_file

MONTH = "2024-01"


def test_contract_schema_has_no_differences():
    assert schema_differences(trips_schema(), trips_schema()) == []


def test_extra_trailing_column_is_a_mismatch():
    assert schema_differences(trips_schema(), schema_2025()) == ["#20: unexpected column cbd_congestion_fee DOUBLE"]


def test_type_changes_are_mismatches_but_name_case_is_not():
    diffs = schema_differences(trips_schema(), schema_2023())
    assert any("VendorID expected INT32, found INT64" in d for d in diffs)
    assert not any("airport_fee" in d.lower() for d in diffs)  # same type, only the casing differs


def test_missing_and_reordered_columns_are_mismatches():
    cols = trips_schema()
    assert schema_differences(cols, cols[:-1]) == ["#19: missing column Airport_fee DOUBLE"]
    swapped = [cols[1], cols[0], *cols[2:]]
    assert len(schema_differences(cols, swapped)) == 2


def test_load_is_one_transaction_that_replaces_the_month():
    sqls = load_statements("s3://data/trips/yellow_tripdata_2024-01.parquet", source_file(MONTH))
    copy = next(i for i, s in enumerate(sqls) if s.startswith("copy landing"))
    delete = next(i for i, s in enumerate(sqls) if s.startswith("delete from ingest.raw_trips"))
    insert = next(i for i, s in enumerate(sqls) if s.startswith("insert into ingest.raw_trips"))
    assert copy < delete < insert  # a failed COPY rolls back before anything is deleted
    assert "iam_role default format as parquet" in sqls[copy]
    assert "where source_file = 'yellow_tripdata_2024-01.parquet'" in sqls[delete]


def _run(files: dict, monkeypatch, policy: dict | None = None):
    if policy is not None:
        monkeypatch.setattr(quality, "POLICY", policy)
    defs = dg.Definitions(
        assets=[taxi_zones, trip_files, raw_trips, stg_trips, zone_hour_stats, trip_features],
        asset_checks=[schema_contract, stg_trips_values, stg_trips_duplicates, stg_trips_complete, stg_trips_volume,
                      feature_contract],
        jobs=[ingest_month],
        resources={"redshift": feature_redshift(), "data_bucket": fake_bucket(files)},
    )
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("trip_months", [MONTH])
    result = defs.resolve_job_def("ingest_month").execute_in_process(partition_key=MONTH, instance=instance, raise_on_error=False)
    done = {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}
    checks = {e.asset_check_evaluation_data.check_name: e.asset_check_evaluation_data.passed
              for e in result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"}
    return done, checks


def _decided(**overrides) -> dict:
    """A permissive policy for tests only: the real values are the student's decision."""
    p = {
        "stg_trips_values": {"blocking": True, "max_rate": dict.fromkeys(quality.POLICY["stg_trips_values"]["max_rate"], 1.0)},
        "stg_trips_duplicates": {"blocking": True, "max_rate": 1.0},
        "stg_trips_complete": {"blocking": True},
        "stg_trips_volume": {"blocking": True, "min_ratio": 0.0, "max_ratio": 100.0},
    }
    p.update(overrides)
    return p


def test_a_good_month_reaches_the_mart(monkeypatch):
    done, checks = _run({MONTH: ("e1", trips_schema())}, monkeypatch, _decided())
    assert done == {"taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats", "trip_features"}
    assert all(checks.values()) and set(checks) >= {"schema_contract", "stg_trips_values", "stg_trips_complete", "feature_contract"}


def test_a_moved_schema_is_never_copied(monkeypatch):
    done, checks = _run({MONTH: ("e1", schema_2025())}, monkeypatch, _decided())
    assert checks["schema_contract"] is False
    assert "raw_trips" not in done and "zone_hour_stats" not in done and "trip_features" not in done
    assert not any(s.startswith("copy landing") for s in LOG)


def test_undecided_thresholds_fail_closed(monkeypatch):
    done, checks = _run({MONTH: ("e1", trips_schema())}, monkeypatch)
    if any(v is None for v in quality.POLICY["stg_trips_values"]["max_rate"].values()):
        assert checks["stg_trips_values"] is False
        assert "zone_hour_stats" not in done and "trip_features" not in done
    else:
        pytest.skip("POLICY is decided")


def _tick(bucket, cursor=None):
    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(instance=instance, cursor=cursor, resources={"data_bucket": bucket})
    return trip_files_sensor(ctx)


def test_sensor_adds_new_months_and_requests_one_run_each():
    r = _tick(fake_bucket({"2024-01": ("a", trips_schema()), "2024-02": ("b", trips_schema())}))
    assert [q.partition_key for q in r.run_requests] == ["2024-01", "2024-02"]
    assert r.dynamic_partitions_requests[0].partition_keys == ["2024-01", "2024-02"]
    assert json.loads(r.cursor) == {"2024-01": "a", "2024-02": "b"}


def test_sensor_skips_when_nothing_changed():
    r = _tick(fake_bucket({"2024-01": ("a", trips_schema())}), cursor=json.dumps({"2024-01": "a"}))
    assert isinstance(r, dg.SkipReason)


def test_sensor_reruns_a_changed_file_without_re_adding_it():
    r = _tick(fake_bucket({"2024-01": ("a2", trips_schema())}), cursor=json.dumps({"2024-01": "a"}))
    assert [(q.partition_key, q.run_key) for q in r.run_requests] == [("2024-01", "2024-01:a2")]
    assert r.dynamic_partitions_requests == []
