"""S2: the feature contract, the training window, the gate, and train_model end to end against the fakes
and a local MLflow (SQLite). No AWS."""

import dagster as dg
import mlflow
import pytest
from fakes import LocalArtifacts, contract_columns, feature_redshift, unloading_redshift
from mlflow import MlflowClient

from labpipes.contracts import features_contract, mlflow_contract
from labpipes.defs import training
from labpipes.defs.feature_check import column_differences, feature_contract, range_sql
from labpipes.defs.features import trip_features
from labpipes.defs.training import beats_champion, challenger, eval_report, gate, plan_window, trained_model, train_model, training_set
from labpipes.resources import MlflowTracking

MONTH = "2024-01"
NAME = mlflow_contract()["registered_model"]


def _found(columns) -> dict:
    from labpipes.defs.feature_check import normalize

    return {n: normalize(t, length) for n, t, length in columns}


# --- feature_contract -------------------------------------------------------------------------------

def test_the_contract_table_has_no_differences():
    assert column_differences(features_contract(), _found(contract_columns())) == []


def test_a_post_trip_column_is_reported_as_forbidden():
    leaky = [*contract_columns(), ["trip_distance", "double precision", None]]
    assert column_differences(features_contract(), _found(leaky)) == ["forbidden post-trip column: trip_distance double precision"]


def test_unknown_missing_and_retyped_columns_are_differences():
    cols = [c for c in contract_columns() if c[0] != "pickup_dow"]
    cols = [["pickup_hour", "integer", None] if c[0] == "pickup_hour" else c for c in cols] + [["weather", "integer", None]]
    diffs = column_differences(features_contract(), _found(cols))
    assert "missing column pickup_dow smallint" in diffs
    assert "pickup_hour: expected smallint, found integer" in diffs
    assert "column not in the contract: weather integer" in diffs


def test_range_query_uses_the_contract_ranges():
    sql = range_sql(features_contract(), MONTH)
    assert "pickup_zone_id < 1.0 or pickup_zone_id > 263.0" in sql
    assert "duration_min <= 0.0 or duration_min > 180.0" in sql  # exclusive_min
    with pytest.raises(ValueError):
        range_sql(features_contract(), "2024-1")


def _check(redshift):
    """Materialize one month of trip_features (fake) and return feature_contract's evaluation."""
    defs = dg.Definitions(assets=[trip_features], asset_checks=[feature_contract], resources={"redshift": redshift},
                          jobs=[dg.define_asset_job("features", selection=[trip_features])])
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("trip_months", [MONTH])
    run = defs.resolve_job_def("features").execute_in_process(partition_key=MONTH, instance=instance, raise_on_error=False)
    return next(e.asset_check_evaluation_data for e in run.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION")


def test_feature_contract_passes_on_the_contract_table():
    assert _check(feature_redshift()).passed


def test_feature_contract_blocks_a_leaky_column():
    ev = _check(feature_redshift(columns=[*contract_columns(), ["fare_amount", "double precision", None]]))
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.ERROR


def test_feature_contract_fails_on_rows_out_of_range():
    assert not _check(feature_redshift(out_of_range={"duration_min": 3})).passed


def test_feature_contract_is_blocking():
    assert next(iter(feature_contract.check_specs)).blocking


# --- window and gate --------------------------------------------------------------------------------

MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04"]


def test_undecided_window_fails_closed():
    with pytest.raises(dg.Failure, match="not decided"):
        plan_window(MONTHS, {"train_months": None})


def test_window_is_the_months_before_the_latest():
    assert plan_window(MONTHS, {"train_months": 3}) == (["2024-01", "2024-02", "2024-03"], "2024-04")
    assert plan_window(MONTHS, {"train_months": 1}) == (["2024-03"], "2024-04")


def test_window_with_a_gap_fails():
    with pytest.raises(dg.Failure, match="missing"):
        plan_window(["2024-01", "2024-03", "2024-04"], {"train_months": 2})


REPORT = {"new.mae_min": 5.0, "champion.mae_min": 6.0, "new.p95_latency_ms": 4.0}


def test_undecided_gate_fails_closed():
    passed, reasons = gate(REPORT, {"metric": None, "min_improvement_min": None, "max_p95_latency_ms": None})
    assert not passed and "not decided" in reasons[0]


@pytest.mark.parametrize(("margin", "budget", "expected"), [(0.5, 10, True), (1.5, 10, False), (0.5, 3, False)])
def test_gate_needs_the_margin_and_the_latency_budget(margin, budget, expected):
    passed, _ = gate(REPORT, {"metric": "mae_min", "min_improvement_min": margin, "max_p95_latency_ms": budget})
    assert passed is expected


def test_beats_champion_is_blocking():
    assert next(iter(beats_champion.check_specs)).blocking


# --- train_model end to end -------------------------------------------------------------------------

@pytest.fixture
def lab(tmp_path, monkeypatch):
    uri = f"sqlite:///{tmp_path}/mlflow.db"
    mlflow.set_tracking_uri(uri)
    mlflow.create_experiment(mlflow_contract()["experiment"], artifact_location=str(tmp_path / "mlartifacts"))
    monkeypatch.setenv("GIT_SHA", "abc1234")
    monkeypatch.setattr(training, "WINDOW", {"train_months": 3})
    resources = {
        "redshift": unloading_redshift(str(tmp_path / "s3")),
        "artifacts": LocalArtifacts(bucket="arts", root=str(tmp_path / "s3")),
        "tracking": MlflowTracking(tracking_uri=uri),
    }
    defs = dg.Definitions(assets=[trip_features, training_set, trained_model, eval_report, challenger],
                          asset_checks=[feature_contract, beats_champion], jobs=[train_model], resources=resources)
    yield defs, MlflowClient(uri)
    mlflow.set_tracking_uri(None)


def _train(defs, monkeypatch, **gate_values):
    monkeypatch.setattr(training, "GATE", {"metric": "mae_min", "min_improvement_min": 0.0, "max_p95_latency_ms": 1000.0, **gate_values})
    result = defs.resolve_job_def("train_model").execute_in_process(instance=dg.DagsterInstance.ephemeral(), raise_on_error=False)
    return {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}


def test_a_better_model_becomes_challenger_with_every_contract_tag(lab, monkeypatch):
    defs, client = lab
    done = _train(defs, monkeypatch)
    assert done == {"training_set", "trained_model", "eval_report", "challenger"}
    mv = client.get_model_version_by_alias(NAME, "challenger")
    assert set(mlflow_contract()["version_tags"]) <= set(mv.tags)
    assert mv.tags["training_window"] == "2024-01..2024-03" and mv.tags["holdout_month"] == "2024-04"
    assert mv.tags["git_sha"] == "abc1234" and mv.tags["features_version"] == str(features_contract()["version"])
    assert float(mv.tags["mae_min"]) < float(mv.tags["champion_mae_min"])  # beat the v1 baseline
    assert mv.tags["gate"] == "passed"  # what promotion (S3) checks
    metrics = client.get_run(mv.run_id).data.metrics
    assert {"mae_min", "p95_latency_ms", "champion_mae_min", "mae_min.manhattan"} <= set(metrics)
    assert "champion" not in client.get_registered_model(NAME).aliases


def test_a_version_that_misses_the_margin_is_registered_but_never_challenger(lab, monkeypatch):
    defs, client = lab
    done = _train(defs, monkeypatch, min_improvement_min=1000.0)
    assert "eval_report" in done and "challenger" not in done
    assert client.get_registered_model(NAME).aliases == {}
    assert "gate" not in client.get_model_version(NAME, "1").tags


def test_an_undecided_gate_never_sets_challenger(lab, monkeypatch):
    defs, client = lab
    done = _train(defs, monkeypatch, metric=None)
    assert "challenger" not in done and client.get_registered_model(NAME).aliases == {}


def test_challenger_on_its_own_rechecks_the_gate(lab, monkeypatch):
    defs, client = lab
    _train(defs, monkeypatch, min_improvement_min=1000.0)
    instance = dg.DagsterInstance.ephemeral()
    # Same instance needed for _latest: re-run the chain without challenger, then challenger alone.
    job = defs.resolve_implicit_global_asset_job_def()
    job.execute_in_process(instance=instance, asset_selection=[dg.AssetKey(k) for k in ("training_set", "trained_model", "eval_report")],
                           raise_on_error=False)
    alone = job.execute_in_process(instance=instance, asset_selection=[dg.AssetKey("challenger")], raise_on_error=False)
    assert not alone.success and client.get_registered_model(NAME).aliases == {}
