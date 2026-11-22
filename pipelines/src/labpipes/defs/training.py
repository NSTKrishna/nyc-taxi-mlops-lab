"""S2: training_set, trained_model, eval_report, the challenger gate and the challenger asset.

- training_set: UNLOAD a window of staging.trip_features (and the holdout month) to the artifact bucket.
  The zone-pair distance lookup (contracts/features.yaml, est_distance_miles) comes from the window only.
- trained_model: fit tripmodel.LightGBMModel (the given v2), log it to MLflow as a pyfunc with
  tripmodel.model_signature() and the tripmodel package bundled, register a version of the model named
  in contracts/mlflow.yaml, and tag it as that contract says.
- eval_report: tripmodel.evaluate the new version and the champion (or the v1 baseline before there is
  one) on the same holdout; log the metrics.
- A blocking check that guards a `challenger` asset, which sets the alias. The pipeline never moves
  `champion`.
Decision points: what stays in the notebook; the promotion metric and margin; the training window.

Each asset reads what it needs from the latest materialization of the one before it (its metadata: S3
prefixes, the window, the version), so a step can be re-run on its own. Every number the student decides
lives in WINDOW and GATE; a value left as None is undecided, and the run then fails (closed) saying so.
"""

import os
from pathlib import Path

import dagster as dg

from ..contracts import features_contract, mlflow_contract
from ..partitions import check_month
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData

UNDECIDED = None

# Decision: training window (decisions/, S2). The holdout is the latest month in staging.trip_features;
# the window is the `train_months` consecutive months right before it, and all of them must be loaded.
WINDOW: dict = {"train_months": UNDECIDED}

# Decision: promotion metric and margin (decisions/, S2). Lower is better for every metric here.
GATE: dict = {
    "metric": UNDECIDED,               # an eval_report metric: "mae_min", or a slice such as "mae_min.airport"
    "min_improvement_min": UNDECIDED,  # minutes the new version must beat the champion (or v1) by; 0 = any gain
    "max_p95_latency_ms": UNDECIDED,   # the latency budget: single-row predict, p95, in process
}

FEATURE_COLUMNS = ["pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow", "duration_min"]
# The rows trip_features keeps (sql/trip_features.sql), so distances come from the trips the model sees.
VALID_TRIP = ("duration_min > 0 and duration_min <= 180 "
              "and pickup_zone_id between 1 and 263 and dropoff_zone_id between 1 and 263")


def _undecided(policy: dict) -> list[str]:
    return [k for k, v in policy.items() if v is UNDECIDED]


def _months_before(month: str, n: int) -> list[str]:
    """The n months right before `month`, oldest first: ('2024-04', 3) -> 2024-01..2024-03."""
    y, m = map(int, check_month(month).split("-"))
    out = []
    for _ in range(n):
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
        out.append(f"{y:04d}-{m:02d}")
    return out[::-1]


def plan_window(available: list[str], policy: dict) -> tuple[list[str], str]:
    """(training months, holdout month) from the months loaded in trip_features."""
    if _undecided(policy):
        raise dg.Failure(f"training window not decided: {', '.join(_undecided(policy))} (training.WINDOW)")
    n = int(policy["train_months"])
    months = sorted({check_month(m) for m in available})
    if n < 1 or len(months) < 2:
        raise dg.Failure(f"need at least one training month and a holdout; trip_features has {months}")
    holdout = months[-1]
    window = _months_before(holdout, n)
    missing = [m for m in window if m not in months]
    if missing:
        raise dg.Failure(f"window {window[0]}..{window[-1]} for holdout {holdout} is missing {missing} in trip_features")
    return window, holdout


def _in(months: list[str]) -> str:
    return ", ".join(f"'{check_month(m)}'" for m in months)


def unload(select: str, uri: str) -> str:
    """UNLOAD a query to Parquet under an S3 prefix, with the cluster's LabRole."""
    return f"unload ('{select.replace(chr(39), chr(39) * 2)}') to '{uri}' iam_role default format as parquet allowoverwrite"


def _latest(context, key: str) -> dict:
    """The metadata of an asset's latest materialization, as plain values."""
    event = context.instance.get_latest_materialization_event(dg.AssetKey(key))
    if not event or not event.asset_materialization:
        raise dg.Failure(f"{key} has not been materialized yet")
    return {k: v.value for k, v in event.asset_materialization.metadata.items()}


@dg.asset(deps=["trip_features"], group_name="model", kinds={"redshift", "s3"})
def training_set(context: dg.AssetExecutionContext, redshift: RedshiftData, artifacts: ArtifactBucket) -> dg.MaterializeResult:
    available = [r["trip_month"] for r in redshift.query("select distinct trip_month from staging.trip_features order by 1")]
    window, holdout = plan_window(available, WINDOW)
    prefix = f"training/{context.run.run_id}/"
    paths = {part: f"{prefix}{part}/" for part in ("train", "holdout", "distances")}
    redshift.execute(
        unload(f"select {', '.join(FEATURE_COLUMNS)} from staging.trip_features where trip_month in ({_in(window)})",
               artifacts.uri(paths["train"])),
        unload(f"select {', '.join(FEATURE_COLUMNS)} from staging.trip_features where trip_month in ({_in([holdout])})",
               artifacts.uri(paths["holdout"])),
        # The window only: a distance learned from the holdout month would leak it into the evaluation.
        unload("select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles "
               f"from staging.stg_trips where trip_month in ({_in(window)}) and {VALID_TRIP} group by 1, 2",
               artifacts.uri(paths["distances"])),
    )
    counts = redshift.query(
        f"select sum(case when trip_month in ({_in(window)}) then 1 else 0 end) as train, "
        f"sum(case when trip_month = '{check_month(holdout)}' then 1 else 0 end) as holdout from staging.trip_features")[0]
    context.log.info(f"window {window[0]}..{window[-1]}, holdout {holdout}: {counts}")
    return dg.MaterializeResult(metadata={
        "training_window": f"{window[0]}..{window[-1]}",
        "holdout_month": holdout,
        "train_prefix": paths["train"],
        "holdout_prefix": paths["holdout"],
        "distances_prefix": paths["distances"],
        "train_rows": int(counts["train"] or 0),
        "holdout_rows": int(counts["holdout"] or 0),
    })


@dg.asset(deps=[training_set], group_name="model", kinds={"mlflow"})
def trained_model(context: dg.AssetExecutionContext, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.MaterializeResult:
    import mlflow

    import tripmodel
    from tripmodel import LightGBMModel, model_signature

    ts = _latest(context, "training_set")
    train = artifacts.read_parquet(ts["train_prefix"])
    distances = artifacts.read_parquet(ts["distances_prefix"])
    client = tracking.client()  # sets the tracking URI and the contract's experiment
    name = mlflow_contract()["registered_model"]
    tags = {
        "training_window": ts["training_window"],
        "holdout_month": ts["holdout_month"],
        "features_version": str(features_contract()["version"]),
        "git_sha": os.environ.get("GIT_SHA", "unknown"),
    }
    model = LightGBMModel().fit(train, distances)
    with mlflow.start_run(run_name=f"lightgbm {ts['training_window']}", tags=tags) as run:
        mlflow.log_params({"model": "LightGBMModel", "train_rows": len(train), "distance_pairs": len(distances), **model.params})
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model,
            signature=model_signature(),
            code_paths=[str(Path(tripmodel.__file__).parent)],  # loading needs no pipeline code
            registered_model_name=name,
        )
    version = str(info.registered_model_version)
    for k, v in tags.items():
        client.set_model_version_tag(name, version, k, v)
    context.log.info(f"registered {name} v{version} from run {run.info.run_id}")
    return dg.MaterializeResult(metadata={"version": version, "run_id": run.info.run_id, **tags})


@dg.asset(deps=[trained_model], group_name="model", kinds={"mlflow"})
def eval_report(context: dg.AssetExecutionContext, redshift: RedshiftData, artifacts: ArtifactBucket,
                tracking: MlflowTracking) -> dg.MaterializeResult:
    import mlflow
    from mlflow.exceptions import MlflowException

    from tripmodel import BaselineModel, evaluate

    ts, tm = _latest(context, "training_set"), _latest(context, "trained_model")
    holdout = artifacts.read_parquet(ts["holdout_prefix"])
    boroughs = {int(r["location_id"]): r["borough"] for r in redshift.query("select location_id, borough from ingest.taxi_zones")}
    client = tracking.client()
    name = mlflow_contract()["registered_model"]

    new = mlflow.pyfunc.load_model(f"models:/{name}/{tm['version']}")  # as serving would load it
    new_metrics = evaluate(new.predict, holdout, boroughs)
    try:
        champ = client.get_model_version_by_alias(name, "champion")
        old = mlflow.pyfunc.load_model(f"models:/{name}/{champ.version}")
        old_metrics, against = evaluate(old.predict, holdout, boroughs), f"champion v{champ.version}"
    except MlflowException:  # no champion yet: the v1 baseline, fit on the same window
        base = BaselineModel().fit(artifacts.read_parquet(ts["train_prefix"]), artifacts.read_parquet(ts["distances_prefix"]))
        old_metrics, against = evaluate(lambda df: base.predict(None, df), holdout, boroughs), "v1 baseline"

    logged = {**new_metrics, "champion_mae_min": old_metrics["mae_min"]}
    for k, v in logged.items():
        client.log_metric(tm["run_id"], k, v)
    for k in ("mae_min", "p95_latency_ms", "champion_mae_min"):
        client.set_model_version_tag(name, tm["version"], k, f"{logged[k]:.4f}")
    context.log.info(f"v{tm['version']} vs {against} on {ts['holdout_month']}: "
                     f"MAE {new_metrics['mae_min']:.3f} vs {old_metrics['mae_min']:.3f}")
    return dg.MaterializeResult(metadata={
        "version": tm["version"],
        "against": against,
        "holdout_month": ts["holdout_month"],
        **{f"new.{k}": float(v) for k, v in new_metrics.items()},
        **{f"champion.{k}": float(v) for k, v in old_metrics.items()},
    })


def gate(report: dict, policy: dict) -> tuple[bool, list[str]]:
    """Does the new version beat what it was compared against, within the latency budget?"""
    if _undecided(policy):
        return False, [f"gate not decided: {', '.join(_undecided(policy))} (training.GATE)"]
    metric, reasons = policy["metric"], []
    new, old = report.get(f"new.{metric}"), report.get(f"champion.{metric}")
    if new is None or old is None:
        reasons.append(f"{metric} is not in the report")
    elif old - new < float(policy["min_improvement_min"]):
        reasons.append(f"{metric} {new:.3f} vs {old:.3f}: {old - new:+.3f} min, needs at least {policy['min_improvement_min']}")
    latency = report.get("new.p95_latency_ms")
    if latency is None or latency > float(policy["max_p95_latency_ms"]):
        reasons.append(f"p95 latency {latency} ms over the budget of {policy['max_p95_latency_ms']} ms")
    return not reasons, reasons


@dg.asset_check(asset=eval_report, blocking=True,  # contracts/mlflow.yaml: pipeline.blocking_checks.eval_report
                description="The new version beats the champion (or v1) on the promotion metric, within the latency budget.")
def beats_champion(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    report = _latest(context, "eval_report")
    passed, reasons = gate(report, GATE)
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description="beats it" if passed else "; ".join(reasons),
        metadata={"version": str(report.get("version")), "against": str(report.get("against")),
                  "gate": dg.MetadataValue.json({k: v for k, v in GATE.items()}), "reasons": dg.MetadataValue.json(reasons)},
    )


@dg.asset(deps=[eval_report], group_name="model", kinds={"mlflow"})
def challenger(context: dg.AssetExecutionContext, tracking: MlflowTracking) -> dg.MaterializeResult:
    """Point the `challenger` alias at the version eval_report passed. Never `champion`."""
    report = _latest(context, "eval_report")
    # The blocking check skips this asset in a train_model run; re-checking here covers a run of this
    # asset on its own, which no check guards.
    passed, reasons = gate(report, GATE)
    if not passed:
        raise dg.Failure(f"v{report.get('version')} did not pass the gate: {'; '.join(reasons)}")
    contract = mlflow_contract()
    alias = "challenger"
    assert alias in contract["aliases"]
    client = tracking.client()
    # Recorded on the version, so promotion (S3, .github/workflows/promote.yaml) can refuse one that
    # never passed the gate, whatever alias it has by then.
    client.set_model_version_tag(contract["registered_model"], str(report["version"]), "gate", "passed")
    client.set_registered_model_alias(contract["registered_model"], alias, str(report["version"]))
    context.log.info(f"{alias} -> {contract['registered_model']} v{report['version']}")
    return dg.MaterializeResult(metadata={"alias": alias, "version": str(report["version"]), "against": str(report["against"])})


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger),
    description="Train v2 on the window, register it, compare it with the champion (or v1), and set challenger behind the gate.",
)
