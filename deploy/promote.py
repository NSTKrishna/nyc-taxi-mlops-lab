"""Promotion steps (S3), run by .github/workflows/promote.yaml inside a serving container, which has
mlflow, PyYAML and MLFLOW_TRACKING_URI. This is the only code that moves `champion`; it never sets
`challenger` (the pipeline does, behind its gate).

    python - resolve [VERSION]       the version to promote: VERSION, or what `challenger` points at
    python - check VERSION           fail unless the pipeline evaluated VERSION and it passed the gate
    python - move VERSION RUN_URL    record the outgoing champion, then point `champion` at VERSION;
                                     prints the outgoing version, or "none"
    python - restore OLD             point `champion` back at OLD ("none": remove the alias)

The contract (contracts/mlflow.yaml) comes in the CONTRACT environment variable, as YAML text.
"""

import os
import re
import sys
from datetime import UTC, datetime

import yaml
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

CONTRACT = yaml.safe_load(os.environ["CONTRACT"])
NAME = CONTRACT["registered_model"]
VERSION = re.compile(r"[1-9][0-9]*")


def fail(msg: str) -> None:
    print(f"::error::{msg}")
    sys.exit(1)


def alias_version(client: MlflowClient, alias: str) -> str | None:
    try:
        return str(client.get_model_version_by_alias(NAME, alias).version)
    except MlflowException:
        return None


def resolve(client: MlflowClient, version: str = "") -> str:
    if version:
        VERSION.fullmatch(version) or fail(f"not a version number: {version!r}")
        return version
    return alias_version(client, "challenger") or fail(f"{NAME} has no challenger: name a version, or run train_model")


def check(client: MlflowClient, version: str) -> None:
    try:
        mv = client.get_model_version(NAME, version)
    except MlflowException:
        fail(f"{NAME} v{version} doesn't exist")
    missing = [t for t in CONTRACT["version_tags"] if t not in mv.tags]
    if missing:
        fail(f"{NAME} v{version} wasn't evaluated by the pipeline: no {', '.join(missing)} tag(s)")
    if mv.tags.get("gate") != "passed":
        fail(f"{NAME} v{version} never passed the pipeline's gate (no gate=passed tag): it was never a challenger")
    print(f"v{version}: trained on {mv.tags['training_window']}, holdout {mv.tags['holdout_month']}, "
          f"MAE {mv.tags['mae_min']} vs {mv.tags['champion_mae_min']}, p95 {mv.tags['p95_latency_ms']} ms, "
          f"commit {mv.tags['git_sha'][:7]}")


def move(client: MlflowClient, version: str, run_url: str) -> None:
    old = alias_version(client, "champion")
    if old == version:
        fail(f"v{version} is already champion")
    now = datetime.now(UTC).isoformat(timespec="seconds")
    # The outgoing champion is recorded on the new one: a model rollback (S8) points champion back at it.
    client.set_model_version_tag(NAME, version, "previous_champion", old or "none")
    client.set_model_version_tag(NAME, version, "promoted_at", now)
    client.set_model_version_tag(NAME, version, "promoted_by", run_url)
    if old:
        client.set_model_version_tag(NAME, old, "retired_at", now)
    client.set_registered_model_alias(NAME, "champion", version)
    print(old or "none")


def restore(client: MlflowClient, old: str) -> None:
    if old == "none":
        client.delete_registered_model_alias(NAME, "champion")
        print("champion alias removed (there was none before)")
    else:
        VERSION.fullmatch(old) or fail(f"not a version number: {old!r}")
        client.set_registered_model_alias(NAME, "champion", old)
        client.delete_model_version_tag(NAME, old, "retired_at")
        print(f"champion -> v{old} again")


def main(argv: list[str]) -> None:
    client = MlflowClient()
    match argv:
        case ["resolve", *rest]:
            print(resolve(client, rest[0] if rest else ""))
        case ["check", version]:
            check(client, version)
        case ["move", version, run_url]:
            move(client, version, run_url)
        case ["restore", old]:
            restore(client, old)
        case _:
            fail(f"usage: {__doc__.splitlines()[3].strip()} ...")


if __name__ == "__main__":
    main(sys.argv[1:])
