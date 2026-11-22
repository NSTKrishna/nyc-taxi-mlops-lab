"""S2: the feature_contract check on trip_features.

contracts/features.yaml lists the model's inputs, label and the post-trip columns that must never be
inputs; contracts/mlflow.yaml says this check blocks. Decide what "matches" means: column names,
types, value ranges. Gameday E1 will test it.

What "matches" means here (review it: it is yours to defend):
- Columns: staging.trip_features has exactly the contract's keys, inputs and label, with the contract's
  types. A missing column, an extra column (whatever its name) or a changed type is a mismatch, and a
  forbidden (post-trip) column is reported as such. Order is not compared: training UNLOADs by name.
- Values: no row of the month is outside a column's contract range, or null. The range is the
  contract's, so any row outside it means the SQL or the data moved, not that a threshold was crossed.
"""

import re

import dagster as dg

from ..contracts import features_contract
from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from .features import trip_features

SCHEMA, TABLE = "staging", "trip_features"
NAME = re.compile(r"[a-z_][a-z0-9_]*")


def expected_columns(contract: dict) -> dict[str, str]:
    """name -> type, for every column the table must have (keys, inputs, label; not `derived`)."""
    cols = [*contract["keys"], *contract["inputs"], contract["label"]]
    return {c["name"]: c["type"].lower() for c in cols}


def normalize(data_type: str, char_len) -> str:
    """information_schema's type as the contract writes it: 'character', 7 -> 'char(7)'."""
    t = data_type.lower()
    if t == "character":
        return f"char({int(char_len)})"
    if t == "character varying":
        return f"varchar({int(char_len)})"
    return t


def column_differences(contract: dict, found: dict[str, str]) -> list[str]:
    """Every way the table's columns (name -> type) differ from the contract's."""
    want = expected_columns(contract)
    forbidden = {f.lower() for f in contract.get("forbidden", [])}
    diffs = []
    for name, typ in want.items():
        if name not in found:
            diffs.append(f"missing column {name} {typ}")
        elif found[name] != typ:
            diffs.append(f"{name}: expected {typ}, found {found[name]}")
    for name in sorted(set(found) - set(want)):
        kind = "forbidden post-trip column" if name in forbidden else "column not in the contract"
        diffs.append(f"{kind}: {name} {found[name]}")
    return diffs


def range_sql(contract: dict, month: str) -> str:
    """One query: per contract column with a range, the month's rows outside it (or null)."""
    parts = []
    for c in [*contract["inputs"], contract["label"]]:
        if "range" not in c:
            continue
        name = c["name"]
        if not NAME.fullmatch(name):  # names are formatted into SQL
            raise ValueError(f"not a column name: {name!r}")
        lo, hi = (float(x) for x in c["range"])
        low = f"{name} <= {lo}" if c.get("exclusive_min") else f"{name} < {lo}"
        parts.append(f"sum(case when {name} is null or {low} or {name} > {hi} then 1 else 0 end) as {name}")
    return (f"select count(*) as rows, {', '.join(parts)} from {SCHEMA}.{TABLE} "
            f"where trip_month = '{check_month(month)}'")


@dg.asset_check(
    asset=trip_features,
    partitions_def=trip_months,
    blocking=True,  # contracts/mlflow.yaml: pipeline.blocking_checks.trip_features
    description="trip_features has exactly the columns of contracts/features.yaml, and values in their ranges.",
)
def feature_contract(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    contract = features_contract()
    month = check_month(context.partition_key)
    cols = redshift.query(
        "select column_name, data_type, character_maximum_length from information_schema.columns "
        f"where table_schema = '{SCHEMA}' and table_name = '{TABLE}' order by ordinal_position")
    found = {r["column_name"].lower(): normalize(r["data_type"], r.get("character_maximum_length")) for r in cols}
    diffs = column_differences(contract, found)
    out_of_range: dict[str, int] = {}
    rows = 0
    if not diffs:  # ranges only mean something once the columns are the contract's
        r = redshift.query(range_sql(contract, month))[0]
        rows = int(r.get("rows") or 0)
        out_of_range = {k: int(v or 0) for k, v in r.items() if k != "rows" and int(v or 0)}
        diffs += [f"{k}: {n} row(s) outside the contract range" for k, n in out_of_range.items()]
        if rows == 0:
            diffs.append(f"no rows for {month}")
    return dg.AssetCheckResult(
        passed=not diffs,
        severity=dg.AssetCheckSeverity.ERROR,
        description="matches contracts/features.yaml" if not diffs else f"{len(diffs)} difference(s) from the contract",
        metadata={
            "differences": dg.MetadataValue.json(diffs),
            "columns": dg.MetadataValue.json(found),
            "rows": dg.MetadataValue.int(rows),
            "features_version": dg.MetadataValue.int(int(contract["version"])),
        },
    )
