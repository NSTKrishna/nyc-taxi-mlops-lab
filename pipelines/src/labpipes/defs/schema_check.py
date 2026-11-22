"""S1: the schema_contract check on trip_files.

Compare the landed file's Parquet footer (the DataBucket resource reads it) with
contracts/trips.schema.yaml, before raw_trips copies anything. contracts/platform.yaml says this check
is blocking. Patterns: pipelines/reference/patterns.py.

What counts as a mismatch follows the contract's own rules: Parquet COPY maps columns by position, so
the column count and order must match exactly; names compare case-insensitively; storage types must be
equal. An extra trailing column (2025's cbd_congestion_fee) is a mismatch, not a warning: COPY would
fail on the count, or with a column dropped elsewhere, load shifted values without any error.
"""

import dagster as dg

from ..contracts import Column, trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


def schema_differences(expected: list[Column], found: list[Column]) -> list[str]:
    """Every way the file's columns differ from the contract's, position by position."""
    diffs = []
    for i in range(max(len(expected), len(found))):
        want = expected[i] if i < len(expected) else None
        got = found[i] if i < len(found) else None
        if want is None:
            diffs.append(f"#{i + 1}: unexpected column {got.name} {got.type}")
        elif got is None:
            diffs.append(f"#{i + 1}: missing column {want.name} {want.type}")
        elif want.name.lower() != got.name.lower():
            diffs.append(f"#{i + 1}: expected {want.name}, found {got.name}")
        elif want.type != got.type:
            diffs.append(f"#{i + 1}: {want.name} expected {want.type}, found {got.type}")
    return diffs


@dg.asset_check(
    asset=trip_files,
    partitions_def=trip_months,
    blocking=True,  # contracts/platform.yaml: blocking_checks.trip_files
    description="The file's Parquet footer matches contracts/trips.schema.yaml (order, names, storage types).",
)
def schema_contract(context: dg.AssetCheckExecutionContext, data_bucket: DataBucket) -> dg.AssetCheckResult:
    month = context.partition_key
    found = data_bucket.footer(month).columns
    diffs = schema_differences(trips_schema(), found)
    return dg.AssetCheckResult(
        passed=not diffs,
        severity=dg.AssetCheckSeverity.ERROR,
        description="matches the contract" if not diffs else f"{len(diffs)} difference(s) from the contract",
        metadata={
            "differences": dg.MetadataValue.json(diffs),
            "expected_columns": dg.MetadataValue.int(len(trips_schema())),
            "found_columns": dg.MetadataValue.int(len(found)),
        },
    )
