"""S1: the raw_trips asset.

One partition per month (partitions.trip_months), depending on trip_files. COPY the month's file into
Redshift with the LabRole attached to the cluster (`iam_role default`), so that re-running a month never
duplicates its rows. The landing columns come from contracts/trips.schema.yaml
(contracts.redshift_columns). Decision points: COPY vs Spectrum; dist and sort keys.

Idempotency: one transaction COPYs the file into a temp table, deletes the month's earlier rows from
ingest.raw_trips (by source_file) and inserts the new ones. A failure anywhere rolls all of it back, so
a month is either fully replaced or untouched. COPY goes through a temp table because Parquet COPY maps
columns by position and the landing table has one more column (source_file) than the file.
"""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import source_file, trip_months
from ..resources import DataBucket, RedshiftData

# Decision: dist and sort keys for ingest.raw_trips (decisions/, S1). Pending: until it's decided this
# stays empty, which is Redshift's default (DISTSTYLE AUTO, SORTKEY AUTO). Example shape:
# "diststyle even sortkey (source_file, tpep_pickup_datetime)". The table is created once, so a later
# change needs ALTER TABLE ... ALTER DISTSTYLE / ALTER SORTKEY (or a drop and reload of every month).
TABLE_KEYS = ""


def load_statements(uri: str, file: str) -> list[str]:
    """One month's load, as statements for a single transaction. `file` comes from source_file()."""
    cols = redshift_columns(trips_schema())
    return [
        "create schema if not exists ingest",
        f"create table if not exists ingest.raw_trips ({cols}, source_file varchar(64) not null) {TABLE_KEYS}".rstrip(),
        f"create temp table landing ({cols})",
        f"copy landing from '{uri}' iam_role default format as parquet",
        f"delete from ingest.raw_trips where source_file = '{file}'",
        f"insert into ingest.raw_trips select *, '{file}' from landing",
        "drop table landing",
    ]


@dg.asset(partitions_def=trip_months, deps=["trip_files"], group_name="ingest", kinds={"redshift"}, pool="redshift")
def raw_trips(context: dg.AssetExecutionContext, redshift: RedshiftData, data_bucket: DataBucket) -> dg.MaterializeResult:
    month = context.partition_key
    file = source_file(month)  # validates the month: it ends up in SQL text
    uri = data_bucket.trip_uri(month)
    redshift.execute(*load_statements(uri, file))
    rows = redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'")
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.int(int(rows)), "source": uri})
