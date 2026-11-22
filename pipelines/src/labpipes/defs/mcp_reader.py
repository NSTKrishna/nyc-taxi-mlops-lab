"""S7: the MCP server's database user. query_zone_stats must not run as labadmin: create a user that can
read the marts schema and nothing else (decision: database role scope), with the RedshiftData resource.
The MCP server connects as it through the Data API's --db-user path (MCP_DB_USER, default lab_ro), so it
needs no password. Remember the mart is refreshed: grants must survive that.

Decision 018: exactly one table. lab_ro gets USAGE on schema marts and SELECT on marts.zone_hour_stats,
nothing else; PUBLIC loses CREATE on schema public, so lab_ro can't create anything anywhere. The mart's
refresh keeps the table (delete + insert, never drop), so its grant would survive anyway; mcp_reader
still runs after every refresh in ingest_month and re-applies it, so a future drop-and-recreate can't
lose it. The blocking check reads the privileges back from Redshift.
"""

import os
import re

import dagster as dg

from ..resources import RedshiftData
from .marts import zone_hour_stats

USER = os.environ.get("MCP_DB_USER", "lab_ro")
TABLE = "marts.zone_hour_stats"
if not re.fullmatch(r"[a-z_][a-z0-9_]*", USER) or USER == "labadmin":  # formatted into GRANTs below
    raise ValueError(f"MCP_DB_USER must be a plain lower-case name and never labadmin, got {USER!r}")

GRANTS = [
    f"grant usage on schema marts to {USER}",
    f"grant select on {TABLE} to {USER}",
    "revoke create on schema public from public",
]

# What lab_ro must be able to do (True) and must not (False), as Redshift reports it.
EXPECTED = {
    "read_mart": True,
    "write_mart": False,
    "delete_mart": False,
    "create_in_marts": False,
    "use_ingest": False,
    "use_staging": False,
    "create_in_public": False,
}
PRIVILEGES_SQL = (
    f"select has_table_privilege('{USER}', '{TABLE}', 'select') as read_mart, "
    f"has_table_privilege('{USER}', '{TABLE}', 'insert') as write_mart, "
    f"has_table_privilege('{USER}', '{TABLE}', 'delete') as delete_mart, "
    f"has_schema_privilege('{USER}', 'marts', 'create') as create_in_marts, "
    f"has_schema_privilege('{USER}', 'ingest', 'usage') as use_ingest, "
    f"has_schema_privilege('{USER}', 'staging', 'usage') as use_staging, "
    f"has_schema_privilege('{USER}', 'public', 'create') as create_in_public"
)


def as_bool(v) -> bool:
    return v in (True, "true", "t", 1, "1")


@dg.asset(deps=[zone_hour_stats], group_name="marts", kinds={"redshift"}, pool="redshift")
def mcp_reader(context: dg.AssetExecutionContext, redshift: RedshiftData) -> dg.MaterializeResult:
    """The read-only user exists, with decision 018's grants (re-applied after every mart refresh)."""
    exists = redshift.scalar(f"select count(*) from pg_user where usename = '{USER}'")
    if not int(exists or 0):
        redshift.execute(f"create user {USER} password disable")  # Data API temporary credentials only
        context.log.info(f"created {USER}")
    redshift.execute(*GRANTS)
    got = {k: as_bool(v) for k, v in redshift.query(PRIVILEGES_SQL)[0].items()}
    return dg.MaterializeResult(metadata={"user": USER, **{k: dg.MetadataValue.bool(v) for k, v in got.items()}})


@dg.asset_check(asset=mcp_reader, blocking=True,
                description="lab_ro can read marts.zone_hour_stats and nothing more: no writes, no ingest or staging, no create.")
def mcp_reader_least_privilege(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    got = {k: as_bool(v) for k, v in redshift.query(PRIVILEGES_SQL)[0].items()}
    wrong = sorted(k for k, want in EXPECTED.items() if got.get(k) != want)
    return dg.AssetCheckResult(
        passed=not wrong,
        severity=dg.AssetCheckSeverity.ERROR,
        description="read-only on the mart, nothing else" if not wrong else "unexpected privileges: " + ", ".join(f"{k}={got.get(k)}" for k in wrong),
        metadata={k: dg.MetadataValue.bool(v) for k, v in got.items()},
    )


# By hand, without re-running a month: scripts/dagster-run.sh grant_mcp_reader --wait
grant_mcp_reader = dg.define_asset_job("grant_mcp_reader", selection=dg.AssetSelection.assets(mcp_reader),
                                       description="Create lab_ro if missing and re-apply its grants (decision 018).")
