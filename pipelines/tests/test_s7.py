"""S7: the MCP server's read-only database user (decision 018), against the fake Redshift."""

import dagster as dg
from fakes import LOG, feature_redshift

from labpipes.defs import mcp_reader as m


def _run(redshift):
    defs = dg.Definitions(assets=[m.mcp_reader], asset_checks=[m.mcp_reader_least_privilege], jobs=[m.grant_mcp_reader],
                          resources={"redshift": redshift})
    result = defs.resolve_job_def("grant_mcp_reader").execute_in_process(instance=dg.DagsterInstance.ephemeral(), raise_on_error=False)
    return {e.asset_check_evaluation_data.check_name: e.asset_check_evaluation_data.passed
            for e in result.all_events if e.event_type_value == "ASSET_CHECK_EVALUATION"}


def test_grants_are_exactly_one_table_and_no_create_in_public():
    _run(feature_redshift())
    assert "grant usage on schema marts to lab_ro" in LOG and "grant select on marts.zone_hour_stats to lab_ro" in LOG
    assert "revoke create on schema public from public" in LOG
    assert not any(s.startswith("grant") and ("ingest" in s or "staging" in s or "all tables" in s) for s in LOG)


def test_least_privilege_check_passes_only_when_redshift_agrees():
    assert _run(feature_redshift())["mcp_reader_least_privilege"] is True
    leaky = dict(m.EXPECTED, use_ingest=True)
    assert _run(feature_redshift(privileges=leaky))["mcp_reader_least_privilege"] is False
    writable = dict(m.EXPECTED, write_mart=True)
    assert _run(feature_redshift(privileges=writable))["mcp_reader_least_privilege"] is False


def test_the_check_is_blocking_and_the_user_is_never_labadmin():
    assert next(iter(m.mcp_reader_least_privilege.check_specs)).blocking
    assert m.USER == "lab_ro" and m.USER != "labadmin"
