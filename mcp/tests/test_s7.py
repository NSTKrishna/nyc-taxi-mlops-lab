"""S7: predict_duration and query_zone_stats, with serving and the warehouse faked."""

import json

import pytest
from mcp.server.fastmcp.exceptions import ToolError

import labmcp.server as s


async def call(name: str, args: dict):
    return await s.mcp.call_tool(name, args)


def payload(result) -> dict:
    return json.loads(result[0].text)


async def test_predict_duration_sends_features_yaml_encodings(monkeypatch):
    seen = []

    async def champion(method, path, body=None):
        seen.append((method, path, body))
        return {"duration_minutes": 63.3, "model": {"name": "trip-duration", "version": "2", "alias": "champion"}}

    monkeypatch.setattr(s, "champion", champion)
    out = payload(await call("predict_duration", {"pickup_zone": "JFK Airport", "dropoff_zone": "Times Sq/Theatre District",
                                                  "hour": 17, "day_of_week": "friday"}))
    assert seen == [("POST", "/predict", {"pickup_zone_id": 132, "dropoff_zone_id": 230, "pickup_hour": 17, "pickup_dow": 5})]
    assert out["duration_minutes"] == 63.3 and out["model"]["alias"] == "champion"
    assert (out["pickup_zone_id"], out["dropoff_zone_id"]) == (132, 230)


async def test_predict_duration_refuses_an_unknown_zone_with_suggestions(monkeypatch):
    async def champion(*a, **k):
        raise AssertionError("serving must not be called for an unknown zone")

    monkeypatch.setattr(s, "champion", champion)
    with pytest.raises(ToolError, match="did you mean"):
        await call("predict_duration", {"pickup_zone": "JFK Airprot", "dropoff_zone": "Times Sq/Theatre District", "hour": 9, "day_of_week": "monday"})


class FakeWarehouse:
    calls: list = []

    def __init__(self, *a, **k):
        pass

    def query(self, sql, params=None, timeout_s=30):
        FakeWarehouse.calls.append((sql, params))
        hours = [params["hour"]] if params and "hour" in params else [7, 8]
        return [{"pickup_hour": h, "value": 41.234, "first_month": "2024-01", "last_month": "2024-04"} for h in hours]


@pytest.fixture
def warehouse(monkeypatch):
    FakeWarehouse.calls = []
    monkeypatch.setattr(s, "Warehouse", FakeWarehouse)
    return FakeWarehouse


async def test_query_zone_stats_uses_named_parameters_only(warehouse):
    out = payload(await call("query_zone_stats", {"zone": "JFK Airport", "metric": "median_duration_min", "hour": 17}))
    sql, params = warehouse.calls[0]
    assert params == {"zone_id": 132, "hour": 17}
    assert ":zone_id" in sql and ":hour" in sql and "132" not in sql and "17" not in sql  # values never in the SQL text
    assert sql.startswith("select pickup_hour, median_duration_min as value") and "from marts.zone_hour_stats" in sql
    assert out == {"zone": "JFK Airport", "zone_id": 132, "metric": "median_duration_min", "months": "2024-01..2024-04",
                   "by_hour": [{"hour": 17, "median_duration_min": 41.23}]}


async def test_query_zone_stats_without_hour_returns_every_hour(warehouse):
    out = payload(await call("query_zone_stats", {"zone": "jfk airport", "metric": "trip_count"}))
    sql, params = warehouse.calls[0]
    assert params == {"zone_id": 132} and ":hour" not in sql
    assert [h["hour"] for h in out["by_hour"]] == [7, 8]


async def test_injected_text_in_a_zone_name_never_reaches_sql(warehouse):
    with pytest.raises(ToolError, match="unknown TLC zone"):
        await call("query_zone_stats", {"zone": "JFK Airport'; drop table marts.zone_hour_stats; --", "metric": "trip_count"})
    assert warehouse.calls == []


async def test_a_metric_outside_the_enum_is_refused(warehouse):
    with pytest.raises(ToolError, match="Input should be 'trip_count', 'median_duration_min' or 'p90_duration_min'"):
        await call("query_zone_stats", {"zone": "JFK Airport", "metric": "pickup_zone_id from ingest.raw_trips --"})
    assert warehouse.calls == []
