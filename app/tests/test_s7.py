"""S7: which tool calls the assistant lets through (decision 017), and the tools prompt stays a fixed prefix."""

import rideapp.assistant as a

Q = "How long from JFK Airport to Times Square at 5pm on a Friday?"


def test_the_three_tools_are_allowed_and_nothing_else():
    assert a.ALLOWED_TOOLS == {"get_model_info", "predict_duration", "query_zone_stats"}
    assert a.allow_tool_call("get_model_info", {}, Q) == (True, "")
    ok, why = a.allow_tool_call("delete_everything", {}, Q)
    assert not ok and "isn't available" in why


def test_a_trip_between_the_zones_the_rider_named_is_allowed():
    args = {"pickup_zone": "JFK Airport", "dropoff_zone": "Times Sq/Theatre District", "hour": 17, "day_of_week": "friday"}
    assert a.allow_tool_call("predict_duration", args, Q) == (True, "")


def test_a_zone_the_rider_never_mentioned_is_refused():
    ok, why = a.allow_tool_call("query_zone_stats", {"zone": "Battery Park", "metric": "trip_count"}, Q)
    assert not ok and "Battery Park" in why
    ok, _ = a.allow_tool_call("predict_duration", {"pickup_zone": "JFK Airport", "dropoff_zone": "Newark Airport",
                                                   "hour": 17, "day_of_week": "friday"}, Q)
    assert ok is True  # "airport" is in the question: the check is coarse, by design (see decisions/017)


def test_stats_for_a_zone_the_rider_asked_about_are_allowed():
    q = "How many trips start at LaGuardia Airport around 8am?"
    assert a.allow_tool_call("query_zone_stats", {"zone": "LaGuardia Airport", "metric": "trip_count", "hour": 8}, q) == (True, "")


def test_tools_prompt_is_fixed_text():
    assert "predict_duration" in a.TOOLS_PROMPT and "{" not in a.TOOLS_PROMPT  # nothing per-request in the cached prefix
