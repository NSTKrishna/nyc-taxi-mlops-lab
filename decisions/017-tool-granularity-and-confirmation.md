# 017: Tool granularity and confirmation

Stage: S7 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

The assistant may call all three of the MCP server's tools: `get_model_info`, `predict_duration` and `query_zone_stats` (`ALLOWED_TOOLS` in `app/src/rideapp/assistant.py`). None needs a person to confirm it, because all three only read. Every call still goes through `allow_tool_call` before it runs: a call may only name zones the rider mentioned in their own question (each zone argument has to share a significant word with the question, so "Times Square" allows `Times Sq/Theatre District`). Anything else is refused, and the refusal is recorded in `tool_calls` and the trace.

Each tool does one narrow thing with typed arguments from `contracts/mcp-tools.json`. The tools can't run arbitrary queries: `query_zone_stats` picks one of six fixed statements, and passes the zone id and hour as named parameters.

## Rejected alternative

Leave `query_zone_stats` out (predict and model info only). It's the one tool that touches the warehouse, so dropping it is the smallest surface for an injected wiki page to use. But riders lose historical zone statistics, and the tool can only read one table through a read-only user anyway (decision 018).

## Why

<!-- Student: confirm or rewrite this in your own words. -->
A confirmation step earns its cost when a call changes something: it moves money, writes data, or messages someone. None of these tools does. The worst a bad call can do is read one row set from `marts.zone_hour_stats`, which is aggregate, public-source data. Asking a rider to confirm a read would add a round trip (and the app has no confirmation UI) without protecting anything.

The risk that remains is the *model* being steered into calls the rider never asked for, by text it read in a wiki excerpt or a tool result. `allow_tool_call` sees the rider's own question, which the model can't rewrite, so a call about other zones is refused there, whatever the prompt says.

The check is coarse by design: it matches words, not meaning. "Airport" in a question allows any airport zone, and the check doesn't look at the metric or the hour.

If a tool could write (say, `refund_trip` or `update_wiki`): it would need its own database role with only that write; a person to confirm every call, outside the model's loop, with the arguments shown; the call made idempotent with a request id; and the tool kept out of `ALLOWED_TOOLS` until both are in place.

## What would show this was wrong

A trace where a tool ran for something the rider didn't ask (the check is too loose: tighten it, or add a check on the metric); legitimate trip questions refused because the rider wrote a zone in a way the words don't match (too strict: map aliases); or a tool being added that writes.
