"""Writes my-slos.json: a focused dashboard with only what I built in S2, S3 and S5: the three SLOs
and their burn rates, /predict (p95 against the 250 ms objective), model vs fallback, the champion.

    python3 observability/dashboards/build-my-slos.py

Import my-slos.json in Grafana (Dashboards -> New -> Import) and pick your Prometheus data source.
Every query uses metrics that budget.alloy keeps and the recording rules in rules/slo.rules.yaml.
"""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "${datasource}"}
PREDICT = 'route="/predict"'
panels: list[dict] = []
y = 0


def nid() -> int:
    return len(panels) + 1


def row(title: str) -> None:
    global y
    panels.append({"id": nid(), "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}})
    y += 1


def target(expr: str, legend: str, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": DS, "expr": expr, "legendFormat": legend}


def stat(title: str, x: int, w: int, h: int, targets: list[dict], unit: str, steps: list[tuple], decimals=None,
         description: str = "", text_mode: str = "value") -> dict:
    p = {
        "id": nid(), "type": "stat", "title": title, "description": description, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}},
                        "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "background", "graphMode": "area", "textMode": text_mode, "justifyMode": "center"},
        "targets": targets,
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    panels.append(p)
    return p


def series(title: str, x: int, w: int, h: int, targets: list[dict], unit: str, threshold=None, description: str = "",
           stack: bool = False) -> dict:
    custom = {"lineWidth": 2, "fillOpacity": 10}
    defaults = {"unit": unit, "min": 0, "custom": custom}
    if stack:
        custom.update({"stacking": {"mode": "normal"}, "fillOpacity": 60})
    if threshold is not None:
        custom["thresholdsStyle"] = {"mode": "line+area"}
        defaults["thresholds"] = {"mode": "absolute", "steps": [{"color": "transparent", "value": None}, {"color": "red", "value": threshold}]}
    p = {
        "id": nid(), "type": "timeseries", "title": title, "description": description, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi"}},
        "targets": targets,
    }
    panels.append(p)
    return p


# --- Right now -------------------------------------------------------------------------------------
row("Right now: what a rider gets")
GOOD = [("red", None), ("green", 0.99)]
stat("Champion model", 0, 4, 5, [target('max by (version) (lab_model_info{alias="champion"})', "v{{version}}")], "none",
     [("blue", None)], description="The version serving-champion has loaded (lab_model_info). Empty: no champion, riders get the static median.",
     text_mode="name")
stat("Quotes per second", 4, 4, 5, [target("sum(rate(lab_predictions_total[5m]))", "quotes/s")], "reqps", [("blue", None)], decimals=2)
stat("From the model (5 min)", 8, 4, 5,
     [target('sum(rate(lab_predictions_total{source="model"}[5m])) / sum(rate(lab_predictions_total[5m]))', "model")],
     "percentunit", GOOD, decimals=1, description="Share of quotes the model produced, not the static-median fallback. SLO: 99%.")
stat("/predict p95 (5 min)", 12, 4, 5,
     [target(f'histogram_quantile(0.95, sum by (le) (rate(lab_http_request_duration_seconds_bucket{{{PREDICT}}}[5m])))', "p95")],
     "s", [("green", None), ("orange", 0.2), ("red", 0.25)], decimals=3, description="Inside the app. SLO: 99% within 250 ms.")
stat("/predict without a 5xx (5 min)", 16, 4, 5,
     [target(f'1 - (sum(rate(lab_http_requests_total{{{PREDICT}, code=~"5.."}}[5m])) or vector(0)) / sum(rate(lab_http_requests_total{{{PREDICT}}}[5m]))', "ok")],
     "percentunit", GOOD, decimals=2, description="SLO: 99%.")
stat("Serving p95 (5 min)", 20, 4, 5,
     [target('histogram_quantile(0.95, sum by (le) (rate(lab_model_predict_duration_seconds_bucket{alias="champion"}[5m])))', "p95")],
     "s", [("green", None), ("orange", 0.025), ("red", 0.05)], decimals=4,
     description="One /predict inside serving-champion, model call included. The promotion gate allows 50 ms (decisions/009).")
y += 5

# --- SLOs ------------------------------------------------------------------------------------------
row("SLOs (observability/slo.yaml): 99% over 1 day, page on 14.4x burn over 1 h and 5 min")
SLOS = [("predict_availability", "Availability", "/predict without a 5xx"),
        ("predict_latency", "Latency", "/predict within 250 ms"),
        ("predict_model_answered", "Model answered", "quotes from the model, not the fallback")]
for i, (rec, name, what) in enumerate(SLOS):
    stat(f"{name}: last hour", i * 8, 8, 5, [target(f"1 - slo:{rec}:error_ratio_rate1h", what)], "percentunit", GOOD, decimals=2,
         description=f"{what}. Objective 99%.")
y += 5
for i, (rec, name, _) in enumerate(SLOS):
    series(f"{name}: burn rate", i * 8, 8, 7,
           [target(f"slo:{rec}:error_ratio_rate1h / 0.01", "1 h", "A"), target(f"slo:{rec}:error_ratio_rate5m / 0.01", "5 min", "B")],
           "none", threshold=14.4, description="Pages when both lines are above 14.4 (the red area) for a minute.")
y += 7

# --- /predict ---------------------------------------------------------------------------------------
row("/predict")
def q(p: float) -> str:
    return f'histogram_quantile({p}, sum by (le) (rate(lab_http_request_duration_seconds_bucket{{{PREDICT}}}[5m])))'



series("/predict latency inside the app (p50, p95, p99)", 0, 12, 8,
       [target(q(0.5), "p50", "A"), target(q(0.95), "p95", "B"), target(q(0.99), "p99", "C")], "s", threshold=0.25,
       description="The red line is the latency SLO's 250 ms.")
series("Quotes per second: model or fallback", 12, 12, 8,
       [target('sum by (source) (rate(lab_predictions_total[1m]))', "{{source}}")], "reqps", stack=True,
       description="fallback = the static median (12.25 min): serving was down, slow (300 ms timeout) or had no champion.")
y += 8
series("/predict responses by code", 0, 12, 7,
       [target(f'sum by (code) (rate(lab_http_requests_total{{{PREDICT}}}[1m]))', "{{code}}")], "reqps", stack=True)
series("The app's calls to serving, by outcome", 12, 12, 7,
       [target('sum by (outcome) (rate(lab_serving_request_duration_seconds_count[1m]))', "{{outcome}}")], "reqps", stack=True,
       description="ok, timeout (over 300 ms), unavailable (503: no model), error.")
y += 7

# --- Models -----------------------------------------------------------------------------------------
row("Models (S2, S3)")
series("Serving latency p95, by alias and version", 0, 12, 7,
       [target('histogram_quantile(0.95, sum by (le, alias, version) (rate(lab_model_predict_duration_seconds_bucket[5m])))', "{{alias}} v{{version}}")],
       "s", threshold=0.05, description="Inside serving. The red line is the promotion gate's 50 ms budget (decisions/009).")
series("Model reloads, by alias and outcome", 12, 12, 7,
       [target('sum by (alias, outcome) (increase(lab_model_reloads_total[5m]))', "{{alias}} {{outcome}}")], "none",
       description="A promotion shows as champion changed (PICKUP: reload, decisions/010).")
y += 7

dashboard = {
    "title": "Ride app: my SLOs and models",
    "uid": "lab-my-slos",
    "editable": True,
    "schemaVersion": 39,
    "time": {"from": "now-1h", "to": "now"},
    "refresh": "30s",
    "tags": ["mlops-lab"],
    "templating": {"list": [{"name": "datasource", "type": "datasource", "query": "prometheus", "label": "Prometheus", "current": {}}]},
    "annotations": {"list": []},
    "panels": panels,
}
out = Path(__file__).with_name("my-slos.json")
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out.name}: {len(panels)} panels")
