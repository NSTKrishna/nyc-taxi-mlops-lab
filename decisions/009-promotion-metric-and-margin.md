# 009: Promotion metric and margin

Stage: S2 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

A new version becomes `challenger` only if, on the same holdout month, its overall holdout MAE (`mae_min`) is at least **0.1 minutes (6 s) lower** than the champion's, or the v1 baseline's before there is a champion, **and** its single-row p95 latency is at most **50 ms**. This is `GATE` in `pipelines/src/labpipes/defs/training.py`, enforced by the blocking `beats_champion` check and re-checked by the `challenger` asset.

## Rejected alternative

Any improvement (margin `0`). It promotes every model that is better by any amount, however small. On a 3.5M-trip holdout a gain of a few hundredths of a minute is within run-to-run noise, so champions would churn without riders noticing a difference.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
First training run (version 1, trained on 2024-01..03, holdout 2024-04, 3,476,146 trips), against the v1 baseline:

| | v2 LightGBM | v1 baseline | difference |
|---|---|---|---|
| `mae_min` | 3.640 | 3.956 | −0.317 min (8.0%) |
| airport | 5.975 | 6.724 | −0.749 |
| Manhattan | 3.153 | 3.341 | −0.188 |
| outer boroughs | 7.498 | 9.193 | −1.695 |
| night (0–5 h) | 2.623 | 2.872 | −0.249 |
| p95 latency | 3.64 ms | 0.91 ms | +2.7 ms |

Overall MAE is what the average rider feels, and absolute error in minutes matches how riders read a quote; a few 3-hour trips don't dominate it the way they would RMSE. A 0.1-minute margin is small enough that today's real gain (0.317) passes easily, and large enough to filter out noise-level "wins". 50 ms leaves room for heavier models, and it stays well under the app's 300 ms serving timeout (`SERVING_TIMEOUT_SECONDS`), past which riders get the static median anyway. The in-process p95 doesn't include the app → Traefik → serving hop.

Known gap: a single overall metric can hide a slice getting worse. Here v2 is better on every slice, but the gate doesn't require that.

## What would show this was wrong

A promoted version that is better overall but clearly worse on one slice (for example airport) and draws complaints; champions changing often with no visible improvement (margin too small); or a clearly better model that keeps failing by a hair (margin too large). Serving's p95 in production (S5 metrics) running far above the in-process number would mean the latency budget measures the wrong thing.
