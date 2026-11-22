# 013: SLO targets

Stage: S5 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Three SLOs on `/predict`, each **99% over a 1-day window** (`observability/slo.yaml`, rules in `observability/rules/slo.rules.yaml`):

| SLO | Good event | Objective |
| --- | --- | --- |
| availability | `/predict` answered without a 5xx | 99% |
| latency | `/predict` answered within **250 ms** | 99% |
| model answered | the quote came from the model, not the static-median fallback | 99% |

Each pages on a **fast burn**: the error ratio above 14.4x the allowed 1% (0.144) over both the last hour and the last 5 minutes, for 1 minute. promtool tests (`slo.test.yaml`) prove that an outage pages and a 2-minute spike doesn't. A total outage after an hour of good traffic pages **10 minutes in**.

## Rejected alternative

99.9%, which allows about 1.4 minutes of total failure a day. One restore after Start Lab (RDS alone took 2 minutes on 2026-11-22), or one first blue-green deploy with Traefik's ~15 s gap plus a slow promotion, would spend it all. The alert would page for things this lab does every day by design.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
The three SLIs are three different ways a rider is let down: no answer (5xx), a slow answer, and a **wrong answer with HTTP 200**, the static median when serving is down. The last one is the most important and the easiest to miss: availability stays at 100% while every rider gets 12.25 minutes, and the promtool test shows exactly that case. 250 ms sits just under the app's 300 ms serving timeout, so a request that waited for serving and then fell back counts as slow too.

99% over a day is a budget of about 14.4 minutes of total failure, which fits what a two-host Learner Lab really does: restores, deploys, reloads.

The cost of the alert design (14.4x on 1 h and 5 m comes from 30-day SLOs): with a 1-day window, the hour window only crosses 14.4% after about 8.6 minutes of total outage, so the page arrives with about 60% of the day's budget already spent. A lower factor (6x pages after about 3.6 minutes) would catch it sooner, but small bursts would come closer to paging.

## What would show this was wrong

An outage riders noticed that didn't page, or paged too late to act within the day's budget (then lower the burn factor, or add a slow-burn warning); pages for things nobody would fix (a stricter-than-needed objective); or the latency SLO burning every time a burstable instance runs out of CPU credits (then 250 ms is measuring the instance type, not the app).
