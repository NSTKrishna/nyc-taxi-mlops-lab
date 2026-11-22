# 011: What triggers the blue-green switch

Stage: S3 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

The CD job switches automatically (`SWITCH: auto` in `.github/workflows/cd.yaml`). Every push to `main` that passes `make check` builds an image, deploys it to the idle colour, and smoke-tests that colour through Traefik, pinned to it with `X-Lab-Color`. The checks are `/version` returning this commit's `git_sha`, and `/predict` answering a sample trip. If both pass, the job flips all app traffic to the new colour. The old colour keeps running, and `switch.yaml` sends traffic back to it with no deploy.

## Rejected alternative

A manual switch (`SWITCH: manual`): CD deploys and smoke-tests the idle colour, then a person runs `gh workflow run switch.yaml` to flip it. It puts a human look between "the new version runs" and "riders get it", but every release then waits for someone, and an unattended push sits idle and unused.

## Why

<!-- Student: confirm or rewrite this in your own words. -->
The gates before the switch are automated, and the rollback is cheap: CI runs `make check`, and the smoke test proves the new colour runs this exact commit and can quote a trip. A bad release that gets past them is undone with one `switch.yaml` run, because the previous colour is still up (measured switch time: ___ s, from the S3 rollback run). A person clicking "switch" after the smoke test passes would mostly be re-checking what the smoke test already checked.

Known gap: the smoke test's `/predict` passes on *any* answer, including the static-median fallback. So a release that breaks the app's call to serving would still be switched to, and riders would quietly get 12.25 minutes. The smoke test doesn't check `model.alias`, because before the first promotion there is no champion to expect.

## What would show this was wrong

A release that passed the smoke test but hurt riders after the switch: more 5xx, a jump in static-median fallbacks (S5's `lab_predictions_total` by source), or a rollback within minutes of a deploy. If that happens more than once, either strengthen the smoke test (for example require `model.alias == "champion"` once a champion exists), or move to `manual`, or add a canary step before the full switch.
