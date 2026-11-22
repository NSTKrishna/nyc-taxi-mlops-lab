# 010: Load at start-up or reload on alias change

Stage: S3 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Serving picks up a new champion through `POST /reload` (`PICKUP: reload` in `.github/workflows/promote.yaml`, which calls `deploy/web.sh reload champion`). After the promotion workflow moves the `champion` alias, it asks the running `serving-champion` container to resolve the alias again. The container loads and warms up the new version in the background, and the old version keeps answering every request until the swap. The workflow then waits until `/model` reports the new version, and checks `/predict` through serving and through the app.

## Rejected alternative

Restart the container (`PICKUP: restart`): `serving-champion` loads whatever the alias points at when it starts. It's the simplest model, because what runs is always what the alias said at start-up. But between the stop and the new model being loaded, there is no champion. Traefik's health check on `/model` takes the container out, and the app falls back to the static median (12.25 minutes) for every rider until it's back.

## Why

<!-- Student: confirm or rewrite this in your own words, and fill in the numbers. -->
With `reload`, a rider never notices a promotion: every request gets an answer from a model, the old one until the instant of the swap, then the new one. With `restart`, every promotion is a short outage of the real model, a few seconds of static-median quotes, and it happens exactly when we are changing the most important thing serving does. Observed on the first promotion: reload took ___ s from request to the new version answering, with ___ static-median fallbacks during it.

The cost: a long-running container holds two models in memory for a moment during the swap. And what it serves now depends on its history (start-up load, then any reloads), not only on the alias at start-up, so `/model` is the source of truth for what is actually served. A container that restarts for another reason still loads the alias at start-up, so the two paths agree.

## What would show this was wrong

A reload that leaves the container serving a different version than the alias (for example `/reload` failing quietly while `/model` still shows the old one); memory pressure on the app host during the double-model moment; or fallbacks to the static median during a promotion (S5's `lab_predictions_total` by source). Any of these would argue for restarting, or for loading the new version into the idle `serving-challenger` slot and shifting Traefik's weights instead.
