# 014: Series budget

Stage: S5 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

Keep only what a dashboard, an SLO or an incident uses (`observability/alloy/budget.alloy`), and stay **under 5,000 active series** for all hosts together, half of Grafana Cloud's free 10,000.

| Source | Kept | Dropped |
| --- | --- | --- |
| host (node exporter) | CPU, available and total memory, disk space of real filesystems, network bytes on the host's own interface, `up` | everything else (node-exporter self-metrics, per-collector timings, tmpfs and overlay filesystems, Docker bridges and veth pairs) |
| containers (cAdvisor) | CPU and working-set memory per named container, `up` | everything else (`container_fs_*` disk I/O and the rest), system cgroups with no container name, and the `id` and `image` labels |
| services | all `lab_*` (contracts/metrics.yaml), `traefik_service_*`, `up` | runtime metrics (`go_*`, `process_*`, `python_*`), Traefik's entrypoint and config metrics |

## Rejected alternative

Drop only the noisy families (`container_fs_*`, `node_scrape_collector_*`) and the per-deploy labels, and keep everything else, about 2,000 series today. An incident nobody planned for would have more context to dig through.

## Why

<!-- Student: confirm or rewrite this in your own words, and fill in the after numbers. -->
Before any rules, app and platform sent about **3,445** series: cAdvisor 1,708 (half; `container_fs_*` alone is about 12 metrics × 72-83 series), node 1,273 (`node_scrape_collector_*` alone 184), Traefik 223, app 190, serving 42. The dashboard reads 9 metrics and the SLO rules read only `lab_*`. S6 adds the LLM host, roughly another 1,500 without rules, and every deploy adds new container-id series until old ones go stale (12 distinct ids already). Kept as they were, a few deploys plus the LLM host would approach 5,000. Dropping `id` and `image` means a redeployed container continues its own series instead of starting a new set.

After the rules: ___ series (app ___, platform ___); after 3 deploys in a row: ___.

The cost: anything not kept is gone. If an incident turns out to be disk I/O (`container_fs_*`), or Traefik's entrypoint-level errors, there's no history to look at. That's a rule change and a redeploy away, but only from then on.

## What would show this was wrong

An incident where the answer was in a dropped metric (then keep that family); `samples_failed > 0` after a deploy, meaning two containers with the same name collided once `id` was dropped; or the count after a burst of deploys climbing toward 5,000 anyway.
