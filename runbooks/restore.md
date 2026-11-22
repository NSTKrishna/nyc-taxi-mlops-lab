# Runbook: the lab after a Learner Lab restart (session end, Start Lab)

Owner: NSTKrishna · Last run: 2026-11-22 · Time to restore last run: about 10 min for the restore itself (RDS 2 min, Redshift 3 min, and a few minutes for the platform stack); about 75 min that day, because the platform had never been deployed to this account (row 4, one-time setup)

## When to use it

"The app quotes 12.25 minutes for every trip", "Dagster / MLflow on localhost:3000 / 127.0.0.1:5000 won't connect", or "AWS says my token expired": the first thing after Start Lab, or whenever `scripts/health.sh` shows a FAIL.

## Before you start

- Learner Lab started, and the new block from **AWS Details** pasted into `~/.aws/credentials` (`[default]`). The keys expire when the session ends, about 4 h after Start Lab.
- `gh` logged in as the repo owner, with `gh repo set-default NSTKrishna/nyc-taxi-mlops-lab`. With two remotes, `gh` otherwise picks `upstream` and gets 403s.
- Terraform 1.11 or newer on PATH (`brew install hashicorp/tap/terraform`; Homebrew's default `terraform` is 1.5.7 and fails), and `infra/live` initialised.
- Then: `scripts/health.sh`. Write every FAIL and WARN down as a row below before fixing anything.

## Steps

| # | Check (command) | Healthy looks like | If not: fix (command) | Took | Automated? |
| --- | --- | --- | --- | --- | --- |
| 1 | `aws sts get-caller-identity` | your account id | Paste the new block from Learner Lab's AWS Details into `~/.aws/credentials`. `voc-cancel-cred` in an error means the session is over: Start Lab first. | 2 min | no: the keys only exist in the browser |
| 2 | `aws rds describe-db-instances --db-instance-identifier lab --query 'DBInstances[0].DBInstanceStatus'` | `available` | `aws rds start-db-instance --db-instance-identifier lab`, then `aws rds wait db-instance-available --db-instance-identifier lab`. Dagster and MLflow keep their state in RDS: both are down until it's up. | 2 min (05:58:55 → 06:00:46 UTC) | yes: `lab-restore` at boot |
| 3 | `aws redshift describe-clusters --cluster-identifier lab --query 'Clusters[0].ClusterStatus'` | `available` | `aws redshift resume-cluster --cluster-identifier lab`. Dagster runs fail until it's back. | 3 min (06:04:03 → 06:07:06 UTC) | yes: `lab-restore` at boot |
| 4 | On lab-platform: `docker ps -a` | `platform-webserver-1`, `-daemon-1`, `-code-1`, `-mlflow-1` up and healthy | Empty: the stack was never deployed to this account (the repo's runners were on someone else's hosts). One-time setup: own fork, `scripts/register-runner.sh` (app) and `scripts/register-runner.sh platform`, the `LAB_*` variables, `gh workflow run platform`. | about 60 min, once | no: setup, not restore |
| 5 | On lab-platform: `curl -sS http://127.0.0.1:5000/health` | `OK` | Containers present but not answering: `docker start -a platform-db-init-1`, then `docker restart platform-code-1 platform-mlflow-1 platform-daemon-1 platform-webserver-1` | 1 min | yes: `lab-restore` at boot |
| 6 | Tunnel: `terraform -chdir=infra/live output -raw platform_instance_id` | the id you forward to | Dagster and MLflow are on **lab-platform**, not lab-app. Run the tunnel from the laptop, not from a shell on a host. Open MLflow as `http://127.0.0.1:5000`: macOS's AirPlay Receiver answers `localhost:5000` with 403. | 5 min | no: a laptop habit |
| 7 | `curl -X POST http://<app_public_ip>/predict -H 'Content-Type: application/json' -d '{"pickup_zone_id": 132, "dropoff_zone_id": 230, "pickup_datetime": "2024-03-08T17:05:00"}'` | `"alias": "champion"` | `static-median`: is serving up (`deploy/web.sh status` on lab-app)? It re-checks MLflow every 30 s once MLflow answers. | 0 min (came back by itself) | by itself: serving retries |
| 8 | `scripts/health.sh` laptop-ip line | OK | WARN while `ingress_cidrs = ["0.0.0.0/0"]` is a false alarm: the check only matches `"<ip>/32"` (open: decisions/001 vs the check) | — | no |
| 9 | `aws sagemaker describe-notebook-instance --notebook-instance-name lab --query NotebookInstanceStatus` | `Stopped`, unless someone is exploring | Only when needed: `aws sagemaker start-notebook-instance --notebook-instance-name lab` | — | never: decisions/012 |
| 10 | `gh workflow run status.yaml`, then `gh run list --workflow status` | green, RDS and Redshift `available` in the artifact | the rows above | 1 min | runs every 20 min by itself |

## Done when

1. `/predict` on the public IP answers `"alias": "champion"` (row 7): what a rider sees.
2. `scripts/health.sh` exits 0.
3. The status workflow is green after the restore and reports RDS and Redshift `available`.

## What we automated, and what we left as a step

[decisions/012](../decisions/012-what-to-automate.md). `lab-restore.service` on lab-platform runs `deploy/lab-restore.sh` at every boot: it does rows 2, 3 and 5 with the host's LabRole, so Start Lab alone brings the lab back. Installed by `scripts/install-boot-restore.sh`; its log is `/var/log/lab-restore.log`.

Still manual: new keys (row 1, they only exist in the browser), the laptop's tunnel (row 6), and the notebook (row 9), which stays stopped unless someone wants it, because it costs money and riders don't need it.
