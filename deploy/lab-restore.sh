#!/usr/bin/env bash
# lab-restore (S4, decisions/012): bring the lab back after the platform host boots, with no one
# touching anything. Installed on lab-platform as /usr/local/sbin/lab-restore and run at every boot by
# lab-restore.service (scripts/install-boot-restore.sh), as root with the host's instance profile
# (LabRole): no laptop, no fresh Learner Lab keys.
#
#   1. start RDS if it's stopped, resume Redshift if it's paused
#   2. wait until both are available
#   3. re-run the platform's one-shot db-init, then restart the platform containers that aren't healthy
#      (on their own they crash while RDS is down and come back only after Docker's growing backoff)
#   4. wait until Dagster and MLflow answer
#
# It only starts what already exists. It never starts the SageMaker notebook (it costs money and riders
# don't need it: decisions/012), never changes infrastructure, never redeploys, never moves an MLflow
# alias. The app host needs nothing: its containers don't depend on RDS, and serving re-checks MLflow
# every 30 s until it answers. Log: /var/log/lab-restore.log. Safe to run again at any time.
set -uo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

DB=lab CLUSTER=lab
CONTAINERS=(platform-code-1 platform-mlflow-1 platform-daemon-1 platform-webserver-1)
t0=$(date +%s)
log() { echo "$(date -u +%FT%TZ) +$(($(date +%s) - t0))s $*"; }

log "lab-restore: start"
for _ in $(seq 1 60); do  # instance-profile credentials can take a moment after boot
  aws sts get-caller-identity >/dev/null 2>&1 && break
  sleep 5
done

rds=$(aws rds describe-db-instances --db-instance-identifier "$DB" --query 'DBInstances[0].DBInstanceStatus' --output text 2>&1)
case $rds in
  stopped) aws rds start-db-instance --db-instance-identifier "$DB" >/dev/null && log "RDS was stopped: starting it" ;;
  available) log "RDS is available" ;;
  *) log "RDS is '$rds': waiting for it" ;;
esac
rs=$(aws redshift describe-clusters --cluster-identifier "$CLUSTER" --query 'Clusters[0].ClusterStatus' --output text 2>&1)
case $rs in
  paused) aws redshift resume-cluster --cluster-identifier "$CLUSTER" >/dev/null && log "Redshift was paused: resuming it" ;;
  available) log "Redshift is available" ;;
  *) log "Redshift is '$rs': waiting for it" ;;
esac

# Waiters poll every 30 s for up to 30 min.
aws rds wait db-instance-available --db-instance-identifier "$DB" && log "RDS available" || log "RDS still not available"
aws redshift wait cluster-available --cluster-identifier "$CLUSTER" && log "Redshift available" || log "Redshift still not available"

# The platform stack (deploy/compose.platform.yaml, project "platform"). Nothing to do if it was never deployed.
if docker inspect platform-db-init-1 >/dev/null 2>&1; then
  if docker start -a platform-db-init-1 >/dev/null 2>&1; then log "db-init: databases ok"; else log "db-init failed: $(docker logs --tail 3 platform-db-init-1 2>&1 | tr '\n' ' ')"; fi
fi
for c in "${CONTAINERS[@]}"; do
  docker inspect "$c" >/dev/null 2>&1 || { log "$c: not deployed, skipped"; continue; }
  state=$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "$c")
  if [[ $state == "running healthy" || $state == "running " ]]; then
    log "$c: $state, left alone"
  else
    docker restart "$c" >/dev/null && log "$c: was '$state', restarted"
  fi
done

ok=""
for _ in $(seq 1 60); do
  if curl -fsS -m 3 http://127.0.0.1:5000/health >/dev/null 2>&1 && curl -fsS -m 3 http://127.0.0.1:3000/server_info >/dev/null 2>&1; then
    ok=1; break
  fi
  sleep 5
done
if [[ -n $ok ]]; then log "lab-restore: done, MLflow and Dagster answer"; else log "lab-restore: MLflow or Dagster still not answering"; exit 1; fi
