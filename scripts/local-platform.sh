#!/usr/bin/env bash
# Run the platform stack (Dagster and MLflow) on your laptop instead of lab-platform.
#
#   scripts/local-platform.sh mlflow     MLflow at http://127.0.0.1:5000   (start this first)
#   scripts/local-platform.sh dagster    Dagster UI + daemon at http://localhost:3000
#
# One terminal each; Ctrl+C stops it. State stays on this machine, in .local/ (git-ignored):
# Dagster's runs in SQLite, MLflow's runs and registry in SQLite, model files in .local/mlflow/artifacts.
#
# Still on AWS: Redshift (Data API as labadmin) and S3 (the data bucket, your artifact bucket for
# UNLOAD), with the Learner Lab credentials in ~/.aws. Redshift must be available, not paused.
#
# What a local stack can't do: the status workflow (the grader's view) reads Dagster on lab-platform,
# and serving (S3) and the notebook read MLflow on lab-platform, so neither sees this one.
#
# Settings (environment, all optional except LAB_DATA_BUCKET for S1's ingest):
#   LAB_DATA_BUCKET      the instructor's data bucket
#   LAB_ARTIFACT_BUCKET  default: terraform output artifact_bucket, else lab-artifacts-<account id>
#   MLFLOW_ARTIFACTS     default: .local/mlflow/artifacts (e.g. s3://<artifact bucket>/mlflow-local)
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
state="$root/.local"
what=${1:?usage: scripts/local-platform.sh mlflow|dagster}

mkdir -p "$state/dagster" "$state/mlflow/artifacts"
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
export LAB_CONTRACTS_DIR="$root/contracts" MLFLOW_DISABLE_AGENT_HINT=1 OMP_WAIT_POLICY=PASSIVE
export REDSHIFT_CLUSTER_ID=${REDSHIFT_CLUSTER_ID:-lab} REDSHIFT_DATABASE=${REDSHIFT_DATABASE:-lab} REDSHIFT_DB_USER=labadmin
export MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://127.0.0.1:5000}
export LLM_HOST=${LLM_HOST:-}
export GIT_SHA=${GIT_SHA:-$(git -C "$root" rev-parse HEAD)}

aws sts get-caller-identity --query Account --output text >/dev/null 2>&1 ||
  { echo "AWS credentials missing or expired: paste the new ones from Learner Lab into ~/.aws/credentials" >&2; exit 1; }

case $what in
  mlflow)
    cd "$root/pipelines"
    # 127.0.0.1, not localhost: macOS's AirPlay Receiver also listens on port 5000 (IPv4 and IPv6), and
    # a browser tries localhost as ::1 first, which reaches AirPlay (403) instead of MLflow.
    echo "MLflow: http://127.0.0.1:5000  (runs: $state/mlflow/mlflow.db, files: ${MLFLOW_ARTIFACTS:-$state/mlflow/artifacts})"
    exec uv run --quiet mlflow server --host 127.0.0.1 --port 5000 \
      --backend-store-uri "sqlite:///$state/mlflow/mlflow.db" \
      --serve-artifacts --artifacts-destination "${MLFLOW_ARTIFACTS:-$state/mlflow/artifacts}"
    ;;
  dagster)
    if [[ -z "${LAB_ARTIFACT_BUCKET:-}" ]]; then
      # infra/live/main.tf: module artifact_bucket, name "lab-artifacts" plus the account id.
      LAB_ARTIFACT_BUCKET=$(terraform -chdir="$root/infra/live" output -raw artifact_bucket 2>/dev/null) ||
        LAB_ARTIFACT_BUCKET="lab-artifacts-$(aws sts get-caller-identity --query Account --output text)"
    fi
    export LAB_ARTIFACT_BUCKET
    [[ -n "${LAB_DATA_BUCKET:-}" ]] || echo "warning: LAB_DATA_BUCKET isn't set: the UI works, ingest runs fail" >&2
    export LAB_DATA_BUCKET=${LAB_DATA_BUCKET:-}
    status=$(aws redshift describe-clusters --cluster-identifier "$REDSHIFT_CLUSTER_ID" --query 'Clusters[0].ClusterStatus' --output text 2>/dev/null || echo unknown)
    [[ "$status" == available ]] || echo "warning: Redshift is $status: runs fail until it's available (aws redshift resume-cluster --cluster-identifier lab)" >&2
    curl -fsS -m 3 "$MLFLOW_TRACKING_URI/health" >/dev/null 2>&1 ||
      echo "warning: MLflow isn't answering at $MLFLOW_TRACKING_URI: start it first (scripts/local-platform.sh mlflow)" >&2

    # A local instance: SQLite storage in DAGSTER_HOME, the same concurrency limits as lab-platform.
    export DAGSTER_HOME="$state/dagster"
    cat > "$DAGSTER_HOME/dagster.yaml" <<'YAML'
# Written by scripts/local-platform.sh: storage is SQLite in this folder (the default).
concurrency:
  runs:
    max_concurrent_runs: 2
  pools:
    granularity: op
    default_limit: 1
telemetry:
  enabled: false
YAML
    cd "$root/pipelines"
    echo "Dagster: http://localhost:3000  (runs: $DAGSTER_HOME)"
    exec uv run --quiet dagster dev -m labpipes.definitions -h 127.0.0.1 -p 3000
    ;;
  *)
    echo "usage: scripts/local-platform.sh mlflow|dagster" >&2
    exit 2
    ;;
esac
