#!/usr/bin/env bash
# S0: register a lab host as a self-hosted GitHub Actions runner with label lab-<role>
# (contracts/runtime.yaml), so its workflows deploy with the host's instance profile (decisions/002).
#
#   scripts/register-runner.sh             the app host, label lab-app (S0)
#   scripts/register-runner.sh platform    the platform host, label lab-platform (S1)
#   scripts/register-runner.sh llm         the LLM host, label lab-llm (S6)
#
# Before running it: set Settings > Actions > General > "Require approval for all external
# contributors" (the repo is public), and have AWS credentials in the environment and `gh` logged in.
#
# How: no SSH. The script asks GitHub for a registration token (valid 1 hour, single use), then runs the
# install on the host through SSM Run Command, as the `runner` user the host's first boot created.
# The token is never printed and never on a command line here; it does sit in the SSM command's
# parameters (visible in the SSM console's command history to anyone with SSM access) until it expires.
# Idempotent: a host already registered to this repo is left alone; one registered to another repo is
# reported and left alone too (remove it on the host with config.sh remove, then run this again).
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

RUNNER_VERSION="${RUNNER_VERSION:-2.338.2}" # pinned: bump deliberately
role=${1:-app}
[[ "$role" =~ ^(app|platform|llm)$ ]] || { echo "usage: scripts/register-runner.sh [app|platform|llm]" >&2; exit 2; }
LABEL=lab-$role
HERE="$(cd "$(dirname "$0")/.." && pwd)"

repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
instance=$(terraform -chdir="$HERE/infra/live" output -raw "${role}_instance_id") ||
  { echo "no ${role}_instance_id output: has terraform apply run?" >&2; exit 1; }

# Hash of the runner tarball from the release itself, so the host verifies what it downloads.
asset="actions-runner-linux-arm64-${RUNNER_VERSION}.tar.gz"
sha=$(gh api "repos/actions/runner/releases/tags/v${RUNNER_VERSION}" \
  --jq ".assets[] | select(.name==\"$asset\") | .digest" | sed 's/^sha256://')
[[ "$sha" =~ ^[0-9a-f]{64}$ ]] ||
  { echo "could not read the sha256 of $asset for v${RUNNER_VERSION}" >&2; exit 1; }

state=$(aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=$instance" --query 'InstanceInformationList[0].PingStatus' --output text)
[[ "$state" == "Online" ]] ||
  { echo "$instance is not reachable through SSM yet (status: $state). Wait for first boot." >&2; exit 1; }

token=$(gh api -X POST "repos/$repo/actions/runners/registration-token" --jq .token)

remote=$(cat <<'REMOTE'
set -euo pipefail
until [ -f /var/lib/lab-setup-done ]; do sleep 5; done
cd /opt/actions-runner
if [ -f .runner ]; then
  url=$(tr -d '\000' < .runner | sed -n 's/.*"gitHubUrl": *"\([^"]*\)".*/\1/p')
  if [ "$url" = "https://github.com/${REPO}" ]; then echo "already registered to ${REPO}"; exit 0; fi
  echo "this host's runner is registered to ${url:-another repo}, not ${REPO}: left alone" >&2; exit 1
fi
find . -mindepth 1 -delete # a fresh install, never unpacked over an auto-updated one
curl -fsSL -o runner.tgz "https://github.com/actions/runner/releases/download/v${VERSION}/actions-runner-linux-arm64-${VERSION}.tar.gz"
echo "${SHA}  runner.tgz" | sha256sum -c -
tar xzf runner.tgz && rm runner.tgz
chown -R runner:runner /opt/actions-runner
sudo -u runner ./config.sh --unattended --replace --url "https://github.com/${REPO}" \
  --token "${TOKEN}" --name "${LABEL}" --labels "${LABEL}" >/dev/null
./svc.sh install runner
./svc.sh start
echo "registered ${LABEL}"
REMOTE
)

params=$(jq -n --arg r "$remote" --arg v "$RUNNER_VERSION" --arg s "$sha" --arg repo "$repo" \
  --arg t "$token" --arg l "$LABEL" \
  '{commands: [("export VERSION=" + ($v|@sh) + " SHA=" + ($s|@sh) + " REPO=" + ($repo|@sh) + " TOKEN=" + ($t|@sh) + " LABEL=" + ($l|@sh) + "\n" + $r)]}')
unset token

pfile=$(mktemp) && trap 'rm -f "$pfile"' EXIT # mktemp files are mode 600
printf '%s' "$params" >"$pfile"
cmd=$(aws ssm send-command --instance-ids "$instance" \
  --document-name AWS-RunShellScript --parameters "file://$pfile" \
  --query Command.CommandId --output text)
echo "sent SSM command $cmd to $instance, waiting"
aws ssm wait command-executed --command-id "$cmd" --instance-id "$instance" ||
  { echo "failed; output:" >&2
    aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
      --query '[StandardOutputContent,StandardErrorContent]' --output text >&2; exit 1; }
aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
  --query StandardOutputContent --output text
echo "check: gh api repos/$repo/actions/runners --jq '.runners[] | [.name,.status] | @tsv'"
