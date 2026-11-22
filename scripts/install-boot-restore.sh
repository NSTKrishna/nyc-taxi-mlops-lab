#!/usr/bin/env bash
# S4: install the boot-time restore (deploy/lab-restore.sh + deploy/lab-restore.service) on the platform
# host, and enable it so it runs at every boot (decisions/012).
#
#   scripts/install-boot-restore.sh          install and enable (takes effect at the next boot)
#   scripts/install-boot-restore.sh --run    ... and run it once now (safe: it only starts what's down)
#
# No SSH: the files go through SSM Run Command, like register-runner.sh. Needs AWS credentials in the
# environment and infra/live applied. Run it again after changing either file.
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
HERE="$(cd "$(dirname "$0")/.." && pwd)"
run_now=${1:-}

instance=$(terraform -chdir="$HERE/infra/live" output -raw platform_instance_id) ||
  { echo "no platform_instance_id output: has terraform apply run?" >&2; exit 1; }

script=$(base64 <"$HERE/deploy/lab-restore.sh" | tr -d '\n')
unit=$(base64 <"$HERE/deploy/lab-restore.service" | tr -d '\n')
remote="set -euo pipefail
echo '$script' | base64 -d > /usr/local/sbin/lab-restore && chmod 755 /usr/local/sbin/lab-restore
echo '$unit' | base64 -d > /etc/systemd/system/lab-restore.service
systemctl daemon-reload && systemctl enable lab-restore.service
echo \"installed: \$(systemctl is-enabled lab-restore.service)\""
[[ $run_now == --run ]] && remote+="
systemctl start lab-restore.service
while systemctl is-active -q lab-restore.service; do sleep 5; done
tail -n 20 /var/log/lab-restore.log"

params=$(jq -n --arg c "$remote" '{commands: [$c], executionTimeout: ["3600"]}')
pfile=$(mktemp) && trap 'rm -f "$pfile"' EXIT
printf '%s' "$params" >"$pfile"
cmd=$(aws ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript \
  --parameters "file://$pfile" --timeout-seconds 3600 --comment "install lab-restore" \
  --query Command.CommandId --output text)
echo "sent SSM command $cmd to $instance, waiting"
until status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" --query Status --output text 2>/dev/null) &&
  [[ ! $status =~ ^(Pending|InProgress|Delayed)$ ]]; do sleep 5; done
aws ssm get-command-invocation --command-id "$cmd" --instance-id "$instance" \
  --query '[StandardOutputContent,StandardErrorContent]' --output text
[[ $status == Success ]]
