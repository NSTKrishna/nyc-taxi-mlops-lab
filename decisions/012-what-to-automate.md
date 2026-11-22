# 012: What to automate after a restart, and what to leave as a step

Stage: S4 · Date: 2026-11-22 · Author: NSTKrishna · Drafted by agent: yes

## Choice

A boot-time restore on the platform host. `lab-restore.service` (systemd, enabled) runs `deploy/lab-restore.sh` at every boot of lab-platform, as root with the host's LabRole. It starts RDS if it's stopped, resumes Redshift if it's paused, waits until both are available, re-runs the platform's one-shot `db-init`, and restarts the platform containers that aren't healthy. Then it waits until MLflow and Dagster answer. Start Lab boots the hosts, so Start Lab alone brings the lab back. It must never start the SageMaker notebook. It is installed with `scripts/install-boot-restore.sh` and logs to `/var/log/lab-restore.log`.

## Rejected alternative

A restore script on the laptop (`scripts/restore.sh`), run by hand after Start Lab with the new keys. A person decides when it runs, but nothing comes back until someone runs it, and the lab can't pass "reboot the platform host and touch nothing".

## Why

<!-- Student: confirm or rewrite this in your own words. -->
Every step of the restore was routine and the same each time: start RDS (2 min), resume Redshift (3 min), bring the containers back once RDS is up. That is toil, a machine's job. The host already has what it needs: LabRole can start RDS and resume Redshift (I did both from a shell on lab-platform), and it doesn't depend on the laptop's keys, which expire with the session and only exist in the browser. On its own the stack comes back only partly: the containers restart, crash while RDS is down, and wait out Docker's growing backoff, and `db-init` (`restart: "no"`) never runs again.

The script only starts what already exists, and is a no-op when everything is up (first run: 6 s, all left alone). The notebook stays manual: it costs money while running, and riders don't need it.

The cost: it runs on *every* boot. If I pause Redshift or stop RDS on purpose and then reboot the platform host, they come back on.

## What would show this was wrong

A reboot where I wanted RDS or Redshift to stay down and the restore started them (then add an opt-out, for example a flag file it checks); a restore that never finishes (the log shows a waiter timing out); or a Start Lab where Dagster or MLflow still need a hand after `lab-restore` reported done.
