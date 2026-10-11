#!/bin/bash
# Started by cron at boot (crontab line: @reboot /bin/bash /local/sramezani/v9/bench/reboot_v9.sh).
# If brain05 reboots while the v9 campaign is unfinished, the campaign is resumed: finished jobs
# are kept, the jobs that were running are run again from the start, the deadline is unchanged.
sleep 120
B=/local/sramezani/v9/bench
[ -f "$B/plan_v9.csv" ] || exit 0
if tail -3 "$B/sweep_v9.out" 2>/dev/null | grep -q -E "SWEEP-DONE|SWEEP-STOPPED|driver finished"; then
  exit 0
fi
tmux has-session -t v9sweep 2>/dev/null && exit 0
echo "[reboot_v9] machine rebooted; resuming the campaign at $(date '+%F %T')" >> "$B/sweep_v9.out"
tmux new-session -d -s v9sweep "bash $B/run_v9.sh"
