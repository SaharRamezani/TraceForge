#!/bin/bash
# v9 campaign driver (runs inside tmux session v9sweep on brain05).
# Starts the scheduler; if the scheduler process dies unexpectedly it is started again with
# --append: finished jobs are skipped, jobs that are still running are adopted, not restarted.
DEADLINE="2026-10-13 08:00"
cd /local/sramezani/v9/bench || exit 2
n=0
while [ $n -lt 6 ]; do
  python3 sweep_v9.py --plan plan_v9.csv --out results_v9.csv --deadline "$DEADLINE" --append >> sweep_v9.out 2>&1
  ec=$?
  if tail -5 sweep_v9.out | grep -q -E "SWEEP-DONE|SWEEP-STOPPED"; then
    break
  fi
  echo "[run_v9] scheduler exited with code $ec at $(date '+%F %T'); restarting" >> sweep_v9.out
  n=$((n + 1))
  sleep 15
done
echo "[run_v9] driver finished at $(date '+%F %T')" >> sweep_v9.out
