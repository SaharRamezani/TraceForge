#!/bin/bash
# v8 rerun driver: started inside tmux session v8r by launch_v8r.sh
cd "$HOME/traceforge-v8r/run"
python3 -u sweep_v8r.py --plan plan_v8r.csv --out results_v8r.csv --deadline "2026-10-09 12:00" --cap 21600 --pools "all=4-243" --append >> sweep_v8r.out 2>&1 &
echo $! > sweep_v8r.pid
wait
echo SWEEP-FINISHED >> sweep_v8r.out
sleep 100000000
