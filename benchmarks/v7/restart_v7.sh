#!/bin/bash
bash ~/traceforge-v7/benchmarks/stop_v7.sh
cd ~/traceforge-v7/benchmarks
rm -rf logs_v7 results_v7_ceiling.csv sweep_v7.out status_v7.json
sed -i 's/--cores 4-243 --append/--append/' run_v7.sh
echo "relaunch (scheduler v2: small pool 4-27, big pool 28-243) $(date '+%F %T')" >> env_v7.txt
tmux new-session -d -s v7sweep "bash $HOME/traceforge-v7/benchmarks/run_v7.sh"
sleep 40
tmux ls; head -4 sweep_v7.out; tail -5 sweep_v7.out
python3 -c "
import json;s=json.load(open('status_v7.json'));print(s['time'],'free big',s['free_cores'],'free small',s['free_small'],'queued',s['queued'],'running',len(s['running']))"
uptime
