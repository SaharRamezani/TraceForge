#!/bin/bash
cd "$HOME/traceforge-v7/benchmarks"
rm -rf logs_v7 test.csv results_v7_ceiling.csv sweep_v7.out status_v7.json
B=/local/sramezani/target-v7/release/examples
md5sum $B/lease_timed $B/swim_timed > env_v7.txt
echo "source: local working tree of TraceForge at HEAD 697fc36 (branch inbox_new_fix) plus uncommitted exec_pool.rs shared-pool hang fix; built 2026-09-20 18:07 with cargo 1.97.1" >> env_v7.txt
echo "launch $(date '+%F %T')" >> env_v7.txt
tmux new-session -d -s v7sweep "bash $HOME/traceforge-v7/benchmarks/run_v7.sh"
sleep 30
tmux ls
head -4 sweep_v7.out
cat status_v7.json 2>/dev/null | head -40
uptime
