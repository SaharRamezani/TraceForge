#!/bin/bash
# Fetch v11 results from both machines into data/ (service cores only on the remote side, nice 19).
D=/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/data
cd "$D" || exit 1
for m in brain05 brain10; do
  H=sramezani@$m.mpi-sws.org
  R=/local/sramezani/v11
  scp -q -o BatchMode=yes $H:$R/bench/results_v11_$m.csv results_v11_$m.csv
  scp -q -o BatchMode=yes $H:$R/bench/plan_v11_$m.csv plan_v11_$m.csv
  scp -q -o BatchMode=yes $H:$R/bench/status_v9.json status_$m.json
  ssh -o BatchMode=yes $H "cd $R/logs && taskset -c 0,1,64,65 nice -n 19 grep -a -H '^TFSTATS' *.log" \
     | sed -e 's/\.log:TFSTATS/\tTFSTATS/' > tfstats_$m.tsv
  ssh -o BatchMode=yes $H "cd $R/logs && taskset -c 0,1,64,65 nice -n 19 grep -a -c -E 'Large thread size|panicked at|thread_threshold' *.log | grep -v ':0\$'" > flags_$m.txt
  wc -l results_v11_$m.csv tfstats_$m.tsv flags_$m.txt | head -3
done
