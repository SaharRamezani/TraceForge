#!/bin/bash
D=/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v12/data
cd "$D" || exit 1
for m in brain05 brain10; do
  H=sramezani@$m.mpi-sws.org; R=/local/sramezani/v12
  scp -q -o BatchMode=yes $H:$R/bench/results_v12_$m.csv results_v12_$m.csv
  scp -q -o BatchMode=yes $H:$R/bench/plan_v12_$m.csv plan_v12_$m.csv
  scp -q -o BatchMode=yes $H:$R/bench/status_v9.json status_$m.json 2>/dev/null
  ssh -o BatchMode=yes $H "cd $R/logs && taskset -c 0,1,64,65 nice -n 19 grep -a -H '^TFSTATS' *.log" | sed -e 's/\.log:TFSTATS/\tTFSTATS/' > tfstats_$m.tsv
  ssh -o BatchMode=yes $H "cd $R/logs && taskset -c 0,1,64,65 nice -n 19 grep -a -c -E 'Large thread size|panicked at' *.log | grep -v ':0\$'" > flags_$m.txt
  echo "$m: $(($(wc -l < results_v12_$m.csv)-1)) rows, $(wc -l < tfstats_$m.tsv) tfstats, flags: $(wc -l < flags_$m.txt)"
done
