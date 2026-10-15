#!/bin/bash
# usage: append_rows.sh <rows_csv_prefix> <tag>   (appends reps_rows_<m>.csv style files to the live plans)
P=$1; TAG=$2
S=/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11
for m in brain05 brain10; do
  f=$S/${P}_$m.csv
  n=$(tail -n +2 $f | wc -l)
  if [ "$n" -eq 0 ]; then echo "$m: nothing to append"; continue; fi
  scp -q -o BatchMode=yes $f sramezani@$m.mpi-sws.org:/local/sramezani/v11/bench/${P}_$m.$TAG.csv
  ssh -o BatchMode=yes sramezani@$m.mpi-sws.org "set -e; cd /local/sramezani/v11/bench; PL=plan_v11_$m.csv; T=plan_v11_$m.csv.new;
    head -1 \$PL | cmp - <(head -1 ${P}_$m.$TAG.csv);
    cp \$PL plan_v11_$m.before_$TAG.csv;
    cp \$PL \$T; [ -n \"\$(tail -c1 \$T)\" ] && echo >> \$T;
    tail -n +2 ${P}_$m.$TAG.csv >> \$T;
    python3 -c \"
import csv
a=list(csv.DictReader(open('\$PL'))); b=list(csv.DictReader(open('\$T')))
ids=[r['id'] for r in b]
assert len(ids)==len(set(ids)), 'duplicate ids'
print('$m rows', len(a), '->', len(b))
\"
    mv -f \$T \$PL; date '+%F %T'"
done
