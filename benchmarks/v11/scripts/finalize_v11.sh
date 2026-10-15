#!/bin/bash
# Run after the 21:50 stop: fetch everything, build the table, archive into the repo.
S=/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11
REPO=/home/sahar/Downloads/TraceForge/benchmarks/v11
set -e
cd $S
./fetch_v11.sh
python3 -I build_table_v11.py
python3 -I top_rungs.py 3 > top_rungs_v11.txt
mkdir -p $REPO/data $REPO/scripts
cp data/results_v11_brain05.csv data/results_v11_brain10.csv data/plan_v11_brain05.csv data/plan_v11_brain10.csv \
   data/tfstats_brain05.tsv data/tfstats_brain10.tsv data/flags_brain05.txt data/flags_brain10.txt \
   data/status_brain05.json data/status_brain10.json $REPO/data/
cp table_v11.csv table_v11.json controls_v11.csv audit_v11.txt top_rungs_v11.txt $REPO/
cp build_table_v11.py make_plan_v11.py make_wave2.py make_wave3.py make_reps_wave.py make_controls2.py fetch_v11.sh \
   append_rows.sh top_rungs.py finalize_v11.sh watch_v11.sh $REPO/scripts/
for m in brain05 brain10; do
  H=sramezani@$m.mpi-sws.org
  ssh -o BatchMode=yes $H "cd /local/sramezani/v11 && taskset -c 0,1,64,65 nice -n 19 tar czf /tmp/v11_logs_$m.tgz logs bench/v11_$m.out bench/bin_set.md5 bench/src_manifest.md5 bench/launch_time.txt"
  scp -q -o BatchMode=yes $H:/tmp/v11_logs_$m.tgz $REPO/logs_$m.tgz
done
ls -la $REPO
