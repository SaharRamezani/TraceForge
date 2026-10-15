#!/bin/bash
# Run after the 07:50 stop: fetch everything, build the table and the paper rows, archive into the repo.
S=/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v12
REPO=/home/sahar/Downloads/TraceForge/benchmarks/v12
set -e
cd $S
./fetch_v12.sh
python3 build_table_v12.py | tee build_v12.txt
python3 controls_v12.py | tee controls_v12.txt
python3 make_tex_v12.py "$S/"
python3 curate_v12.py | tee curate_v12.txt
python3 summary_v12b.py "$S/"
python3 frontier_v12.py > frontier_v12.txt
python3 headline_v12.py > headline_v12.txt
python3 report_v12.py "$S/"
python3 update_caveats_v12.py $REPO/CAVEATS.md
mkdir -p $REPO/data $REPO/scripts
cp data/results_v12_brain05.csv data/results_v12_brain10.csv data/plan_v12_brain05.csv data/plan_v12_brain10.csv \
   data/tfstats_brain05.tsv data/tfstats_brain10.tsv data/flags_brain05.txt data/flags_brain10.txt \
   data/status_brain05.json data/status_brain10.json $REPO/data/
cp table_v12.csv controls_v12.json controls_v12.txt frontier_v12.csv frontier_v12.txt RESULTS_v12.md build_v12.txt \
   tab_evaluation_v12_main.tex tab_evaluation_v12_full.tex tab_evaluation_v12_rows_main.tex tab_evaluation_v12_rows_full.tex \
   tab_evaluation_v12_curated.tex tab_evaluation_v12_curated_rows.tex curate_v12.txt curated_text_v12.txt headline_v12.txt REPORT_v12.md $REPO/
cp cells_v12*.json $REPO/scripts/
cp regimes_v12.py make_plan_v12.py make_ladders_v12.py make_nisshi_ladders.py climb_v12.py build_table_v12.py fetch_v12.sh \
   watch_v12.sh events_v12.sh rebalance_v12.py scaleup_candidates.py wave2_v12.py killplan_v12.py attempts_v12.py gaps_v12.py \
   extra_reps_v12.py controls_v12.py make_tex_v12.py summary_v12b.py frontier_v12.py finalize_v12.sh curate_v12.py launch_rung.py report_v12.py headline_v12.py update_caveats_v12.py \
   $REPO/scripts/
cp par_patch/*.rs $REPO/scripts/ 2>/dev/null || true
for m in brain05 brain10; do
  H=sramezani@$m.mpi-sws.org
  ssh -o BatchMode=yes $H "cd /local/sramezani/v12 && taskset -c 0,1,64,65 nice -n 19 tar czf /tmp/v12_logs_$m.tgz logs bench/v12_$m.out bench/control_v9.txt bench/climb_$m.log bench/launch_time.txt bench/bin_set.md5"
  scp -q -o BatchMode=yes $H:/tmp/v12_logs_$m.tgz $REPO/logs_$m.tgz
done
ls -la $REPO
