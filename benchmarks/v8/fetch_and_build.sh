#!/bin/bash
# Fetch the v8 campaign results from brain05 (VPN needed) and rebuild results_v8.xlsx.
# Run any time; after the deadline (Fri 2 Oct 08:00) the jobs still running are
# recorded as DNF with their progress, and the workbook is final.
set -e
cd "$(dirname "$0")"
R=sramezani@brain05.mpi-sws.org:traceforge-v8/benchmarks/v8
scp -q $R/results_v8.csv $R/results_v8_w0A.csv $R/plan_v8.csv $R/env_v8.txt $R/status_v8.json .
python3 build_v8_xlsx.py
