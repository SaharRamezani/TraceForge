#!/bin/bash
# Fetch the v8 rerun results from brain05 (VPN needed) and print the comparison.
set -e
cd "$(dirname "$0")"
R=sramezani@brain05.mpi-sws.org:traceforge-v8r/run
scp -q $R/results_v8r.csv $R/status_v8r.json $R/env_v8r.txt .
python3 compare_v8r.py "$@"
