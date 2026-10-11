#!/bin/bash
# Fetch the v9 campaign results from brain05 (needs the MPI VPN). Safe to run while the
# campaign is still going: partial results are complete rows.
HERE="$(cd "$(dirname "$0")" && pwd)"
R=sramezani@brain05.mpi-sws.org:/local/sramezani/v9/bench
scp -q "$R/results_v9.csv" "$R/plan_v9.csv" "$R/sweep_v9.out" "$R/status_v9.json" "$R/env_v9.txt" "$HERE/" || exit 1
python3 "$HERE/check_v9.py"
