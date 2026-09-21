#!/bin/bash
# pull the current ceiling-sweep state from brain05 into benchmarks/v7_snapshot/ceiling/
set -e
D=/home/sahar/Desktop/MPI/TraceForge/benchmarks/v7_snapshot/ceiling
mkdir -p $D
scp -q sramezani@brain05.mpi-sws.org:traceforge-v7/benchmarks/results_v7_ceiling.csv $D/ || true
scp -q sramezani@brain05.mpi-sws.org:traceforge-v7/benchmarks/plan_v7.csv $D/
scp -q sramezani@brain05.mpi-sws.org:traceforge-v7/benchmarks/env_v7.txt $D/ || true
ssh -o BatchMode=yes sramezani@brain05.mpi-sws.org "ps -u sramezani -o etimes,args | grep -E 'target-v[7]/release/examples/(lease|swim)_timed' | grep -v -E 'grep|/usr/bin/time|taskset'" > $D/ps_running.txt || true
date '+%F %T' > $D/fetched_at.txt
wc -l $D/results_v7_ceiling.csv $D/ps_running.txt
