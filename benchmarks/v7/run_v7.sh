#!/bin/bash
cd "$HOME/traceforge-v7/benchmarks"
python3 sweep_v7.py --plan plan_v7.csv --out results_v7_ceiling.csv --deadline "2026-09-22 17:00" --cores 4-243 --append 2>&1 | tee -a sweep_v7.out
echo SWEEP-FINISHED >> sweep_v7.out
sleep 100000000
