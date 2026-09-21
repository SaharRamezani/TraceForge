#!/bin/bash
cd "$HOME/traceforge-v7/benchmarks"
GROWTH_TIMEOUT=900 GROWTH_JOBS=64 python3 growth.py growth.csv 2>&1 | tee growth.out
