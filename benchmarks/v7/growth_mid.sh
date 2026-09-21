#!/bin/bash
cd "$HOME/traceforge-v7/benchmarks"
G_U=2 G_L=1 G_SD=1 GROWTH_TIMEOUT=600 GROWTH_JOBS=64 python3 growth_mid.py growth_mid.csv 2>&1 | tee growth_mid.out
