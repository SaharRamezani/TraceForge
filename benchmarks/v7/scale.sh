#!/bin/bash
# worker-scaling calibration on an otherwise idle brain05 (run inside tmux)
cd "$HOME/traceforge-v7/benchmarks"
for w in 256 128 32 8; do
  python3 scale.py lease:4:2:timed:$w:900 swim:12:1:timed:$w:900 lease:4:2:baseline:$w:900 2>&1 | tee -a scale.out
done
echo SCALE-DONE >> scale.out
