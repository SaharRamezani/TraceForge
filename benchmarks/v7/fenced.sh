#!/bin/bash
B=/local/sramezani/target-v7/release/examples
OUT=~/traceforge-v7/benchmarks/fenced.out; rm -f $OUT
L="$B/lease_timed --u 2 --l-ratio 0 --sd-ratio 0.5 --ttl-ratio 5 --pause-ratio 2 --fencing on --keep-going"
one() { local cores=$1 label=$2; shift 2
  ( t0=$(date +%s.%N); out=$(taskset -c $cores /usr/bin/time -f "cpu=%U+%S" timeout 900 "$@" 2>&1); t1=$(date +%s.%N)
    echo "$label wall=$(echo "$t1 - $t0" | bc) $(echo "$out" | grep -o 'execs=[0-9]*\|blocked=[0-9]*\|violations=[0-9]*\|cpu=[0-9.+]*' | tr '\n' ' ')" >> $OUT ) & }
c=216
for cfg in "2 3" "2 5" "3 1" "3 2" "4 1"; do set -- $cfg
  for mode in timed baseline; do one $c "fenced C$1 R$2 $mode" $L --clients $1 --rounds $2 --mode $mode; c=$((c+1)); done; done
one 230-245 "fenced C4 R2 timed part16" $L --clients 4 --rounds 2 --mode timed --parallel partitioned
one 246-255 "fenced C4 R2 base part10" $L --clients 4 --rounds 2 --mode baseline --parallel partitioned
wait; echo FENCED-DONE >> $OUT
