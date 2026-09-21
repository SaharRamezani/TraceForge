#!/bin/bash
# does SWIM parallelize at larger R?  each test pinned to its own disjoint cores, all concurrent
B=/local/sramezani/target-v7/release/examples
OUT=~/traceforge-v7/benchmarks/swimpar.out; rm -f $OUT
one() { # cores label workers cmd...
  local cores=$1 label=$2 workers=$3; shift 3
  ( t0=$(date +%s.%N)
    out=$(MUST_PARALLEL_WORKERS=$workers taskset -c $cores /usr/bin/time -f "cpu=%U+%S" timeout 1200 "$@" 2>&1)
    t1=$(date +%s.%N)
    echo "$label cores=$cores wall=$(echo "$t1 - $t0" | bc) $(echo "$out" | grep -o 'execs=[0-9]*\|blocked=[0-9]*\|violations=[0-9]*\|cpu=[0-9.+]*' | tr '\n' ' ')" >> $OUT ) &
}
SW="$B/swim_timed --u 2 --l-ratio 0 --sd-ratio 0.5 --w-probe-ratio 1 --w-suspect-ratio 3.5 --keep-going"
one 100-100 "timed N5R8 none"        1  $SW --nodes 5 --rounds 8 --mode timed
one 101-116 "timed N5R8 shared16"    16 $SW --nodes 5 --rounds 8 --mode timed --parallel shared
one 120-135 "timed N5R8 partition16" 16 $SW --nodes 5 --rounds 8 --mode timed --parallel partitioned
one 140-140 "timed N8R4 none"        1  $SW --nodes 8 --rounds 4 --mode timed
one 141-156 "timed N8R4 shared16"    16 $SW --nodes 8 --rounds 4 --mode timed --parallel shared
one 160-175 "timed N8R4 partition16" 16 $SW --nodes 8 --rounds 4 --mode timed --parallel partitioned
one 180-180 "base N8R6 none"         1  $SW --nodes 8 --rounds 6 --mode baseline
one 181-196 "base N8R6 shared16"     16 $SW --nodes 8 --rounds 6 --mode baseline --parallel shared
one 200-215 "base N8R6 partition16"  16 $SW --nodes 8 --rounds 6 --mode baseline --parallel partitioned
wait
echo SWIMPAR-DONE >> $OUT
