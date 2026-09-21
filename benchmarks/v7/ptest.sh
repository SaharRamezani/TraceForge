#!/bin/bash
# does --parallel partitioned parallelize SWIM timed / lease? (cores limited with taskset; num_cpus honors affinity)
B=/local/sramezani/target-v7/release/examples
run() { # cpus, label, cmd...
  local cpus=$1; shift; local label=$1; shift
  local t0=$(date +%s.%N)
  out=$(taskset -c $cpus /usr/bin/time -f "cpu=%U+%S" "$@" 2>&1)
  local t1=$(date +%s.%N)
  echo "$label cpus=$cpus wall=$(echo "$t1 - $t0" | bc) $(echo "$out" | grep -o 'execs=[0-9]*\|blocked=[0-9]*\|violations=[0-9]*\|cpu=[0-9.+]*' | tr '\n' ' ')"
}
SW="$B/swim_timed --nodes 12 --rounds 1 --u 2 --l-ratio 0 --sd-ratio 0.5 --w-probe-ratio 1 --w-suspect-ratio 3.5 --keep-going --mode timed"
for c in 0-15 0-63; do run $c "swim N12R1 timed partitioned" $SW --parallel partitioned; done
LS="$B/lease_timed --clients 4 --rounds 2 --u 2 --l-ratio 0 --sd-ratio 0.5 --ttl-ratio 5 --pause-ratio 1.5 --fencing off --keep-going --mode"
for c in 0-15 0-63; do run $c "lease C4R2 timed partitioned" $LS timed --parallel partitioned; done
for c in 0-15; do run $c "lease C4R2 baseline partitioned" $LS baseline --parallel partitioned; done
echo PTEST-DONE
