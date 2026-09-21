#!/bin/bash
# lease R=1 large C, partitioned on 16 pinned cores, timed and baseline (b = blocked/execs growth)
B=/local/sramezani/target-v7/release/examples
for C in 6 7 8; do
 for mode in baseline timed; do
  t0=$(date +%s.%N)
  out=$(taskset -c 200-215 /usr/bin/time -f "cpu=%U+%S rss=%MkB" timeout 1500 $B/lease_timed --clients $C --rounds 1 --u 2 --l-ratio 0 --sd-ratio 0.5 --ttl-ratio 5 --pause-ratio 1.5 --fencing off --keep-going --parallel partitioned --mode $mode 2>&1)
  t1=$(date +%s.%N)
  echo "lease C$C R1 $mode wall=$(echo "$t1 - $t0" | bc) $(echo "$out" | grep -o 'execs=[0-9]*\|blocked=[0-9]*\|violations=[0-9]*\|cpu=[0-9.+]*' | tr '\n' ' ')" | tee -a ~/traceforge-v7/benchmarks/lease_r1.out
 done
done
echo LEASE-R1-DONE >> ~/traceforge-v7/benchmarks/lease_r1.out
