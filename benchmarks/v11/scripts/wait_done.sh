#!/bin/bash
# exits (one notification) when both schedulers have stopped, or at 21:57 local at the latest
while true; do
  now=$(date +%H%M)
  a=$(ssh -o BatchMode=yes -o ConnectTimeout=10 sramezani@brain05.mpi-sws.org "taskset -c 0,1,64,65 python3 /tmp/check_v11.py brain05" 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("alive"), d.get("rows"), d.get("dnf"), d.get("running"))' 2>/dev/null)
  b=$(ssh -o BatchMode=yes -o ConnectTimeout=10 sramezani@brain10.mpi-sws.org "taskset -c 0,1,64,65 python3 /tmp/check_v11.py brain10" 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("alive"), d.get("rows"), d.get("dnf"), d.get("running"))' 2>/dev/null)
  case "$a" in 0*) case "$b" in 0*) echo "BOTH SCHEDULERS STOPPED at $now: brain05[$a] brain10[$b]"; exit 0;; esac;; esac
  if [ "$now" -ge 2157 ]; then echo "TIME LIMIT at $now: brain05[$a] brain10[$b]"; exit 0; fi
  sleep 20
done
