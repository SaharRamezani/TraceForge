#!/bin/bash
# Event stream for the v11 campaign: emits only on scheduler death, unexpected job status, and time marks.
MARKS="2130 2152"
declare -A fired
declare -A prevbad
declare -A prevalive
while true; do
  now=$(date +%H%M)
  line=""
  for m in brain05 brain10; do
    j=$(ssh -o BatchMode=yes -o ConnectTimeout=10 sramezani@$m.mpi-sws.org "taskset -c 0,1,64,65 python3 /tmp/check_v11.py $m" 2>/dev/null)
    [ -z "$j" ] && { echo "WARN $m: ssh/check failed at $now"; continue; }
    alive=$(echo "$j" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("alive"))')
    bad=$(echo "$j" | python3 -c 'import sys,json; print(",".join(json.load(sys.stdin).get("bad",[])))')
    if [ "$alive" = "0" ] && [ "${prevalive[$m]}" != "0" ]; then echo "SCHEDULER NOT RUNNING on $m at $now: $j"; fi
    prevalive[$m]=$alive
    if [ -n "$bad" ] && [ "${prevbad[$m]}" != "$bad" ]; then echo "UNEXPECTED STATUS on $m at $now: $bad"; fi
    prevbad[$m]=$bad
    line="$line $m:$(echo "$j" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("rows=%s fin=%s dnf=%s run=%s free=%s" % (d.get("rows"),d.get("fin"),d.get("dnf"),d.get("running"),d.get("free")))')"
  done
  for mk in $MARKS; do
    if [ "$now" -ge "$mk" ] && [ -z "${fired[$mk]}" ]; then
      fired[$mk]=1
      echo "MARK $mk (local $now):$line"
    fi
  done
  sleep 60
done
