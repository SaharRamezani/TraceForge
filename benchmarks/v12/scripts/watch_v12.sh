#!/bin/bash
# one line on: scheduler death, unexpected job status, climber death; plus a progress line every ~30 min
last=0
while true; do
  now=$(date +%s); line=""
  for m in brain05 brain10; do
    j=$(ssh -o BatchMode=yes -o ConnectTimeout=10 sramezani@$m.mpi-sws.org "taskset -c 0,1,64,65 python3 /tmp/check_v12.py $m; tmux ls 2>/dev/null | grep -c v12climb" 2>/dev/null)
    [ -z "$j" ] && { echo "WARN $m: check failed"; continue; }
    js=$(echo "$j" | head -1); cl=$(echo "$j" | tail -1)
    alive=$(echo "$js" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("alive"))')
    bad=$(echo "$js" | python3 -c 'import sys,json; print(",".join(json.load(sys.stdin).get("bad",[])))')
    [ "$alive" = "0" ] && echo "SCHEDULER NOT RUNNING on $m: $js"
    [ -n "$bad" ] && echo "UNEXPECTED STATUS on $m: $bad"
    line="$line $m: $(echo "$js" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("rows=%s fin=%s dnf=%s run=%s q=%s free=%s" % (d.get("rows"),d.get("fin"),d.get("dnf"),d.get("running"),d.get("queued"),d.get("free")))') climb=$cl"
  done
  if [ $((now-last)) -ge 1800 ]; then last=$now; echo "PROGRESS $(date +%H:%M):$line"; fi
  sleep 120
done
