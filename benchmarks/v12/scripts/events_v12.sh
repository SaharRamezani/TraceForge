#!/bin/bash
# events: long runs finishing (wall >= 600 s, any parallel/phase C row), idle capacity
S=/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v12/events_seen.txt
touch $S
declare -A lastidle
first=$([ -s $S ] && echo 0 || echo 1)
while true; do
  for m in brain05 brain10; do
    scp -q -o BatchMode=yes -o ConnectTimeout=10 /tmp/events_probe.py sramezani@$m.mpi-sws.org:/tmp/events_probe.py 2>/dev/null
    out=$(ssh -o BatchMode=yes -o ConnectTimeout=10 sramezani@$m.mpi-sws.org "taskset -c 0,1,64,65 python3 /tmp/events_probe.py $m" 2>/dev/null)
    [ -z "$out" ] && continue
    st=$(echo "$out" | grep '^STATUS' | head -1)
    run=$(echo "$st" | awk '{print $3}'); q=$(echo "$st" | awk '{print $4}'); fr=$(echo "$st" | awk '{print $5}')
    echo "$out" | grep '^RES' | while read -r _ mm id status wall cores cell; do
      if ! grep -q -F " $id " $S; then
        echo " $id " >> $S
        [ $first -eq 0 ] && echo "FINISHED $mm $id $status ${wall}s cores=$cores $cell"
      fi
    done
    now=$(date +%s)
    if [ "$q" = "0" ] && [ "${fr:-0}" -ge 24 ] 2>/dev/null; then
      l=${lastidle[$m]:-0}
      if [ $((now-l)) -ge 900 ]; then lastidle[$m]=$now; echo "IDLE $m free=$fr queued=$q running=$run"; fi
    fi
  done
  first=0
  sleep 90
done
