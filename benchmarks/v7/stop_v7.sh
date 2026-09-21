#!/bin/bash
# stop the ceiling sweep (kills driver + jobs; the driver records running jobs as DNF on SIGTERM, we SIGKILL instead
# because this is used to discard a discarded first attempt)
tmux kill-session -t v7sweep 2>/dev/null
for p in $(pgrep -u sramezani -f "sweep_v[7]"); do kill -9 $p; done
for p in $(pgrep -u sramezani -f "target-v[7]/release/examples/(lease|swim)_time[d]"); do kill -9 $p; done
sleep 3
echo "left:"; ps -u sramezani -o pid,args | grep -E "sweep_v[7]|target-v[7]/release" | grep -v grep | cut -c1-100
