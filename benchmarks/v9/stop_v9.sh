#!/bin/bash
# Stop the v9 campaign early (run on brain05). The scheduler stops every running job, records
# it as DNF with its progress, CPU time and violation count, and exits; nothing is restarted.
B=/local/sramezani/v9/bench
kill -TERM "$(cat $B/sweep_v9.pid)"
sleep 30
tail -3 $B/sweep_v9.out
