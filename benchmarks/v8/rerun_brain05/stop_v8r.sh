#!/bin/bash
# graceful stop: SIGTERM the scheduler (it kills its jobs and records them as DNF)
kill -TERM "$(cat $HOME/traceforge-v8r/run/sweep_v8r.pid)"
sleep 5
tmux kill-session -t v8r 2>/dev/null
