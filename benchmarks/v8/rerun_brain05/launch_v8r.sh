#!/bin/bash
cd "$HOME/traceforge-v8r/run"
T=/local/sramezani/target-v8r
{ echo "v8 rerun, launched $(date '+%F %T') on $(hostname), $(cargo --version)";
  echo "old = commit bc296a7; new = commit e96fe7a; vsend = bc296a7 + send change only; vrest = e96fe7a with the send change reverted";
  for b in old new vsend vrest; do md5sum $HOME/traceforge-v8r/$b/traceforge/src/must.rs $HOME/traceforge-v8r/$b/traceforge/src/parallel_verify.rs $T/$b/release/examples/lease_timed; done; } > env_v8r.txt
tmux new-session -d -s v8r "bash $HOME/traceforge-v8r/run/run_v8r.sh"
sleep 15
tmux ls
head -3 sweep_v8r.out
uptime
