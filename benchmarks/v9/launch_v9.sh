#!/bin/bash
# v9 campaign launch on brain05: records the environment, then starts the driver in tmux.
ROOT=/local/sramezani/v9
B=$ROOT/bench
cd "$B" || exit 2
{
  echo "v9 campaign, launched $(date '+%F %T') on $(hostname)"
  echo "source: git archive of commit $(cat $ROOT/COMMIT.txt) (branch nissih), plus Cargo.lock md5 $(md5sum $ROOT/src/Cargo.lock | cut -d' ' -f1)"
  echo "build : cd $ROOT/src && CARGO_TARGET_DIR=$ROOT/target cargo build --release --examples --locked"
  $HOME/.cargo/bin/cargo --version
  $HOME/.cargo/bin/rustc --version
  uname -srvm
  grep PRETTY_NAME /etc/os-release
  lscpu | grep -E "Model name|Socket|Core\(s\)|Thread|NUMA node\(s\)|CPU max MHz"
  echo "governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor), boost $(cat /sys/devices/system/cpu/cpufreq/boost), smt $(cat /sys/devices/system/cpu/smt/control)"
  free -g | sed -n 2p
  echo "load at launch: $(cat /proc/loadavg)"
  echo "scheduler: sweep_v9.py md5 $(md5sum sweep_v9.py | cut -d' ' -f1); plan_v9.csv md5 $(md5sum plan_v9.csv | cut -d' ' -f1), $(($(wc -l < plan_v9.csv) - 1)) jobs"
  echo "measurement cores 2-63,66-127 (one job thread per physical core, hyper-thread siblings 130-191,194-255 unused); service cores 0,1,64,65"
  echo "job environment: TF_PRINT_STATS=1 TF_PROGRESS=100000 RAYON_NUM_THREADS=<cores of the job>; taskset -c <cores>; /usr/bin/time around the binary"
  echo "binaries:"
  md5sum $ROOT/target/release/examples/*_timed $ROOT/target/release/examples/raft_leader_election $ROOT/target/release/examples/comm_closed_leader_election
} > env_v9.txt 2>&1
tmux new-session -d -s v9sweep "bash $B/run_v9.sh"
sleep 3
tmux ls | grep v9sweep
tail -3 sweep_v9.out
