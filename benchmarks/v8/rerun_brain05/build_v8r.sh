#!/bin/bash
# Build the four source trees of the v8 rerun (old = bc296a7, new = e96fe7a,
# vsend = bc296a7 + send change only, vrest = e96fe7a with the send change reverted).
# Targets are explicit: brain05 exports an old CARGO_TARGET_DIR.
T=/local/sramezani/target-v8r
: > $T/build.status
for b in old new vsend vrest; do
  ( cd $HOME/traceforge-v8r/$b && CARGO_TARGET_DIR=$T/$b $HOME/.cargo/bin/cargo build --release --examples --locked -j 56 > $T/build_$b.log 2>&1; echo "BUILD $b rc=$?" >> $T/build.status ) &
done
wait
echo ALL-BUILDS-DONE >> $T/build.status
