//! Regression guard for the shared execution pool's end-of-run protocol.
//!
//! The drainer used to decide "run finished" from two separate observations
//! (queue empty, then no worker Busy) while a worker marked itself Busy a few
//! instructions after popping the last graph, and worker state stores could
//! overwrite a `Shutdown` that is issued exactly once. Either way a finished
//! run could hang without ever joining its workers. The window is tiny, so
//! this test simply drives the pool through its end-of-run path many times
//! on a small program with backward revisits (shared-queue traffic) and
//! checks that every run terminates with the sequential execution count.

use traceforge::thread;
use traceforge::Config;

#[derive(Clone, Debug, PartialEq)]
struct Msg(u32);

fn program() {
    let receiver = thread::spawn(|| {
        let a: Msg = traceforge::recv_msg_block();
        let b: Msg = traceforge::recv_msg_block();
        traceforge::assert(a.0 != b.0 || a.0 == b.0);
    });
    let tid = receiver.thread().id();
    for i in 0..3u32 {
        let t = tid.clone();
        thread::spawn(move || traceforge::send_msg(t, Msg(i)));
    }
}

#[test]
fn shared_pool_terminates_every_time() {
    let sequential = traceforge::verify(Config::builder().with_parallel(false).build(), program);
    assert!(sequential.execs > 1, "program must have several executions");

    for round in 0..300 {
        let stats = traceforge::verify(
            Config::builder()
                .with_parallel(true)
                .with_parallel_workers(4)
                .build(),
            program,
        );
        assert_eq!(
            stats.execs, sequential.execs,
            "round {round}: shared pool count differs from sequential"
        );
        assert_eq!(stats.block, sequential.block, "round {round}: blocked count differs");
    }
}
