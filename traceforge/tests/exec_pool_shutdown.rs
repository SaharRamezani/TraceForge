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

/// Two receives of three racing sends; the assertion fails whenever the
/// larger value is read first, so some interleavings violate it.
fn failing_program() {
    let receiver = thread::spawn(|| {
        let a: Msg = traceforge::recv_msg_block();
        let b: Msg = traceforge::recv_msg_block();
        traceforge::assert(a.0 < b.0);
    });
    let tid = receiver.thread().id();
    for i in 0..3u32 {
        let t = tid.clone();
        thread::spawn(move || traceforge::send_msg(t, Msg(i)));
    }
}

/// Runs `verify` on its own thread and reports whether it panicked, or
/// `None` if it did not finish within the deadline (a hang).
fn panics_within(conf: Config, secs: u64) -> Option<bool> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            traceforge::verify(conf, failing_program);
        }));
        let _ = tx.send(r.is_err());
    });
    rx.recv_timeout(std::time::Duration::from_secs(secs)).ok()
}

/// A failed assertion without keep-going panics inside the worker that found
/// it. That used to kill the worker while its state stayed Busy, so the
/// drainer waited forever for "no worker Busy" and the violation was never
/// reported, where a sequential run reports it at once. The run must end in
/// the model's panic, exactly as a sequential run does.
#[test]
fn shared_pool_reports_a_failed_assertion() {
    assert_eq!(
        panics_within(Config::builder().with_parallel(false).build(), 60),
        Some(true),
        "the program must violate its assertion sequentially"
    );
    for round in 0..20 {
        let conf = Config::builder()
            .with_parallel(true)
            .with_parallel_workers(4)
            .build();
        match panics_within(conf, 60) {
            Some(true) => {}
            Some(false) => panic!("round {round}: shared pool finished without reporting the violation"),
            None => panic!("round {round}: shared pool hung instead of reporting the violation"),
        }
    }
}

/// With keep-going the assertion does not panic: the shared pool must still
/// terminate and agree with the sequential execution counts.
#[test]
fn shared_pool_keep_going_matches_sequential() {
    let sequential = traceforge::verify(
        Config::builder().with_keep_going_after_error(true).build(),
        failing_program,
    );
    let shared = traceforge::verify(
        Config::builder()
            .with_keep_going_after_error(true)
            .with_parallel(true)
            .with_parallel_workers(4)
            .build(),
        failing_program,
    );
    assert_eq!(shared.execs, sequential.execs);
    assert_eq!(shared.block, sequential.block);
}
