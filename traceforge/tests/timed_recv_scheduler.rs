//! Does a plain TIMED RECV (no inbox) have the same scheduler/revisit gap the
//! timed inbox had?
//!
//! The inbox bug: when the collector reaches the inbox before any send is in
//! the graph (the normal L2R order), a finite-wait inbox times out with
//! rfs()==None, and that timed-out inbox was not backward-revisitable, so the
//! sends that arrive later were never collected.
//!
//! A plain finite-wait recv is constructed with non_blocking==true, so
//! Consistency::reads_tiebreaker takes its `if rlab.is_non_blocking() { return
//! rlab.rf().is_none() }` branch: a timed-out recv (rf()==None) is already
//! maximal/revisitable. So the prediction is: recv does NOT have the bug, and a
//! timed recv reached before its senders still explores reading each send.
//!
//! These tests verify that empirically. Each sender ships a distinct value; we
//! record what the recv returned (None = timeout, Some(v) = read sender v)
//! across all executions and assert the full set of outcomes is explored.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use traceforge::{thread, Config, WaitTime};

type Sink = Arc<Mutex<Vec<Option<u32>>>>;

fn distinct(sink: &Sink) -> BTreeSet<Option<u32>> {
    sink.lock().unwrap().iter().cloned().collect()
}

fn dump(tag: &str, execs: usize, block: usize, sink: &Sink) {
    eprintln!(
        "[{tag}] execs={execs} block={block} outcomes={:?}",
        distinct(sink)
    );
}

// ---------------------------------------------------------------------
// CONTROL: untimed NON-BLOCKING recv (recv_msg -> Option), 2 senders, no time.
// Establishes the reference outcome set for a non-blocking recv whose senders
// are scheduled after it: {None (got nothing), Some(1), Some(2)}.
// ---------------------------------------------------------------------
#[test]
fn recv_untimed_nonblocking_two_senders() {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(Config::builder().build(), move || {
        let s = Arc::clone(&s);
        let collector = thread::spawn(move || {
            let v = traceforge::recv_msg::<u32>();
            s.lock().unwrap().push(v);
        });
        let cid = collector.thread().id();
        for v in 1u32..=2 {
            let cid = cid.clone();
            thread::spawn(move || traceforge::send_msg(cid, v));
        }
    });
    dump("CTRL untimed nonblocking", stats.execs, stats.block, &sink);
    assert_eq!(
        distinct(&sink),
        BTreeSet::from([None, Some(1), Some(2)]),
        "non-blocking recv should explore timeout + each send"
    );
}

// ---------------------------------------------------------------------
// EXP R-E (mirror of inbox EXP E): TIMED recv, 2 senders at t=0, W=10, sd=0.
// The recv is reached before the sends exist and times out; the prediction is
// that it is still revisited to read each send.
//
// PREDICTION (no bug): {None, Some(1), Some(2)}.
// IF IT HAD THE INBOX BUG: {None} only.
// ---------------------------------------------------------------------
#[test]
fn recv_timed_sd0_two_senders_at_t0() {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        move || {
            let s = Arc::clone(&s);
            let collector = thread::spawn(move || {
                let v = traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10));
                s.lock().unwrap().push(v);
            });
            let cid = collector.thread().id();
            let c1 = cid.clone();
            thread::spawn(move || traceforge::send_msg(c1, 1u32));
            thread::spawn(move || traceforge::send_msg(cid, 2u32));
        },
    );
    dump("R-E timed sd=0 two@t0", stats.execs, stats.block, &sink);
    assert_eq!(
        distinct(&sink),
        BTreeSet::from([None, Some(1), Some(2)]),
        "timed recv reached before its senders should still read each send (plus timeout)"
    );
}

// ---------------------------------------------------------------------
// EXP R-D (mirror of inbox EXP D): TIMED recv, single LATE sender (t=5), W=10,
// sd=0. The send arrives within the window, so reading it is feasible.
//
// PREDICTION (no bug): {None, Some(7)}.
// ---------------------------------------------------------------------
#[test]
fn recv_timed_sd0_one_late_sender_in_window() {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        move || {
            let s = Arc::clone(&s);
            let collector = thread::spawn(move || {
                let v = traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10));
                s.lock().unwrap().push(v);
            });
            let cid = collector.thread().id();
            thread::spawn(move || {
                traceforge::sleep(5);
                traceforge::send_msg(cid, 7u32);
            });
        },
    );
    dump("R-D timed sd=0 one late", stats.execs, stats.block, &sink);
    assert_eq!(
        distinct(&sink),
        BTreeSet::from([None, Some(7)]),
        "timed recv should explore both timeout and reading the in-window late send"
    );
}

// ---------------------------------------------------------------------
// EXP R-LATE: TIMED recv, single sender that arrives AFTER the window
// (sleep 100, W=2, sd=0). Reading it is timed-INFEASIBLE, so only the timeout
// should be explored. Sanity check that the timed filter still prunes the
// out-of-window read (analogue of timed_inbox_min2_wait2_below_min).
//
// PREDICTION: {None} only, no block.
// ---------------------------------------------------------------------
#[test]
fn recv_timed_sd0_one_sender_out_of_window() {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        move || {
            let s = Arc::clone(&s);
            let collector = thread::spawn(move || {
                let v = traceforge::recv_msg_timed::<u32>(WaitTime::Finite(2));
                s.lock().unwrap().push(v);
            });
            let cid = collector.thread().id();
            thread::spawn(move || {
                traceforge::sleep(100);
                traceforge::send_msg(cid, 9u32);
            });
        },
    );
    dump("R-LATE timed sd=0 out-of-window", stats.execs, stats.block, &sink);
    assert_eq!(stats.block, 0);
    assert_eq!(
        distinct(&sink),
        BTreeSet::from([None]),
        "an out-of-window send is timed-infeasible to read; only the timeout remains"
    );
}

// ---------------------------------------------------------------------
// EXP R-3: TIMED recv, 3 senders at t=0, W=10, sd=0. Stress the
// before-the-sends ordering with more senders.
//
// PREDICTION (no bug): {None, Some(1), Some(2), Some(3)}.
// ---------------------------------------------------------------------
#[test]
fn recv_timed_sd0_three_senders_at_t0() {
    let sink: Sink = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        move || {
            let s = Arc::clone(&s);
            let collector = thread::spawn(move || {
                let v = traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10));
                s.lock().unwrap().push(v);
            });
            let cid = collector.thread().id();
            for v in 1u32..=3 {
                let cid = cid.clone();
                thread::spawn(move || traceforge::send_msg(cid, v));
            }
        },
    );
    dump("R-3 timed sd=0 three@t0", stats.execs, stats.block, &sink);
    assert_eq!(
        distinct(&sink),
        BTreeSet::from([None, Some(1), Some(2), Some(3)]),
        "timed recv with 3 senders reached first should explore timeout + each send"
    );
}
