//! Integration tests for the composed timed-inbox primitive
//! ([`traceforge::inbox_timed`] / [`traceforge::inbox_with_tag_timed`] /
//! [`traceforge::inbox_with_vec_tag_timed`]).
//!
//! These exercise the interaction between the inbox subset enumeration
//! and the timed walker arm for `LabelEnum::Inbox`.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use traceforge::{thread, Config, Val, WaitTime};

// ---------------------------------------------------------------------
// The timed inbox forbids min == 0 (the untimed non-blocking marker).
// ---------------------------------------------------------------------
//
// `min == 0` only exists to make the *untimed* paper inbox non-blocking.
// With a timeout it is redundant: non-blocking is `WaitTime::Finite(0)`
// and "receive one message with a timeout" is `(1, Some(1), wait)`. The
// guard fires on entry, before any execution state is touched, so we can
// assert it directly without a `verify` run.
//
// (The earlier `min == 0` timed-inbox tests that asserted the "two
// time-distinct empties" enumeration were removed: that outcome no longer
// exists under the new contract.)
#[test]
#[should_panic(expected = "timed inbox requires min >= 1")]
fn timed_inbox_min0_is_forbidden() {
    let _ = traceforge::inbox_timed(0, None, WaitTime::Infinite);
}

// ---------------------------------------------------------------------
// Untimed `inbox()` inside a timed config is time-transparent.
// ---------------------------------------------------------------------
//
// Mirrors `legacy_recv_inside_timed_is_transparent` from tests/timed.rs:
// a `with_timed` config is set but the legacy `inbox()` primitive is
// used. Its `wait` is `None` so the walker passes the inbox event
// through unchanged. (This is the untimed inbox, where `min == 0` is the
// legitimate non-blocking paper semantics, out of scope for the guard.)
#[test]
fn legacy_inbox_inside_timed_is_transparent() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 10, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _v = traceforge::inbox();
            });
            let cid = collector.thread().id();
            // A late send that a *timed* inbox would prune, but a
            // legacy one ignores.
            let _a = thread::spawn(move || {
                traceforge::sleep(1000);
                traceforge::send_msg(cid, 42i32);
            });
        },
    );
    // Subsets are {} and {a}; both admitted since this inbox is untimed.
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Below-min finite-W_r inbox returns only the (timeout) empty set.
// ---------------------------------------------------------------------
//
// The user asks for exactly 2 messages within W_r = 2 time units, but the
// second sender sleeps 100 before sending. With L=U=0, sd=0 the only size-2
// subset {a,b} has an empty window (b is late). An under-min subset like
// {a} (size 1 < min 2) is NEVER a valid return (boss semantics: return is
// {} or a set of [min, max]). So the only outcome is the timeout empty {}
// (t = [2, 2]): exactly one execution that returns {}, and it does NOT
// block (a finite-W_r inbox always times out rather than blocking).
#[test]
fn timed_inbox_min2_wait2_below_min_returns_empty() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let v = traceforge::inbox_timed(2, Some(2), WaitTime::Finite(2));
                // The inbox never returns an under-min non-empty set: it is
                // either empty or has exactly `min` (= max = 2) messages.
                assert!(v.is_empty() || v.len() == 2);
            });
            let cid = collector.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg(cid, 0i32); // t = 0, fits W_r = 2
            });
            let _b = thread::spawn(move || {
                traceforge::sleep(100); // misses W_r = 2
                traceforge::send_msg(cid, 1i32);
            });
        },
    );
    // The only feasible outcome is the timeout empty: one execution, no block.
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// W_r=∞ keeps the original block-until-≥min behaviour.
// ---------------------------------------------------------------------
//
// An infinite-wait inbox with min=2 must collect ≥ 2 messages or stay
// blocked. With only one matching send available, the inbox blocks on
// every schedule. Distinguishes the new finite-W_r semantics above
// from the infinite-W_r case which is unchanged.
#[test]
fn timed_inbox_min2_infinite_one_sender_blocks() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _v = traceforge::inbox_timed(2, Some(2), WaitTime::Infinite);
            });
            let cid = collector.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg(cid, 0i32);
            });
        },
    );
    // No execution can finish: ≥2 messages required but only 1 exists,
    // and W_r=∞ means no timeout fallback.
    assert_eq!(stats.execs, 0);
    assert!(stats.block > 0);
}

// ---------------------------------------------------------------------
// Two sequential timed inboxes explore every reachable outcome.
// ---------------------------------------------------------------------

// min=max=1 -> the inbox is empty (timeout) or holds exactly one message.
fn one_u32(v: &[Option<Val>]) -> Option<u32> {
    let vals: Vec<u32> = v
        .iter()
        .flatten()
        .map(|val| *val.as_any_ref().downcast_ref::<u32>().unwrap())
        .collect();
    assert!(vals.len() <= 1, "expected <= 1 message, got {vals:?}");
    vals.first().copied()
}

fn two_sequential_run(use_inbox: bool) -> (usize, Vec<(Option<u32>, Option<u32>)>) {
    let sink: Arc<Mutex<Vec<(Option<u32>, Option<u32>)>>> = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 0).build(), move || {
        let s = Arc::clone(&s);
        let collector = thread::spawn(move || {
            let (r1, r2) = if use_inbox {
                (
                    one_u32(&traceforge::inbox_timed(1, Some(1), WaitTime::Finite(10))),
                    one_u32(&traceforge::inbox_timed(1, Some(1), WaitTime::Finite(10))),
                )
            } else {
                (
                    traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10)),
                    traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10)),
                )
            };
            s.lock().unwrap().push((r1, r2));
        });
        let cid = collector.thread().id();
        let c1 = cid.clone();
        thread::spawn(move || traceforge::send_msg(c1, 2u32));
        thread::spawn(move || traceforge::send_msg(cid, 3u32));
    });
    let records = sink.lock().unwrap().clone();
    (stats.execs, records)
}

fn distinct<T: Ord + Clone>(records: &[T]) -> BTreeSet<T> {
    records.iter().cloned().collect()
}

#[test]
fn two_sequential_timed_inboxes_explore_all_outcomes() {
    let expected: BTreeSet<(Option<u32>, Option<u32>)> = [
        (None, None),
        (Some(2), None),
        (Some(3), None),
        (Some(2), Some(3)),
        (Some(3), Some(2)),
    ]
    .into_iter()
    .collect();

    // The timed-recv oracle pins the reachable set (sanity-checks the shape).
    let (_, recv) = two_sequential_run(false);
    assert_eq!(
        distinct(&recv),
        expected,
        "recv oracle should find all 5 reachable outcomes"
    );

    // The timed inbox must explore exactly the same outcomes.
    let (_, inbox) = two_sequential_run(true);
    assert_eq!(
        distinct(&inbox),
        expected,
        "two sequential timed inboxes should explore every reachable outcome"
    );
}

// ---------------------------------------------------------------------
// Two sequential timed inboxes explore each execution exactly once.
// ---------------------------------------------------------------------

#[test]
fn two_sequential_timed_inboxes_have_no_duplicate_executions() {
    let (inbox_execs, inbox) = two_sequential_run(true);
    assert_eq!(
        inbox_execs,
        distinct(&inbox).len(),
        "each execution should be explored exactly once (no duplicates)"
    );

    // The recv oracle is duplicate-free by construction; the inbox should
    // explore the same number of executions, no more.
    let (recv_execs, _) = two_sequential_run(false);
    assert_eq!(
        inbox_execs, recv_execs,
        "inbox exec count should match the duplicate-free recv oracle"
    );
}

// ---------------------------------------------------------------------
// A single min<max finite inbox explores every size-bounded subset once.
// ---------------------------------------------------------------------

#[test]
fn single_timed_inbox_min1_max2_explores_all_subsets_once() {
    let sink: Arc<Mutex<Vec<Vec<u32>>>> = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 0).build(), move || {
        let s = Arc::clone(&s);
        let collector = thread::spawn(move || {
            let mut got: Vec<u32> = traceforge::inbox_timed(1, Some(2), WaitTime::Finite(10))
                .iter()
                .flatten()
                .map(|v| *v.as_any_ref().downcast_ref::<u32>().unwrap())
                .collect();
            got.sort();
            s.lock().unwrap().push(got);
        });
        let cid = collector.thread().id();
        for v in 2u32..=4 {
            let cid = cid.clone();
            thread::spawn(move || traceforge::send_msg(cid, v));
        }
    });
    let records = sink.lock().unwrap().clone();

    // Combinatorial oracle: timeout {} + all size-1 + all size-2 subsets.
    let expected: BTreeSet<Vec<u32>> = [
        vec![],
        vec![2],
        vec![3],
        vec![4],
        vec![2, 3],
        vec![2, 4],
        vec![3, 4],
    ]
    .into_iter()
    .collect();
    assert_eq!(
        distinct(&records),
        expected,
        "a min=1,max=2 inbox should explore the timeout plus every size-[1,2] subset"
    );
    assert_eq!(
        stats.execs,
        expected.len(),
        "each subset should be explored exactly once (no duplicates)"
    );
}
