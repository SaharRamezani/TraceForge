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
    let _ = traceforge::inbox_timed(0, WaitTime::Infinite);
}

// ---------------------------------------------------------------------
// Untimed `inbox()` inside a timed config is time-transparent.
// ---------------------------------------------------------------------
//
// Mirrors `legacy_recv_inside_timed_is_rejected` from tests/timed.rs:
// a `with_timed` config is set but the legacy `inbox()` primitive is
// used. A program is either timed or untimed, so the untimed inbox is
// rejected at its first use (a timed program collects with
// `inbox_timed`, whose `min >= 1`).
#[test]
#[should_panic(expected = "TraceForge usage error")]
fn legacy_inbox_inside_timed_is_rejected() {
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
// (t = [2, 2]), and it does NOT block (a finite-W_r inbox always times
// out rather than blocking). Every send is lossy under with_timed, so
// that single outcome appears once per drop combination of a and b.
#[test]
fn timed_inbox_min2_wait2_below_min_returns_empty() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let v = traceforge::inbox_timed(2, WaitTime::Finite(2));
                // The inbox never returns an under-min non-empty set: it is
                // either empty or has exactly `min` (= max = 2) messages.
                assert!(v.is_empty() || v.len() == 2);
                // b is late, so {a, b} never fits W_r: only {} is feasible.
                assert!(v.is_empty(), "only the timeout empty is feasible");
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
    // Every send lossy: the timeout empty once per drop combination of
    // a and b (2 x 2 = 4), no block.
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 4);
    assert_eq!(stats.timeline_impossible, 0);
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
                let _v = traceforge::inbox_timed(2, WaitTime::Infinite);
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
//
// "Reachable" here means EXPLORED, not counted: since (C6') the three
// outcomes with a timeout are dropped when the execution is judged
// whole, but the program has already run and recorded them, so the sink
// still shows all five. What this test pins is unchanged and is what it
// was always for: the inbox and the recv oracle explore the same set.

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
                    one_u32(&traceforge::inbox_timed(1, WaitTime::Finite(10))),
                    one_u32(&traceforge::inbox_timed(1, WaitTime::Finite(10))),
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

/// Outcomes of `two_sequential_run`'s program as seen by an execution
/// observer, which reports exactly the explored endings. (The program's
/// own sink also records runs stopped at a pruned step, whose code ran
/// before the step that has no timeline.)
fn two_sequential_observed(use_inbox: bool) -> (traceforge::Stats, Vec<String>) {
    use traceforge::coverage::ExecutionObserver;
    struct Obs(Arc<Mutex<Vec<String>>>);
    impl ExecutionObserver for Obs {
        fn after(
            &mut self,
            _eid: traceforge::ExecutionId,
            _cond: &traceforge::monitor_types::EndCondition,
            c: traceforge::CoverageInfo,
        ) {
            let mut goals: Vec<String> = c.coverage.keys().cloned().collect();
            goals.sort();
            self.0.lock().unwrap().push(goals.join(" "));
        }
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 0, 0)
            .with_callback(Box::new(Obs(Arc::clone(&seen))))
            .build(),
        move || {
            let collector = thread::spawn(move || {
                let (r1, r2) = if use_inbox {
                    (
                        one_u32(&traceforge::inbox_timed(1, WaitTime::Finite(10))),
                        one_u32(&traceforge::inbox_timed(1, WaitTime::Finite(10))),
                    )
                } else {
                    (
                        traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10)),
                        traceforge::recv_msg_timed::<u32>(WaitTime::Finite(10)),
                    )
                };
                traceforge::cover!(format!("{r1:?} {r2:?}"));
            });
            let cid = collector.thread().id();
            let c1 = cid.clone();
            thread::spawn(move || traceforge::send_msg(c1, 2u32));
            thread::spawn(move || traceforge::send_msg(cid, 3u32));
        },
    );
    let seen = seen.lock().unwrap().clone();
    (stats, seen)
}

#[test]
fn two_sequential_timed_inboxes_have_no_duplicate_executions() {
    // Every send lossy (L = U = sd = 0): the five outcomes are
    // (None, None) with both dropped, (2, None) and (3, None) with one
    // delivered, and (2, 3), (3, 2) with both delivered. A timeout
    // beside a delivered message has no timeline (both are readable
    // only at 0) and is never explored.
    let (inbox_stats, inbox) = two_sequential_observed(true);
    assert_eq!(
        inbox.len(),
        distinct(&inbox).len(),
        "each explored execution should be explored exactly once (no duplicates)"
    );
    assert_eq!(inbox_stats.execs, 5);
    assert_eq!(inbox_stats.timeline_impossible, 0);

    // The recv oracle is duplicate-free by construction; the inbox should
    // explore the same executions, no more.
    let (recv_stats, recv) = two_sequential_observed(false);
    assert_eq!(
        recv.len(),
        distinct(&recv).len(),
        "the recv oracle should not duplicate either"
    );
    assert_eq!(recv_stats.timeline_impossible, 0);
    assert_eq!(
        distinct(&inbox),
        distinct(&recv),
        "inbox outcomes should match the duplicate-free recv oracle"
    );
}

// ---------------------------------------------------------------------
// A timed inbox returns EXACTLY k or the empty set: never fewer, never
// more. This pins the contract that replaced the old [min, max] range.
// ---------------------------------------------------------------------

#[test]
fn single_timed_inbox_returns_exactly_k_or_empty() {
    let sink: Arc<Mutex<Vec<Vec<u32>>>> = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&sink);
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 0).build(), move || {
        let s = Arc::clone(&s);
        let collector = thread::spawn(move || {
            let mut got: Vec<u32> = traceforge::inbox_timed(2, WaitTime::Finite(10))
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

    // Combinatorial oracle: the timeout {} plus every size-2 subset, and
    // NOTHING else. All three sends land at time 0 (L = U = sd = 0), so
    // every pair is jointly readable at the collector's ready instant;
    // singletons and the full triple must not appear, because k = 2 is
    // now exact rather than a lower bound.
    let expected: BTreeSet<Vec<u32>> =
        [vec![], vec![2, 3], vec![2, 4], vec![3, 4]].into_iter().collect();
    assert_eq!(
        distinct(&records),
        expected,
        "a k=2 timed inbox should explore the timeout plus every size-2 subset, and no other size"
    );
    // Every send lossy, by delivered set D: |D| <= 1 (4 drop combos) gives
    // the timeout only (4); |D| = 2 (3 combos) gives the pair + timeout (6);
    // |D| = 3 gives 3 pairs + timeout (4). 4 + 6 + 4 = 14, each once.
    assert_eq!(
        stats.execs, 14,
        "each (drop combination, subset) should be explored exactly once (no duplicates)"
    );
    assert_eq!(stats.timeline_impossible, 0);
}
