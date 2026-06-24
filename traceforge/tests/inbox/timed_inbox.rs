//! Integration tests for the composed timed-inbox primitive
//! ([`traceforge::inbox_timed`] / [`traceforge::inbox_with_tag_timed`] /
//! [`traceforge::inbox_with_vec_tag_timed`]).
//!
//! These exercise the interaction between the inbox subset enumeration
//! and the timed walker arm for `LabelEnum::Inbox`.

use traceforge::{thread, Config, WaitTime};

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
