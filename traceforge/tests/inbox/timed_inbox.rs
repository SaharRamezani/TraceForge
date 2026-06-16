//! Integration tests for the composed timed-inbox primitive
//! ([`traceforge::inbox_timed`] / [`traceforge::inbox_with_tag_timed`] /
//! [`traceforge::inbox_with_vec_tag_timed`]).
//!
//! These exercise the interaction between the inbox subset enumeration
//! and the timed walker arm for `LabelEnum::Inbox`.

use traceforge::{thread, Config, WaitTime};

// ---------------------------------------------------------------------
// Untimed-style inbox under a timed config: should explore every subset
// that the untimed `inbox()` would.
// ---------------------------------------------------------------------
//
// Two senders, min=0 inbox: subsets are {}, {a}, {b}, {a,b} = 4.
// With W_r=∞ the timed walker should accept all of them (sends fit the
// generous global L=0, U=100 window).
#[test]
fn timed_inbox_infinite_admits_all_subsets() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 100, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _v = traceforge::inbox_timed(0, None, WaitTime::Infinite);
            });
            let cid = collector.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg(cid, 1i32);
            });
            let _b = thread::spawn(move || {
                traceforge::send_msg(cid, 2i32);
            });
        },
    );
    assert_eq!(stats.execs, 4);
}

// ---------------------------------------------------------------------
// Finite counterpart of the test above: same two on-time senders, but a
// finite W_r adds the timeout empty as a distinct outcome.
// ---------------------------------------------------------------------
//
// min=0 inbox, both sends fit the window. The structural subsets are
// {}, {a}, {b}, {a,b}; the empty {} additionally has the time-distinct
// timeout form. So 5 executions (one more than the infinite case).
#[test]
fn timed_inbox_finite_two_on_time_senders() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 100, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _v = traceforge::inbox_timed(0, None, WaitTime::Finite(100));
            });
            let cid = collector.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg(cid, 1i32);
            });
            let _b = thread::spawn(move || {
                traceforge::send_msg(cid, 2i32);
            });
        },
    );
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 5);
}

// ---------------------------------------------------------------------
// Finite W_r prunes subsets whose sends would arrive after the wait.
// ---------------------------------------------------------------------
//
// Sender sleeps for 100, then sends. The inbox has wait=10. The send
// window is [100, 100]; the inbox window with the send in the subset is
// max(0, 100+0)..min(0+10, 100+0+0) = [100, 10] (empty), so the singleton
// subset {a} is pruned. With min=0 the empty result is explored in both of
// its time-distinct forms (boss semantics): the immediate empty {} at t=0
// and the timeout empty {} at t=10. So 2 executions.
#[test]
fn timed_inbox_finite_wait_prunes_late_send() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _v = traceforge::inbox_timed(0, None, WaitTime::Finite(10));
            });
            let cid = collector.thread().id();
            let _a = thread::spawn(move || {
                traceforge::sleep(100);
                traceforge::send_msg(cid, 1i32);
            });
        },
    );
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Finite W_r admits the singleton subset whose send fits the window.
// ---------------------------------------------------------------------
//
// Sender sleeps for 5, then sends. The inbox has wait=10. The send
// window is [5, 5]; the inbox window with the send in the subset is
// max(0, 5+0)..min(0+10, 5+0+0) = [5, 5] (non-empty).
// With min=0 the outcomes are: the immediate empty {} (t = 0), the timeout
// empty {} (t = 10), and {a} (t = 5). All three survive: 3 executions.
#[test]
fn timed_inbox_finite_wait_admits_on_time_send() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _v = traceforge::inbox_timed(0, None, WaitTime::Finite(10));
            });
            let cid = collector.thread().id();
            let _a = thread::spawn(move || {
                traceforge::sleep(5);
                traceforge::send_msg(cid, 1i32);
            });
        },
    );
    assert_eq!(stats.execs, 3);
}

// ---------------------------------------------------------------------
// min=0 finite-W_r inbox with zero senders: the two time-distinct empties.
// ---------------------------------------------------------------------
//
// Even with no matching sends at all, a non-blocking (min=0) finite-W_r
// inbox has two executions that both return {} but at different times: the
// immediate empty (t = pred) and the timeout empty (t = pred + W_r). They
// must not be collapsed (boss semantics).
#[test]
fn timed_inbox_min0_finite_zero_senders_two_empties() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let _collector = thread::spawn(|| {
                let v = traceforge::inbox_timed(0, None, WaitTime::Finite(10));
                assert!(v.is_empty());
            });
        },
    );
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Untimed `inbox()` inside a timed config is time-transparent.
// ---------------------------------------------------------------------
//
// Mirrors `legacy_recv_inside_timed_is_transparent` from tests/timed.rs:
// a `with_timed` config is set but the legacy `inbox()` primitive is
// used. Its `wait` is `None` so the walker passes the inbox event
// through unchanged.
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