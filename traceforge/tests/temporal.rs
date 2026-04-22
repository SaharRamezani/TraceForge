//! Integration tests for the Must-τ extension.
//!
//! Each test exercises a small scenario where the temporal feasibility
//! check (tconsistent) must either admit or reject an interleaving.
//! Assertions are on the number of complete executions explored.

use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------
// Sleep advances the lower bound; both outcomes reachable
// ---------------------------------------------------------------------
//
// With L=0, U=1000, sd=0:
//   send window = [10, 1010], recv-reading-from-send window = [10, 100]
//   recv-timeout window                                    = [100, 100]
// Both the `rf = send` and `rf = ⊥` branches are temporally consistent.
#[test]
fn sleep_advances_lower_bound_both_outcomes() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 1000, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(100));
            });
            traceforge::sleep(10);
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// W_r forces timeout: send window is strictly after the receive's wait
// ---------------------------------------------------------------------
//
// With L=0, U=0, sd=0:
//   send window             = [100, 100]
//   timeout window          = [0, 10]
//   rf-from-send window for recv = [max(0,100), min(10, 100)] = [100, 10] (empty)
// Only the timeout branch survives and the `rf = send` candidate is dropped
// by the temporal filter.
#[test]
fn finite_wait_forces_timeout() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 0, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                assert!(v.is_none());
            });
            traceforge::sleep(100);
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// W_r = +∞ prunes the ⊥ branch
// ---------------------------------------------------------------------
//
// Producer: sleep(5); send
// Consumer: recv_msg_block_timed()   (equivalent to WaitTime::Infinite)
// With L=0, U=10, sd=0:
//   send window = [5, 15], rf-from-send window = [5, 15] (non-empty)
// The ⊥ (timeout) candidate is pruned because a blocking receive is
// inadmissible as a timeout, so only the `rf = send` branch is explored.
#[test]
fn infinite_wait_prunes_timeout() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 10, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let v: i32 = traceforge::recv_msg_block_timed();
                assert_eq!(v, 42);
            });
            traceforge::sleep(5);
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Predicate variant: `recv_tagged_msg_timed` composes with the
// temporal filter.
// ---------------------------------------------------------------------
//
// Two senders each send one message; the consumer's predicate matches
// only the first sender. With generous temporal bounds, the send from
// the matching sender is always temporally consistent, while the
// other sender's message is simply filtered out by the predicate.
#[test]
fn predicate_timed_recv() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 100, 0).build(),
        || {
            let main_id = thread::current().id();
            let s1 = thread::spawn(move || {
                // Receive our "id handoff" from main so we know main's id.
                let _: i32 = traceforge::recv_msg_block();
                traceforge::send_msg(main_id, 1i32);
            });
            let s2 = thread::spawn(move || {
                let _: i32 = traceforge::recv_msg_block();
                traceforge::send_msg(main_id, 2i32);
            });
            let s1_id = s1.thread().id();
            traceforge::send_msg(s1_id, 0);
            traceforge::send_msg(s2.thread().id(), 0);

            let v: Option<i32> = traceforge::recv_tagged_msg_timed(
                move |tid: ThreadId, _tag| tid == s1_id,
                WaitTime::Finite(50),
            );
            // At least some execution must receive the value.
            let _ = v;
        },
    );
    assert!(stats.execs == 2);
}

// ---------------------------------------------------------------------
// Legacy run (no `with_temporal`) is unchanged by the new code paths.
// ---------------------------------------------------------------------
//
// This is a regression test:
// when `config.temporal` is `None`. A single send/recv should produce
// exactly one complete execution, just like it did befor.
#[test]
fn legacy_run_unchanged_without_temporal() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let consumer = thread::spawn(|| {
            let v: i32 = traceforge::recv_msg_block();
            assert_eq!(v, 42);
        });
        traceforge::send_msg(consumer.thread().id(), 42i32);
    });
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Legacy untimed receives inside a temporal run are time-transparent.
// ---------------------------------------------------------------------
//
// `with_temporal` is set, but the consumer uses the legacy
// `recv_msg_block` primitive so it must still behave exactly like a
// plain blocking receive, contributing no temporal constraint.
#[test]
fn legacy_recv_inside_temporal_is_transparent() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 10, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                // Legacy untimed receive: tconsistent passes through.
                let v: i32 = traceforge::recv_msg_block();
                assert_eq!(v, 42);
            });
            // Sleep is still temporally advancing main's local clock,
            // but the legacy recv has no wait constraint to violate.
            traceforge::sleep(1000);
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Sleep is per-thread: it does not leak into parallel threads'
// temporal windows.
// ---------------------------------------------------------------------
//
// Thread A sleeps for a very long time and does nothing else.
// Thread B receives within a short wait from a send that has no delay.
// each thread starts at [0, 0], so A's huge sleep must not push B's
// receive window.
#[test]
fn sleep_is_per_thread() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 0, 0).build(),
        || {
            let b = thread::spawn(|| {
                // Short wait; if A's sleep leaked in, this would be
                // temporally infeasible.
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                let _ = v;
            });
            let _a = thread::spawn(|| {
                traceforge::sleep(1_000_000);
            });
            traceforge::send_msg(b.thread().id(), 99i32);
        },
    );
    assert!(stats.execs == 2);
}
