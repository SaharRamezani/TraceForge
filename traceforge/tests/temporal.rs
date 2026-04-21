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
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Legacy run (no `with_temporal`) is unchanged by the new code paths.
// ---------------------------------------------------------------------
//
// This is a regression test:
// when `config.temporal` is `None`. A single send/recv should produce
// exactly one complete execution, just like it did before.
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
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Storage delay `sd` bounds how long a delivered message is pickupable.
// ---------------------------------------------------------------------
//
// Producer: send (no sleep)  -> send_iv = [0, 0]
// Consumer: sleep(10); recv(wait=100)
//   pred_iv at recv = [10, 10], hi_cap = 10+100 = 110
// With L=0, U=0, sd=5:
//   delivery window = [0+0, 0+0+5] = [0, 5]
//   rf-from-send: lo = max(10, 0) = 10, hi = min(110, 5) = 5  -> empty
//   timeout:      [10+100, 10+100] = [110, 110]               -> feasible
// The message was discarded from the destination buffer before the
// consumer woke up, so only the timeout branch survives.
#[test]
fn storage_delay_prunes_late_pickup() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 0, 5).build(),
        || {
            let consumer = thread::spawn(|| {
                traceforge::sleep(10);
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(100));
                assert!(v.is_none());
            });
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Lower bound `L` prunes receives that would fire before a message can
// physically transit the network.
// ---------------------------------------------------------------------
//
// With L=100, U=100, sd=0:
//   send_iv = [0, 0], delivery window = [0+100, 0+100+0] = [100, 100]
//   recv pred = [0, 0], wait = 10, hi_cap = 10
//   rf-from-send: lo = max(0, 100) = 100, hi = min(10, 100) = 10 -> empty
//   timeout: [10, 10] -> feasible
// The send exists but can't possibly have arrived in time.
#[test]
fn lower_bound_prunes_early_pickup() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(100, 100, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                assert!(v.is_none());
            });
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Forward rf-filtering: with multiple candidate sends for a single
// receive, the temporal filter drops only the infeasible ones.
// ---------------------------------------------------------------------
//
// Producer issues two sends separated by a sleep:
//   send1_iv = [0, 0]     (before the sleep)
//   send2_iv = [50, 50]   (after sleep(50))
// Consumer: recv(wait=10), pred = [0, 0], hi_cap = 10
// With L=0, U=0, sd=0:
//   rf=send1: lo = max(0, 0)  = 0,  hi = min(10, 0)  = 0  -> [0, 0] feasible
//   rf=send2: lo = max(0, 50) = 50, hi = min(10, 50) = 10 -> empty, pruned
//   timeout: [10, 10]                                      -> feasible
// Without the temporal filter this would be 3 executions; with it, 2.
#[test]
fn tight_wait_prunes_far_send_same_producer() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 0, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
            });
            let cid = consumer.thread().id();
            traceforge::send_msg(cid, 1i32); // send1 @ t = 0
            traceforge::sleep(50);
            traceforge::send_msg(cid, 2i32); // send2 @ t = 50
        },
    );
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Time propagates across threads through a timed receive: the delivery
// window of the upstream send becomes the receive's pred_iv, which in
// turn constrains anything po-after it on the same thread.
// ---------------------------------------------------------------------
//
// Main:   sleep(10); send_to_B                -> B_in_iv  = [10, 10]
// B:      recv(wait=100) then send_to_C
//   rf=main: pred[0,0], hi_cap=100,
//            lo = max(0, 10) = 10, hi = min(100, 10) = 10  -> [10, 10]
//   B's send_to_C inherits [10, 10] as pred_iv (pass-through for Send).
// C:      recv(wait=5)
//   rf=B's send: lo = max(0, 10) = 10, hi = min(5, 10) = 5 -> empty
//   timeout:     [5, 5]                                    -> feasible
// B can also time out (rf=⊥), in which case it does not send to C, so
// C's only option is rf=⊥. Surviving executions:
//   (B rf=main, C rf=⊥) and (B rf=⊥, C rf=⊥)  -> 2
// Without the temporal filter, (B rf=main, C rf=B) would also survive.
#[test]
fn multi_hop_temporal_propagation() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 0, 0).build(),
        || {
            let c = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                assert!(v.is_none());
            });
            let cid = c.thread().id();
            let b = thread::spawn(move || {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(100));
                if let Some(v) = v {
                    traceforge::send_msg(cid, v);
                }
            });
            let bid = b.thread().id();
            traceforge::sleep(10);
            traceforge::send_msg(bid, 42i32);
        },
    );
    assert_eq!(stats.execs, 2);
}

// ---------------------------------------------------------------------
// Two sequential timed receives on one thread: pred_iv propagates
// across receives in program order, so the second receive's feasible
// rf set is gated by which branch the first receive took.
// ---------------------------------------------------------------------
//
// P1:       send(1)                 -> send1_iv = [0, 0]
// P2:       sleep(100); send(2)     -> send2_iv = [100, 100]
// Consumer: recv1(wait=50); recv2(wait=60)
//
// With L=0, U=0, sd=0:
//   recv1 (pred_iv=[0, 0], hi_cap=50):
//     rf=send1: lo=0,   hi=min(50, 0)  =0   -> [0, 0]   feasible
//                                              (next pred_iv = [0, 0])
//     rf=send2: lo=100, hi=min(50, 100)=50  -> empty    pruned
//     rf=⊥:    [50, 50]                      feasible
//                                              (next pred_iv = [50, 50])
//
//   recv2 after recv1=send1 (pred_iv=[0, 0], hi_cap=60):
//     rf=send2: lo=100, hi=min(60, 100)=60  -> empty    pruned
//     rf=⊥:    [60, 60]                      feasible
//   recv2 after recv1=⊥ (pred_iv=[50, 50], hi_cap=110):
//     rf=send1: lo=50,  hi=min(110, 0)  =0  -> empty    pruned (sd=0)
//     rf=send2: lo=100, hi=min(110, 100)=100 -> [100, 100] feasible
//     rf=⊥:    [110, 110]                    feasible
//
// Of the seven baseline (rf1, rf2) combinations, four are pruned by
// the temporal filter. Surviving executions:
//   (recv1=send1, recv2=⊥), (recv1=⊥, recv2=send2), (recv1=⊥, recv2=⊥)
#[test]
fn pred_iv_propagates_across_sequential_recvs() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 0, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(50));
                let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(60));
            });
            let cid = consumer.thread().id();
            let _p1 = thread::spawn(move || {
                traceforge::send_msg(cid, 1i32);
            });
            let _p2 = thread::spawn(move || {
                traceforge::sleep(100);
                traceforge::send_msg(cid, 2i32);
            });
        },
    );
    assert_eq!(stats.execs, 3);
}
