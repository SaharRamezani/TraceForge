//! Integration tests for the exact timed-feasibility engine (the only
//! timed engine on this branch.
//!
//! The mini-model below is the minimal shared-ancestor pattern from
//! the swim benchmark: a prober runs R probe periods against a
//! reactive target; each period's timers chain to the period's start,
//! whose time interval widens by 2(U+sd) per refuted round. The old
//! interval walker judged reads by interval endpoints and admitted a
//! round-1 "late refutation" read pairing endpoints from two different
//! pasts: a read no single timeline realizes. The exact engine prunes
//! it; the pinned execution counts below are the hand-checked exact
//! state-space sizes and act as regressions in both directions (a
//! count above the pin = relaxation crept back in; below = something
//! over-prunes).

use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum M {
    Ping(u32),
    Ack,
    Suspect(u32),
    Alive,
    Done,
}

fn ack_tag(r: u32) -> u32 {
    10 + r
}
fn alive_tag(r: u32) -> u32 {
    20 + r
}

/// Reactive target: acks every ping, answers every suspicion, exits on
/// Done. Blocking receives only (no timers).
fn target(main_tid: ThreadId) {
    let a: ThreadId = traceforge::recv_tagged_msg_block(move |s, _| s == main_tid);
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, M>(move |s, _| s == a) {
            M::Ping(r) => traceforge::send_tagged_msg(a, ack_tag(r), M::Ack),
            M::Suspect(r) => traceforge::send_tagged_msg(a, alive_tag(r), M::Alive),
            M::Done => return,
            m => panic!("target: unexpected {m:?}"),
        }
    }
}

/// Prober driving `rounds` probe periods with probe window 1 and
/// suspicion timeout `ws`. On an unrefuted suspicion it performs the
/// timeout-validation read (the pinch read whose feasibility the exact
/// engine decides), then either asserts false (`fire`) or branches on
/// a nondet (an execution-count multiplier).
fn prober(b: ThreadId, main_tid: ThreadId, rounds: u32, ws: u64, fire: bool) {
    let _: ThreadId = traceforge::recv_tagged_msg_block(move |s, _| s == main_tid);
    for r in 0..rounds {
        traceforge::send_msg(b, M::Ping(r));
        let acked = traceforge::recv_tagged_msg_timed::<_, M>(
            move |s, t| s == b && t == Some(ack_tag(r)),
            WaitTime::Finite(1),
        )
        .is_some();
        if acked {
            continue;
        }
        traceforge::send_msg(b, M::Suspect(r));
        // Timeout-validation read: assume the ack really was late.
        let _ = traceforge::recv_tagged_msg_block_timed::<_, M>(move |s, t| {
            s == b && t == Some(ack_tag(r))
        });
        let refuted = traceforge::recv_tagged_msg_timed::<_, M>(
            move |s, t| s == b && t == Some(alive_tag(r)),
            WaitTime::Finite(ws),
        )
        .is_some();
        if !refuted {
            // The pinch read: feasible iff the refutation can genuinely
            // arrive after the suspicion deadline.
            let _ = traceforge::recv_tagged_msg_block_timed::<_, M>(move |s, t| {
                s == b && t == Some(alive_tag(r))
            });
            if fire {
                traceforge::assert(false);
            } else {
                let _ = traceforge::nondet();
            }
        }
    }
    traceforge::send_msg(b, M::Done);
}

fn run_mini_swim(rounds: u32, ws: u64, fire: bool) -> Stats {
    traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        move || {
            let main_tid = thread::current().id();
            let t = thread::spawn(move || target(main_tid));
            let t_id = t.thread().id();
            let p = thread::spawn(move || prober(t_id, main_tid, rounds, ws, fire));
            let p_id = p.thread().id();
            traceforge::send_msg(t_id, p_id);
            traceforge::send_msg(p_id, t_id);
            let _ = t.join();
            let _ = p.join();
        },
    )
}

// ---------------------------------------------------------------------
// Chain scenarios: hand-computable state spaces (the shapes where the
// old walker was already exact, so these counts are semantic truths).
// ---------------------------------------------------------------------

#[test]
fn chain_scenario_counts() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1000, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(100));
            });
            traceforge::sleep(10);
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 2);
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
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
// Pinned exact state spaces for the mini-swim shared-ancestor model.
// At Ws=4 (true bound is 2) every dead branch is timeline-impossible:
// the exact engine prunes it into a blocked execution instead of
// exploring it (the old walker explored it at R=2 and its nondet split
// inflated execs; a count above the pin means relaxation returned).
// ---------------------------------------------------------------------

// Blocked counts include the GC refusal classes: a
// waiting timed receive may also NEVER read (all matching messages
// die before or while it waits); each such world is one more blocked
// execution. Execs are unchanged by refusals.
#[test]
fn mini_swim_single_round_counts() {
    let stats = run_mini_swim(1, 4, false);
    assert_eq!((stats.execs, stats.block), (2, 2));
}

#[test]
fn mini_swim_two_rounds_shared_ancestor_pruned() {
    let stats = run_mini_swim(2, 4, false);
    assert_eq!((stats.execs, stats.block), (4, 6));
}

// ---------------------------------------------------------------------
// The impossible dead branch never reaches its assert under the exact
// engine (with the walker it did, and only certification stopped the
// false report).
// ---------------------------------------------------------------------

#[test]
fn exact_never_reaches_spurious_assert() {
    let stats = run_mini_swim(2, 4, true);
    assert_eq!((stats.execs, stats.block), (4, 6));
}

// ---------------------------------------------------------------------
// Genuine counterexamples must keep firing: with Ws=2 the refutation
// can legitimately arrive after the suspicion deadline (2U + 2sd = 2),
// the pinch read is realizable, and the assert is a real violation.
// ---------------------------------------------------------------------

#[test]
#[should_panic]
fn genuine_fire_still_panics() {
    let _ = run_mini_swim(1, 2, true);
}

// ---------------------------------------------------------------------
// GC semantics (decided 2026-08-08): a time-dead front message no
// longer seals its channel. The same shape that once livelocked the
// scheduler (item 2) and later terminated blocked (seal semantics) now
// SKIPS the dead front and reads the live message behind it. Both
// branches complete; nothing blocks; nothing spins.
// ---------------------------------------------------------------------

#[test]
fn expired_fifo_front_skipped_under_gc() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                // Timeout branch leaves the front message unread and
                // expired (readable until 1, clock now 10).
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                // Untagged blocking read: the dead m1 is skipped
                // (never readable again), m2 is offered instead.
                let _: u32 = traceforge::recv_msg_block_timed();
            });
            let r = receiver.thread().id();
            traceforge::send_msg(r, 1u32);
            traceforge::sleep(100);
            traceforge::send_msg(r, 2u32);
        },
    );
    // Branch A: first recv reads m1 in time, second reads m2.
    // Branch B: first recv times out, second skips dead m1, reads m2.
    assert_eq!((stats.execs, stats.block), (2, 0));
}

// ---------------------------------------------------------------------
// GC must not weaken FIFO for LIVE fronts: when the older message is
// still readable, it remains the only offer; the newer one is not an
// alternative branch.
// ---------------------------------------------------------------------

#[test]
fn live_front_still_mandatory_under_gc() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let v: u32 = traceforge::recv_msg_block_timed();
                traceforge::assert(v == 1);
            });
            let r = receiver.thread().id();
            traceforge::send_msg(r, 1u32);
            traceforge::send_msg(r, 2u32);
        },
    );
    // Exactly one execution: m1 is alive, minimal, and mandatory.
    assert_eq!((stats.execs, stats.block), (1, 0));
}

// ---------------------------------------------------------------------
// A skipped message is EVICTED (the overtaking rule): once a later
// message was read past it, the corpse can never be read afterwards.
// A third read finds nothing in both branches and blocks.
// ---------------------------------------------------------------------

#[test]
fn skipped_message_is_evicted() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                let _: u32 = traceforge::recv_msg_block_timed();
                // Nothing left: m1 is either consumed (branch A) or
                // skipped-and-evicted (branch B). Must block, not
                // resurrect the corpse.
                let _: u32 = traceforge::recv_msg_block_timed();
            });
            let r = receiver.thread().id();
            traceforge::send_msg(r, 1u32);
            traceforge::sleep(100);
            traceforge::send_msg(r, 2u32);
        },
    );
    assert_eq!((stats.execs, stats.block), (0, 2));
}

// ---------------------------------------------------------------------
// FIFO vacuity pin (2026-08-28 decision): b is sent first on the same
// channel but its transit [5,5] would have to overtake s1's [0,0].
// FIFO delivery couples arrivals (a_b <= a_s1), so NO timeline
// satisfies this program at all: it has no behaviors, and neither
// branch counts as anything. (The pre-FIFO lateness-side eviction
// this test used to pin, hand-derived (1, 1), required overtaking;
// overtaking is now reserved for the NoOrder/Bag model.)
// ---------------------------------------------------------------------

#[test]
fn late_skipped_message_evicted() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(1));
                let _: u32 = traceforge::recv_msg_block_timed();
            });
            let r = receiver.thread().id();
            traceforge::send_msg_timed(r, 100u32, 5, 5); // b, sent first
            traceforge::send_msg_timed(r, 1u32, 0, 0); // s1: would overtake
        },
    );
    assert_eq!((stats.execs, stats.block), (0, 0));
}

// ---------------------------------------------------------------------
// Context-coupled eligibility is complete (audit adjudication): R's
// offer depends on whether E's read constrained A's clock, across
// threads and through revisits. Original (2026-08-09) spec: exactly 8
// complete executions: (b,TO,x2), (c,e,x2), (c,TO,x2), (sig,e,x2).
// Re-derived 2026-08-28 under dead-front unsealing: (sig,TO,x2) is
// ALSO operationally real (with E=timeout, t_A is unconstrained, and
// in the t_A <= 5 timelines b arrives at R and dies, sd = 0, before
// R's wait begins at 6; the 2026-08-08 GC decision says a time-dead
// front does not seal its channel, so R reads sig). The old count
// relied on the possibly-readable front b monopolizing the offer;
// with the joint dodge probe those two worlds are found: 10 total.
// ---------------------------------------------------------------------

#[test]
fn context_coupled_eligibility_complete() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let main_tid = thread::current().id();
            let q = thread::spawn(|| {
                let _: Option<u32> =
                    traceforge::recv_tagged_msg_timed(|_, t| t == Some(105), WaitTime::Finite(5));
            });
            let q_id = q.thread().id();
            let e_thr = thread::spawn(move || {
                let qid: traceforge::thread::ThreadId =
                    traceforge::recv_tagged_msg_block(move |s, t| {
                        s == main_tid && t == Some(109)
                    });
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    |_, t| t == Some(104),
                    WaitTime::Finite(3),
                );
                traceforge::send_tagged_msg(qid, 105, 55u32);
            });
            let e_id = e_thr.thread().id();
            let r_thr = thread::spawn(|| {
                traceforge::sleep(6);
                let _: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| {
                    t == Some(101) || t == Some(102) || t == Some(103)
                });
            });
            let r_id = r_thr.thread().id();
            let a_thr = thread::spawn(move || {
                let (rid, eid): (traceforge::thread::ThreadId, traceforge::thread::ThreadId) =
                    traceforge::recv_tagged_msg_block(move |s, t| {
                        s == main_tid && t == Some(109)
                    });
                let _: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(106));
                traceforge::send_tagged_msg_timed(rid, 101, 10u32, 0, 0); // b
                traceforge::send_tagged_msg_timed(rid, 102, 20u32, 25, 25); // sig
                traceforge::send_tagged_msg_timed(eid, 104, 30u32, 0, 0); // e
            });
            let a_id = a_thr.thread().id();
            let d_thr = thread::spawn(move || {
                let aid: traceforge::thread::ThreadId =
                    traceforge::recv_tagged_msg_block(move |s, t| {
                        s == main_tid && t == Some(109)
                    });
                traceforge::send_tagged_msg_timed(aid, 106, 1u32, 0, 10); // m: t_A in [0,10]
            });
            let d_id = d_thr.thread().id();
            let c_thr = thread::spawn(move || {
                let rid: traceforge::thread::ThreadId =
                    traceforge::recv_tagged_msg_block(move |s, t| {
                        s == main_tid && t == Some(109)
                    });
                traceforge::sleep(7);
                traceforge::send_tagged_msg_timed(rid, 103, 40u32, 0, 0); // c, alive at 7
            });
            let c_id = c_thr.thread().id();
            traceforge::send_tagged_msg(e_id, 109, q_id);
            traceforge::send_tagged_msg(a_id, 109, (r_id, e_id));
            traceforge::send_tagged_msg(d_id, 109, a_id);
            traceforge::send_tagged_msg(c_id, 109, r_id);
            let _ = q.join();
            let _ = e_thr.join();
            let _ = r_thr.join();
            let _ = a_thr.join();
            let _ = d_thr.join();
            let _ = c_thr.join();
        },
    );
    assert_eq!((stats.execs, stats.block), (10, 0));
}

#[test]
fn first_skipper_only_carries_dodge() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 4).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(1));
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(4));
            });
            let r = receiver.thread().id();
            traceforge::send_msg_timed(r, 1u32, 2, 3); // b
            traceforge::send_msg_timed(r, 2u32, 0, 0); // s1
            traceforge::send_msg_timed(r, 3u32, 3, 3); // s2
        },
    );
    // FIFO vacuity (2026-08-28): b [2,3] sent before s1 [0,0] on one
    // channel would have to be overtaken; a_b <= a_s1 <= a_s2 admits
    // no timeline, so the program has no behaviors. (Pre-FIFO counts:
    // (s1,s2), (s1,timeout), (timeout,b), (timeout,timeout) = (4, 0).)
    assert_eq!((stats.execs, stats.block), (0, 0));
}

// ---------------------------------------------------------------------
// Late-arrival skip: the older message cannot arrive within the
// receive's wait window (transit override [50,50]), the newer one can.
// The receive skips the in-flight older message and reads the newer,
// or times out (always explorable).
// ---------------------------------------------------------------------

#[test]
fn in_flight_front_skipped_within_cap() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
            });
            let r = receiver.thread().id();
            traceforge::send_msg_timed(r, 1u32, 50, 50); // arrives at 50
            traceforge::send_msg_timed(r, 2u32, 0, 0); // arrives at 0
        },
    );
    // FIFO vacuity (2026-08-28): m1 [50,50] sent before m2 [0,0] on
    // one channel admits no coupled arrival order: no behaviors.
    // (Pre-FIFO: read m2 skipping the in-flight m1, or time out = 2.)
    assert_eq!((stats.execs, stats.block), (0, 0));
}

// ---------------------------------------------------------------------
// Mixed-mode contract at the unblock gate: a Block of an UNTIMED
// (wait = None) receive under a timed config must not be gated by any
// readability window. Audit regression: this execution used to end
// blocked (execs = 0) because the wake-up check imposed [s+L, s+U+sd]
// on a receive that contributes no timing constraints.
// ---------------------------------------------------------------------

#[test]
fn untimed_blocking_recv_unaffected_by_timed_config() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        || {
            let main_tid = thread::current().id();
            let r = thread::spawn(move || {
                traceforge::sleep(100);
                traceforge::send_msg(main_tid, 1u32);
                // Untimed blocking read of a message sent around t=0:
                // readable window long expired, must be read anyway
                // (untimed receives carry no window).
                let _: u32 = traceforge::recv_msg_block();
            });
            let _: u32 = traceforge::recv_msg_block();
            traceforge::send_msg(r.thread().id(), 2u32);
        },
    );
    assert_eq!(stats.execs, 1);
    assert_eq!(stats.block, 0);
}

// ---------------------------------------------------------------------
// Inbox x backward-revisit x exact-engine coexistence: a model whose
// exploration is DRIVEN by inbox backward revisits (a late send
// re-pairing an already executed inbox) must keep its pinned state
// space while a plain timed receive downstream goes through the exact
// oracle over a base containing committed waited-inbox reads (a case
// disjunction). Termination itself is part of the assertion.
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Exact inbox case split: a committed WAITED inbox read happens at
// max(t0, completing arrival), a point, not a window. The pinch read
// downstream (Finite(0) = exactly at the inbox's time) can only see a
// message whose readability window contains that point. Under the old
// conjunctive encoding the inbox time floated in [arrival, arrival+sd]
// and the pinch read of a message at t=3 was (spuriously) feasible;
// exactly the eats-at-3 impostor. Counts are hand-derived.
// ---------------------------------------------------------------------

#[test]
fn committed_inbox_read_time_is_at_the_arrival() {
    // L = 0, U = 0, sd = 3: arrivals are exact, storage lingers 3.
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 3).build(),
        || {
            let collector = thread::spawn(|| {
                // Member arrives at t = 1: the waited read happens AT 1.
                let v = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    1,
                    WaitTime::Finite(10),
                );
                if v.len() == 1 {
                    // Pinch read: tag-9 message readable only in [3, 6];
                    // a Finite(0) read happens exactly at the inbox time
                    // (1), so it MUST time out. The conjunctive
                    // relaxation would let the inbox time float to
                    // [1, 4] and read it at 3 - the impostor timeline.
                    let got: Option<u32> = traceforge::recv_tagged_msg_timed(
                        |_, t| t == Some(9),
                        WaitTime::Finite(0),
                    );
                    traceforge::assert(got.is_none());
                }
            });
            let c = collector.thread().id();
            let s = thread::spawn(move || {
                traceforge::sleep(1);
                traceforge::send_tagged_msg(c, 1, 7u32);
            });
            traceforge::sleep(3);
            traceforge::send_tagged_msg(c, 9, 9u32);
            let _ = s.join();
            let _ = collector.join();
        },
    );
    // Subset branch (read at 1, pinch times out) + inbox-timeout
    // branch (inbox at 10, guard skips the pinch): 2 executions; a
    // third (the impostor) must never appear, and the assert must
    // never fire.
    assert_eq!((stats.execs, stats.block), (2, 0));
}

#[test]
fn min2_completing_arrival_pins_read_time() {
    // Members arrive at 1 and 5 (sd = 10 keeps the first alive): the
    // min = 2 waited read happens exactly at 5, the COMPLETING
    // arrival, with no storage slack of its own. The pinch message
    // (window [7, 17]) is out of reach at 5; conjunctively the read
    // could float to 7 and see it.
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 10).build(),
        || {
            let collector = thread::spawn(|| {
                let v = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Finite(20),
                );
                if v.len() == 2 {
                    let got: Option<u32> = traceforge::recv_tagged_msg_timed(
                        |_, t| t == Some(9),
                        WaitTime::Finite(0),
                    );
                    traceforge::assert(got.is_none());
                }
            });
            let c = collector.thread().id();
            let s1 = thread::spawn(move || {
                traceforge::sleep(1);
                traceforge::send_tagged_msg(c, 1, 1u32);
            });
            let s2 = thread::spawn(move || {
                traceforge::sleep(5);
                traceforge::send_tagged_msg(c, 1, 2u32);
            });
            traceforge::sleep(7);
            traceforge::send_tagged_msg(c, 9, 9u32);
            let _ = s1.join();
            let _ = s2.join();
            let _ = collector.join();
        },
    );
    // Subset branch (read at 5, pinch times out) + inbox-timeout
    // branch: 2 executions, no impostor, no fire.
    assert_eq!((stats.execs, stats.block), (2, 0));
}

#[test]
fn min2_genuine_completing_read_kept() {
    // Completeness guard for the case split: move the pinch message
    // to exactly the completing arrival (t = 5, window [5, 15]) and
    // the Some branch is GENUINE: it must stay explored.
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 10).build(),
        || {
            let collector = thread::spawn(|| {
                let v = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Finite(20),
                );
                if v.len() == 2 {
                    let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                        |_, t| t == Some(9),
                        WaitTime::Finite(0),
                    );
                }
            });
            let c = collector.thread().id();
            let s1 = thread::spawn(move || {
                traceforge::sleep(1);
                traceforge::send_tagged_msg(c, 1, 1u32);
            });
            let s2 = thread::spawn(move || {
                traceforge::sleep(5);
                traceforge::send_tagged_msg(c, 1, 2u32);
            });
            traceforge::sleep(5);
            traceforge::send_tagged_msg(c, 9, 9u32);
            let _ = s1.join();
            let _ = s2.join();
            let _ = collector.join();
        },
    );
    // Subset branch: pinch Some (genuine) + pinch timeout = 2;
    // inbox-timeout branch: 1. Total 3.
    assert_eq!((stats.execs, stats.block), (3, 0));
}

// ---------------------------------------------------------------------
// min >= 2 livelock regression: two individually feasible members with
// DISJOINT lifetimes (alive [0,0] and [100,100] at sd = 0) can never
// form one batch. The old per-send wake-up count saw 2 >= 2 and woke
// the block; visit_inbox_rfs found no feasible subset, re-blocked, and
// the scheduler spun forever. The joint-subset check keeps it asleep:
// the branch terminates as an ordinary blocked execution. Returning at
// all is the regression assertion.
// ---------------------------------------------------------------------

#[test]
fn pairwise_incompatible_min2_inbox_terminates_blocked() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let collector = thread::spawn(|| {
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Infinite,
                );
            });
            let c = collector.thread().id();
            let s = thread::spawn(move || {
                traceforge::sleep(100);
                traceforge::send_tagged_msg(c, 1, 2u32);
            });
            traceforge::send_tagged_msg(c, 1, 1u32);
            let _ = s.join();
        },
    );
    // Exact pin: this scenario ends in the empty-combinations Block
    // (no jointly feasible subset), NOT in a GC refusal branch; the
    // refusal machinery must not double-count it.
    assert_eq!((stats.execs, stats.block), (0, 1));
}

// ---------------------------------------------------------------------
// Genuine inbox counterexamples must keep firing, now as CERTIFIED
// reports: members at 1 and 2 with sd = 5 are jointly readable at 2,
// the subset branch is realizable, and the assert is a real violation
// (had certification wrongly judged the inbox base infeasible, the
// report would be suppressed and this test would fail to panic).
// ---------------------------------------------------------------------

#[test]
#[should_panic]
fn genuine_inbox_fire_still_panics() {
    let _ = traceforge::verify(
        Config::builder().with_timed(0, 0, 5).build(),
        || {
            let collector = thread::spawn(|| {
                let v = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Finite(10),
                );
                if v.len() == 2 {
                    traceforge::assert(false);
                }
            });
            let c = collector.thread().id();
            let s1 = thread::spawn(move || {
                traceforge::sleep(1);
                traceforge::send_tagged_msg(c, 1, 1u32);
            });
            let s2 = thread::spawn(move || {
                traceforge::sleep(2);
                traceforge::send_tagged_msg(c, 1, 2u32);
            });
            let _ = s1.join();
            let _ = s2.join();
            let _ = collector.join();
        },
    );
}

#[test]
fn inbox_backward_revisits_coexist_with_exact() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 1, 0).build(),
        || {
            let main_tid = thread::current().id();
            let collector = thread::spawn(move || {
                // Waited inbox (k = 1): executes early; the late
                // sender's message can only join via inbox backward
                // revisits.
                let v = traceforge::inbox_timed(1, WaitTime::Finite(4));
                assert!(v.len() <= 1);
                // A plain timed receive downstream of the inbox,
                // sharing the senders' ancestry: exercises the exact
                // oracle over a base system that CONTAINS committed
                // waited-inbox reads.
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    move |s, _| s == main_tid,
                    WaitTime::Finite(3),
                );
            });
            let c = collector.thread().id();
            traceforge::send_msg(c, 1u32); // early sender
            traceforge::sleep(2);
            traceforge::send_msg(c, 2u32); // late sender: backward revisits
            traceforge::send_msg(c, 3u32); // the plain recv's message
        },
    );
    assert_eq!((stats.execs, stats.block), (3, 0));
}

// ---------------------------------------------------------------------
// Spawn-order invariance canary (lost-executions adjudication,
// 2026-08-08): timing-coupled eligibility once interacted with the
// revisit machinery so that a fork existing only in a sibling branch
// was lost, and the loss DEPENDED ON SPAWN ORDER (5 vs 6 executions
// for the same program). Spawn order changes stamps, never timing
// semantics, so unequal counts here prove incompleteness without any
// hand derivation. Both orders must explore all 6 reachable graphs.
// ---------------------------------------------------------------------

fn context_coupling_model(r_before_c: bool) -> Stats {
    traceforge::verify(Config::builder().with_timed(0, 10, 0).build(), move || {
        let main_tid = thread::current().id();
        let q = thread::spawn(|| {
            let _: Option<u32> =
                traceforge::recv_tagged_msg_timed(|_, t| t == Some(50), WaitTime::Finite(5));
        });
        let q_id = q.thread().id();
        let e_thr = thread::spawn(move || {
            let qid: ThreadId =
                traceforge::recv_tagged_msg_block(move |s, t| s == main_tid && t == Some(99));
            let _: Option<u32> =
                traceforge::recv_tagged_msg_timed(|_, t| t == Some(40), WaitTime::Finite(3));
            traceforge::send_tagged_msg(qid, 50, 5u32); // s_new
        });
        let e_id = e_thr.thread().id();
        let a_thr = thread::spawn(move || {
            let (rid, eid): (ThreadId, ThreadId) =
                traceforge::recv_tagged_msg_block(move |s, t| s == main_tid && t == Some(99));
            let _: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(30)); // m
            traceforge::send_tagged_msg_timed(rid, 20, 1u32, 0, 0); // b at t_A
            traceforge::send_tagged_msg_timed(eid, 40, 2u32, 0, 0); // e at t_A
        });
        let a_id = a_thr.thread().id();
        let spawn_r = || {
            thread::spawn(|| {
                traceforge::sleep(6);
                let _: u32 =
                    traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(20) || t == Some(21));
            })
        };
        let spawn_c = |main_tid: ThreadId| {
            thread::spawn(move || {
                let rid: ThreadId =
                    traceforge::recv_tagged_msg_block(move |s, t| s == main_tid && t == Some(99));
                traceforge::send_tagged_msg_timed(rid, 21, 3u32, 0, 10); // c
            })
        };
        let (r_id, c_thr) = if r_before_c {
            let r = spawn_r();
            let c = spawn_c(main_tid);
            (r.thread().id(), c)
        } else {
            let c = spawn_c(main_tid);
            let r = spawn_r();
            (r.thread().id(), c)
        };
        traceforge::send_tagged_msg(e_id, 99, q_id);
        traceforge::send_tagged_msg(a_id, 99, (r_id, e_id));
        traceforge::send_tagged_msg(c_thr.thread().id(), 99, r_id);
        traceforge::send_tagged_msg_timed(a_id, 30, 9u32, 0, 10); // m: t_A in [0,10]
    })
}

#[test]
fn context_coupling_spawn_order_invariant() {
    let a = context_coupling_model(true);
    let b = context_coupling_model(false);
    assert_eq!((a.execs, a.block), (6, 4));
    assert_eq!((b.execs, b.block), (6, 4));
}

// ---------------------------------------------------------------------
// Cross-predicate eviction (audit10, was MAJOR): a receive that reads
// past b evicts b even when the message it read does not match a LATER
// receive's predicate. Tags select messages; they do not shield
// corpses. r1 (tags 5|6) can only read s2 (b arrives at 4, cap 1);
// that skip evicts b, so r2 (tag 5 only) must never read it.
// Branches: (s2, timeout), (timeout, b), (timeout, timeout).
// ---------------------------------------------------------------------

#[test]
fn cross_predicate_skip_still_evicts() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    |_, t| t == Some(5) || t == Some(6),
                    WaitTime::Finite(1),
                );
                let _: Option<u32> =
                    traceforge::recv_tagged_msg_timed(|_, t| t == Some(5), WaitTime::Finite(10));
            });
            let r = receiver.thread().id();
            traceforge::send_tagged_msg_timed(r, 5, 50u32, 4, 4); // b
            traceforge::send_tagged_msg_timed(r, 6, 60u32, 0, 0); // s2
        },
    );
    // FIFO vacuity (2026-08-28): tags select messages but the channel
    // is one FIFO stream; b [4,4] before s2 [0,0] admits no coupled
    // arrival order, so the program has no behaviors. (Pre-FIFO:
    // (s2, timeout), (timeout, b), (timeout, timeout) = (3, 0).)
    assert_eq!((stats.execs, stats.block), (0, 0));
}

// =====================================================================
// Inbox exclusion + order-invariance regressions (audit 2026-08-09,
// fixed 2026-08-10). All tests below are live pins.
// =====================================================================

// Waited-inbox excluded members must dodge the completion count
// (arrive at/after the read, be dead before it, or co-arrive with a
// member at the completing tick): arrivals 0,3,5 with min=2 complete
// at 3, so only {m0,m1} is real. Before the 2026-08-09 exclusion fix
// this explored impossible batches (4,0) and certified their asserts.
// The {m0,m2} batch enters via a backward revisit whose cut view hides
// m1; the completion-time feasibility gate detects it and (2026-08-28
// rule) counts the timeline-impossible branch as nothing at all.
#[test]
fn inbox_excluded_members_constrain_completion() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 10).build(),
        || {
            let c = thread::spawn(|| {
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Finite(20),
                );
            });
            let c_id = c.thread().id();
            let s1 = thread::spawn(move || {
                traceforge::sleep(3);
                traceforge::send_tagged_msg(c_id, 1, 2u32);
            });
            let s2 = thread::spawn(move || {
                traceforge::sleep(5);
                traceforge::send_tagged_msg(c_id, 1, 3u32);
            });
            traceforge::send_tagged_msg(c_id, 1, 1u32);
            let _ = s1.join();
            let _ = s2.join();
            let _ = c.join();
        },
    );
    assert_eq!((stats.execs, stats.block), (2, 0));
}

// A timing-coupled Finite receive executing after a concurrent BLOCKING
// receive used to lose the (E=e, R-blocked-forever) class under spawn
// order A,R,E (fixed 2026-08-09 by the GC refusal branch: a waiting
// receive with offers also forks a never-reads sibling). Both orders
// now explore the same three classes: exec (R=b, E=timeout); blocked
// (R-refuses, E=e) and (R-refuses, E=timeout).
#[test]
fn spawn_order_blocked_class_invariant() {
    fn run(spawn_order: [usize; 3]) -> Stats {
        traceforge::verify(
            Config::builder().with_timed(0, 10, 0).build(),
            move || {
                let main = thread::current().id();
                let mut ids: [Option<ThreadId>; 3] = [None; 3];
                let mut handles = Vec::new();
                for &role in spawn_order.iter() {
                    let h = match role {
                        0 => thread::spawn(move || {
                            let (rid, eid): (ThreadId, ThreadId) =
                                traceforge::recv_tagged_msg_block(move |s, t| {
                                    s == main && t == Some(99)
                                });
                            let _: u32 =
                                traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(30));
                            traceforge::send_tagged_msg_timed(rid, 20, 1u32, 0, 0);
                            traceforge::send_tagged_msg_timed(eid, 40, 2u32, 0, 0);
                        }),
                        1 => thread::spawn(|| {
                            let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                                |_, t| t == Some(40),
                                WaitTime::Finite(3),
                            );
                        }),
                        _ => thread::spawn(|| {
                            traceforge::sleep(6);
                            let _: u32 =
                                traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(20));
                        }),
                    };
                    ids[role] = Some(h.thread().id());
                    handles.push(h);
                }
                traceforge::send_tagged_msg(
                    ids[0].unwrap(),
                    99,
                    (ids[2].unwrap(), ids[1].unwrap()),
                );
                traceforge::send_tagged_msg_timed(ids[0].unwrap(), 30, 9u32, 0, 10);
            },
        )
    }
    let a = run([0, 1, 2]);
    let b = run([0, 2, 1]);
    assert_eq!((a.execs, a.block), (b.execs, b.block));
    assert_eq!((a.execs, a.block), (1, 2));
}

// The certified-false-counterexample hole: an assert firing
// on the impossible {m0,m2} batch must be SUPPRESSED, not certified
// (the porf-prefix witness omitted m1's thread; the gate now judges
// the full committed graph). No violation is reported; the impossible
// branch counts as nothing at all (2026-08-28 rule).
#[test]
fn inbox_false_counterexample_suppressed() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 10).build(),
        || {
            let c = thread::spawn(|| {
                let v: Vec<u32> = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Finite(20),
                )
                .into_iter()
                .filter_map(|m| m.and_then(|x| x.as_any().downcast_ref::<u32>().copied()))
                .collect();
                traceforge::assert(!(v == vec![1, 3]));
            });
            let c_id = c.thread().id();
            let s1 = thread::spawn(move || {
                traceforge::sleep(3);
                traceforge::send_tagged_msg(c_id, 1, 2u32);
            });
            let s2 = thread::spawn(move || {
                traceforge::sleep(5);
                traceforge::send_tagged_msg(c_id, 1, 3u32);
            });
            traceforge::send_tagged_msg(c_id, 1, 1u32);
            let _ = s1.join();
            let _ = s2.join();
            let _ = c.join();
        },
    );
    assert_eq!((stats.execs, stats.block), (2, 0));
}

// =====================================================================
// GC refusal branch for blocking (infinite-wait) INBOXES: the inbox
// may also never collect min messages (every matching message dies
// before or while it waits). Exact for min == 1; for min >= 2 the
// all-dead encoding is conservative by design (disjoint-lifetime
// refusal worlds are not separately enumerated; the jointly-infeasible
// ones end in the empty-combinations block, see the pairwise pin).
// Execs are never changed by refusals, only blocked counts grow.
// =====================================================================

// Read feasible (arrival in [3,5]) AND all-dead feasible (arrival <= 2,
// dead before the wait starts at 3): the read world plus one refusal
// world. Was (1, 0) before the inbox refusal branch landed.
#[test]
fn inbox_refusal_class_appears() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 5, 0).build(),
        || {
            let c = thread::spawn(|| {
                traceforge::sleep(3);
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    1,
                    WaitTime::Infinite,
                );
            });
            traceforge::send_tagged_msg(c.thread().id(), 1, 7u32);
            let _ = c.join();
        },
    );
    assert_eq!((stats.execs, stats.block), (1, 1));
}

// No sleep: the wait begins at 0 and the message cannot die before it
// (dead-before needs t_block >= send + L + sd + 1 = 1): the refusal is
// infeasible and must not be pushed. Byte-for-byte the old behavior.
#[test]
fn inbox_refusal_not_pushed_when_infeasible() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 5, 0).build(),
        || {
            let c = thread::spawn(|| {
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    1,
                    WaitTime::Infinite,
                );
            });
            traceforge::send_tagged_msg(c.thread().id(), 1, 7u32);
            let _ = c.join();
        },
    );
    assert_eq!((stats.execs, stats.block), (1, 0));
}

// min = 2: joint read feasible (both arrivals in [3,5]) and all-dead
// feasible (both <= 2). The Option-A gap lives in THIS shape: refusal
// worlds where exactly one message stays alive past the wait are not a
// separate class (documented conservatism), so the count is (1, 1),
// not (1, 2).
#[test]
fn inbox_refusal_min2_all_dead() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 5, 0).build(),
        || {
            let c = thread::spawn(|| {
                traceforge::sleep(3);
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    WaitTime::Infinite,
                );
            });
            let cid = c.thread().id();
            let s1 = thread::spawn(move || traceforge::send_tagged_msg(cid, 1, 1u32));
            let s2 = thread::spawn(move || traceforge::send_tagged_msg(cid, 1, 2u32));
            let _ = s1.join();
            let _ = s2.join();
            let _ = c.join();
        },
    );
    assert_eq!((stats.execs, stats.block), (1, 1));
}

// Two candidates, min = max = 1: worlds are read {a}, read {b}, and
// ONE refuse-all (block == 1 is the load-bearing assertion: refusal and
// exclusion machinery must agree on a single refusal class). The exec
// component may re-baseline when the inbox backward-closure fix lands.
#[test]
fn inbox_refusal_and_exclusions_agree() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 5, 0).build(),
        || {
            let c = thread::spawn(|| {
                traceforge::sleep(3);
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    1,
                    WaitTime::Infinite,
                );
            });
            let cid = c.thread().id();
            let s1 = thread::spawn(move || traceforge::send_tagged_msg(cid, 1, 1u32));
            let s2 = thread::spawn(move || traceforge::send_tagged_msg(cid, 1, 2u32));
            let _ = s1.join();
            let _ = s2.join();
            let _ = c.join();
        },
    );
    assert_eq!(stats.block, 1);
    assert_eq!(stats.execs, 2);
}

// Sender b's send at t = 10 is inevitably alive during the wait (which
// starts at 3), so every refusal pushed against a partial graph is
// voided once b's send is re-derived into the encoding: the branch
// counts as NOTHING (blocked-arm feasibility gate), and the refusing
// block must never wake on b's arrival (wake-skip): returning at all
// with block == 0 pins both. The exec component may re-baseline when
// the inbox backward-closure fix lands.
#[test]
fn inbox_refusal_gated_by_live_future_send() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 5, 0).build(),
        || {
            let c = thread::spawn(|| {
                traceforge::sleep(3);
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    1,
                    WaitTime::Infinite,
                );
            });
            let cid = c.thread().id();
            let s1 = thread::spawn(move || traceforge::send_tagged_msg(cid, 1, 1u32));
            let s2 = thread::spawn(move || {
                traceforge::sleep(10);
                traceforge::send_tagged_msg(cid, 1, 2u32);
            });
            let _ = s1.join();
            let _ = s2.join();
            let _ = c.join();
        },
    );
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 2);
}
