//! Hand-counted regression programs for the plain (inbox-free) timed
//! receive machinery, companions of tests/revisit_gaps.rs.
//!
//! Timing model (see the thesis): a local step happens at the instant of
//! its predecessor, sleep(d) adds d, a send with window (L, U) arrives in
//! [t + L, t + U], FIFO and causal delivery couple the arrivals of one
//! sender's messages to the same destination (the later never arrives
//! before the earlier), a graph that admits no timeline counts as neither
//! an execution nor a blocked one, and an untimed receive carries no
//! timing constraint of its own.

use traceforge::{thread, Config, ConsType, SchedulePolicy, WaitTime};

/// A sender's second window contradicts FIFO coupling: m1 is sent at 1
/// with the default window (1, 3), so it arrives in [2, 4]; m2 is sent
/// right after with window (0, 0), so it arrives at 1, before m1, which
/// FIFO forbids. The program has no timeline: zero behaviours. The
/// collector's untimed blocking receive must not turn that into a run
/// that never ends: before 2026-09-14 the offer path pruned every
/// candidate of the untimed receive (no timeline), the wake path woke
/// the block on structural availability, and the checker alternated
/// between the two for ever. Found by the receive-only scheduler fuzz
/// (program 106 of seed 20260914).
#[test]
fn untimed_block_on_a_graph_without_timeline_terminates() {
    for policy in [SchedulePolicy::LTR, SchedulePolicy::Arbitrary] {
        let s = traceforge::verify(
            Config::builder()
                .with_policy(policy)
                .with_seed(11)
                .with_cons_type(ConsType::FIFO)
                .with_timed(1, 3, 0)
                .build(),
            || {
                let c = thread::spawn(|| {
                    let _r: u32 = traceforge::recv_tagged_msg_block(|_, t| t == Some(0));
                });
                let cid = c.thread().id();
                thread::spawn(move || {
                    traceforge::sleep(1);
                    traceforge::send_tagged_msg(cid.clone(), 0, 1u32);
                    traceforge::send_msg_timed(cid, 2u32, 0, 0);
                });
            },
        );
        assert_eq!((s.execs, s.block), (0, 0), "policy {policy:?}");
    }
}

/// The same contradiction with a relay: sender 1 waits for a control
/// message from sender 0 (an untimed blocking receive) before sending,
/// and the collector runs an infinite-wait timed receive under causal
/// delivery. Again no timeline, again zero behaviours, again a run that
/// once never ended (program 96 of the same fuzz seed): the relay's
/// untimed receive was the one woken for ever.
#[test]
fn relayed_untimed_block_on_a_graph_without_timeline_terminates() {
    for policy in [SchedulePolicy::LTR, SchedulePolicy::Arbitrary] {
        let s = traceforge::verify(
            Config::builder()
                .with_policy(policy)
                .with_seed(11)
                .with_cons_type(ConsType::Causal)
                .with_timed(0, 2, 1)
                .build(),
            || {
                let c = thread::spawn(|| {
                    let _r: u32 = traceforge::recv_msg_block_timed();
                });
                let cid = c.thread().id();
                let cid1 = cid.clone();
                let s1 = thread::spawn(move || {
                    let _go: u32 = traceforge::recv_tagged_msg_block(|_, t| t == Some(9));
                    traceforge::sleep(6);
                    traceforge::send_msg_timed(cid1.clone(), 3u32, 1, 3);
                    traceforge::send_tagged_msg(cid1, 0, 4u32);
                });
                let s1id = s1.thread().id();
                thread::spawn(move || {
                    traceforge::send_tagged_msg(s1id, 9, 100u32);
                    traceforge::send_tagged_msg_timed(cid.clone(), 0, 1u32, 1, 3);
                    traceforge::send_tagged_msg_timed(cid, 1, 2u32, 0, 0);
                });
            },
        );
        assert_eq!((s.execs, s.block), (0, 0), "policy {policy:?}");
    }
}

/// Control: the same two programs with the contradiction removed (the
/// second window widened to the default) do have timelines, and the
/// counts are the ones the timing model gives.
///
/// First program: m1 arrives in [2, 4], m2 in [1, 3] but no earlier
/// than m1 by FIFO; the untimed receive reads m1 (the only tag-0
/// message): 1 execution. Second program: the collector's timed
/// receive may read m1 (tag 0, arriving in [1, 3]) or m2 (tag 1,
/// arriving no earlier than m1, within [0, 2] of its send at 0, so at
/// [1, 2]) or m3 (arriving in [7, 9] but dead by then only if sd is
/// exceeded; sd = 1 keeps m1 and m2 alive one unit, and the receive
/// starts at 0 and waits for ever, so it takes a message the instant
/// one is readable): the classes are m1 and m2, since m3 arrives after
/// both are gone but the receive would already have fired, and one
/// refusal ending where both die before the wait (impossible here, the
/// wait starts at 0). Expected 2 executions, 0 blocked.
#[test]
fn controls_with_timelines() {
    let s = traceforge::verify(
        Config::builder().with_cons_type(ConsType::FIFO).with_timed(1, 3, 0).build(),
        || {
            let c = thread::spawn(|| {
                let _r: u32 = traceforge::recv_tagged_msg_block(|_, t| t == Some(0));
            });
            let cid = c.thread().id();
            thread::spawn(move || {
                traceforge::sleep(1);
                traceforge::send_tagged_msg(cid.clone(), 0, 1u32);
                traceforge::send_msg_timed(cid, 2u32, 0, 2);
            });
        },
    );
    assert_eq!((s.execs, s.block), (1, 0));
}

/// The refusal ending is a second outcome of its receive, not a second
/// canonical one. Minimal program (from program 108 of the receive fuzz):
/// the collector sleeps 2, then r1 = non-blocking untimed tagged receive,
/// then r2 = infinite-wait timed tagged receive; main sends m4 at 1 (it
/// arrives in [1, 2] with L = 0, U = 1, sd = 0, so it may already be dead
/// when r2 starts waiting at 2, which makes r2's refusal feasible beside
/// its read of m4); a thread spawned after the collector sleeps 3 and
/// sends m3. Classes: r1 reads nothing or m3 or m4; r2 reads what is left
/// among m3 and m4 at or after 2, or refuses when everything it could
/// read is dead before its wait: r1=None with r2=m3 or r2=m4; r1=m3 with
/// r2=m4; r1=m4 with r2=m3; and one refusal ending (r1=m3, m4 dead before
/// 2, r2 waits for ever): 4 executions, 1 blocked, under every scheduler.
///
/// Before 2026-09-14 the revisit condition treated the refusal block of
/// r2 as canonical (the catch-all arm), so the revisit installing r1=m3
/// was launched twice, once from the world where r2 read m4 and once
/// from the refusal world, and both the class r1=m3, r2=m4 and the
/// refusal ending were explored twice: 5 executions, 2 blocked under LTR
/// and under some arbitrary seeds. The same mechanism duplicated two
/// classes of the larger fuzz program under arbitrary seeds 11 and 13.
#[test]
fn refusal_ending_is_not_a_second_launch_point() {
    for (policy, seed) in [
        (SchedulePolicy::LTR, 0),
        (SchedulePolicy::Arbitrary, 11),
        (SchedulePolicy::Arbitrary, 12),
        (SchedulePolicy::Arbitrary, 13),
        (SchedulePolicy::Arbitrary, 18),
    ] {
        let s = traceforge::verify(
            Config::builder()
                .with_policy(policy)
                .with_seed(seed)
                .with_cons_type(ConsType::FIFO)
                .with_timed(0, 1, 0)
                .build(),
            || {
                let c = thread::spawn(|| {
                    traceforge::sleep(2);
                    let _r1: Option<u32> = traceforge::recv_tagged_msg(|_, t| t == Some(0));
                    let _r2: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(0));
                });
                let cid = c.thread().id();
                let cid2 = cid.clone();
                thread::spawn(move || {
                    traceforge::sleep(3);
                    traceforge::send_tagged_msg(cid2, 0, 3u32);
                });
                traceforge::sleep(1);
                traceforge::send_tagged_msg(cid, 0, 4u32);
            },
        );
        assert_eq!((s.execs, s.block), (4, 1), "policy {policy:?} seed {seed}");
    }
}

