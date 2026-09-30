//! (C6') Timeout feasibility: a finite-wait receive may return `None`
//! only in timelines where every message it could have consumed missed
//! its wait window `[t0, t0 + W]`.
//!
//! Before this constraint the `rf = None` branch was unconstrained, so
//! a receive could time out while a matching message sat readable in
//! its window. That produced CERTIFIED counterexamples for worlds that
//! cannot happen (the witness in `test_no_timeout_when_message_must_be_readable`
//! is the one Sahar reported on 2026-09-16).
//!
//! Since Must-tau (2026-09-29) the constraint is part of the
//! consistency check of every exploration step, so an impossible
//! timeout is never explored: `timeline_impossible` stays 0. Every send
//! of a timed program may also be dropped (no budget), so each count
//! below includes the worlds where messages are lost.
//!
//! Every expected count below is derived by hand from the semantics,
//! not read off the implementation, and was cross-checked graph by
//! graph against the pre-Must-tau exploration with every send lossy.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::*;

fn cfg(l: u64, u: u64, sd: u64) -> Config {
    Config::builder().with_timed(l, u, sd).build()
}

struct Collector(Arc<Mutex<Vec<String>>>);

impl ExecutionObserver for Collector {
    fn after(&mut self, _eid: ExecutionId, _cond: &EndCondition, c: CoverageInfo) {
        let mut goals: Vec<String> = c.coverage.keys().cloned().collect();
        goals.sort();
        self.0.lock().unwrap().push(goals.join(" "));
    }
}

/// Runs `f` and returns its stats and the multiset of `cover!` goals
/// per explored ending.
fn outcomes<F>(l: u64, u: u64, sd: u64, f: F) -> (Stats, BTreeMap<String, usize>)
where
    F: Fn() + Send + Sync + 'static,
{
    let sink = Arc::new(Mutex::new(Vec::new()));
    let stats = verify(
        Config::builder()
            .with_timed(l, u, sd)
            .with_callback(Box::new(Collector(Arc::clone(&sink))))
            .build(),
        f,
    );
    let mut out = BTreeMap::new();
    for o in sink.lock().unwrap().iter() {
        *out.entry(o.clone()).or_insert(0) += 1;
    }
    (stats, out)
}

fn expect(pairs: &[(&str, usize)]) -> BTreeMap<String, usize> {
    pairs.iter().map(|(k, n)| (k.to_string(), *n)).collect()
}

/// L=0, U=1, sd=0, W=10: a delivered message arrives by 1 and the
/// receive waits until 10, so no timeline lets it miss. Every send
/// lossy: the read (delivered) and one timeout (dropped). A timeout
/// beside the delivered message would be a second `None`.
#[test]
fn test_no_timeout_when_message_must_be_readable() {
    let (stats, out) = outcomes(0, 1, 0, || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        cover!(format!("got={got:?}"));
    });
    assert_eq!(
        out,
        expect(&[("got=None", 1), ("got=Some(1)", 1)]),
        "the only timeout is the one where the message was dropped"
    );
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (2, 0, 0));
}

/// Same program with U = 20 > W = 10: the message may genuinely arrive
/// after the deadline, so the timeout is a real world and the assert
/// is a real violation. This is the control that the fix did not just
/// delete the branch.
#[test]
#[should_panic(expected = "assertion failed")]
fn test_timeout_survives_when_message_can_be_late() {
    let _ = verify(cfg(0, 20, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(got.is_some());
    });
}

/// The tie, which pins the HALF-OPEN form of the constraint: with
/// L = U = W the message arrives exactly at the deadline, and the
/// timer may win that race. Both worlds stay: read (a_b = t_e is still
/// inside the storage window) and timeout.
///
/// With the strict form `a_b > t_e` the timeout would vanish here, and
/// every PAR tie cell `To = dK + dL + dR` would turn into a false hold.
///
/// Every send lossy: read and timeout beside the delivered message,
/// plus the timeout with it dropped. Strict form would give 1 + 1.
#[test]
fn test_arrival_exactly_at_deadline_keeps_both_worlds() {
    let (stats, out) = outcomes(5, 5, 0, || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(5));
        cover!(format!("got={got:?}"));
    });
    assert_eq!(
        out,
        expect(&[("got=None", 2), ("got=Some(1)", 1)]),
        "arrival exactly at the deadline races the timer: read and timeout"
    );
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (3, 0, 0));
}

/// A message that died before the wait began cannot be blamed for the
/// timeout: sd = 0 and L = U = 0 put the whole storage window at time
/// 0, while the receive starts at 5 after a sleep. Only the timeout
/// survives (the read is impossible), so the assert fires.
#[test]
#[should_panic(expected = "assertion failed")]
fn test_message_dead_before_the_wait_began_allows_timeout() {
    let _ = verify(cfg(0, 0, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        sleep(5);
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(got.is_some());
    });
}

/// Two matching messages, both certain to be readable in the window:
/// dodging one is not enough, so the timeout needs both dropped.
/// Every send lossy: read 1 (2 delivered or dropped), read 2 (1
/// dropped; FIFO forbids it otherwise), timeout (both dropped).
#[test]
fn test_every_candidate_must_miss_the_window() {
    let (stats, out) = outcomes(0, 1, 0, || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
            send_msg(me, 2u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        cover!(format!("got={got:?}"));
    });
    assert_eq!(
        out,
        expect(&[("got=None", 1), ("got=Some(1)", 2), ("got=Some(2)", 1)]),
        "a timeout only when every candidate was dropped"
    );
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (4, 0, 0));
}

/// A timeout followed by a blocking read of the same message (the
/// timeout-validation idiom). The constraint now does the work the
/// idiom used to do by hand: the timeout forces the message late, and
/// the later read picks it up. The world stays feasible, so this must
/// not regress into a blocked or dropped execution.
#[test]
fn test_timeout_then_late_read_stays_feasible() {
    let stats = verify(cfg(0, 20, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        if recv_msg_timed::<u32>(WaitTime::Finite(10)).is_none() {
            // Only reachable when the message was still in flight.
            let late = recv_msg_block_timed::<u32>();
            assert(late == 1);
        }
    });
    assert!(
        stats.execs >= 2,
        "both the in-window read and the late read must survive, got {}",
        stats.execs
    );
}

/// A message consumed by an earlier receive of the same thread is gone
/// and cannot forbid a later timeout.
#[test]
fn test_message_consumed_earlier_does_not_forbid_timeout() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let first = recv_msg_block_timed::<u32>();
        assert(first == 1);
        let second = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(second.is_none());
    });
    assert_eq!(stats.execs, 1, "nothing is left to read: the timeout stands");
}

/// No matching message at all: the timeout is unconditional.
#[test]
fn test_timeout_with_no_sender_is_unconstrained() {
    let stats = verify(cfg(0, 5, 0), || {
        let got = recv_msg_timed::<u32>(WaitTime::Finite(3));
        assert(got.is_none());
    });
    assert_eq!(stats.execs, 1);
}

/// A collector of one is a receive by another name: with the message
/// certain to arrive inside the window, its empty (timeout) result has
/// no timeline beside the delivered message. Every send lossy: one batch
/// {1} (delivered) and one empty (dropped), and the timeout beside the
/// delivered message is never explored. This also covers the vouching
/// hazard: an inbox visit must not vouch for a graph whose inbox timed
/// out.
#[test]
fn test_inbox_of_one_timeout_obeys_the_same_rule() {
    let (stats, out) = outcomes(0, 1, 0, || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = inbox_timed(1, WaitTime::Finite(10));
        cover!(format!("got={}", got.len()));
    });
    assert_eq!(out, expect(&[("got=0", 1), ("got=1", 1)]));
    assert_eq!(stats.execs, 2);
    assert_eq!(stats.timeline_impossible, 0);
}

/// The same collector when the message may genuinely be late: the
/// empty result is a real world, so the assert is a real violation.
#[test]
#[should_panic(expected = "assertion failed")]
fn test_inbox_of_one_timeout_survives_when_late() {
    let _ = verify(cfg(0, 20, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = inbox_timed(1, WaitTime::Finite(10));
        assert(!got.is_empty());
    });
}

/// A collector of two with only one message available times out for a
/// reason the constraint deliberately does not model (a cardinality
/// condition a difference system cannot express), so that branch stays
/// unconstrained. Pinned so the exemption is a decision, not a drift.
#[test]
fn test_inbox_of_two_timeout_stays_unconstrained() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = inbox_timed(2, WaitTime::Finite(10));
        assert(got.is_empty());
    });
    // Every send lossy: the timeout beside the delivered message and
    // the timeout with it dropped.
    assert_eq!(
        (stats.execs, stats.block, stats.timeline_impossible),
        (2, 0, 0),
        "one message can never complete a batch of two: only timeouts"
    );
}

/// The 16 Sep design-note program: L = U = 0, sd = 100, W = 10. R
/// first does `recv_msg_timed(Finite(10))`; S1 sends b to R, then m to
/// S2; S2 receives m, then sends s to R. Hand count: threads start at
/// 0 and sends are instantaneous, so a(b) = 0 and R's deadline is 10.
///
///   E1 {r = bot, b unread, s unread}: for b neither a(b) >= 10 nor
///      a(b) + 100 < 0 holds. NO timeline: never explored.
///   E2 {r reads s, b unread}: a(s) = t(recv m) in [0, 10], read at
///      a(s); b and s come from different senders, so no (C7) skip
///      group binds them under LocalOrder. Feasible.
///   E3 {r reads b, s unread}: feasible.
///
/// E2 is reachable only through a timeout (s is created there and
/// revisits r from it), so a checker that cut that branch too early
/// would lose E2. Must-tau rejects E1 at the step that makes it
/// impossible and still reaches E2.
///
/// Every send lossy. m delivered: (b, s) both delivered gives E2, E3;
/// only b: read b; only s: read s, or timeout (S2 may take m after
/// R's deadline, so s can be late); neither: timeout. That is 6.
/// m dropped: S2 blocks; R reads b or times out with b dropped: 2
/// blocked.
#[test]
fn test_design_note_program_counts_two_and_drops_one() {
    let stats = verify(cfg(0, 0, 100), || {
        let r = thread::spawn(|| {
            let _ = recv_msg_timed::<u32>(WaitTime::Finite(10));
        });
        let rid = r.thread().id();
        let s2 = thread::spawn(move || {
            let _m: u32 = recv_msg_block_timed();
            send_msg(rid, 3u32); // s
        });
        let s2id = s2.thread().id();
        let _s1 = thread::spawn(move || {
            send_msg(rid, 1u32); // b
            send_msg(s2id, 2u32); // m
        });
    });
    assert_eq!(
        (stats.execs, stats.block, stats.timeline_impossible),
        (6, 2, 0),
        "E2 and E3 are real; E1 (the timeout beside the stored b) is never explored"
    );
}

/// Three threads, L = U = sd = 0: R does `recv_msg_timed(Finite(10))`,
/// S1 sends b, S2 sends s. With both delivered, Sem(P) = {r reads b}
/// and {r reads s}; {r = bot} has no timeline (a(b) = 0 < 10). Both
/// worlds must exist in BOTH thread-creation orders: this is the
/// impossibility example for pruning the timeout at the read (with R
/// visited first there is no candidate yet, the timeout is committed,
/// and the world "r reads s" is only reachable through it).
///
/// Every send lossy: both delivered: read b, read s; one dropped: read
/// the other (2); both dropped: timeout. 5 executions, none dead.
#[test]
fn test_three_thread_example_receiver_first() {
    let stats = verify(cfg(0, 0, 0), || {
        let r = thread::spawn(|| {
            let _ = recv_msg_timed::<u32>(WaitTime::Finite(10));
        });
        let rid = r.thread().id();
        let _s1 = thread::spawn(move || send_msg(rid, 1u32));
        let _s2 = thread::spawn(move || send_msg(rid, 2u32));
    });
    assert_eq!(
        (stats.execs, stats.block, stats.timeline_impossible),
        (5, 0, 0)
    );
}

#[test]
fn test_three_thread_example_senders_first() {
    let stats = verify(cfg(0, 0, 0), || {
        let me = thread::current().id();
        let _s1 = thread::spawn(move || send_msg(me, 1u32));
        let _s2 = thread::spawn(move || send_msg(me, 2u32));
        let _ = recv_msg_timed::<u32>(WaitTime::Finite(10));
    });
    assert_eq!(
        (stats.execs, stats.block, stats.timeline_impossible),
        (5, 0, 0)
    );
}

/// Vouch hole, complete arm. An Infinite-wait inbox visit builds a
/// full-graph exploration oracle and, finding it feasible, used to
/// vouch the completion check off; a finite-wait receive that then
/// timed out was never re-checked, and a graph with no timeline was
/// counted. L = 0, U = 1, sd = 0: the inbox takes message 1 at
/// a1 in [0, 1] (FIFO front, never dead before its wait); the receive
/// then waits [a1, a1 + 10] and message 2 arrives at a2 in [a1, 1]
/// (FIFO), readable exactly then, inside the wait. Its timeout has no
/// timeline. Every send lossy: both delivered: inbox 1, read 2; 2
/// dropped: inbox 1, timeout; 1 dropped: inbox 2, timeout; both
/// dropped: the inbox blocks. 3 executions, 1 blocked, none dead.
#[test]
fn test_vouch_hole_complete_arm() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
            send_msg(me, 2u32);
        });
        let _ = inbox_timed(1, WaitTime::Infinite);
        let _ = recv_msg_timed::<u32>(WaitTime::Finite(10));
    });
    assert_eq!(
        (stats.execs, stats.block, stats.timeline_impossible),
        (3, 1, 0),
        "no timeout beside the delivered message 2 after the inbox"
    );
}

/// Vouch hole, blocked arm: same shape, but the thread then blocks on
/// a receive nothing can satisfy, so the ending is blocked rather than
/// complete and goes through the other gate. Every send lossy: the
/// same four worlds as above, each ending blocked (the last receive
/// has nothing left, or the inbox blocks). 4 blocked, none dead.
#[test]
fn test_vouch_hole_blocked_arm() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
            send_msg(me, 2u32);
        });
        let _ = inbox_timed(1, WaitTime::Infinite);
        let _ = recv_msg_timed::<u32>(WaitTime::Finite(10));
        let _: u32 = recv_msg_block_timed();
    });
    assert_eq!(
        (stats.execs, stats.block, stats.timeline_impossible),
        (0, 4, 0),
        "no blocked ending is reached through an impossible timeout"
    );
}
