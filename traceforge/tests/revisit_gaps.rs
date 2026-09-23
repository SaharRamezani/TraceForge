//! Regression programs for gaps in the inbox revisit and timing machinery,
//! each with an execution count derived by hand (see the thesis,
//! evaluation chapter, "Three hand-counted regression programs").
//!
//! Thread ids follow spawn order; the main thread is 0. LTR scheduling
//! runs the lowest runnable thread id first, and the candidate order of
//! a receive or inbox is the fixed event order (thread id, then index).

use traceforge::thread::ThreadId;
use traceforge::{thread, Config, ConsType, WaitTime};

#[derive(Clone, Debug, PartialEq)]
struct M(u32);

const DATA: u32 = 2;
const CTRL: u32 = 9;
const XT: u32 = 3;

/// Gap 1 (older unread members of the new set must not rank in a
/// deleted event's candidate list).
///
/// main (0): r = inbox(exactly 2 of tag DATA); then x = recv(tag DATA or XT).
/// C1 (1), C2 (2): send c1, c2 (DATA); S1 (3): send s1 (DATA);
/// C3 (4): send c3 (XT, readable by x only);
/// E (5): waits for F's control message, then sends e (DATA); F (6): control.
///
/// r reads any 2-subset of {c1, c2, s1, e} (6 sets) and x then reads any
/// of the five messages r did not take (3 remain each time): 6 x 3 = 18
/// executions.
///
/// Before the fix, the set {s1, e} was never installed: it is launched
/// from the branch in which x reads c3, and there x was judged
/// non-canonical because the unread s1, a member of the set that r is
/// about to consume, still ranked first in x's list (members of the new
/// set now rank last). 15 executions.
#[test]
fn gap1_new_set_members_excluded_from_deleted_ranking() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(1)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(2)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(3)));
        thread::spawn(move || traceforge::send_tagged_msg(me, XT, M(4)));
        let e = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(100));
        });
        let eid = e.thread().id();
        thread::spawn(move || traceforge::send_tagged_msg(eid, CTRL, M(0)));
        let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(got.len(), 2);
        let _x: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(DATA) || t == Some(XT));
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 18);
}

/// Gap 3 (inbox revisit members must be causally independent of the inbox).
///
/// main (0): r = inbox(exactly 2 of tag DATA); then sends GO to B.
/// E (1): waits for a control message from F, then sends e (DATA).
/// F (2): waits for a control message from H, then sends the control to E.
/// H (3): waits for a control message from D, then sends the control to F.
/// C (4): sends c (DATA).  D (5): sends d (DATA), then the control to H.
/// B (6): waits for GO, then sends s (DATA) to main.
///
/// s is causally after r (it answers GO, which main sends after r), so r
/// can never read s. Read sets: {c, d}, {c, e}, {d, e}: 3 executions.
///
/// When no thread is runnable, every thread whose wake condition holds
/// is unblocked at once and the lowest thread id runs first: main and H
/// wake together (main reads {c, d} and sends GO), then H sends the
/// control to F, then F and B wake together (F first, passing the
/// control to E), then B sends s, and only then does E wake and send e.
/// So s is registered before e is added, and e is causally independent
/// of r. Before the fix, the revisit at e then enumerated {s, e} as
/// well (the member pool held every matching send and only the fresh
/// send was tested for causal independence): a fourth, causally cyclic
/// execution.
#[test]
fn gap3_members_causally_after_inbox_rejected() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        let e = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(3));
        });
        let eid = e.thread().id();
        let f = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(eid, CTRL, M(0));
        });
        let fid = f.thread().id();
        let h = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(fid, CTRL, M(0));
        });
        let hid = h.thread().id();
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(1)));
        thread::spawn(move || {
            traceforge::send_tagged_msg(me, DATA, M(2));
            traceforge::send_tagged_msg(hid, CTRL, M(0));
        });
        let b = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(100));
        });
        let bid = b.thread().id();
        let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(got.len(), 2);
        for v in got.iter().flatten() {
            let m = v.as_any_ref().downcast_ref::<M>().unwrap();
            assert_ne!(m.0, 100, "read a message sent in reply to GO");
        }
        traceforge::send_tagged_msg(bid, CTRL, M(0));
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 3);
}

/// Gap 2 (canonical read set of a blocking timed inbox is the first
/// FEASIBLE subset, as the base choice is).
///
/// L = U = 0, sd = 5, main (0): r = inbox(2, W = inf) waiting from t = 0.
/// A (1): sleep 1; send s1.  B (2): sleep 8; send s2.  D (3): sleep 4; send s3.
/// E (4): waits for F's control message, then sends e.
/// F (5): sleep 2; send the control message.
///
/// Timing model (Chapter methodology, (C3), (C5), Definition def:inbox-tau):
/// a local step happens at the instant of its predecessor, so s1, s2, s3
/// are sent and arrive at 1, 8, 4 and are stored until 6, 13, 9; a
/// blocking receive completes anywhere in the storage window of its
/// message, so E receives the control message at some t in [2, 7] and e
/// arrives at that same t. A waited batch completes at the arrival of
/// its second member, and every message left behind must arrive at or
/// after the read, or be dead before it (or satisfy the documented
/// co-arrival corner of audit A2, which does not change this count).
///
/// Read sets: {s1, s3} at 4 (s2 late; e at or after the read, e >= 4) feasible;
/// {s2, s3} at 8 (s1 dead at 6, e dead at 8 with e = 2) feasible;
/// {s1, e} at e in [2, 4] (s3, s2 late) feasible; {s3, e} at e = 7
/// (s1 dead at 6, s3 stored until 9, s2 late) feasible; {s1, s2}
/// (s1 dead at 8) and {s2, e} (s3 neither late nor dead at 8) infeasible.
/// Hence exactly 4 executions.
///
/// The base branch reads {s2, s3}: the first-in-order pair {s1, s2} is
/// infeasible, so the visit falls back to the first feasible pair in
/// enumeration order. Before the fix the self-check of r still compared
/// its read set with {s1, s2}, judged r non-canonical in every branch,
/// and withheld the revisits {s1, e} and {s3, e} (only {s1, s3} and
/// {s2, s3} were explored; with gap 4 present the inbox never woke at all).
#[test]
fn gap2_canonical_inbox_subset_is_first_feasible() {
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 5).build(), || {
        let me = thread::current().id();
        thread::spawn(move || {
            traceforge::sleep(1);
            traceforge::send_tagged_msg(me, DATA, M(1));
        });
        thread::spawn(move || {
            traceforge::sleep(8);
            traceforge::send_tagged_msg(me, DATA, M(2));
        });
        thread::spawn(move || {
            traceforge::sleep(4);
            traceforge::send_tagged_msg(me, DATA, M(3));
        });
        let e = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(100));
        });
        let eid = e.thread().id();
        thread::spawn(move || {
            traceforge::sleep(2);
            traceforge::send_tagged_msg(eid, CTRL, M(0));
        });
        let got = traceforge::inbox_with_tag_timed(|_, t| t == Some(DATA), 2, WaitTime::Infinite);
        assert_eq!(got.len(), 2);
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 4);
}

/// Gap 4 (member eligibility of a k >= 2 timed inbox must not demand a
/// singleton read).
///
/// L = U = 0, sd = 5, main (0): r = inbox(2, W = inf) from t = 0.
/// A (1): sleep 1; send s1 (arrives 1, stored until 6).
/// D (2): sleep 4; send s3 (arrives 4, stored until 9).
///
/// The batch {s1, s3} completes at 4 while s1 is still stored: exactly
/// one execution. Before the fix, each member was tested as if the inbox
/// read it alone with the other message dodged; s3 failed that test
/// (s1 is alive and unread at 4), the member pool shrank to {s1}, no
/// batch of size 2 existed, and the inbox blocked forever.
#[test]
fn gap4_member_eligibility_is_window_only_for_batches() {
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 5).build(), || {
        let me = thread::current().id();
        thread::spawn(move || {
            traceforge::sleep(1);
            traceforge::send_tagged_msg(me, DATA, M(1));
        });
        thread::spawn(move || {
            traceforge::sleep(4);
            traceforge::send_tagged_msg(me, DATA, M(3));
        });
        let got = traceforge::inbox_with_tag_timed(|_, t| t == Some(DATA), 2, WaitTime::Infinite);
        assert_eq!(got.len(), 2);
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 1);
}

/// Gap 5 (a member read by a receive the revisit deletes is free).
///
/// main (0): r = inbox(exactly 2 of tag DATA); then x = recv(DATA).
/// C1 (1), C2 (2), U (3): send c1, c2, u (DATA) immediately.
/// E (4): waits for F's control message, then sends e (DATA); F (5): control.
///
/// r reads any 2-subset of {c1, c2, u, e} (6 sets) and x one of the two
/// remaining messages: 12 executions.
///
/// The set {u, e} can only be launched from the base branch r = {c1, c2},
/// where x has already read u (its only option). Before the fix that set
/// was rejected because u had a reader, although the revisit deletes x
/// and frees u; the two executions with r = {u, e} were lost (10). The
/// deleted x is now judged canonical when it reads the first member of
/// the new set and has no other candidate (the members rank last).
#[test]
fn gap5_member_read_by_deleted_receive_is_free() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(1)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(2)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(3)));
        let e = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(100));
        });
        let eid = e.thread().id();
        thread::spawn(move || traceforge::send_tagged_msg(eid, CTRL, M(0)));
        let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(got.len(), 2);
        let _x: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(DATA));
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 12);
}

/// Gap 6 (a read set is a set: its members' order must not decide
/// whether the reading event is canonical).
///
/// main (0): r = inbox(exactly 2 of DATA); then x = inbox(exactly 2 of
/// DATA or XT).
/// C1 (1), C2 (2): c1, c2 (DATA); U (3): u (DATA); N (4): n (XT);
/// E (5): waits for F's control message, then sends e (DATA); F (6): control.
///
/// r reads any 2-subset of {c1, c2, u, e} (6 sets) and x any 2 of the
/// three messages left (3 ways): 18 executions.
///
/// The set {u, e} is launched from the branch r = {c1, c2}, x = {u, n},
/// where x must be canonical. Members of the new set rank last, so x's
/// ranked list is [n, u] while x reads the set {u, n}, stored in event
/// order. Comparing the two as ordered lists rejected a canonical x and
/// lost the three executions with r = {u, e} (15); the comparison is on
/// the set.
#[test]
fn gap6_read_set_identity_is_order_insensitive() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(1)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(2)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(3)));
        thread::spawn(move || traceforge::send_tagged_msg(me, XT, M(4)));
        let e = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(100));
        });
        let eid = e.thread().id();
        thread::spawn(move || traceforge::send_tagged_msg(eid, CTRL, M(0)));
        let first = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(first.len(), 2);
        let second = traceforge::inbox_with_tag_and_bounds(
            |_, t| t == Some(DATA) || t == Some(XT),
            2,
            Some(2),
        );
        assert_eq!(second.len(), 2);
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 18);
}

/// Gap 7 (an inbox collects from every unread matching send, so two
/// messages of one sender may share a batch).
///
/// main (0): r = inbox(exactly 2 of DATA).
/// A (1): a; B (2): b; T (3): waits for D's control, then sends t1, t2;
/// D (4): the control message to T.
///
/// The pool is every unread matching send, as in the source algorithm,
/// and a read set is closed under the channel's delivery order: a batch
/// holding t2 also holds t1, which T sent first and which is unread
/// (Definition A.4(b) of the source algorithm: no unread matching send
/// precedes a read one). Read sets: {a,b}, {a,t1}, {b,t1}, {t1,t2}:
/// 4 executions. {a,t2} and {b,t2} would leave t1 behind t2.
///
/// Before the fix the pool was the per-sender antichain, which held t2
/// back while t1 was unread and so never offered {t1,t2} forward, while
/// a backward revisit could still install it: the count depended on when
/// the two-message sender ran. With the closure dropped instead, all six
/// 2-subsets are read: 6.
#[test]
fn gap7_inbox_collects_from_all_unread_sends() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(1)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(2)));
        let t = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(10));
            traceforge::send_tagged_msg(me, DATA, M(11));
        });
        let tid = t.thread().id();
        thread::spawn(move || traceforge::send_tagged_msg(tid, CTRL, M(0)));
        let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(got.len(), 2);
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 4);
}

/// Gap 9 (the send count that decides whether an inbox may proceed is
/// the number of unread matching sends).
///
/// main (0): r = inbox(exactly 2 of DATA). A (1): sends a1 then a2.
///
/// Two matching messages are available, so the inbox collects both and
/// the program has exactly one execution and no blocked one. Counting
/// the per-sender antichain instead gave one available message, so the
/// inbox blocked with its quorum already on the queue: no execution at
/// all. This is the shape the source algorithm's extensibility condition
/// names, where an inbox blocks only when the unread matching sends
/// number fewer than its minimum.
#[test]
fn gap9_send_count_is_over_unread_sends() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        thread::spawn(move || {
            traceforge::send_tagged_msg(me, DATA, M(1));
            traceforge::send_tagged_msg(me, DATA, M(2));
        });
        let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(got.len(), 2);
        let mut ids: Vec<u32> = got
            .iter()
            .flatten()
            .map(|v| v.as_any_ref().downcast_ref::<M>().unwrap().0)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 1);
}

/// Gap 8 (the late member sorts first: the ranking must not depend on
/// when a message was added).
///
/// Same shape as gap 5, but the delayed sender has the LOWEST thread id,
/// so its message ranks before every other candidate.
/// main (0): r = inbox(exactly 2 of DATA); then x = recv(DATA).
/// E (1): waits for F's control message, then sends e (DATA).
/// C1 (2), C2 (3), U (4): send c1, c2, u (DATA); F (5): the control message.
///
/// r reads any 2-subset of {e, c1, c2, u} (6 sets) and x one of the two
/// messages left: 12 executions.
///
/// This is the ordering under which a set's members rank first rather
/// than last in a deleted event's list, so it exercises the same rules
/// as gap 1 and gap 5 from the other side.
#[test]
fn gap8_late_member_sorting_first() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        let e = thread::spawn(move || {
            let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
            traceforge::send_tagged_msg(me, DATA, M(100));
        });
        let eid = e.thread().id();
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(1)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(2)));
        thread::spawn(move || traceforge::send_tagged_msg(me, DATA, M(3)));
        thread::spawn(move || traceforge::send_tagged_msg(eid, CTRL, M(0)));
        let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 2, Some(2));
        assert_eq!(got.len(), 2);
        let _x: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(DATA));
    });
    assert_eq!(stats.block, 0);
    assert_eq!(stats.execs, 12);
}

/// Gap 10 (a batch is closed under the channel's delivery order).
///
/// Untimed FIFO. main (0): r = inbox(exactly 1); then x = recv (non-blocking).
/// S (1): sends m1 then m2 to main.
///
/// The inbox runs first and blocks on an empty pool; S sends m1, the
/// inbox wakes and reads {m1}; S sends m2. A backward revisit would
/// install {m2}, but that batch leaves m1, sent first by the same sender,
/// unread behind m2 (Definition A.4(b) of the source algorithm), so the
/// inbox reads {m1} only and x then takes m2 or nothing: 2 executions.
///
/// With the closure dropped the revisit installs {m2} and x reads m1 or
/// nothing: 4, two of them impossible under FIFO delivery.
#[test]
fn gap10_batch_is_closed_under_fifo_order() {
    let stats = traceforge::verify(Config::builder().build(), || {
        let me = thread::current().id();
        thread::spawn(move || {
            traceforge::send_msg(me, M(1));
            traceforge::send_msg(me, M(2));
        });
        let got = traceforge::inbox_with_bounds(1, Some(1));
        assert_eq!(got.len(), 1);
        let _x: Option<M> = traceforge::recv_msg();
    });
    assert_eq!((stats.execs, stats.block), (2, 0));
}

/// Gap 10 under causal delivery: the order is porf between sends.
///
/// main (0): r = inbox(exactly 1 of DATA); then x = recv(DATA, non-blocking).
/// P (1): waits for a control message from Q, then sends p (DATA).
/// Q (2): sends q (DATA) to main, then the control message to P.
///
/// q is causally before p (q, then the control message, then p), so a
/// batch holding p must hold q: the inbox reads {q} and x takes p or
/// nothing: 2 executions. With the closure dropped, {p} is also
/// installed by the backward revisit and x reads q or nothing: 4.
#[test]
fn gap10_batch_is_closed_under_causal_order() {
    let stats = traceforge::verify(
        Config::builder().with_cons_type(ConsType::Causal).build(),
        || {
            let me = thread::current().id();
            let p = thread::spawn(move || {
                let _go: M = traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL));
                traceforge::send_tagged_msg(me, DATA, M(2));
            });
            let pid = p.thread().id();
            thread::spawn(move || {
                traceforge::send_tagged_msg(me, DATA, M(1));
                traceforge::send_tagged_msg(pid, CTRL, M(0));
            });
            let got = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(DATA), 1, Some(1));
            assert_eq!(got.len(), 1);
            let _x: Option<M> = traceforge::recv_tagged_msg(|_, t| t == Some(DATA));
        },
    );
    assert_eq!((stats.execs, stats.block), (2, 0));
}

/// Gap 10, timed: the closure is judged by the timeline oracle, with a
/// dead older sibling as the only way to leave one behind.
///
/// Timed FIFO, L = U = 0, sd = 100: a message arrives the instant it is
/// sent and stays alive for the whole run. main (0) sends m1 then m2 to
/// the collector C (1), which runs inbox(exactly 1, infinite wait) and
/// then recv(wait 50). Both messages are stored at C's t0 = 0; the
/// batch {m2} would leave m1 behind while alive, so the inbox reads {m1}
/// and the receive takes m2: 1 execution. It cannot time out instead,
/// since (C6b) (2026-09-22): m2 is stored over [0, 100] and the wait
/// runs [0, 50], so no timeline lets that wait miss it.
///
/// This pin read 2 until 2026-09-22 through an accounting hole: the
/// Infinite-wait inbox visit vouched the completion-time check off, and
/// a timeout committed afterwards never re-armed it, so the timeout
/// world was counted without being judged. Every timeout commit now
/// re-arms the check (tests/timeout_feasibility.rs, the vouch-hole pins).
///
/// The offer probe once lacked the skip disjunction the committed
/// encoding carries, so {m2} was offered, and the completion-time
/// timeline check had been vouched off by the inbox visit: the graph
/// with the inbox reading m2 and the receive timing out was counted
/// although no timeline satisfies it (one extra execution without a
/// timeline). Debug builds now re-check vouched completions.
#[test]
fn gap10_timed_batch_cannot_leave_a_live_sibling() {
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 100).build(), || {
        let c = thread::spawn(|| {
            let got = traceforge::inbox_timed(1, WaitTime::Infinite);
            assert_eq!(got.len(), 1);
            let _rest: Option<M> = traceforge::recv_msg_timed(WaitTime::Finite(50));
        });
        let cid = c.thread().id();
        traceforge::send_msg(cid, M(1));
        traceforge::send_msg(cid, M(2));
    });
    assert_eq!((stats.execs, stats.block), (1, 0));
}

/// Gap 10, the condition that stays: a batch completed by a later
/// arrival cannot leave behind a message that was already stored.
/// main sends m1, m2 at t = 0, sleeps 3, sends m3 at t = 3; C runs
/// inbox(exactly 2, infinite wait) then recv(wait 50). At t0 = 0 the
/// inbox has m1, m2 stored and reads {m1,m2} immediately. The batches
/// {m1,m3} and {m2,m3} would complete at m3's arrival, t = 3, with the
/// excluded message stored since t = 0 and alive until t = 100: it
/// would have completed the batch earlier, and it is an unread older
/// sibling of m3 besides. The receive then takes m3: 1 execution, the
/// same under every variant of the rule. It cannot time out, since
/// (C6b): m3 is stored over [3, 103] and the wait runs [0, 50]. (Read 2
/// until 2026-09-22 through the same vouch hole as the test above.)
#[test]
fn gap10_waited_batch_cannot_ignore_a_stored_message() {
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 100).build(), || {
        let c = thread::spawn(|| {
            let got = traceforge::inbox_timed(2, WaitTime::Infinite);
            assert_eq!(got.len(), 2);
            let _rest: Option<M> = traceforge::recv_msg_timed(WaitTime::Finite(50));
        });
        let cid = c.thread().id();
        traceforge::send_msg(cid, M(1));
        traceforge::send_msg(cid, M(2));
        traceforge::sleep(3);
        traceforge::send_msg(cid, M(3));
    });
    assert_eq!((stats.execs, stats.block), (1, 0));
}

/// Gap 11 (a message consumed by a reader that follows the inbox is
/// still stored at the inbox's read).
///
/// Timed FIFO, L = U = 0, sd = 100. main (0) spawns C (1), A (2), B (3),
/// X (4) and tells A, B and X who C is. A: sleep 1, send m1 to C.
/// B: sleep 5, send m2 to C. X: sleep 3, send x to C. C: inbox(exactly
/// 2, infinite wait) then recv(wait 50).
///
/// Arrivals: m1 at 1, x at 3, m2 at 5. The stored count first reaches
/// two at 3, with {m1, x}, so that is the only batch; the receive then
/// takes m2 (arrival 5): 1 execution. It cannot time out instead, since
/// (C6') (2026-09-21): m2 is stored over [5, 105] (sd = 100) and the
/// wait runs [3, 53], so no timeline lets that wait miss it. The gap
/// this test guards against would still show up as an EXTRA execution
/// (the {m1, m2} batch with x exempted), so the pin still detects it.
///
/// The inbox visit runs while m1 and m2 exist and x does not, and
/// commits {m1, m2} completing at 5. When x is added, the completion
/// timeline check exempted x from the batch's exclusion condition
/// because C's later receive consumes it, although x was stored at the
/// inbox's read, and the checker counted that graph: 3 executions, one
/// with no timeline. A consumer that follows the read no longer exempts.
#[test]
fn gap11_consumer_after_the_inbox_leaves_the_message_stored() {
    // C is spawned first so that A, B and X learn its id at spawn time:
    // a control message would have to be read with a timed receive, and
    // with sd = 100 that read could be up to 100 late, which is not the
    // program this pin describes.
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 100).build(), || {
        let c = thread::spawn(|| {
            let got = traceforge::inbox_timed(2, WaitTime::Infinite);
            assert_eq!(got.len(), 2);
            let _rest: Option<M> = traceforge::recv_msg_timed(WaitTime::Finite(50));
        });
        let cid = c.thread().id();
        let _a = thread::spawn(move || {
            traceforge::sleep(1);
            traceforge::send_msg(cid, M(1));
        });
        let _b = thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_msg(cid, M(2));
        });
        let _x = thread::spawn(move || {
            traceforge::sleep(3);
            traceforge::send_msg(cid, M(3));
        });
    });
    assert_eq!((stats.execs, stats.block), (1, 0));
}

/// Gap 12 (a batch of two or more may leave behind a sibling that
/// expired mid-wait).
///
/// Timed FIFO, L = U = 0, sd = 1. C (1): inbox(exactly 2, infinite
/// wait) from t = 0. main (0) sends m1 at 0, sleeps 5, sends m2 at 5;
/// T (2) sleeps 5 and sends t at 5.
///
/// m1 is stored in [0, 1] and nothing else is stored while it lives, so
/// it completes no batch; at 5 the count reaches two with {m2, t}, and
/// the batch completes there: 1 execution. Leaving m1 out is legal for a
/// batch of two because a single readable message completes nothing, so
/// the dead alternative of the skip clause is anchored at the read; with
/// the anchor at the wait start (the front rule of a receive, which
/// stays for a batch of one) the inbox blocked for ever: 0 executions,
/// 1 blocked. The message of another sender in the same position was
/// always allowed to expire (exclusion alternative B), so the two rules
/// now agree.
#[test]
fn gap12_batch_may_leave_a_sibling_that_expired_mid_wait() {
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 1).build(), || {
        let c = thread::spawn(|| {
            let got = traceforge::inbox_timed(2, WaitTime::Infinite);
            assert_eq!(got.len(), 2);
        });
        let cid = c.thread().id();
        thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_msg(cid, M(3));
        });
        traceforge::send_msg(cid, M(1));
        traceforge::sleep(5);
        traceforge::send_msg(cid, M(2));
    });
    assert_eq!((stats.execs, stats.block), (1, 0));
}

/// Gap 12, finite wait: the same program with inbox(exactly 2, wait 20).
/// The batch {m2, t} completes at 5 and the timeout branch is explorable
/// by design: 2 executions. With the wait-start anchor the batch was
/// silently absent and only the timeout remained: 1.
#[test]
fn gap12_finite_wait_batch_is_not_silently_lost() {
    let stats = traceforge::verify(Config::builder().with_timed(0, 0, 1).build(), || {
        let c = thread::spawn(|| {
            let got = traceforge::inbox_timed(2, WaitTime::Finite(20));
            assert!(got.is_empty() || got.len() == 2);
        });
        let cid = c.thread().id();
        thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_msg(cid, M(3));
        });
        traceforge::send_msg(cid, M(1));
        traceforge::sleep(5);
        traceforge::send_msg(cid, M(2));
    });
    assert_eq!((stats.execs, stats.block), (2, 0));
}
