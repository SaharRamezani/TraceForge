//! Pins for the 2026-08-28 FIFO-arrival decision.
//!
//! Model under test: every non-dropped send s gets ONE explicit
//! arrival variable a_s in [t_s + L, t_s + U] (per-send overrides via
//! send_msg_timed and friends); a read of s happens at some t with
//! a_s <= t <= a_s + sd; same-channel sends b1, b2 with b1 in sb(b2)
//! (delivery-model order; LocalOrder, the default, is same-sender
//! program order) satisfy a_b1 <= a_b2 when both are delivered.
//! Dropped lossy sends never arrive and are exempt from coupling.
//! A message has one arrival consistent across every constraint site
//! (single-arrival consistency; previously each site projected its own
//! phantom arrival out of [t_s + L, t_s + U]). Programs whose transit
//! overrides contradict FIFO admit no timeline at all: they have zero
//! behaviors, and timeline-impossible branches count as nothing
//! (neither execs nor blocked).
//!
//! Operational rules the derivations below also rely on (established
//! semantics on this branch, see timed_exact.rs and the inbox_timed
//! docs in src/lib.rs):
//!   - a timeout branch (rf = None) is ALWAYS explorable by design and
//!     adds exactly W to the clock, constraining no arrivals;
//!   - a waiting receive takes the channel front the moment it becomes
//!     readable, so reading past an sb-earlier matching message is
//!     legal only in timelines where that message was never readable
//!     during the wait (skip = evict; time-dead fronts do not seal);
//!   - the timed inbox member pool is the sb-minimal antichain: at
//!     most one candidate per (sender, predicate) at a time.
//!
//! Every expected (execs, block) pair is derived BY HAND in the
//! comment above its assert; asserts are never tuned to the checker.
//! Where the checker disagrees, the test is #[ignore]d, keeps the
//! derived value, and documents both sides. (The one disagreement
//! this file originally documented, offer sealing by a possibly-dead
//! front, was FIXED the same day by dead-front unsealing; that test
//! is live now.)

use traceforge::thread;
use traceforge::*;

// ---------------------------------------------------------------------
// 1. Single-arrival consistency: a read site and a FIFO coupling site
// share ONE arrival.
//
// Config (0, 0, 0). One sender (main), one channel, two sends at
// sender-clock 0:
//   m  (tag 1, transit [0, 10]): a_m in [0, 10]
//   m2 (tag 2, transit [0, 0]):  a_m2 = 0
// FIFO (same sender): a_m <= a_m2 = 0, so m's wide window collapses
// to a_m = 0 in EVERY timeline. The base stays satisfiable (a_m = 0):
// the program is not vacuous; the collapse only shows up when a read
// site puts its own demand on the same single arrival.
//
// Receiver: sleep(5); r1 = recv(tag 1, Finite(0)), a point read at
// exactly t = 5 (sd = 0: a message is readable only at its arrival
// instant).
//
// Branches:
//   (r1 = m):  needs a_m = 5. The read site (a_m = 5, arriving late)
//              and the coupling site (a_m <= 0, forced early) bind
//              the SAME variable: no timeline.           IMPOSSIBLE
//              Pre-FIFO the read site projected its own phantom
//              arrival out of [0, 10] (5 fits) and no coupling
//              existed: the branch was feasible, giving (2, 0).
//   (r1 = TO): timeouts are always explorable.             FEASIBLE
//
// Expected: (1, 0).
// ---------------------------------------------------------------------

#[test]
fn single_arrival_pins_wide_window_through_successor() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                traceforge::sleep(5);
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    |_, t| t == Some(1),
                    WaitTime::Finite(0),
                );
            });
            let r = receiver.thread().id();
            traceforge::send_tagged_msg_timed(r, 1, 10u32, 0, 10); // m
            traceforge::send_tagged_msg_timed(r, 2, 20u32, 0, 0); // m2
        },
    );
    assert_eq!((stats.execs, stats.block), (1, 0));
}

// ---------------------------------------------------------------------
// 1b. Single-arrival consistency, two-reads form: read1's skip-dodge
// needs b dead early, read2's rf (through the FIFO chain) needs b
// alive late. DISAGREEMENT, see below.
//
// Config (0, 0, 0). One sender (main), one channel, three sends at
// sender-clock 0, each with transit [0, 10]:
//   c (tag 1), then b (tag 2), then s (tag 3).
// FIFO: a_c <= a_b <= a_s, each in [0, 10].
// Receiver: sleep(5); rA = recv(tag 1, Finite(0)); rB = recv(tag 2 or
// tag 3, Finite(0)). Every read and timeout lands exactly at t = 5.
//
// Hand enumeration over timelines (rA in {c, TO}, rB in {b, s, TO}):
//   (c, b):   a_c = 5, a_b = 5, a_s in [5, 10].            FEASIBLE
//   (c, s):   rB reading s past the unread matching b needs b never
//             readable during rB's wait, the point 5: a_b != 5. But
//             rA's read pins a_c = 5 and rB's pins a_s = 5, so the
//             chain forces a_b = 5. b's ONE arrival cannot satisfy
//             both sites.                                IMPOSSIBLE
//             (Pre-FIFO the dodge used its own phantom a_b = 0 while
//             no chain existed: feasible, so the operational counts
//             are pre 6, post 5.)
//   (c, TO):  timeout by design (also realizable: a_b, a_s >= 6).
//                                                          FEASIBLE
//   (TO, b):  a_b = 5, a_c <= 4 (or anything <= 5).        FEASIBLE
//   (TO, s):  timeline a_c <= a_b = 4, a_s = 5: b arrives and dies
//             at 4 during the receiver's sleep; at 5 the front b is
//             a corpse, s is readable, rB takes s. Realizes every
//             stated constraint (windows, chain, single arrival,
//             read within [a, a + sd]).                    FEASIBLE
//   (TO, TO): by design.                                   FEASIBLE
//
// Derived: (5, 0).
//
// RESOLVED 2026-08-28: the offer path now performs dead-front
// unsealing (coherent_rfs_in_view offers a deeper eligible candidate
// when the joint probe "every earlier front dodged AND candidate
// readable" is satisfiable; probe_recv_rf_skipping in timed_dcs.rs),
// with parity at the wake path and revisit-blocker judgment. The
// checker now finds all five executions. Original analysis kept
// below for the record.
//
// Analysis (why the pre-fix checker was wrong): the offer filter kept a
// candidate if SOME timeline lets this receive read it, then retains
// only sb-minimal survivors (cons.rs coherent_rfs_in_view). Front b
// is readable at 5 in SOME timeline (a_b = 5), so it survives the
// eligibility filter and monopolizes the offer; the worlds a_b <= 4,
// in which b is time-dead at rB and the 2026-08-08 GC decision says
// a time-dead front does not seal its channel, are silently folded
// away. The per-candidate existential test was adequate for interval
// semantics, but under explicit arrivals eligibility is per timeline:
// the correct offer condition for s is the JOINT system "b dead
// throughout rB's wait AND s readable at the read", which is exactly
// what the committed skip-dodge honesty encodes
// (timed_dcs.rs push_recv_skip_cases) and it is satisfiable here
// (a_b = 4). So exploration refuses a graph its own committed
// encoding certifies as feasible: an under-approximation at the seam
// between the 2026-08-08 offer rule and the 2026-08-28 arrival model.
// Note the same collapse is CORRECT in test
// same_sender_uniform_windows_match_prefifo_count below, where the
// joint dodge is unsatisfiable; the two tests bracket the frontier.
// ---------------------------------------------------------------------

#[test]
fn possibly_dead_front_skip_world_is_lost_by_offer_sealing() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                traceforge::sleep(5);
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    |_, t| t == Some(1),
                    WaitTime::Finite(0),
                );
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    |_, t| t == Some(2) || t == Some(3),
                    WaitTime::Finite(0),
                );
            });
            let r = receiver.thread().id();
            traceforge::send_tagged_msg_timed(r, 1, 10u32, 0, 10); // c
            traceforge::send_tagged_msg_timed(r, 2, 20u32, 0, 10); // b
            traceforge::send_tagged_msg_timed(r, 3, 30u32, 0, 10); // s
        },
    );
    assert_eq!((stats.execs, stats.block), (5, 0));
}

// ---------------------------------------------------------------------
// 2. Per-sender FIFO with uniform overlapping windows [0, 10]:
// coupling satisfiable, count equals the pre-FIFO value.
//
// Config (0, 10, 0): every send has window [0, 10], sd = 0. Main
// sends m1 then m2 (same sender, same channel): a1, a2 in [0, 10],
// a1 <= a2. Receiver: r1 = recv(Finite(20)); r2 = recv(Finite(20))
// from t0 = 0.
//
// Branches:
//   (m1, m2): t_r1 = a1, t_r2 = a2 >= a1.                  FEASIBLE
//   (m1, TO): timeout by design.                           FEASIBLE
//   (m2, _):  r1 taking m2 past the unread m1 needs m1 never
//             readable during r1's wait [0, 20]. m1's arrival a1 is
//             in [0, 10], inside the wait, in EVERY timeline (a skip
//             would need m1 dead before the wait began, a1 + sd < 0,
//             impossible), so a waiting receiver always takes m1
//             first (FIFO puts it at the front no later than m2).
//                                                        IMPOSSIBLE
//   (TO, m1) and (TO, m2): r1's timeout advances the clock to 20;
//             both readable instants are <= 10 < 20.     IMPOSSIBLE
//   (TO, TO): by design.                                   FEASIBLE
//
// 3 executions, 0 blocked.
// Pre-FIFO value: also 3. The skip already required death before the
// wait began (impossible at t0 = 0 with or without coupling);
// (m1, m2) already forced a2 >= a1 through read program order
// (t_r2 >= t_r1 = a1 and t_r2 = a2); the TO windows are unchanged.
// The coupling is invisible on overlapping uniform windows, as it
// should be.
// ---------------------------------------------------------------------

#[test]
fn same_sender_uniform_windows_match_prefifo_count() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 10, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(20));
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(20));
            });
            let r = receiver.thread().id();
            traceforge::send_msg(r, 1u32); // m1
            traceforge::send_msg(r, 2u32); // m2
        },
    );
    assert_eq!((stats.execs, stats.block), (3, 0));
}

// ---------------------------------------------------------------------
// 3. FIFO vacuity is total, not partial.
//
// Same-sender transit inversion like the timed_exact.rs pins, plus a
// THIRD live message whose delivery is perfectly consistent on its
// own. Partial vacuity is not a thing: the arrival system is one
// conjunction per graph, so a contradiction between m1 and m2 leaves
// NO timeline in which m3 arrives either.
//
// Config (0, 0, 0). Main sends to one channel:
//   m1 (tag 1, [5, 5]): a1 = 5
//   m2 (tag 2, [0, 0]): a2 = 0
//   m3 (tag 3, [7, 7]): a3 = 7
// FIFO: a1 <= a2 <= a3, and 5 <= 0 is false: the base system has no
// solution regardless of what any receive does.
//
// Receiver: recv(tag 3, Finite(10)). If partial vacuity existed, the
// branches (read m3 at 7) and (timeout) would both be live, giving
// (2, 0): m3's read passes only non-matching sends (tags select, so
// they impose no skip obligation on it). Instead every branch shares
// the contradictory sends: the program has zero behaviors, and
// timeline-impossible branches count as nothing.
//
// Expected: (0, 0).
// ---------------------------------------------------------------------

#[test]
fn fifo_vacuity_is_total_not_partial() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_tagged_msg_timed(
                    |_, t| t == Some(3),
                    WaitTime::Finite(10),
                );
            });
            let r = receiver.thread().id();
            traceforge::send_tagged_msg_timed(r, 1, 1u32, 5, 5); // m1
            traceforge::send_tagged_msg_timed(r, 2, 2u32, 0, 0); // m2
            traceforge::send_tagged_msg_timed(r, 3, 3u32, 7, 7); // m3, live
        },
    );
    assert_eq!((stats.execs, stats.block), (0, 0));
}

// ---------------------------------------------------------------------
// 4. Lossy exemption: a dropped send has no arrival, hence no
// coupling.
//
// Config (0, 0, 0) with lossy budget 1. Main sends to one channel:
//   m1 = send_lossy_msg_timed(r, 1, 10, 10): a1 = 10 when delivered
//   m2 = send_msg_timed(r, 2, 0, 0):         a2 = 0
// Receiver: recv(Finite(0)) at t0 = 0, wait is the point [0, 0].
//
// The lossy budget (see lossy_channels.rs: one lossy send under
// with_lossy(1) explores exactly the delivered world and the dropped
// world) splits the exploration:
//
// m1 DELIVERED: FIFO couples a1 <= a2, i.e. 10 <= 0: contradiction.
//   The whole delivered world is vacuous; every branch in it counts
//   as nothing.
// m1 DROPPED: m1 never arrives, gets no arrival variable, is exempt
//   from coupling, and is never readable, so reading past it carries
//   no obligation. a2 = 0.
//     (read m2): t = 0 = a2.                               FEASIBLE
//     (timeout): by design.                                FEASIBLE
//
// Expected: (2, 0).
// ---------------------------------------------------------------------

#[test]
fn lossy_drop_exempts_fifo_coupling() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).with_lossy(1).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(0));
            });
            let r = receiver.thread().id();
            traceforge::send_lossy_msg_timed(r, 1u32, 10, 10); // m1
            traceforge::send_msg_timed(r, 2u32, 0, 0); // m2
        },
    );
    assert_eq!((stats.execs, stats.block), (2, 0));
}

// ---------------------------------------------------------------------
// 5. Cross-sender NON-coupling under LocalOrder.
//
// Two different senders to one channel with inverted windows. Under
// LocalOrder sb is per-sender program order, so sends of different
// senders are never sb-related: no arrival coupling, and overtaking
// across senders stays allowed. Cross-sender messages also impose no
// skip obligations on each other and are not evicted by each other
// (dodge and eviction are sb-based too).
//
// Config (0, 0, 0). Thread A: send x [5, 5] (a_x = 5). Thread B:
// send y [0, 0] (a_y = 0). Receiver: r1 = recv(Finite(10)); r2 =
// recv(Finite(10)) from t0 = 0. sd = 0: each message is readable only
// at its arrival instant.
//
// Branches (candidate choice across senders is free scheduling; only
// same-sender FIFO restricts delivery order):
//   (y, x):  t_r1 = 0, t_r2 = 5. y overtakes x: legal, the pair is
//            uncoupled.                                    FEASIBLE
//   (y, TO): timeout by design.                            FEASIBLE
//   (x, TO): t_r1 = 5; passing over the readable y is allowed
//            (cross-sender, no order to honor) and y is NOT evicted,
//            but r2 = y would need t_r2 = 0 >= t_r1 = 5, so only the
//            timeout continues.                            FEASIBLE
//   (x, y):  t_r2 = a_y = 0 < t_r1 = 5.                  IMPOSSIBLE
//   (TO, x) and (TO, y): r1's timeout moves the clock to 10; both
//            readable instants (0 and 5) are past.       IMPOSSIBLE
//   (TO, TO): by design.                                   FEASIBLE
//
// Expected: (4, 0). (A wrongly channel-global coupling would force
// a_x <= a_y, 5 <= 0, and collapse this to (0, 0); losing only the
// overtaking would drop the (y, _) branches to (2, 0). The pin
// distinguishes these.)
// ---------------------------------------------------------------------

#[test]
fn cross_sender_sends_not_coupled_under_local_order() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 0, 0).build(),
        || {
            let receiver = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
            });
            let r = receiver.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg_timed(r, 1u32, 5, 5); // x
            });
            let _b = thread::spawn(move || {
                traceforge::send_msg_timed(r, 2u32, 0, 0); // y
            });
        },
    );
    assert_eq!((stats.execs, stats.block), (4, 0));
}

// ---------------------------------------------------------------------
// 6a. Same-sender min = 2 inbox: the antichain rule dominates the sd
// question entirely.
//
// The task shape (same-sender m1 [0, 0], m2 [5, 5], a min = 2 inbox
// on the channel) never even reaches the lifetime analysis: the inbox
// member pool is the sb-minimal ANTICHAIN of the channel (accepted
// semantics, see the "Single-sender batches" section of the
// inbox_timed docs in src/lib.rs), so at most ONE message per
// (sender, predicate) is a candidate at a time. The pool is {m1},
// 1 < min = 2 in every timeline, and the only outcome is the timeout
// empty at t0 + 10. This holds for ANY sd: with sd = 2 (disjoint
// lifetimes [0, 2] and [5, 7]) and equally with sd = 5 (touching
// lifetimes), pinning that sd is invisible in the same-sender shape.
//
// Expected: (1, 0) for both sd values. (Finite wait: the
// never-collects world IS the timeout execution; GC refusal classes
// exist only for infinite waits, so block = 0.)
// ---------------------------------------------------------------------

fn run_min2_inbox(sd: u64, same_sender: bool) -> Stats {
    traceforge::verify(Config::builder().with_timed(0, 0, sd).build(), move || {
        let collector = thread::spawn(|| {
            let _ = traceforge::inbox_with_tag_timed(
                |_, t| t == Some(1),
                2,
                Some(2),
                WaitTime::Finite(10),
            );
        });
        let c = collector.thread().id();
        if same_sender {
            traceforge::send_tagged_msg_timed(c, 1, 1u32, 0, 0); // m1
            traceforge::send_tagged_msg_timed(c, 1, 2u32, 5, 5); // m2
        } else {
            let _a = thread::spawn(move || {
                traceforge::send_tagged_msg_timed(c, 1, 1u32, 0, 0); // m1
            });
            let _b = thread::spawn(move || {
                traceforge::send_tagged_msg_timed(c, 1, 2u32, 5, 5); // m2
            });
        }
    })
}

#[test]
fn same_sender_min2_inbox_starves_by_antichain() {
    let disjoint = run_min2_inbox(2, true);
    assert_eq!((disjoint.execs, disjoint.block), (1, 0));
    let touching = run_min2_inbox(5, true);
    assert_eq!((touching.execs, touching.block), (1, 0));
}

// ---------------------------------------------------------------------
// 6b. Cross-sender min = 2 inbox, disjoint lifetimes: the sd
// interaction proper.
//
// Sender A: m1 [0, 0] (a1 = 0); sender B: m2 [5, 5] (a2 = 5); both
// tag 1. Cross-sender, so both are sb-minimal (both in the pool) and
// no FIFO coupling applies (the base is trivially satisfiable).
// Collector: inbox(tag 1, min 2, max 2, Finite(10)) from t0 = 0.
//
// With sd = 2 the storage lifetimes are [0, 2] and [5, 7]: disjoint.
// A waited min-sized inbox read happens at max(t0, completing
// arrival) and needs every member stored and alive then:
//   immediate at t0 = 0:      a2 <= 0 is false.          INFEASIBLE
//   completing at a1 (t = 0): m2 stored needs a2 <= 0.   INFEASIBLE
//   completing at a2 (t = 5): m1 alive needs a1 + 2 >= 5.INFEASIBLE
// The only subset {m1, m2} admits no read time: that branch counts
// as nothing. The timeout branch (empty at t0 + 10) remains.
//
// Expected: (1, 0).
// ---------------------------------------------------------------------

#[test]
fn disjoint_lifetimes_never_fill_min2_inbox() {
    let stats = run_min2_inbox(2, false);
    assert_eq!((stats.execs, stats.block), (1, 0));
}

// ---------------------------------------------------------------------
// 6c. Boundary companion: sd = 5 makes the lifetimes touch, [0, 5]
// and [5, 10]. The completing-at-a2 case (read exactly at t = 5, no
// storage slack for the completer) finds m1 still alive at its last
// legal instant:
//   immediate at t0 = 0:      a2 <= 0 is false.          INFEASIBLE
//   completing at a1 (t = 0): a2 <= 0 is false.          INFEASIBLE
//   completing at a2 (t = 5): a1 <= 5 <= a1 + 5, t <= 10.  FEASIBLE
// Subset branch + timeout branch.
//
// Expected: (2, 0).
// ---------------------------------------------------------------------

#[test]
fn touching_lifetimes_fill_min2_inbox() {
    let stats = run_min2_inbox(5, false);
    assert_eq!((stats.execs, stats.block), (2, 0));
}
