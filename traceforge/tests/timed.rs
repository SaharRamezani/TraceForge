//! Integration tests for the Must-τ extension.
//!
//! Each test exercises a small scenario where the timed feasibility
//! check (timed_consistent) must either admit or reject an interleaving.
//! Assertions are on the number of complete executions explored.
//!
//! Since Must-tau (2026-09-29) every send of a timed program may be
//! dropped (no budget), so each count below includes the worlds where
//! messages are lost, and no ending without a timeline is explored
//! (`timeline_impossible` stays 0). Where a count alone no longer
//! separates the property under test, the test asserts the multiset of
//! receive outcomes over the explored endings. Every new count was
//! cross-checked graph by graph against the pre-Must-tau exploration
//! with every send lossy.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

struct Collector(Arc<Mutex<Vec<String>>>);

impl ExecutionObserver for Collector {
    fn after(&mut self, _eid: ExecutionId, cond: &EndCondition, c: CoverageInfo) {
        let mut goals: Vec<String> = c.coverage.keys().cloned().collect();
        goals.sort();
        let tag = if matches!(cond, EndCondition::Deadlock) { "blocked " } else { "" };
        self.0.lock().unwrap().push(format!("{tag}{}", goals.join(" ")));
    }
}

/// Runs `f` under `builder` and returns its stats and the multiset of
/// `cover!` goals per explored ending (blocked endings prefixed).
fn outcomes<F>(builder: ConfigBuilder, f: F) -> (Stats, BTreeMap<String, usize>)
where
    F: Fn() + Send + Sync + 'static,
{
    let sink = Arc::new(Mutex::new(Vec::new()));
    let stats = traceforge::verify(
        builder.with_callback(Box::new(Collector(Arc::clone(&sink)))).build(),
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

// ---------------------------------------------------------------------
// Sleep advances the lower bound; both outcomes reachable
// ---------------------------------------------------------------------
//
// With L=0, U=1000, sd=0:
//   send window = [10, 1010], recv-reading-from-send window = [10, 100]
//   recv-timeout window                                    = [100, 100]
// Both the `rf = send` and `rf = ⊥` branches are timed consistent.
// Every send lossy: read and timeout beside the delivered send, plus
// the timeout with it dropped.
#[test]
fn sleep_advances_lower_bound_both_outcomes() {
    let (stats, out) = outcomes(Config::builder().with_timed(0, 1000, 0), || {
        let consumer = thread::spawn(|| {
            let got: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(100));
            cover!(format!("got={got:?}"));
        });
        traceforge::sleep(10);
        traceforge::send_msg(consumer.thread().id(), 42i32);
    });
    assert_eq!(out, expect(&[("got=None", 2), ("got=Some(42)", 1)]));
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (3, 0, 0));
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
// by the timed filter. Every send lossy: the timeout with the send
// delivered and with it dropped; the in-program assert pins the outcome.
#[test]
fn finite_wait_forces_timeout() {
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
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (2, 0, 0));
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
        Config::builder().with_timed(0, 10, 0).build(),
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
// timed filter.
// ---------------------------------------------------------------------
//
// Two senders each send one message; the consumer's predicate matches
// only the first sender. With generous timed bounds, the send from
// the matching sender is always timed consistent, while the
// other sender's message is simply filtered out by the predicate.
//
// Every send lossy. A dropped handoff leaves its sender blocked forever.
// Complete (both handoffs delivered): reply 1 delivered gives read or
// timeout (it may arrive after 50), dropped gives timeout, times reply
// 2 delivered or dropped: 3 x 2 = 6. Blocked: handoff 1 dropped gives a
// timeout times reply 2 (2); handoff 2 dropped gives the same 3 main
// outcomes (3); both dropped gives 1: 6. Reply 2 is never read.
#[test]
fn predicate_timed_recv() {
    let (stats, out) = outcomes(
        Config::builder().with_timed(0, 100, 0),
        || {
            let main_id = thread::current().id();
            let s1 = thread::spawn(move || {
                // Receive our "id handoff" from main so we know main's id.
                let _: i32 = traceforge::recv_msg_block_timed();
                traceforge::send_msg(main_id, 1i32);
            });
            let s2 = thread::spawn(move || {
                let _: i32 = traceforge::recv_msg_block_timed();
                traceforge::send_msg(main_id, 2i32);
            });
            let s1_id = s1.thread().id();
            traceforge::send_msg(s1_id, 0);
            traceforge::send_msg(s2.thread().id(), 0);

            let v: Option<i32> = traceforge::recv_tagged_msg_timed(
                move |tid: ThreadId, _tag| tid == s1_id,
                WaitTime::Finite(50),
            );
            cover!(format!("got={v:?}"));
        },
    );
    assert_eq!(
        out,
        expect(&[
            ("got=None", 4),
            ("got=Some(1)", 2),
            ("blocked got=None", 5),
            ("blocked got=Some(1)", 1),
        ])
    );
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (6, 6, 0));
}

// ---------------------------------------------------------------------
// Legacy run (no `with_timed`) is unchanged by the new code paths.
// ---------------------------------------------------------------------
//
// This is a regression test:
// when `config.timed` is `None`. A single send/recv should produce
// exactly one complete execution, just like it did before.
#[test]
fn legacy_run_unchanged_without_timed() {
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
// Legacy untimed receives inside a timed run are time-transparent.
// ---------------------------------------------------------------------
//
// `with_timed` is set, but the consumer uses the legacy
// `recv_msg_block` primitive. A program is either timed or untimed:
// the untimed receive is rejected at its first use (it used to pass
// through with no timed constraint, which left it exempt from
// eviction; see tests/api_families.rs).
#[test]
#[should_panic(expected = "TraceForge usage error")]
fn legacy_recv_inside_timed_is_rejected() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 10, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                // Legacy untimed receive: timed_consistent passes through.
                let v: i32 = traceforge::recv_msg_block();
                assert_eq!(v, 42);
            });
            // Sleep is still timed advancing main's local clock,
            // but the legacy recv has no wait constraint to violate.
            traceforge::sleep(1000);
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Per-send (L, U) overrides the globals.
// ---------------------------------------------------------------------
//
// Global L = U = 10 would force the send's window to [10, 10], which
// does not intersect the receiver's [0, 5] wait window, the pairing is
// rejected and only the timeout branch survives.
//
// With per-send L = U = 0 (via `send_msg_timed`), the send window
// collapses to [0, 0], inside [0, 5], so the receive must take it:
// since (C6') a timeout needs every candidate to miss the whole wait,
// and this one is readable at 0 in every timeline, `rf = ⊥` is gone
// and exactly one execution remains.
//
// The count alone does not separate the halves, so each asserts WHICH
// branches survived: the override reads the message, the fallback
// times out. That is the property this test was always about.
//
// Every send lossy: each half also has the timeout with the send
// dropped, so the override is {read, timeout} and the fallback is
// {timeout, timeout}.
#[test]
fn per_send_bounds_override_global() {
    let (stats_override, out_override) = outcomes(
        Config::builder().with_timed(10, 10, 0),
        || {
            let consumer = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                cover!(format!("got={v:?}"));
            });
            traceforge::send_msg_timed(consumer.thread().id(), 42i32, 0, 0);
        },
    );
    assert_eq!(out_override, expect(&[("got=None", 1), ("got=Some(42)", 1)]));
    assert_eq!(
        (stats_override.execs, stats_override.block, stats_override.timeline_impossible),
        (2, 0, 0)
    );

    let (stats_no_override, out_no_override) = outcomes(
        Config::builder().with_timed(10, 10, 0),
        || {
            let consumer = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                cover!(format!("got={v:?}"));
            });
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(out_no_override, expect(&[("got=None", 2)]));
    assert_eq!(
        (stats_no_override.execs, stats_no_override.block, stats_no_override.timeline_impossible),
        (2, 0, 0)
    );
}

// ---------------------------------------------------------------------
// Two concurrent sends with disjoint per-send (L, U) windows.
// ---------------------------------------------------------------------
//
// Admissible rf candidates: send_a only. send_b (window [20, 30])
// cannot be read inside the [0, 10] wait, and since (C6') the timeout
// cannot be taken either, because send_a arrives in [0, 5] and is
// readable inside the wait in every timeline. The in-program assert
// pins that the surviving execution is the send_a read, so the test
// still separates "b was pruned" from "everything was pruned".
//
// Every send lossy: read a (a delivered) or timeout (a dropped), times
// b delivered or dropped = 2 x 2. b is never read, and a timeout beside
// a delivered a would be a third `None` pair.
#[test]
fn per_send_distinct_windows_disjoint() {
    let (stats, out) = outcomes(
        Config::builder().with_timed(0, 100, 0),
        || {
            let consumer = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                cover!(format!("got={v:?}"));
            });
            let cid = consumer.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg_timed(cid, 1i32, 0, 5);
            });
            let _b = thread::spawn(move || {
                traceforge::send_msg_timed(cid, 2i32, 20, 30);
            });
        },
    );
    assert_eq!(out, expect(&[("got=None", 2), ("got=Some(1)", 2)]));
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (4, 0, 0));
}

// ---------------------------------------------------------------------
// Two concurrent sends with distinct but overlapping per-send windows.
// ---------------------------------------------------------------------
//
// Both reads survive: a at a_a in [0, 5], and b at a_b in [3, 8] in the
// timelines where a arrives after that read (a_a > a_b, e.g. 4 > 3).
// The timeout does not: each send is readable inside the [0, 10] wait
// in every timeline, so (C6') removes it. 3 became 2 on 2026-09-21.
//
// Every send lossy: read a (b delivered or dropped), read b (a
// delivered or dropped), and the timeout only when both are dropped:
// 2 + 2 + 1.
#[test]
fn per_send_distinct_windows_overlap() {
    let (stats, out) = outcomes(
        Config::builder().with_timed(0, 100, 0),
        || {
            let consumer = thread::spawn(|| {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                cover!(format!("got={v:?}"));
            });
            let cid = consumer.thread().id();
            let _a = thread::spawn(move || {
                traceforge::send_msg_timed(cid, 1i32, 0, 5);
            });
            let _b = thread::spawn(move || {
                traceforge::send_msg_timed(cid, 2i32, 3, 8);
            });
        },
    );
    assert_eq!(
        out,
        expect(&[("got=None", 1), ("got=Some(1)", 2), ("got=Some(2)", 2)])
    );
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (5, 0, 0));
}

// ---------------------------------------------------------------------
// Per-node sd overrides the global storage delay.
// ---------------------------------------------------------------------
//
// With main's sd = 10 the message (arrival in [0, 5]) is still stored
// at t = 10, so the point read takes it; without the override it is a
// corpse by then and only the timeout remains. Since (C6') each half
// has exactly ONE execution with the send delivered, so, as in
// per_send_bounds_override_global, each asserts which branches survived
// rather than counting branches.
//
// Every send lossy: each half also has the timeout with the send
// dropped, so the override is {read, timeout} and the fallback is
// {timeout, timeout}.
#[test]
fn per_node_sd_overrides_global() {
    let (stats, out) = outcomes(
        Config::builder()
            .with_timed(0, 5, 0)
            .with_node_sd(traceforge::thread::main_thread_id(), 10),
        || {
            let main_id = thread::current().id();
            let _p = thread::spawn(move || {
                traceforge::send_msg(main_id, 42i32);
            });
            traceforge::sleep(10);
            let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(0));
            cover!(format!("got={v:?}"));
        },
    );
    assert_eq!(out, expect(&[("got=None", 1), ("got=Some(42)", 1)]));
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (2, 0, 0));

    // Same scenario without the per-node override: the read is
    // rejected, only the timeout branch survives (delivered or dropped).
    let (stats_fallback, out_fallback) = outcomes(
        Config::builder().with_timed(0, 5, 0),
        || {
            let main_id = thread::current().id();
            let _p = thread::spawn(move || {
                traceforge::send_msg(main_id, 42i32);
            });
            traceforge::sleep(10);
            let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(0));
            cover!(format!("got={v:?}"));
        },
    );
    assert_eq!(out_fallback, expect(&[("got=None", 2)]));
    assert_eq!(
        (stats_fallback.execs, stats_fallback.block, stats_fallback.timeline_impossible),
        (2, 0, 0)
    );
}

// ---------------------------------------------------------------------
// Sleep is per-thread: it does not leak into parallel threads'
// timed windows.
// ---------------------------------------------------------------------
//
// Every send lossy: the read (delivered) and the timeout (dropped). A
// leaked sleep would give two timeouts instead.
#[test]
fn sleep_is_per_thread() {
    let (stats, out) = outcomes(
        Config::builder().with_timed(0, 0, 0),
        || {
            let b = thread::spawn(|| {
                // Short wait; if A's sleep leaked in, B's wait would
                // start at 1_000_000, the message (arrival 0, sd = 0)
                // would be long dead, and the ONLY surviving branch
                // would be the timeout. Since (C6') the correct
                // behaviour also has exactly one execution (the read,
                // because a readable message forbids the timeout), so
                // the count no longer separates the two: assert the
                // value instead.
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                cover!(format!("got={v:?}"));
            });
            let _a = thread::spawn(|| {
                traceforge::sleep(1_000_000);
            });
            traceforge::send_msg(b.thread().id(), 99i32);
        },
    );
    assert_eq!(out, expect(&[("got=None", 1), ("got=Some(99)", 1)]));
    assert_eq!((stats.execs, stats.block, stats.timeline_impossible), (2, 0, 0));
}

// ---------------------------------------------------------------------
// 3-thread relay pipeline: main -> t1 -> t2 -> main, repeated 3 times.
// 9 sends + 9 receives, every receive blocking with infinite wait, and
// generous timed bounds. The pipeline structure forces a unique
// rf-mapping (each receive has only one upstream sender at a time), so
// timed pruning must accept exactly one execution.
// ---------------------------------------------------------------------
#[test]
fn relay_pipeline_three_threads_blocking() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 50, 0).build(),
        || {
            let main_id = thread::current().id();
            let t2 = thread::spawn(move || {
                for _ in 0..3 {
                    let v: i32 = traceforge::recv_msg_block_timed();
                    traceforge::send_msg(main_id, v + 100);
                }
            });
            let t2_id = t2.thread().id();
            let t1 = thread::spawn(move || {
                for _ in 0..3 {
                    let v: i32 = traceforge::recv_msg_block_timed();
                    traceforge::send_msg(t2_id, v + 10);
                }
            });
            let t1_id = t1.thread().id();
            for i in 0..3 {
                traceforge::send_msg(t1_id, i);
                let _v: i32 = traceforge::recv_msg_block_timed();
            }
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// Star topology, 5 threads (1 hub + 4 workers).
// Hub sends one tagged message to each worker (4 sends), then each
// worker replies once with a uniquely tagged message (4 sends), and the
// hub collects them via tagged blocking receives that match by tag
// (4 receives) on top of the workers' 4 receives. 8 sends + 8 receives.
//
// Tag-uniqueness forces a single rf-mapping per receive, so the only
// remaining freedom is scheduling of the 4 concurrent worker replies.
// The timed filter must not reject this fully consistent scenario.
// ---------------------------------------------------------------------
#[test]
fn star_hub_with_workers_tagged() {
    let stats = traceforge::verify(
        Config::builder().with_timed(0, 50, 0).build(),
        || {
            let main_id = thread::current().id();
            let mut worker_ids = Vec::new();
            for w in 0..4u32 {
                let h = thread::spawn(move || {
                    let v: i32 =
                        traceforge::recv_tagged_msg_block_timed(move |_tid, tag| tag == Some(w));
                    traceforge::send_tagged_msg(main_id, 100 + w, v + 1);
                });
                worker_ids.push(h.thread().id());
            }
            for (w, wid) in worker_ids.iter().enumerate() {
                traceforge::send_tagged_msg(*wid, w as u32, w as i32);
            }
            for w in 0..4u32 {
                let _v: i32 =
                    traceforge::recv_tagged_msg_block_timed(move |_tid, tag| tag == Some(100 + w));
            }
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// 4-thread pipeline where timed pruning genuinely matters.
// Same pipeline shape as the relay test (main -> a -> b -> c -> main,
// 2 rounds = 8 sends + 8 receives) but the consumer-side receives use
// `recv_msg_timed` with a finite wait, and there is a sleep in front
// of every send. With permissive bounds (run_loose) all `rf=send`
// pairings are admissible AND the timeout branch is also admissible
// at each receive, so the explored exec count is strictly larger than
// the variant with tight bounds (run_tight) where the timeout branches
// for the early receives are pruned.
// ---------------------------------------------------------------------
#[test]
fn four_thread_pipeline_timed_pruning() {
    fn run(global_u: u64, wait_ns: u64) -> usize {
        let stats = traceforge::verify(
            Config::builder().with_timed(0, global_u, 0).build(),
            move || {
                let main_id = thread::current().id();
                let c = thread::spawn(move || {
                    for _ in 0..2 {
                        let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                        if let Some(v) = v {
                            traceforge::sleep(1);
                            traceforge::send_msg(main_id, v + 1000);
                        }
                    }
                });
                let c_id = c.thread().id();
                let b = thread::spawn(move || {
                    for _ in 0..2 {
                        let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                        if let Some(v) = v {
                            traceforge::sleep(1);
                            traceforge::send_msg(c_id, v + 100);
                        }
                    }
                });
                let b_id = b.thread().id();
                let a = thread::spawn(move || {
                    for _ in 0..2 {
                        let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                        if let Some(v) = v {
                            traceforge::sleep(1);
                            traceforge::send_msg(b_id, v + 10);
                        }
                    }
                });
                let a_id = a.thread().id();
                for i in 0..2 {
                    traceforge::sleep(1);
                    traceforge::send_msg(a_id, i);
                    let _v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                }
            },
        );
        // Every ending completes; none is timeline-impossible.
        assert_eq!((stats.block, stats.timeline_impossible), (0, 0));
        stats.execs
    }

    let loose = run(50, 50);
    let tight = run(2, 2);
    // The exact numbers below are the observed
    // exploration counts; they should drop in lockstep if the pruning is
    // strengthened, and divergence here means the timed filter changed
    // shape and the counts should be re-baselined.
    // Re-baselined 2026-08-28 (was loose 38, tight 22; now 48, 23): dead-front
    // unsealing legitimately adds classes where a receive whose wait
    // starts late skips a front that arrived and died (sd = 0 makes
    // instant corpses) before the wait began, then reads the next
    // message; the old offer sealed those worlds behind the possibly
    // readable front. The loose > tight pruning relation still holds.
    //
    // Re-baselined again 2026-09-21 (was 48, 23; now 38, 21) for (C6'):
    // a relay stage may time out only where the upstream message really
    // can miss its whole wait, so the worlds where a stage timed out
    // while its input sat readable are gone. Both sides drop and the
    // loose > tight relation still holds, which is what this test pins.
    //
    // Re-baselined 2026-09-30 (was 38, 21; now 130, 76) for Must-tau:
    // every send lossy adds the worlds where a relay message is dropped
    // (its receive times out); no ending is timeline-impossible. Both
    // sets match the pre-Must-tau exploration with every send lossy
    // graph for graph, and loose > tight still holds.
    assert_eq!((loose, tight), (130, 76));
}
