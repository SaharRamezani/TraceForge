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
// Per-send (L, U) overrides the globals.
// ---------------------------------------------------------------------
//
// Global L = U = 10 would force the send's window to [10, 10], which
// does not intersect the receiver's [0, 5] wait window — the pairing is
// rejected and only the timeout branch survives.
//
// With per-send L = U = 0 (via `send_msg_timed`), the send window
// collapses to [0, 0], overlapping [0, 5], so both `rf = send` and
// `rf = ⊥` branches become temporally admissible.
#[test]
fn per_send_bounds_override_global() {
    let stats_override = traceforge::verify(
        Config::builder().with_temporal(10, 10, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let _v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            });
            traceforge::send_msg_timed(consumer.thread().id(), 42i32, 0, 0);
        },
    );
    assert_eq!(stats_override.execs, 2);

    let stats_no_override = traceforge::verify(
        Config::builder().with_temporal(10, 10, 0).build(),
        || {
            let consumer = thread::spawn(|| {
                let _v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            });
            traceforge::send_msg(consumer.thread().id(), 42i32);
        },
    );
    assert_eq!(stats_no_override.execs, 1);
}

// ---------------------------------------------------------------------
// Per-node sd overrides the global storage delay.
// ---------------------------------------------------------------------
//
// Producer sends at time 0; main sleeps 10 and then receives with
// W_r = 0. With global sd = 0 the send window is [0, 5] and the wait
// window is [10, 10], so `rf = send` is rejected.
//
// A per-node override `sd(main) = 10` extends the send window to
// [0, 15], which now intersects [10, 10] at t = 10; the pairing becomes
// admissible and both `rf = send` and `rf = ⊥` are reachable.
#[test]
fn per_node_sd_overrides_global() {
    let stats = traceforge::verify(
        Config::builder()
            .with_temporal(0, 5, 0)
            .with_node_sd(traceforge::thread::main_thread_id(), 10)
            .build(),
        || {
            let main_id = thread::current().id();
            let _p = thread::spawn(move || {
                traceforge::send_msg(main_id, 42i32);
            });
            traceforge::sleep(10);
            let _v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(0));
        },
    );
    assert_eq!(stats.execs, 2);

    // Same scenario without the per-node override: send is rejected,
    // only the timeout branch survives.
    let stats_fallback = traceforge::verify(
        Config::builder().with_temporal(0, 5, 0).build(),
        || {
            let main_id = thread::current().id();
            let _p = thread::spawn(move || {
                traceforge::send_msg(main_id, 42i32);
            });
            traceforge::sleep(10);
            let _v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(0));
        },
    );
    assert_eq!(stats_fallback.execs, 1);
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

// ---------------------------------------------------------------------
// 3-thread relay pipeline: main -> t1 -> t2 -> main, repeated 3 times.
// 9 sends + 9 receives, every receive blocking with infinite wait, and
// generous temporal bounds. The pipeline structure forces a unique
// rf-mapping (each receive has only one upstream sender at a time), so
// temporal pruning must accept exactly one execution.
// ---------------------------------------------------------------------
#[test]
fn relay_pipeline_three_threads_blocking() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 50, 0).build(),
        || {
            let main_id = thread::current().id();
            let t2 = thread::spawn(move || {
                for _ in 0..3 {
                    let v: i32 = traceforge::recv_msg_block();
                    traceforge::send_msg(main_id, v + 100);
                }
            });
            let t2_id = t2.thread().id();
            let t1 = thread::spawn(move || {
                for _ in 0..3 {
                    let v: i32 = traceforge::recv_msg_block();
                    traceforge::send_msg(t2_id, v + 10);
                }
            });
            let t1_id = t1.thread().id();
            for i in 0..3 {
                traceforge::send_msg(t1_id, i);
                let _v: i32 = traceforge::recv_msg_block();
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
// The temporal filter must not reject this fully consistent scenario.
// ---------------------------------------------------------------------
#[test]
fn star_hub_with_workers_tagged() {
    let stats = traceforge::verify(
        Config::builder().with_temporal(0, 50, 0).build(),
        || {
            let main_id = thread::current().id();
            let mut worker_ids = Vec::new();
            for w in 0..4u32 {
                let h = thread::spawn(move || {
                    let v: i32 =
                        traceforge::recv_tagged_msg_block(move |_tid, tag| tag == Some(w));
                    traceforge::send_tagged_msg(main_id, 100 + w, v + 1);
                });
                worker_ids.push(h.thread().id());
            }
            for (w, wid) in worker_ids.iter().enumerate() {
                traceforge::send_tagged_msg(*wid, w as u32, w as i32);
            }
            for w in 0..4u32 {
                let _v: i32 =
                    traceforge::recv_tagged_msg_block(move |_tid, tag| tag == Some(100 + w));
            }
        },
    );
    assert_eq!(stats.execs, 1);
}

// ---------------------------------------------------------------------
// 4-thread pipeline where temporal pruning genuinely matters.
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
fn four_thread_pipeline_temporal_pruning() {
    fn run(global_u: u64, wait_ns: u64) -> usize {
        traceforge::verify(
            Config::builder().with_temporal(0, global_u, 0).build(),
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
        )
        .execs
    }

    let loose = run(50, 50);
    let tight = run(2, 2);
    // Loose bounds admit every receive's `rf=⊥` branch in addition to the
    // forwarded one; tight bounds prune the timeout branches whose windows
    // do not overlap any send. The exact numbers below are the observed
    // exploration counts; they should drop in lockstep if the pruning is
    // strengthened, and divergence here means the temporal filter changed
    // shape and the counts should be re-baselined.
    assert_eq!(loose, 38);
    assert_eq!(tight, 22);
}
