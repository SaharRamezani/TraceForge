//! Temporal consistency
//!
//! This module implements the temporal extension of the Must algorithm.
//! It provides the [`WaitTime`] user-facing wait type, the [`TemporalConfig`]
//! that holds the global transit bounds (`L`, `U`) and storage delay (`sd`)
//! (with optional per-node `sd` overrides), and the [`tconsistent`] walker
//! that computes the feasible time window `[τ_lo, τ_hi]` for a given event
//! by recursing over the program order.
//!
//! Per-send `L` / `U` overrides live on the `SendMsg` label itself
//! (see [`crate::event_label::SendMsg`]). When a send does not carry its
//! own bounds, the walker falls back to the globals on `TemporalConfig`.
//!
//! The module is independent of the structural consistency check in
//! [`crate::cons`]. If the config's `temporal` field is `None`, none
//! of this code runs and legacy behaviour is preserved.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::event::Event;
use crate::event_label::LabelEnum;
use crate::exec_graph::ExecutionGraph;
use crate::thread::ThreadId;

/// Per-receive wait time `W_r` from the Must-τ algorithm.
///
/// `Finite(w)` is a concrete timeout in time units; `Infinite` corresponds to
/// `+∞` which is a blocking receive that must be paired with a
/// matching send. Timeout (`rf = ⊥`) is inadmissible when `W_r = +∞`.
///
/// `WaitTime` is *not* `Option<u64>`: `Option<u64>` would conflate
/// "no wait specified / legacy untimed receive" with "infinite wait".
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitTime {
    Finite(u64),
    Infinite,
}

/// Temporal parameters.
///
/// The fields `l`, `u`, `sd` are *defaults* used for any send / node that
/// does not carry its own override:
///
/// * `l`, `u`: fallback transit-time bounds used when a send event
///   does not specify its own `L` / `U` (e.g. when the model uses the
///   untimed [`crate::send_msg`] instead of [`crate::send_msg_timed`]).
/// * `sd`: fallback storage delay used for any node whose thread id is
///   not present in `node_sd`.
///
/// Per-node storage-delay overrides live in `node_sd`, keyed by the
/// *destination* thread's id.
///
/// A run with `temporal = None` on the parent [`crate::Config`] is a
/// legacy verification and this struct is ignored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalConfig {
    pub l: u64,
    pub u: u64,
    pub sd: u64,
    /// Per-destination-thread storage-delay overrides `sd(q)`.
    /// Threads not present here use the global `sd`.
    #[serde(default)]
    pub node_sd: HashMap<ThreadId, u64>,
}

impl TemporalConfig {
    pub fn new(l: u64, u: u64, sd: u64) -> Self {
        assert!(l <= u, "TemporalConfig requires L <= U");
        Self {
            l,
            u,
            sd,
            node_sd: HashMap::new(),
        }
    }

    /// Override the storage delay for destination thread `tid`.
    /// Chainable on the builder.
    pub fn with_node_sd(mut self, tid: ThreadId, sd: u64) -> Self {
        self.node_sd.insert(tid, sd);
        self
    }

    /// Storage delay `sd(q)` for destination thread `q`.
    /// Falls back to the global `sd` when `q` has no per-node override.
    pub fn sd_for(&self, tid: ThreadId) -> u64 {
        self.node_sd.get(&tid).copied().unwrap_or(self.sd)
    }
}

/// A feasible time window `[lo, hi]` attached to an event.
/// An interval is empty iff `lo > hi`. `u64::MAX` is used internally
/// as the `+∞` sentinel in intermediate computations (saturating add
/// keeps it from wrapping), and is never exposed to the user.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct TimeInterval {
    pub lo: u64,
    pub hi: u64,
}

impl TimeInterval {
    pub(crate) fn new(lo: u64, hi: u64) -> Self {
        Self { lo, hi }
    }

    pub(crate) fn empty() -> Self {
        // Any (lo > hi) works; pick a pair that's obviously empty.
        Self { lo: 1, hi: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lo > self.hi
    }
}

/// Compute the timestamp range `[τ_lo(e), τ_hi(e)]` for event `e` by
/// walking the program order backwards and dispatching on the
/// label of each event.
///
/// The walk stops at each thread's first event (`Begin`), returning
/// `[0, 0]`.
pub(crate) fn tconsistent(
    g: &ExecutionGraph,
    e: Event,
    cfg: &TemporalConfig,
) -> TimeInterval {
    let mut cache = HashMap::new();
    tconsistent_rec(g, e, cfg, &mut cache)
}

fn tconsistent_rec(
    g: &ExecutionGraph,
    e: Event,
    cfg: &TemporalConfig,
    cache: &mut HashMap<Event, TimeInterval>,
) -> TimeInterval {
    if let Some(cached) = cache.get(&e) {
        return *cached;
    }

    // Base case: thread's first event (Begin) starts the local clock at 0.
    // po connects init to every thread's first event, so
    // recursion stops here and we do not walk across TCreate edges.
    if e.index == 0 {
        let iv = TimeInterval::new(0, 0);
        cache.insert(e, iv);
        return iv;
    }

    let pred = Event::new(e.thread, e.index - 1);
    let pred_iv = tconsistent_rec(g, pred, cfg, cache);

    let lab = g.label(e);
    let iv = match lab {
        LabelEnum::Sleep(slab) => {
            let d = slab.duration();
            TimeInterval::new(
                pred_iv.lo.saturating_add(d),
                pred_iv.hi.saturating_add(d),
            )
        }
        LabelEnum::RecvMsg(rlab) => {
            // Legacy (untimed) receive: it contributes
            // no constraint at all, neither from its own wait time nor
            // from the send it reads from. Mixed-mode tests rely on this.
            let Some(wait) = rlab.wait() else {
                cache.insert(e, pred_iv);
                return pred_iv;
            };
            match rlab.rf() {
                // Receive reading from a send.
                Some(s) => {
                    if pred_iv.is_empty() {
                        cache.insert(e, TimeInterval::empty());
                        return TimeInterval::empty();
                    }
                    let send_iv = tconsistent_rec(g, s, cfg, cache);
                    if send_iv.is_empty() {
                        cache.insert(e, TimeInterval::empty());
                        return TimeInterval::empty();
                    }
                    // hi_cap = τ_hi(e') + W_r
                    let hi_cap = match wait {
                        WaitTime::Finite(w) => pred_iv.hi.saturating_add(w),
                        WaitTime::Infinite => u64::MAX,
                    };
                    // Per-send L / U overrides; fall back to globals.
                    let (l_val, u_val) = g
                        .send_label(s)
                        .and_then(|slab| slab.transit())
                        .map(|(l, u)| (l, u))
                        .unwrap_or((cfg.l, cfg.u));
                    // sd(dst(s)): destination of s is the receiver's thread.
                    let sd_val = cfg.sd_for(e.thread);

                    // send upper bound = τ_hi(s) + U + sd
                    let send_hi_with_bounds = send_iv
                        .hi
                        .saturating_add(u_val)
                        .saturating_add(sd_val);
                    // send lower bound = τ_lo(s) + L
                    let send_lo_with_bounds = send_iv.lo.saturating_add(l_val);

                    let lo = pred_iv.lo.max(send_lo_with_bounds);
                    let hi = hi_cap.min(send_hi_with_bounds);
                    TimeInterval::new(lo, hi)
                }
                // Receive timed out (rf = ⊥).
                None => match wait {
                    WaitTime::Finite(w) => TimeInterval::new(
                        pred_iv.lo.saturating_add(w),
                        pred_iv.hi.saturating_add(w),
                    ),
                    // Infinite-wait with rf = ⊥ should have been pruned by
                    // the `VisitIfConsistent(G, r=⊥) ∧ W_r = +∞ → return`
                    // short-circuit. If we somehow get here, return an
                    // empty interval so the caller drops the graph.
                    WaitTime::Infinite => TimeInterval::empty(),
                },
            }
        }
        // Any other label: pass through unchanged.
        _ => pred_iv,
    };

    cache.insert(e, iv);
    iv
}
