//! Timed types: [`WaitTime`], [`TimedConfig`] (global transit bounds
//! `L`, `U` and storage delay `sd`, with per-node `sd` overrides), and
//! the [`TimeInterval`] window type used by reporting/prune-log paths.
//!
//! The interval WALKER that used to live here (per-event `[lo, hi]`
//! chain propagation) was deleted after the exact difference-constraint
//! engine ([`crate::timed_dcs`]) took over every feasibility decision,
//! including the inbox case split; it survives in the pre-timed-exact
//! branch history for A/B comparisons.
//!
//! If the config's `timed` field is `None`, no timed code runs and
//! legacy behaviour is preserved.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::thread::ThreadId;

/// Per-receive wait time `W_r` from the Must-τ algorithm.
///
/// `Finite(w)` is a concrete timeout in time units; `Infinite` corresponds to
/// `+∞` which is a blocking receive that must be paired with a
/// matching send. Timeout (`rf = ⊥`) is inadmissible when `W_r = +∞`
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitTime {
    Finite(u64),
    Infinite,
}

/// Timed parameters.
///
/// The fields `l`, `u`, `sd` are defaults used for any send / node that
/// does not carry its own override:
///
/// * `l`, `u`: fallback transit-time bounds used when a send event
///   does not specify its own `L` / `U` (e.g. when the model uses the
///   untimed [`crate::send_msg`] instead of [`crate::send_msg_timed`]).
/// * `sd`: fallback storage delay used for any node whose thread id is
///   not present in `node_sd`.
///
/// Per-node storage-delay overrides live in `node_sd`, keyed by the
/// destination thread's id.
///
/// A run with `timed = None` on the parent [`crate::Config`] is a
/// legacy verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimedConfig {
    pub l: u64,
    pub u: u64,
    pub sd: u64,
    /// Per-destination-thread storage-delay overrides `sd(q)`.
    /// Threads not present here use the global `sd`.
    #[serde(default)]
    pub node_sd: HashMap<ThreadId, u64>,
}

impl TimedConfig {
    pub fn new(l: u64, u: u64, sd: u64) -> Self {
        assert!(l <= u, "TimedConfig requires L <= U");
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
    pub(crate) fn empty() -> Self {
        // Any (lo > hi) works; pick a pair that's obviously empty.
        Self { lo: 1, hi: 0 }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.lo > self.hi
    }
}
