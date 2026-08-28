//! Exact timed feasibility via difference constraints.
//!
//! The interval walker in [`crate::timed_cons`] propagates one `[lo, hi]`
//! pair per event and judges reads by endpoint overlap. That is a
//! relaxation: two constraint chains may implicitly assign different
//! times to a shared ancestor event, so endpoint overlap can accept
//! graphs that no single timeline realizes (spurious counterexamples).
//!
//! This module answers the exact question instead: does one assignment
//! of a time to every event satisfy ALL timing constraints at once?
//! Every timing rule of the semantics is a difference constraint
//! `t_a - t_b <= c`, so satisfiability is equivalent to the absence of
//! a negative cycle in the constraint graph (Bellman-Ford / SPFA), and
//! a solution doubles as a concrete witness timeline.
//!
//! Guarantees relied upon by the exploration:
//! * soundness: a graph is admitted iff a witness timeline exists, so
//!   reported counterexamples are realizable (and printable with
//!   concrete timestamps);
//! * completeness: a candidate is pruned iff its constraint system is
//!   unsatisfiable; constraints only accumulate along a branch, so an
//!   unsatisfiable prefix has no realizable extension;
//! * maximal pruning: any filter pruning more than unsatisfiable
//!   prefixes would drop a realizable execution.
//!
//! Scope notes:
//! * The `rf = None` timeout branch stays always-explorable; a timeout
//!   is an exact clock advance here, exactly as in the walker.
//! * Arrivals are EXPLICIT variables (2026-08-28): every non-dropped
//!   send s carries `a_s` in `[t_s + L, t_s + U]`, a message has ONE
//!   arrival consistent across every constraint that mentions it, and
//!   same-channel delivered sends arrive in their delivery-model
//!   (`sb`) order; overtaking is a Bag/NoOrder-only behavior. A graph
//!   whose transit overrides contradict FIFO admits no timeline at
//!   all (a real verdict: such branches count as nothing).
//! * Waited-inbox reads return at `max(t0, completing arrival)`: a
//!   disjunction over which member's arrival completed the batch
//!   (`t_e = a_k` exactly for the completer, no sd slack); committed
//!   reads carry one edge-set alternative per disjunct, probes fold
//!   their case list into a transient group; feasibility searches
//!   over one case per read (CDCL-lite with trail commits).
//!
//! The oracle is deliberately stateless across graph mutations: it is
//! built per filtering batch from the graph (the single source of
//! truth) and dropped before the next mutation. Probes never mutate
//! the graph and always restore the solver's potentials, so a batch
//! may probe any number of candidates against one base system.

use std::collections::{HashMap, VecDeque};

use crate::event::Event;
use crate::event_label::{AsEventLabel, LabelEnum, RecvMsg};
use crate::exec_graph::ExecutionGraph;
use crate::timed_cons::{TimedConfig, WaitTime};
use crate::vector_clock::VectorClock;

/// Signed time for difference constraints (`t_r >= t_s + L` is the
/// edge weight `-L`). u64-scale durations fit with a wide margin:
/// |weight| < 2^66 and any shortest-path label is bounded by
/// #vars * max|weight| < 2^76, far below `i128` limits.
pub(crate) type Time = i128;

/// One difference constraint `t[to] - t[from] <= w`, stored as
/// `(from, to, w)` over interned variable indices.
type Edge = (u32, u32, Time);

const ROOT: u32 = 0;

/// Interned variables of one oracle scope: `ev` maps each event to its
/// timestamp variable, `arr` maps each non-dropped send to its arrival
/// variable (`a_s`, the instant the message reaches the receiving
/// node; storage lifetime is `[a_s, a_s + sd]`).
pub(crate) struct ScopeVars {
    ev: HashMap<Event, u32>,
    arr: HashMap<Event, u32>,
}

// =====================================================================
// DcsCore: pure difference-constraint solver (graph-independent)
// =====================================================================

/// Outcome of a conflict-extraction run: either the system
/// base + extra is satisfiable after all, or it is not and the
/// returned indices (into `extra`) participate in one negative
/// cycle certifying it. An EMPTY index list means the cycle uses
/// base edges only: the base system itself is infeasible and no
/// choice of extras can matter.
pub(crate) enum Conflict {
    Feasible,
    Infeasible(Vec<usize>),
}

/// A difference-constraint system with maintained potentials.
///
/// `pi` is a feasible assignment (`pi[to] <= pi[from] + w` for every
/// edge) whenever `feasible` is true; it IS a witness timeline up to a
/// constant shift.
#[derive(Clone)]
pub(crate) struct DcsCore {
    n: usize,
    edges: Vec<Edge>,
    /// Outgoing edge ids per `from` variable.
    adj: Vec<Vec<u32>>,
    pi: Vec<Time>,
    feasible: bool,
    /// Probe scratch (reused across probes to avoid per-probe allocation).
    scratch_pi: Vec<Time>,
    scratch_inq: Vec<bool>,
    scratch_cnt: Vec<u32>,
    scratch_parent: Vec<u32>,
    scratch_pedge: Vec<u32>,
}

impl DcsCore {
    pub(crate) fn new(n: usize, edges: Vec<Edge>) -> Self {
        let mut adj = vec![Vec::new(); n];
        for (i, &(from, _, _)) in edges.iter().enumerate() {
            adj[from as usize].push(i as u32);
        }
        let mut core = Self {
            n,
            edges,
            adj,
            pi: Vec::new(),
            feasible: false,
            scratch_pi: Vec::new(),
            scratch_inq: vec![false; n],
            scratch_cnt: vec![0; n],
            scratch_parent: vec![u32::MAX; n],
            scratch_pedge: vec![u32::MAX; n],
        };
        core.solve();
        core
    }

    pub(crate) fn feasible(&self) -> bool {
        self.feasible
    }

    /// Full solve from scratch: SPFA seeded with all-zero potentials
    /// (equivalent to a virtual source with 0-edges to every var). A
    /// variable relaxed more than `n` times witnesses a negative cycle.
    fn solve(&mut self) {
        let n = self.n;
        self.pi = vec![0; n];
        let mut inq = vec![true; n];
        let mut cnt = vec![0u32; n];
        let mut queue: VecDeque<u32> = (0..n as u32).collect();
        while let Some(u) = queue.pop_front() {
            inq[u as usize] = false;
            for &eid in &self.adj[u as usize] {
                let (_, v, w) = self.edges[eid as usize];
                let cand = self.pi[u as usize] + w;
                if cand < self.pi[v as usize] {
                    self.pi[v as usize] = cand;
                    // Negative-cycle bound: count ENQUEUES, not
                    // relaxations. Without a negative cycle a vertex
                    // enters the queue at most n times (one per
                    // Bellman-Ford phase); a vertex may legally be
                    // relaxed several times within one phase.
                    if !inq[v as usize] {
                        cnt[v as usize] += 1;
                        if cnt[v as usize] > n as u32 {
                            self.feasible = false;
                            return;
                        }
                        inq[v as usize] = true;
                        queue.push_back(v);
                    }
                }
            }
        }
        self.feasible = true;
    }

    /// Exact satisfiability of `base edges + extra`, restoring `pi`
    /// afterwards (pure predicate). Starting from feasible potentials,
    /// only additions can violate anything, so the repair is seeded
    /// from the violated extras and label-corrects; a variable relaxed
    /// more than `n` times can only happen on a negative cycle.
    pub(crate) fn probe(&mut self, extra: &[Edge]) -> bool {
        self.repair(extra, false)
    }

    /// Like [`Self::probe`] but on success KEEPS the repaired
    /// potentials and appends `extra` to the system (they become base
    /// edges for subsequent probes/commits: necessary, or a later
    /// repair could silently re-violate them). The caller must record
    /// `edges.len()` beforehand and pass the pre-commit potentials to
    /// [`Self::rollback`] to undo. On failure, state is untouched.
    pub(crate) fn commit(&mut self, extra: &[Edge], saved_pi: &mut Vec<Time>) -> bool {
        saved_pi.clear();
        saved_pi.extend_from_slice(&self.pi);
        if !self.repair(extra, true) {
            return false;
        }
        let base = self.edges.len();
        for (k, &e) in extra.iter().enumerate() {
            self.adj[e.0 as usize].push((base + k) as u32);
            self.edges.push(e);
        }
        true
    }

    /// Undo a successful [`Self::commit`]: `mark` is `edges.len()`
    /// from before the commit, `saved_pi` the potentials it saved.
    /// Commits must be rolled back in LIFO order.
    pub(crate) fn rollback(&mut self, mark: usize, saved_pi: &[Time]) {
        while self.edges.len() > mark {
            let (from, _, _) = self.edges.pop().expect("rollback past empty");
            let popped = self.adj[from as usize].pop();
            debug_assert_eq!(popped, Some(self.edges.len() as u32));
        }
        self.pi.copy_from_slice(saved_pi);
    }

    fn repair(&mut self, extra: &[Edge], commit: bool) -> bool {
        if !self.feasible {
            return false;
        }
        // Reuse scratch buffers: probes run in tight per-candidate loops.
        self.scratch_pi.clear();
        self.scratch_pi.extend_from_slice(&self.pi);
        self.scratch_inq.fill(false);
        self.scratch_cnt.fill(0);
        self.scratch_parent.fill(u32::MAX);
        self.scratch_pedge.fill(u32::MAX);
        let n = self.n;
        let nb = self.edges.len();
        let mut inq = std::mem::take(&mut self.scratch_inq);
        let mut cnt = std::mem::take(&mut self.scratch_cnt);
        let mut parent = std::mem::take(&mut self.scratch_parent);
        let mut pedge = std::mem::take(&mut self.scratch_pedge);
        let mut queue: VecDeque<u32> = VecDeque::new();
        let mut ok = true;
        // Rejections are decided FAST by parent-graph cycle detection:
        // once any vertex's enqueue count passes a small threshold (a
        // near-certain infeasibility tell; feasible repairs rarely
        // enqueue anything twice), every improvement checks whether
        // the relaxed edge closes a cycle in the parent pointers and
        // verifies that cycle's weight. Without this, an infeasible
        // probe only terminates via the O(n*m) enqueue-count bound,
        // which profiling showed dominating the case search (rejected
        // probes are the searcher's common case). The count bound
        // stays as the unconditional backstop, so the verdict is
        // exact regardless of what the walk finds.
        let mut deep = false;
        const DEEP_AT: u32 = 3;
        // Bucket large extra slices by `from` so the per-dequeue scan
        // below stays cheap; tiny slices keep the allocation-free scan.
        // Bucket entries carry the provenance id (nb + extra index).
        let extra_by_from: Option<HashMap<u32, Vec<(u32, Time, u32)>>> = if extra.len() > 16 {
            let mut m: HashMap<u32, Vec<(u32, Time, u32)>> = HashMap::new();
            for (i, &(from, v, w)) in extra.iter().enumerate() {
                m.entry(from).or_default().push((v, w, (nb + i) as u32));
            }
            Some(m)
        } else {
            None
        };
        for (i, &(u, v, w)) in extra.iter().enumerate() {
            let cand = self.pi[u as usize] + w;
            if cand < self.pi[v as usize] {
                self.pi[v as usize] = cand;
                parent[v as usize] = u;
                pedge[v as usize] = (nb + i) as u32;
                if !inq[v as usize] {
                    cnt[v as usize] += 1;
                    inq[v as usize] = true;
                    queue.push_back(v);
                }
            }
        }
        'repair: while let Some(u) = queue.pop_front() {
            inq[u as usize] = false;
            macro_rules! relax {
                ($v:expr, $w:expr, $ei:expr) => {{
                    let v = $v;
                    let w = $w;
                    let cand = self.pi[u as usize] + w;
                    if cand < self.pi[v as usize] {
                        self.pi[v as usize] = cand;
                        parent[v as usize] = u;
                        pedge[v as usize] = $ei;
                        if deep {
                            // Does u's ancestor chain contain v? Then
                            // (u -> v) closes a parent-graph cycle;
                            // trust it only after verifying its weight
                            // (stale provenance can produce a
                            // non-negative walk: then just continue,
                            // the count bound still decides).
                            let mut x = u;
                            let mut wsum = w;
                            for _ in 0..n {
                                if x == v {
                                    if wsum < 0 {
                                        ok = false;
                                        break 'repair;
                                    }
                                    break;
                                }
                                if pedge[x as usize] == u32::MAX {
                                    break;
                                }
                                let ei = pedge[x as usize] as usize;
                                wsum += if ei < nb {
                                    self.edges[ei].2
                                } else {
                                    extra[ei - nb].2
                                };
                                x = parent[x as usize];
                            }
                        }
                        if !inq[v as usize] {
                            cnt[v as usize] += 1;
                            // Enqueue-count bound (see solve()).
                            if cnt[v as usize] > n as u32 {
                                ok = false;
                                break 'repair;
                            }
                            if cnt[v as usize] >= DEEP_AT {
                                deep = true;
                            }
                            inq[v as usize] = true;
                            queue.push_back(v);
                        }
                    }
                }};
            }
            for &eid in &self.adj[u as usize] {
                let (_, v, w) = self.edges[eid as usize];
                relax!(v, w, eid);
            }
            // Recv/unblock extras are tiny (a linear scan beats a
            // map), but the case machinery can pass one chosen case
            // per committed waited inbox read in a single slice; the
            // bucketed index keeps the per-dequeue cost proportional
            // to the edges actually leaving `u`.
            if let Some(buckets) = &extra_by_from {
                for &(v, w, ei) in buckets.get(&u).map(|b| b.as_slice()).unwrap_or(&[]) {
                    relax!(v, w, ei);
                }
            } else {
                for (i, &(from, v, w)) in extra.iter().enumerate() {
                    if from == u {
                        relax!(v, w, (nb + i) as u32);
                    }
                }
            }
        }
        if !(commit && ok) {
            std::mem::swap(&mut self.pi, &mut self.scratch_pi); // restore saved potentials
        }
        self.scratch_inq = inq;
        self.scratch_cnt = cnt;
        self.scratch_parent = parent;
        self.scratch_pedge = pedge;
        ok
    }

    /// Failure-path conflict extraction: recover one negative cycle of
    /// base + `extra` and report which `extra` edges lie on it.
    ///
    /// Fast path: an incremental SPFA repair seeded from the
    /// base-feasible potentials (only the cone the extras disturb is
    /// touched, mirroring `probe`) with parent provenance; the
    /// recovered cycle is re-verified (weight < 0, in release too)
    /// before being trusted. Any recovery ambiguity falls back to the
    /// exact textbook Bellman-Ford, so the verdict is exact either
    /// way. Pure by construction (touches no solver state).
    pub(crate) fn probe_conflict(&self, extra: &[Edge]) -> Conflict {
        match self.probe_conflict_incremental(extra) {
            Some(c) => c,
            None => self.probe_conflict_bf(extra),
        }
    }

    /// Incremental extractor: `Some(verdict)` when it can certify the
    /// answer, `None` when cycle recovery failed (caller falls back to
    /// the exact extractor). Requires a feasible base (otherwise the
    /// cycle may live entirely in base edges the repair never walks).
    fn probe_conflict_incremental(&self, extra: &[Edge]) -> Option<Conflict> {
        if !self.feasible {
            return None;
        }
        let n = self.n;
        let nb = self.edges.len();
        if n == 0 {
            return Some(Conflict::Feasible);
        }
        let mut pi: Vec<Time> = self.pi.clone();
        // Provenance per vertex: which edge last improved it
        // (0..nb-1 = base edge id, nb.. = nb + extra index).
        let mut parent: Vec<u32> = vec![u32::MAX; n];
        let mut pedge: Vec<u32> = vec![u32::MAX; n];
        let mut inq = vec![false; n];
        let mut cnt = vec![0u32; n];
        let mut queue: VecDeque<u32> = VecDeque::new();
        let extra_by_from: Option<HashMap<u32, Vec<(u32, Time, u32)>>> = if extra.len() > 16 {
            let mut m: HashMap<u32, Vec<(u32, Time, u32)>> = HashMap::new();
            for (i, &(from, v, w)) in extra.iter().enumerate() {
                m.entry(from).or_default().push((v, w, (nb + i) as u32));
            }
            Some(m)
        } else {
            None
        };
        for (i, &(u, v, w)) in extra.iter().enumerate() {
            let cand = pi[u as usize] + w;
            if cand < pi[v as usize] {
                pi[v as usize] = cand;
                parent[v as usize] = u;
                pedge[v as usize] = (nb + i) as u32;
                if !inq[v as usize] {
                    cnt[v as usize] += 1;
                    inq[v as usize] = true;
                    queue.push_back(v);
                }
            }
        }
        while let Some(u) = queue.pop_front() {
            inq[u as usize] = false;
            macro_rules! relax {
                ($v:expr, $w:expr, $ei:expr) => {{
                    let v = $v;
                    let w = $w;
                    let ei0 = $ei;
                    let cand = pi[u as usize] + w;
                    if cand < pi[v as usize] {
                        pi[v as usize] = cand;
                        parent[v as usize] = u;
                        pedge[v as usize] = ei0;
                        // Parent-graph cycle check on every improvement
                        // (this IS the failure path: rejection is the
                        // expected outcome, so pay for early detection
                        // immediately rather than after a threshold as
                        // `probe` does). A verified negative cycle
                        // yields the certificate directly.
                        {
                            let mut x = u;
                            let mut wsum = w;
                            let mut hits: Vec<usize> = Vec::new();
                            if ei0 as usize >= nb {
                                hits.push(ei0 as usize - nb);
                            }
                            for _ in 0..n {
                                if x == v {
                                    if wsum < 0 {
                                        hits.sort_unstable();
                                        hits.dedup();
                                        return Some(Conflict::Infeasible(hits));
                                    }
                                    break;
                                }
                                let px = pedge[x as usize];
                                if px == u32::MAX {
                                    break;
                                }
                                let pei = px as usize;
                                wsum += if pei < nb {
                                    self.edges[pei].2
                                } else {
                                    hits.push(pei - nb);
                                    extra[pei - nb].2
                                };
                                x = parent[x as usize];
                            }
                        }
                        if !inq[v as usize] {
                            cnt[v as usize] += 1;
                            if cnt[v as usize] > n as u32 {
                                return Some(self.recover_cycle(
                                    v, nb, extra, &parent, &pedge,
                                )?);
                            }
                            inq[v as usize] = true;
                            queue.push_back(v);
                        }
                    }
                }};
            }
            for &eid in &self.adj[u as usize] {
                let (_, v, w) = self.edges[eid as usize];
                relax!(v, w, eid);
            }
            if let Some(buckets) = &extra_by_from {
                for &(v, w, ei) in buckets.get(&u).map(|b| b.as_slice()).unwrap_or(&[]) {
                    relax!(v, w, ei);
                }
            } else {
                for (i, &(from, v, w)) in extra.iter().enumerate() {
                    if from == u {
                        relax!(v, w, (nb + i) as u32);
                    }
                }
            }
        }
        Some(Conflict::Feasible)
    }

    /// Walk parent provenance from `v` to find a cycle, verify its
    /// weight is negative, and report the extra indices on it. `None`
    /// when the chain breaks or the cycle fails verification (caller
    /// falls back to the exact extractor; never trusted unverified).
    fn recover_cycle(
        &self,
        v: u32,
        nb: usize,
        extra: &[Edge],
        parent: &[u32],
        pedge: &[u32],
    ) -> Option<Conflict> {
        let n = self.n;
        let edge_at = |i: usize| -> Edge {
            if i < nb {
                self.edges[i]
            } else {
                extra[i - nb]
            }
        };
        // Stamp-walk up to n+1 parents; the first repeated vertex
        // closes the cycle.
        let mut seen = vec![false; n];
        let mut cur = v;
        let mut start = u32::MAX;
        for _ in 0..=n {
            if cur == u32::MAX {
                return None;
            }
            if seen[cur as usize] {
                start = cur;
                break;
            }
            seen[cur as usize] = true;
            cur = parent[cur as usize];
        }
        if start == u32::MAX {
            return None;
        }
        let mut extras_hit: Vec<usize> = Vec::new();
        let mut weight: Time = 0;
        let mut x = start;
        for _ in 0..=n {
            let ei = pedge[x as usize] as usize;
            if pedge[x as usize] == u32::MAX {
                return None;
            }
            let (u, to, w) = edge_at(ei);
            if to != x || u != parent[x as usize] {
                return None; // provenance out of sync: do not trust
            }
            weight += w;
            if ei >= nb {
                extras_hit.push(ei - nb);
            }
            x = parent[x as usize];
            if x == start {
                if weight < 0 {
                    extras_hit.sort_unstable();
                    extras_hit.dedup();
                    return Some(Conflict::Infeasible(extras_hit));
                }
                return None; // not negative: not a certificate
            }
        }
        None
    }

    /// Exact fallback extractor: a fresh textbook Bellman-Ford with
    /// parent tracking over base + `extra` (O(n*m), local buffers,
    /// no incremental subtleties). Also the differential oracle for
    /// the incremental path.
    fn probe_conflict_bf(&self, extra: &[Edge]) -> Conflict {
        let n = self.n;
        let nb = self.edges.len();
        let m = nb + extra.len();
        if n == 0 || m == 0 {
            return Conflict::Feasible;
        }
        // Provenance per vertex: which edge last improved it.
        // 0..nb-1 = base edge id, nb.. = nb + extra index.
        let mut pi: Vec<Time> = vec![0; n];
        let mut parent: Vec<u32> = vec![u32::MAX; n];
        let mut pedge: Vec<u32> = vec![u32::MAX; n];
        let edge_at = |i: usize| -> Edge {
            if i < nb {
                self.edges[i]
            } else {
                extra[i - nb]
            }
        };
        for _round in 0..n {
            let mut changed = false;
            for i in 0..m {
                let (u, v, w) = edge_at(i);
                let cand = pi[u as usize] + w;
                if cand < pi[v as usize] {
                    pi[v as usize] = cand;
                    parent[v as usize] = u;
                    pedge[v as usize] = i as u32;
                    changed = true;
                }
            }
            if !changed {
                return Conflict::Feasible;
            }
        }
        // After n full rounds, any still-improvable edge witnesses a
        // negative cycle reachable through the parent pointers.
        let mut head: Option<u32> = None;
        for i in 0..m {
            let (u, v, w) = edge_at(i);
            if pi[u as usize] + w < pi[v as usize] {
                pi[v as usize] = pi[u as usize] + w;
                parent[v as usize] = u;
                pedge[v as usize] = i as u32;
                head = Some(v);
                break;
            }
        }
        let Some(mut v) = head else {
            return Conflict::Feasible;
        };
        // Walk back n steps to land ON the cycle, then collect it.
        for _ in 0..n {
            v = parent[v as usize];
            debug_assert!(v != u32::MAX, "parent chain broke inside walk-back");
        }
        let start = v;
        let mut extras_hit: Vec<usize> = Vec::new();
        let mut weight: Time = 0;
        loop {
            let ei = pedge[v as usize] as usize;
            let (u, to, w) = edge_at(ei);
            debug_assert!(to == v && u == parent[v as usize], "provenance out of sync");
            weight += w;
            if ei >= nb {
                extras_hit.push(ei - nb);
            }
            v = parent[v as usize];
            if v == start {
                break;
            }
        }
        debug_assert!(
            weight < 0,
            "recovered cycle is not negative (weight {weight}): not a certificate"
        );
        extras_hit.sort_unstable();
        extras_hit.dedup();
        Conflict::Infeasible(extras_hit)
    }

    /// Witness assignment (shifted so the root is 0), or `None` when
    /// infeasible. Feasibility of `pi` plus the monotone chains from
    /// each Begin (anchored to the root) make every shifted value
    /// non-negative.
    pub(crate) fn witness(&self) -> Option<Vec<Time>> {
        if !self.feasible {
            return None;
        }
        let root = self.pi[ROOT as usize];
        Some(self.pi.iter().map(|&p| p - root).collect())
    }

    /// Exact bounds of variable `v` relative to the root:
    /// `hi = sp(Root -> v)` (None = unbounded), `lo = -sp(v -> Root)`.
    /// Only meaningful on a feasible system; used by reporting paths.
    pub(crate) fn bounds(&self, v: u32) -> Option<(Time, Option<Time>)> {
        if !self.feasible {
            return None;
        }
        let hi = self.sssp(ROOT).get(&v).copied();
        let lo = self.sssp(v).get(&ROOT).map(|d| -d);
        // Every event chains down to the root (t_root <= t_e), so lo
        // is always defined on systems built by this module.
        Some((lo.unwrap_or(0), hi))
    }

    /// SPFA single-source shortest paths over the base edges via the
    /// adjacency lists. Returns only reachable variables.
    fn sssp(&self, src: u32) -> HashMap<u32, Time> {
        let mut dist: HashMap<u32, Time> = HashMap::new();
        dist.insert(src, 0);
        let mut inq = vec![false; self.n];
        let mut queue = VecDeque::new();
        queue.push_back(src);
        inq[src as usize] = true;
        while let Some(u) = queue.pop_front() {
            inq[u as usize] = false;
            let du = dist[&u];
            for &eid in &self.adj[u as usize] {
                let (_, b, w) = self.edges[eid as usize];
                let cand = du + w;
                if dist.get(&b).map_or(true, |&d| cand < d) {
                    dist.insert(b, cand);
                    if !inq[b as usize] {
                        inq[b as usize] = true;
                        queue.push_back(b);
                    }
                }
            }
        }
        dist
    }
}

// =====================================================================
// TimedDcs: constraint derivation from an execution graph
// =====================================================================

/// Exact timed-feasibility oracle over one graph state.
///
/// Built per filtering batch; the graph must not be mutated between
/// `build` and the last probe (probes themselves never mutate it).
/// `floating` suppresses the label-derived constraints of one event so
/// probes can supply hypothetical ones for it.
///
/// Committed waited-inbox reads make the system a small disjunction: a
/// read of exactly `min` members returns at `max(t0, completing
/// arrival)`, i.e. either immediately at its invocation time or
/// exactly at one member's arrival. Arrivals are EXPLICIT variables
/// (one per non-dropped send, `a_s` in `[t_s+L, t_s+U]`), so a message
/// has ONE arrival consistent across every constraint that mentions
/// it, and FIFO delivery is a real constraint (2026-08-28 decision):
///
/// ```text
/// window:          t_s + L <= a_s <= t_s + U
/// FIFO:            b1 in sb(b2), same channel, both delivered
///                    => a_b1 <= a_b2      (NoOrder/Bag: sb empty, free)
/// immediate:       t_e = t_p;             forall s: a_s <= t_e <= a_s+sd
/// completed by k:  t_p <= t_e (<= t_p+W); t_e = a_k   (no sd for k!)
///                                          forall s != k: a_s <= t_e <= a_s+sd
/// ```
///
/// The system is feasible iff SOME choice of one case per such read
/// is; `build` runs that search once, and every probe quantifies over
/// the same choices (fast path: the assignment the search settled on).
pub(crate) struct TimedDcs<'g> {
    g: &'g ExecutionGraph,
    cfg: &'g TimedConfig,
    vars: ScopeVars,
    n: usize,
    base_edges: Vec<Edge>,
    /// One group per committed waited inbox read (|subset| == min):
    /// the alternative edge sets of its disjunction.
    case_sets: Vec<Vec<Vec<Edge>>>,
    /// Flattened edges of the feasible case assignment found by
    /// `solve_cases` (probe fast path; empty when no case sets).
    chosen_edges: Vec<Edge>,
    /// Per-group contiguous slice of `chosen_edges` (conflict
    /// attribution for the fast-path short-circuit).
    chosen_ranges: Vec<(usize, usize)>,
    /// The committed option index behind each `chosen_ranges` entry.
    chosen_choice: Vec<usize>,
    /// Learned no-goods: sets of (group, option) literals certified
    /// (by a negative cycle) to be jointly unsatisfiable with the base
    /// system. Valid for this instance's lifetime: `case_sets` is
    /// immutable after `build`. Only committed-group literals are ever
    /// stored here; transient/extras-conditional conflicts stay local
    /// to one search invocation.
    nogoods: Vec<Vec<(u32, u32)>>,
    /// Base-only system (case edges excluded); probes run against it
    /// with case edges supplied as extras.
    core: DcsCore,
    /// Base + chosen assignment, built lazily for bounds/witness.
    leaf: Option<DcsCore>,
    feasible: bool,
    scratch: Vec<Edge>,
}

impl<'g> TimedDcs<'g> {
    pub(crate) fn build(
        g: &'g ExecutionGraph,
        cfg: &'g TimedConfig,
        view: Option<&VectorClock>,
        floating: Option<Event>,
    ) -> Self {
        let full;
        let view = match view {
            Some(v) => v,
            None => {
                full = g.view_from_stamp(g.stamp());
                &full
            }
        };

        // Intern every event in scope. ROOT is variable 0.
        let mut ev: HashMap<Event, u32> = HashMap::new();
        let mut order: Vec<Event> = Vec::new();
        for (tid, maxidx) in view.entries() {
            for j in 0..=maxidx {
                let pos = Event::new(tid, j);
                if !g.contains(pos) {
                    continue;
                }
                let id = 1 + order.len() as u32;
                ev.insert(pos, id);
                order.push(pos);
            }
        }

        // Arrival variables: one per non-dropped send in scope, with
        // its transit window, grouped by channel for FIFO coupling.
        let mut arr: HashMap<Event, u32> = HashMap::new();
        let mut next = 1 + order.len() as u32;
        let mut chan: HashMap<crate::loc::Loc, Vec<Event>> = HashMap::new();
        let mut edges: Vec<Edge> = Vec::new();
        for &pos in &order {
            let Some(slab) = g.send_label(pos) else {
                continue;
            };
            if slab.is_dropped() {
                continue;
            }
            let a = next;
            next += 1;
            arr.insert(pos, a);
            chan.entry(slab.loc().clone()).or_default().push(pos);
            if Some(pos) == floating {
                continue; // window suppressed like any label constraint
            }
            let s = ev[&pos];
            let (l, u) = slab.transit().unwrap_or((cfg.l, cfg.u));
            edges.push((a, s, -i128::from(l))); // a_s >= t_s + L
            edges.push((s, a, i128::from(u))); // a_s <= t_s + U
        }
        // FIFO delivery (2026-08-28 decision): same-channel delivered
        // sends arrive in their delivery-model order (`sb`); overtaking
        // is a Bag/NoOrder-only behavior (empty sb = no coupling).
        // Lossy sends that were dropped never arrive and are already
        // excluded above (no arrival variable = no coupling edge).
        // Same-thread pairs are chained through the NEAREST sb-ordered
        // predecessor only: same-thread sb containment is monotone
        // along po for every ordered model, so the chain's transitive
        // DCS paths cover the farther pairs (O(k) edges per sender
        // instead of O(k^2)). Cross-thread sb pairs (Causal/Total)
        // stay explicit.
        for sends in chan.values() {
            fifo_coupling_edges(
                sends,
                |b2, b1| g.send_label(b2).unwrap().sb().contains(b1),
                |b| arr[&b],
                &mut edges,
            );
        }
        let vars = ScopeVars { ev, arr };
        let mut case_sets: Vec<Vec<Vec<Edge>>> = Vec::new();
        let mut infeasible_label = false;
        for &pos in &order {
            if Some(pos) == floating {
                continue;
            }
            let e = vars.ev[&pos];
            if pos.index == 0 {
                // Begin: every thread's clock starts at absolute 0
                // (walker parity: index 0 has window [0, 0]).
                edges.push((ROOT, e, 0));
                edges.push((e, ROOT, 0));
                continue;
            }
            let pred = Event::new(pos.thread, pos.index - 1);
            let p = match vars.ev.get(&pred) {
                Some(&p) => p,
                // Predecessor outside the view: anchor loosely to the
                // root lower bound only (defensive; views are
                // po-closed so this should not happen).
                None => {
                    debug_assert!(false, "view not po-closed at {pos}");
                    continue;
                }
            };
            match g.label(pos) {
                LabelEnum::Sleep(slab) => {
                    let d = i128::from(slab.duration());
                    edges.push((p, e, d));
                    edges.push((e, p, -d));
                }
                LabelEnum::RecvMsg(rlab) => match rlab.wait() {
                    // Untimed receive: transparent for timing; the
                    // walker propagates pred's window unchanged, which
                    // is equality for downstream chaining.
                    None => {
                        edges.push((p, e, 0));
                        edges.push((e, p, 0));
                    }
                    Some(wait) => match rlab.rf() {
                        Some(s) => {
                            self_read_edges(
                                &mut edges,
                                &vars,
                                g,
                                cfg,
                                e,
                                p,
                                pos,
                                s,
                                wait,
                                &mut infeasible_label,
                            );
                            // GC semantics: skipped older unread messages
                            // must not have been readable at this read in
                            // the witness timeline (honesty cases; see
                            // push_recv_skip_cases).
                            push_recv_skip_cases(
                                &mut case_sets,
                                &vars,
                                g,
                                cfg,
                                view,
                                e,
                                pos,
                                rlab,
                                s,
                            );
                        }
                        None => match wait {
                            // Timeout: exact clock advance by W.
                            WaitTime::Finite(w) => {
                                let w = i128::from(w);
                                edges.push((p, e, w));
                                edges.push((e, p, -w));
                            }
                            // Blocking recv with no rf: walker parity
                            // (empty window; the graph is dropped).
                            WaitTime::Infinite => infeasible_label = true,
                        },
                    },
                },
                LabelEnum::Inbox(ilab) => match ilab.wait() {
                    None => {
                        edges.push((p, e, 0));
                        edges.push((e, p, 0));
                    }
                    Some(wait) => match ilab.rfs() {
                        Some(subset) if !subset.is_empty() => {
                            push_inbox_skip_cases(
                                &mut case_sets,
                                &vars,
                                g,
                                cfg,
                                view,
                                e,
                                pos,
                                ilab,
                                &subset,
                            );
                            let at_capacity = ilab.max() == Some(subset.len());
                            let excl = inbox_exclusions(
                                &vars,
                                g,
                                cfg,
                                pos,
                                ilab.recv_loc(),
                                ilab.comm(),
                                &subset,
                            );
                            if subset.len() > ilab.min() {
                                // A read of MORE than `min` members can
                                // only have happened immediately at t0
                                // (had it waited, it would have returned
                                // at the min-th arrival): one exact case,
                                // plus exclusion dodges below capacity.
                                if excl.is_empty() || at_capacity {
                                    immediate_inbox_edges(
                                        &mut edges,
                                        &vars,
                                        g,
                                        cfg,
                                        e,
                                        p,
                                        pos,
                                        &subset,
                                        &mut infeasible_label,
                                    );
                                } else {
                                    match subset_windows(&vars, g, cfg, pos, &subset) {
                                        Some(windows) => {
                                            let sd = i128::from(cfg.sd_for(pos.thread));
                                            let mut c: Vec<Edge> =
                                                vec![(e, p, 0), (p, e, 0)];
                                            for &(_sv, av, _l, _u) in &windows {
                                                c.push((e, av, 0));
                                                c.push((av, e, sd));
                                            }
                                            case_sets.push(fold_exclusions(
                                                vec![c],
                                                &excl,
                                                |_, x| {
                                                    exclusion_options(
                                                        None, &windows, false, e, sd, x,
                                                    )
                                                },
                                            ));
                                        }
                                        None => infeasible_label = true,
                                    }
                                }
                            } else {
                                // Waited min-sized read: disjunction
                                // over t_e = max(t_p, completing arrival),
                                // exclusion dodges folded per case.
                                match (
                                    waited_inbox_cases(
                                        &vars, g, cfg, e, p, pos, &subset, wait,
                                    ),
                                    subset_windows(&vars, g, cfg, pos, &subset),
                                ) {
                                    (Some(cases), Some(windows)) => {
                                        let sd = i128::from(cfg.sd_for(pos.thread));
                                        case_sets.push(fold_exclusions(
                                            cases,
                                            &excl,
                                            |i, x| {
                                                exclusion_options(
                                                    i.checked_sub(1),
                                                    &windows,
                                                    at_capacity,
                                                    e,
                                                    sd,
                                                    x,
                                                )
                                            },
                                        ));
                                    }
                                    _ => infeasible_label = true,
                                }
                            }
                        }
                        // Immediate empty: happens at pred time.
                        Some(_empty) => {
                            edges.push((p, e, 0));
                            edges.push((e, p, 0));
                        }
                        None => match wait {
                            WaitTime::Finite(w) => {
                                let w = i128::from(w);
                                edges.push((p, e, w));
                                edges.push((e, p, -w));
                            }
                            WaitTime::Infinite => infeasible_label = true,
                        },
                    },
                },
                LabelEnum::Block(blab) => {
                    edges.push((p, e, 0));
                    edges.push((e, p, 0));
                    if blab.refuses_matching() {
                        // min and from_inbox are deliberately ignored:
                        // the refusal conjunction is min-unaware by the
                        // staged design (see push_refusal_edges doc).
                        if let crate::event_label::BlockType::Value(loc, _, _, _, _) =
                            blab.btype()
                        {
                            push_refusal_edges(
                                &mut edges,
                                &vars,
                                g,
                                cfg,
                                pos,
                                blab.as_event_label(),
                                loc,
                                e,
                            );
                        }
                    }
                }
                // Every other label is instantaneous at its
                // predecessor's time (walker parity: pass-through).
                _ => {
                    edges.push((p, e, 0));
                    edges.push((e, p, 0));
                }
            }
        }

        let n = next as usize;
        let core = DcsCore::new(n, edges.clone());
        let mut dcs = Self {
            g,
            cfg,
            vars,
            n,
            base_edges: edges,
            case_sets,
            chosen_edges: Vec::new(),
            chosen_ranges: Vec::new(),
            chosen_choice: Vec::new(),
            nogoods: Vec::new(),
            core,
            leaf: None,
            feasible: false,
            scratch: Vec::new(),
        };
        dcs.feasible = !infeasible_label && dcs.solve_cases();
        dcs
    }

    pub(crate) fn base_feasible(&self) -> bool {
        self.feasible
    }

    /// Install `choice` as the committed assignment / fast-path hint,
    /// keeping the per-group ranges in sync and invalidating the leaf.
    fn adopt_choice(&mut self, choice: &[usize]) {
        self.chosen_edges.clear();
        self.chosen_ranges.clear();
        self.chosen_choice.clear();
        for (d, &c) in choice.iter().enumerate() {
            let start = self.chosen_edges.len();
            self.chosen_edges.extend_from_slice(&self.case_sets[d][c]);
            self.chosen_ranges.push((start, self.chosen_edges.len()));
            self.chosen_choice.push(c);
        }
        self.leaf = None;
    }

    /// Search one feasible case assignment for the committed inbox
    /// disjunctions (the system is feasible iff one exists).
    fn solve_cases(&mut self) -> bool {
        if !self.core.feasible() {
            return false;
        }
        if self.case_sets.is_empty() {
            return true;
        }
        let found = {
            let Self {
                ref mut core,
                ref case_sets,
                ref mut nogoods,
                ..
            } = *self;
            CaseSearcher::new(core, case_sets, None, &[], nogoods, None).run()
        };
        match found {
            Some(choice) => {
                self.adopt_choice(&choice);
                true
            }
            None => false,
        }
    }

    /// Exact probe: is base + committed-case disjunctions + `extra`
    /// satisfiable for SOME case assignment? Fast path first probes
    /// the assignment `solve_cases` settled on; only a rejection pays
    /// for the full re-search (rejections are the pruned candidates,
    /// and a candidate is pruned only when EVERY assignment rejects
    /// it, otherwise the probe would over-prune).
    fn probe_exact(&mut self, extra: &[Edge]) -> bool {
        if !self.feasible {
            return false;
        }
        if self.case_sets.is_empty() {
            return self.core.probe(extra);
        }
        prof::add(&prof::PE_CALLS, 1);
        // Fast path probes against the LEAF (base + chosen committed
        // into solved potentials): the chosen prefix costs nothing per
        // probe instead of being re-seeded every time. Identical
        // constraint system, identical verdict.
        let mut acc = std::mem::take(&mut self.scratch);
        acc.clear();
        acc.extend_from_slice(&self.chosen_edges);
        acc.extend_from_slice(extra);
        self.ensure_leaf();
        let fast = self.leaf.as_mut().expect("ensure_leaf").probe(extra);
        if fast {
            prof::add(&prof::PE_FAST_OK, 1);
            self.scratch = acc;
            return true;
        }
        // Conflict-driven short-circuit: ask WHY the hint failed. If
        // the negative cycle touches no chosen-option edge, no case
        // assignment can change it: definitively infeasible, no search.
        // If it implicates hint options, learn that combination before
        // searching (persistently when the caller extras are not on
        // the cycle: the certificate then holds unconditionally).
        let chosen_len = self.chosen_edges.len();
        let mut seed_local: Option<Vec<(u32, u32)>> = None;
        match self.core.probe_conflict(&acc) {
            Conflict::Infeasible(idx) => {
                let touches_chosen = idx.iter().any(|&i| i < chosen_len);
                if !touches_chosen {
                    prof::add(&prof::PE_KILL, 1);
                    acc.clear();
                    self.scratch = acc;
                    return false;
                }
                let extras_involved = idx.iter().any(|&i| i >= chosen_len);
                let mut lits: Vec<(u32, u32)> = idx
                    .iter()
                    .filter(|&&i| i < chosen_len)
                    .map(|&i| {
                        let d = self
                            .chosen_ranges
                            .iter()
                            .position(|&(s, e)| s <= i && i < e)
                            .expect("chosen_ranges out of sync");
                        (d as u32, self.chosen_choice[d] as u32)
                    })
                    .collect();
                lits.sort_unstable();
                lits.dedup();
                if extras_involved {
                    seed_local = Some(lits);
                } else if self.nogoods.len() < NOGOOD_CAP {
                    self.nogoods.push(lits);
                }
            }
            // Disagreement between the incremental probe and the exact
            // extractor: defensively fall through to the full search
            // with no lesson (never tighter).
            Conflict::Feasible => {}
        }
        acc.clear();
        self.scratch = acc;
        prof::add(&prof::PE_SEARCH, 1);
        let found = {
            let Self {
                ref mut core,
                ref case_sets,
                ref mut nogoods,
                ..
            } = *self;
            CaseSearcher::new(core, case_sets, None, extra, nogoods, seed_local).run()
        };
        match found {
            Some(choice) => {
                // Adopt the found assignment as the new fast-path hint
                // (an assignment feasible WITH the extras is feasible
                // without them, so it is as certified as the
                // build-time one); the lazily built leaf must follow.
                self.adopt_choice(&choice);
                true
            }
            None => false,
        }
    }

    /// Like [`Self::probe_exact`] but with one TRANSIENT extra case
    /// group of alternatives on top of the committed ones (used by the
    /// inbox probes, whose exclusion-folded case list is probe-local).
    /// The fast-path hint is not updated.
    fn probe_exact_with_set(&mut self, extra: &[Edge], extra_set: &[Vec<Edge>]) -> bool {
        if !self.feasible {
            return false;
        }
        if extra_set.is_empty() {
            return self.probe_exact(extra);
        }
        // Fast path: the committed assignment plus each alternative
        // (cheap incremental probes, exactly the pre-CDCL cost). At
        // most ONE conflict extraction happens per call, and only if
        // every alternative failed: on hint + caller-extras alone. If
        // that conjunction is itself contradictory without touching
        // any hint option, the whole probe dies with one certificate
        // (the dominant rejection shape in the candidate swarms);
        // per-alternative extraction is deliberately avoided: folded
        // groups carry thousands of alternatives and an O(n*m)
        // certificate per alternative would dwarf the search it saves.
        let chosen_len = self.chosen_edges.len();
        prof::add(&prof::PEWS_CALLS, 1);
        let _t = prof::Timer::start(&prof::PEWS_NS);
        // Cheap per-alternative fast path against the LEAF (base +
        // chosen committed into solved potentials): each probe seeds
        // only the small alternative + caller extras instead of
        // re-seeding the whole chosen prefix (which grows with the
        // number of committed batch reads and dominated these probes
        // at larger node counts). Identical constraint system.
        let mut acc = std::mem::take(&mut self.scratch);
        self.ensure_leaf();
        let hint_alone_ok;
        {
            let leaf = self.leaf.as_mut().expect("ensure_leaf");
            for alt in extra_set {
                acc.clear();
                acc.extend_from_slice(alt);
                acc.extend_from_slice(extra);
                prof::add(&prof::PEWS_CHEAP_PROBES, 1);
                if leaf.probe(&acc) {
                    prof::add(&prof::PEWS_FAST_OK, 1);
                    self.scratch = acc;
                    return true;
                }
            }
            hint_alone_ok = leaf.probe(extra);
        }
        acc.clear();
        acc.extend_from_slice(&self.chosen_edges);
        acc.extend_from_slice(extra);
        if !hint_alone_ok {
            if let Conflict::Infeasible(idx) = self.core.probe_conflict(&acc) {
                if !idx.iter().any(|&i| i < chosen_len) {
                    // Base + caller extras contradictory on their own:
                    // no assignment and no alternative can help.
                    prof::add(&prof::PEWS_BASE_KILL, 1);
                    acc.clear();
                    self.scratch = acc;
                    return false;
                }
            }
        }
        // Cheap per-alternative elimination, no extraction: an
        // alternative that fails against the BASE alone (no committed
        // options at all) fails under every assignment, since every
        // assignment's system contains base + alt + extras. Only
        // alternatives that are base-viable but hint-blocked justify
        // the full search. This is what turns "dead candidate probed
        // against a big folded group" into a linear scan of cheap
        // probes instead of a product walk.
        let mut any_needs_search = false;
        for alt in extra_set {
            acc.clear();
            acc.extend_from_slice(alt);
            acc.extend_from_slice(extra);
            prof::add(&prof::PEWS_CHEAP_PROBES, 1);
            if self.core.probe(&acc) {
                any_needs_search = true;
                break;
            }
        }
        acc.clear();
        self.scratch = acc;
        if !any_needs_search {
            // Every alternative is contradictory with the base alone.
            prof::add(&prof::PEWS_ALT_KILL, 1);
            return false;
        }
        if self.case_sets.is_empty() {
            return false; // fast path was exhaustive (no committed cases)
        }
        prof::add(&prof::PEWS_SEARCH, 1);
        let found = {
            let Self {
                ref mut core,
                ref case_sets,
                ref mut nogoods,
                ..
            } = *self;
            CaseSearcher::new(core, case_sets, Some(extra_set), extra, nogoods, None).run()
        };
        found.is_some()
    }

    /// Base + chosen case assignment, for bounds/witness reporting.
    fn ensure_leaf(&mut self) {
        if self.leaf.is_none() {
            let mut edges = self.base_edges.clone();
            edges.extend_from_slice(&self.chosen_edges);
            self.leaf = Some(DcsCore::new(self.n, edges));
        }
    }

    /// Exact feasibility of "the floating receive reads from `cand`".
    /// No graph mutation; potentials restored after the check.
    pub(crate) fn probe_recv_rf(&mut self, recv: Event, cand: Event) -> bool {
        let Some(&e) = self.vars.ev.get(&recv) else {
            debug_assert!(false, "probe target {recv} not in scope");
            return true;
        };
        let pred = Event::new(recv.thread, recv.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        let rlab = self.g.recv_label(recv).unwrap();
        let mut extra: Vec<Edge> = Vec::new();
        match rlab.wait() {
            // Untimed receive: transparent; candidate always feasible
            // relative to the base system.
            None => {
                extra.push((p, e, 0));
                extra.push((e, p, 0));
            }
            Some(wait) => {
                extra.push((e, p, 0)); // t_pred <= t_recv
                if let WaitTime::Finite(w) = wait {
                    extra.push((p, e, i128::from(w))); // t_recv <= t_pred + W
                }
                let Some(&av) = self.vars.arr.get(&cand) else {
                    // Invariant breach tripwire (candidates come from
                    // the same graph the oracle was built on): keep the
                    // candidate, never tighter; certification gates any
                    // report.
                    debug_assert!(false, "candidate send {cand} not in scope");
                    return true;
                };
                let sd = i128::from(self.cfg.sd_for(recv.thread));
                extra.push((e, av, 0)); // a_send <= t_recv
                extra.push((av, e, sd)); // t_recv <= a_send + sd
            }
        }
        self.probe_exact(&extra)
    }

    /// Exact feasibility of waking the blocked receive at `block_pos`
    /// by reading `send`: same window as a blocking read (no wait cap).
    pub(crate) fn probe_block_unblock(&mut self, block_pos: Event, send: Event) -> bool {
        self.probe_unblock_subset(block_pos, &[send])
    }

    /// Dead-front unsealing probe (2026-08-28): can the timed read at
    /// `recv` read `cand` in a timeline where EVERY front in `fronts`
    /// is never readable during the wait? A front dodges by being
    /// fully dead before the wait began (`a_b + sd < t_p`; a waiting
    /// receive takes a front the instant it becomes readable, so
    /// mid-wait readability forbids the skip) or by arriving strictly
    /// after the read (`a_b > t_e`). Exact per-front 2-case
    /// disjunction, folded into one transient group.
    pub(crate) fn probe_recv_rf_skipping(
        &mut self,
        recv: Event,
        cand: Event,
        fronts: &[Event],
    ) -> bool {
        if fronts.is_empty() {
            return self.probe_recv_rf(recv, cand);
        }
        let Some(&e) = self.vars.ev.get(&recv) else {
            debug_assert!(false, "probe target {recv} not in scope");
            return true;
        };
        let pred = Event::new(recv.thread, recv.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        match self.g.recv_label(recv).unwrap().wait() {
            // Untimed receive: no timing to dodge with; the plain
            // candidate probe is the whole judgment.
            None => self.probe_recv_rf(recv, cand),
            Some(wait) => self.probe_dodging_fronts(recv, e, p, Some(wait), cand, fronts),
        }
    }

    /// Wake-side sibling of [`Self::probe_recv_rf_skipping`] for a
    /// blocked (infinite-wait) receive: no wait cap; the wait begins
    /// at the block's own time (t_e = t_p for a Block label, so the
    /// dead-before-the-wait dodge anchors at `p`).
    pub(crate) fn probe_unblock_skipping(
        &mut self,
        block_pos: Event,
        send: Event,
        fronts: &[Event],
    ) -> bool {
        if fronts.is_empty() {
            return self.probe_block_unblock(block_pos, send);
        }
        let Some(&e) = self.vars.ev.get(&block_pos) else {
            debug_assert!(false, "block {block_pos} not in scope");
            return true;
        };
        if block_pos.index == 0 {
            return true;
        }
        let pred = Event::new(block_pos.thread, block_pos.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        self.probe_dodging_fronts(block_pos, e, p, None, send, fronts)
    }

    fn probe_dodging_fronts(
        &mut self,
        pos: Event,
        e: u32,
        p: u32,
        wait: Option<WaitTime>,
        cand: Event,
        fronts: &[Event],
    ) -> bool {
        let Some(&av) = self.vars.arr.get(&cand) else {
            debug_assert!(false, "candidate send {cand} not in scope");
            return true;
        };
        let sd = i128::from(self.cfg.sd_for(pos.thread));
        let mut extra: Vec<Edge> = vec![(e, p, 0)];
        if let Some(WaitTime::Finite(w)) = wait {
            extra.push((p, e, i128::from(w)));
        }
        extra.push((e, av, 0)); // a_cand <= t_e
        extra.push((av, e, sd)); // t_e <= a_cand + sd
        let mut cases: Vec<Vec<Edge>> = vec![Vec::new()];
        for &b in fronts {
            if b == cand {
                continue;
            }
            let Some(&bav) = self.vars.arr.get(&b) else {
                continue;
            };
            if cases.len() * 2 > EXCLUSION_FOLD_CAP {
                break; // drop remaining fronts: looser only
            }
            let opts: [Edge; 2] = [(bav, e, -1), (p, bav, -(sd + 1))];
            let mut next = Vec::with_capacity(cases.len() * 2);
            for c in &cases {
                for &o in &opts {
                    let mut cc = c.clone();
                    cc.push(o);
                    next.push(cc);
                }
            }
            cases = next;
        }
        if cases.len() == 1 {
            // No dodgeable front carried an arrival variable.
            extra.extend_from_slice(&cases[0]);
            return self.probe_exact(&extra);
        }
        self.probe_exact_with_set(&extra, &cases)
    }

    /// Joint wake-up probe for a blocked value read: can all of
    /// `sends` be read together as ONE batch by the blocked thread?
    /// The block is the last event of its thread, so the conjunctive
    /// window system is exact even for an inbox-shaped block
    /// (terminal-read equivalence, see [`Self::probe_inbox_rfs`]); no
    /// wait cap applies (only infinite-wait reads block).
    pub(crate) fn probe_unblock_subset(&mut self, block_pos: Event, sends: &[Event]) -> bool {
        let Some(&e) = self.vars.ev.get(&block_pos) else {
            debug_assert!(false, "block {block_pos} not in scope");
            return true;
        };
        if block_pos.index == 0 {
            return true; // defensive, mirrors is_waiting_on_written
        }
        let pred = Event::new(block_pos.thread, block_pos.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        let sd = i128::from(self.cfg.sd_for(block_pos.thread));
        let mut extra: Vec<Edge> = Vec::with_capacity(1 + 2 * sends.len());
        extra.push((e, p, 0));
        for &send in sends {
            let Some(&av) = self.vars.arr.get(&send) else {
                // Invariant breach tripwire: keep (never tighter).
                debug_assert!(false, "send {send} not in scope");
                return true;
            };
            extra.push((e, av, 0)); // a_send <= t_read
            extra.push((av, e, sd)); // t_read <= a_send + sd
        }
        self.probe_exact(&extra)
    }

    /// Inbox-shaped sibling of [`Self::probe_unblock_subset`]: the
    /// blocked read follows the completion-count rule (it fires at the
    /// arrival completing min), so excluded matching sends carry the
    /// same dodge disjunction as committed waited reads. Without it
    /// the wake test is looser than the offer filter and the scheduler
    /// livelocks on wake -> no offer -> block (the min >= 2 lesson).
    pub(crate) fn probe_unblock_inbox_subset(
        &mut self,
        block_pos: Event,
        sends: &[Event],
        loc: &crate::loc::RecvLoc,
        comm: crate::loc::CommunicationModel,
    ) -> bool {
        let Some(&e) = self.vars.ev.get(&block_pos) else {
            debug_assert!(false, "block {block_pos} not in scope");
            return true;
        };
        if block_pos.index == 0 {
            return true; // defensive, mirrors is_waiting_on_written
        }
        let pred = Event::new(block_pos.thread, block_pos.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        let Some(windows) = subset_windows(&self.vars, self.g, self.cfg, block_pos, sends)
        else {
            debug_assert!(false, "wake send not in scope");
            return true;
        };
        let Some(base) = waited_inbox_cases(
            &self.vars,
            self.g,
            self.cfg,
            e,
            p,
            block_pos,
            sends,
            WaitTime::Infinite,
        ) else {
            return true;
        };
        let excl =
            inbox_exclusions(&self.vars, self.g, self.cfg, block_pos, loc, comm, sends);
        let sd = i128::from(self.cfg.sd_for(block_pos.thread));
        let folded = fold_exclusions(base, &excl, |i, x| {
            // Blocks have no capacity bound in scope; the immediate
            // case still constrains (a t0 read returns all stored).
            exclusion_options(i.checked_sub(1), &windows, false, e, sd, x)
        });
        self.probe_exact_with_set(&[], &folded)
    }

    /// Feasibility of the GC refusal branch for the blocking receive
    /// at `pos`: some timeline lets every matching unconsumed message
    /// die before the wait begins. `anchor` is the receive's label
    /// base (for cancellation checks); the receive itself must be
    /// floating in this oracle.
    pub(crate) fn probe_gc_block(
        &mut self,
        pos: Event,
        anchor: &crate::event_label::EventLabel,
        loc: &crate::loc::RecvLoc,
    ) -> bool {
        let Some(&e) = self.vars.ev.get(&pos) else {
            return false;
        };
        if pos.index == 0 {
            return false;
        }
        let Some(&p) = self.vars.ev.get(&Event::new(pos.thread, pos.index - 1)) else {
            return self.feasible;
        };
        let mut extra: Vec<Edge> = vec![(e, p, 0), (p, e, 0)];
        push_refusal_edges(&mut extra, &self.vars, self.g, self.cfg, pos, anchor, loc, e);
        self.probe_exact(&extra)
    }

    /// Exact feasibility of the floating inbox read at `inbox`
    /// returning exactly `subset`.
    ///
    /// Terminal-read equivalence: at every call site (subset filtering
    /// at visit time, inbox backward revisits on the cut view) the
    /// inbox read is the LAST event of its thread and its own label
    /// constraints are floating, so no other constraint mentions its
    /// time. Under that condition the conjunctive window system below
    /// is EQUIVALENT to the exact disjunctive rule: from any
    /// window-feasible timeline, set each arrival a_s = clamp(t_e,
    /// t_s+L, t_s+U) and lower the read to t' = max(t_p, max_s a_s)
    /// <= t_e; every constraint still holds and t' realizes
    /// max(t0, completing arrival) exactly. Committed reads (which DO
    /// have dependents chaining off their time) get the full case
    /// split in `build` instead.
    pub(crate) fn probe_inbox_rfs(&mut self, inbox: Event, subset: &[Event]) -> bool {
        prof::add(&prof::IRF_CALLS, 1);
        let _t = prof::Timer::start(&prof::IRF_NS);
        let Some(&e) = self.vars.ev.get(&inbox) else {
            return true;
        };
        let pred = Event::new(inbox.thread, inbox.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        let ilab = self.g.inbox_label(inbox).unwrap();
        let Some(wait) = ilab.wait() else {
            // Untimed inbox: transparent, like probe_recv_rf's
            // wait=None arm (still consults base feasibility).
            let extra: [Edge; 2] = [(p, e, 0), (e, p, 0)];
            return self.probe_exact(&extra);
        };
        if subset.is_empty() {
            // Immediate empty: committed form is t_e = t_p exactly.
            // Defensive only: timed inboxes have min >= 1, so their
            // probed subsets are never empty (the timeout empty goes
            // through probe_inbox_timeout, which adds W instead).
            let extra: [Edge; 2] = [(p, e, 0), (e, p, 0)];
            return self.probe_exact(&extra);
        }
        let Some(windows) = subset_windows(&self.vars, self.g, self.cfg, inbox, subset)
        else {
            // Invariant breach tripwire (subsets are drawn from the
            // graph the oracle was built on): keep (never tighter).
            debug_assert!(false, "subset member not in scope");
            return true;
        };
        let sd = i128::from(self.cfg.sd_for(inbox.thread));
        let at_capacity = ilab.max() == Some(subset.len());
        let excl = inbox_exclusions(
            &self.vars,
            self.g,
            self.cfg,
            inbox,
            ilab.recv_loc(),
            ilab.comm(),
            subset,
        );
        let cases: Vec<Vec<Edge>> = if subset.len() > ilab.min() {
            // Superset of min: only an immediate read at t0; excluded
            // stored messages are legal only at capacity.
            let mut c: Vec<Edge> = vec![(e, p, 0), (p, e, 0)];
            for &(_sv, av, _l, _u) in &windows {
                c.push((e, av, 0));
                c.push((av, e, sd));
            }
            fold_exclusions(vec![c], &excl, |_, x| {
                exclusion_options(None, &windows, at_capacity, e, sd, x)
            })
        } else {
            let Some(base) =
                waited_inbox_cases(&self.vars, self.g, self.cfg, e, p, inbox, subset, wait)
            else {
                debug_assert!(false, "subset member not in scope");
                return true;
            };
            // Case 0 is the immediate t0 read; case k+1 completes at
            // member k's arrival. A waited read leaves nothing behind
            // regardless of capacity, so at_capacity applies to the
            // immediate case only.
            fold_exclusions(base, &excl, |i, x| {
                exclusion_options(i.checked_sub(1), &windows, at_capacity, e, sd, x)
            })
        };
        prof::add(&prof::IRF_CASES, cases.len() as u64);
        self.probe_exact_with_set(&[], &cases)
    }

    /// Exact feasibility of the floating finite-wait inbox at `inbox`
    /// timing out (rf = None): an exact clock advance by W, mirroring
    /// the committed encoding. Untimed inboxes are transparent;
    /// infinite waits cannot time out.
    pub(crate) fn probe_inbox_timeout(&mut self, inbox: Event) -> bool {
        let Some(&e) = self.vars.ev.get(&inbox) else {
            return true;
        };
        let pred = Event::new(inbox.thread, inbox.index - 1);
        let Some(&p) = self.vars.ev.get(&pred) else {
            return self.feasible;
        };
        match self.g.inbox_label(inbox).unwrap().wait() {
            None => {
                let extra: [Edge; 2] = [(p, e, 0), (e, p, 0)];
                self.probe_exact(&extra)
            }
            Some(WaitTime::Finite(w)) => {
                let w = i128::from(w);
                let extra: [Edge; 2] = [(p, e, w), (e, p, -w)];
                self.probe_exact(&extra)
            }
            Some(WaitTime::Infinite) => false,
        }
    }

    /// Exact `[lo, hi]` window of one event under the certified case
    /// assignment (reporting paths only; other assignments describe
    /// alternative timelines). `None` hi means unbounded (blocking
    /// read with no cap anywhere above).
    pub(crate) fn exact_bounds(&mut self, e: Event) -> Option<(u64, Option<u64>)> {
        if !self.feasible {
            return None;
        }
        let &v = self.vars.ev.get(&e)?;
        self.ensure_leaf();
        let (lo, hi) = self.leaf.as_ref().unwrap().bounds(v)?;
        Some((
            u64::try_from(lo.max(0)).unwrap_or(u64::MAX),
            hi.map(|h| u64::try_from(h.max(0)).unwrap_or(u64::MAX)),
        ))
    }

    /// One-shot: does the committed graph (or the given view of it,
    /// e.g. a violation's porf prefix) admit any timeline?
    pub(crate) fn graph_feasible(
        g: &ExecutionGraph,
        cfg: &TimedConfig,
        view: Option<&VectorClock>,
    ) -> bool {
        TimedDcs::build(g, cfg, view, None).base_feasible()
    }

    /// One-shot witness timeline for the committed graph (or view).
    pub(crate) fn graph_witness(
        g: &ExecutionGraph,
        cfg: &TimedConfig,
        view: Option<&VectorClock>,
    ) -> Option<Vec<(Event, u64)>> {
        let mut dcs = TimedDcs::build(g, cfg, view, None);
        if !dcs.feasible {
            return None;
        }
        dcs.ensure_leaf();
        let w = dcs.leaf.as_ref().unwrap().witness()?;
        let mut out: Vec<(Event, u64)> = dcs
            .vars
            .ev
            .iter()
            .map(|(&ev, &v)| (ev, u64::try_from(w[v as usize].max(0)).unwrap_or(u64::MAX)))
            .collect();
        out.sort();
        Some(out)
    }
}

/// Edges of a committed timed read (shared by the base derivation):
/// the read happens while the message is stored, `a_s <= t_e <= a_s +
/// sd` (the arrival window itself lives in the base edges).
#[allow(clippy::too_many_arguments)]
fn self_read_edges(
    edges: &mut Vec<Edge>,
    vars: &ScopeVars,
    _g: &ExecutionGraph,
    cfg: &TimedConfig,
    e: u32,
    p: u32,
    pos: Event,
    s: Event,
    wait: WaitTime,
    infeasible: &mut bool,
) {
    edges.push((e, p, 0));
    if let WaitTime::Finite(w) = wait {
        edges.push((p, e, i128::from(w)));
    }
    let Some(&av) = vars.arr.get(&s) else {
        // rf source outside the view: the read cannot be scheduled in
        // this cut; mark infeasible. Reachable benignly on tiebreak
        // views that still contain a previously revisited receive
        // whose stamp-later source was cut (the revisit is rejected by
        // other maximality checks either way), so no assert here.
        *infeasible = true;
        return;
    };
    let sd = i128::from(cfg.sd_for(pos.thread));
    edges.push((e, av, 0)); // a_s <= t_e
    edges.push((av, e, sd)); // t_e <= a_s + sd
}

/// Per-member `(send var, arrival var, L, U)` of an inbox subset, or
/// `None` when a member send is outside the view (a committed read
/// whose source is outside the view cannot be scheduled in this cut;
/// never silently under-constrain). The send var + U are kept for the
/// projected co-arrival corner option (see `exclusion_options`).
fn subset_windows(
    vars: &ScopeVars,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    _pos: Event,
    subset: &[Event],
) -> Option<Vec<(u32, u32, Time, Time)>> {
    subset
        .iter()
        .map(|&s| {
            // Outside the view: same benign-on-tiebreak-views contract
            // as self_read_edges; the caller marks infeasible.
            let &sv = vars.ev.get(&s)?;
            let &av = vars.arr.get(&s)?;
            let (l, u) = g
                .send_label(s)
                .and_then(|slab| slab.transit())
                .unwrap_or((cfg.l, cfg.u));
            Some((sv, av, i128::from(l), i128::from(u)))
        })
        .collect()
}

/// Edges of a committed IMMEDIATE inbox read of more than `min`
/// members: the read happened at t0 with every member already stored
/// and still alive (`a_s <= t_e <= a_s + sd` each).
#[allow(clippy::too_many_arguments)]
fn immediate_inbox_edges(
    edges: &mut Vec<Edge>,
    vars: &ScopeVars,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    e: u32,
    p: u32,
    pos: Event,
    subset: &[Event],
    infeasible: &mut bool,
) {
    let Some(windows) = subset_windows(vars, g, cfg, pos, subset) else {
        *infeasible = true;
        return;
    };
    edges.push((e, p, 0));
    edges.push((p, e, 0));
    let sd = i128::from(cfg.sd_for(pos.thread));
    for &(_sv, av, _l, _u) in &windows {
        edges.push((e, av, 0));
        edges.push((av, e, sd));
    }
}

/// Alternative edge sets of a committed WAITED min-sized inbox read
/// (`t_e = max(t_p, completing arrival)`): index 0 is the immediate
/// read at t0; index 1+k is "member k's arrival completed the batch"
/// (the read happens AT that arrival, `t_e = a_k`, so k gets no
/// storage slack, while every other member need only be stored and
/// alive). Returns `None` when a member send is outside the view.
#[allow(clippy::too_many_arguments)]
fn waited_inbox_cases(
    vars: &ScopeVars,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    e: u32,
    p: u32,
    pos: Event,
    subset: &[Event],
    wait: WaitTime,
) -> Option<Vec<Vec<Edge>>> {
    let windows = subset_windows(vars, g, cfg, pos, subset)?;
    let sd = i128::from(cfg.sd_for(pos.thread));
    let mut cases: Vec<Vec<Edge>> = Vec::with_capacity(1 + windows.len());
    let mut immediate: Vec<Edge> = Vec::with_capacity(2 + 2 * windows.len());
    immediate.push((e, p, 0));
    immediate.push((p, e, 0));
    for &(_sv, av, _l, _u) in &windows {
        immediate.push((e, av, 0));
        immediate.push((av, e, sd));
    }
    cases.push(immediate);
    for (k, &(_svk, avk, _lk, _uk)) in windows.iter().enumerate() {
        let mut case: Vec<Edge> = Vec::with_capacity(2 + 2 * windows.len());
        case.push((e, p, 0)); // t_e >= t_p
        if let WaitTime::Finite(w) = wait {
            case.push((p, e, i128::from(w))); // t_e <= t_p + W
        }
        // The completing member: the read is AT its arrival.
        case.push((e, avk, 0)); // t_e >= a_k
        case.push((avk, e, 0)); // t_e <= a_k (no sd!)
        for (j, &(_sv, av, _l, _u)) in windows.iter().enumerate() {
            if j != k {
                case.push((e, av, 0));
                case.push((av, e, sd));
            }
        }
        cases.push(case);
    }
    Some(cases)
}

/// Dead-before-the-wait edges of a GC refusal block: every matching
/// unconsumed send (present or future: the encoding runs on whatever
/// the graph holds when the oracle is built) must be fully dead before
/// the blocked receive's wait begins (t_s + L + sd < t_block). Sends
/// consumed by OTHER readers are how a competing consumer legitimately
/// takes a message away and are exempt; a send read by the refusing
/// position itself (a leftover rf being converted) is not.
///
/// This is THE single refusal collector, shared by recv-shaped and
/// inbox-shaped refusals at both push-probe time (`probe_gc_block`)
/// and committed-encoding time (the Block arm of `build`). Its
/// exemption set deliberately differs from `inbox_exclusions` (no
/// GC-evicted exemption; the reader nuance above; cancellation checked
/// here): every delta is STRICTLY STRONGER, so it can only shrink the
/// refusal class, never admit a spurious refusal, and the same
/// conjunction is applied symmetrically at push and encode. For
/// min >= 2 inboxes the all-dead conjunction is intentionally
/// over-strong on both sides (staged design: "count never reaches
/// min" also holds for disjoint-lifetime timelines this rule
/// rejects). Widening it (e.g. adopting the evicted exemption) must
/// change every site at once, or push and encode disagree.
#[allow(clippy::too_many_arguments)]
fn push_refusal_edges(
    edges: &mut Vec<Edge>,
    vars: &ScopeVars,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    pos: Event,
    anchor: &crate::event_label::EventLabel,
    loc: &crate::loc::RecvLoc,
    e: u32,
) {
    let sd = i128::from(cfg.sd_for(pos.thread));
    for b in g.matching_stores(loc) {
        let bpos = b.pos();
        let Some(&av) = vars.arr.get(&bpos) else {
            continue;
        };
        if b.reader().is_some_and(|r| r != pos && vars.ev.contains_key(&r)) {
            continue;
        }
        if b.monitor_readers()
            .iter()
            .any(|&mr| mr.thread == pos.thread && vars.ev.contains_key(&mr))
        {
            continue;
        }
        if b.is_cancelled_wrt(anchor) {
            continue;
        }
        edges.push((e, av, -(sd + 1))); // a_b + sd < t_block
    }
}

/// Cap on the folded exclusion expansion per read; beyond it the
/// remaining excluded sends are dropped (looser, never tighter).
const EXCLUSION_FOLD_CAP: usize = 4096;

/// One excluded matching send's dodge data for the completion-count
/// rule (audit 2026-08-09). A waited inbox read at t_e is legal only
/// if the stored matching count stayed BELOW min at every instant
/// before t_e, and an immediate t0 read returns every stored message
/// (capacity permitting). Excluded matching sends previously carried
/// no constraints at all, so batches no operational run produces were
/// explored, and asserts on them yielded certified false
/// counterexamples (the porf-prefix witness simply omitted the
/// excluded sender's thread).
struct InboxExclusion {
    /// The excluded send's ARRIVAL variable (the dodge options bind
    /// the arrival directly; the transit window lives in the base).
    av: u32,
}

/// Collect the excluded matching sends that constrain an inbox-shaped
/// read of `subset` at `pos`. `vars` membership stands in for the
/// view (build interns exactly the view's events). Guards mirror
/// `push_inbox_skip_cases`: consumed and monitor-consumed sends are
/// no longer stored; under GC comm models an already-evicted send
/// (some read in view passed over it) is out of the mailbox, so its
/// count constraint is dropped (looser when the skipper is
/// concurrent, never tighter).
fn inbox_exclusions(
    vars: &ScopeVars,
    g: &ExecutionGraph,
    _cfg: &TimedConfig,
    pos: Event,
    loc: &crate::loc::RecvLoc,
    comm: crate::loc::CommunicationModel,
    subset: &[Event],
) -> Vec<InboxExclusion> {
    let gc = comm != crate::loc::CommunicationModel::NoOrder
        && comm != crate::loc::CommunicationModel::TotalOrder;
    let mut out = Vec::new();
    for b in g.matching_stores(loc) {
        let bpos = b.pos();
        if subset.contains(&bpos) {
            continue;
        }
        let Some(&av) = vars.arr.get(&bpos) else {
            continue; // outside the view: cannot be stored in this cut
        };
        if b.reader().is_some_and(|r| vars.ev.contains_key(&r)) {
            continue; // consumed in this view: no longer stored
        }
        if b.monitor_readers()
            .iter()
            .any(|&mr| mr.thread == pos.thread && vars.ev.contains_key(&mr))
        {
            continue;
        }
        if gc {
            let evicted = g.get_thr(&bpos.thread).labels[(bpos.index as usize + 1)..]
                .iter()
                .any(|lab2| {
                    let LabelEnum::SendMsg(s2) = lab2 else {
                        return false;
                    };
                    s2.sb().contains(bpos)
                        && s2.reader().is_some_and(|r2| {
                            r2 != pos
                                && vars.ev.contains_key(&r2)
                                // Full parity with send_overtaken_in_view:
                                // the skipper's reader must be one that
                                // COULD have read b (channel + predicate),
                                // or no eviction of b ever happened.
                                && match g.label(r2) {
                                    LabelEnum::RecvMsg(rl) => rl.matches(b),
                                    LabelEnum::Inbox(il) => il.matches(b),
                                    _ => false,
                                }
                        })
                });
            if evicted {
                continue;
            }
        }
        out.push(InboxExclusion { av });
    }
    out
}

/// Fold per-exclusion dodge alternatives into a list of read cases.
/// `options_for(i, x)` returns the single-edge alternatives legal in
/// case `i` (None = x unconstrained there). Alternatives: (A) x
/// arrives at/after the read (t_e <= s_x + U_x for a completion read,
/// minus 1 for an immediate one: not stored AT t0), (B) x is fully
/// dead before the read (t_e >= s_x + L_x + sd + 1), and for
/// completion cases (C_m) some OTHER member co-arrives exactly at the
/// read tick, so the count still first reaches min at t_e with x
/// stored (integer-time jump corner; without it true simultaneity
/// timelines would be over-pruned). The union covers every legal
/// timeline (never tighter); B and C admit some illegal transient
/// co-storage timelines, so the rule is exact up to that documented
/// slack.
fn fold_exclusions(
    cases: Vec<Vec<Edge>>,
    excl: &[InboxExclusion],
    mut options_for: impl FnMut(usize, &InboxExclusion) -> Option<Vec<Edge>>,
) -> Vec<Vec<Edge>> {
    if excl.is_empty() {
        return cases;
    }
    let mut out: Vec<Vec<Edge>> = Vec::new();
    for (i, case) in cases.into_iter().enumerate() {
        let mut partial: Vec<Vec<Edge>> = vec![case];
        for x in excl {
            let Some(alts) = options_for(i, x) else {
                continue;
            };
            if alts.is_empty()
                || partial.len().saturating_mul(alts.len()) > EXCLUSION_FOLD_CAP
            {
                break; // drop remaining exclusions here: looser only
            }
            let mut next = Vec::with_capacity(partial.len() * alts.len());
            for pcase in &partial {
                for &alt in &alts {
                    let mut c = pcase.clone();
                    c.push(alt);
                    next.push(c);
                }
            }
            partial = next;
        }
        out.extend(partial);
    }
    out
}

/// The standard `options_for` closure body shared by every inbox-read
/// site: `comp` = Some(k) when the case reads at member k's arrival.
/// Alternatives bind the excluded send's ARRIVAL variable: (A) x
/// arrives at/after the read (`a_x >= t_e`; strictly after for an
/// immediate read, which collects everything stored AT t0), (B) x is
/// fully dead before the read (`a_x + sd < t_e`), and for completion
/// cases (C_m) the projected co-arrival corner (unchanged form,
/// documented slack).
fn exclusion_options(
    comp: Option<usize>,
    windows: &[(u32, u32, Time, Time)],
    at_capacity: bool,
    e: u32,
    sd: Time,
    x: &InboxExclusion,
) -> Option<Vec<Edge>> {
    match comp {
        // Immediate t0 read: at capacity the read may leave stored
        // messages behind, so x is unconstrained.
        None if at_capacity => None,
        None => Some(vec![(x.av, e, -1), (e, x.av, -(sd + 1))]),
        Some(k) => {
            let mut v = vec![(x.av, e, 0), (e, x.av, -(sd + 1))];
            for (m, &(sv, _av, _l, u)) in windows.iter().enumerate() {
                if m != k {
                    v.push((sv, e, u)); // C_m: co-arrival at the read tick
                }
            }
            Some(v)
        }
    }
}

/// GC-semantics honesty cases for a committed receive that SKIPPED older
/// unread same-channel messages (read s while an sb-earlier matching send
/// b stayed unread). The skip is legal only in timelines where b was
/// NEVER readable during the wait: b arrives strictly after the read
/// (`t_e <= a_b - 1`) or is fully dead before the wait began
/// (`a_b + sd < t_p`; a waiting receive takes a front the instant it
/// becomes readable, so mid-wait readability forbids the skip). Same
/// pair as the offer-side probe_dodging_fronts, always emitted (FIFO
/// coupling can pin a_b, so no wide-window shortcut). Ordering uses
/// the send's sb (the same order retain_sb_minimals consults for
/// plain reads); omitting a monitor-order skip only loosens, never
/// tightens.
#[allow(clippy::too_many_arguments)]
fn push_recv_skip_cases(
    case_sets: &mut Vec<Vec<Vec<Edge>>>,
    vars: &ScopeVars,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    view: &VectorClock,
    e: u32,
    pos: Event,
    rlab: &RecvMsg,
    source: Event,
) {
    if rlab.comm() == crate::loc::CommunicationModel::NoOrder
        || rlab.comm() == crate::loc::CommunicationModel::TotalOrder
    {
        return; // no order to invert / Mailbox exempt from GC skips
    }
    let Some(src_lab) = g.send_label(source) else {
        return;
    };
    let pred = Event::new(pos.thread, pos.index - 1);
    let Some(&p) = vars.ev.get(&pred) else {
        return; // defensive: dropping the groups only loosens
    };
    let matching: Vec<&crate::event_label::SendMsg> =
        g.matching_stores(rlab.recv_loc()).collect();
    let sd = i128::from(cfg.sd_for(pos.thread));
    for &b in &matching {
        let bpos = b.pos();
        if bpos == source || !src_lab.sb().contains(bpos) {
            continue; // only messages ordered before the read's source
        }
        if !view.contains(bpos) {
            continue;
        }
        if b.is_cancelled_wrt(rlab.as_event_label()) {
            continue;
        }
        if b.reader().is_some_and(|r| view.contains(r)) {
            continue; // read in this view: not skipped
        }
        if b
            .monitor_readers()
            .iter()
            .any(|&mr| mr.thread == pos.thread && view.contains(mr))
        {
            continue; // consumed by this monitor already: not a skip
        }
        // First-skipper discipline: only the porf-earliest read that
        // passed over b carries the dodge obligation; b is evicted for
        // everyone after (their read times need not dodge it).
        // Scan b's OWN SENDER's later sends so cross-predicate earlier
        // skippers also suppress, requiring FULL parity with
        // send_overtaken_in_view: the skipper's reader must be one
        // that COULD have read b (channel + predicate), or no
        // eviction of b ever happened.
        let earlier_skipper = g.get_thr(&bpos.thread).labels[(bpos.index as usize + 1)..]
            .iter()
            .any(|lab2| {
                let LabelEnum::SendMsg(s2) = lab2 else {
                    return false;
                };
                s2.sb().contains(bpos)
                    && s2.reader().is_some_and(|r2| {
                        r2 != pos
                            && view.contains(r2)
                            && g.in_porf(r2, pos)
                            && match g.label(r2) {
                                LabelEnum::RecvMsg(rl) => rl.matches(b),
                                LabelEnum::Inbox(il) => il.matches(b),
                                _ => false,
                            }
                    })
            });
        if earlier_skipper {
            continue;
        }
        // No "wide window = always dodgeable" shortcut: FIFO coupling
        // can pin b's arrival through its channel neighbors, so the
        // dodge disjunction must always be enforced (the pair is
        // exhaustive in arrival space). Anchors mirror the operational
        // rule (and the offer probe probe_dodging_fronts): a waiting
        // receive takes a front the instant it becomes readable, so
        // the skip needs b to arrive strictly after the read, or be
        // fully dead before the WAIT BEGAN (t_p), not merely before
        // the read.
        let Some(&av) = vars.arr.get(&bpos) else {
            continue;
        };
        case_sets.push(vec![
            vec![(av, e, -1)],        // read strictly before b arrives
            vec![(p, av, -(sd + 1))], // b dead before the wait began
        ]);
    }
}

/// Inbox sibling of `push_recv_skip_cases`: a committed inbox batch
/// that excluded an sb-earlier unread matching send b (GC skip) must
/// not have been able to see b at its read in the witness timeline.
/// Same dodge disjunction and guards as the recv version; b qualifies
/// when it is ordered before SOME included member.
#[allow(clippy::too_many_arguments)]
fn push_inbox_skip_cases(
    case_sets: &mut Vec<Vec<Vec<Edge>>>,
    vars: &ScopeVars,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    view: &VectorClock,
    e: u32,
    pos: Event,
    ilab: &crate::event_label::Inbox,
    subset: &[Event],
) {
    if ilab.comm() == crate::loc::CommunicationModel::NoOrder
        || ilab.comm() == crate::loc::CommunicationModel::TotalOrder
    {
        return;
    }
    let member_sbs: Vec<_> = subset
        .iter()
        .filter_map(|&m| g.send_label(m).map(|sl| sl.sb()))
        .collect();
    let pred = Event::new(pos.thread, pos.index - 1);
    let Some(&p) = vars.ev.get(&pred) else {
        return; // defensive: dropping the groups only loosens
    };
    let matching: Vec<&crate::event_label::SendMsg> =
        g.matching_stores(ilab.recv_loc()).collect();
    let sd = i128::from(cfg.sd_for(pos.thread));
    for &b in &matching {
        let bpos = b.pos();
        if subset.contains(&bpos) || !view.contains(bpos) {
            continue;
        }
        if !member_sbs.iter().any(|sb| sb.contains(bpos)) {
            continue; // not ordered before any included member
        }
        if b.reader().is_some_and(|r| view.contains(r)) {
            continue;
        }
        if b
            .monitor_readers()
            .iter()
            .any(|&mr| mr.thread == pos.thread && view.contains(mr))
        {
            continue;
        }
        // Scan b's OWN SENDER's later sends so cross-predicate earlier
        // skippers also suppress; the skipper's reader must have been
        // ABLE to read b (full parity with send_overtaken_in_view).
        let earlier_skipper = g.get_thr(&bpos.thread).labels[(bpos.index as usize + 1)..]
            .iter()
            .any(|lab2| {
                let LabelEnum::SendMsg(s2) = lab2 else {
                    return false;
                };
                s2.sb().contains(bpos)
                    && s2.reader().is_some_and(|r2| {
                        r2 != pos
                            && view.contains(r2)
                            && g.in_porf(r2, pos)
                            && match g.label(r2) {
                                LabelEnum::RecvMsg(rl) => rl.matches(b),
                                LabelEnum::Inbox(il) => il.matches(b),
                                _ => false,
                            }
                    })
            });
        if earlier_skipper {
            continue;
        }
        // See push_recv_skip_cases: no wide-window shortcut under FIFO
        // coupling, and the dead dodge anchors at the WAIT START.
        let Some(&av) = vars.arr.get(&bpos) else {
            continue;
        };
        case_sets.push(vec![
            vec![(av, e, -1)],
            vec![(p, av, -(sd + 1))],
        ]);
    }
}

/// TEMPORARY profiling counters, env-gated (TF_DCS_PROF=1); dumped at
/// the end of `verify`. Strip after the perf investigation.
pub(crate) mod prof {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::sync::OnceLock;

    pub static IRF_CALLS: AtomicU64 = AtomicU64::new(0);
    pub static IRF_NS: AtomicU64 = AtomicU64::new(0);
    pub static IRF_CASES: AtomicU64 = AtomicU64::new(0);
    pub static PE_CALLS: AtomicU64 = AtomicU64::new(0);
    pub static PE_FAST_OK: AtomicU64 = AtomicU64::new(0);
    pub static PE_KILL: AtomicU64 = AtomicU64::new(0);
    pub static PE_SEARCH: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_CALLS: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_FAST_OK: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_BASE_KILL: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_ALT_KILL: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_SEARCH: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_CHEAP_PROBES: AtomicU64 = AtomicU64::new(0);
    pub static PEWS_NS: AtomicU64 = AtomicU64::new(0);
    pub static CS_RUNS: AtomicU64 = AtomicU64::new(0);
    pub static CS_NODES: AtomicU64 = AtomicU64::new(0);
    pub static CS_CONFLICTS: AtomicU64 = AtomicU64::new(0);
    pub static CS_NS: AtomicU64 = AtomicU64::new(0);
    pub static CS_XTRACT_NS: AtomicU64 = AtomicU64::new(0);
    pub static RETAIN_CALLS: AtomicU64 = AtomicU64::new(0);
    pub static RETAIN_SUBSETS: AtomicU64 = AtomicU64::new(0);
    pub static RETAIN_KEPT: AtomicU64 = AtomicU64::new(0);
    pub static RETAIN_NS: AtomicU64 = AtomicU64::new(0);
    pub static FILTER_NS: AtomicU64 = AtomicU64::new(0);

    #[inline]
    pub fn on() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| {
            let on = std::env::var_os("TF_DCS_PROF").is_some();
            if on {
                // Periodic dump so killed/timeouted runs still report.
                std::thread::spawn(|| loop {
                    std::thread::sleep(std::time::Duration::from_secs(30));
                    dump();
                });
            }
            on
        })
    }

    #[inline]
    pub fn add(c: &AtomicU64, v: u64) {
        if on() {
            c.fetch_add(v, Relaxed);
        }
    }

    pub struct Timer(Option<std::time::Instant>, &'static AtomicU64);
    impl Timer {
        #[inline]
        pub fn start(c: &'static AtomicU64) -> Self {
            Timer(if on() { Some(std::time::Instant::now()) } else { None }, c)
        }
    }
    impl Drop for Timer {
        #[inline]
        fn drop(&mut self) {
            if let Some(t) = self.0 {
                self.1.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
            }
        }
    }

    pub fn dump() {
        if !on() {
            return;
        }
        let g = |c: &AtomicU64| c.load(Relaxed);
        let ms = |c: &AtomicU64| g(c) / 1_000_000;
        eprintln!(
            "TF_DCS_PROF inbox_rfs: calls={} ms={} cases={}",
            g(&IRF_CALLS), ms(&IRF_NS), g(&IRF_CASES)
        );
        eprintln!(
            "TF_DCS_PROF probe_exact: calls={} fast_ok={} kill={} search={}",
            g(&PE_CALLS), g(&PE_FAST_OK), g(&PE_KILL), g(&PE_SEARCH)
        );
        eprintln!(
            "TF_DCS_PROF probe_exact_with_set: calls={} ms={} fast_ok={} base_kill={} alt_kill={} search={} cheap_probes={}",
            g(&PEWS_CALLS), ms(&PEWS_NS), g(&PEWS_FAST_OK), g(&PEWS_BASE_KILL),
            g(&PEWS_ALT_KILL), g(&PEWS_SEARCH), g(&PEWS_CHEAP_PROBES)
        );
        eprintln!(
            "TF_DCS_PROF case_search: runs={} ms={} nodes={} conflicts={} xtract_ms={}",
            g(&CS_RUNS), ms(&CS_NS), g(&CS_NODES), g(&CS_CONFLICTS), ms(&CS_XTRACT_NS)
        );
        eprintln!(
            "TF_DCS_PROF retain_swarm: calls={} ms={} subsets={} kept={}",
            g(&RETAIN_CALLS), ms(&RETAIN_NS), g(&RETAIN_SUBSETS), g(&RETAIN_KEPT)
        );
        eprintln!("TF_DCS_PROF filter_rfs: ms={}", ms(&FILTER_NS));
    }
}

/// FIFO coupling edges for one channel's delivered sends (2026-08-28
/// decision): `a_b1 <= a_b2` for every same-channel pair with b1 in
/// b2's delivery-model order. Same-thread pairs are chained through
/// the NEAREST sb-ordered predecessor only (the per-thread sublist of
/// `sends` is po-ordered and same-thread sb containment is monotone
/// along po in every ordered model, so the chain's transitive DCS
/// paths reproduce every all-pairs constraint with O(k) edges per
/// sender); cross-thread sb pairs (Causal/Total) stay explicit and
/// scan the whole list (thread iteration order is not causal order).
/// Equivalence with the all-pairs encoding is pinned by
/// `chained_coupling_matches_all_pairs`.
fn fifo_coupling_edges(
    sends: &[Event],
    sb_contains: impl Fn(Event, Event) -> bool,
    arr: impl Fn(Event) -> u32,
    edges: &mut Vec<Edge>,
) {
    for (i, &b2) in sends.iter().enumerate() {
        // Nearest sb-contained same-thread predecessor = the chain
        // link. Farther contained predecessors are skipped only when
        // the link itself contains them (then the link's OWN iteration
        // covers them inductively); on a channel mixing ordered and
        // Bag sends from one thread the link can be a Bag send whose
        // sb is empty, and the farther pair must then stay explicit.
        let mut link: Option<Event> = None;
        for &b1 in sends[..i].iter().rev() {
            if b1.thread != b2.thread || !sb_contains(b2, b1) {
                continue;
            }
            match link {
                None => {
                    edges.push((arr(b2), arr(b1), 0)); // a_b1 <= a_b2
                    link = Some(b1);
                }
                Some(m) => {
                    if !sb_contains(m, b1) {
                        edges.push((arr(b2), arr(b1), 0));
                    }
                }
            }
        }
        // Cross-thread sb predecessors can sit anywhere in the list
        // (thread iteration order is not causal order).
        for &b1 in sends {
            if b1.thread != b2.thread && sb_contains(b2, b1) {
                edges.push((arr(b2), arr(b1), 0));
            }
        }
    }
}

/// Bound on the learned no-good store per oracle instance (a runaway
/// safety valve, far above anything real; overflow just stops
/// learning, which is always sound).
const NOGOOD_CAP: usize = 10_000;

/// Upper bound on n * m for running the O(n*m) conflict extraction at
/// LEAF failures (interior failures always extract: they kill whole
/// subtrees). Above it, leaves fall back to chronological next-option,
/// which is sound (see the leaf comment in `CaseSearcher::dfs`).
const LEAF_EXTRACT_MAX_WORK: u64 = 8_000_000;

/// Conflict-driven search over the case groups (CDCL-lite).
///
/// The brute-force predecessor walked the full cross-product of group
/// options on every rejection, which is exponential in the number of
/// committed batch reads and was measured turning 8-second benchmark
/// cells into >5h DNFs. This driver keeps the same DFS skeleton but,
/// on every failed probe, extracts a negative-cycle certificate
/// (`DcsCore::probe_conflict`) and acts on its shape:
///   - cycle in base (+ caller extras) only: NO assignment can ever
///     succeed: the whole search aborts with one certificate;
///   - cycle implicating assigned options: learn the combination as a
///     no-good and backjump to the deepest implicated group, skipping
///     the siblings of innocent deeper groups (sound: the no-good's
///     literals are untouched by them).
/// No-goods over committed groups only are kept for the oracle's
/// lifetime in `TimedDcs::nogoods`; conflicts conditioned on the
/// caller extras or the transient group live only for one invocation.
/// Learning is only ever done from verified certificates, so the
/// search can never reject an assignment the brute force would accept
/// (the never-tighter invariant); a differential test enforces this.
struct CaseSearcher<'a> {
    core: &'a mut DcsCore,
    /// Pristine clone taken BEFORE any trail commit, for conflict
    /// extraction: certificates must see the prefix edges as extras
    /// (attributable to depths), never as base edges, or a prefix
    /// cycle would misclassify as base-only and over-prune.
    xcore: DcsCore,
    /// Per-depth trail state: `edges.len()` mark and saved potentials
    /// for rolling back that depth's commit.
    marks: Vec<usize>,
    saved_pi: Vec<Vec<Time>>,
    sets: &'a [Vec<Vec<Edge>>],
    transient: Option<&'a [Vec<Edge>]>,
    extra: &'a [Edge],
    choice: Vec<usize>,
    acc: Vec<Edge>,
    /// Per assigned depth: the contiguous slice of `acc` holding that
    /// depth's chosen option (the DFS discipline guarantees
    /// contiguity).
    ranges: Vec<(usize, usize)>,
    persistent: &'a mut Vec<Vec<(u32, u32)>>,
    local: Vec<Vec<(u32, u32)>>,
    /// Cause of the most recent same-depth refutation (read by the
    /// parent loop when it receives Jump(==depth)): the implicated
    /// ANCESTOR depths (strictly below the jump target), and whether
    /// that cause is precise (extraction ran) or conservative.
    cause_anc: Vec<u32>,
    cause_precise: bool,
}

enum Step {
    Sat,
    Unsat,
    /// Backjump: pop frames without trying siblings until this depth,
    /// where the next option is tried.
    Jump(usize),
}

impl<'a> CaseSearcher<'a> {
    fn new(
        core: &'a mut DcsCore,
        sets: &'a [Vec<Vec<Edge>>],
        transient: Option<&'a [Vec<Edge>]>,
        extra: &'a [Edge],
        persistent: &'a mut Vec<Vec<(u32, u32)>>,
        seed_local: Option<Vec<(u32, u32)>>,
    ) -> Self {
        let total = sets.len() + usize::from(transient.is_some());
        let xcore = core.clone();
        Self {
            core,
            xcore,
            marks: vec![0; total],
            saved_pi: vec![Vec::new(); total],
            sets,
            transient,
            extra,
            choice: vec![0; total],
            acc: Vec::new(),
            ranges: vec![(0, 0); total],
            persistent,
            local: seed_local.into_iter().collect(),
            cause_anc: Vec::new(),
            cause_precise: false,
        }
    }

    fn total(&self) -> usize {
        self.sets.len() + usize::from(self.transient.is_some())
    }

    fn group(&self, d: usize) -> &'a [Vec<Edge>] {
        if d < self.sets.len() {
            &self.sets[d]
        } else {
            self.transient.expect("depth beyond committed groups")
        }
    }

    /// Would choosing (d, ci) complete a learned no-good under the
    /// current partial assignment? Returns the ancestor depths of the
    /// matched no-good (for exhaustion-time backjumping).
    fn blocked(&self, d: usize, ci: usize) -> Option<Vec<u32>> {
        let lit = (d as u32, ci as u32);
        let hit = |ng: &&Vec<(u32, u32)>| {
            ng.last() == Some(&lit)
                && ng[..ng.len() - 1]
                    .iter()
                    .all(|&(gd, go)| self.choice[gd as usize] == go as usize)
        };
        self.persistent
            .iter()
            .find(hit)
            .or_else(|| self.local.iter().find(hit))
            .map(|ng| ng[..ng.len() - 1].iter().map(|&(gd, _)| gd).collect())
    }

    /// A probe of the current prefix (depths 0..=depth assigned)
    /// failed: extract the certificate and classify it.
    fn analyze(&mut self, depth: usize) -> Step {
        prof::add(&prof::CS_CONFLICTS, 1);
        let keep = self.acc.len();
        self.acc.extend_from_slice(self.extra);
        // Extraction runs on the PRISTINE clone (see `xcore`): the
        // main core carries trail commits during the search.
        let verdict = {
            let _t = prof::Timer::start(&prof::CS_XTRACT_NS);
            self.xcore.probe_conflict(&self.acc)
        };
        self.acc.truncate(keep);
        let idx = match verdict {
            Conflict::Infeasible(idx) => idx,
            // Extractor disagreement with the incremental probe:
            // treat as a plain chronological failure, learn nothing.
            Conflict::Feasible => {
                self.cause_anc.clear();
                self.cause_precise = false;
                return Step::Jump(depth);
            }
        };
        let extras_involved = idx.iter().any(|&i| i >= keep);
        let mut lits: Vec<(u32, u32)> = idx
            .iter()
            .filter(|&&i| i < keep)
            .map(|&i| {
                let d = self.ranges[..=depth]
                    .iter()
                    .position(|&(s, e)| s <= i && i < e)
                    .expect("acc index outside every assigned range");
                (d as u32, self.choice[d] as u32)
            })
            .collect();
        lits.sort_unstable();
        lits.dedup();
        if lits.is_empty() {
            // The cycle lives in base (+ caller extras): no group
            // choice anywhere can break it.
            return Step::Unsat;
        }
        let deepest = lits.last().unwrap().0 as usize;
        self.cause_anc.clear();
        self.cause_anc
            .extend(lits[..lits.len() - 1].iter().map(|&(gd, _)| gd));
        self.cause_precise = true;
        let transient_involved = deepest >= self.sets.len();
        if extras_involved || transient_involved {
            self.local.push(lits);
        } else if self.persistent.len() < NOGOOD_CAP {
            self.persistent.push(lits);
        }
        Step::Jump(deepest)
    }

    fn dfs(&mut self, depth: usize) -> Step {
        prof::add(&prof::CS_NODES, 1);
        let total = self.total();
        if depth == total {
            // The whole prefix is trail-committed; only the caller
            // extras remain to check (their probe seeds just those
            // few edges: the prefix costs nothing here).
            let ok = self.extra.is_empty() || self.core.probe(self.extra);
            if ok {
                return Step::Sat;
            }
            // A leaf failure necessarily involves the LAST group's
            // option (the commit at each depth already verified the
            // prefix), so chronological next-option is a sound
            // fallback. Extraction still pays for itself twice:
            // the learned pair no-goods convert repeated later
            // failures into lookups, and the precise ancestor set
            // feeds exhaustion-time backjumping (skipping the sibling
            // product of innocent middle groups). It is skipped only
            // when the system is large enough that O(n*m) per leaf
            // failure would dominate the search it prunes.
            let work = (self.xcore.n as u64)
                .saturating_mul((self.xcore.edges.len() + self.acc.len() + self.extra.len()) as u64);
            if work <= LEAF_EXTRACT_MAX_WORK {
                return self.analyze(total - 1);
            }
            self.cause_anc.clear();
            self.cause_precise = false;
            return Step::Jump(total - 1);
        }
        // Exhaustion bookkeeping for conflict-directed backjumping:
        // the deepest ancestor implicated by ANY option's refutation,
        // and whether every refutation had a precise cause.
        let mut exh_max: i64 = -1;
        let mut exh_precise = true;
        for ci in 0..self.group(depth).len() {
            if let Some(anc) = self.blocked(depth, ci) {
                for a in anc {
                    exh_max = exh_max.max(a as i64);
                }
                continue;
            }
            let keep = self.acc.len();
            let case: &'a [Edge] = &self.group(depth)[ci];
            self.acc.extend_from_slice(case);
            self.ranges[depth] = (keep, self.acc.len());
            self.choice[depth] = ci;
            // Trail-commit this depth's option: the feasibility check
            // pays only for the option's own cone (the committed
            // prefix lives in the potentials), where the pre-trail
            // code re-seeded the whole prefix at every node.
            self.marks[depth] = self.core.edges.len();
            let mut saved = std::mem::take(&mut self.saved_pi[depth]);
            let committed = self.core.commit(case, &mut saved);
            self.saved_pi[depth] = saved;
            let step = if !committed {
                // Prefix + option infeasible (state untouched).
                self.analyze(depth)
            } else {
                let viable = self.extra.is_empty() || self.core.probe(self.extra);
                let s = if viable {
                    self.dfs(depth + 1)
                } else {
                    self.analyze(depth)
                };
                self.core.rollback(self.marks[depth], &self.saved_pi[depth]);
                s
            };
            self.acc.truncate(keep);
            match step {
                Step::Sat => return Step::Sat,
                Step::Unsat => return Step::Unsat,
                Step::Jump(j) if j < depth => return Step::Jump(j),
                Step::Jump(_) => {
                    // Refuted at this depth: fold the cause into the
                    // exhaustion set, then try the next option.
                    if self.cause_precise {
                        for &a in &self.cause_anc {
                            exh_max = exh_max.max(a as i64);
                        }
                    } else {
                        exh_precise = false;
                    }
                }
            }
        }
        // Every option here is refuted. If every refutation's cause is
        // known and none involves any ancestor, no ancestor choice can
        // revive this group: the whole search is unsatisfiable. If the
        // deepest implicated ancestor is a, siblings of the innocent
        // depths between a and here cannot help either: jump to a.
        if exh_precise {
            self.cause_anc.clear();
            self.cause_precise = true;
            if exh_max < 0 {
                return Step::Unsat;
            }
            // The receiving frame folds OUR cause; report the
            // ancestors below the jump target (unknown here beyond
            // the max: conservative = everything below it).
            self.cause_anc.extend(0..exh_max as u32);
            return Step::Jump(exh_max as usize);
        }
        self.cause_anc.clear();
        self.cause_precise = false;
        if depth == 0 {
            Step::Unsat
        } else {
            Step::Jump(depth - 1)
        }
    }

    /// Run the search; `Some(choice)` iff a satisfying assignment over
    /// (committed groups ++ transient group) exists with `extra`.
    fn run(mut self) -> Option<Vec<usize>> {
        prof::add(&prof::CS_RUNS, 1);
        let _t = prof::Timer::start(&prof::CS_NS);
        let base_edges = self.core.edges.len();
        let step = self.dfs(0);
        // Every frame rolls back its own commit on every exit path;
        // the caller's core must come back pristine.
        debug_assert_eq!(self.core.edges.len(), base_edges, "trail not fully unwound");
        match step {
            Step::Sat => Some(self.choice),
            _ => None,
        }
    }
}

/// Brute-force reference search (the pre-CDCL implementation), kept as
/// the differential-testing oracle: same verdicts, exponential cost.
#[cfg(test)]
fn search_cases(
    core: &mut DcsCore,
    sets: &[Vec<Vec<Edge>>],
    extra: &[Edge],
    depth: usize,
    choice: &mut [usize],
    acc: &mut Vec<Edge>,
) -> bool {
    if depth == sets.len() {
        let keep = acc.len();
        acc.extend_from_slice(extra);
        let ok = core.probe(acc);
        acc.truncate(keep);
        return ok;
    }
    for (ci, case) in sets[depth].iter().enumerate() {
        let keep = acc.len();
        acc.extend_from_slice(case);
        let viable = if depth + 1 == sets.len() {
            true // the leaf probe below decides
        } else {
            let mark = acc.len();
            acc.extend_from_slice(extra);
            let v = core.probe(acc);
            acc.truncate(mark);
            v
        };
        if viable {
            choice[depth] = ci;
            if search_cases(core, sets, extra, depth + 1, choice, acc) {
                acc.truncate(keep);
                return true;
            }
        }
        acc.truncate(keep);
    }
    false
}

// =====================================================================
// Unit tests: pure DcsCore (no graph required)
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn core(n: usize, edges: &[Edge]) -> DcsCore {
        DcsCore::new(n, edges.to_vec())
    }

    /// Run the conflict-driven searcher standalone (fresh no-good
    /// store), returning the satisfying choice if any.
    fn cdcl_search(
        c: &mut DcsCore,
        sets: &[Vec<Vec<Edge>>],
        transient: Option<&[Vec<Edge>]>,
        extra: &[Edge],
    ) -> Option<Vec<usize>> {
        let mut nogoods = Vec::new();
        CaseSearcher::new(c, sets, transient, extra, &mut nogoods, None).run()
    }

    /// A choice satisfies iff base + its case edges + extra probe ok.
    fn choice_satisfies(
        c: &mut DcsCore,
        sets: &[Vec<Vec<Edge>>],
        transient: Option<&[Vec<Edge>]>,
        extra: &[Edge],
        choice: &[usize],
    ) -> bool {
        let mut acc: Vec<Edge> = Vec::new();
        for (d, &ci) in choice.iter().enumerate() {
            let g: &[Vec<Edge>] = if d < sets.len() {
                &sets[d]
            } else {
                transient.unwrap()
            };
            acc.extend_from_slice(&g[ci]);
        }
        acc.extend_from_slice(extra);
        c.probe(&acc)
    }

    /// O(n^3) reference: Floyd-Warshall negative-cycle detection.
    fn reference_feasible(n: usize, edges: &[Edge]) -> bool {
        const INF: Time = i128::MAX / 4;
        let mut d = vec![vec![INF; n]; n];
        for i in 0..n {
            d[i][i] = 0;
        }
        for &(u, v, w) in edges {
            let (u, v) = (u as usize, v as usize);
            if w < d[u][v] {
                d[u][v] = w;
            }
        }
        for k in 0..n {
            for i in 0..n {
                for j in 0..n {
                    if d[i][k] + d[k][j] < d[i][j] {
                        d[i][j] = d[i][k] + d[k][j];
                    }
                }
            }
        }
        (0..n).all(|i| d[i][i] >= 0)
    }

    #[test]
    fn detects_negative_cycle() {
        // t1 >= t0 + 5 and t1 <= t0 + 3: the 5 <= 3 contradiction.
        let c = core(2, &[(1, 0, -5), (0, 1, 3)]);
        assert!(!c.feasible());
        // Relaxed version is fine.
        let c = core(2, &[(1, 0, -2), (0, 1, 3)]);
        assert!(c.feasible());
    }

    #[test]
    fn equality_chain_witness_offsets() {
        // t1 = t0 + 4, t2 = t1 + 3, anchored at t0.
        let c = core(
            3,
            &[(0, 1, 4), (1, 0, -4), (1, 2, 3), (2, 1, -3)],
        );
        assert!(c.feasible());
        let w = c.witness().unwrap();
        assert_eq!(w[1] - w[0], 4);
        assert_eq!(w[2] - w[0], 7);
    }

    #[test]
    fn zero_weight_equality_plus_negative_edge() {
        // t1 = t0 (equality) and t0 <= t1 - 1: negative cycle.
        let c = core(2, &[(0, 1, 0), (1, 0, 0), (1, 0, -1)]);
        assert!(!c.feasible());
    }

    #[test]
    fn probe_is_pure_and_correct() {
        // Base: t1 in [t0+2, t0+6] (via two inequalities).
        let base: Vec<Edge> = vec![(1, 0, -2), (0, 1, 6)];
        let mut c = core(2, &base);
        assert!(c.feasible());
        let pi_before = c.pi.clone();
        // Feasible extra: t1 <= t0 + 3.
        assert!(c.probe(&[(0, 1, 3)]));
        assert_eq!(c.pi, pi_before, "probe must restore potentials");
        // Infeasible extra: t1 <= t0 + 1 (< the +2 lower bound).
        assert!(!c.probe(&[(0, 1, 1)]));
        assert_eq!(c.pi, pi_before, "failed probe must restore potentials");
        // Repeat both: identical answers (no state leak).
        assert!(c.probe(&[(0, 1, 3)]));
        assert!(!c.probe(&[(0, 1, 1)]));
    }

    #[test]
    fn probe_matches_fresh_solve_randomized() {
        // Deterministic xorshift so the test is reproducible.
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut rnd = move |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        for _case in 0..300 {
            let n = 2 + rnd(6) as usize;
            let n_base = rnd(10) as usize;
            let n_extra = 1 + rnd(4) as usize;
            let mut base = Vec::new();
            for _ in 0..n_base {
                let u = rnd(n as u64) as u32;
                let v = rnd(n as u64) as u32;
                let w = rnd(21) as Time - 10;
                if u != v {
                    base.push((u, v, w));
                }
            }
            let mut extra = Vec::new();
            for _ in 0..n_extra {
                let u = rnd(n as u64) as u32;
                let v = rnd(n as u64) as u32;
                let w = rnd(21) as Time - 10;
                if u != v {
                    extra.push((u, v, w));
                }
            }
            let mut c = DcsCore::new(n, base.clone());
            if !c.feasible() {
                assert!(!reference_feasible(n, &base), "solve vs reference (base)");
                continue;
            }
            assert!(reference_feasible(n, &base), "solve vs reference (base)");
            let combined: Vec<Edge> =
                base.iter().chain(extra.iter()).copied().collect();
            let expected = reference_feasible(n, &combined);
            assert_eq!(
                c.probe(&extra),
                expected,
                "probe vs reference on base={base:?} extra={extra:?}"
            );
            // Probe again to confirm purity.
            assert_eq!(c.probe(&extra), expected);
        }
    }

    /// Conflict certificates must be SOUND: whenever the probe rejects,
    /// the extractor must also reject, and the sub-system consisting of
    /// the base plus ONLY the implicated extras must itself be
    /// infeasible per the O(n^3) reference. (Minimality is not
    /// promised; soundness is the invariant everything rests on.)
    #[test]
    fn conflict_certificates_are_sound() {
        let mut state = 0xD1B54A32D192ED03u64;
        let mut rnd = move |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        let mut rejected = 0usize;
        for _case in 0..600 {
            let n = 2 + rnd(6) as usize;
            let n_base = rnd(10) as usize;
            let n_extra = 1 + rnd(6) as usize;
            let mut base = Vec::new();
            for _ in 0..n_base {
                let (u, v) = (rnd(n as u64) as u32, rnd(n as u64) as u32);
                let w = rnd(21) as Time - 10;
                if u != v {
                    base.push((u, v, w));
                }
            }
            let mut extra = Vec::new();
            for _ in 0..n_extra {
                let (u, v) = (rnd(n as u64) as u32, rnd(n as u64) as u32);
                let w = rnd(21) as Time - 10;
                if u != v {
                    extra.push((u, v, w));
                }
            }
            let mut c = DcsCore::new(n, base.clone());
            if !c.feasible() {
                // Base-only conflict: extractor must certify with an
                // empty implicated set.
                match c.probe_conflict(&[]) {
                    Conflict::Infeasible(idx) => assert!(idx.is_empty()),
                    Conflict::Feasible => panic!("solve said infeasible, extractor disagrees"),
                }
                continue;
            }
            if c.probe(&extra) {
                // Feasible per probe: the extractor must agree.
                assert!(
                    matches!(c.probe_conflict(&extra), Conflict::Feasible),
                    "extractor rejected a probe-feasible system: base={base:?} extra={extra:?}"
                );
                continue;
            }
            rejected += 1;
            match c.probe_conflict(&extra) {
                Conflict::Feasible => {
                    panic!("probe rejected but extractor found feasible: base={base:?} extra={extra:?}")
                }
                Conflict::Infeasible(idx) => {
                    // The certificate sub-system must be infeasible on its own.
                    let sub: Vec<Edge> = base
                        .iter()
                        .copied()
                        .chain(idx.iter().map(|&i| extra[i]))
                        .collect();
                    assert!(
                        !reference_feasible(n, &sub),
                        "certificate not self-sufficient: base={base:?} extra={extra:?} idx={idx:?}"
                    );
                }
            }
        }
        assert!(rejected >= 30, "test corpus too easy: only {rejected} rejections");
    }

    /// The incremental extractor must agree with the exact textbook
    /// Bellman-Ford on the VERDICT (certificate content may differ:
    /// different negative cycles are equally valid), and its
    /// certificates must be self-sufficient per the reference oracle.
    #[test]
    fn incremental_extractor_matches_bf() {
        let mut r = TRng(0x9E3779B97F4A7C15);
        let mut rejected = 0usize;
        let mut fallbacks = 0usize;
        for _case in 0..800 {
            let n = 2 + r.next(7) as usize;
            let mut base = Vec::new();
            for _ in 0..r.next(12) {
                // Positive bias keeps most bases feasible so the
                // incremental path (which needs a feasible base) is
                // actually the one under test.
                if let Some(e) = rand_edge(&mut r, n, 3) {
                    base.push(e);
                }
            }
            let mut extra = Vec::new();
            for _ in 0..1 + r.next(8) {
                if let Some(e) = rand_edge(&mut r, n, -2) {
                    extra.push(e);
                }
            }
            let c = DcsCore::new(n, base.clone());
            if !c.feasible() {
                continue;
            }
            let inc = c.probe_conflict_incremental(&extra);
            let bf = c.probe_conflict_bf(&extra);
            match (inc, &bf) {
                (None, _) => fallbacks += 1,
                (Some(Conflict::Feasible), Conflict::Feasible) => {}
                (Some(Conflict::Infeasible(idx)), Conflict::Infeasible(_)) => {
                    rejected += 1;
                    let sub: Vec<Edge> = base
                        .iter()
                        .copied()
                        .chain(idx.iter().map(|&i| extra[i]))
                        .collect();
                    assert!(
                        !reference_feasible(n, &sub),
                        "incremental certificate not self-sufficient: \
                         base={base:?} extra={extra:?} idx={idx:?}"
                    );
                }
                (Some(a), b) => panic!(
                    "verdict disagreement (incremental {:?} vs bf {:?}): base={base:?} extra={extra:?}",
                    matches!(a, Conflict::Feasible),
                    matches!(b, Conflict::Feasible),
                ),
            }
        }
        assert!(rejected >= 80, "corpus too easy: only {rejected} rejections");
        // The incremental path must actually carry the load; constant
        // fallback would silently reintroduce the O(n*m) cost.
        assert!(fallbacks * 10 <= 800, "incremental extractor fell back {fallbacks}/800 times");
    }

    /// The chained FIFO coupling emission must induce EXACTLY the same
    /// order relation as the naive all-pairs emission: compare
    /// transitive closures over randomized channels, including
    /// channels mixing ordered and Bag sends from one thread (the
    /// chain must not break at a Bag link whose own sb is empty).
    #[test]
    fn chained_coupling_matches_all_pairs() {
        use crate::thread::construct_thread_id;
        let mut r = TRng(0xC0FFEE1234);
        for case in 0..2000 {
            let nthreads = 1 + r.next(3) as usize;
            let mut sends: Vec<Event> = Vec::new();
            let mut counts = vec![0u32; nthreads];
            let total = 2 + r.next(7) as usize;
            for _ in 0..total {
                let t = r.next(nthreads as u64) as usize;
                sends.push(Event::new(construct_thread_id(t as u32 + 1), counts[t]));
                counts[t] += 1;
            }
            // Per-send model: ordered (sb = same-thread positional
            // prefix, downward closed like every real ordered model,
            // plus a random cross-thread set) or Bag (empty sb).
            let ordered: Vec<bool> = sends.iter().map(|_| r.next(4) != 0).collect();
            let mut cross: Vec<Vec<usize>> = vec![Vec::new(); sends.len()];
            for i in 0..sends.len() {
                if ordered[i] {
                    for j in 0..sends.len() {
                        if sends[j].thread != sends[i].thread && r.next(4) == 0 {
                            cross[i].push(j);
                        }
                    }
                }
            }
            let idx_of = |e: Event| sends.iter().position(|&s| s == e).unwrap();
            let contains = |b2: Event, b1: Event| -> bool {
                let i2 = idx_of(b2);
                if !ordered[i2] {
                    return false;
                }
                if b1.thread == b2.thread {
                    return b1.index < b2.index;
                }
                cross[i2].contains(&idx_of(b1))
            };
            let mut chained: Vec<Edge> = Vec::new();
            fifo_coupling_edges(&sends, &contains, |e| idx_of(e) as u32, &mut chained);
            let mut allpairs: Vec<Edge> = Vec::new();
            for &b2 in &sends {
                for &b1 in &sends {
                    if b1 != b2 && contains(b2, b1) {
                        allpairs.push((idx_of(b2) as u32, idx_of(b1) as u32, 0));
                    }
                }
            }
            let n = sends.len();
            let close = |es: &[Edge]| -> Vec<Vec<bool>> {
                let mut m = vec![vec![false; n]; n];
                for &(f, t, _) in es {
                    m[f as usize][t as usize] = true;
                }
                for k in 0..n {
                    for i in 0..n {
                        for j in 0..n {
                            if m[i][k] && m[k][j] {
                                m[i][j] = true;
                            }
                        }
                    }
                }
                m
            };
            assert_eq!(
                close(&chained),
                close(&allpairs),
                "case {case}: ordered={ordered:?}"
            );
        }
    }

    /// Tiny xorshift for the randomized tests below (a struct, so
    /// helper fns can borrow it without closure-capture conflicts).
    struct TRng(u64);
    impl TRng {
        fn next(&mut self, m: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % m
        }
    }

    fn rand_edge(r: &mut TRng, n: usize, bias: i128) -> Option<Edge> {
        let u = r.next(n as u64) as u32;
        let v = r.next(n as u64) as u32;
        if u == v {
            return None;
        }
        Some((u, v, r.next(15) as Time - 7 + bias))
    }

    /// The conflict-driven searcher must give EXACTLY the brute-force
    /// verdict on random group systems (with and without a transient
    /// group and caller extras), and any satisfying choice it returns
    /// must actually satisfy. This is the never-tighter guard for the
    /// learning/backjumping machinery.
    #[test]
    fn cdcl_matches_bruteforce_randomized() {
        let mut r = TRng(0x243F6A8885A308D3);
        let mut unsat_seen = 0usize;
        for _case in 0..400 {
            let n = 2 + r.next(5) as usize;
            let mut base = Vec::new();
            for _ in 0..r.next(6) {
                if let Some(e) = rand_edge(&mut r, n, 1) {
                    base.push(e);
                }
            }
            let mut c = DcsCore::new(n, base.clone());
            if !c.feasible() {
                continue;
            }
            let n_groups = 1 + r.next(3) as usize;
            let mut sets: Vec<Vec<Vec<Edge>>> = Vec::new();
            for _ in 0..n_groups {
                let n_opts = 1 + r.next(3) as usize;
                let mut group = Vec::new();
                for _ in 0..n_opts {
                    let mut opt = Vec::new();
                    for _ in 0..1 + r.next(2) {
                        if let Some(e) = rand_edge(&mut r, n, 0) {
                            opt.push(e);
                        }
                    }
                    group.push(opt);
                }
                sets.push(group);
            }
            let transient: Option<Vec<Vec<Edge>>> = if r.next(2) == 0 {
                let mut g = Vec::new();
                for _ in 0..1 + r.next(3) {
                    let mut opt = Vec::new();
                    if let Some(e) = rand_edge(&mut r, n, 0) {
                        opt.push(e);
                    }
                    g.push(opt);
                }
                Some(g)
            } else {
                None
            };
            let mut extra = Vec::new();
            for _ in 0..r.next(3) {
                if let Some(e) = rand_edge(&mut r, n, -2) {
                    extra.push(e);
                }
            }
            // Brute force over committed ++ transient as one list.
            let mut all_sets = sets.clone();
            if let Some(t) = &transient {
                all_sets.push(t.clone());
            }
            let mut choice = vec![0usize; all_sets.len()];
            let mut acc = Vec::new();
            let brute = search_cases(&mut c, &all_sets, &extra, 0, &mut choice, &mut acc);
            let smart = cdcl_search(&mut c, &sets, transient.as_deref(), &extra);
            assert_eq!(
                brute,
                smart.is_some(),
                "verdict mismatch: base={base:?} sets={sets:?} transient={transient:?} extra={extra:?}"
            );
            if let Some(ch) = smart {
                assert!(
                    choice_satisfies(&mut c, &sets, transient.as_deref(), &extra, &ch),
                    "cdcl returned a non-satisfying choice"
                );
            } else {
                unsat_seen += 1;
            }
            // Purity of the shared core across both engines.
            assert!(c.feasible());
        }
        assert!(unsat_seen >= 40, "corpus too easy: only {unsat_seen} unsat cases");
    }

    fn consistent_edge(r: &mut TRng, t: &[Time], slack: u64) -> Option<Edge> {
        let n = t.len();
        let u = r.next(n as u64) as usize;
        let v = r.next(n as u64) as usize;
        if u == v {
            return None;
        }
        Some((u as u32, v as u32, t[v] - t[u] + r.next(slack + 1) as Time))
    }

    /// Feasible-by-construction group systems must NEVER be rejected
    /// by the conflict-driven search (the over-pruning direction is
    /// the unforgivable one). Constructed from a known timeline.
    #[test]
    fn cdcl_never_over_prunes_feasible_by_construction() {
        let mut r = TRng(0x452821E638D01377);
        for _case in 0..300 {
            let n = 2 + r.next(5) as usize;
            // Ground-truth timeline.
            let t: Vec<Time> = (0..n).map(|_| r.next(20) as Time).collect();
            let mut base = Vec::new();
            for _ in 0..r.next(6) {
                if let Some(e) = consistent_edge(&mut r, &t, 3) {
                    base.push(e);
                }
            }
            let mut c = DcsCore::new(n, base);
            assert!(c.feasible());
            // Groups where at least one option is timeline-consistent;
            // other options are arbitrary (possibly contradictory).
            let mut sets: Vec<Vec<Vec<Edge>>> = Vec::new();
            for _ in 0..1 + r.next(3) {
                let n_opts = 1 + r.next(3) as usize;
                let good = r.next(n_opts as u64) as usize;
                let mut group = Vec::new();
                for oi in 0..n_opts {
                    let mut opt = Vec::new();
                    for _ in 0..1 + r.next(2) {
                        let e = if oi == good {
                            consistent_edge(&mut r, &t, 2)
                        } else {
                            rand_edge(&mut r, n, -1)
                        };
                        if let Some(e) = e {
                            opt.push(e);
                        }
                    }
                    group.push(opt);
                }
                sets.push(group);
            }
            let mut extra = Vec::new();
            for _ in 0..r.next(3) {
                if let Some(e) = consistent_edge(&mut r, &t, 1) {
                    extra.push(e);
                }
            }
            assert!(
                cdcl_search(&mut c, &sets, None, &extra).is_some(),
                "over-pruned a feasible-by-construction system"
            );
        }
    }

    #[test]
    fn u64_scale_weights_no_overflow() {
        let big = i128::from(u64::MAX);
        // t1 = t0 + u64::MAX, t2 = t1 + u64::MAX; consistent.
        let c = core(
            3,
            &[(0, 1, big), (1, 0, -big), (1, 2, big), (2, 1, -big)],
        );
        assert!(c.feasible());
        let w = c.witness().unwrap();
        assert_eq!(w[2] - w[0], 2 * big);
        // Now force a contradiction at that scale.
        let c = core(2, &[(1, 0, -big), (0, 1, big - 1)]);
        assert!(!c.feasible());
    }

    #[test]
    fn reviewer_dag_probe_not_false_infeasible() {
        // Audit repro: a feasible 6-var DAG whose probe tripped the old
        // relaxation-count bound (a vertex may be relaxed more than n
        // times without any cycle; only ENQUEUES are bounded by n).
        let base: Vec<Edge> = vec![
            (1, 2, 0),
            (3, 4, 0),
            (1, 5, 6),
            (3, 5, 5),
            (2, 5, 4),
            (4, 5, 3),
        ];
        let mut c = DcsCore::new(6, base);
        assert!(c.feasible());
        let extras: Vec<Edge> = vec![
            (0, 5, -1),
            (0, 5, -2),
            (0, 5, -3),
            (0, 1, -10),
            (0, 3, -10),
        ];
        assert!(c.probe(&extras), "feasible system judged infeasible");
        assert!(c.probe(&extras), "probe must stay pure");
    }

    #[test]
    fn feasible_by_construction_fuzz_never_infeasible() {
        // Systems built from a potential function are feasible by
        // construction (w >= phi[v] - phi[u]); neither solve nor probe
        // may ever call them infeasible.
        let mut state = 0xD1B54A32D192ED03u64;
        let mut rnd = move |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        for _case in 0..20_000 {
            let n = 2 + rnd(7) as usize;
            let phi: Vec<Time> = (0..n).map(|_| rnd(60) as Time - 30).collect();
            let mk = |rnd: &mut dyn FnMut(u64) -> u64, count: usize| -> Vec<Edge> {
                (0..count)
                    .filter_map(|_| {
                        let u = rnd(n as u64) as u32;
                        let v = rnd(n as u64) as u32;
                        if u == v {
                            return None;
                        }
                        let slack = rnd(6) as Time;
                        Some((u, v, phi[v as usize] - phi[u as usize] + slack))
                    })
                    .collect()
            };
            let n_base = 3 + rnd(9) as usize;
            let n_extra = 1 + rnd(4) as usize;
            let base = mk(&mut rnd, n_base);
            let extra = mk(&mut rnd, n_extra);
            let mut c = DcsCore::new(n, base.clone());
            assert!(c.feasible(), "false negative-cycle on base {base:?}");
            assert!(
                c.probe(&extra),
                "false negative-cycle on probe base={base:?} extra={extra:?}"
            );
        }
    }

    #[test]
    fn bounds_shortest_paths() {
        // t1 in [t0+2, t0+6], t2 = t1 + 1 => t2 in [t0+3, t0+7].
        let c = core(
            3,
            &[(1, 0, -2), (0, 1, 6), (1, 2, 1), (2, 1, -1)],
        );
        assert!(c.feasible());
        let (lo, hi) = c.bounds(2).unwrap();
        assert_eq!(lo, 3);
        assert_eq!(hi, Some(7));
        // Unbounded above: only a lower bound on t1.
        let c = core(2, &[(1, 0, -2)]);
        let (lo, hi) = c.bounds(1).unwrap();
        assert_eq!(lo, 2);
        assert_eq!(hi, None);
    }

    #[test]
    fn case_search_over_disjuncts() {
        // Empty base over {t0, t1}; two disjunction groups:
        //   A: t1 = t0 + 1   OR   t1 = t0 + 5
        //   B: t1 <= t0 + 2  OR   t1 >= t0 + 4
        let eq = |d: Time| vec![(0u32, 1u32, d), (1u32, 0u32, -d)];
        let sets: Vec<Vec<Vec<Edge>>> = vec![
            vec![eq(1), eq(5)],
            vec![vec![(0, 1, 2)], vec![(1, 0, -4)]],
        ];
        let mut c = DcsCore::new(2, Vec::new());
        // Both engines must agree on all three verdicts; when Sat, the
        // returned choice must actually satisfy (which assignment is
        // found first is no longer pinned: backjumping may reorder).
        for extra in [vec![], vec![(1u32, 0u32, -3i128)], vec![(0u32, 1u32, 0i128)]] {
            let mut choice = vec![0usize; 2];
            let mut acc = Vec::new();
            let brute = search_cases(&mut c, &sets, &extra, 0, &mut choice, &mut acc);
            assert!(acc.is_empty(), "search must unwind its accumulator");
            let smart = cdcl_search(&mut c, &sets, None, &extra);
            assert_eq!(brute, smart.is_some(), "engines disagree on {extra:?}");
            if let Some(ch) = smart {
                assert!(choice_satisfies(&mut c, &sets, None, &extra, &ch));
            }
        }
        // Purity: the base core is untouched by all of the above.
        assert!(c.feasible());
        assert!(c.probe(&[(0, 1, 0)]), "base alone must stay feasible");
    }

    #[test]
    fn waited_case_union_is_tighter_than_conjunction() {
        // One send with window [L, U] = [1, 1] and sd = 3, read waited
        // (min-sized). Exact rule: t_e = max(t_p, arrival) with the
        // arrival at exactly 1; with t_p = 0 the read happens at 1 and
        // ONLY at 1. The conjunctive relaxation would allow t_e in
        // [1, 4] (window + sd). Encode: t0 = root anchor (var 0 = t_p
        // = 0 via base), var 1 = read, var 2 = send at time 0.
        let base: Vec<Edge> = vec![
            // send at exactly 0 relative to root/pred (equality)
            (0, 2, 0),
            (2, 0, 0),
        ];
        let sets: Vec<Vec<Vec<Edge>>> = vec![vec![
            // immediate at t_p = t0 = 0: needs arrival <= 0, but the
            // arrival is >= send + 1 = 1: infeasible.
            vec![(1, 0, 0), (0, 1, 0), (1, 2, -1), (2, 1, 1 + 3)],
            // completed by the send's arrival: t_e in [s+1, s+1], no sd.
            vec![(1, 0, 0), (1, 2, -1), (2, 1, 1)],
        ]];
        let mut c = DcsCore::new(3, base);
        let mut choice = vec![0usize; 1];
        let mut acc = Vec::new();
        // Read pinned to 1 is feasible (the completing case): both
        // engines; the found choice must satisfy.
        let pin = |t: Time| vec![(0u32, 1u32, t), (1u32, 0u32, -t)];
        assert!(search_cases(&mut c, &sets, &pin(1), 0, &mut choice, &mut acc));
        let smart = cdcl_search(&mut c, &sets, None, &pin(1)).expect("cdcl must accept");
        assert!(choice_satisfies(&mut c, &sets, None, &pin(1), &smart));
        // Read pinned to 3 satisfies the conjunctive relaxation
        // ([1, 4]) but NO case: the eats-at-3 impostor is rejected by
        // both engines.
        assert!(!search_cases(&mut c, &sets, &pin(3), 0, &mut choice, &mut acc));
        assert!(cdcl_search(&mut c, &sets, None, &pin(3)).is_none());
        // Read pinned to 0 (before the arrival): rejected too.
        assert!(!search_cases(&mut c, &sets, &pin(0), 0, &mut choice, &mut acc));
    }
}
