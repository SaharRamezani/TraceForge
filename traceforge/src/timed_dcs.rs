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
//! * Waited-inbox reads return at `max(t0, completing arrival)`: a
//!   disjunction over which member's arrival completed the batch. Each
//!   disjunct projects onto a pure difference-constraint edge set over
//!   existing events (an arrival variable couples only to its own send
//!   and the read, so Fourier-Motzkin eliminates it), and committed
//!   reads carry one edge-set alternative per disjunct; feasibility
//!   searches over one case per read. At the probe sites the read is
//!   the last event of its thread, where the plain conjunctive window
//!   system is provably equivalent (see [`TimedDcs::probe_inbox_rfs`]),
//!   so probes stay single-shot.
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

// =====================================================================
// DcsCore: pure difference-constraint solver (graph-independent)
// =====================================================================

/// A difference-constraint system with maintained potentials.
///
/// `pi` is a feasible assignment (`pi[to] <= pi[from] + w` for every
/// edge) whenever `feasible` is true; it IS a witness timeline up to a
/// constant shift.
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
        if !self.feasible {
            return false;
        }
        // Reuse scratch buffers: probes run in tight per-candidate loops.
        self.scratch_pi.clear();
        self.scratch_pi.extend_from_slice(&self.pi);
        self.scratch_inq.fill(false);
        self.scratch_cnt.fill(0);
        let n = self.n;
        let mut inq = std::mem::take(&mut self.scratch_inq);
        let mut cnt = std::mem::take(&mut self.scratch_cnt);
        let mut queue: VecDeque<u32> = VecDeque::new();
        let mut ok = true;
        // Bucket large extra slices by `from` so the per-dequeue scan
        // below stays cheap; tiny slices keep the allocation-free scan.
        let extra_by_from: Option<HashMap<u32, Vec<(u32, Time)>>> = if extra.len() > 16 {
            let mut m: HashMap<u32, Vec<(u32, Time)>> = HashMap::new();
            for &(from, v, w) in extra {
                m.entry(from).or_default().push((v, w));
            }
            Some(m)
        } else {
            None
        };
        for &(u, v, w) in extra {
            let cand = self.pi[u as usize] + w;
            if cand < self.pi[v as usize] {
                self.pi[v as usize] = cand;
                if !inq[v as usize] {
                    cnt[v as usize] += 1;
                    inq[v as usize] = true;
                    queue.push_back(v);
                }
            }
        }
        'repair: while let Some(u) = queue.pop_front() {
            inq[u as usize] = false;
            let relax = |pi: &mut Vec<Time>,
                         cnt: &mut Vec<u32>,
                         inq: &mut Vec<bool>,
                         queue: &mut VecDeque<u32>,
                         v: u32,
                         cand: Time|
             -> bool {
                if cand < pi[v as usize] {
                    pi[v as usize] = cand;
                    // Enqueue-count bound (see solve()).
                    if !inq[v as usize] {
                        cnt[v as usize] += 1;
                        if cnt[v as usize] > n as u32 {
                            return false;
                        }
                        inq[v as usize] = true;
                        queue.push_back(v);
                    }
                }
                true
            };
            for &eid in &self.adj[u as usize] {
                let (_, v, w) = self.edges[eid as usize];
                let cand = self.pi[u as usize] + w;
                if !relax(&mut self.pi, &mut cnt, &mut inq, &mut queue, v, cand) {
                    ok = false;
                    break 'repair;
                }
            }
            // Recv/unblock extras are tiny (a linear scan beats a
            // map), but the case machinery can pass one chosen case
            // per committed waited inbox read in a single slice; the
            // bucketed index keeps the per-dequeue cost proportional
            // to the edges actually leaving `u`.
            if let Some(buckets) = &extra_by_from {
                for &(v, w) in buckets.get(&u).map(|b| b.as_slice()).unwrap_or(&[]) {
                    let cand = self.pi[u as usize] + w;
                    if !relax(&mut self.pi, &mut cnt, &mut inq, &mut queue, v, cand) {
                        ok = false;
                        break 'repair;
                    }
                }
            } else {
                for &(from, v, w) in extra {
                    if from == u {
                        let cand = self.pi[u as usize] + w;
                        if !relax(&mut self.pi, &mut cnt, &mut inq, &mut queue, v, cand) {
                            ok = false;
                            break 'repair;
                        }
                    }
                }
            }
        }
        std::mem::swap(&mut self.pi, &mut self.scratch_pi); // restore saved potentials
        self.scratch_inq = inq;
        self.scratch_cnt = cnt;
        ok
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
/// exactly at one member's arrival:
///
/// ```text
/// immediate:       t_e = t_p;               forall s: t_s+L <= t_e <= t_s+U+sd
/// completed by k:  t_p <= t_e (<= t_p + W); t_k+L <= t_e <= t_k+U   (no sd!)
///                                           forall s != k: t_s+L <= t_e <= t_s+U+sd
/// ```
///
/// The system is feasible iff SOME choice of one case per such read
/// is; `build` runs that search once, and every probe quantifies over
/// the same choices (fast path: the assignment the search settled on).
pub(crate) struct TimedDcs<'g> {
    g: &'g ExecutionGraph,
    cfg: &'g TimedConfig,
    vars: HashMap<Event, u32>,
    n: usize,
    base_edges: Vec<Edge>,
    /// One group per committed waited inbox read (|subset| == min):
    /// the alternative edge sets of its disjunction.
    case_sets: Vec<Vec<Vec<Edge>>>,
    /// Flattened edges of the feasible case assignment found by
    /// `solve_cases` (probe fast path; empty when no case sets).
    chosen_edges: Vec<Edge>,
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
        let mut vars: HashMap<Event, u32> = HashMap::new();
        let mut order: Vec<Event> = Vec::new();
        for (tid, maxidx) in view.entries() {
            for j in 0..=maxidx {
                let pos = Event::new(tid, j);
                if !g.contains(pos) {
                    continue;
                }
                let id = 1 + order.len() as u32;
                vars.insert(pos, id);
                order.push(pos);
            }
        }

        let mut edges: Vec<Edge> = Vec::new();
        let mut case_sets: Vec<Vec<Vec<Edge>>> = Vec::new();
        let mut infeasible_label = false;
        for &pos in &order {
            if Some(pos) == floating {
                continue;
            }
            let e = vars[&pos];
            if pos.index == 0 {
                // Begin: every thread's clock starts at absolute 0
                // (walker parity: index 0 has window [0, 0]).
                edges.push((ROOT, e, 0));
                edges.push((e, ROOT, 0));
                continue;
            }
            let pred = Event::new(pos.thread, pos.index - 1);
            let p = match vars.get(&pred) {
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
                                            for &(sv, l, u) in &windows {
                                                c.push((e, sv, -l));
                                                c.push((sv, e, u + sd));
                                            }
                                            case_sets.push(fold_exclusions(
                                                vec![c],
                                                &excl,
                                                |_, x| {
                                                    exclusion_options(
                                                        None, &windows, false, e, x,
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
                                        case_sets.push(fold_exclusions(
                                            cases,
                                            &excl,
                                            |i, x| {
                                                exclusion_options(
                                                    i.checked_sub(1),
                                                    &windows,
                                                    at_capacity,
                                                    e,
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

        let n = 1 + order.len();
        let core = DcsCore::new(n, edges.clone());
        let mut dcs = Self {
            g,
            cfg,
            vars,
            n,
            base_edges: edges,
            case_sets,
            chosen_edges: Vec::new(),
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

    /// Search one feasible case assignment for the committed inbox
    /// disjunctions (the system is feasible iff one exists).
    fn solve_cases(&mut self) -> bool {
        if !self.core.feasible() {
            return false;
        }
        if self.case_sets.is_empty() {
            return true;
        }
        let mut choice = vec![0usize; self.case_sets.len()];
        let mut acc: Vec<Edge> = Vec::new();
        if search_cases(&mut self.core, &self.case_sets, &[], 0, &mut choice, &mut acc) {
            self.chosen_edges = choice
                .iter()
                .zip(&self.case_sets)
                .flat_map(|(&c, set)| set[c].iter().copied())
                .collect();
            true
        } else {
            false
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
        let mut acc = std::mem::take(&mut self.scratch);
        acc.clear();
        acc.extend_from_slice(&self.chosen_edges);
        acc.extend_from_slice(extra);
        let fast = self.core.probe(&acc);
        if fast {
            self.scratch = acc;
            return true;
        }
        acc.clear();
        let mut choice = vec![0usize; self.case_sets.len()];
        let ok = search_cases(&mut self.core, &self.case_sets, extra, 0, &mut choice, &mut acc);
        self.scratch = acc;
        if ok {
            // Adopt the found assignment as the new fast-path hint (an
            // assignment feasible WITH the extras is feasible without
            // them, so it is as certified as the build-time one), else
            // a batch of candidates favoring a different assignment
            // would pay the full re-search for every acceptance. The
            // lazily built leaf must follow the new assignment.
            self.chosen_edges = choice
                .iter()
                .zip(&self.case_sets)
                .flat_map(|(&c, set)| set[c].iter().copied())
                .collect();
            self.leaf = None;
        }
        ok
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
        // Fast path: the committed assignment plus each alternative.
        let mut acc = std::mem::take(&mut self.scratch);
        for alt in extra_set {
            acc.clear();
            acc.extend_from_slice(&self.chosen_edges);
            acc.extend_from_slice(alt);
            acc.extend_from_slice(extra);
            if self.core.probe(&acc) {
                self.scratch = acc;
                return true;
            }
        }
        acc.clear();
        self.scratch = acc;
        if self.case_sets.is_empty() {
            return false; // fast path was exhaustive (no committed cases)
        }
        let mut sets: Vec<Vec<Vec<Edge>>> = Vec::with_capacity(self.case_sets.len() + 1);
        sets.extend(self.case_sets.iter().cloned());
        sets.push(extra_set.to_vec());
        let mut choice = vec![0usize; sets.len()];
        let mut acc = std::mem::take(&mut self.scratch);
        acc.clear();
        let ok = search_cases(&mut self.core, &sets, extra, 0, &mut choice, &mut acc);
        self.scratch = acc;
        ok
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
        let Some(&e) = self.vars.get(&recv) else {
            debug_assert!(false, "probe target {recv} not in scope");
            return true;
        };
        let pred = Event::new(recv.thread, recv.index - 1);
        let Some(&p) = self.vars.get(&pred) else {
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
                let Some(&s) = self.vars.get(&cand) else {
                    // Invariant breach tripwire (candidates come from
                    // the same graph the oracle was built on): keep the
                    // candidate, never tighter; certification gates any
                    // report.
                    debug_assert!(false, "candidate send {cand} not in scope");
                    return true;
                };
                let (l, u) = self
                    .g
                    .send_label(cand)
                    .and_then(|slab| slab.transit())
                    .unwrap_or((self.cfg.l, self.cfg.u));
                let sd = self.cfg.sd_for(recv.thread);
                extra.push((e, s, -i128::from(l))); // t_send <= t_recv - L
                extra.push((s, e, i128::from(u) + i128::from(sd))); // t_recv <= t_send + U + sd
            }
        }
        self.probe_exact(&extra)
    }

    /// Exact feasibility of waking the blocked receive at `block_pos`
    /// by reading `send`: same window as a blocking read (no wait cap).
    pub(crate) fn probe_block_unblock(&mut self, block_pos: Event, send: Event) -> bool {
        self.probe_unblock_subset(block_pos, &[send])
    }

    /// Joint wake-up probe for a blocked value read: can all of
    /// `sends` be read together as ONE batch by the blocked thread?
    /// The block is the last event of its thread, so the conjunctive
    /// window system is exact even for an inbox-shaped block
    /// (terminal-read equivalence, see [`Self::probe_inbox_rfs`]); no
    /// wait cap applies (only infinite-wait reads block).
    pub(crate) fn probe_unblock_subset(&mut self, block_pos: Event, sends: &[Event]) -> bool {
        let Some(&e) = self.vars.get(&block_pos) else {
            debug_assert!(false, "block {block_pos} not in scope");
            return true;
        };
        if block_pos.index == 0 {
            return true; // defensive, mirrors is_waiting_on_written
        }
        let pred = Event::new(block_pos.thread, block_pos.index - 1);
        let Some(&p) = self.vars.get(&pred) else {
            return self.feasible;
        };
        let sd = self.cfg.sd_for(block_pos.thread);
        let mut extra: Vec<Edge> = Vec::with_capacity(1 + 2 * sends.len());
        extra.push((e, p, 0));
        for &send in sends {
            let Some(&s) = self.vars.get(&send) else {
                // Invariant breach tripwire: keep (never tighter).
                debug_assert!(false, "send {send} not in scope");
                return true;
            };
            let (l, u) = self
                .g
                .send_label(send)
                .and_then(|slab| slab.transit())
                .unwrap_or((self.cfg.l, self.cfg.u));
            extra.push((e, s, -i128::from(l)));
            extra.push((s, e, i128::from(u) + i128::from(sd)));
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
        let Some(&e) = self.vars.get(&block_pos) else {
            debug_assert!(false, "block {block_pos} not in scope");
            return true;
        };
        if block_pos.index == 0 {
            return true; // defensive, mirrors is_waiting_on_written
        }
        let pred = Event::new(block_pos.thread, block_pos.index - 1);
        let Some(&p) = self.vars.get(&pred) else {
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
        let folded = fold_exclusions(base, &excl, |i, x| {
            // Blocks have no capacity bound in scope; the immediate
            // case still constrains (a t0 read returns all stored).
            exclusion_options(i.checked_sub(1), &windows, false, e, x)
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
        let Some(&e) = self.vars.get(&pos) else {
            return false;
        };
        if pos.index == 0 {
            return false;
        }
        let Some(&p) = self.vars.get(&Event::new(pos.thread, pos.index - 1)) else {
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
        let Some(&e) = self.vars.get(&inbox) else {
            return true;
        };
        let pred = Event::new(inbox.thread, inbox.index - 1);
        let Some(&p) = self.vars.get(&pred) else {
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
            for &(sv, l, u) in &windows {
                c.push((e, sv, -l));
                c.push((sv, e, u + sd));
            }
            fold_exclusions(vec![c], &excl, |_, x| {
                exclusion_options(None, &windows, at_capacity, e, x)
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
                exclusion_options(i.checked_sub(1), &windows, at_capacity, e, x)
            })
        };
        self.probe_exact_with_set(&[], &cases)
    }

    /// Exact feasibility of the floating finite-wait inbox at `inbox`
    /// timing out (rf = None): an exact clock advance by W, mirroring
    /// the committed encoding. Untimed inboxes are transparent;
    /// infinite waits cannot time out.
    pub(crate) fn probe_inbox_timeout(&mut self, inbox: Event) -> bool {
        let Some(&e) = self.vars.get(&inbox) else {
            return true;
        };
        let pred = Event::new(inbox.thread, inbox.index - 1);
        let Some(&p) = self.vars.get(&pred) else {
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
        let &v = self.vars.get(&e)?;
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
            .iter()
            .map(|(&ev, &v)| (ev, u64::try_from(w[v as usize].max(0)).unwrap_or(u64::MAX)))
            .collect();
        out.sort();
        Some(out)
    }
}

/// Edges of a committed timed read (shared by the base derivation).
#[allow(clippy::too_many_arguments)]
fn self_read_edges(
    edges: &mut Vec<Edge>,
    vars: &HashMap<Event, u32>,
    g: &ExecutionGraph,
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
    let Some(&sv) = vars.get(&s) else {
        // rf source outside the view: the read cannot be scheduled in
        // this cut; mark infeasible. Reachable benignly on tiebreak
        // views that still contain a previously revisited receive
        // whose stamp-later source was cut (the revisit is rejected by
        // other maximality checks either way), so no assert here.
        *infeasible = true;
        return;
    };
    let (l, u) = g
        .send_label(s)
        .and_then(|slab| slab.transit())
        .unwrap_or((cfg.l, cfg.u));
    let sd = cfg.sd_for(pos.thread);
    edges.push((e, sv, -i128::from(l)));
    edges.push((sv, e, i128::from(u) + i128::from(sd)));
}

/// Per-member `(var, L, U)` transit windows of an inbox subset, or
/// `None` when a member send is outside the view (a committed read
/// whose source is outside the view cannot be scheduled in this cut;
/// never silently under-constrain).
fn subset_windows(
    vars: &HashMap<Event, u32>,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    _pos: Event,
    subset: &[Event],
) -> Option<Vec<(u32, Time, Time)>> {
    subset
        .iter()
        .map(|&s| {
            // Outside the view: same benign-on-tiebreak-views contract
            // as self_read_edges; the caller marks infeasible.
            let &sv = vars.get(&s)?;
            let (l, u) = g
                .send_label(s)
                .and_then(|slab| slab.transit())
                .unwrap_or((cfg.l, cfg.u));
            Some((sv, i128::from(l), i128::from(u)))
        })
        .collect()
}

/// Edges of a committed IMMEDIATE inbox read of more than `min`
/// members: the read happened at t0 with every member already stored
/// and still alive. Exact: the arrival variables project onto plain
/// windows (each couples only to its own send and the read).
#[allow(clippy::too_many_arguments)]
fn immediate_inbox_edges(
    edges: &mut Vec<Edge>,
    vars: &HashMap<Event, u32>,
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
    for &(sv, l, u) in &windows {
        edges.push((e, sv, -l));
        edges.push((sv, e, u + sd));
    }
}

/// Alternative edge sets of a committed WAITED min-sized inbox read
/// (`t_e = max(t_p, completing arrival)`): index 0 is the immediate
/// read at t0; index 1+k is "member k's arrival completed the batch"
/// (the read happens AT that arrival, so k gets no storage slack,
/// while every other member need only be stored and alive). Returns
/// `None` when a member send is outside the view.
#[allow(clippy::too_many_arguments)]
fn waited_inbox_cases(
    vars: &HashMap<Event, u32>,
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
    for &(sv, l, u) in &windows {
        immediate.push((e, sv, -l));
        immediate.push((sv, e, u + sd));
    }
    cases.push(immediate);
    for (k, &(svk, lk, uk)) in windows.iter().enumerate() {
        let mut case: Vec<Edge> = Vec::with_capacity(2 + 2 * windows.len());
        case.push((e, p, 0)); // t_e >= t_p
        if let WaitTime::Finite(w) = wait {
            case.push((p, e, i128::from(w))); // t_e <= t_p + W
        }
        // The completing member: the read is AT its arrival.
        case.push((e, svk, -lk)); // t_e >= t_k + L
        case.push((svk, e, uk)); // t_e <= t_k + U (no sd!)
        for (j, &(sv, l, u)) in windows.iter().enumerate() {
            if j != k {
                case.push((e, sv, -l));
                case.push((sv, e, u + sd));
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
#[allow(clippy::too_many_arguments)]
fn push_refusal_edges(
    edges: &mut Vec<Edge>,
    vars: &HashMap<Event, u32>,
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
        let Some(&bv) = vars.get(&bpos) else {
            continue;
        };
        if b.reader().is_some_and(|r| r != pos && vars.contains_key(&r)) {
            continue;
        }
        if b.monitor_readers()
            .iter()
            .any(|&mr| mr.thread == pos.thread && vars.contains_key(&mr))
        {
            continue;
        }
        if b.is_cancelled_wrt(anchor) {
            continue;
        }
        let (l, _u) = b.transit().unwrap_or((cfg.l, cfg.u));
        edges.push((e, bv, -(i128::from(l) + sd + 1)));
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
    /// The excluded send's variable.
    xv: u32,
    /// Its upper transit bound U_x (per-send override honored).
    u: Time,
    /// -(L_x + sd + 1): weight of the fully-dead-before edge.
    dead_w: Time,
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
    vars: &HashMap<Event, u32>,
    g: &ExecutionGraph,
    cfg: &TimedConfig,
    pos: Event,
    loc: &crate::loc::RecvLoc,
    comm: crate::loc::CommunicationModel,
    subset: &[Event],
) -> Vec<InboxExclusion> {
    let sd = i128::from(cfg.sd_for(pos.thread));
    let gc = comm != crate::loc::CommunicationModel::NoOrder
        && comm != crate::loc::CommunicationModel::TotalOrder;
    let mut out = Vec::new();
    for b in g.matching_stores(loc) {
        let bpos = b.pos();
        if subset.contains(&bpos) {
            continue;
        }
        let Some(&xv) = vars.get(&bpos) else {
            continue; // outside the view: cannot be stored in this cut
        };
        if b.reader().is_some_and(|r| vars.contains_key(&r)) {
            continue; // consumed in this view: no longer stored
        }
        if b.monitor_readers()
            .iter()
            .any(|&mr| mr.thread == pos.thread && vars.contains_key(&mr))
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
                        && s2
                            .reader()
                            .is_some_and(|r2| r2 != pos && vars.contains_key(&r2))
                });
            if evicted {
                continue;
            }
        }
        let (l, u) = b.transit().unwrap_or((cfg.l, cfg.u));
        out.push(InboxExclusion {
            xv,
            u: i128::from(u),
            dead_w: -(i128::from(l) + sd + 1),
        });
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
fn exclusion_options(
    comp: Option<usize>,
    windows: &[(u32, Time, Time)],
    at_capacity: bool,
    e: u32,
    x: &InboxExclusion,
) -> Option<Vec<Edge>> {
    match comp {
        // Immediate t0 read: at capacity the read may leave stored
        // messages behind, so x is unconstrained.
        None if at_capacity => None,
        None => Some(vec![(x.xv, e, x.u - 1), (e, x.xv, x.dead_w)]),
        Some(k) => {
            let mut v = vec![(x.xv, e, x.u), (e, x.xv, x.dead_w)];
            for (m, &(sv, _l, u)) in windows.iter().enumerate() {
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
/// b stayed unread). The skip is legal only in timelines where b was not
/// readable at the read: some arrival choice of b dodges the read time t,
/// which holds iff t lies outside b's forced-readable region
/// [t_b + U, t_b + L + sd] (nonempty only when U <= L + sd). Encoded as a
/// two-case disjunction per skipped message: t <= t_b + U - 1  OR
/// t >= t_b + L + sd + 1. In the full graph these are implied by the
/// offer-time eligibility probe; they matter for porf-prefix
/// certification, where dropping events can widen t's range. Ordering
/// uses the send's sb (the same order retain_sb_minimals consults for
/// plain reads); omitting a monitor-order skip only loosens, never
/// tightens.
#[allow(clippy::too_many_arguments)]
fn push_recv_skip_cases(
    case_sets: &mut Vec<Vec<Vec<Edge>>>,
    vars: &HashMap<Event, u32>,
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
        // skippers also suppress (parity with send_overtaken_in_view;
        // audit 2026-08-09).
        let earlier_skipper = g.get_thr(&bpos.thread).labels[(bpos.index as usize + 1)..]
            .iter()
            .any(|lab2| {
                let LabelEnum::SendMsg(s2) = lab2 else {
                    return false;
                };
                s2.sb().contains(bpos)
                    && s2.reader().is_some_and(|r2| {
                        r2 != pos && view.contains(r2) && g.in_porf(r2, pos)
                    })
            });
        if earlier_skipper {
            continue;
        }
        let (l, u) = b.transit().unwrap_or((cfg.l, cfg.u));
        let (l, u) = (i128::from(l), i128::from(u));
        if u > l + sd {
            continue; // forced-readable region empty: always dodgeable
        }
        let Some(&bv) = vars.get(&bpos) else {
            continue;
        };
        case_sets.push(vec![
            vec![(bv, e, u - 1)],          // read before b could be forced-stored
            vec![(e, bv, -(l + sd + 1))],  // read after b is certainly dead
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
    vars: &HashMap<Event, u32>,
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
        // skippers also suppress (parity with send_overtaken_in_view;
        // audit 2026-08-09).
        let earlier_skipper = g.get_thr(&bpos.thread).labels[(bpos.index as usize + 1)..]
            .iter()
            .any(|lab2| {
                let LabelEnum::SendMsg(s2) = lab2 else {
                    return false;
                };
                s2.sb().contains(bpos)
                    && s2.reader().is_some_and(|r2| {
                        r2 != pos && view.contains(r2) && g.in_porf(r2, pos)
                    })
            });
        if earlier_skipper {
            continue;
        }
        let (l, u) = b.transit().unwrap_or((cfg.l, cfg.u));
        let (l, u) = (i128::from(l), i128::from(u));
        if u > l + sd {
            continue;
        }
        let Some(&bv) = vars.get(&bpos) else {
            continue;
        };
        case_sets.push(vec![
            vec![(bv, e, u - 1)],
            vec![(e, bv, -(l + sd + 1))],
        ]);
    }
}

/// DFS over one case per group, probing `core` (the base-only system)
/// with the accumulated case edges plus `extra` at each leaf; interior
/// nodes prune early (constraints only grow down a path, so a partial
/// assignment that is already infeasible cannot become feasible
/// below). Writes the found assignment into `choice`.
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
        let mut choice = vec![0usize; 2];
        let mut acc = Vec::new();
        // No extra: (1, <=2) works.
        assert!(search_cases(&mut c, &sets, &[], 0, &mut choice, &mut acc));
        assert!(acc.is_empty(), "search must unwind its accumulator");
        // Extra t1 >= t0 + 3: only (5, >=4) works; the search must
        // backtrack past the first group's first case.
        assert!(search_cases(&mut c, &sets, &[(1, 0, -3)], 0, &mut choice, &mut acc));
        assert_eq!(choice, vec![1, 1]);
        // Extra t1 <= t0: no assignment works (t1 is 1 or 5).
        assert!(!search_cases(&mut c, &sets, &[(0, 1, 0)], 0, &mut choice, &mut acc));
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
        // Read pinned to 1 is feasible (the completing case).
        let pin = |t: Time| vec![(0u32, 1u32, t), (1u32, 0u32, -t)];
        assert!(search_cases(&mut c, &sets, &pin(1), 0, &mut choice, &mut acc));
        assert_eq!(choice, vec![1]);
        // Read pinned to 3 satisfies the conjunctive relaxation
        // ([1, 4]) but NO case: the eats-at-3 impostor is rejected.
        assert!(!search_cases(&mut c, &sets, &pin(3), 0, &mut choice, &mut acc));
        // Read pinned to 0 (before the arrival): rejected too.
        assert!(!search_cases(&mut c, &sets, &pin(0), 0, &mut choice, &mut acc));
    }
}
