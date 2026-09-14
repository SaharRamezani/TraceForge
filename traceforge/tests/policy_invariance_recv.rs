//! Scheduler invariance for programs WITHOUT inboxes.
//!
//! The inbox DPOR assumes a left-to-right scheduler (its source paper
//! says so), so inbox programs may legitimately explore different
//! class sets under different policies. Plain receives carry no such
//! assumption: the set of explored execution classes must be the same
//! under LTR and under Arbitrary with any seed, timed or untimed, for
//! every communication model. This file fuzzes that claim.
//!
//! Programs: one collector running 1 to 3 receives in sequence
//! (non-blocking, blocking, finite-wait or infinite-wait; optionally
//! tagged), 1 to 3 sender threads with 1 to 2 sends each (optional
//! sleeps and per-send transit windows in timed mode, tags none/0/1),
//! optionally a relay edge (sender 0 releases sender 1 with a control
//! message, so a send can be causally after a receive), under Bag,
//! FIFO, Causal or Mailbox delivery. Every send carries a globally
//! distinct id and every receive covers a line naming what it read,
//! so a fingerprint identifies the reads-from of the whole execution.
//!
//! The harness mirrors tests/policy_invariance.rs: LTR versus
//! Arbitrary with three pinned seeds, compared on counts, on the
//! multiset of outcome fingerprints, and seed against seed. Capped
//! runs are skipped. Repro strings are self-contained.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread;
use traceforge::{Config, ConsType, CoverageInfo, ExecutionId, SchedulePolicy, WaitTime};

type Fingerprint = (String, BTreeSet<String>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunResult {
    execs: usize,
    block: usize,
    outcomes: BTreeMap<Fingerprint, usize>,
    capped: bool,
    /// The observer also fires for executions the checker then discards
    /// (a completed or blocked graph that admits no timeline), and it
    /// cannot tell them apart, so when the fingerprint total exceeds the
    /// counted total the multiset is not a faithful picture of the
    /// counted classes and only the counts are compared.
    discarded: bool,
}

impl RunResult {
    fn classes(&self) -> BTreeSet<&Fingerprint> {
        self.outcomes.keys().collect()
    }
}

struct OutcomeCollector {
    sink: Arc<Mutex<Vec<Fingerprint>>>,
}

impl ExecutionObserver for OutcomeCollector {
    fn after(&mut self, _eid: ExecutionId, cond: &EndCondition, c: CoverageInfo) {
        let goals: BTreeSet<String> = c.coverage.keys().cloned().collect();
        self.sink.lock().unwrap().push((format!("{cond:?}"), goals));
    }
}

const MAX_ITERS: u64 = 20_000;
/// Programs whose LTR run exceeds this many outcomes are skipped (void):
/// the three Arbitrary runs would triple the time for no extra signal.
const HEAVY: usize = 4_000;

fn run_once(
    policy: SchedulePolicy,
    seed: u64,
    cons: ConsType,
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
) -> RunResult {
    let sink: Arc<Mutex<Vec<Fingerprint>>> = Arc::new(Mutex::new(Vec::new()));
    let mut builder = Config::builder()
        .with_policy(policy)
        .with_seed(seed)
        .with_cons_type(cons)
        .with_max_iterations(MAX_ITERS)
        .with_callback(Box::new(OutcomeCollector {
            sink: Arc::clone(&sink),
        }));
    if let Some((l, u, sd)) = timed {
        builder = builder.with_timed(l, u, sd);
    }
    let stats = traceforge::verify(builder.build(), move || prog());
    let mut outcomes: BTreeMap<Fingerprint, usize> = BTreeMap::new();
    for fp in sink.lock().unwrap().iter() {
        *outcomes.entry(fp.clone()).or_insert(0) += 1;
    }
    let total: usize = outcomes.values().sum();
    RunResult {
        execs: stats.execs,
        block: stats.block,
        outcomes,
        capped: (stats.execs + stats.block) as u64 >= MAX_ITERS,
        discarded: total > stats.execs + stats.block,
    }
}

fn diff_classes(a: &RunResult, b: &RunResult) -> String {
    let (ca, cb) = (a.classes(), b.classes());
    let mut out = String::new();
    for fp in ca.difference(&cb) {
        out.push_str(&format!("  only in first:  {fp:?}\n"));
    }
    for fp in cb.difference(&ca) {
        out.push_str(&format!("  only in second: {fp:?}\n"));
    }
    if out.is_empty() {
        out.push_str("  (same classes, multiplicities differ)\n");
    }
    out
}

const SEEDS: [u64; 3] = [11, 12, 13];

enum Verdict {
    Invariant { execs: usize, block: usize },
    Void { execs: usize, block: usize },
    Diverged(String),
}

/// LTR versus Arbitrary over the pinned seeds. Void when any run hit
/// the iteration cap or the LTR run was heavier than `HEAVY`.
fn policy_divergence(
    cons: ConsType,
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
) -> Verdict {
    let ltr = run_once(SchedulePolicy::LTR, 0, cons, timed, Arc::clone(&prog));
    println!("    ltr done ({},{})", ltr.execs, ltr.block);
    if ltr.capped || ltr.execs + ltr.block > HEAVY {
        return Verdict::Void { execs: ltr.execs, block: ltr.block };
    }
    let arb: Vec<(u64, RunResult)> = SEEDS
        .iter()
        .map(|&s| {
            let r = run_once(SchedulePolicy::Arbitrary, s, cons, timed, Arc::clone(&prog));
            println!("    arb[{s}] done ({},{})", r.execs, r.block);
            (s, r)
        })
        .collect();
    if arb.iter().any(|(_, r)| r.capped) {
        return Verdict::Void { execs: ltr.execs, block: ltr.block };
    }
    if std::env::var_os("TF_FUZZ_DUMP").is_some() {
        let dump = |name: &str, r: &RunResult| {
            println!("    {name}: execs={} block={} classes={}", r.execs, r.block, r.outcomes.len());
            for (fp, n) in &r.outcomes {
                println!("      x{n} {} {:?}", fp.0, fp.1);
            }
        };
        dump("ltr", &ltr);
        for (s, r) in &arb {
            dump(&format!("arb[{s}]"), r);
        }
    }
    for (seed, r) in &arb {
        if (r.execs, r.block) != (ltr.execs, ltr.block) {
            return Verdict::Diverged(format!(
                "counts: ltr=({},{}) arb[{seed}]=({},{})\n{}",
                ltr.execs,
                ltr.block,
                r.execs,
                r.block,
                diff_classes(&ltr, r)
            ));
        }
        if !r.discarded && !ltr.discarded && r.outcomes != ltr.outcomes {
            return Verdict::Diverged(format!(
                "multiset: arb[{seed}] differs\n{}",
                diff_classes(&ltr, r)
            ));
        }
    }
    for w in arb.windows(2) {
        if !w[0].1.discarded && !w[1].1.discarded && w[0].1.outcomes != w[1].1.outcomes {
            return Verdict::Diverged(format!(
                "arb[{}] vs arb[{}]\n{}",
                w[0].0,
                w[1].0,
                diff_classes(&w[0].1, &w[1].1)
            ));
        }
    }
    Verdict::Invariant { execs: ltr.execs, block: ltr.block }
}

// =====================================================================
// Deterministic generator
// =====================================================================

const TIMED_GRID: [(u64, u64, u64); 3] = [(0, 1, 0), (0, 2, 1), (1, 3, 0)];
const SLEEPS: [u64; 4] = [0, 1, 3, 6];
const WAITS: [u64; 3] = [2, 5, 9];
const WINDOWS: [(u64, u64); 3] = [(0, 0), (0, 2), (1, 3)];
const CONS: [ConsType; 4] = [ConsType::Bag, ConsType::FIFO, ConsType::Causal, ConsType::Mailbox];
const CTRL_TAG: u32 = 9;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecvKind {
    NonBlocking,
    Blocking,
    Finite(u8),
    Infinite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecvSpec {
    kind: RecvKind,
    tagged: bool, // matches tag == Some(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SendSpec {
    sleep_ix: u8,
    tag: Option<u8>, // None | Some(0) matching | Some(1) chaff
    win_ix: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgramSpec {
    timed_ix: Option<u8>,
    cons_ix: u8,
    recvs: Vec<RecvSpec>,
    senders: Vec<Vec<SendSpec>>,
    relay: bool,
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

impl ProgramSpec {
    fn generate(seed: u64) -> ProgramSpec {
        let mut r = SplitMix64(seed);
        let timed_ix = if r.chance(2, 3) {
            Some(r.below(TIMED_GRID.len() as u64) as u8)
        } else {
            None
        };
        let timed = timed_ix.is_some();
        let cons_ix = r.below(CONS.len() as u64) as u8;
        let recvs = (0..1 + r.below(3) as usize)
            .map(|_| {
                let kind = if timed {
                    match r.below(4) {
                        0 => RecvKind::NonBlocking,
                        1 => RecvKind::Blocking,
                        2 => RecvKind::Finite(r.below(WAITS.len() as u64) as u8),
                        _ => RecvKind::Infinite,
                    }
                } else if r.chance(1, 2) {
                    RecvKind::NonBlocking
                } else {
                    RecvKind::Blocking
                };
                RecvSpec {
                    kind,
                    tagged: r.chance(1, 2),
                }
            })
            .collect();
        let senders: Vec<Vec<SendSpec>> = (0..1 + r.below(3) as usize)
            .map(|_| {
                (0..1 + r.below(2) as usize)
                    .map(|_| SendSpec {
                        sleep_ix: if timed { r.below(SLEEPS.len() as u64) as u8 } else { 0 },
                        tag: match r.below(3) {
                            0 => None,
                            1 => Some(0),
                            _ => Some(1),
                        },
                        win_ix: if timed && r.chance(1, 2) {
                            Some(r.below(WINDOWS.len() as u64) as u8)
                        } else {
                            None
                        },
                    })
                    .collect()
            })
            .collect();
        let relay = senders.len() >= 2 && r.chance(1, 2);
        ProgramSpec {
            timed_ix,
            cons_ix,
            recvs,
            senders,
            relay,
        }
    }

    /// A sender whose later send is pinned to arrive before an earlier
    /// send's minimum arrival contradicts FIFO or causal coupling: the
    /// program then has no timeline at all (zero behaviours by design),
    /// which says nothing about scheduler invariance. Such programs are
    /// skipped and counted; the checker must still terminate on them,
    /// which is pinned separately (see the hang found on 14 Sep 2026).
    fn vacuous(&self) -> bool {
        let Some((l, u, _)) = self.timed() else { return false };
        if self.cons() == ConsType::Bag { return false; }
        for th in &self.senders {
            let mut t = 0u64;
            let mut min_prev: Option<u64> = None;
            for sd in th {
                t += SLEEPS[sd.sleep_ix as usize];
                let (lo, hi) = sd.win_ix.map_or((l, u), |w| WINDOWS[w as usize]);
                let (amin, amax) = (t + lo, t + hi);
                if let Some(mp) = min_prev {
                    if amax < mp { return true; }
                }
                min_prev = Some(min_prev.map_or(amin, |mp| mp.max(amin)));
            }
        }
        false
    }

    fn timed(&self) -> Option<(u64, u64, u64)> {
        self.timed_ix.map(|i| TIMED_GRID[i as usize])
    }

    fn cons(&self) -> ConsType {
        CONS[self.cons_ix as usize]
    }

    fn print_repro(&self) -> String {
        let t = self.timed_ix.map_or("N".into(), |i| i.to_string());
        let c = ["B", "F", "C", "M"][self.cons_ix as usize];
        let rs = self
            .recvs
            .iter()
            .map(|rv| {
                let k = match &rv.kind {
                    RecvKind::NonBlocking => "n".to_string(),
                    RecvKind::Blocking => "b".to_string(),
                    RecvKind::Finite(i) => format!("f{i}"),
                    RecvKind::Infinite => "i".to_string(),
                };
                format!("{k}:{}", if rv.tagged { "t" } else { "u" })
            })
            .collect::<Vec<_>>()
            .join(";");
        let ss = self
            .senders
            .iter()
            .map(|th| {
                th.iter()
                    .map(|sd| {
                        format!(
                            "{}.{}.{}",
                            sd.sleep_ix,
                            sd.tag.map_or("N".into(), |t: u8| t.to_string()),
                            sd.win_ix.map_or("N".into(), |w: u8| w.to_string())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect::<Vec<_>>()
            .join(";");
        format!("t={t}|c={c}|r={rs}|s={ss}|relay={}", u8::from(self.relay))
    }

    fn program(&self) -> Arc<dyn Fn() + Send + Sync> {
        let spec = self.clone();
        Arc::new(move || {
            let recvs = spec.recvs.clone();
            let collector = thread::spawn(move || {
                for (i, rv) in recvs.iter().enumerate() {
                    let got: Option<u32> = match (&rv.kind, rv.tagged) {
                        (RecvKind::NonBlocking, false) => traceforge::recv_msg(),
                        (RecvKind::NonBlocking, true) => {
                            traceforge::recv_tagged_msg(|_, t| t == Some(0))
                        }
                        (RecvKind::Blocking, false) => Some(traceforge::recv_msg_block()),
                        (RecvKind::Blocking, true) => {
                            Some(traceforge::recv_tagged_msg_block(|_, t| t == Some(0)))
                        }
                        (RecvKind::Finite(w), false) => {
                            traceforge::recv_msg_timed(WaitTime::Finite(WAITS[*w as usize]))
                        }
                        (RecvKind::Finite(w), true) => traceforge::recv_tagged_msg_timed(
                            |_, t| t == Some(0),
                            WaitTime::Finite(WAITS[*w as usize]),
                        ),
                        (RecvKind::Infinite, false) => Some(traceforge::recv_msg_block_timed()),
                        (RecvKind::Infinite, true) => {
                            Some(traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(0)))
                        }
                    };
                    traceforge::cover!(format!("r{i}={got:?}"));
                }
            });
            let cid = collector.thread().id();
            let timed = spec.timed_ix.is_some();
            let n = spec.senders.len();
            // Spawn senders in reverse so sender 0 (the releaser) can
            // address sender 1 (the relayed one) by id.
            let mut relayed_id: Option<traceforge::thread::ThreadId> = None;
            let mut base_ids: Vec<u32> = Vec::with_capacity(n);
            let mut next_id: u32 = 1;
            for th in &spec.senders {
                base_ids.push(next_id);
                next_id += th.len() as u32;
            }
            for idx in (0..n).rev() {
                let th = spec.senders[idx].clone();
                let cid = cid.clone();
                let base = base_ids[idx];
                let relay = spec.relay;
                let target = relayed_id.clone();
                let handle = thread::spawn(move || {
                    if relay && idx == 1 {
                        let _go: u32 =
                            traceforge::recv_tagged_msg_block(|_, t| t == Some(CTRL_TAG));
                    }
                    if relay && idx == 0 {
                        if let Some(t) = target {
                            traceforge::send_tagged_msg(t, CTRL_TAG, 100u32);
                        }
                    }
                    for (j, sd) in th.iter().enumerate() {
                        let id = base + j as u32;
                        if timed {
                            traceforge::sleep(SLEEPS[sd.sleep_ix as usize]);
                        }
                        match (sd.tag, sd.win_ix.filter(|_| timed)) {
                            (None, None) => traceforge::send_msg(cid.clone(), id),
                            (None, Some(w)) => {
                                let (l, u) = WINDOWS[w as usize];
                                traceforge::send_msg_timed(cid.clone(), id, l, u)
                            }
                            (Some(t), None) => {
                                traceforge::send_tagged_msg(cid.clone(), u32::from(t), id)
                            }
                            (Some(t), Some(w)) => {
                                let (l, u) = WINDOWS[w as usize];
                                traceforge::send_tagged_msg_timed(
                                    cid.clone(),
                                    u32::from(t),
                                    id,
                                    l,
                                    u,
                                )
                            }
                        }
                    }
                });
                if idx == 1 {
                    relayed_id = Some(handle.thread().id());
                }
            }
        })
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Receive-only programs must explore the same classes under every
/// scheduler. Any divergence is printed with its repro string.
///
/// Status 2026-09-14: 400 programs, one divergence, the duplication
/// pinned by `canary_program_108_duplicates` below; the two runs that
/// once never terminated (programs 96 and 106, a graph with no timeline
/// and an untimed blocking receive) are fixed and pinned in
/// tests/timed_recv_gaps.rs. Un-ignore both tests when the duplication
/// is fixed.
#[test]
#[ignore = "receive-only scheduler-invariance campaign; RED while program 108 duplicates two classes under Arbitrary seeds 11 and 13. Run with --ignored --nocapture (env: TF_FUZZ_SEED, TF_FUZZ_N, TF_FUZZ_DUMP, TF_FUZZ_NO_SKIP). Last run: 1/400 divergent."]
fn fuzz_recv_policy_invariance() {
    let base_seed = env_u64("TF_FUZZ_SEED", 20260914);
    let n = env_u64("TF_FUZZ_N", 400) as usize;
    println!("recv fuzz: base_seed={base_seed} n={n} arb_seeds={SEEDS:?}");
    let mut divergent: Vec<String> = Vec::new();
    let mut void: Vec<String> = Vec::new();
    let mut invariant = 0usize;
    let mut skipped = 0usize;
    let mut timed_n = 0usize;
    let mut per_cons = [0usize; 4];
    let t0 = std::time::Instant::now();
    for i in 0..n {
        let spec = ProgramSpec::generate(SplitMix64(base_seed.wrapping_add(i as u64)).next());
        if spec.timed_ix.is_some() {
            timed_n += 1;
        }
        per_cons[spec.cons_ix as usize] += 1;
        if spec.vacuous() && std::env::var_os("TF_FUZZ_NO_SKIP").is_none() {
            println!("[{}/{n}] skipped  (contradictory windows, no timeline) {}", i + 1, spec.print_repro());
            skipped += 1;
            continue;
        }
        let t1 = std::time::Instant::now();
        println!("[{}/{n}] start {}", i + 1, spec.print_repro());
        let v = policy_divergence(spec.cons(), spec.timed(), spec.program());
        let (tag, ex, bl) = match &v {
            Verdict::Invariant { execs, block } => ("ok", *execs, *block),
            Verdict::Void { execs, block } => ("void", *execs, *block),
            Verdict::Diverged(_) => ("DIVERGED", 0, 0),
        };
        println!(
            "[{}/{n}] {tag:8} ltr=({ex},{bl}) {:.1}s {}",
            i + 1,
            t1.elapsed().as_secs_f64(),
            spec.print_repro()
        );
        match v {
            Verdict::Diverged(detail) => {
                println!("=== DIVERGENCE #{} ===", divergent.len() + 1);
                println!("spec:  {}", spec.print_repro());
                println!("{detail}");
                divergent.push(spec.print_repro());
            }
            Verdict::Void { .. } => void.push(spec.print_repro()),
            Verdict::Invariant { .. } => invariant += 1,
        }
    }
    println!(
        "SUMMARY: {}/{n} divergent, {invariant} invariant, {} void, {skipped} skipped as vacuous; timed={timed_n} untimed={}; per model B/F/C/M={:?}; {:.0}s",
        divergent.len(),
        void.len(),
        n - timed_n,
        per_cons,
        t0.elapsed().as_secs_f64()
    );
    assert!(
        divergent.is_empty(),
        "receive-only exploration depends on the scheduler for {} of {n} programs:\n{}",
        divergent.len(),
        divergent.join("\n")
    );
}

/// Program 108 of seed 20260914: under Arbitrary seeds 11 and 13 the
/// classes {r0=None, r1=Some(2), r2=Some(3)} and {r0=None, r1=Some(2),
/// r2=Some(4)} are each explored twice (15 executions for 13 classes);
/// LTR and seed 12 explore each once. A receive-only program, so this
/// is a violation of the no-duplication theorem for the timed receive
/// fragment, not an inbox artefact. Expected RED until fixed.
#[test]
#[ignore = "duplicate execution classes under Arbitrary seeds 11 and 13; expected RED until the duplication is fixed"]
fn canary_program_108_duplicates() {
    let spec = ProgramSpec::generate(SplitMix64(20260914u64.wrapping_add(107)).next());
    assert_eq!(
        spec.print_repro(),
        "t=0|c=F|r=f0:u;n:t;i:t|s=3.1.1,3.0.2;2.0.0;1.0.N,3.N.0|relay=1"
    );
    match policy_divergence(spec.cons(), spec.timed(), spec.program()) {
        Verdict::Diverged(detail) => panic!("program 108 still diverges:\n{detail}"),
        Verdict::Void { .. } => panic!("program 108 hit the cap"),
        Verdict::Invariant { execs, block } => assert_eq!((execs, block), (13, 0)),
    }
}
