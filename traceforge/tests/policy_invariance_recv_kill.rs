//! Timeout kill (`with_kill_dead_timeouts`) against the reference, on the
//! receive-only programs of tests/policy_invariance_recv.rs (same
//! generator, same fingerprints; this file only changes the judge).
//!
//! Per program: one reference run (LTR, kill off) and four kill runs
//! (LTR plus Arbitrary seeds 11, 12, 13). Every kill run must count the
//! same executions and blocked endings as the reference, must explore
//! no class twice, must explore only classes the reference explored,
//! and the kill runs must agree with each other. The observer does not
//! see executions stopped at a kill, so a kill run's multiset is exact
//! whenever it discards nothing else; the reference also reports the
//! endings it discards, so it is compared by inclusion.
//! Run with --ignored --nocapture (env: TF_FUZZ_SEED, TF_FUZZ_N).
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
    dead: usize,
    killed: usize,
    outcomes: BTreeMap<Fingerprint, usize>,
    capped: bool,
    /// Fingerprints outnumber counted endings: the observer also saw
    /// endings the checker discarded, so the multiset is not exact.
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

struct Printer;
impl log::Log for Printer {
    fn enabled(&self, m: &log::Metadata) -> bool {
        m.level() <= log::Level::Info
    }
    fn log(&self, r: &log::Record) {
        let s = format!("{}", r.args());
        if s.contains("[kill]") || s.contains("[dead]") || s.contains("begin backward_revisit") || s.contains("[revisit/forward] start") || s.contains("enqueue") {
            println!("LOG {s}");
        }
    }
    fn flush(&self) {}
}
static PRINTER: Printer = Printer;

fn run_once(
    policy: SchedulePolicy,
    seed: u64,
    cons: ConsType,
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
    kill: bool,
) -> RunResult {
    let traced = kill
        && match std::env::var("TF_KILL_TRACE_SEED") {
            Ok(v) => policy == SchedulePolicy::Arbitrary && v.parse::<u64>().ok() == Some(seed),
            Err(_) => policy == SchedulePolicy::LTR,
        };
    if std::env::var_os("TF_KILL_LOG").is_some() && traced {
        let _ = log::set_logger(&PRINTER);
        log::set_max_level(log::LevelFilter::Info);
    } else {
        log::set_max_level(log::LevelFilter::Off);
    }
    let sink: Arc<Mutex<Vec<Fingerprint>>> = Arc::new(Mutex::new(Vec::new()));
    let mut builder = Config::builder()
        .with_policy(policy)
        .with_seed(seed)
        .with_cons_type(cons)
        .with_max_iterations(MAX_ITERS)
        .with_kill_dead_timeouts(kill)
        .with_verbose(if traced {
            std::env::var("TF_KILL_VERBOSE").ok().and_then(|v| v.parse().ok()).unwrap_or(0)
        } else {
            0
        })
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
        dead: stats.timeline_impossible,
        killed: stats.killed,
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
    Agrees { execs: usize, block: usize, dead_ref: usize, dead_kill: usize, killed: usize },
    Void,
    Diverged(String),
}

fn kill_divergence(
    cons: ConsType,
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
) -> Verdict {
    let reference = run_once(SchedulePolicy::LTR, 0, cons, timed, Arc::clone(&prog), false);
    if reference.capped || reference.execs + reference.block > HEAVY {
        return Verdict::Void;
    }
    let mut runs: Vec<(String, RunResult)> =
        vec![("ltr".into(), run_once(SchedulePolicy::LTR, 0, cons, timed, Arc::clone(&prog), true))];
    for &s in SEEDS.iter() {
        runs.push((
            format!("arb[{s}]"),
            run_once(SchedulePolicy::Arbitrary, s, cons, timed, Arc::clone(&prog), true),
        ));
    }
    if runs.iter().any(|(_, r)| r.capped) {
        return Verdict::Void;
    }
    for (name, r) in &runs {
        if (r.execs, r.block) != (reference.execs, reference.block) {
            let dump = |x: &RunResult| {
                x.outcomes.iter().map(|(fp, n)| format!("      x{n} {} {:?}\n", fp.0, fp.1)).collect::<String>()
            };
            return Verdict::Diverged(format!(
                "counts: reference=({},{}) kill {name}=({},{})\n{}    reference (discarded={}):\n{}    kill {name} (discarded={}):\n{}",
                reference.execs, reference.block, r.execs, r.block,
                diff_classes(&reference, r), reference.discarded, dump(&reference), r.discarded, dump(r)
            ));
        }
        if !r.discarded {
            if let Some((fp, n)) = r.outcomes.iter().find(|(_, &n)| n > 1) {
                return Verdict::Diverged(format!("kill {name} explores a class {n} times: {fp:?}"));
            }
        }
        if r.discarded {
            continue; // not an exact picture of the counted classes
        }
        if let Some(fp) = r.outcomes.keys().find(|fp| !reference.outcomes.contains_key(*fp)) {
            return Verdict::Diverged(format!("kill {name} explores a class the reference never saw: {fp:?}"));
        }
        if !reference.discarded && !r.discarded && r.outcomes != reference.outcomes {
            return Verdict::Diverged(format!("multiset: kill {name} vs reference\n{}", diff_classes(&reference, r)));
        }
    }
    for w in runs.windows(2) {
        if !w[0].1.discarded && !w[1].1.discarded && w[0].1.outcomes != w[1].1.outcomes {
            return Verdict::Diverged(format!(
                "kill {} vs kill {}\n{}", w[0].0, w[1].0, diff_classes(&w[0].1, &w[1].1)
            ));
        }
    }
    Verdict::Agrees {
        execs: reference.execs,
        block: reference.block,
        dead_ref: reference.dead,
        dead_kill: runs[0].1.dead,
        killed: runs[0].1.killed,
    }
}


const TIMED_GRID: [(u64, u64, u64); 3] = [(0, 1, 0), (0, 2, 1), (1, 3, 0)];
const SLEEPS: [u64; 4] = [0, 1, 3, 6];
const WAITS: [u64; 3] = [2, 5, 9];
const WINDOWS: [(u64, u64); 3] = [(0, 0), (0, 2), (1, 3)];
const CONS: [ConsType; 4] = [ConsType::Bag, ConsType::FIFO, ConsType::Causal, ConsType::Mailbox];
const CTRL_TAG: u32 = 9;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecvKind {
    /// `recv_msg_timed(Finite(0))`: the non-blocking receive of a timed program.
    Zero,
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
                // A program is either timed or untimed (the checker rejects
                // an untimed receive under a timed configuration), so timed
                // programs draw from the timed kinds only; the zero wait is
                // the timed non-blocking form.
                let kind = if timed {
                    match r.below(4) {
                        0 => RecvKind::Zero,
                        1 => RecvKind::Infinite,
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
                    RecvKind::Zero => "z".to_string(),
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
                        (RecvKind::Zero, false) => traceforge::recv_msg_timed(WaitTime::Finite(0)),
                        (RecvKind::Zero, true) => {
                            traceforge::recv_tagged_msg_timed(|_, t| t == Some(0), WaitTime::Finite(0))
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
                            traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(CTRL_TAG));
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


#[test]
#[ignore = "timeout-kill campaign against the reference; run with --ignored --nocapture"]
fn fuzz_timeout_kill_vs_reference() {
    let base_seed = env_u64("TF_FUZZ_SEED", 20260914);
    let n = env_u64("TF_FUZZ_N", 400) as usize;
    println!("kill fuzz: base_seed={base_seed} n={n} arb_seeds={SEEDS:?}");
    let (mut divergent, mut void, mut agree, mut skipped) = (Vec::new(), 0usize, 0usize, 0usize);
    let (mut dead_ref, mut dead_kill, mut killed, mut execs_total) = (0usize, 0usize, 0usize, 0usize);
    let t0 = std::time::Instant::now();
    for i in 0..n {
        let spec = ProgramSpec::generate(SplitMix64(base_seed.wrapping_add(i as u64)).next());
        if spec.vacuous() {
            skipped += 1;
            continue;
        }
        let t1 = std::time::Instant::now();
        match kill_divergence(spec.cons(), spec.timed(), spec.program()) {
            Verdict::Agrees { execs, block, dead_ref: dr, dead_kill: dk, killed: k } => {
                agree += 1;
                dead_ref += dr;
                dead_kill += dk;
                killed += k;
                execs_total += execs + block;
                println!("[{}/{n}] ok   ({execs},{block}) dead ref/kill={dr}/{dk} killed={k} {:.1}s {}",
                    i + 1, t1.elapsed().as_secs_f64(), spec.print_repro());
            }
            Verdict::Void => {
                void += 1;
                println!("[{}/{n}] void {}", i + 1, spec.print_repro());
            }
            Verdict::Diverged(detail) => {
                println!("[{}/{n}] DIVERGED {}\n{detail}", i + 1, spec.print_repro());
                divergent.push(spec.print_repro());
            }
        }
    }
    println!(
        "SUMMARY: {}/{n} divergent, {agree} agree, {void} void, {skipped} vacuous; counted endings {execs_total}; dead endings explored: reference {dead_ref}, kill {dead_kill}; executions stopped at a kill {killed}; {:.0}s",
        divergent.len(), t0.elapsed().as_secs_f64()
    );
    assert!(divergent.is_empty(), "kill diverges from the reference on:\n{}", divergent.join("\n"));
}
