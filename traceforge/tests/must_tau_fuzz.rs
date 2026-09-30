//! Must-tau fuzz (2026-09-29): receive-only timed programs from the
//! generator of tests/policy_invariance_recv.rs (1-3 receives of the
//! timed kinds, 1-3 senders, FIFO/Causal/Bag, an optional relay, a timed
//! grid; every send lossy, as in every timed configuration). Mailbox
//! programs are outside Must-tau's scope and are skipped.
//!
//! Per program: LTR and Arbitrary seeds 11, 12, 13 must count the same
//! executions and blocked endings, report the same outcome multiset, and
//! explore no graph without a timeline (timeline_impossible == 0).
//! `must_tau_fuzz_smoke` runs a small batch in the suite;
//! `must_tau_fuzz_campaign` is the long run (env TF_FUZZ_SEED,
//! TF_FUZZ_N), run with --ignored --nocapture.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread;
use traceforge::{Config, ConsType, CoverageInfo, ExecutionId, SchedulePolicy, WaitTime};

type Fingerprint = (String, BTreeSet<String>);

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
/// Programs whose LTR run exceeds this many endings are skipped (void).
const HEAVY: usize = 4_000;
const SEEDS: [u64; 3] = [11, 12, 13];

struct Run {
    execs: usize,
    block: usize,
    dead: usize,
    pruned: usize,
    capped: bool,
    outcomes: BTreeMap<Fingerprint, usize>,
}

fn run_once(
    policy: SchedulePolicy,
    seed: u64,
    cons: ConsType,
    timed: (u64, u64, u64),
    prog: Arc<dyn Fn() + Send + Sync>,
) -> Run {
    let sink: Arc<Mutex<Vec<Fingerprint>>> = Arc::new(Mutex::new(Vec::new()));
    let stats = traceforge::verify(
        Config::builder()
            .with_policy(policy)
            .with_seed(seed)
            .with_cons_type(cons)
            .with_max_iterations(MAX_ITERS)
            .with_timed(timed.0, timed.1, timed.2)
            .with_callback(Box::new(OutcomeCollector { sink: Arc::clone(&sink) }))
            .build(),
        move || prog(),
    );
    let mut outcomes: BTreeMap<Fingerprint, usize> = BTreeMap::new();
    for fp in sink.lock().unwrap().iter() {
        *outcomes.entry(fp.clone()).or_insert(0) += 1;
    }
    Run {
        execs: stats.execs,
        block: stats.block,
        dead: stats.timeline_impossible,
        pruned: stats.pruned,
        capped: (stats.execs + stats.block) as u64 >= MAX_ITERS,
        outcomes,
    }
}

enum Verdict {
    Agrees { endings: usize, pruned: usize },
    Void,
    Diverged(String),
}

fn judge(cons: ConsType, timed: (u64, u64, u64), prog: Arc<dyn Fn() + Send + Sync>) -> Verdict {
    let ltr = run_once(SchedulePolicy::LTR, 0, cons, timed, Arc::clone(&prog));
    if ltr.capped || ltr.execs + ltr.block > HEAVY {
        return Verdict::Void;
    }
    if ltr.dead > 0 {
        return Verdict::Diverged(format!("LTR explored {} graphs without a timeline", ltr.dead));
    }
    for &s in SEEDS.iter() {
        let r = run_once(SchedulePolicy::Arbitrary, s, cons, timed, Arc::clone(&prog));
        if r.capped {
            return Verdict::Void;
        }
        if r.dead > 0 {
            return Verdict::Diverged(format!("arb[{s}] explored {} graphs without a timeline", r.dead));
        }
        if (r.execs, r.block) != (ltr.execs, ltr.block) {
            return Verdict::Diverged(format!(
                "counts: LTR=({},{}) arb[{s}]=({},{})",
                ltr.execs, ltr.block, r.execs, r.block
            ));
        }
        if r.outcomes != ltr.outcomes {
            return Verdict::Diverged(format!("arb[{s}]: outcome multiset differs from LTR"));
        }
    }
    Verdict::Agrees { endings: ltr.execs + ltr.block, pruned: ltr.pruned }
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



fn campaign(base_seed: u64, n: usize) {
    println!("must-tau fuzz: base_seed={base_seed} n={n} arb_seeds={SEEDS:?}");
    let (mut divergent, mut void, mut agree, mut skipped) = (Vec::new(), 0usize, 0usize, 0usize);
    let (mut endings, mut pruned) = (0usize, 0usize);
    for i in 0..n {
        let spec = ProgramSpec::generate(SplitMix64(base_seed.wrapping_add(i as u64)).next());
        let Some(timed) = spec.timed() else {
            skipped += 1;
            continue;
        };
        if spec.vacuous() || spec.cons() == ConsType::Mailbox {
            skipped += 1;
            continue;
        }
        match judge(spec.cons(), timed, spec.program()) {
            Verdict::Agrees { endings: e, pruned: p } => {
                agree += 1;
                endings += e;
                pruned += p;
            }
            Verdict::Void => void += 1,
            Verdict::Diverged(detail) => {
                println!("[{}/{n}] DIVERGED {}\n  {detail}", i + 1, spec.print_repro());
                divergent.push(spec.print_repro());
            }
        }
    }
    println!(
        "SUMMARY: {}/{n} divergent, {agree} agree, {void} void, {skipped} skipped (untimed, vacuous or Mailbox); endings {endings}, pruned children {pruned}",
        divergent.len()
    );
    assert!(divergent.is_empty(), "Must-tau diverges on:\n{}", divergent.join("\n"));
}

#[test]
fn must_tau_fuzz_smoke() {
    campaign(20260929, 60);
}

#[test]
#[ignore = "Must-tau fuzz campaign; run with --ignored --nocapture (env TF_FUZZ_SEED, TF_FUZZ_N)"]
fn must_tau_fuzz_campaign() {
    campaign(env_u64("TF_FUZZ_SEED", 20260914), env_u64("TF_FUZZ_N", 400) as usize);
}

/// One line per (program, run) in the format of the pre-Must-tau
/// reference rows, for an offline comparison (env TF_FUZZ_SEED,
/// TF_FUZZ_N). Untimed programs are skipped here: their sends are not
/// lossy, unlike in the all-lossy reference.
#[test]
#[ignore = "rows for an offline comparison; run with --ignored --nocapture"]
fn must_tau_rows() {
    let base_seed = env_u64("TF_FUZZ_SEED", 20260914);
    let n = env_u64("TF_FUZZ_N", 400) as usize;
    for i in 0..n {
        let spec = ProgramSpec::generate(SplitMix64(base_seed.wrapping_add(i as u64)).next());
        let Some(timed) = spec.timed() else { continue };
        if spec.vacuous() || spec.cons() == ConsType::Mailbox {
            continue;
        }
        let mut runs = vec![("ltr".to_string(), SchedulePolicy::LTR, 0u64)];
        for &s in SEEDS.iter() {
            runs.push((format!("arb{s}"), SchedulePolicy::Arbitrary, s));
        }
        for (name, policy, seed) in runs {
            let r = run_once(policy, seed, spec.cons(), timed, spec.program());
            let ms: Vec<String> = r.outcomes.iter().map(|(fp, n)| format!("{n}x{}{:?}", fp.0, fp.1)).collect();
            println!(
                "ROW i={i} run={name} execs={} block={} dead={} pruned={} capped={} ms={} spec={}",
                r.execs, r.block, r.dead, r.pruned, r.capped, ms.join("|").replace(' ', ""), spec.print_repro()
            );
            if r.capped || r.execs + r.block > HEAVY {
                break;
            }
        }
    }
    println!("DONE");
}

/// One program, verbose, for graph-level comparisons: env TF_FUZZ_SEED
/// (the program is index 0 of that base), POL (ltr or arbN), V.
#[test]
#[ignore = "single-program driver; run with --ignored --nocapture"]
fn must_tau_one() {
    let spec = ProgramSpec::generate(SplitMix64(env_u64("TF_FUZZ_SEED", 20260914)).next());
    println!("SPEC {}", spec.print_repro());
    let pol = std::env::var("POL").unwrap_or_else(|_| "ltr".into());
    let (policy, seed) = if pol == "ltr" {
        (SchedulePolicy::LTR, 0)
    } else {
        (SchedulePolicy::Arbitrary, pol[3..].parse().unwrap())
    };
    let timed = spec.timed().expect("timed program");
    let prog = spec.program();
    if std::env::var_os("TF_LOG").is_some() {
        struct Printer;
        impl log::Log for Printer {
            fn enabled(&self, m: &log::Metadata) -> bool {
                m.level() <= log::Level::Info
            }
            fn log(&self, r: &log::Record) {
                println!("LOG {}", r.args());
            }
            fn flush(&self) {}
        }
        static PRINTER: Printer = Printer;
        let _ = log::set_logger(&PRINTER);
        log::set_max_level(log::LevelFilter::Info);
    }
    let st = traceforge::verify(
        Config::builder()
            .with_policy(policy)
            .with_seed(seed)
            .with_cons_type(spec.cons())
            .with_max_iterations(MAX_ITERS)
            .with_timed(timed.0, timed.1, timed.2)
            .with_verbose(env_u64("V", 2) as usize)
            .build(),
        move || prog(),
    );
    println!("STATS execs={} block={} dead={} pruned={}", st.execs, st.block, st.timeline_impossible, st.pruned);
}
