//! Policy-invariance harness for the KNOWN ISSUE documented on
//! `SchedulePolicy` (lib.rs): inbox exploration is schedule-dependent.
//! The same inbox program under LTR vs Arbitrary can explore DIFFERENT
//! sets of execution classes, and both can under-explore; the bug is
//! untimed (inherited inbox-DPOR machinery). Diagnosis, minimized
//! repros, and root-cause classification: the "Inbox schedule-policy
//! divergence" section of P1_FEEDBACK_LEASE_SWIM.md (primary cause:
//! the backward-revisit candidate pool lacks the coherence/antichain
//! filter of the forward path; contributing: the Event-order take(min)
//! canonical in inbox_reads_tiebreaker and the break-not-continue in
//! calc_revisits' inbox arm).
//!
//! Contents: a differential harness (LTR vs Arbitrary x pinned seeds,
//! compared on counts AND outcome multisets), a deterministic fuzzer
//! with self-contained repro strings, and canary pins. The #[ignore]d
//! fuzz campaign and canaries are expected RED while the bug lives:
//! turning them green un-ignored is the fix milestone's done bar.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread;
use traceforge::{Config, CoverageInfo, ExecutionId, SchedulePolicy, WaitTime};

// =====================================================================
// Differential harness
// =====================================================================

/// End condition + the set of cover!-goals of one execution. Goal SETS,
/// not counts: prefix replay after a backward revisit can re-fire a
/// cover! within one execution, so counts are replay-sensitive while
/// the set is stable. Programs under test cover! a line per inbox with
/// globally distinct payload ids, so a fingerprint identifies every
/// inbox's rf-set. Count equality alone is necessary, NOT sufficient
/// (a mirror-flip divergence preserves counts while swapping which
/// class is explored); the multiset closes most of that gap.
type Fingerprint = (String, BTreeSet<String>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunResult {
    execs: usize,
    block: usize,
    outcomes: BTreeMap<Fingerprint, usize>,
    /// Hit max_iterations: exploration truncated, comparison void.
    capped: bool,
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

const MAX_ITERS: u64 = 50_000;

/// One deterministic run. The seed is always pinned (the default
/// Config seed is random); parallel/symmetry are never enabled.
fn run_once(
    policy: SchedulePolicy,
    seed: u64,
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
) -> RunResult {
    let sink: Arc<Mutex<Vec<Fingerprint>>> = Arc::new(Mutex::new(Vec::new()));
    let mut builder = Config::builder()
        .with_policy(policy)
        .with_seed(seed)
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
    // The observer also fires for finished-but-discarded executions
    // (timeline-impossible blocked branches, Mailbox-inconsistent
    // graphs), which Stats counts as neither exec nor block, so the
    // fingerprint total may exceed the counted one; never the reverse.
    let total: usize = outcomes.values().sum();
    assert!(
        total >= stats.execs + stats.block,
        "outcome accounting drifted from Stats: {total} < {}",
        stats.execs + stats.block
    );
    RunResult {
        execs: stats.execs,
        block: stats.block,
        outcomes,
        capped: (stats.execs + stats.block) as u64 >= MAX_ITERS,
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

/// LTR vs Arbitrary over `arb_seeds`: Some(diff) on the first
/// divergence (counts, multiset, or seed-vs-seed), None if invariant.
/// Capped runs make the comparison void (never reported as divergence).
fn policy_divergence(
    arb_seeds: &[u64],
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
) -> Option<String> {
    let ltr = run_once(SchedulePolicy::LTR, 0, timed, Arc::clone(&prog));
    let arb: Vec<(u64, RunResult)> = arb_seeds
        .iter()
        .map(|&s| {
            (
                s,
                run_once(SchedulePolicy::Arbitrary, s, timed, Arc::clone(&prog)),
            )
        })
        .collect();
    if ltr.capped || arb.iter().any(|(_, r)| r.capped) {
        return None;
    }
    for (seed, r) in &arb {
        if (r.execs, r.block) != (ltr.execs, ltr.block) {
            return Some(format!(
                "counts: ltr=({},{}) arb[{seed}]=({},{})\n{}",
                ltr.execs,
                ltr.block,
                r.execs,
                r.block,
                diff_classes(&ltr, r)
            ));
        }
        if r.outcomes != ltr.outcomes {
            return Some(format!(
                "multiset: arb[{seed}] differs\n{}",
                diff_classes(&ltr, r)
            ));
        }
    }
    for w in arb.windows(2) {
        if w[0].1.outcomes != w[1].1.outcomes {
            return Some(format!(
                "arb[{}] vs arb[{}]\n{}",
                w[0].0,
                w[1].0,
                diff_classes(&w[0].1, &w[1].1)
            ));
        }
    }
    None
}

const SEEDS: [u64; 3] = [11, 12, 13];

fn assert_policy_invariant(
    timed: Option<(u64, u64, u64)>,
    prog: Arc<dyn Fn() + Send + Sync>,
) {
    if let Some(detail) = policy_divergence(&SEEDS, timed, prog) {
        panic!("policy divergence:\n{detail}");
    }
}

// =====================================================================
// Deterministic program generator (fuzzer)
// =====================================================================

const TIMED_GRID: [(u64, u64, u64); 3] = [(0, 1, 0), (0, 2, 1), (1, 3, 0)];
const SLEEPS: [u64; 4] = [0, 1, 3, 6];
const WAITS: [u64; 3] = [2, 5, 9];
const WINDOWS: [(u64, u64); 3] = [(0, 0), (0, 2), (1, 3)];

#[derive(Debug, Clone, PartialEq, Eq)]
struct SendSpec {
    sleep_ix: u8,
    tag: Option<u8>, // None | Some(0) matching | Some(1) chaff
    win_ix: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WaitSpec {
    Untimed,
    Finite(u8),
    Infinite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InboxSpec {
    min: u8,
    max: Option<u8>,
    wait: WaitSpec,
    tagged: bool, // matches tag == Some(0)
}

/// A tiny message-passing program: one collector thread running 1-2
/// inboxes sequentially, 1-3 sender threads with 1-2 sends each
/// (<= 6 sends, so every program verifies in well under a second).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgramSpec {
    timed_ix: Option<u8>,
    inboxes: Vec<InboxSpec>,
    senders: Vec<Vec<SendSpec>>,
}

/// Self-contained SplitMix64: deterministic, no dependency drift.
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
    /// Constraints (timed => min >= 1, Finite/Infinite waits; untimed
    /// => no sleeps/windows) are enforced by construction so the seed
    /// stream stays aligned.
    fn generate(seed: u64) -> ProgramSpec {
        let mut r = SplitMix64(seed);
        let timed_ix = if r.chance(2, 3) {
            Some(r.below(TIMED_GRID.len() as u64) as u8)
        } else {
            None
        };
        let timed = timed_ix.is_some();
        let inboxes = (0..1 + r.below(2) as usize)
            .map(|_| {
                let min = if timed {
                    1 + r.below(2) as u8
                } else {
                    r.below(3) as u8
                };
                let max = match r.below(3) {
                    0 => None,
                    1 => Some(min.max(1)),
                    _ => Some(min + 1),
                };
                let wait = if timed {
                    if r.chance(1, 2) {
                        WaitSpec::Finite(r.below(WAITS.len() as u64) as u8)
                    } else {
                        WaitSpec::Infinite
                    }
                } else {
                    WaitSpec::Untimed
                };
                InboxSpec {
                    min,
                    max,
                    wait,
                    tagged: r.chance(1, 2),
                }
            })
            .collect();
        let senders = (0..1 + r.below(3) as usize)
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
        ProgramSpec {
            timed_ix,
            inboxes,
            senders,
        }
    }

    /// Compact self-contained repro string, e.g.
    /// `t=N|ib=1,N:U:u|s=0.N.N;0.N.N,0.N.N`. Round-trip pinned below,
    /// so a divergence found once can never be lost again.
    fn print_repro(&self) -> String {
        let t = self.timed_ix.map_or("N".into(), |i| i.to_string());
        let ibs = self
            .inboxes
            .iter()
            .map(|ib| {
                let max = ib.max.map_or("N".into(), |m| m.to_string());
                let w = match &ib.wait {
                    WaitSpec::Untimed => "U".into(),
                    WaitSpec::Finite(i) => format!("F{i}"),
                    WaitSpec::Infinite => "I".into(),
                };
                format!("{},{}:{}:{}", ib.min, max, w, if ib.tagged { "t" } else { "u" })
            })
            .collect::<Vec<_>>()
            .join(";");
        let sends = self
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
        format!("t={t}|ib={ibs}|s={sends}")
    }

    fn parse_repro(s: &str) -> Option<ProgramSpec> {
        let mut parts = s.split('|');
        let t = parts.next()?.strip_prefix("t=")?;
        let timed_ix = if t == "N" { None } else { Some(t.parse().ok()?) };
        let inboxes = parts
            .next()?
            .strip_prefix("ib=")?
            .split(';')
            .map(|ib| {
                let (mm, rest) = ib.split_once(':')?;
                let (w, tg) = rest.split_once(':')?;
                let (min, max) = mm.split_once(',')?;
                Some(InboxSpec {
                    min: min.parse().ok()?,
                    max: if max == "N" { None } else { Some(max.parse().ok()?) },
                    wait: match w {
                        "U" => WaitSpec::Untimed,
                        "I" => WaitSpec::Infinite,
                        f => WaitSpec::Finite(f.strip_prefix('F')?.parse().ok()?),
                    },
                    tagged: tg == "t",
                })
            })
            .collect::<Option<Vec<_>>>()?;
        let senders = parts
            .next()?
            .strip_prefix("s=")?
            .split(';')
            .map(|th| {
                th.split(',')
                    .map(|sd| {
                        let mut f = sd.split('.');
                        Some(SendSpec {
                            sleep_ix: f.next()?.parse().ok()?,
                            tag: match f.next()? {
                                "N" => None,
                                t => Some(t.parse().ok()?),
                            },
                            win_ix: match f.next()? {
                                "N" => None,
                                w => Some(w.parse().ok()?),
                            },
                        })
                    })
                    .collect::<Option<Vec<_>>>()
            })
            .collect::<Option<Vec<_>>>()?;
        Some(ProgramSpec {
            timed_ix,
            inboxes,
            senders,
        })
    }

    fn timed(&self) -> Option<(u64, u64, u64)> {
        self.timed_ix.map(|i| TIMED_GRID[i as usize])
    }

    /// Every send gets a globally distinct payload id; each inbox
    /// result is cover!-ed as `ib<i>=<sorted ids>`.
    fn program(&self) -> Arc<dyn Fn() + Send + Sync> {
        let spec = self.clone();
        Arc::new(move || {
            let inboxes = spec.inboxes.clone();
            let collector = thread::spawn(move || {
                for (i, ib) in inboxes.iter().enumerate() {
                    let vals: Vec<Option<traceforge::Val>> = match (&ib.wait, &ib.tagged) {
                        (WaitSpec::Untimed, false) => traceforge::inbox_with_bounds(
                            ib.min as usize,
                            ib.max.map(|m| m as usize),
                        ),
                        (WaitSpec::Untimed, true) => traceforge::inbox_with_tag_and_bounds(
                            |_, t| t == Some(0),
                            ib.min as usize,
                            ib.max.map(|m| m as usize),
                        ),
                        (w, &tagged) => {
                            let wait = match w {
                                WaitSpec::Finite(i) => WaitTime::Finite(WAITS[*i as usize]),
                                _ => WaitTime::Infinite,
                            };
                            if tagged {
                                traceforge::inbox_with_tag_timed(
                                    |_, t| t == Some(0),
                                    ib.min as usize,
                                    ib.max.map(|m| m as usize),
                                    wait,
                                )
                            } else {
                                traceforge::inbox_timed(
                                    ib.min as usize,
                                    ib.max.map(|m| m as usize),
                                    wait,
                                )
                            }
                        }
                    };
                    let mut ids: Vec<u32> = vals
                        .iter()
                        .flatten()
                        .filter_map(|v: &traceforge::Val| {
                            v.as_any_ref().downcast_ref::<u32>().copied()
                        })
                        .collect();
                    ids.sort_unstable();
                    traceforge::cover!(format!("ib{i}={ids:?}"));
                }
            });
            let cid = collector.thread().id();
            let mut next_id: u32 = 1;
            for th in &spec.senders {
                let th = th.clone();
                let cid = cid.clone();
                let base = next_id;
                next_id += th.len() as u32;
                let timed = spec.timed_ix.is_some();
                thread::spawn(move || {
                    for (j, sd) in th.iter().enumerate() {
                        let id = base + j as u32;
                        if timed {
                            traceforge::sleep(SLEEPS[sd.sleep_ix as usize]);
                        }
                        match (sd.tag, sd.win_ix) {
                            (None, _) => traceforge::send_msg(cid.clone(), id),
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
            }
        })
    }
}

#[test]
fn repro_string_roundtrip() {
    for seed in 0..200u64 {
        let spec = ProgramSpec::generate(seed);
        let s = spec.print_repro();
        let back =
            ProgramSpec::parse_repro(&s).unwrap_or_else(|| panic!("unparseable repro {s}"));
        assert_eq!(spec, back, "roundtrip failed for {s}");
    }
}

// =====================================================================
// Fuzz campaign: the fix milestone's acceptance test.
// =====================================================================

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "policy-divergence fuzz campaign; expected RED while the inbox order-invariance bug lives. Run with --ignored --nocapture (env: TF_FUZZ_SEED, TF_FUZZ_N). Last run: 61/400 divergent."]
fn fuzz_inbox_policy_divergence() {
    let base_seed = env_u64("TF_FUZZ_SEED", 20260819);
    let n = env_u64("TF_FUZZ_N", 400) as usize;
    println!("fuzz campaign: base_seed={base_seed} n={n} arb_seeds={SEEDS:?}");
    let mut divergent = 0usize;
    for i in 0..n {
        let spec = ProgramSpec::generate(SplitMix64(base_seed.wrapping_add(i as u64)).next());
        if let Some(detail) = policy_divergence(&SEEDS, spec.timed(), spec.program()) {
            divergent += 1;
            println!("=== DIVERGENCE #{divergent} ===");
            println!("spec:  {}", spec.print_repro());
            println!("{detail}");
        }
    }
    println!("SUMMARY: {divergent}/{n} divergent");
    assert_eq!(divergent, 0, "{divergent} of {n} specs diverge between policies");
}

// =====================================================================
// Controls (must always pass) and canaries (#[ignore]d, expected RED;
// un-ignore when the order-invariance fix lands).
// =====================================================================

/// Recv-only programs are policy-invariant (the TruSt closure): if
/// this ever fails, the bug is wider than inboxes.
#[test]
fn control_recv_only_invariant() {
    assert_policy_invariant(
        None,
        Arc::new(|| {
            let c = thread::spawn(|| {
                let _: u32 = traceforge::recv_msg_block();
                let _: u32 = traceforge::recv_msg_block();
            });
            let cid = c.thread().id();
            let c1 = cid.clone();
            thread::spawn(move || traceforge::send_msg(c1, 1u32));
            thread::spawn(move || traceforge::send_msg(cid, 2u32));
        }),
    );
}

/// Minimized flagship fuzz repro t=N|ib=1,N:U:u|s=0.N.N;0.N.N,0.N.N.
/// LTR explores the antichain-correct {[1],[2],[1,2]}; Arbitrary seed
/// 13 ALSO explores the illegal [3], [1,3], [2,3], [1,2,3] (same-sender
/// pair, and reads past an unread FIFO predecessor) through the
/// unfiltered backward candidate pool: 3 vs 7 executions.
#[test]
#[ignore = "known issue: inbox schedule-policy divergence (backward pool lacks the antichain filter; fails at least on Arbitrary seed 13)"]
fn canary_backward_pool_spurious_batches() {
    assert_policy_invariant(
        None,
        Arc::new(|| {
            let c = thread::spawn(|| {
                let ids: Vec<u32> = traceforge::inbox_with_bounds(1, None)
                    .iter()
                    .flatten()
                    .filter_map(|v| v.as_any_ref().downcast_ref::<u32>().copied())
                    .collect();
                traceforge::cover!(format!("ib0={ids:?}"));
            });
            let cid = c.thread().id();
            let c1 = cid.clone();
            thread::spawn(move || traceforge::send_msg(c1, 1u32));
            thread::spawn(move || {
                traceforge::send_msg(cid.clone(), 2u32);
                traceforge::send_msg(cid, 3u32);
            });
        }),
    );
}

/// Mirror repro t=N|ib=0,N:U:u|s=0.N.N,0.N.N (min=0, ONE sender, two
/// sends): here LTR is the wrong one, exploring the illegal [2] and
/// [1,2] (4 executions) while Arbitrary seed 13 explores the
/// antichain-correct {[], [1]} (2): both directions concrete.
#[test]
#[ignore = "known issue: inbox schedule-policy divergence (mirror: LTR over-explores; fails at least on Arbitrary seed 13)"]
fn canary_min0_single_sender_mirror() {
    assert_policy_invariant(
        None,
        Arc::new(|| {
            let c = thread::spawn(|| {
                let ids: Vec<u32> = traceforge::inbox_with_bounds(0, None)
                    .iter()
                    .flatten()
                    .filter_map(|v| v.as_any_ref().downcast_ref::<u32>().copied())
                    .collect();
                traceforge::cover!(format!("ib0={ids:?}"));
            });
            let cid = c.thread().id();
            thread::spawn(move || {
                traceforge::send_msg(cid.clone(), 1u32);
                traceforge::send_msg(cid, 2u32);
            });
        }),
    );
}
