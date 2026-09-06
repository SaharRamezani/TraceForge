//! Independent probe: does SchedulePolicy change what TraceForge explores
//! for INBOX programs, on the current tree?
//!
//! This file is a *measurement harness*, not an assertion suite: it runs a
//! small matrix of inbox programs under `SchedulePolicy::LTR` and under
//! `SchedulePolicy::Arbitrary` (several pinned seeds, since Arbitrary is
//! seed-driven) and prints (execs, blocked) side by side. Nothing here
//! modifies existing tests or sources.
//!
//! Run with:
//!   CARGO_INCREMENTAL=0 cargo test -p traceforge --release \
//!     --test inbox_policy_probe -- --nocapture

use std::sync::Arc;

use traceforge::{thread, Config, SchedulePolicy, WaitTime};

// ---------------------------------------------------------------------
// Program shapes
// ---------------------------------------------------------------------

/// How the collector's single inbox is configured.
#[derive(Clone, Copy, Debug)]
enum Ib {
    /// untimed `inbox_with_bounds(min, max)`
    Untimed(usize, Option<usize>),
    /// timed `inbox_timed(k, wait)` (exactly `k`, or the empty batch)
    Timed(usize, WaitTime),
}

/// A whole program: `sends[i]` is how many messages sender-thread `i`
/// sends to the collector; `ib` is the collector's inbox.
#[derive(Clone, Debug)]
struct Prog {
    name: &'static str,
    sends: Vec<usize>,
    ib: Ib,
    /// `Some((l, u, sd))` sets `.with_timed(l, u, sd)`.
    timed_cfg: Option<(u64, u64, u64)>,
}

impl Prog {
    fn build(&self) -> Arc<dyn Fn() + Send + Sync> {
        let sends = self.sends.clone();
        let ib = self.ib;
        Arc::new(move || {
            let collector = thread::spawn(move || match ib {
                Ib::Untimed(min, max) => {
                    let _v = traceforge::inbox_with_bounds(min, max);
                }
                Ib::Timed(k, w) => {
                    let _v = traceforge::inbox_timed(k, w);
                }
            });
            let cid = collector.thread().id();
            let mut handles = Vec::new();
            // Globally distinct payloads so every message is
            // distinguishable in a graph.
            let mut next: u32 = 1;
            for k in sends.iter() {
                let tid = cid.clone();
                let base = next;
                next += *k as u32;
                let k = *k;
                handles.push(thread::spawn(move || {
                    for j in 0..k {
                        traceforge::send_msg(tid.clone(), base + j as u32);
                    }
                }));
            }
            for h in handles {
                let _ = h.join();
            }
            let _ = collector.join();
        })
    }
}

const MAX_ITERS: u64 = 200_000;

fn run(p: &Prog, policy: SchedulePolicy, seed: u64) -> (usize, usize, bool) {
    let mut b = Config::builder()
        .with_policy(policy)
        .with_seed(seed)
        .with_max_iterations(MAX_ITERS);
    if let Some((l, u, sd)) = p.timed_cfg {
        b = b.with_timed(l, u, sd);
    }
    let prog = p.build();
    let stats = traceforge::verify(b.build(), move || prog());
    let capped = (stats.execs + stats.block) as u64 >= MAX_ITERS;
    (stats.execs, stats.block, capped)
}

/// Arbitrary is seed-driven, so a single seed proves nothing; sweep.
const SEEDS: [u64; 8] = [1, 7, 11, 12, 13, 42, 99, 20260829];

fn matrix() -> Vec<Prog> {
    let mut v = Vec::new();

    // ---- untimed, 2 senders x 1 send, min/max variants -------------
    for (name, min, max) in [
        ("U 2x1 ib(1,1)", 1usize, Some(1usize)),
        ("U 2x1 ib(1,2)", 1, Some(2)),
        ("U 2x1 ib(2,2)", 2, Some(2)),
        ("U 2x1 ib(1,N)", 1, None),
        ("U 2x1 ib(0,1)", 0, Some(1)),
        ("U 2x1 ib(0,N)", 0, None),
    ] {
        v.push(Prog {
            name,
            sends: vec![1, 1],
            ib: Ib::Untimed(min, max),
            timed_cfg: None,
        });
    }

    // ---- untimed, 3 senders x 1 send -------------------------------
    for (name, min, max) in [
        ("U 3x1 ib(1,1)", 1usize, Some(1usize)),
        ("U 3x1 ib(1,2)", 1, Some(2)),
        ("U 3x1 ib(2,2)", 2, Some(2)),
        ("U 3x1 ib(2,3)", 2, Some(3)),
        ("U 3x1 ib(1,N)", 1, None),
        ("U 3x1 ib(0,1)", 0, Some(1)),
    ] {
        v.push(Prog {
            name,
            sends: vec![1, 1, 1],
            ib: Ib::Untimed(min, max),
            timed_cfg: None,
        });
    }

    // ---- untimed, senders with 2 sends (FIFO predecessor shapes) ---
    for (name, sends) in [
        ("U 1x2 ib(1,1)", vec![2usize]),
        ("U 2sends+1 ib(1,1)", vec![2, 1]),
        ("U 1+2sends ib(1,1)", vec![1, 2]),
    ] {
        v.push(Prog {
            name,
            sends,
            ib: Ib::Untimed(1, Some(1)),
            timed_cfg: None,
        });
    }
    for (name, sends, min, max) in [
        ("U 1x2 ib(0,N)", vec![2usize], 0usize, None),
        ("U 1x2 ib(1,2)", vec![2], 1, Some(2)),
        ("U 2sends+1 ib(1,2)", vec![2, 1], 1, Some(2)),
        ("U 2sends+1 ib(2,2)", vec![2, 1], 2, Some(2)),
        ("U 2sends+1 ib(1,N)", vec![2, 1], 1, None),
    ] {
        v.push(Prog {
            name,
            sends,
            ib: Ib::Untimed(min, max),
            timed_cfg: None,
        });
    }

    // ---- timed variants (k >= 1 required) --------------------------
    // A timed inbox now collects EXACTLY `k`, so there is no max column;
    // rows that differed only in `max` have collapsed into one.
    for (name, sends, k, w) in [
        ("T 2x1 ib(1) Inf", vec![1usize, 1usize], 1usize, WaitTime::Infinite),
        ("T 2x1 ib(2) Inf", vec![1, 1], 2, WaitTime::Infinite),
        ("T 2x1 ib(1) F2", vec![1, 1], 1, WaitTime::Finite(2)),
        ("T 2x1 ib(2) F2", vec![1, 1], 2, WaitTime::Finite(2)),
        ("T 3x1 ib(1) Inf", vec![1, 1, 1], 1, WaitTime::Infinite),
        ("T 3x1 ib(2) F5", vec![1, 1, 1], 2, WaitTime::Finite(5)),
        ("T 2sends+1 ib(1) Inf", vec![2, 1], 1, WaitTime::Infinite),
        ("T 2sends+1 ib(1) F5", vec![2, 1], 1, WaitTime::Finite(5)),
        ("T 1x2 ib(1) Inf", vec![2], 1, WaitTime::Infinite),
        ("T 1x2 ib(1) F5", vec![2], 1, WaitTime::Finite(5)),
    ] {
        v.push(Prog {
            name,
            sends,
            ib: Ib::Timed(k, w),
            timed_cfg: Some((0, 2, 1)),
        });
    }

    v
}

#[test]
fn inbox_policy_probe() {
    let progs = matrix();
    let mut diverged: Vec<String> = Vec::new();

    println!();
    println!(
        "{:<24} {:>12} | {}",
        "program",
        "LTR(e,b)",
        "Arbitrary (execs,block) per seed"
    );
    println!("{}", "-".repeat(110));

    for p in &progs {
        let (le, lb, lcap) = run(p, SchedulePolicy::LTR, 0);
        let mut cells = Vec::new();
        let mut any_diff = false;
        for &s in SEEDS.iter() {
            let (ae, ab, acap) = run(p, SchedulePolicy::Arbitrary, s);
            let mark = if (ae, ab) != (le, lb) {
                any_diff = true;
                "*"
            } else {
                ""
            };
            cells.push(format!("{s}:({ae},{ab}){mark}"));
            if acap {
                cells.push("CAPPED".into());
            }
        }
        println!(
            "{:<24} {:>12} | {}",
            p.name,
            format!("({le},{lb}){}", if lcap { "CAP" } else { "" }),
            cells.join(" ")
        );
        if any_diff {
            diverged.push(format!(
                "{}: LTR=({le},{lb})  arb={}",
                p.name,
                cells.join(" ")
            ));
        }
    }

    println!();
    println!("=== SUMMARY: {} / {} programs diverge ===", diverged.len(), progs.len());
    for d in &diverged {
        println!("  {d}");
    }
    println!();
}

/// Dump the rf-sets an (min, max) untimed inbox explores for a
/// one-sender-two-sends program, under a given policy/seed.
fn dump_1x2(min: usize, max: Option<usize>, policy: SchedulePolicy, seed: u64) -> (usize, usize, Vec<Vec<u32>>) {
    let seen: Arc<std::sync::Mutex<Vec<Vec<u32>>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let stats = traceforge::verify(
        Config::builder()
            .with_policy(policy)
            .with_seed(seed)
            .with_max_iterations(MAX_ITERS)
            .build(),
        move || {
            let sink = Arc::clone(&sink);
            let collector = thread::spawn(move || {
                let v = traceforge::inbox_with_bounds(min, max);
                let got: Vec<u32> = v
                    .iter()
                    .filter_map(|o| o.as_ref())
                    .map(|val| *val.as_any_ref().downcast_ref::<u32>().unwrap())
                    .collect();
                sink.lock().unwrap().push(got);
            });
            let cid = collector.thread().id();
            let h = thread::spawn(move || {
                traceforge::send_msg(cid.clone(), 1u32);
                traceforge::send_msg(cid.clone(), 2u32);
            });
            let _ = h.join();
            let _ = collector.join();
        },
    );
    let mut sets = seen.lock().unwrap().clone();
    sets.sort();
    sets.dedup();
    (stats.execs, stats.block, sets)
}

/// The other direction: for `min = 0, max = None` on the same
/// one-sender-two-sends program, LTR is the one exploring MORE.
#[test]
fn mirror_one_sender_two_sends_inbox_0_none() {
    println!();
    println!("MIRROR  1 sender x 2 sends, untimed inbox_with_bounds(0, None)");
    for (label, policy, seed) in [
        ("LTR seed 0", SchedulePolicy::LTR, 0u64),
        ("Arb seed 0", SchedulePolicy::Arbitrary, 0),
        ("Arb seed 13", SchedulePolicy::Arbitrary, 13),
        ("Arb seed 20260829", SchedulePolicy::Arbitrary, 20260829),
    ] {
        let (e, b, sets) = dump_1x2(0, None, policy, seed);
        println!("  {label}: execs={e} block={b} rf-sets = {sets:?}");
    }
    println!();
}

/// The smallest divergent shape found by `inbox_policy_probe`:
/// ONE sender thread sending two messages to a collector whose single
/// untimed inbox is `min = max = 1`. Three events total.
///
/// Prints LTR and Arbitrary over a wide seed sweep so the seed-stability
/// of each policy is visible.
#[test]
fn minimal_repro_one_sender_two_sends_inbox_1_1() {
    let p = Prog {
        name: "minimal",
        sends: vec![2],
        ib: Ib::Untimed(1, Some(1)),
        timed_cfg: None,
    };
    println!();
    let mut ltr = Vec::new();
    let mut arb = Vec::new();
    for s in 0..24u64 {
        let (e, b, _) = run(&p, SchedulePolicy::LTR, s);
        ltr.push(format!("{s}:({e},{b})"));
        let (e, b, _) = run(&p, SchedulePolicy::Arbitrary, s);
        arb.push(format!("{s}:({e},{b})"));
    }
    println!("MINIMAL REPRO  1 sender x 2 sends, untimed inbox_with_bounds(1, Some(1))");
    println!("  LTR       {}", ltr.join(" "));
    println!("  Arbitrary {}", arb.join(" "));

    // Which rf-sets does each policy actually explore?
    for (label, policy, seed) in [
        ("LTR    seed 0", SchedulePolicy::LTR, 0u64),
        ("Arb    seed 0", SchedulePolicy::Arbitrary, 0),
        ("Arb    seed 4", SchedulePolicy::Arbitrary, 4),
        ("Arb    seed 5", SchedulePolicy::Arbitrary, 5),
        ("Arb    seed 42", SchedulePolicy::Arbitrary, 42),
    ] {
        let seen: Arc<std::sync::Mutex<Vec<Vec<u32>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let stats = traceforge::verify(
            Config::builder()
                .with_policy(policy)
                .with_seed(seed)
                .with_max_iterations(MAX_ITERS)
                .build(),
            move || {
                let sink = Arc::clone(&sink);
                let collector = thread::spawn(move || {
                    let v = traceforge::inbox_with_bounds(1, Some(1));
                    let got: Vec<u32> = v
                        .iter()
                        .filter_map(|o| o.as_ref())
                        .map(|val| *val.as_any_ref().downcast_ref::<u32>().unwrap())
                        .collect();
                    sink.lock().unwrap().push(got);
                });
                let cid = collector.thread().id();
                let h = thread::spawn(move || {
                    traceforge::send_msg(cid.clone(), 1u32);
                    traceforge::send_msg(cid.clone(), 2u32);
                });
                let _ = h.join();
                let _ = collector.join();
            },
        );
        let mut sets: Vec<Vec<u32>> = seen.lock().unwrap().clone();
        sets.sort();
        sets.dedup();
        println!(
            "  {label}: execs={} block={} rf-sets explored = {:?}",
            stats.execs, stats.block, sets
        );
    }
    println!();
}

/// Isolation check: a fresh process running ONLY Arbitrary/seed 4 on the
/// minimal program, to rule out cross-`verify` state contamination as the
/// cause of the divergence. Run alone with
///   --test inbox_policy_probe -- --exact isolated_arb_seed4 --nocapture
#[test]
fn isolated_arb_seed4() {
    let (e, b, sets) = dump_1x2(1, Some(1), SchedulePolicy::Arbitrary, 4);
    println!("\nISOLATED Arb seed 4, ib(1,1), 1x2: execs={e} block={b} rf-sets={sets:?}\n");
}

/// Isolation check for the LTR side of the same program.
#[test]
fn isolated_ltr_seed0() {
    let (e, b, sets) = dump_1x2(1, Some(1), SchedulePolicy::LTR, 0);
    println!("\nISOLATED LTR seed 0, ib(1,1), 1x2: execs={e} block={b} rf-sets={sets:?}\n");
}
