//! Basic Three-Phase Commit (3PC) with long-lived rounds.
//!
//! Skeen Figure 7 (central-site 3PC), without the Section 5 termination
//! protocol. One fixed coordinator and N fixed participants. If the
//! coordinator crashes mid-execution, the round dies; no backup takes
//! over.
//!
//! ## Normal-case protocol
//!
//!   Phase 1: coord ── Prepare ──▶ every participant
//!            participant ── Yes / No ──▶ coord
//!            coord: all Yes  →  advance to Phase 2
//!                   any No   →  broadcast Abort
//!
//!   Phase 2: coord ── PreCommit ──▶ every participant
//!            participant ── Ack ──▶ coord
//!            coord: all Ack ⇒ advance to Phase 3
//!
//!   Phase 3: coord ── Commit ──▶ every participant
//!
//! ## Rounds and cross-round arrivals
//!
//! Threads are long-lived: each of the N participants and the coordinator
//! is spawned once and loops over R rounds internally. With long-lived
//! threads the model checker's revisit mechanism *can* pair a `send` from
//! round R with a `recv` from round R'. That's fine. We make the protocol
//! correct under such cross-round arrivals by:
//!
//!   1. Embedding `round: u32` in every protocol message.
//!   2. Round-filtering every recv: a message whose round does not match
//!      the receiver's `current_round` is silently dropped and the recv
//!      is reissued.
//!
//! ## Failure model
//!
//! `maybe_crash` returns `true` for fail-stop crashes. The coordinator
//! and every participant may crash at every send/recv boundary when
//! `--crashes` is on; a crash is permanent (the thread `return`s, ending
//! all subsequent rounds).
//!
//! ## Mode::Timed
//!
//! An orthogonal layer. `with_timed(0, U, 0)` registers per-send
//! transit bounds; `timed_consistent` (in must.rs) then prunes rfs
//! that violate the bounds. Its job is to catch *timing violations*; it
//! is not what makes the round design semantically correct.

use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};
use std::sync::atomic::{AtomicUsize, Ordering};

// Commit-outcome counters (analog of the leader-election count for commit protocols):
// incremented once per coordinator decision; reset before each verify, read after.
static COMMITS: AtomicUsize = AtomicUsize::new(0);
static ABORTS: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_PARTICIPANTS: u32 = 3;
const DEFAULT_U: u64 = 1;
const DEFAULT_W_RATIO: u64 = 2;
const DEFAULT_ROUNDS: u32 = 1;
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];
const SWEEP_PARTICIPANTS: &[u32] = &[3, 4, 5];

#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    Prepare   { round: u32, coord: ThreadId },
    PreCommit { round: u32 },
    Commit    { round: u32 },
    Abort     { round: u32 },
}

#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    Init { peers: Vec<ThreadId> },
    Yes  { round: u32 },
    No   { round: u32 },
    Ack  { round: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    w: u64,
    l: u64,
    sd: u64,
}

impl Bounds {
    fn from_ratio(u: u64, w_ratio: u64, l: u64, sd: u64) -> Self {
        assert!(u >= 1);
        assert!(w_ratio >= 2);
        assert!(l <= u, "transit lower bound L must be <= U");
        let w = u * w_ratio;
        Self { u, w, l, sd }
    }
}

fn maybe_crash(crashes: bool) -> bool {
    crashes && traceforge::nondet()
}

// =====================================================================
// Coordinator
// =====================================================================

fn coordinator(b: Bounds, crashes: bool, rounds: u32) {
    let ps: Vec<ThreadId> = loop {
        match traceforge::recv_msg_block::<CMsg>() {
            CMsg::Init { peers } => break peers,
            _ => {}
        }
    };
    let me = thread::current().id();

    for round in 0..rounds {
        if maybe_crash(crashes) { return; }

        for id in &ps {
            traceforge::send_msg(*id, PMsg::Prepare { round, coord: me });
            if maybe_crash(crashes) { return; }
        }

        let mut yes = 0usize;
        let mut received = 0usize;
        for _ in 0..ps.len() {
            let vote = loop {
                match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
                    Some(CMsg::Yes { round: r }) if r == round => break Some(true),
                    Some(CMsg::No  { round: r }) if r == round => break Some(false),
                    Some(_) => {}
                    None => break None,
                }
            };
            match vote {
                Some(true)  => { received += 1; yes += 1; }
                Some(false) => { received += 1; }
                None => {}
            }
            if maybe_crash(crashes) { return; }
        }

        if received != ps.len() || yes != ps.len() {
            ABORTS.fetch_add(1, Ordering::Relaxed);
            for id in &ps {
                if maybe_crash(crashes) { return; }
                traceforge::send_msg(*id, PMsg::Abort { round });
            }
            continue;
        }

        if maybe_crash(crashes) { return; }

        for id in &ps {
            traceforge::send_msg(*id, PMsg::PreCommit { round });
            if maybe_crash(crashes) { return; }
        }

        let mut acks = 0usize;
        for _ in 0..ps.len() {
            let ack = loop {
                match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
                    Some(CMsg::Ack { round: r }) if r == round => break Some(()),
                    Some(_) => {}
                    None => break None,
                }
            };
            if ack.is_some() { acks += 1; }
            if maybe_crash(crashes) { return; }
        }

        if acks != ps.len() {
            continue;
        }

        if maybe_crash(crashes) { return; }

        COMMITS.fetch_add(1, Ordering::Relaxed);
        for id in &ps {
            traceforge::send_msg(*id, PMsg::Commit { round });
            if maybe_crash(crashes) { return; }
        }
    }
}

// =====================================================================
// Participant
// =====================================================================

fn participant(b: Bounds, crashes: bool, rounds: u32) {
    if maybe_crash(crashes) { return; }

    let mut dead = false;
    for round in 0..rounds {
        if dead { continue; }

        // Wait for this round's Prepare.
        let coord_id = loop {
            match traceforge::recv_msg_timed::<PMsg>(WaitTime::Finite(b.w)) {
                Some(PMsg::Prepare { round: r, coord }) if r == round => break Some(coord),
                Some(_) => {}
                None => break None,
            }
        };
        let coord_id = match coord_id {
            Some(c) => c,
            None => { dead = true; continue; }
        };

        if maybe_crash(crashes) { return; }

        let voted_yes: bool = traceforge::nondet();
        let vote = if voted_yes {
            CMsg::Yes { round }
        } else {
            CMsg::No { round }
        };
        if maybe_crash(crashes) { return; }
        traceforge::send_msg(coord_id, vote);
        if !voted_yes { continue; }

        if maybe_crash(crashes) { return; }

        // Phase 2: wait for PreCommit or Abort.
        let phase2 = loop {
            match traceforge::recv_msg_timed::<PMsg>(WaitTime::Finite(b.w)) {
                Some(PMsg::PreCommit { round: r }) if r == round => break Some(true),
                Some(PMsg::Abort     { round: r }) if r == round => break Some(false),
                Some(_) => {}
                None => break None,
            }
        };
        match phase2 {
            Some(true) => {}
            Some(false) => continue,
            None => { dead = true; continue; }
        }

        if maybe_crash(crashes) { return; }
        traceforge::send_msg(coord_id, CMsg::Ack { round });
        if maybe_crash(crashes) { return; }

        // Phase 3: wait for Commit or Abort.
        let phase3 = loop {
            match traceforge::recv_msg_timed::<PMsg>(WaitTime::Finite(b.w)) {
                Some(PMsg::Commit { round: r }) if r == round => break Some(true),
                Some(PMsg::Abort  { round: r }) if r == round => break Some(false),
                Some(_) => {}
                None => break None,
            }
        };
        match phase3 {
            Some(true) => { traceforge::assert(voted_yes); }
            Some(false) => {}
            None => { dead = true; continue; }
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, b: Bounds) -> Config {
    let builder = Config::builder().with_progress_report(usize::MAX);
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
    }
}

fn run(mode: Mode, num_ps: u32, b: Bounds, crashes: bool, rounds: u32) -> (Stats, Duration) {
    let cfg = build_config(mode, b);
    COMMITS.store(0, Ordering::Relaxed);
    ABORTS.store(0, Ordering::Relaxed);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let mut handles = Vec::new();
        for _ in 0..num_ps {
            handles.push(thread::spawn(move || participant(b, crashes, rounds)));
        }
        let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        let c = thread::spawn(move || coordinator(b, crashes, rounds));
        traceforge::send_msg(c.thread().id(), CMsg::Init { peers: peer_ids });
        for h in handles { let _ = h.join(); }
        let _ = c.join();
    });
    (stats, start.elapsed())
}

// =====================================================================
// Reporting
// =====================================================================

fn print_one(label: &str, num_ps: u32, b: Bounds, rounds: u32, stats: &Stats, dur: Duration, crashes: bool,
             commits: usize, aborts: usize) {
    let crash_tag = if crashes { " (crashes)" } else { "" };
    println!(
        "{label:<10}{crash_tag} N={num_ps} R={rounds}  L={l} U={u} W={w} (W/U={r}) sd={sd}  execs={execs:<6} \
         blocked={block:<6} commit={commits:<6} abort={aborts:<6} time={dur:?}",
        l = b.l, u = b.u, w = b.w, r = b.w / b.u, sd = b.sd, execs = stats.execs, block = stats.block, dur = dur,
    );
}

fn print_compare(num_ps: u32, b: Bounds, rounds: u32, crashes: bool, baseline: (Stats, Duration), timed: (Stats, Duration),
                 b_commits: usize, b_aborts: usize, t_commits: usize, t_aborts: usize) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!(
        "Three-Phase Commit (basic{}): MUST vs MUST-τ",
        if crashes { " + crashes" } else { "" }
    );
    println!("======================================================");
    println!("N = {num_ps}    R = {rounds}    L = {}    U = {}    W = {} (= {}·U)    sd = {}", b.l, b.u, b.w, b.w / b.u, b.sd);
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
    println!();
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("execs reduction: {exec_ratio:.2}x");
    println!("time  speedup  : {time_ratio:.2}x");
    println!("commit/abort (complete execs): baseline {b_commits}/{b_aborts}   timed {t_commits}/{t_aborts}");
}

fn print_sweep(num_ps: u32, u: u64, rounds: u32, crashes: bool, rows: &[(u64, Bounds, Stats, Duration, Stats, Duration)]) {
    println!();
    println!("Three-Phase Commit (basic): W/U sweep{}", if crashes { " + crashes" } else { "" });
    println!("====================================================================");
    let (hl, hsd) = rows.first().map(|r| (r.1.l, r.1.sd)).unwrap_or((0, 0));
    println!("N = {num_ps}    R = {rounds}    L = {hl}    U = {u}    sd = {hsd}");
    println!();
    println!("{:<6} {:<6} {:>10} {:>10} {:>10} {:>10} {:>8}", "ratio", "W", "base.exec", "temp.exec", "base.blk", "temp.blk", "× exec");
    for (ratio, b, b_stats, _, t_stats, _) in rows {
        let r = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
        println!("{:<6} {:<6} {:>10} {:>10} {:>10} {:>10} {:>7.2}x",
            ratio, b.w, b_stats.execs, t_stats.execs, b_stats.block, t_stats.block, r);
    }
}

fn print_n_sweep(u: u64, w_ratio: u64, rounds: u32, crashes: bool, rows: &[(u32, Bounds, Stats, Duration, Stats, Duration)]) {
    println!();
    println!("Three-Phase Commit (basic): N sweep{}", if crashes { " + crashes" } else { "" });
    println!("====================================================================");
    let (hl, hsd) = rows.first().map(|r| (r.1.l, r.1.sd)).unwrap_or((0, 0));
    println!("R = {rounds}    L = {hl}    U = {u}    W = {} (= {w_ratio}·U)    sd = {hsd}", u * w_ratio);
    println!();
    println!("{:<3} {:<6} {:>10} {:>10} {:>10} {:>10} {:>8}", "N", "W", "base.exec", "temp.exec", "base.blk", "temp.blk", "× exec");
    for (n, b, b_stats, _, t_stats, _) in rows {
        let r = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
        println!("{:<3} {:<6} {:>10} {:>10} {:>10} {:>10} {:>7.2}x",
            n, b.w, b_stats.execs, t_stats.execs, b_stats.block, t_stats.block, r);
    }
}

// =====================================================================
// CLI
// =====================================================================

fn parse_args() -> (String, u32, u64, u64, u32, bool, f64, f64) {
    let mut mode = String::from("compare");
    let mut num_ps = DEFAULT_PARTICIPANTS;
    let mut u = DEFAULT_U;
    let mut w_ratio = DEFAULT_W_RATIO;
    let mut rounds = DEFAULT_ROUNDS;
    let mut crashes = false;
    let mut l_ratio: f64 = 0.0;
    let mut sd_ratio: f64 = 0.0;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => mode = args.next().expect("--mode value"),
            "--participants" => num_ps = args.next().expect("--participants value").parse().expect("u32"),
            "--u" => u = args.next().expect("--u value").parse().expect("u64"),
            "--w-ratio" => w_ratio = args.next().expect("--w-ratio value").parse().expect("u64"),
            "--rounds" => rounds = args.next().expect("--rounds value").parse().expect("u32"),
            "--l-ratio" => l_ratio = args.next().expect("--l-ratio value").parse().expect("f64"),
            "--sd-ratio" => sd_ratio = args.next().expect("--sd-ratio value").parse().expect("f64"),
            "--crashes" => crashes = true,
            "--help" | "-h" => {
                eprintln!("Usage: three_pc_timed [--mode MODE] [--participants N] [--u U] [--w-ratio R] [--rounds R] [--l-ratio LR] [--sd-ratio SR] [--crashes]\n\
                          Modes: baseline | timed | compare | sweep | n-sweep\n\
                          Defaults: U=1, W/U=2 (Skeen), N=3, R=1, L/U=0, sd/U=0.");
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, num_ps, u, w_ratio, rounds, crashes, l_ratio, sd_ratio)
}

fn main() {
    let (mode_str, num_ps, u, w_ratio, rounds, crashes, l_ratio, sd_ratio) = parse_args();
    assert!(num_ps >= 1);
    assert!(rounds >= 1);
    // L and sd are derived from dimensionless ratios over U (network-parameters §2).
    let l = (l_ratio * u as f64).round() as u64;
    let sd = (sd_ratio * u as f64).round() as u64;
    match mode_str.as_str() {
        "baseline" => {
            let b = Bounds::from_ratio(u, w_ratio, l, sd);
            let (s, d) = run(Mode::Baseline, num_ps, b, crashes, rounds);
            print_one("baseline", num_ps, b, rounds, &s, d, crashes,
                      COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
        }
        "timed" => {
            let b = Bounds::from_ratio(u, w_ratio, l, sd);
            let (s, d) = run(Mode::Timed, num_ps, b, crashes, rounds);
            print_one("timed", num_ps, b, rounds, &s, d, crashes,
                      COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
        }
        "compare" => {
            let b = Bounds::from_ratio(u, w_ratio, l, sd);
            let baseline = run(Mode::Baseline, num_ps, b, crashes, rounds);
            let (bc, ba) = (COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
            let timed = run(Mode::Timed, num_ps, b, crashes, rounds);
            let (tc, ta) = (COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
            print_compare(num_ps, b, rounds, crashes, baseline, timed, bc, ba, tc, ta);
        }
        "sweep" => {
            let mut rows = Vec::new();
            for &r in SWEEP_RATIOS {
                let b = Bounds::from_ratio(u, r, l, sd);
                let baseline = run(Mode::Baseline, num_ps, b, crashes, rounds);
                let timed = run(Mode::Timed, num_ps, b, crashes, rounds);
                rows.push((r, b, baseline.0, baseline.1, timed.0, timed.1));
            }
            print_sweep(num_ps, u, rounds, crashes, &rows);
        }
        "n-sweep" => {
            let mut rows = Vec::new();
            for &n in SWEEP_PARTICIPANTS {
                let b = Bounds::from_ratio(u, w_ratio, l, sd);
                let baseline = run(Mode::Baseline, n, b, crashes, rounds);
                let timed = run(Mode::Timed, n, b, crashes, rounds);
                rows.push((n, b, baseline.0, baseline.1, timed.0, timed.1));
            }
            print_n_sweep(u, w_ratio, rounds, crashes, &rows);
        }
        other => panic!("invalid --mode: {other}"),
    }
}
