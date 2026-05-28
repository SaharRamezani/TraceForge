//! Side-by-side comparison of MUST-τ vs. MUST on **basic 3PC** (Skeen
//! Figure 7, no termination/backup protocol) with long-lived rounds.
//!
//! One fixed coordinator, N fixed participants. If the coordinator
//! crashes mid-execution, the round dies; no backup takes over.
//!
//! ## Rounds and cross-round arrivals
//!
//! Threads are long-lived: the coordinator and each participant are
//! spawned once and loop over R rounds internally. With long-lived
//! threads the model checker's revisit mechanism *can* pair a `send`
//! from round R with a `recv` from round R'. Every message carries a
//! `round: u32`; each recv applies a round-id filter so cross-round
//! arrivals are silently dropped. That round-id filter at the user
//! level is what keeps the protocol semantically correct; `Mode::Timed`
//! is an independent layer that additionally prunes timing-infeasible
//! rfs via `timed_consistent`.
//!
//! ## Tests
//!
//! Two W/U sweeps at N=2 with crashes on, one per round count:
//!   `wu_sweep_r1` — R=1
//!   `wu_sweep_r2` — R=2
//!
//! Each prints a single table showing MUST-τ vs untimed MUST across
//! `W/U ∈ {2, 3, 5, 7}`.
//!
//! Run:
//!   cargo run --release --example three_pc_compare

use std::fmt::Write;
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ==== Constants =======================================================

const DEFAULT_PARTICIPANTS: u32 = 2;
const U: u64 = 1;
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];

// ==== Protocol types ==================================================

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

#[derive(Clone, Copy, Debug)]
struct Bounds { u: u64, w: u64, delta: u64 }

impl Bounds {
    fn with_delta(u: u64, w_ratio: u64, delta: u64) -> Self {
        Self { u, w: u * w_ratio, delta }
    }
}

fn maybe_crash(crashes: bool) -> bool {
    crashes && traceforge::nondet()
}

// ==== Coordinator =====================================================

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
        let mut got = 0usize;
        for _ in 0..ps.len() {
            traceforge::sleep(b.delta);
            let vote = loop {
                match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
                    Some(CMsg::Yes { round: r }) if r == round => break Some(true),
                    Some(CMsg::No  { round: r }) if r == round => break Some(false),
                    Some(_) => {}
                    None => break None,
                }
            };
            match vote {
                Some(true)  => { got += 1; yes += 1; }
                Some(false) => { got += 1; }
                None => {}
            }
            if maybe_crash(crashes) { return; }
        }

        if got != ps.len() || yes != ps.len() {
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
            traceforge::sleep(b.delta);
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
        if acks != ps.len() { continue; }

        if maybe_crash(crashes) { return; }

        for id in &ps {
            traceforge::send_msg(*id, PMsg::Commit { round });
            if maybe_crash(crashes) { return; }
        }
    }
}

// ==== Participant =====================================================

fn participant(b: Bounds, num_ps: u32, index: u32, crashes: bool, rounds: u32) {
    if maybe_crash(crashes) { return; }
    let mut dead = false;
    for round in 0..rounds {
        if dead { continue; }

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
        traceforge::sleep(b.delta * (index as u64 + 1));

        let voted_yes: bool = traceforge::nondet();
        let vote = if voted_yes { CMsg::Yes { round } } else { CMsg::No { round } };
        if maybe_crash(crashes) { return; }
        traceforge::send_msg(coord_id, vote);
        if !voted_yes { continue; }

        if maybe_crash(crashes) { return; }

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
        traceforge::sleep(b.delta * num_ps as u64);
        traceforge::send_msg(coord_id, CMsg::Ack { round });
        if maybe_crash(crashes) { return; }

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

// ==== Runner ==========================================================

fn build_config(b: Bounds, use_timed: bool) -> Config {
    let mut builder = Config::builder()
        .with_keep_going_after_error(true)
        .with_verbose(0)
        .with_progress_report(usize::MAX);
    if use_timed {
        builder = builder.with_timed(0, b.u, 0);
    }
    builder.build()
}

fn run_3pc(b: Bounds, num_ps: u32, rounds: u32, use_timed: bool, crashes: bool) -> (Stats, Duration) {
    let cfg = build_config(b, use_timed);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let mut handles = Vec::new();
        for i in 0..num_ps {
            handles.push(thread::spawn(move || participant(b, num_ps, i, crashes, rounds)));
        }
        let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        let c = thread::spawn(move || coordinator(b, crashes, rounds));
        traceforge::send_msg(c.thread().id(), CMsg::Init { peers: peer_ids });
        for h in handles { let _ = h.join(); }
        let _ = c.join();
    });
    (stats, t0.elapsed())
}

// ==== Tests ===========================================================

fn wu_sweep_table(rounds: u32) {
    let n = DEFAULT_PARTICIPANTS;
    let mid_idx = SWEEP_RATIOS.len() / 2;
    let delta = U * SWEEP_RATIOS[mid_idx];
    let crashes = true;

    let mut rows = Vec::new();
    for &r in SWEEP_RATIOS {
        let b = Bounds::with_delta(U, r, delta);
        let (s_temp, d_temp) = run_3pc(b, n, rounds, true, crashes);
        let (s_orig, d_orig) = run_3pc(b, n, rounds, false, crashes);
        rows.push((r, b, s_temp, d_temp, s_orig, d_orig));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== W/U sweep === N={n}, R={rounds}, crashes={crashes}, U={U}, DELTA={delta}").unwrap();
    writeln!(out, "{:<6} {:<4} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "ratio", "W", "tmp.exe", "orig.exe", "tmp.blk", "orig.blk", "wall.tmp", "wall.or", "× exec").unwrap();
    for (r, b, s_temp, d_temp, s_orig, d_orig) in &rows {
        let exec_ratio = s_orig.execs as f64 / s_temp.execs.max(1) as f64;
        writeln!(out, "{:<6} {:<4} {:>10} {:>10} {:>10} {:>10} {:>9.2}s {:>9.2}s {:>7.2}x",
            r, b.w, s_temp.execs, s_orig.execs, s_temp.block, s_orig.block,
            d_temp.as_secs_f64(), d_orig.as_secs_f64(), exec_ratio).unwrap();
    }
    print!("{}", out);
}

fn wu_sweep_r1() {
    wu_sweep_table(1);
}

fn wu_sweep_r2() {
    wu_sweep_table(2);
}

fn main() {
    wu_sweep_r1();
    wu_sweep_r2();
}
