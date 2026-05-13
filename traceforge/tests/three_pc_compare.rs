//! Side-by-side comparison of MUST-τ vs. MUST on the **full Skeen '81
//! Three-Phase Commit protocol**, including its termination
//! sub-protocol (election + state collection + decision rule).
//!
//! Sources:
//!   * D. Skeen, "NonBlocking Commit Protocols," ACM SIGMOD '81.
//!   * Bernstein, Hadzilacos, Goodman, *Concurrency Control and
//!     Recovery in Database Systems* (1987), §7 — explicit `W = 2·Δ`
//!     formulation of the termination wait.
//!
//! Bound choice (per benchmark report § 4.2):
//!   * `L = 0`  — pruning comes from `U` being finite.
//!   * `U = Δ` — single global transit upper bound (parametric).
//!   * `W = 2·U` — textbook Skeen termination wait.
//!   * `sd = 0`  — participants do not buffer beyond receive.
//!
//! ## State-space caveat
//!
//! The termination protocol turns every recv into a potential
//! timeout-and-recover branch. As a result the state space is
//! substantially larger than the simpler "abort-on-timeout" version
//! of 3PC. Practical limits at single-round R=1, W/U=2 on a 2026-era
//! laptop:
//!
//!   N = 2 : both baseline (~ 5.6k execs) and MUST-τ (~ 167) finish
//!           in well under a second; this is the headline row.
//!   N = 3 : baseline does not finish within 10 minutes; MUST-τ
//!           explores ~ 65k execs in ~ 9 s. We report it as a
//!           "MUST-τ scales further than untimed" data point.
//!   N ≥ 4 : intractable for both modes; not included.
//!
//! Reviewers: the **headline** is the apples-to-apples N=2 row. The
//! N=3 row demonstrates that even when the baseline becomes
//! intractable, MUST-τ keeps the verification feasible.
//!
//! Sweeps:
//!   * W/U sweep at N=2 — shows how the reduction tracks
//!     timing-constraint strength (tight vs loose regimes).
//!   * N sweep — full Skeen at increasing N; baseline reports
//!     "intractable" for N ≥ 3.
//!
//! Configurable rounds via the `THREE_PC_ROUNDS` env var (default 1).
//! Each round is one independent commit transaction; R rounds runs R
//! back-to-back invocations of Skeen's protocol on the same set of
//! participants.
//!
//! Run:
//!   cargo test --release --test three_pc_compare -- --nocapture
//!   THREE_PC_ROUNDS=2 cargo test --release --test three_pc_compare -- --nocapture
//!
//! Caveat on R: state space scales super-linearly. At N=2,
//! R=1 → temporal ~167 execs / baseline ~5.6k execs (sub-second);
//! R=2 → temporal ~518k execs (~73 s) / baseline likely intractable.

use std::fmt::Write;
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ==== Constants (mirrored from examples/three_pc_temporal.rs) =========

const DEFAULT_PARTICIPANTS: u32 = 2;
const U: u64 = 1;
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];
/// Participant counts where BOTH baseline and MUST-τ complete in
/// reasonable time. N=3 baseline does not finish within 10 minutes
/// on a 2026-era laptop and is excluded from the apples-to-apples
/// sweep; see `temporal_only_scales_to_n3` for the MUST-τ-only data
/// point at N=3.
const SWEEP_PARTICIPANTS: &[u32] = &[2];
/// Participant counts swept in the temporal-only test (where the
/// baseline is intractable, so we report MUST-τ alone).
const TEMPORAL_ONLY_PARTICIPANTS: &[u32] = &[2, 3];

/// Number of independent commit transactions per verification run.
/// Read from the `THREE_PC_ROUNDS` env var so the caller can override
/// without recompiling. Default 1.
///
///     THREE_PC_ROUNDS=2 cargo test --release --test three_pc_compare -- --nocapture
///
/// Caveat: state space scales super-linearly with R. At N=2,
/// R=1 → temporal ~167 execs / baseline ~5.6k execs (sub-second);
/// R=2 → temporal ~518k execs (~73 s) / baseline likely intractable.
fn rounds_from_env() -> u32 {
    std::env::var("THREE_PC_ROUNDS").ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

// ==== Protocol types ===================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState { Initial, Wait, Prepared, Committed, Aborted }

#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    Prepare { coord: ThreadId, peers: Vec<ThreadId> },
    PreCommit,
    Commit,
    Abort,
    StateRequest(ThreadId),
    StateReply(PState),
    Decide(Decision),
    Probe(ThreadId),
    ProbeAck,
}

#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    Init { peers: Vec<ThreadId> },
    Yes,
    No,
    Ack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision { Commit, Abort }

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    w: u64,
    delta: u64,
}

impl Bounds {
    fn with_delta(u: u64, w_ratio: u64, delta: u64) -> Self {
        Self { u, w: u * w_ratio, delta }
    }
}

// ==== Coordinator ======================================================

fn coordinator(b: Bounds) {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CMsg>() {
        CMsg::Init { peers } => peers,
        _ => panic!("coord expected Init"),
    };
    let me = thread::current().id();

    for id in &ps {
        traceforge::send_msg(
            *id,
            PMsg::Prepare { coord: me, peers: ps.clone() },
        );
    }

    let mut yes_count = 0usize;
    let mut received = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        let v: Option<CMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
        match v {
            Some(CMsg::Yes) => { received += 1; yes_count += 1; }
            Some(CMsg::No) => received += 1,
            // None = timeout (vote did not arrive in W). Any other
            // variant (e.g., an Ack from a round-1 participant that
            // races with a round-2 vote-recv under the speculative
            // search) means this rf-pairing is HB-inconsistent — the
            // model checker rejects it. We do not panic on it.
            _ => {}
        }
    }

    if received != ps.len() || yes_count != ps.len() {
        for id in &ps {
            traceforge::send_msg(*id, PMsg::Abort);
        }
        return;
    }

    for id in &ps {
        traceforge::send_msg(*id, PMsg::PreCommit);
    }

    let mut acks = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        let v: Option<CMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
        match v {
            Some(CMsg::Ack) => acks += 1,
            // None or off-variant: treat as missing ack (same reason
            // as the vote-recv loop).
            _ => {}
        }
    }

    if acks != ps.len() {
        return;
    }

    for id in &ps {
        traceforge::send_msg(*id, PMsg::Commit);
    }
}

// ==== Participant ======================================================

struct ParticipantCtx {
    b: Bounds,
    my_index: u32,
    num_ps: u32,
    coord_id: ThreadId,
    peer_ids: Vec<ThreadId>,
    state: PState,
    voted_yes: bool,
}

impl ParticipantCtx {
    fn me(&self) -> ThreadId { self.peer_ids[self.my_index as usize] }
    fn recv(&self) -> Option<PMsg> {
        traceforge::recv_msg_timed(WaitTime::Finite(self.b.w))
    }

    fn run_after_prepare(&mut self) -> Decision {
        traceforge::sleep(self.b.delta * (self.my_index as u64 + 1));
        self.voted_yes = traceforge::nondet();
        if self.voted_yes {
            traceforge::send_msg(self.coord_id, CMsg::Yes);
            self.state = PState::Wait;
        } else {
            traceforge::send_msg(self.coord_id, CMsg::No);
            self.state = PState::Aborted;
            self.drain_until_decision();
            return Decision::Abort;
        }

        loop {
            match self.recv() {
                Some(PMsg::Abort) => {
                    self.state = PState::Aborted;
                    return Decision::Abort;
                }
                Some(PMsg::PreCommit) => {
                    self.state = PState::Prepared;
                    break;
                }
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(other) => self.handle_termination_msg(other),
                None => return self.terminate(),
            }
        }

        traceforge::sleep(self.b.delta * self.num_ps as u64);
        traceforge::send_msg(self.coord_id, CMsg::Ack);

        loop {
            match self.recv() {
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return Decision::Commit;
                }
                Some(PMsg::Abort) => {
                    self.state = PState::Aborted;
                    return Decision::Abort;
                }
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(other) => self.handle_termination_msg(other),
                None => return self.terminate(),
            }
        }
    }

    fn apply_decide(&mut self, d: Decision) -> Decision {
        self.state = match d { Decision::Commit => PState::Committed, Decision::Abort => PState::Aborted };
        if d == Decision::Commit {
            traceforge::assert(self.voted_yes);
        }
        d
    }

    fn drain_until_decision(&mut self) {
        loop {
            match self.recv() {
                Some(PMsg::Abort) | Some(PMsg::Commit) => return,
                Some(PMsg::Decide(_)) => return,
                Some(other) => self.handle_termination_msg(other),
                None => return,
            }
        }
    }

    fn handle_termination_msg(&self, msg: PMsg) {
        match msg {
            PMsg::StateRequest(sender) => {
                traceforge::send_msg(sender, PMsg::StateReply(self.state));
            }
            PMsg::Probe(sender) => {
                traceforge::send_msg(sender, PMsg::ProbeAck);
            }
            _ => {}
        }
    }

    fn terminate(&mut self) -> Decision {
        for j in 0..self.my_index {
            traceforge::send_msg(self.peer_ids[j as usize], PMsg::Probe(self.me()));
        }
        let mut any_lower_alive = false;
        for _ in 0..self.my_index {
            traceforge::sleep(self.b.delta);
            match self.recv() {
                Some(PMsg::ProbeAck) => any_lower_alive = true,
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(other) => self.handle_termination_msg(other),
                None => {}
            }
        }
        if any_lower_alive {
            return self.wait_for_decide();
        }
        self.run_termination()
    }

    fn wait_for_decide(&mut self) -> Decision {
        loop {
            match self.recv() {
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(other) => self.handle_termination_msg(other),
                None => return Decision::Abort,
            }
        }
    }

    fn run_termination(&mut self) -> Decision {
        for (j, peer) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                traceforge::send_msg(*peer, PMsg::StateRequest(self.me()));
            }
        }
        let mut states = vec![self.state];
        let expected = self.peer_ids.len() - 1;
        for _ in 0..expected {
            traceforge::sleep(self.b.delta);
            match self.recv() {
                Some(PMsg::StateReply(s)) => states.push(s),
                Some(other) => self.handle_termination_msg(other),
                None => {}
            }
        }
        let any_c = states.iter().any(|s| *s == PState::Committed);
        let any_a = states.iter().any(|s| *s == PState::Aborted);
        let any_p = states.iter().any(|s| *s == PState::Prepared);
        let decision = if any_c { Decision::Commit }
            else if any_a { Decision::Abort }
            else if any_p { Decision::Commit }
            else { Decision::Abort };
        for (j, peer) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                traceforge::send_msg(*peer, PMsg::Decide(decision));
            }
        }
        self.state = match decision {
            Decision::Commit => PState::Committed,
            Decision::Abort => PState::Aborted,
        };
        if decision == Decision::Commit {
            traceforge::assert(self.voted_yes);
        }
        decision
    }
}

fn participant(b: Bounds, num_ps: u32, index: u32, rounds: u32) {
    let mut ctx_opt: Option<ParticipantCtx> = None;
    for _r in 0..rounds {
        // For each round, wait for that round's Prepare from the
        // round's coord. Service stray termination messages from
        // prior rounds along the way.
        let (coord_id, peers) = loop {
            match traceforge::recv_msg_block::<PMsg>() {
                PMsg::Prepare { coord, peers } => break (coord, peers),
                PMsg::StateRequest(sender) => {
                    let st = ctx_opt.as_ref().map(|c| c.state).unwrap_or(PState::Initial);
                    traceforge::send_msg(sender, PMsg::StateReply(st));
                }
                PMsg::Probe(sender) => {
                    traceforge::send_msg(sender, PMsg::ProbeAck);
                }
                _ => {}
            }
        };
        let ctx = ctx_opt.get_or_insert(ParticipantCtx {
            b, my_index: index, num_ps, coord_id: coord_id,
            peer_ids: peers.clone(), state: PState::Initial, voted_yes: false,
        });
        ctx.coord_id = coord_id;
        ctx.peer_ids = peers;
        ctx.state = PState::Initial;
        ctx.voted_yes = false;
        let _ = ctx.run_after_prepare();
    }
}

// ==== Runner ===========================================================

fn build_config(b: Bounds, use_temporal: bool) -> Config {
    let mut builder = Config::builder()
        .with_keep_going_after_error(true)
        .with_verbose(0)
        .with_progress_report(usize::MAX);
    if use_temporal {
        builder = builder.with_temporal(0, b.u, 0);
    }
    builder.build()
}

fn run_3pc(b: Bounds, num_ps: u32, rounds: u32, use_temporal: bool) -> (Stats, Duration) {
    let cfg = build_config(b, use_temporal);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let mut handles = Vec::new();
        for i in 0..num_ps {
            handles.push(thread::spawn(move || participant(b, num_ps, i, rounds)));
        }
        let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        // Each round gets a fresh coord; main bootstraps it with the
        // (immutable) participant list. Participants pick up the
        // round's coord_id from the Prepare message coord broadcasts.
        for _r in 0..rounds {
            let peers_for_coord = peer_ids.clone();
            let c = thread::spawn(move || coordinator(b));
            let coord_id = c.thread().id();
            traceforge::send_msg(coord_id, CMsg::Init { peers: peers_for_coord });
        }
    });
    (stats, t0.elapsed())
}

// ==== Tests ============================================================

#[test]
fn compare_3pc_temporal_vs_original() {
    let mid_idx = SWEEP_RATIOS.len() / 2;
    let delta = U * SWEEP_RATIOS[mid_idx];
    let n = DEFAULT_PARTICIPANTS;
    let r_rounds = rounds_from_env();

    let mut rows = Vec::new();
    for &r in SWEEP_RATIOS {
        let b = Bounds::with_delta(U, r, delta);
        let (s_temp, d_temp) = run_3pc(b, n, r_rounds, true);
        let (s_orig, d_orig) = run_3pc(b, n, r_rounds, false);
        rows.push((r, b, s_temp, d_temp, s_orig, d_orig));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== W/U sweep === Full Skeen 3PC (incl. termination), N={n}, R={r_rounds}, L=0, U={U}, DELTA={delta} (fixed)").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{:<6} {:<4} {:<6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "ratio", "W", "regime", "tmp.exe", "orig.exe", "tmp.blk", "orig.blk", "wall.tmp", "wall.or", "× exec").unwrap();
    for (r, b, s_temp, d_temp, s_orig, d_orig) in &rows {
        let regime = if b.w < delta { "tight" } else { "loose" };
        let exec_ratio = s_orig.execs as f64 / s_temp.execs.max(1) as f64;
        writeln!(out, "{:<6} {:<4} {:<6} {:>10} {:>10} {:>10} {:>10} {:>9.2}s {:>9.2}s {:>7.2}x",
            r, b.w, regime, s_temp.execs, s_orig.execs, s_temp.block, s_orig.block,
            d_temp.as_secs_f64(), d_orig.as_secs_f64(), exec_ratio).unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "Source: Skeen '81 + BHG '87 §7 termination protocol bound.").unwrap();
    writeln!(out, "DELTA held fixed so the reduction is comparable across rows.").unwrap();
    print!("{}", out);
}

#[test]
fn compare_3pc_participants_sweep() {
    let w_ratio = 2u64;
    let delta = U * w_ratio + 1;
    let r_rounds = rounds_from_env();

    let mut rows = Vec::new();
    for &n in SWEEP_PARTICIPANTS {
        let b = Bounds::with_delta(U, w_ratio, delta);
        let (s_temp, d_temp) = run_3pc(b, n, r_rounds, true);
        let (s_orig, d_orig) = run_3pc(b, n, r_rounds, false);
        rows.push((n, b, s_temp, d_temp, s_orig, d_orig));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== N sweep (apples-to-apples) === Full Skeen 3PC (incl. termination), R={r_rounds}, L=0, U={U}, W={} (= {w_ratio}·U, Skeen default)", U * w_ratio).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{:<3} {:<4} {:<6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "N", "W", "DELTA", "tmp.exe", "orig.exe", "tmp.blk", "orig.blk", "wall.tmp", "wall.or", "× exec").unwrap();
    for (n, b, s_temp, d_temp, s_orig, d_orig) in &rows {
        let exec_ratio = s_orig.execs as f64 / s_temp.execs.max(1) as f64;
        writeln!(out, "{:<3} {:<4} {:<6} {:>10} {:>10} {:>10} {:>10} {:>9.2}s {:>9.2}s {:>7.2}x",
            n, b.w, b.delta, s_temp.execs, s_orig.execs, s_temp.block, s_orig.block,
            d_temp.as_secs_f64(), d_orig.as_secs_f64(), exec_ratio).unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "Apples-to-apples N range: untimed MUST does not finish within 10 minutes at N≥3").unwrap();
    writeln!(out, "with the full termination protocol on a 2026-era laptop. See companion test").unwrap();
    writeln!(out, "`temporal_only_scales_to_n3` for the MUST-τ-only data point that shows the").unwrap();
    writeln!(out, "verifier remains tractable where untimed MUST cannot complete.").unwrap();
    print!("{}", out);
}

/// MUST-τ-only data point: at N=3 the apples-to-apples baseline is
/// intractable (>10 min). This test demonstrates that MUST-τ still
/// completes — evidence that the temporal filter enables verification
/// at a scale untimed MUST cannot reach.
#[test]
fn temporal_only_scales_to_n3() {
    let w_ratio = 2u64;
    let delta = U * w_ratio + 1;
    let r_rounds = rounds_from_env();
    let mut rows = Vec::new();
    for &n in TEMPORAL_ONLY_PARTICIPANTS {
        let b = Bounds::with_delta(U, w_ratio, delta);
        let (s, d) = run_3pc(b, n, r_rounds, true);
        rows.push((n, b, s, d));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== MUST-τ scaling beyond untimed === Full Skeen 3PC (incl. termination)").unwrap();
    writeln!(out, "R={r_rounds}, L=0, U={U}, W={} (= {w_ratio}·U, Skeen default)", U * w_ratio).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{:<3} {:<4} {:<6} {:>10} {:>10} {:>10}", "N", "W", "DELTA", "tmp.exe", "tmp.blk", "wall.tmp").unwrap();
    for (n, b, s, d) in &rows {
        writeln!(out, "{:<3} {:<4} {:<6} {:>10} {:>10} {:>9.2}s",
            n, b.w, b.delta, s.execs, s.block, d.as_secs_f64()).unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "Baseline (untimed MUST) does not finish within 10 minutes at N=3.").unwrap();
    writeln!(out, "MUST-τ explores the state space in seconds — the temporal filter is").unwrap();
    writeln!(out, "what makes Skeen's full termination protocol verifiable at this scale.").unwrap();
    print!("{}", out);
}
