//! Side-by-side comparison of MUST-τ vs. MUST on the **full Skeen '81
//! Three-Phase Commit protocol**, with the termination sub-protocol
//! implemented strictly per Section 5 of the paper:
//!
//!   - **Election**: preassigned ranking (lowest-index slave alive
//!     becomes backup; on its failure the next-indexed slave takes
//!     over). Paper: "the choice could be based on a preassigned
//!     ranking."
//!   - **Decision rule** (page 141, verbatim): "If the concurrency
//!     set for the current state of the backup contains a commit
//!     state, then the transaction is committed. Otherwise, it is
//!     aborted." Uses **only** the backup's own state, not collected
//!     peer states.
//!   - **Backup two-phase protocol**: Phase 1 broadcasts MoveTo(my
//!     state) and waits for acks (skipped if backup is already in c
//!     or a); Phase 2 broadcasts the decision.
//!   - **Reentrant**: every slave may also fail while acting as
//!     backup; the next-indexed slave takes over.
//!
//! Source:
//!   D. Skeen, "NonBlocking Commit Protocols," ACM SIGMOD '81, §5
//!   (pp. 140-141, Figure 7).
//!
//! ## Failures
//!
//! Set `THREE_PC_CRASHES=1` to enable fail-stop crash points at every
//! send/receive boundary on every site (coord and every slave,
//! including when acting as backup). Default is no crashes, matching
//! the original measurement story.
//!
//! ## State-space caveat
//!
//! The reentrant backup loop multiplies branching by N (election
//! depth). Practical limits on a 2026-era laptop at single-round R=1,
//! W/U=2, no crashes:
//!
//!   N = 2 : both baseline and MUST-τ finish in well under a second.
//!   N = 3 : baseline does not finish within 10 minutes; MUST-τ
//!           remains tractable.
//!   N ≥ 4 : intractable for both modes; not included.
//!
//! Reviewers: the headline is the apples-to-apples N=2 row. The N=3
//! row demonstrates that even when the baseline becomes intractable,
//! MUST-τ keeps verification feasible.
//!
//! Configurable rounds via `THREE_PC_ROUNDS` (default 1). Each round
//! is a fresh-thread invocation of Skeen's protocol — true clean-slate
//! restart.
//!
//! Run:
//!   cargo test --release --test three_pc_compare -- --nocapture
//!   THREE_PC_ROUNDS=2 cargo test --release --test three_pc_compare -- --nocapture
//!   THREE_PC_CRASHES=1 cargo test --release --test three_pc_compare -- --nocapture

use std::fmt::Write;
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ==== Constants =======================================================

const DEFAULT_PARTICIPANTS: u32 = 2;
const U: u64 = 1;
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];
const SWEEP_PARTICIPANTS: &[u32] = &[2];
const TEMPORAL_ONLY_PARTICIPANTS: &[u32] = &[2, 3];

fn rounds_from_env() -> u32 {
    std::env::var("THREE_PC_ROUNDS").ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

fn crashes_from_env() -> bool {
    std::env::var("THREE_PC_CRASHES").ok()
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ==== Protocol types ==================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState { Initial, Wait, Prepared, Committed, Aborted }

#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    Prepare { coord: ThreadId, peers: Vec<ThreadId> },
    PreCommit,
    Commit,
    Abort,
    MoveTo { backup: ThreadId, state: PState },
    BackupAck { from: ThreadId },
}

#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    Init { peers: Vec<ThreadId> },
    Yes, No,
    Ack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision { Commit, Abort }

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

// ==== Coordinator (Skeen Figure 7, site 1) ============================

fn coordinator(b: Bounds, crashes: bool) {
    let ps: Vec<ThreadId> = loop {
        match traceforge::recv_msg_block::<CMsg>() {
            CMsg::Init { peers } => break peers,
            _ => {}
        }
    };
    let me = thread::current().id();

    if maybe_crash(crashes) { return; }

    // Phase 1: xact broadcast (q1 → w1).
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Prepare { coord: me, peers: ps.clone() });
        if maybe_crash(crashes) { return; }
    }

    // Phase 1: vote collection.
    let mut yes = 0usize;
    let mut got = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
            Some(CMsg::Yes) => { got += 1; yes += 1; }
            Some(CMsg::No)  => got += 1,
            _ => {} // None timeout or HB-inconsistent off-variant
        }
        if maybe_crash(crashes) { return; }
    }

    if got != ps.len() || yes != ps.len() {
        for id in &ps {
            if maybe_crash(crashes) { return; }
            traceforge::send_msg(*id, PMsg::Abort);
        }
        return;
    }

    if maybe_crash(crashes) { return; }

    // Phase 2: prepare broadcast (w1 → p1).
    for id in &ps {
        traceforge::send_msg(*id, PMsg::PreCommit);
        if maybe_crash(crashes) { return; }
    }

    let mut acks = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
            Some(CMsg::Ack) => acks += 1,
            _ => {}
        }
        if maybe_crash(crashes) { return; }
    }
    if acks != ps.len() { return; }

    if maybe_crash(crashes) { return; }

    // Phase 3: commit broadcast (p1 → c1).
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Commit);
        if maybe_crash(crashes) { return; }
    }
}

// ==== Slave + Skeen Section 5 termination =============================

struct SlaveCtx {
    b: Bounds,
    my_index: u32,
    num_ps: u32,
    coord_id: ThreadId,
    peer_ids: Vec<ThreadId>,
    state: PState,
    voted_yes: bool,
    crashes: bool,
}

enum BackupOutcome { Decision(Decision), CurrentDead }

impl SlaveCtx {
    fn me(&self) -> ThreadId { self.peer_ids[self.my_index as usize] }
    fn recv(&self) -> Option<PMsg> {
        traceforge::recv_msg_timed(WaitTime::Finite(self.b.w))
    }

    /// Skeen decision rule (page 141). For 3PC: { p, c } ⇒ commit.
    fn decision_rule(&self) -> Decision {
        match self.state {
            PState::Prepared | PState::Committed => Decision::Commit,
            _ => Decision::Abort,
        }
    }

    fn handle_move_to(&mut self, backup: ThreadId, target: PState) {
        if self.state != PState::Committed && self.state != PState::Aborted {
            self.state = target;
        }
        traceforge::send_msg(backup, PMsg::BackupAck { from: self.me() });
    }

    fn run_after_prepare(&mut self) -> Decision {
        if maybe_crash(self.crashes) { return Decision::Abort; }

        traceforge::sleep(self.b.delta * (self.my_index as u64 + 1));

        self.voted_yes = traceforge::nondet();
        if self.voted_yes {
            if maybe_crash(self.crashes) { return Decision::Abort; }
            traceforge::send_msg(self.coord_id, CMsg::Yes);
            self.state = PState::Wait;
        } else {
            if maybe_crash(self.crashes) { return Decision::Abort; }
            traceforge::send_msg(self.coord_id, CMsg::No);
            self.state = PState::Aborted;
            return Decision::Abort;
        }

        if maybe_crash(self.crashes) { return Decision::Abort; }

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
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return Decision::Commit;
                }
                Some(PMsg::MoveTo { backup, state }) => {
                    self.handle_move_to(backup, state);
                }
                Some(_) => {}
                None => return self.terminate(),
            }
        }

        if maybe_crash(self.crashes) { return Decision::Abort; }

        traceforge::sleep(self.b.delta * self.num_ps as u64);
        traceforge::send_msg(self.coord_id, CMsg::Ack);

        if maybe_crash(self.crashes) { return Decision::Abort; }

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
                Some(PMsg::MoveTo { backup, state }) => {
                    self.handle_move_to(backup, state);
                }
                Some(_) => {}
                None => return self.terminate(),
            }
        }
    }

    fn terminate(&mut self) -> Decision {
        let mut current = 0u32;
        while current < self.num_ps {
            if current == self.my_index {
                return self.run_backup_protocol();
            }
            match self.wait_for_backup(current) {
                BackupOutcome::Decision(d) => return d,
                BackupOutcome::CurrentDead => current += 1,
            }
        }
        let d = self.decision_rule();
        self.state = match d {
            Decision::Commit => PState::Committed,
            Decision::Abort => PState::Aborted,
        };
        if d == Decision::Commit { traceforge::assert(self.voted_yes); }
        d
    }

    fn wait_for_backup(&mut self, current_idx: u32) -> BackupOutcome {
        let current_peer = self.peer_ids[current_idx as usize];
        loop {
            match self.recv() {
                Some(PMsg::MoveTo { backup, state }) => {
                    if backup == current_peer {
                        self.handle_move_to(backup, state);
                    }
                }
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return BackupOutcome::Decision(Decision::Commit);
                }
                Some(PMsg::Abort) => {
                    self.state = PState::Aborted;
                    return BackupOutcome::Decision(Decision::Abort);
                }
                Some(_) => {}
                None => return BackupOutcome::CurrentDead,
            }
        }
    }

    fn run_backup_protocol(&mut self) -> Decision {
        let decision = self.decision_rule();
        let need_phase1 = self.state != PState::Committed
            && self.state != PState::Aborted;

        if need_phase1 {
            let my_state = self.state;
            let me = self.me();

            for (j, p) in self.peer_ids.iter().enumerate() {
                if j as u32 != self.my_index {
                    if maybe_crash(self.crashes) { return decision; }
                    traceforge::send_msg(*p, PMsg::MoveTo { backup: me, state: my_state });
                }
            }

            let needed = self.peer_ids.len() - 1;
            for _ in 0..needed {
                if maybe_crash(self.crashes) { return decision; }
                traceforge::sleep(self.b.delta);
                match self.recv() {
                    Some(PMsg::BackupAck { .. }) => {}
                    Some(PMsg::Commit) => {
                        self.state = PState::Committed;
                        traceforge::assert(self.voted_yes);
                        return Decision::Commit;
                    }
                    Some(PMsg::Abort) => {
                        self.state = PState::Aborted;
                        return Decision::Abort;
                    }
                    Some(PMsg::MoveTo { backup, state }) => {
                        self.handle_move_to(backup, state);
                    }
                    Some(_) => {}
                    None => break,
                }
            }
        }

        if maybe_crash(self.crashes) { return decision; }

        let msg = match decision {
            Decision::Commit => PMsg::Commit,
            Decision::Abort => PMsg::Abort,
        };
        for (j, p) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                if maybe_crash(self.crashes) { return decision; }
                traceforge::send_msg(*p, msg.clone());
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

fn slave(b: Bounds, num_ps: u32, index: u32, crashes: bool) {
    if maybe_crash(crashes) { return; }

    let (coord_id, peers) = loop {
        match traceforge::recv_msg_block::<PMsg>() {
            PMsg::Prepare { coord, peers } => break (coord, peers),
            _ => {}
        }
    };

    let mut ctx = SlaveCtx {
        b, my_index: index, num_ps, coord_id,
        peer_ids: peers, state: PState::Initial, voted_yes: false, crashes,
    };
    let _ = ctx.run_after_prepare();
}

// ==== Runner ==========================================================

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

fn run_3pc(b: Bounds, num_ps: u32, rounds: u32, use_temporal: bool, crashes: bool) -> (Stats, Duration) {
    let cfg = build_config(b, use_temporal);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        // Each round = fresh threads = clean slate.
        for _r in 0..rounds {
            let mut handles = Vec::new();
            for i in 0..num_ps {
                handles.push(thread::spawn(move || slave(b, num_ps, i, crashes)));
            }
            let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
            let c = thread::spawn(move || coordinator(b, crashes));
            traceforge::send_msg(c.thread().id(), CMsg::Init { peers: peer_ids });
            for h in handles { let _ = h.join(); }
            let _ = c.join();
        }
    });
    (stats, t0.elapsed())
}

// ==== Tests ===========================================================

#[test]
fn compare_3pc_temporal_vs_original() {
    let mid_idx = SWEEP_RATIOS.len() / 2;
    let delta = U * SWEEP_RATIOS[mid_idx];
    let n = DEFAULT_PARTICIPANTS;
    let r_rounds = rounds_from_env();
    let crashes = crashes_from_env();

    let mut rows = Vec::new();
    for &r in SWEEP_RATIOS {
        let b = Bounds::with_delta(U, r, delta);
        let (s_temp, d_temp) = run_3pc(b, n, r_rounds, true, crashes);
        let (s_orig, d_orig) = run_3pc(b, n, r_rounds, false, crashes);
        rows.push((r, b, s_temp, d_temp, s_orig, d_orig));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== W/U sweep === Skeen '81 3PC (incl. §5 termination, reentrant backup), N={n}, R={r_rounds}, crashes={crashes}, L=0, U={U}, DELTA={delta} (fixed)").unwrap();
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
    writeln!(out, "Source: Skeen '81 §5 (central-site termination protocol).").unwrap();
    writeln!(out, "DELTA held fixed so the reduction is comparable across rows.").unwrap();
    print!("{}", out);
}

#[test]
fn compare_3pc_participants_sweep() {
    let w_ratio = 2u64;
    let delta = U * w_ratio + 1;
    let r_rounds = rounds_from_env();
    let crashes = crashes_from_env();

    let mut rows = Vec::new();
    for &n in SWEEP_PARTICIPANTS {
        let b = Bounds::with_delta(U, w_ratio, delta);
        let (s_temp, d_temp) = run_3pc(b, n, r_rounds, true, crashes);
        let (s_orig, d_orig) = run_3pc(b, n, r_rounds, false, crashes);
        rows.push((n, b, s_temp, d_temp, s_orig, d_orig));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== N sweep (apples-to-apples) === Skeen '81 3PC (incl. §5 termination, reentrant backup), R={r_rounds}, crashes={crashes}, L=0, U={U}, W={} (= {w_ratio}·U, Skeen default)", U * w_ratio).unwrap();
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
    writeln!(out, "with the reentrant backup termination protocol on a 2026-era laptop. See companion").unwrap();
    writeln!(out, "test `temporal_only_scales_to_n3` for the MUST-τ-only data point at N=3.").unwrap();
    print!("{}", out);
}

#[test]
fn temporal_only_scales_to_n3() {
    let w_ratio = 2u64;
    let delta = U * w_ratio + 1;
    let r_rounds = rounds_from_env();
    let crashes = crashes_from_env();
    let mut rows = Vec::new();
    for &n in TEMPORAL_ONLY_PARTICIPANTS {
        let b = Bounds::with_delta(U, w_ratio, delta);
        let (s, d) = run_3pc(b, n, r_rounds, true, crashes);
        rows.push((n, b, s, d));
    }

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(out, "=== MUST-τ scaling beyond untimed === Skeen '81 3PC (incl. §5 termination)").unwrap();
    writeln!(out, "R={r_rounds}, crashes={crashes}, L=0, U={U}, W={} (= {w_ratio}·U, Skeen default)", U * w_ratio).unwrap();
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
