//! Three-Phase Commit (3PC) — Skeen '81 with the **full termination
//! protocol** (election + state collection + decision rule), ported
//! to TraceForge with bounded message delays.
//!
//! Reference: D. Skeen, "NonBlocking Commit Protocols," ACM SIGMOD '81
//! (pp. 133–142). Termination protocol decision rule is rendered per
//! the canonical textbook treatment in Bernstein, Hadzilacos & Goodman,
//! *Concurrency Control and Recovery in Database Systems* (1987), §7.
//!
//! ## Protocol — normal case
//!
//!   Phase 1  CanCommit / Vote
//!     coord  ── Prepare ──▶ all participants
//!     each p ── Yes/No  ──▶ coord
//!     coord aborts unless every vote is Yes (and every vote arrived)
//!
//!   Phase 2  PreCommit / Ack
//!     coord  ── PreCommit ──▶ all participants
//!     each p ── Ack       ──▶ coord
//!     coord refuses to commit unless every Ack arrived
//!
//!   Phase 3  DoCommit
//!     coord  ── Commit ──▶ all participants
//!
//! ## Participant state machine (Skeen)
//!
//!   Q (initial) → W (voted yes) → P (received PreCommit) → C (committed)
//!   any state may transition → A (aborted)
//!
//! ## Termination protocol (Skeen §3 / BHG §7)
//!
//! Triggered when a participant times out waiting for a coord message.
//! Election: the lowest-index alive participant becomes the new
//! coordinator (chosen via probe-and-timeout — non-replies = crashed).
//! Then the new coordinator runs:
//!
//!   1. Send STATE-REQ to all alive participants.
//!   2. Collect STATE-REPLY (each participant's local PState) with
//!      a finite wait. Non-replies → assumed crashed.
//!   3. Apply the decision rule on the multiset S of collected states:
//!      a. ∃ C ∈ S        →  broadcast Commit
//!      b. ∃ A ∈ S        →  broadcast Abort
//!      c. ∃ P ∈ S        →  broadcast Commit (rule (c) of Skeen,
//!                            since no C/A means safe to commit if any
//!                            participant is past PreCommit)
//!      d. all in W or Q  →  broadcast Abort
//!
//! Rule (c) is what makes 3PC *non-blocking*: even if the coord crashed
//! mid-Phase-2 leaving some participants in P and others in W, the
//! termination protocol can safely commit.
//!
//! ## Bounded execution
//!
//!   * `--rounds R` — number of independent commit transactions to
//!                    run. Each round is a fresh 3PC invocation.
//!                    Default 1 (Skeen's per-transaction analysis).
//!   * Termination attempts — naturally bounded at N (once every
//!                            participant has tried, nobody left).
//!
//! ## Implementation note: unified message enums
//!
//! TraceForge's `recv_msg_block::<T>()` panics if the next queued
//! message isn't of type T (it does not filter by type). Therefore
//! every message sent to a participant uses the same `PMsg` enum and
//! every message sent to the coordinator uses the same `CMsg` enum —
//! the *variants* distinguish protocol-level message types.

use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DEFAULT_PARTICIPANTS: u32 = 3;
const DEFAULT_U: u64 = 1;
const DEFAULT_W_RATIO: u64 = 2;
const DEFAULT_ROUNDS: u32 = 1;
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];
const SWEEP_PARTICIPANTS: &[u32] = &[3, 4, 5];

/// Skeen participant state machine. {q, w, p, c, a} from the paper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState {
    Initial,    // q
    Wait,       // w
    Prepared,   // p
    Committed,  // c
    Aborted,    // a
}

/// Every message that can arrive at a *participant*. Single enum so the
/// recv at the participant's queue is type-stable across all protocol
/// roles.
#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    /// Phase 1 from coord. Doubles as participant bootstrap: carries
    /// the peer-id list and the coord id (so participants learn the
    /// world from the coord rather than from a separate main-thread
    /// Init send — that way every message at the participant has a
    /// single canonical sender per logical round).
    Prepare { coord: ThreadId, peers: Vec<ThreadId> },
    /// Phase 2 from coord (or termination-coord rule (c)).
    PreCommit,
    /// Phase 3 from coord (or termination-coord rule (a)/(c)).
    Commit,
    /// Coord-decided abort (Phase 1 outcome, or termination rule (b)/(d)).
    Abort,
    /// Termination protocol: peer is asking for our state.
    StateRequest(ThreadId), // sender id for the reply
    /// Termination protocol: peer is replying with their state.
    StateReply(PState),
    /// Termination protocol: final decision broadcast by terminator.
    Decide(Decision),
    /// Election: peer is checking if we are alive.
    Probe(ThreadId),
    /// Election: peer is confirming they are alive.
    ProbeAck,
}

/// Every message that can arrive at the *coordinator*. Single enum.
#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    /// Bootstrap from main thread.
    Init { peers: Vec<ThreadId> },
    /// Vote replies.
    Yes,
    No,
    /// Phase-2 ack replies.
    Ack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Temporal,
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    w: u64,
    delta: u64,
}

impl Bounds {
    fn from_ratio(u: u64, w_ratio: u64) -> Self {
        assert!(u >= 1);
        assert!(w_ratio >= 2);
        let w = u * w_ratio;
        Self { u, w, delta: w + 1 }
    }
    fn with_delta(u: u64, w_ratio: u64, delta: u64) -> Self {
        Self { u, w: u * w_ratio, delta }
    }
}

// =====================================================================
// Coordinator
// =====================================================================

fn coordinator(mode: Mode, b: Bounds, crashes: bool) {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CMsg>() {
        CMsg::Init { peers } => peers,
        _ => panic!("coord expected Init"),
    };

    // Phase 1: Prepare → Vote.  Each Prepare doubles as participant
    // bootstrap, carrying coord_id and the full peer list.
    let me_id = thread::current().id();
    for id in &ps {
        traceforge::send_msg(
            *id,
            PMsg::Prepare { coord: me_id, peers: ps.clone() },
        );
    }

    let mut yes_count = 0usize;
    let mut received_count = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(b.delta);
        }
        // Both modes use the *same* timed receive. The only difference
        // is whether `with_temporal` is set on the config — if it is,
        // the temporal filter prunes infeasible Some-rfs. None branch
        // is admissible in both modes (timeout is always a possibility
        // when the wait is finite). This is the "same code, toggled
        // pruning" comparison the eval needs.
        let v: Option<CMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
        match v {
            Some(CMsg::Yes) => {
                received_count += 1;
                yes_count += 1;
            }
            Some(CMsg::No) => {
                received_count += 1;
            }
            // None = timeout; off-variant = HB-inconsistent rf the
            // model checker will reject. Either way, count as missing.
            _ => {}
        }
    }

    let unanimous = received_count == ps.len() && yes_count == ps.len();
    if !unanimous {
        for id in &ps {
            traceforge::send_msg(*id, PMsg::Abort);
        }
        return;
    }

    // Coord may crash before broadcasting PreCommit.
    if crashes && traceforge::nondet() {
        return;
    }

    // Phase 2: PreCommit → Ack
    for id in &ps {
        traceforge::send_msg(*id, PMsg::PreCommit);
    }

    if crashes && traceforge::nondet() {
        return;
    }

    let mut acks_received = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(b.delta);
        }
        let v: Option<CMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
        match v {
            Some(CMsg::Ack) => acks_received += 1,
            _ => {} // None or off-variant → missing ack
        }
    }

    if acks_received != ps.len() {
        return;
    }

    if crashes && traceforge::nondet() {
        return;
    }

    // Phase 3: Commit
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Commit);
    }
}

// =====================================================================
// Participant + termination protocol
// =====================================================================

struct ParticipantCtx {
    mode: Mode,
    b: Bounds,
    my_index: u32,
    num_ps: u32,
    coord_id: ThreadId,
    peer_ids: Vec<ThreadId>,
    state: PState,
    voted_yes: bool,
    crashes: bool,
}

impl ParticipantCtx {
    fn me(&self) -> ThreadId {
        self.peer_ids[self.my_index as usize]
    }

    /// Receive next participant-bound message. Both modes use the same
    /// timed receive; only the temporal-config differs between baseline
    /// and temporal modes (and thus the pruning of admissible reads).
    fn recv(&self) -> Option<PMsg> {
        traceforge::recv_msg_timed(WaitTime::Finite(self.b.w))
    }

    /// Run one round of 3PC starting AFTER Prepare has already been
    /// consumed (by the outer participant() bootstrap loop). Returns
    /// the participant's decision.
    fn run_after_prepare(&mut self) -> Decision {
        // Crash before voting.
        if self.crashes && traceforge::nondet() {
            self.state = PState::Initial;
            return Decision::Abort;
        }

        if self.mode == Mode::Temporal {
            traceforge::sleep(self.b.delta * (self.my_index as u64 + 1));
        }

        self.voted_yes = traceforge::nondet();
        if self.voted_yes {
            traceforge::send_msg(self.coord_id, CMsg::Yes);
            self.state = PState::Wait;
        } else {
            traceforge::send_msg(self.coord_id, CMsg::No);
            self.state = PState::Aborted;
            // No-voter still needs to receive the eventual Abort
            // from the coord (or termination); drain it so this
            // participant's state stays clean across rounds.
            self.drain_until_decision();
            return Decision::Abort;
        }

        // Wait for PreCommit / Abort.
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

        // Crash before sending Ack.
        if self.crashes && traceforge::nondet() {
            return Decision::Abort;
        }

        if self.mode == Mode::Temporal {
            traceforge::sleep(self.b.delta * self.num_ps as u64);
        }
        traceforge::send_msg(self.coord_id, CMsg::Ack);

        // Wait for Commit.
        loop {
            match self.recv() {
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    assert!(self.voted_yes, "atomicity: committed without Yes vote");
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

    /// Apply a terminator's Decide and update state with atomicity check.
    fn apply_decide(&mut self, d: Decision) -> Decision {
        self.state = match d {
            Decision::Commit => PState::Committed,
            Decision::Abort => PState::Aborted,
        };
        if d == Decision::Commit {
            assert!(self.voted_yes, "atomicity: termination committed a No-voter");
        }
        d
    }

    /// Drain stray messages until we observe a decision. Used by the
    /// No-voter path so it doesn't leave undelivered Aborts in its
    /// queue across rounds.
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

    /// Service an off-protocol termination message (state request,
    /// probe, etc.) while we are on the normal-case wait path.
    fn handle_termination_msg(&self, msg: PMsg) {
        match msg {
            PMsg::StateRequest(sender) => {
                traceforge::send_msg(sender, PMsg::StateReply(self.state));
            }
            PMsg::Probe(sender) => {
                traceforge::send_msg(sender, PMsg::ProbeAck);
            }
            // StateReply/ProbeAck/Init/Prepare/PreCommit out of place
            // → protocol error; ignoring is safe under fail-stop.
            _ => {}
        }
    }

    /// Termination protocol entry point. Determines whether we are the
    /// designated terminator (lowest-index alive participant) and
    /// either runs termination or waits for the terminator's decision.
    fn terminate(&mut self) -> Decision {
        // Probe every lower-index peer. If any reply, defer.
        for j in 0..self.my_index {
            traceforge::send_msg(self.peer_ids[j as usize], PMsg::Probe(self.me()));
        }
        let mut any_lower_alive = false;
        for _ in 0..self.my_index {
            if self.mode == Mode::Temporal {
                traceforge::sleep(self.b.delta);
            }
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

    /// Block (with timeout) for the terminator's Decide message.
    fn wait_for_decide(&mut self) -> Decision {
        loop {
            match self.recv() {
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(other) => self.handle_termination_msg(other),
                None => return Decision::Abort, // give up; model-checker logs it
            }
        }
    }

    /// We are the designated terminator. Run Skeen's termination protocol.
    fn run_termination(&mut self) -> Decision {
        // 1. STATE-REQUEST to every other peer.
        for (j, peer) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                traceforge::send_msg(*peer, PMsg::StateRequest(self.me()));
            }
        }

        // 2. Collect STATE-REPLY with timeout. Include our own state.
        let mut states: Vec<PState> = vec![self.state];
        let expected = self.peer_ids.len() - 1;
        for _ in 0..expected {
            if self.mode == Mode::Temporal {
                traceforge::sleep(self.b.delta);
            }
            match self.recv() {
                Some(PMsg::StateReply(s)) => states.push(s),
                Some(other) => self.handle_termination_msg(other),
                None => {}
            }
        }

        // 3. Skeen decision rule.
        let any_c = states.iter().any(|s| *s == PState::Committed);
        let any_a = states.iter().any(|s| *s == PState::Aborted);
        let any_p = states.iter().any(|s| *s == PState::Prepared);

        let decision = if any_c {
            Decision::Commit
        } else if any_a {
            Decision::Abort
        } else if any_p {
            // Rule (c): some past PreCommit, none past Commit, none
            // Aborted → safe to commit (and bring W-stragglers along).
            Decision::Commit
        } else {
            Decision::Abort
        };

        // 4. Broadcast Decide to every other peer.
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
            assert!(self.voted_yes, "atomicity: terminator committed a No-voter (self)");
        }
        decision
    }
}

fn participant(mode: Mode, b: Bounds, num_ps: u32, index: u32, crashes: bool, rounds: u32) {
    // We don't have peer_ids or coord_id yet — those come in the first
    // Prepare message. So we can't construct ParticipantCtx until then.
    // For each round, wait for Prepare gracefully (handling any stray
    // termination messages from prior rounds along the way).
    let mut ctx_opt: Option<ParticipantCtx> = None;

    for _r in 0..rounds {
        // Receive Prepare (the round-start signal from coord). Service
        // any stray peer-to-peer termination messages while waiting —
        // see file docstring re: cross-sender ordering.
        let (coord_id, peers, mut prepare_pending) = loop {
            match traceforge::recv_msg_block::<PMsg>() {
                PMsg::Prepare { coord, peers } => break (coord, peers, true),
                // Strays from peers (a previous round's terminator
                // still talking, or a probe that raced ahead of our
                // Prepare). Reply with our current state so the peer
                // can finish its termination round.
                PMsg::StateRequest(sender) => {
                    let state = ctx_opt.as_ref().map(|c| c.state).unwrap_or(PState::Initial);
                    traceforge::send_msg(sender, PMsg::StateReply(state));
                }
                PMsg::Probe(sender) => {
                    traceforge::send_msg(sender, PMsg::ProbeAck);
                }
                // StateReply / ProbeAck / Decide arriving before our
                // Prepare for this round are leftover from prior
                // rounds — safe to drop.
                _ => {}
            }
        };
        let _ = prepare_pending; // suppress unused

        let ctx = ctx_opt.get_or_insert_with(|| ParticipantCtx {
            mode,
            b,
            my_index: index,
            num_ps,
            coord_id,
            peer_ids: peers.clone(),
            state: PState::Initial,
            voted_yes: false,
            crashes,
        });
        ctx.coord_id = coord_id;
        ctx.peer_ids = peers;
        ctx.state = PState::Initial;
        ctx.voted_yes = false;
        let _ = ctx.run_after_prepare();
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, b: Bounds) -> Config {
    let builder = Config::builder().with_progress_report(usize::MAX);
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Temporal => builder.with_temporal(0, b.u, 0).build(),
    }
}

fn run(mode: Mode, num_ps: u32, b: Bounds, crashes: bool, rounds: u32) -> (Stats, Duration) {
    let cfg = build_config(mode, b);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        // Spawn participants; collect their IDs for the peer list.
        let mut handles = Vec::new();
        for i in 0..num_ps {
            handles.push(thread::spawn(move || {
                participant(mode, b, num_ps, i, crashes, rounds)
            }));
        }
        let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();

        for _r in 0..rounds {
            let peers_for_coord = peer_ids.clone();
            let c = thread::spawn(move || coordinator(mode, b, crashes));
            let coord_id = c.thread().id();
            // Only one initial send: the Init bundle that names the
            // peer list goes to the coord. Coord then broadcasts
            // Prepare (which doubles as participant bootstrap).
            traceforge::send_msg(coord_id, CMsg::Init { peers: peers_for_coord });
        }
    });
    (stats, start.elapsed())
}

// =====================================================================
// Reporting (unchanged from prior single-round version)
// =====================================================================

fn print_one(label: &str, num_ps: u32, b: Bounds, rounds: u32, stats: &Stats, dur: Duration, crashes: bool) {
    let crash_tag = if crashes { " (crashes)" } else { "" };
    println!(
        "{label:<10}{crash_tag} N={num_ps} R={rounds}  L=0 U={u} W={w} (W/U={r})  execs={execs:<6} \
         blocked={block:<6} time={dur:?}",
        u = b.u, w = b.w, r = b.w / b.u, execs = stats.execs, block = stats.block, dur = dur,
    );
}

fn print_compare(num_ps: u32, b: Bounds, rounds: u32, crashes: bool, baseline: (Stats, Duration), temporal: (Stats, Duration)) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = temporal;
    println!();
    println!(
        "Three-Phase Commit (Skeen '81 + termination{}): MUST vs MUST-τ",
        if crashes { " + crashes" } else { "" }
    );
    println!("======================================================");
    println!("N = {num_ps}    R = {rounds}    L = 0    U = {}    W = {} (= {}·U)", b.u, b.w, b.w / b.u);
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "temporal", t_stats.execs, t_stats.block, t_dur);
    println!();
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("execs reduction: {exec_ratio:.2}x");
    println!("time  speedup  : {time_ratio:.2}x");
}

fn print_sweep(num_ps: u32, u: u64, delta: u64, rounds: u32, crashes: bool, rows: &[(u64, Bounds, Stats, Duration, Stats, Duration)]) {
    println!();
    println!("Three-Phase Commit (Skeen + termination): W/U sweep{}", if crashes { " + crashes" } else { "" });
    println!("====================================================================");
    println!("N = {num_ps}    R = {rounds}    L = 0    U = {u}    DELTA = {delta} (held fixed)");
    println!();
    println!("{:<6} {:<6} {:<8} {:>10} {:>10} {:>10} {:>10} {:>8}", "ratio", "W", "regime", "base.exec", "temp.exec", "base.blk", "temp.blk", "× exec");
    for (ratio, b, b_stats, _, t_stats, _) in rows {
        let regime = if b.w < delta { "tight" } else { "loose" };
        let r = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
        println!("{:<6} {:<6} {:<8} {:>10} {:>10} {:>10} {:>10} {:>7.2}x",
            ratio, b.w, regime, b_stats.execs, t_stats.execs, b_stats.block, t_stats.block, r);
    }
}

fn print_n_sweep(u: u64, w_ratio: u64, rounds: u32, crashes: bool, rows: &[(u32, Bounds, Stats, Duration, Stats, Duration)]) {
    println!();
    println!("Three-Phase Commit (Skeen + termination): N sweep{}", if crashes { " + crashes" } else { "" });
    println!("====================================================================");
    println!("R = {rounds}    L = 0    U = {u}    W = {} (= {w_ratio}·U)", u * w_ratio);
    println!();
    println!("{:<3} {:<6} {:<6} {:>10} {:>10} {:>10} {:>10} {:>8}", "N", "W", "DELTA", "base.exec", "temp.exec", "base.blk", "temp.blk", "× exec");
    for (n, b, b_stats, _, t_stats, _) in rows {
        let r = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
        println!("{:<3} {:<6} {:<6} {:>10} {:>10} {:>10} {:>10} {:>7.2}x",
            n, b.w, b.delta, b_stats.execs, t_stats.execs, b_stats.block, t_stats.block, r);
    }
}

// =====================================================================
// CLI
// =====================================================================

fn parse_args() -> (String, u32, u64, u64, u32, bool) {
    let mut mode = String::from("compare");
    let mut num_ps = DEFAULT_PARTICIPANTS;
    let mut u = DEFAULT_U;
    let mut w_ratio = DEFAULT_W_RATIO;
    let mut rounds = DEFAULT_ROUNDS;
    let mut crashes = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => mode = args.next().expect("--mode value"),
            "--participants" => num_ps = args.next().expect("--participants value").parse().expect("u32"),
            "--u" => u = args.next().expect("--u value").parse().expect("u64"),
            "--w-ratio" => w_ratio = args.next().expect("--w-ratio value").parse().expect("u64"),
            "--rounds" => rounds = args.next().expect("--rounds value").parse().expect("u32"),
            "--crashes" => crashes = true,
            "--help" | "-h" => {
                eprintln!("Usage: three_pc_temporal [--mode MODE] [--participants N] [--u U] [--w-ratio R] [--rounds R] [--crashes]\n\
                          Modes: baseline | temporal | compare | sweep | n-sweep\n\
                          Defaults: U=1, W/U=2 (Skeen), N=3, R=1.");
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, num_ps, u, w_ratio, rounds, crashes)
}

fn main() {
    let (mode_str, num_ps, u, w_ratio, rounds, crashes) = parse_args();
    assert!(num_ps >= 1);
    assert!(rounds >= 1);
    match mode_str.as_str() {
        "baseline" => {
            let b = Bounds::from_ratio(u, w_ratio);
            let (s, d) = run(Mode::Baseline, num_ps, b, crashes, rounds);
            print_one("baseline", num_ps, b, rounds, &s, d, crashes);
        }
        "temporal" => {
            let b = Bounds::from_ratio(u, w_ratio);
            let (s, d) = run(Mode::Temporal, num_ps, b, crashes, rounds);
            print_one("temporal", num_ps, b, rounds, &s, d, crashes);
        }
        "compare" => {
            let b = Bounds::from_ratio(u, w_ratio);
            let baseline = run(Mode::Baseline, num_ps, b, crashes, rounds);
            let temporal = run(Mode::Temporal, num_ps, b, crashes, rounds);
            print_compare(num_ps, b, rounds, crashes, baseline, temporal);
        }
        "sweep" => {
            let mid_idx = SWEEP_RATIOS.len() / 2;
            let delta = u * SWEEP_RATIOS[mid_idx];
            let mut rows = Vec::new();
            for &r in SWEEP_RATIOS {
                let b = Bounds::with_delta(u, r, delta);
                let baseline = run(Mode::Baseline, num_ps, b, crashes, rounds);
                let temporal = run(Mode::Temporal, num_ps, b, crashes, rounds);
                rows.push((r, b, baseline.0, baseline.1, temporal.0, temporal.1));
            }
            print_sweep(num_ps, u, delta, rounds, crashes, &rows);
        }
        "n-sweep" => {
            let delta = u * w_ratio + 1;
            let mut rows = Vec::new();
            for &n in SWEEP_PARTICIPANTS {
                let b = Bounds::with_delta(u, w_ratio, delta);
                let baseline = run(Mode::Baseline, n, b, crashes, rounds);
                let temporal = run(Mode::Temporal, n, b, crashes, rounds);
                rows.push((n, b, baseline.0, baseline.1, temporal.0, temporal.1));
            }
            print_n_sweep(u, w_ratio, rounds, crashes, &rows);
        }
        other => panic!("invalid --mode: {other}"),
    }
}
