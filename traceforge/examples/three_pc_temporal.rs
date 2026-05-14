//! Three-Phase Commit (3PC) — **strictly** as described in
//!
//!   D. Skeen, "NonBlocking Commit Protocols," ACM SIGMOD '81
//!   pp. 133-142.
//!
//! Section / figure references below cite that paper directly.
//!
//! ## Normal-case protocol (Figure 7, central-site 3PC)
//!
//!   Phase 1 (coord state q1 → w1)
//!     coord  ── xact ──▶ every slave
//!     slave  qi ── xact / yes ──▶ wi   (vote yes)   OR
//!            qi ── xact / no  ──▶ ai   (vote no, terminal abort)
//!   coord decision in w1:
//!     all yes  →  w1 → p1 (broadcast `prepare`)
//!     any no   →  w1 → a1 (broadcast `abort`)
//!
//!   Phase 2 (coord state p1)
//!     coord  ── prepare ──▶ every slave
//!     slave  wi ── prepare / ack ──▶ pi
//!     coord collects acks
//!
//!   Phase 3 (coord state c1)
//!     coord  ── commit ──▶ every slave
//!     slave  pi ── commit ──▶ ci
//!
//! ## Termination protocol (Section 5 — central-site termination)
//!
//! Triggered whenever a slave's wait times out (= coord failure
//! detected). Per the paper:
//!
//!   "The basic idea of this scheme is to choose a coordinator, which
//!   we will call a *backup coordinator*, from the set of operational
//!   sites. ... Since the backup can fail before terminating the
//!   transaction, the protocol must be reentrant."
//!
//! Election: "the choice could be based on a preassigned ranking."
//! We use the lowest-index slave still operational; on its failure
//! the next-indexed slave takes over (reentrancy).
//!
//! Decision rule (quoted from page 141):
//!   "If the concurrency set for the current state of the backup
//!    contains a commit state, then the transaction is committed.
//!    Otherwise, it is aborted."
//!
//! For the canonical 3PC of Figure 7 the concurrency sets are:
//!   q : { q, w }           → no commit → abort
//!   w : { w, p }           → no commit → abort
//!   p : { p, c }           → has commit → commit
//!   c : { c }              → commit
//!   a : { a }              → abort
//!
//! Backup's two-phase protocol (quoted, page 141):
//!   "Phase 1: The backup issues a message to all sites to make a
//!    transition to its local state. The backup then waits for an
//!    acknowledgment from each site.
//!    Phase 2: The backup issues a commit or abort message to each
//!    site (by applying the decision rule given above).
//!    If the backup is initially in a commit or an abort state, then
//!    the first phase can be omitted."
//!
//! Reentrancy is realised by every slave keeping `current_backup` —
//! starting at index 0, incremented on each timeout. When
//! `current_backup == my_index`, the slave assumes the role.
//!
//! ## Failures
//!
//! Every site (coordinator and every slave, including a slave acting
//! as backup) may fail at every send/receive boundary when the
//! `crashes` flag is set, modelled as `crashes && nondet()`. Failures
//! are fail-stop: a crashed thread simply returns.
//!
//! ## Rounds
//!
//! Each round is one independent invocation of the protocol with
//! freshly spawned threads (true clean slate — no carried-over state
//! or stale queue entries).
//!
//! ## Implementation note: unified message enums
//!
//! TraceForge's `recv_msg_block::<T>()` panics if the next queued
//! message isn't of type T (it does not filter by type). Therefore
//! every message sent to a slave uses the same `PMsg` enum and every
//! message sent to the coordinator uses the same `CMsg` enum.

use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DEFAULT_PARTICIPANTS: u32 = 3;
const DEFAULT_U: u64 = 1;
const DEFAULT_W_RATIO: u64 = 2;
const DEFAULT_ROUNDS: u32 = 1;
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];
const SWEEP_PARTICIPANTS: &[u32] = &[3, 4, 5];

/// Skeen slave-state FSA (Figure 7): {q, w, p, c, a}.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState {
    Initial,    // q
    Wait,       // w
    Prepared,   // p
    Committed,  // c  (terminal)
    Aborted,    // a  (terminal)
}

/// Messages received by a slave. The same queue carries normal-protocol
/// messages from the coord *and* termination-protocol messages from
/// whichever slave is currently acting as backup coordinator.
#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    /// Phase 1 from coord. Doubles as bootstrap: carries the coord's
    /// id and the full peer list so slaves can address each other
    /// during termination without a separate init step.
    Prepare { coord: ThreadId, peers: Vec<ThreadId> },
    /// Phase 2 from coord.
    PreCommit,
    /// Phase 3 from coord OR Phase 2 of a backup (final decision).
    Commit,
    /// Phase 1 abort from coord OR Phase 2 of a backup (final decision).
    Abort,
    /// Termination Phase 1: backup tells slave to transition to `state`.
    /// `backup` is the sender id so the slave knows where to send the
    /// ack and which backup it is currently obeying.
    MoveTo { backup: ThreadId, state: PState },
    /// Termination Phase 1 ack from a slave to the backup.
    BackupAck { from: ThreadId },
}

/// Messages received by the coordinator. The original coord's queue
/// only ever sees these. (Backup coordinators are slaves and therefore
/// receive PMsg, not CMsg.)
#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    /// Bootstrap from main: tells the coord who the slaves are.
    Init { peers: Vec<ThreadId> },
    /// Phase 1 vote replies.
    Yes,
    No,
    /// Phase 2 acks.
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

/// Fail-stop crash: returns `true` iff the site should crash here.
/// Threaded through every send/recv boundary on every role per the
/// paper's assumption that "site failures are fail-stop" and can occur
/// at any point during a state transition.
fn maybe_crash(crashes: bool) -> bool {
    crashes && traceforge::nondet()
}

// =====================================================================
// Coordinator (Skeen Figure 7, site 1)
// =====================================================================

fn coordinator(mode: Mode, b: Bounds, crashes: bool) {
    // Wait for bootstrap. Drop strays the model checker speculatively
    // pairs with this recv under HB-inconsistent rfs (rejected later).
    let ps: Vec<ThreadId> = loop {
        match traceforge::recv_msg_block::<CMsg>() {
            CMsg::Init { peers } => break peers,
            _ => {}
        }
    };
    let me = thread::current().id();

    if maybe_crash(crashes) { return; }

    // Phase 1: q1 → w1 (broadcast xact).
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Prepare { coord: me, peers: ps.clone() });
        if maybe_crash(crashes) { return; }
    }

    // Phase 1: collect votes.
    let mut yes = 0usize;
    let mut received = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(b.delta);
        }
        let v: Option<CMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
        match v {
            Some(CMsg::Yes) => { received += 1; yes += 1; }
            Some(CMsg::No)  => { received += 1; }
            _ => {} // timeout or HB-inconsistent off-variant
        }
        if maybe_crash(crashes) { return; }
    }

    // Phase 1 decision: any non-yes or any missing vote → abort all.
    if received != ps.len() || yes != ps.len() {
        for id in &ps {
            if maybe_crash(crashes) { return; }
            traceforge::send_msg(*id, PMsg::Abort);
        }
        return;
    }

    if maybe_crash(crashes) { return; }

    // Phase 2: w1 → p1 (broadcast prepare).
    for id in &ps {
        traceforge::send_msg(*id, PMsg::PreCommit);
        if maybe_crash(crashes) { return; }
    }

    // Phase 2: collect acks.
    let mut acks = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(b.delta);
        }
        let v: Option<CMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
        match v {
            Some(CMsg::Ack) => acks += 1,
            _ => {}
        }
        if maybe_crash(crashes) { return; }
    }

    if acks != ps.len() {
        // Missing acks. Per the paper coord stays in p1 and times out;
        // the termination protocol takes over at the slaves. Coord
        // simply returns without sending commit.
        return;
    }

    if maybe_crash(crashes) { return; }

    // Phase 3: p1 → c1 (broadcast commit).
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Commit);
        if maybe_crash(crashes) { return; }
    }
}

// =====================================================================
// Slave + termination protocol (Skeen Figure 7 site i, Section 5)
// =====================================================================

struct SlaveCtx {
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

/// Outcome of a single iteration of the reentrant backup loop.
enum BackupOutcome {
    /// We received a final commit/abort from the current backup.
    Decision(Decision),
    /// The current backup did not respond within W; advance election.
    CurrentDead,
}

impl SlaveCtx {
    fn me(&self) -> ThreadId { self.peer_ids[self.my_index as usize] }

    fn recv(&self) -> Option<PMsg> {
        traceforge::recv_msg_timed(WaitTime::Finite(self.b.w))
    }

    /// Skeen decision rule (Section 5): commit iff concurrency set of
    /// my state contains the commit state. For 3PC: { p, c } ⇒ commit.
    fn decision_rule(&self) -> Decision {
        match self.state {
            PState::Prepared | PState::Committed => Decision::Commit,
            _ => Decision::Abort,
        }
    }

    /// Honor an incoming `MoveTo` from a backup. The paper says a
    /// committed/aborted slave is in a terminal state and cannot
    /// transition further (page 137: "the act of committing or
    /// aborting is irreversible"). Such a slave still acks so the
    /// backup makes progress on its Phase-1 wait.
    fn handle_move_to(&mut self, backup: ThreadId, target: PState) {
        if self.state != PState::Committed && self.state != PState::Aborted {
            self.state = target;
        }
        traceforge::send_msg(backup, PMsg::BackupAck { from: self.me() });
    }

    /// Run the slave-side protocol after we've consumed Prepare.
    fn run_after_prepare(&mut self) -> Decision {
        if maybe_crash(self.crashes) { return Decision::Abort; }

        if self.mode == Mode::Temporal {
            // Stagger so the model checker can prune cross-mapping rfs.
            traceforge::sleep(self.b.delta * (self.my_index as u64 + 1));
        }

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

        // Wait for Phase 2 message from coord (PreCommit or Abort).
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
                    // Some backup already decided commit while we were
                    // still in w. Honor it.
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return Decision::Commit;
                }
                Some(PMsg::MoveTo { backup, state }) => {
                    self.handle_move_to(backup, state);
                }
                Some(_) => {} // stray BackupAck — ignore
                None => return self.terminate(),
            }
        }

        if maybe_crash(self.crashes) { return Decision::Abort; }

        if self.mode == Mode::Temporal {
            traceforge::sleep(self.b.delta * self.num_ps as u64);
        }
        traceforge::send_msg(self.coord_id, CMsg::Ack);

        if maybe_crash(self.crashes) { return Decision::Abort; }

        // Wait for Phase 3 (Commit) or a backup's final decision.
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

    /// Skeen Section 5 termination protocol. Reentrant: on every
    /// timeout the elected backup index is advanced; if it reaches
    /// our own index we run the backup protocol ourselves.
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
        // Election exhausted. Per the paper this can only happen if
        // every slave (including us) is presumed dead — but we are
        // still running. The decision rule applies to our state.
        let d = self.decision_rule();
        self.state = match d {
            Decision::Commit => PState::Committed,
            Decision::Abort => PState::Aborted,
        };
        if d == Decision::Commit {
            traceforge::assert(self.voted_yes);
        }
        d
    }

    /// Wait for the slave at `current_idx` (acting as backup) to send
    /// us either a `MoveTo` (Phase 1) or a final `Commit`/`Abort`
    /// (Phase 2). On timeout, treat the backup as dead.
    fn wait_for_backup(&mut self, current_idx: u32) -> BackupOutcome {
        let current_peer = self.peer_ids[current_idx as usize];
        loop {
            match self.recv() {
                Some(PMsg::MoveTo { backup, state }) => {
                    if backup == current_peer {
                        self.handle_move_to(backup, state);
                        // Stay in the loop, waiting for the decision.
                    }
                    // MoveTo from a different backup (higher or lower
                    // ranked) — drop it. Strict preassigned ranking
                    // means we only obey the index we're currently
                    // waiting on.
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

    /// Run as backup coordinator. Strictly the two-phase protocol of
    /// Skeen Section 5: Phase 1 (move-to-state, omitted if I'm already
    /// in c or a) + Phase 2 (decision broadcast).
    fn run_backup_protocol(&mut self) -> Decision {
        // Decision is fixed at the moment we become backup, based on
        // our local state. Crucially, this is the *same* rule any
        // future backup would apply once Phase 1 has synchronised
        // everyone — that is what makes the protocol reentrant.
        let decision = self.decision_rule();

        let need_phase1 = self.state != PState::Committed
            && self.state != PState::Aborted;

        if need_phase1 {
            let my_state = self.state;
            let me = self.me();

            // Phase 1 broadcast.
            for (j, p) in self.peer_ids.iter().enumerate() {
                if j as u32 != self.my_index {
                    if maybe_crash(self.crashes) { return decision; }
                    traceforge::send_msg(*p, PMsg::MoveTo { backup: me, state: my_state });
                }
            }

            // Phase 1: wait for acks. Per the paper backup "waits for
            // an acknowledgment from each site"; we use a finite wait
            // — missing acks ⇒ that slave is presumed failed, but the
            // decision rule still applies (a future backup will reach
            // the same decision from its own state).
            let needed = self.peer_ids.len() - 1;
            for _ in 0..needed {
                if maybe_crash(self.crashes) { return decision; }
                if self.mode == Mode::Temporal {
                    traceforge::sleep(self.b.delta);
                }
                match self.recv() {
                    Some(PMsg::BackupAck { .. }) => {}
                    Some(PMsg::Commit) => {
                        // Another backup beat us. Honor it.
                        self.state = PState::Committed;
                        traceforge::assert(self.voted_yes);
                        return Decision::Commit;
                    }
                    Some(PMsg::Abort) => {
                        self.state = PState::Aborted;
                        return Decision::Abort;
                    }
                    Some(PMsg::MoveTo { backup, state }) => {
                        // A concurrent backup is also running and
                        // outranks us in some slave's view. We treat
                        // it as background noise and continue our run;
                        // the model checker explores all interleavings.
                        self.handle_move_to(backup, state);
                    }
                    Some(_) => {}
                    None => break, // give up Phase-1 wait; proceed.
                }
            }
        }

        if maybe_crash(self.crashes) { return decision; }

        // Phase 2: broadcast decision.
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

        // Apply locally.
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

fn slave(mode: Mode, b: Bounds, num_ps: u32, index: u32, crashes: bool) {
    // Crash before even waiting for Prepare — captures a slave that
    // never comes up for this round.
    if maybe_crash(crashes) { return; }

    // Wait for Prepare (round bootstrap). Drop strays from the model
    // checker's speculative pairings.
    let (coord_id, peers) = loop {
        match traceforge::recv_msg_block::<PMsg>() {
            PMsg::Prepare { coord, peers } => break (coord, peers),
            _ => {}
        }
    };

    let mut ctx = SlaveCtx {
        mode, b, my_index: index, num_ps, coord_id,
        peer_ids: peers, state: PState::Initial, voted_yes: false, crashes,
    };
    let _ = ctx.run_after_prepare();
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
        // Each round is a fully independent invocation of the protocol
        // on fresh threads — the "clean slate" semantics the paper
        // implicitly assumes (Skeen analyses a single transaction).
        for _r in 0..rounds {
            let mut handles = Vec::new();
            for i in 0..num_ps {
                handles.push(thread::spawn(move || slave(mode, b, num_ps, i, crashes)));
            }
            let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
            let c = thread::spawn(move || coordinator(mode, b, crashes));
            traceforge::send_msg(c.thread().id(), CMsg::Init { peers: peer_ids });

            // Drain this round before starting the next so each round
            // is independent. Failing threads return immediately, so
            // join is sufficient.
            for h in handles { let _ = h.join(); }
            let _ = c.join();
        }
    });
    (stats, start.elapsed())
}

// =====================================================================
// Reporting
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
