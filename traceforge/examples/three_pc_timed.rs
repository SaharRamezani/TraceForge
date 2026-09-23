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

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use traceforge::monitor_types::*;
use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};
use traceforge_macros::monitor;
use std::sync::atomic::{AtomicUsize, Ordering};

// Commit-outcome counters (analog of the leader-election count for commit protocols):
// incremented once per coordinator decision; reset before each verify, read after.
static COMMITS: AtomicUsize = AtomicUsize::new(0);
static ABORTS: AtomicUsize = AtomicUsize::new(0);

// Participant-side decisions, summed over all explored executions. They
// are the monitor's antecedent: `commit-decisions=0` would mean the
// atomicity clause held vacuously, so every run reports them.
static PARTICIPANT_COMMITS: AtomicUsize = AtomicUsize::new(0);
static PARTICIPANT_ABORTS: AtomicUsize = AtomicUsize::new(0);

// Acceptance switches. These MUST be process-level statics, not fields:
// traceforge-macros generates the acceptor as a plain `fn` that tests
// each message against a freshly `Default`-constructed monitor (see
// traceforge-macros/src/lib.rs, `let mut mon = #aname::default()`), so
// an `accept` that reads `self` state or a constructor flag silently
// has no effect on what the checker records.
static VETO_ONLY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static OBSERVE_NOTHING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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

// =====================================================================
// Atomicity monitor
// =====================================================================
//
// The property this benchmark checks. It replaces the participant-local
// `assert(voted_yes)` that used to sit on the Commit branch, which could
// never fail: a participant that votes No does `continue` immediately
// after sending its vote, so the Commit branch was reachable only with
// `voted_yes == true` and the assertion was true by construction.
//
// What is actually worth checking here is the claim the round design
// makes (see the header): round-filtering every receive keeps the
// protocol correct even when the checker pairs a send from one round
// with a receive from another. Written as a property of the messages:
//
//   A1 (atomicity / validity): the coordinator sends Commit for round r
//      only if every participant sent Yes for round r. A commit decided
//      on a stale vote, a missing vote or a No violates it.
//   A2 (uniform agreement): no round has both a Commit and an Abort.
//
// A monitor is the right home for both: they quantify over all
// participants, so no single thread can check them locally, and the
// monitor observes messages without adding any to the protocol.
//
// Why A1 cannot raise a false alarm: a monitor's receives "act as if
// they are CausalOrder" (cons.rs, note 3 at the top of the file), and
// every vote the coordinator counted causally precedes the Commit it
// then sent. So whenever the monitor observes Commit(r) it has already
// observed every vote that decision rests on, and a missing Yes really
// is a missing vote rather than one still in flight.
//
// Neither clause is true by construction. `--inject quorum` replaces
// the coordinator's unanimity rule with a majority quorum for votes and
// acks, the classic commit-protocol mis-design, and the monitor then
// reports a genuine atomicity violation (a round commits while a
// participant voted No), so it is demonstrably load-bearing.
//
// Note which guard actually carries atomicity here, since it is not the
// one the header advertises: dropping the round filter on votes alone
// can never produce a bad commit, because a No voter never acks and the
// coordinator waits for an ack from every participant. Unanimity in
// BOTH phases is the invariant; round filtering only keeps rounds from
// interfering.
#[monitor(PMsg, CMsg)]
#[derive(Clone, Debug, Default)]
struct Atomicity {
    /// Participants that sent Yes / No in a round, by round.
    yes: HashMap<u32, HashSet<ThreadId>>,
    no: HashMap<u32, HashSet<ThreadId>>,
    /// Rounds the coordinator has decided, by decision.
    committed: HashSet<u32>,
    aborted: HashSet<u32>,
    num_ps: usize,
    /// Diagnostic: accept nothing at all, to separate the cost of a
    /// monitor's OBSERVATIONS from the cost of its mere presence.
    observe_nothing: bool,
    /// Veto-only mode: observe No votes and decisions, not Yes votes.
    /// A1 then reads "no round commits after a No", dropping the
    /// "every participant voted" half. Much cheaper, because the Yes
    /// votes of N distinct senders are what the checker has to
    /// interleave; see the measurements in the report.
    veto_only: bool,
}

impl Atomicity {
    fn new(num_ps: u32, veto_only: bool, observe_nothing: bool) -> Self {
        Self {
            num_ps: num_ps as usize,
            veto_only,
            observe_nothing,
            ..Default::default()
        }
    }
}

impl Monitor for Atomicity {}

impl Acceptor<CMsg> for Atomicity {
    fn accept(&mut self, _who: ThreadId, _whom: ThreadId, what: &CMsg) -> bool {
        // Votes only: Init carries no decision information and Ack is
        // not part of either clause (rejecting here is cheaper than
        // receiving and ignoring, see the spawn_monitor docs).
        if OBSERVE_NOTHING.load(Ordering::Relaxed) {
            false
        } else if VETO_ONLY.load(Ordering::Relaxed) {
            matches!(what, CMsg::No { .. })
        } else {
            matches!(what, CMsg::Yes { .. } | CMsg::No { .. })
        }
    }
}

impl Observer<CMsg> for Atomicity {
    fn notify(&mut self, who: ThreadId, _whom: ThreadId, what: &CMsg) -> MonitorResult {
        match what {
            CMsg::Yes { round } => {
                self.yes.entry(*round).or_default().insert(who);
            }
            CMsg::No { round } => {
                self.no.entry(*round).or_default().insert(who);
            }
            _ => {}
        }
        Ok(())
    }
}

impl Acceptor<PMsg> for Atomicity {
    fn accept(&mut self, _who: ThreadId, _whom: ThreadId, what: &PMsg) -> bool {
        // The coordinator sends its decision to every participant, but
        // both clauses are about the decision, not its recipients, so
        // one observation per round is enough. Rejecting the other
        // N - 1 copies keeps them out of the monitor's read set
        // entirely, which is where the cost of a monitor lives (every
        // observed send becomes a read event the checker interleaves).
        if OBSERVE_NOTHING.load(Ordering::Relaxed) {
            return false;
        }
        // Content-only: a stateful "one decision per round" filter is
        // impossible here for the reason given at VETO_ONLY.
        matches!(what, PMsg::Commit { .. } | PMsg::Abort { .. })
    }
}

impl Observer<PMsg> for Atomicity {
    fn notify(&mut self, _who: ThreadId, _whom: ThreadId, what: &PMsg) -> MonitorResult {
        match what {
            PMsg::Commit { round } => {
                // A1: every participant must have voted Yes in THIS round.
                let yes = self.yes.get(round).map_or(0, |s| s.len());
                if let Some(no) = self.no.get(round) {
                    if !no.is_empty() {
                        return Err(format!(
                            "atomicity: Commit in round {round} after {} No vote(s)",
                            no.len()
                        ));
                    }
                }
                if !VETO_ONLY.load(Ordering::Relaxed) && yes != self.num_ps {
                    return Err(format!(
                        "atomicity: Commit in round {round} backed by only {yes} of {} \
                         Yes votes for that round",
                        self.num_ps
                    ));
                }
                // A2: no round decides both ways.
                if self.aborted.contains(round) {
                    return Err(format!("agreement: round {round} both aborts and commits"));
                }
                self.committed.insert(*round);
            }
            PMsg::Abort { round } => {
                if self.committed.contains(round) {
                    return Err(format!("agreement: round {round} both commits and aborts"));
                }
                self.aborted.insert(*round);
            }
            _ => {}
        }
        Ok(())
    }
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

fn coordinator(b: Bounds, crashes: bool, rounds: u32, inject: bool) {
    let ps: Vec<ThreadId> = loop {
        match traceforge::recv_msg_block_timed::<CMsg>() {
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

        // Unanimity is what makes 3PC atomic: one No aborts. `inject`
        // replaces it with a majority quorum (the classic mis-design,
        // applied to votes and acks alike), which is exactly what the
        // atomicity monitor must catch. Off by default, and identical
        // in both modes when on.
        let enough_votes = if inject { ps.len() / 2 + 1 } else { ps.len() };
        if received < enough_votes || yes < enough_votes {
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

        let enough_acks = if inject { ps.len() / 2 + 1 } else { ps.len() };
        if acks < enough_acks {
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
            // The commit decision itself. The old `assert(voted_yes)`
            // here was vacuous (a No voter `continue`s above and never
            // reaches this point); atomicity is checked by the monitor,
            // which can see every participant's vote. Counting the
            // decisions keeps that check honest: a run where no
            // participant ever commits would satisfy the monitor
            // vacuously, so the counter is reported alongside it.
            Some(true) => { PARTICIPANT_COMMITS.fetch_add(1, Ordering::Relaxed); }
            Some(false) => { PARTICIPANT_ABORTS.fetch_add(1, Ordering::Relaxed); }
            None => { dead = true; continue; }
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

/// Exploration strategy chosen on the command line (`--parallel`):
/// `none` (single-threaded, the default), `shared` (the shared work-queue
/// pool, count-identical to sequential exploration; pool size follows
/// MUST_PARALLEL_WORKERS) or `partitioned`.
static PARALLEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn apply_parallel(builder: traceforge::ConfigBuilder) -> traceforge::ConfigBuilder {
    match PARALLEL.get().map(|s| s.as_str()).unwrap_or("none") {
        "none" => builder,
        "shared" => builder.with_parallel(true),
        "partitioned" => builder.with_partitioned_parallelization(true),
        other => panic!("invalid --parallel: {other} (expected none|shared|partitioned)"),
    }
}

fn build_config(mode: Mode, b: Bounds) -> Config {
    let builder = apply_parallel(Config::builder().with_progress_report(usize::MAX));
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
    }
}

fn run(mode: Mode, num_ps: u32, b: Bounds, crashes: bool, rounds: u32, inject: bool, veto_only: bool, observe_nothing: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, b);
    COMMITS.store(0, Ordering::Relaxed);
    ABORTS.store(0, Ordering::Relaxed);
    PARTICIPANT_COMMITS.store(0, Ordering::Relaxed);
    PARTICIPANT_ABORTS.store(0, Ordering::Relaxed);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        // The atomicity monitor observes votes and decisions. It is
        // spawned first so it is registered before any protocol message
        // exists, and it is part of the program in BOTH modes.
        let mon = start_monitor_atomicity(Atomicity::new(num_ps, veto_only, observe_nothing));
        let mut handles = Vec::new();
        for _ in 0..num_ps {
            handles.push(thread::spawn(move || participant(b, crashes, rounds)));
        }
        let peer_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        let c = thread::spawn(move || coordinator(b, crashes, rounds, inject));
        traceforge::send_msg(c.thread().id(), CMsg::Init { peers: peer_ids });
        for h in handles { let _ = h.join(); }
        let _ = c.join();
        terminate_monitor_atomicity(mon.thread().id());
        let verdict: MonitorResult = mon.join().unwrap();
        assert!(verdict.is_ok(), "{}", verdict.unwrap_err());
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
         blocked={block:<6} commit={commits:<6} abort={aborts:<6} pcommit={pc:<6} time={dur:?}",
        pc = PARTICIPANT_COMMITS.load(Ordering::Relaxed),
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
    println!(
        "participant decisions (monitor antecedent): commit {} abort {}",
        PARTICIPANT_COMMITS.load(Ordering::Relaxed),
        PARTICIPANT_ABORTS.load(Ordering::Relaxed)
    );
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

fn parse_args() -> (String, u32, u64, u64, u32, bool, f64, f64, String, bool, bool, bool) {
    let mut mode = String::from("compare");
    let mut inject = false;
    let mut veto_only = false;
    let mut observe_nothing = false;
    let mut parallel = String::from("none");
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
            "--property" => {
                let what = args.next().expect("--property value");
                veto_only = match what.as_str() {
                    "full" => false,
                    "veto-only" => true,
                    "none" => { observe_nothing = true; false }
                    other => panic!("invalid --property: {other} (expected full|veto-only)"),
                };
            }
            "--inject" => {
                let what = args.next().expect("--inject value");
                assert_eq!(what, "quorum", "unknown --inject value: {what} (expected quorum)");
                inject = true;
            }
            "--parallel" => parallel = args.next().expect("--parallel value"),
            "--help" | "-h" => {
                eprintln!("Usage: three_pc_timed [--mode MODE] [--participants N] [--u U] [--w-ratio R] [--rounds R] [--l-ratio LR] [--sd-ratio SR] [--crashes] [--inject quorum] [--property full|veto-only|none] [--parallel none|shared|partitioned]\n\
                          Modes: baseline | timed | compare | sweep | n-sweep\n\
                          Defaults: U=1, W/U=2 (Skeen), N=3, R=1, L/U=0, sd/U=0.");
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, num_ps, u, w_ratio, rounds, crashes, l_ratio, sd_ratio, parallel, inject, veto_only, observe_nothing)
}

fn main() {
    let (mode_str, num_ps, u, w_ratio, rounds, crashes, l_ratio, sd_ratio, parallel, inject, veto_only, observe_nothing) = parse_args();
    PARALLEL.set(parallel).expect("PARALLEL set once");
    VETO_ONLY.store(veto_only, Ordering::Relaxed);
    OBSERVE_NOTHING.store(observe_nothing, Ordering::Relaxed);
    assert!(num_ps >= 1);
    assert!(rounds >= 1);
    // L and sd are derived from dimensionless ratios over U (network-parameters §2).
    let l = (l_ratio * u as f64).round() as u64;
    let sd = (sd_ratio * u as f64).round() as u64;
    match mode_str.as_str() {
        "baseline" => {
            let b = Bounds::from_ratio(u, w_ratio, l, sd);
            let (s, d) = run(Mode::Baseline, num_ps, b, crashes, rounds, inject, veto_only, observe_nothing);
            print_one("baseline", num_ps, b, rounds, &s, d, crashes,
                      COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
        }
        "timed" => {
            let b = Bounds::from_ratio(u, w_ratio, l, sd);
            let (s, d) = run(Mode::Timed, num_ps, b, crashes, rounds, inject, veto_only, observe_nothing);
            print_one("timed", num_ps, b, rounds, &s, d, crashes,
                      COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
        }
        "compare" => {
            let b = Bounds::from_ratio(u, w_ratio, l, sd);
            let baseline = run(Mode::Baseline, num_ps, b, crashes, rounds, inject, veto_only, observe_nothing);
            let (bc, ba) = (COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
            let timed = run(Mode::Timed, num_ps, b, crashes, rounds, inject, veto_only, observe_nothing);
            let (tc, ta) = (COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
            print_compare(num_ps, b, rounds, crashes, baseline, timed, bc, ba, tc, ta);
        }
        "sweep" => {
            let mut rows = Vec::new();
            for &r in SWEEP_RATIOS {
                let b = Bounds::from_ratio(u, r, l, sd);
                let baseline = run(Mode::Baseline, num_ps, b, crashes, rounds, inject, veto_only, observe_nothing);
                let timed = run(Mode::Timed, num_ps, b, crashes, rounds, inject, veto_only, observe_nothing);
                rows.push((r, b, baseline.0, baseline.1, timed.0, timed.1));
            }
            print_sweep(num_ps, u, rounds, crashes, &rows);
        }
        "n-sweep" => {
            let mut rows = Vec::new();
            for &n in SWEEP_PARTICIPANTS {
                let b = Bounds::from_ratio(u, w_ratio, l, sd);
                let baseline = run(Mode::Baseline, n, b, crashes, rounds, inject, veto_only, observe_nothing);
                let timed = run(Mode::Timed, n, b, crashes, rounds, inject, veto_only, observe_nothing);
                rows.push((n, b, baseline.0, baseline.1, timed.0, timed.1));
            }
            print_n_sweep(u, w_ratio, rounds, crashes, &rows);
        }
        other => panic!("invalid --mode: {other}"),
    }
}
