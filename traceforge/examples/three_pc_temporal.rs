//! Three-Phase Commit (3PC) with timeouts and optional coordinator /
//! participant crashes — mirrors the structure of the original 3PC
//! reference (see `Rounds`-based version) but uses the simpler
//! `traceforge::send_msg` / `recv_msg_timed` API for parity with the 2PC
//! examples.
//!
//! 3PC adds a `PreCommit` round between the `Prepare/Vote` and the final
//! `Commit/Abort`. Compared to 2PC, the coordinator collects two waves
//! of N concurrent receives (votes, then acks).
//!
//! Receives that may legitimately fail to deliver (because the
//! coordinator or a participant crashed, or because a message simply
//! arrived too late under the temporal model) use
//! `recv_msg_timed(WaitTime::Finite(W))` and treat `None` as a timeout.
//! That matches the original protocol's resilience design: a missing
//! vote → abort the round; a missing ack → don't commit; a missing
//! PreCommit/Abort → participant gives up; a missing Commit →
//! participant times out.
//!
//! Two run modes share the same protocol skeleton:
//!
//!   * `--mode baseline`
//!     Plain Must verification, no `with_temporal`. Receives still admit
//!     a timeout branch in the temporal sense, but baseline mode uses
//!     plain `recv_msg_block` (no temporal pruning, no Option). The
//!     state space scales with the vote/ack interleavings × 2^N
//!     vote configs × any crash branches.
//!
//!   * `--mode temporal`
//!     Must-τ verification with `with_temporal(0, 1, 0)`. Each
//!     participant `i` sleeps `(i+1)*delta` before sending its vote and
//!     `N*delta` after PreCommit before sending its ack. The
//!     coordinator sleeps `delta` before each successive vote / ack
//!     recv. Cross-mappings get pruned by the empty-interval rule, so
//!     only the canonical pairing for each surviving recv survives.
//!     The ⊥ branch (timeout) is *also* admissible (its singleton
//!     interval is non-empty), so each recv branches into "canonical
//!     send" + "timeout."
//!
//! Usage:
//!
//!     cargo run --release --example three_pc_temporal -- --mode compare --participants 3 --delta 5
//!     cargo run --release --example three_pc_temporal -- --mode temporal --participants 3 --delta 5
//!     cargo run --release --example three_pc_temporal -- --mode temporal --participants 3 --delta 5 --crashes

use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DEFAULT_PARTICIPANTS: u32 = 3;
const DEFAULT_DELTA: u64 = 5;

#[derive(Clone, Debug, PartialEq)]
enum CoordinatorMsg {
    Init(Vec<ThreadId>),
    Yes,
    No,
    Ack,
}

#[derive(Clone, Debug, PartialEq)]
enum ParticipantMsg {
    Prepare(ThreadId),
    PreCommit,
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Temporal,
}

/// Wait window for the coord's vote/ack receives. Must be < delta so
/// cross-mappings (j != k) get pruned by the empty-interval rule.
fn vote_wait(delta: u64) -> u64 {
    delta - 1
}

// Note: participant receives are deliberately *untimed* (`recv_msg_block`)
// rather than `recv_msg_block_timed`. The block_timed variant advances
// the participant's local clock to the actual rf-window time on
// completion, which converges all participants to the same broadcast-
// arrival τ and erases the (i+1)*delta staggering — the very staggering
// that lets the temporal filter prune cross-mapped acks. With the
// untimed variant the clock stays put and the canonical ack pairing
// fits the coordinator's tight wait window.
//
// "Timeout" on the participant side under `--crashes` is modelled the
// same way the original 3PC reference does it: the participant
// `nondet()`-returns before the recv, never reaching it. That captures
// the resilience semantics (a missing message → participant gives up)
// without breaking the temporal staggering.

fn coordinator(mode: Mode, delta: u64, num_ps: usize, crashes: bool) {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CoordinatorMsg>() {
        CoordinatorMsg::Init(ids) => ids,
        _ => panic!("expected Init"),
    };
    let me = thread::current().id();

    // Phase 1: CanCommit -> Vote
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Prepare(me));
    }

    let mut yes_count = 0usize;
    let mut received_count = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(delta);
        }
        let v: Option<CoordinatorMsg> = match mode {
            Mode::Baseline => Some(traceforge::recv_msg_block()),
            Mode::Temporal => traceforge::recv_msg_timed(WaitTime::Finite(vote_wait(delta))),
        };
        match v {
            Some(CoordinatorMsg::Yes) => {
                received_count += 1;
                yes_count += 1;
            }
            Some(CoordinatorMsg::No) => {
                received_count += 1;
            }
            // Timeout: participant didn't deliver in time. Treat as a
            // missing vote → not all yes → will abort below.
            None => {}
            Some(_) => panic!("expected vote"),
        }
    }

    let _ = num_ps; // silence unused if cargo gets pedantic
    let unanimous = received_count == ps.len() && yes_count == ps.len();

    if !unanimous {
        for id in &ps {
            traceforge::send_msg(*id, ParticipantMsg::Abort);
        }
        return;
    }

    // Coordinator may crash here, before broadcasting PreCommit. Mirrors
    // the original 3PC's `crashes_enabled && nondet() return` branch.
    if crashes && traceforge::nondet() {
        return;
    }

    // Phase 2: PreCommit -> Ack
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::PreCommit);
    }

    let mut acks_received = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(delta);
        }
        let v: Option<CoordinatorMsg> = match mode {
            Mode::Baseline => Some(traceforge::recv_msg_block()),
            Mode::Temporal => traceforge::recv_msg_timed(WaitTime::Finite(vote_wait(delta))),
        };
        match v {
            Some(CoordinatorMsg::Ack) => acks_received += 1,
            None => {} // timeout — leaves the protocol uncommitted
            Some(_) => panic!("expected ack"),
        }
    }

    if acks_received != ps.len() {
        // Missing ack(s): cannot safely commit this round. Don't send
        // Commit. Participants that already moved past PreCommit will
        // time out waiting for Commit and exit cleanly.
        return;
    }

    // Phase 3: DoCommit
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Commit);
    }
}

fn participant(mode: Mode, delta: u64, num_ps: u32, index: u32, crashes: bool) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };

    // Participant may crash before voting.
    if crashes && traceforge::nondet() {
        return;
    }

    if mode == Mode::Temporal {
        traceforge::sleep(delta * (index as u64 + 1));
    }

    let yes = traceforge::nondet();
    let vote = if yes {
        CoordinatorMsg::Yes
    } else {
        CoordinatorMsg::No
    };
    traceforge::send_msg(cid, vote);

    // Untimed blocking recv (see comment near `vote_wait`). If the
    // coord crashes, this thread will simply block forever; the model
    // checker captures the resulting graph as a blocked execution.
    // Crash-driven "timeouts" are simulated above via the
    // `crashes && nondet() return` short-circuit that runs *before*
    // this recv even starts.
    let action: ParticipantMsg = traceforge::recv_msg_block();

    match action {
        ParticipantMsg::Abort => return,
        ParticipantMsg::PreCommit => (),
        _ => panic!("expected PreCommit or Abort"),
    }

    // Participant may crash before sending Ack.
    if crashes && traceforge::nondet() {
        return;
    }

    if mode == Mode::Temporal {
        traceforge::sleep(delta * num_ps as u64);
    }
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    // Untimed blocking recv. If the coord crashes after collecting acks
    // but before broadcasting Commit, this blocks; the model checker
    // captures it as a blocked execution.
    let action: ParticipantMsg = traceforge::recv_msg_block();
    match action {
        ParticipantMsg::Commit => assert!(yes),
        _ => panic!("expected Commit"),
    }
}

fn build_config(mode: Mode) -> Config {
    match mode {
        Mode::Baseline => Config::builder().build(),
        Mode::Temporal => Config::builder().with_temporal(0, 1, 0).build(),
    }
}

fn run(mode: Mode, num_ps: u32, delta: u64, crashes: bool) -> (Stats, Duration) {
    let cfg = build_config(mode);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let c = thread::spawn(move || coordinator(mode, delta, num_ps as usize, crashes));

        let mut ps = Vec::new();
        for i in 0..num_ps {
            ps.push(thread::spawn(move || {
                participant(mode, delta, num_ps, i, crashes)
            }));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    });
    (stats, start.elapsed())
}

fn print_one(label: &str, num_ps: u32, stats: &Stats, dur: Duration, crashes: bool) {
    let crash_tag = if crashes { " (crashes)" } else { "" };
    println!(
        "{label:<10}{crash_tag} participants={num_ps}  execs={execs:<6} blocked={block:<6} time={dur:?}",
        execs = stats.execs,
        block = stats.block,
        dur = dur,
    );
}

fn print_compare(
    num_ps: u32,
    crashes: bool,
    baseline: (Stats, Duration),
    temporal: (Stats, Duration),
) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = temporal;

    println!();
    println!(
        "Three-Phase Commit (with timeouts{}): Must vs Must-τ",
        if crashes { " + crashes" } else { "" }
    );
    println!("======================================================");
    println!("participants = {num_ps}");
    println!();
    println!(
        "{:<10} {:>10} {:>10} {:>14}",
        "mode", "execs", "blocked", "time"
    );
    println!(
        "{:<10} {:>10} {:>10} {:>14?}",
        "baseline", b_stats.execs, b_stats.block, b_dur
    );
    println!(
        "{:<10} {:>10} {:>10} {:>14?}",
        "temporal", t_stats.execs, t_stats.block, t_dur
    );
    println!();
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("execs reduction: {exec_ratio:.2}x  (= baseline / temporal)");
    println!("time  speedup  : {time_ratio:.2}x");
}

fn parse_args() -> (String, u32, u64, bool) {
    let mut mode = String::from("compare");
    let mut num_ps = DEFAULT_PARTICIPANTS;
    let mut delta = DEFAULT_DELTA;
    let mut crashes = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = args
                    .next()
                    .unwrap_or_else(|| panic!("--mode requires a value"));
            }
            "--participants" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--participants requires a value"));
                num_ps = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --participants: {v}"));
            }
            "--delta" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--delta requires a value"));
                delta = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --delta: {v}"));
            }
            "--crashes" => crashes = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: three_pc_temporal [--mode baseline|temporal|compare] \
                     [--participants N] [--delta D] [--crashes]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, num_ps, delta, crashes)
}

fn main() {
    let (mode_str, num_ps, delta, crashes) = parse_args();
    assert!(num_ps >= 1, "need at least 1 participant");
    assert!(
        delta >= 2,
        "delta must be >= 2 to make cross-mappings infeasible"
    );

    match mode_str.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, num_ps, delta, crashes);
            print_one("baseline", num_ps, &s, d, crashes);
        }
        "temporal" => {
            let (s, d) = run(Mode::Temporal, num_ps, delta, crashes);
            print_one("temporal", num_ps, &s, d, crashes);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, num_ps, delta, crashes);
            let temporal = run(Mode::Temporal, num_ps, delta, crashes);
            print_compare(num_ps, crashes, baseline, temporal);
        }
        other => panic!("invalid --mode: {other} (expected baseline|temporal|compare)"),
    }
}
