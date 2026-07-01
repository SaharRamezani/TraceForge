//! Two-Phase Commit (2PC)
//!
//! Two run modes share the same protocol skeleton:
//!
//!   * `--mode baseline`
//!     Plain Must verification, no `with_timed`. The state space is
//!     `2^N * N!`:
//!       - 2^N from each participant's nondet vote (Yes / No), and
//!       - N! from interleaving the N concurrent vote arrivals at the
//!         coordinator (each (participant_i -> coordinator) is its own
//!         FIFO channel, so cross-channel orderings are free).
//!
//!   * `--mode timed`
//!     Must-τ verification with `with_timed(0, 1, 0)`. Each
//!     participant `i` sleeps `(i+1)*delta` *before* sending its vote,
//!     and the coordinator sleeps `delta` *before each* successive
//!     `recv_msg_block_timed`.
//!
//!     With `delta >= 2`, every cross-mapping "recv k reads from
//!     participant j (j != k)" has an empty feasible-time interval:
//!         - For j > k: the recv pred clock is `(k+1)*delta`, the
//!           send window is `[(j+1)*delta, (j+1)*delta + 1]`, the
//!           rf-window upper bound `(j+1)*delta + 1` is fine but the
//!           combinatorial *which-vote-still-unread* constraint has to
//!           hold for every later recv too, and only the canonical
//!           `j == k` choice satisfies all of them simultaneously.
//!         - For j < k: rf-window lo `(k+1)*delta` exceeds hi
//!           `(j+1)*delta + 1` whenever `delta >= 2`, so the interval
//!           is empty and the rf candidate is dropped by `timed_consistent`.
//!     Net effect: timed pruning collapses the N! factor to 1, and
//!     `recv_msg_block_timed` (W_r = +∞) drops the timeout branches by
//!     construction. Total exploration shrinks from `2^N * N!` to `2^N`.
//!
//! Usage:
//!
//!     cargo run --release --example two_pc_timed -- --mode compare --participants 4 --delta 5
//!
//!     cargo run --release --example two_pc_timed -- --mode baseline --participants 5
//!
//!     cargo run --release --example two_pc_timed -- --mode timed --participants 5 --delta 5

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

// Commit-outcome counters (analog of the leader-election count for commit protocols):
// incremented once per coordinator decision; reset before each verify, read after.
static COMMITS: AtomicUsize = AtomicUsize::new(0);
static ABORTS: AtomicUsize = AtomicUsize::new(0);

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats};

const DEFAULT_PARTICIPANTS: u32 = 4;
const DEFAULT_DELTA: u64 = 5;

#[derive(Clone, Debug, PartialEq)]
enum CoordinatorMsg {
    Init(Vec<ThreadId>),
    Yes,
    No,
}

#[derive(Clone, Debug, PartialEq)]
enum ParticipantMsg {
    Prepare(ThreadId),
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

fn coordinator(mode: Mode, delta: u64) {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CoordinatorMsg>() {
        CoordinatorMsg::Init(ids) => ids,
        _ => panic!("expected Init"),
    };
    let me = thread::current().id();

    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Prepare(me));
    }

    let mut yes_count = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Timed {
            // Stagger the recv windows so only one participant's vote is
            // timed consistent with each successive recv.
            traceforge::sleep(delta);
        }
        let v: CoordinatorMsg = match mode {
            Mode::Baseline => traceforge::recv_msg_block(),
            Mode::Timed => traceforge::recv_msg_block_timed(),
        };
        match v {
            CoordinatorMsg::Yes => yes_count += 1,
            CoordinatorMsg::No => (),
            _ => panic!("expected vote"),
        }
    }

    let decision = if yes_count == ps.len() {
        COMMITS.fetch_add(1, Ordering::Relaxed);
        ParticipantMsg::Commit
    } else {
        ABORTS.fetch_add(1, Ordering::Relaxed);
        ParticipantMsg::Abort
    };
    for id in &ps {
        traceforge::send_msg(*id, decision.clone());
    }
}

fn participant(mode: Mode, delta: u64, index: u32) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };

    if mode == Mode::Timed {
        // Participant `i` sends its vote at local time ~(i+1)*delta. The
        // coordinator's recv k expects the vote at clock (k+1)*delta, so
        // only the canonical i == k pairing has a non-empty rf-window.
        traceforge::sleep(delta * (index as u64 + 1));
    }

    let yes = traceforge::nondet();
    let vote = if yes {
        CoordinatorMsg::Yes
    } else {
        CoordinatorMsg::No
    };
    traceforge::send_msg(cid, vote);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    match action {
        ParticipantMsg::Commit => assert!(yes),
        ParticipantMsg::Abort => (),
        _ => panic!("expected decision"),
    }
}

fn build_config(mode: Mode) -> Config {
    match mode {
        Mode::Baseline => Config::builder().build(),
        // (L=0, U=1, sd=0): tightest non-trivial transit window.
        // Combined with delta >= 2 in the protocol code, this forces a
        // unique rf-mapping for the coordinator's vote-collection.
        Mode::Timed => Config::builder().with_timed(0, 1, 0).build(),
    }
}

fn run(mode: Mode, num_ps: u32, delta: u64) -> (Stats, Duration) {
    let cfg = build_config(mode);
    COMMITS.store(0, Ordering::Relaxed);
    ABORTS.store(0, Ordering::Relaxed);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let c = thread::spawn(move || coordinator(mode, delta));

        let mut ps = Vec::new();
        for i in 0..num_ps {
            ps.push(thread::spawn(move || participant(mode, delta, i)));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    });
    (stats, start.elapsed())
}

fn factorial(n: u32) -> u64 {
    (1..=n as u64).product()
}

fn print_one(label: &str, num_ps: u32, stats: &Stats, dur: Duration, commits: usize, aborts: usize) {
    println!(
        "{label:<10} participants={num_ps}  execs={execs:<8} blocked={block:<6} commit={commits:<6} abort={aborts:<6} time={dur:?}",
        execs = stats.execs,
        block = stats.block,
        dur = dur,
    );
}

fn print_compare(num_ps: u32, baseline: (Stats, Duration), timed: (Stats, Duration),
                 b_commits: usize, b_aborts: usize, t_commits: usize, t_aborts: usize) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;

    println!();
    println!("Two-Phase Commit: Must vs Must-τ");
    println!("=================================");
    println!("participants = {num_ps}");
    println!(
        "expected baseline execs = 2^N * N! = {} * {} = {}",
        1u64 << num_ps,
        factorial(num_ps),
        (1u64 << num_ps) * factorial(num_ps),
    );
    println!("expected timed execs = 2^N        = {}", 1u64 << num_ps);
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
        "timed", t_stats.execs, t_stats.block, t_dur
    );
    println!();
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("execs reduction: {exec_ratio:.2}x  (= baseline / timed)");
    println!("time  speedup  : {time_ratio:.2}x");
    println!("commit/abort (complete execs): baseline {b_commits}/{b_aborts}   timed {t_commits}/{t_aborts}");
}

fn parse_args() -> (String, u32, u64) {
    let mut mode = String::from("compare");
    let mut num_ps = DEFAULT_PARTICIPANTS;
    let mut delta = DEFAULT_DELTA;
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
            "--help" | "-h" => {
                eprintln!(
                    "Usage: two_pc_timed [--mode baseline|timed|compare] \
                     [--participants N] [--delta D]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, num_ps, delta)
}

fn main() {
    let (mode_str, num_ps, delta) = parse_args();
    assert!(num_ps >= 1, "need at least 1 participant");
    assert!(
        delta >= 2,
        "delta must be >= 2 to make cross-mappings infeasible"
    );

    match mode_str.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, num_ps, delta);
            print_one("baseline", num_ps, &s, d,
                      COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, num_ps, delta);
            print_one("timed", num_ps, &s, d,
                      COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
        }
        "compare" => {
            let baseline = run(Mode::Baseline, num_ps, delta);
            let (bc, ba) = (COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
            let timed = run(Mode::Timed, num_ps, delta);
            let (tc, ta) = (COMMITS.load(Ordering::Relaxed), ABORTS.load(Ordering::Relaxed));
            print_compare(num_ps, baseline, timed, bc, ba, tc, ta);
        }
        other => panic!("invalid --mode: {other} (expected baseline|timed|compare)"),
    }
}
