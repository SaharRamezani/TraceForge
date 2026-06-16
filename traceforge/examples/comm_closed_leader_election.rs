//! Communication-closed leader election
//!
//! Implements the ballot leader-election protocol of Damian, Drăgoi,
//! Militaru & Widder, "Communication-closed asynchronous protocols"

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DEFAULT_NODES: usize = 3;
const DEFAULT_BALLOTS: u64 = 1;
const DEFAULT_W: u64 = 1;
const DEFAULT_L: u64 = 0;
const DEFAULT_U: u64 = 1;

static EXECS_WITH_ELECTION: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug)]
struct TimedBounds {
    /// `send_msg` network transit-time lower bound (`--l`).
    l: u64,
    /// `send_msg` network transit-time upper bound (`--u`).
    u: u64,
    /// Per-node storage delay (`--sd`). `None` derives the feasibility minimum.
    sd: Option<u64>,
}

impl TimedBounds {
    fn storage_delay(self) -> u64 {
        self.sd.unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

/// The two phases of a ballot. `NewBallot` proposes a leader; `AckBallot`
/// collects leadership acknowledgements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    NewBallot,
    AckBallot,
}

impl Phase {
    fn index(self) -> u32 {
        match self {
            Phase::NewBallot => 0,
            Phase::AckBallot => 1,
        }
    }
}

fn tag_of(ballot: u64, phase: Phase) -> u32 {
    (ballot as u32) * 2 + phase.index()
}

#[derive(Clone, Debug, PartialEq)]
struct Init {
    peers: Vec<ThreadId>,
    me: usize,
}

/// The single wire type. Carries its round (`ballot`) and `phase`
/// explicitly; `(ballot, phase)` is mirrored into the tag by [`tag_of`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProtoMsg {
    ballot: u64,
    phase: Phase,
    leader: usize,
    sender: usize,
}

/// A successful election outcome recorded in a process's local log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LogEntry {
    ballot: u64,
    leader: usize,
}

struct Node {
    me: usize,
    peers: Vec<ThreadId>,
    n: usize,
    ballot: u64,
    leader: usize,
    log: Vec<LogEntry>,
}

/// Send `msg` for `(ballot, phase)` to every peer (self included).
fn broadcast(node: &Node, ballot: u64, phase: Phase, leader: usize) {
    let tag = tag_of(ballot, phase);
    let msg = ProtoMsg {
        ballot,
        phase,
        leader,
        sender: node.me,
    };
    for &peer in &node.peers {
        traceforge::send_tagged_msg(peer, tag, msg);
    }
}

fn all_same_leader(msgs: &[ProtoMsg], leader: usize) -> bool {
    msgs.iter().all(|m| m.leader == leader)
}

fn collect_phase(ballot: u64, phase: Phase, enough: usize, max: usize, w: u64) -> Vec<ProtoMsg> {
    let want = tag_of(ballot, phase);
    let mut got: Vec<ProtoMsg> = Vec::new();
    loop {
        if got.len() >= enough || got.len() >= max {
            break;
        }
        let matches = move |_sender: ThreadId, tag: Option<u32>| tag == Some(want);
        match traceforge::recv_tagged_msg_timed::<_, ProtoMsg>(matches, WaitTime::Finite(w)) {
            Some(msg) => got.push(msg),
            None => break, // timeout
        }
    }
    got
}

fn phase_ack_ballot(node: &mut Node, ballot: u64, w: u64) {
    broadcast(node, ballot, Phase::AckBallot, node.leader);
    let enough = node.n / 2 + 1; // strict majority, > n/2
    let acks = collect_phase(ballot, Phase::AckBallot, enough, node.n, w);
    if acks.len() >= enough && all_same_leader(&acks, node.leader) {
        node.log.push(LogEntry {
            ballot,
            leader: node.leader,
        });
    }
}

/// Leader branch of a ballot (Fig. 3, left).
fn run_leader_round(node: &mut Node, w: u64) {
    node.ballot += 1;
    let ballot = node.ballot;
    node.leader = node.me;
    broadcast(node, ballot, Phase::NewBallot, node.me);
    phase_ack_ballot(node, ballot, w);
}

/// Follower branch of a ballot
fn run_follower_round(node: &mut Node, w: u64) {
    node.ballot += 1;
    let ballot = node.ballot;
    let proposals = collect_phase(ballot, Phase::NewBallot, 1, 1, w);
    if let Some(proposal) = proposals.first() {
        node.ballot = proposal.ballot;
        node.leader = proposal.sender;
        phase_ack_ballot(node, node.ballot, w);
    }
    // No NewBallot heard: skip AckBallot. Acking a leader that no
    // candidate announced would risk a spurious safety failure; a
    // follower with nothing to ack simply records no LogEntry.
}

/// One ballot: `coord()` decides the branch, per process, per ballot.
fn step(node: &mut Node, w: u64) {
    if traceforge::nondet() {
        run_leader_round(node, w);
    } else {
        run_follower_round(node, w);
    }
}

/// Per-process entry point: wait for `Init`, then run `ballots` ballots.
fn node(
    ballots: u64,
    w: u64,
    me: usize,
    n: usize,
    main_tid: ThreadId,
) -> Vec<LogEntry> {
    let Init { peers, me: _ } =
        traceforge::recv_tagged_msg_block::<_, Init>(move |sender, _tag| sender == main_tid);

    let mut node = Node {
        me,
        peers,
        n,
        ballot: 0,
        leader: me,
        log: Vec::new(),
    };
    for _ in 0..ballots {
        step(&mut node, w);
    }
    node.log
}

/// Safety property: for every ballot, all `LogEntry`s across all
/// processes that name that ballot agree on the same leader.
fn assert_log_consistency(logs: &[Vec<LogEntry>]) {
    let mut chosen: HashMap<u64, usize> = HashMap::new();
    for log in logs {
        for entry in log {
            match chosen.get(&entry.ballot) {
                Some(&leader) => traceforge::assert(leader == entry.leader),
                None => {
                    chosen.insert(entry.ballot, entry.leader);
                }
            }
        }
    }
    // Record whether this execution elected at least one leader, so the
    // run summary can confirm leader election stays reachable.
    if logs.iter().any(|log| !log.is_empty()) {
        EXECS_WITH_ELECTION.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Parallel {
    Shared,
    Partitioned,
}

fn apply_parallel(builder: traceforge::ConfigBuilder, parallel: Parallel) -> traceforge::ConfigBuilder {
    match parallel {
        Parallel::Shared => builder.with_parallel(true),
        Parallel::Partitioned => builder.with_partitioned_parallelization(true),
    }
}

fn build_config(mode: Mode, _n: usize, tb: TimedBounds, parallel: Parallel) -> Config {
    let builder = Config::builder()
        .with_progress_report(0)
        .with_verbose(0);
    let builder = apply_parallel(builder, parallel);
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(tb.l, tb.u, tb.storage_delay()).build(),
    }
}

fn run(
    mode: Mode,
    n: usize,
    ballots: u64,
    w: u64,
    tb: TimedBounds,
    parallel: Parallel,
) -> (Stats, Duration, usize) {
    let cfg = build_config(mode, n, tb, parallel);
    EXECS_WITH_ELECTION.store(0, Ordering::Relaxed);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            handles.push(thread::spawn(move || {
                node(ballots, w, i, n, main_tid)
            }));
        }
        let all_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        // Send every Init *before* joining, or the nodes block on recv
        // forever and the join below deadlocks.
        for (me, h) in handles.iter().enumerate() {
            traceforge::send_msg(
                h.thread().id(),
                Init {
                    peers: all_ids.clone(),
                    me,
                },
            );
        }
        let logs: Vec<Vec<LogEntry>> = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        assert_log_consistency(&logs);
    });
    let elected = EXECS_WITH_ELECTION.load(Ordering::Relaxed);
    (stats, t0.elapsed(), elected)
}

#[allow(clippy::too_many_arguments)]
fn print_one(
    label: &str,
    n: usize,
    ballots: u64,
    w: u64,
    timed: Option<(u64, u64, u64)>,
    stats: &Stats,
    dur: Duration,
    elected: usize,
) {
    let bounds = match timed {
        Some((l, u, sd)) => format!("w={w} l={l} u={u} sd={sd}"),
        None => format!("w={w} (untimed)"),
    };
    println!(
        "{label:<10} nodes={n} ballots={ballots} {bounds}  \
         execs={execs} blocked={blk} elected={elected} time={dur:?}",
        execs = stats.execs,
        blk = stats.block,
    );
}

#[allow(clippy::too_many_arguments)]
fn print_compare(
    n: usize,
    ballots: u64,
    w: u64,
    tb: TimedBounds,
    baseline: (Stats, Duration, usize),
    timed: (Stats, Duration, usize),
) {
    let (b_stats, b_dur, _b_elected) = baseline;
    let (t_stats, t_dur, t_elected) = timed;

    println!();
    println!("Communication-Closed Leader Election");
    println!("==================================================");
    println!(
        "nodes = {n}  (majority = {})  ballots = {ballots}",
        n / 2 + 1
    );
    println!(
        "timed bounds: w = {w}  l = {}  u = {}  sd = {}",
        tb.l,
        tb.u,
        tb.storage_delay(),
    );
    println!();
    println!(
        "{:<10} {:>12} {:>12} {:>10} {:>14}",
        "mode", "execs", "blocked", "elected", "time"
    );
    println!(
        "{:<10} {:>12} {:>12} {:>10} {:>14?}",
        "baseline", b_stats.execs, b_stats.block, _b_elected, b_dur
    );
    println!(
        "{:<10} {:>12} {:>12} {:>10} {:>14?}",
        "timed", t_stats.execs, t_stats.block, t_elected, t_dur
    );
    println!();
    let total_b = b_stats.execs + b_stats.block;
    let total_t = (t_stats.execs + t_stats.block).max(1);
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    println!(
        "(execs+blocked) ratio: {:.2}x   execs ratio: {exec_ratio:.2}x   (= baseline / timed)",
        total_b as f64 / total_t as f64,
    );
    println!();
    println!("At most one leader per ballot is checked by assert_log_consistency:");
    println!("every process's log is collected and, per ballot, all recorded");
    println!("leaders must agree.");
}

fn parse_args() -> (String, usize, u64, u64, TimedBounds, Parallel) {
    let mut mode = String::from("compare");
    let mut nodes = DEFAULT_NODES;
    let mut ballots = DEFAULT_BALLOTS;
    let mut w = DEFAULT_W;
    let mut l = DEFAULT_L;
    let mut u = DEFAULT_U;
    let mut sd: Option<u64> = None;
    let mut parallel = Parallel::Shared;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = args
                    .next()
                    .unwrap_or_else(|| panic!("--mode requires a value"));
            }
            "--nodes" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--nodes requires a value"));
                nodes = v.parse().unwrap_or_else(|_| panic!("invalid --nodes: {v}"));
            }
            "--ballots" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--ballots requires a value"));
                ballots = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --ballots: {v}"));
            }
            "--w" => {
                let v = args.next().unwrap_or_else(|| panic!("--w requires a value"));
                w = v.parse().unwrap_or_else(|_| panic!("invalid --w: {v}"));
            }
            "--l" => {
                let v = args.next().unwrap_or_else(|| panic!("--l requires a value"));
                l = v.parse().unwrap_or_else(|_| panic!("invalid --l: {v}"));
            }
            "--u" => {
                let v = args.next().unwrap_or_else(|| panic!("--u requires a value"));
                u = v.parse().unwrap_or_else(|_| panic!("invalid --u: {v}"));
            }
            "--sd" => {
                let v = args.next().unwrap_or_else(|| panic!("--sd requires a value"));
                sd = Some(v.parse().unwrap_or_else(|_| panic!("invalid --sd: {v}")));
            }
            "--parallel" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--parallel requires a value (shared|partitioned)"));
                parallel = match v.as_str() {
                    "shared" => Parallel::Shared,
                    "partitioned" => Parallel::Partitioned,
                    other => panic!("invalid --parallel: {other} (expected shared|partitioned)"),
                };
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: comm_closed_leader_election [--mode baseline|timed|compare] \
                     [--nodes N] [--ballots B] [--w W] \
                     [--l L] [--u U] [--sd SD] \
                     [--parallel shared|partitioned]\n\
                     \n  \
                     --l/--u/--sd tune the timed model (timed mode only).\n  \
                     --parallel selects the verifier's parallel strategy (default: shared)."
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, nodes, ballots, w, TimedBounds { l, u, sd }, parallel)
}

fn main() {
    let (mode_str, n, ballots, w, tb, parallel) = parse_args();
    assert!(n >= 3, "need at least 3 nodes for a meaningful majority");
    assert!(ballots >= 1, "ballots must be >= 1");
    assert!(tb.l <= tb.u, "transit lower bound --l must be <= upper bound --u");

    let resolved = (tb.l, tb.u, tb.storage_delay());
    match mode_str.as_str() {
        "baseline" => {
            let (s, d, e) = run(Mode::Baseline, n, ballots, w, tb, parallel);
            print_one("baseline", n, ballots, w, None, &s, d, e);
        }
        "timed" => {
            let (s, d, e) = run(Mode::Timed, n, ballots, w, tb, parallel);
            print_one("timed", n, ballots, w, Some(resolved), &s, d, e);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, n, ballots, w, tb, parallel);
            let timed = run(Mode::Timed, n, ballots, w, tb, parallel);
            print_compare(n, ballots, w, tb, baseline, timed);
        }
        other => panic!("invalid --mode: {other} (expected baseline|timed|compare)"),
    }
}
