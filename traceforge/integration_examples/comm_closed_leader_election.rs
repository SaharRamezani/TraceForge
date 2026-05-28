//! Communication-closed leader-election safety integration example.
//!
//! Integration example for the CAV'19 ballot leader-election protocol — see
//! [traceforge/examples/comm_closed_leader_election.rs] for the full
//! protocol design notes. This file re-declares the protocol and verifies
//! the **at-most-one-leader-per-ballot** safety property under both the
//! baseline MUST verifier and MUST-τ, parameterised by `(mode, N)`. It
//! used to live under `tests/` as a multi-`#[test]` driver; it now sits
//! under `integration_examples/` as a Cargo example so it stays out of
//! the default `cargo test` run.
//!
//! The property is checked by `assert_log_consistency`: after every
//! process finishes, its log is collected and, per ballot, all recorded
//! leaders must agree. A disagreement makes `traceforge::assert` fire and
//! the enclosing `traceforge::verify` call panics, exiting non-zero.
//!
//! ## Run
//!
//! ```bash
//! # default — the N=3 cases (baseline + timed + compare) that fn main() invokes
//! cargo run --release --example comm_closed_leader_election_tests
//! ```
//!
//! To run the heavier N=5 cases (still defined in this file as dead
//! code), uncomment the corresponding calls inside `fn main()` and
//! re-run.
//!
//! | test name                       | mode             | N | ballots | default |
//! | ------------------------------- | ---------------- | - | ------- | ------- |
//! | `log_consistency_n3_baseline`   | baseline         | 3 | 1       | run     |
//! | `log_consistency_n3_timed`   | timed         | 3 | 1       | run     |
//! | `compare_n3`                    | baseline+timed| 3 | 1       | run     |
//! | `log_consistency_n5_baseline`   | baseline         | 5 | 1       | ignored |
//! | `log_consistency_n5_timed`   | timed         | 5 | 1       | ignored |
//!
//! `compare_n3` prints the MUST vs MUST-τ table and asserts that timed
//! pruning keeps `timed.execs < baseline.execs`.

use std::collections::HashMap;
use std::fmt::Write;
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

/// The two phases of a ballot.
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

/// Encode `(ballot, phase)` into the `u32` message tag — a receive accepts
/// only messages whose tag matches the round + phase it expects.
fn tag_of(ballot: u64, phase: Phase) -> u32 {
    (ballot as u32) * 2 + phase.index()
}

/// Bootstrapping message — its own type so the init-wait only matches it.
#[derive(Clone, Debug, PartialEq)]
struct Init {
    peers: Vec<ThreadId>,
    me: usize,
}

/// The single wire type; carries its round (`ballot`) and `phase`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProtoMsg {
    ballot: u64,
    phase: Phase,
    leader: usize,
    sender: usize,
}

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

/// True iff every message acknowledges `leader` — the paper's
/// `all_same(mbox, leader)`. Checking only that the acks agree with each
/// other is unsound and elects two leaders in one ballot.
fn all_same_leader(msgs: &[ProtoMsg], leader: usize) -> bool {
    msgs.iter().all(|m| m.leader == leader)
}

/// The paper's timeout-based receive loop, and the only receive strategy:
/// collect messages for `(ballot, phase)` until `enough`/`max` are in
/// hand or a receive times out.
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
            None => break,
        }
    }
    got
}

fn phase_ack_ballot(node: &mut Node, ballot: u64, w: u64) {
    broadcast(node, ballot, Phase::AckBallot, node.leader);
    let enough = node.n / 2 + 1;
    let acks = collect_phase(ballot, Phase::AckBallot, enough, node.n, w);
    if acks.len() >= enough && all_same_leader(&acks, node.leader) {
        node.log.push(LogEntry {
            ballot,
            leader: node.leader,
        });
    }
}

fn run_leader_round(node: &mut Node, mode: Mode, delta: u64, w: u64) {
    node.ballot += 1;
    let ballot = node.ballot;
    if mode == Mode::Timed {
        traceforge::sleep((node.me as u64 + 1) * delta);
    }
    node.leader = node.me;
    broadcast(node, ballot, Phase::NewBallot, node.me);
    phase_ack_ballot(node, ballot, w);
}

fn run_follower_round(node: &mut Node, mode: Mode, delta: u64, w: u64) {
    node.ballot += 1;
    let ballot = node.ballot;
    if mode == Mode::Timed {
        traceforge::sleep((node.me as u64 + 1) * delta);
    }
    let proposals = collect_phase(ballot, Phase::NewBallot, 1, 1, w);
    if let Some(proposal) = proposals.first() {
        node.ballot = proposal.ballot;
        node.leader = proposal.sender;
        phase_ack_ballot(node, node.ballot, w);
    }
}

fn step(node: &mut Node, mode: Mode, delta: u64, w: u64) {
    if traceforge::nondet() {
        run_leader_round(node, mode, delta, w);
    } else {
        run_follower_round(node, mode, delta, w);
    }
}

fn node(
    mode: Mode,
    delta: u64,
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
        step(&mut node, mode, delta, w);
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
}

fn build_config(mode: Mode, n: usize, delta: u64) -> Config {
    match mode {
        Mode::Baseline => Config::builder()
            .with_parallel(true)
            .with_progress_report(usize::MAX)
            .with_verbose(0)
            .build(),
        Mode::Timed => {
            let sd = (n as u64).saturating_sub(1) * delta;
            Config::builder()
                .with_timed(0, 1, sd)
                .with_parallel(true)
                .with_progress_report(usize::MAX)
                .with_verbose(0)
                .build()
        }
    }
}

fn run(mode: Mode, n: usize, delta: u64, ballots: u64, w: u64) -> (Stats, Duration) {
    let cfg = build_config(mode, n, delta);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            handles.push(thread::spawn(move || {
                node(mode, delta, ballots, w, i, n, main_tid)
            }));
        }
        let all_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        for (me, h) in handles.iter().enumerate() {
            traceforge::send_msg(
                h.thread().id(),
                Init {
                    peers: all_ids.clone(),
                    me,
                },
            );
        }
        let logs: Vec<Vec<LogEntry>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_log_consistency(&logs);
    });
    (stats, t0.elapsed())
}

// =============== N=3 (run by default) ===============

fn log_consistency_n3_baseline() {
    let (s, d) = run(Mode::Baseline, 3, 2, 1, 1);
    println!(
        "[log_consistency_n3_baseline] execs={} blocked={} time={:.3?}",
        s.execs, s.block, d
    );
    assert!(s.execs + s.block > 0);
}

fn log_consistency_n3_timed() {
    let (s, d) = run(Mode::Timed, 3, 2, 1, 1);
    println!(
        "[log_consistency_n3_timed] execs={} blocked={} time={:.3?}",
        s.execs, s.block, d
    );
    assert!(s.execs + s.block > 0);
}

/// Baseline MUST vs MUST-τ, side by side. Each `verify` call also checks
/// the safety property (a violation panics and fails this test); on top
/// of that, MUST-τ must prune strictly below baseline. The whole report
/// is built into one string and printed with a single `print!` so the
/// block stays intact even when cargo runs tests in parallel.
fn compare_n3() {
    let (b, bd) = run(Mode::Baseline, 3, 2, 1, 1);
    let (t, td) = run(Mode::Timed, 3, 2, 1, 1);

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(
        out,
        "=== compare_n3: communication-closed leader election, MUST vs MUST-tau (N=3, 1 ballot) ==="
    )
    .unwrap();
    writeln!(
        out,
        "  {:<9} {:>8} {:>9} {:>13}",
        "mode", "execs", "blocked", "time"
    )
    .unwrap();
    writeln!(
        out,
        "  {:<9} {:>8} {:>9} {:>13.3?}",
        "baseline", b.execs, b.block, bd
    )
    .unwrap();
    writeln!(
        out,
        "  {:<9} {:>8} {:>9} {:>13.3?}",
        "timed", t.execs, t.block, td
    )
    .unwrap();
    writeln!(
        out,
        "  execs reduction: {:.2}x  (= baseline / timed)",
        b.execs as f64 / t.execs.max(1) as f64
    )
    .unwrap();
    print!("{}", out);

    assert!(
        t.execs < b.execs,
        "timed execs ({}) should be < baseline execs ({})",
        t.execs,
        b.execs,
    );
}

// =============== N=5 (kept around but ignored by default) ===============

fn log_consistency_n5_baseline() {
    let (s, d) = run(Mode::Baseline, 5, 2, 1, 1);
    println!(
        "[log_consistency_n5_baseline] execs={} blocked={} time={:.3?}",
        s.execs, s.block, d
    );
    assert!(s.execs + s.block > 0);
}

fn log_consistency_n5_timed() {
    let (s, d) = run(Mode::Timed, 5, 2, 1, 1);
    println!(
        "[log_consistency_n5_timed] execs={} blocked={} time={:.3?}",
        s.execs, s.block, d
    );
    assert!(s.execs + s.block > 0);
}

fn main() {
    log_consistency_n3_baseline();
    log_consistency_n3_timed();
    compare_n3();
}
