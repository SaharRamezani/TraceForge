//! [traceforge/examples/raft_leader_election.rs] for the protocol
//! design notes.
//!
//!   * `compare_n3_r1`: single-term, both modes. Called by `fn main()`.
//!   * `compare_n5_r1`: N=5 single-term, kept as dead code (the state
//!     space is very large). Uncomment in `fn main()` to run.
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --example raft_leader_election_r1_compare
//! ```
//!
//! ## Parameters
//!
//! The `DELTA` const at the top of the file controls the staggered
//! sleep unit used in timed mode.

use std::fmt::Write;
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DELTA: u64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq)]
struct Init {
    peers: Vec<ThreadId>,
    me: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum NodeMsg {
    RequestVote { term: u64, candidate: usize },
    RequestVoteResponse { term: u64, granted: bool, voter: usize },
    AppendEntries { term: u64, leader: usize },
    AppendEntriesResponse { term: u64, success: bool, from: usize },
}

struct NodeState {
    me: usize,
    current_term: u64,
    voted_for: Option<usize>,
    role: Role,
    votes_received: u64,
    seen_leader: Option<(u64, usize)>,
}

impl NodeState {
    fn new(me: usize) -> Self {
        Self {
            me,
            current_term: 0,
            voted_for: None,
            role: Role::Follower,
            votes_received: 0,
            seen_leader: None,
        }
    }
}

fn peers_index_to_tid(peers: &[ThreadId], target: usize, self_idx: usize) -> ThreadId {
    let i = if target < self_idx { target } else { target - 1 };
    peers[i]
}

fn record_leader(state: &mut NodeState, term: u64, leader: usize) {
    match state.seen_leader {
        Some((t, l)) if t == term => {
            traceforge::assert(l == leader);
        }
        _ => state.seen_leader = Some((term, leader)),
    }
}

fn send_heartbeats(state: &NodeState, peers: &[ThreadId]) {
    for &peer in peers {
        traceforge::send_msg(
            peer,
            NodeMsg::AppendEntries {
                term: state.current_term,
                leader: state.me,
            },
        );
    }
}

fn become_leader(state: &mut NodeState, peers: &[ThreadId]) {
    state.role = Role::Leader;
    record_leader(state, state.current_term, state.me);
    send_heartbeats(state, peers);
}

/// "Term completed for me" predicate — see the example for rationale.
fn term_completed(state: &NodeState, round: u64) -> bool {
    match state.role {
        Role::Leader => true,
        Role::Follower => matches!(state.seen_leader, Some((t, _)) if t >= round),
        Role::Candidate => false,
    }
}

fn handle_msg(
    state: &mut NodeState,
    msg: NodeMsg,
    peers: &[ThreadId],
    majority: u64,
    reply_target: &mut Option<(ThreadId, NodeMsg)>,
) {
    let incoming_term = match &msg {
        NodeMsg::RequestVote { term, .. }
        | NodeMsg::RequestVoteResponse { term, .. }
        | NodeMsg::AppendEntries { term, .. }
        | NodeMsg::AppendEntriesResponse { term, .. } => *term,
    };
    if incoming_term > state.current_term {
        state.current_term = incoming_term;
        state.voted_for = None;
        state.role = Role::Follower;
        state.votes_received = 0;
    }
    match msg {
        NodeMsg::RequestVote { term, candidate } => {
            let mut granted = false;
            if term >= state.current_term {
                let can_grant = state.voted_for.is_none() || state.voted_for == Some(candidate);
                if can_grant {
                    state.voted_for = Some(candidate);
                    granted = true;
                }
            }
            *reply_target = Some((
                peers_index_to_tid(peers, candidate, state.me),
                NodeMsg::RequestVoteResponse {
                    term: state.current_term,
                    granted,
                    voter: state.me,
                },
            ));
        }
        NodeMsg::RequestVoteResponse { term, granted, .. } => {
            if state.role == Role::Candidate && term == state.current_term && granted {
                state.votes_received += 1;
                if state.votes_received >= majority && state.role != Role::Leader {
                    become_leader(state, peers);
                }
            }
        }
        NodeMsg::AppendEntries { term, leader } => {
            record_leader(state, term, leader);
            let success = term >= state.current_term;
            if success {
                state.role = Role::Follower;
                state.voted_for = Some(leader);
                state.votes_received = 0;
            }
            *reply_target = Some((
                peers_index_to_tid(peers, leader, state.me),
                NodeMsg::AppendEntriesResponse {
                    term: state.current_term,
                    success,
                    from: state.me,
                },
            ));
        }
        NodeMsg::AppendEntriesResponse { .. } => {}
    }
}

fn node(mode: Mode, delta: u64, rounds: u64, me: usize, num_nodes: usize, main_tid: ThreadId) {
    let Init { peers, me: _ } = traceforge::recv_tagged_msg_block::<_, Init>(
        move |sender, _tag| sender == main_tid,
    );

    let majority = (num_nodes as u64) / 2 + 1;
    let mut state = NodeState::new(me);

    for round in 1..=rounds {
        if mode == Mode::Timed {
            traceforge::sleep((me as u64 + 1) * delta);
        }

        let saw_message_in_peek = match mode {
            Mode::Baseline => false,
            Mode::Timed => {
                if let Some(msg) =
                    traceforge::recv_msg_timed::<NodeMsg>(WaitTime::Finite(0))
                {
                    let mut reply = None;
                    handle_msg(&mut state, msg, &peers, majority, &mut reply);
                    if let Some((tid, m)) = reply {
                        traceforge::send_msg(tid, m);
                    }
                    true
                } else {
                    false
                }
            }
        };

        let should_start_election = match mode {
            Mode::Baseline => {
                state.role != Role::Leader
                    && state.current_term < round
                    && traceforge::nondet()
            }
            Mode::Timed => {
                state.role == Role::Follower
                    && state.voted_for.is_none()
                    && !saw_message_in_peek
            }
        };

        if should_start_election {
            if round > state.current_term {
                state.voted_for = None;
            }
            state.current_term = round;
            state.role = Role::Candidate;
            state.voted_for = Some(me);
            state.votes_received = 1;
            if state.votes_received >= majority {
                become_leader(&mut state, &peers);
            } else {
                for &peer in &peers {
                    traceforge::send_msg(
                        peer,
                        NodeMsg::RequestVote {
                            term: state.current_term,
                            candidate: me,
                        },
                    );
                }
            }
        }

        while !term_completed(&state, round) {
            let msg = traceforge::recv_msg_block_timed::<NodeMsg>();
            let mut reply = None;
            handle_msg(&mut state, msg, &peers, majority, &mut reply);
            if let Some((tid, m)) = reply {
                traceforge::send_msg(tid, m);
            }
        }
        return;
    }
    let _ = delta;
}

fn build_config(mode: Mode, num_nodes: usize, delta: u64) -> Config {
    match mode {
        Mode::Baseline => Config::builder()
            .with_progress_report(usize::MAX)
            .with_verbose(0)
            .build(),
        Mode::Timed => {
            let sd = (num_nodes as u64).saturating_sub(1) * delta;
            Config::builder()
                .with_timed(0, 1, sd)
                .with_progress_report(usize::MAX)
                .with_verbose(0)
                .build()
        }
    }
}

fn run(mode: Mode, num_nodes: usize, delta: u64, rounds: u64) -> (Stats, Duration) {
    let cfg = build_config(mode, num_nodes, delta);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let mut handles = Vec::with_capacity(num_nodes);
        for i in 0..num_nodes {
            handles.push(thread::spawn(move || {
                node(mode, delta, rounds, i, num_nodes, main_tid)
            }));
        }
        let all_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        for (me, h) in handles.iter().enumerate() {
            let peers: Vec<ThreadId> = all_ids
                .iter()
                .enumerate()
                .filter_map(|(i, id)| if i == me { None } else { Some(*id) })
                .collect();
            traceforge::send_msg(h.thread().id(), Init { peers, me });
        }
    });
    (stats, t0.elapsed())
}

fn print_compare(num_nodes: usize, rounds: u64, baseline_only: bool) {
    let (s_base, d_base) = run(Mode::Baseline, num_nodes, DELTA, rounds);
    let timed_result = if baseline_only {
        None
    } else {
        Some(run(Mode::Timed, num_nodes, DELTA, rounds))
    };

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(
        out,
        "=== Raft leader election (paper-faithful, R={rounds}): MUST vs MUST-τ ==="
    )
    .unwrap();
    writeln!(
        out,
        "N={num_nodes}  delta={DELTA}  rounds={rounds}  majority={}",
        num_nodes / 2 + 1
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "{:<10} {:>10} {:>10} {:>14}",
        "mode", "execs", "blocked", "wall"
    )
    .unwrap();
    writeln!(
        out,
        "{:<10} {:>10} {:>10} {:>11.2?}",
        "baseline", s_base.execs, s_base.block, d_base,
    )
    .unwrap();
    if let Some((s_temp, d_temp)) = timed_result {
        writeln!(
            out,
            "{:<10} {:>10} {:>10} {:>11.2?}",
            "timed", s_temp.execs, s_temp.block, d_temp,
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "{:<10} {:>10}",
            "timed", "(skipped — see source for why)"
        )
        .unwrap();
    }
    writeln!(out).unwrap();
    writeln!(
        out,
        "Election Safety (≤ 1 leader per term in 1..={rounds}) is asserted on every"
    )
    .unwrap();
    writeln!(out, "AppendEntries receive — no central monitor thread.").unwrap();
    print!("{}", out);
}

fn compare_n3_r1() {
    print_compare(3, 1, false);
}

fn compare_n5_r1() {
    print_compare(5, 1, false);
}

fn main() {
    compare_n3_r1();
}
