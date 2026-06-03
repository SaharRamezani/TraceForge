//! Raft leader-election safety integration example.
//!
//! See [traceforge/examples/raft_leader_election.rs] for the protocol
//! design notes. This file re-imports the same Figure-2 protocol and
//! verifies Election Safety (Figure 3), parameterised by `(mode, N, R)`.
//! It used to live under `tests/` as a multi-`#[test]` driver; it now
//! sits under `integration_examples/` as a Cargo example, so it stays
//! out of the default `cargo test` run.
//!
//! ## Run
//!
//! ```bash
//! # default — the N=3 / R=1 cases that fn main() invokes
//! cargo run --release --example raft_leader_election_tests
//! ```
//!
//! To run the heavier N=5 / R≥2 cases (still defined in this file as
//! dead code), uncomment the corresponding calls inside `fn main()`
//! and re-run.
//!
//! ## Test bundles
//!
//! Under the current `term_completed`-exit protocol every node finishes
//! round 1 (Leader or Follower-with-seen-leader) and `return`s, so the
//! outer `for round in 1..=R` never iterates past round 1 and `R` is
//! vestigial. We therefore only ship R=1 tests here; for genuine
//! multi-term retry-on-timer-fire semantics see
//! [traceforge/tests/raft_leader_election_r2_compare.rs] which uses
//! `recv_msg_timed(Finite(W))` per recv to model election-timeout
//! races.
//!
//! | test name                           | mode     | N | R | default |
//! | ----------------------------------- | -------- | - | - | ------- |
//! | `election_safety_n3_baseline_r1`    | baseline | 3 | 1 | run     |
//! | `election_safety_n3_timed_r1`    | timed | 3 | 1 | run     |
//! | `election_safety_n5_baseline_r1`    | baseline | 5 | 1 | ignored |
//! | `election_safety_n5_timed_r1`    | timed | 5 | 1 | ignored |

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

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

/// Init is its own type (not a `NodeMsg` variant) so the init-wait
/// `recv_tagged_msg_block::<_, Init>(sender == main)` doesn't accidentally
/// match a racing RPC from a peer. See the example for the rationale.
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
            // Election Safety (Figure 3).
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

        // Keep receiving until this term has visibly completed for us
        // (paper-faithful — Figure 2 servers run forever within a
        // term). No iteration budget; R bounds terms, not iters.
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
        Mode::Baseline => Config::builder().with_progress_report(usize::MAX).build(),
        Mode::Timed => {
            let sd = (num_nodes as u64).saturating_sub(1) * delta;
            Config::builder()
                .with_timed(0, 1, sd)
                .with_progress_report(usize::MAX)
                .build()
        }
    }
}

fn run(mode: Mode, num_nodes: usize, delta: u64, rounds: u64) -> Stats {
    let cfg = build_config(mode, num_nodes, delta);
    traceforge::verify(cfg, move || {
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
    })
}

// =============== R=1 (single term) ===============

fn election_safety_n3_baseline_r1() {
    let s = run(Mode::Baseline, 3, 2, 1);
    println!("baseline N=3 R=1: execs={} blocked={}", s.execs, s.block);
    assert!(s.execs + s.block > 0);
}

fn election_safety_n3_timed_r1() {
    let s = run(Mode::Timed, 3, 2, 1);
    println!("timed N=3 R=1: execs={} blocked={}", s.execs, s.block);
    assert!(s.execs + s.block > 0);
}

// =============== N=5 (kept around but ignored by default) ===============

fn election_safety_n5_baseline_r1() {
    let s = run(Mode::Baseline, 5, 2, 1);
    println!("baseline N=5 R=1: execs={} blocked={}", s.execs, s.block);
    assert!(s.execs + s.block > 0);
}

fn election_safety_n5_timed_r1() {
    let s = run(Mode::Timed, 5, 2, 1);
    println!("timed N=5 R=1: execs={} blocked={}", s.execs, s.block);
    assert!(s.execs + s.block > 0);
}

fn main() {
    election_safety_n3_baseline_r1();
    election_safety_n3_timed_r1();
}
