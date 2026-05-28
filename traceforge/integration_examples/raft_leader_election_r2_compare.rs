//! Side-by-side compare for Raft leader election with **genuine
//! multi-term retry semantics** — the timer-fire-then-bump-term path
//! of Figure 2 that the `term_completed`-exit protocol in
//! [traceforge/tests/raft_leader_election_compare.rs] doesn't model.
//!
//! ## How this differs from `raft_leader_election_compare.rs`
//!
//! The main-loop recv here is `recv_msg_timed(WaitTime::Finite(W))`
//! rather than `recv_msg_block_timed`. The `None` branch of that recv
//! is the paper's "election timer fired without hearing from anyone" —
//! the node increments `currentTerm`, votes for itself, and sends a
//! fresh round of `RequestVote`s. `R` (max term explored) is the only
//! bound; in `compare_n3_r2` below we set `R = 2`, meaning a node may
//! attempt election in term 1 and, if that times out, retry in term 2.
//!
//! State space is much larger than the single-term file (each recv
//! becomes a Some/None split). Per the user's instruction this file
//! does **not** `#[ignore]` timed mode even if it's slow — both
//! modes run by default.
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --example raft_leader_election_r2_compare
//! ```
//!
//! ## Parameters
//!
//! | const  | default | meaning                                                    |
//! | ------ | ------- | ---------------------------------------------------------- |
//! | `N`    | 3       | cluster size                                               |
//! | `R`    | 2       | max term — `compare_n3_r2` uses this                       |
//! | `DELTA`| 2       | staggered-sleep unit (timed mode)                       |
//! | `W`    | 1       | election-timeout window per recv (Finite(W))               |

use std::fmt::Write;
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DELTA: u64 = 2;
const W: u64 = 1;

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

fn start_election(state: &mut NodeState, peers: &[ThreadId], majority: u64) {
    // Figure 2, Rules for Candidates: increment currentTerm, vote for
    // self, send RequestVote to all peers. The implicit timer reset is
    // the next `recv_msg_timed(Finite(W))` call at the top of the loop.
    state.current_term += 1;
    state.role = Role::Candidate;
    state.voted_for = Some(state.me);
    state.votes_received = 1;
    if state.votes_received >= majority {
        become_leader(state, peers);
        return;
    }
    for &peer in peers {
        traceforge::send_msg(
            peer,
            NodeMsg::RequestVote {
                term: state.current_term,
                candidate: state.me,
            },
        );
    }
}

fn handle_msg(
    state: &mut NodeState,
    msg: NodeMsg,
    peers: &[ThreadId],
    majority: u64,
    reply: &mut Option<(ThreadId, NodeMsg)>,
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
            *reply = Some((
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
            *reply = Some((
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

    // Timed-mode initial stagger so the first election-timeout
    // attempt is spread across nodes.
    if mode == Mode::Timed {
        traceforge::sleep((me as u64 + 1) * delta);
    }

    // Paper-faithful loop. Every iteration is a single `recv_msg_timed`
    // call: Some(msg) means an RPC arrived inside the timer window;
    // None means the election timer expired. The latter triggers a
    // new election in term `current_term + 1` — bounded by R.
    loop {
        // R bound.
        if state.current_term > rounds {
            return;
        }
        // Simplification: once we win this term we're done. (Real Raft
        // would keep heartbeating; for safety verification one round of
        // AEs is enough — Figure 3 only cares about ≤1 leader/term.)
        if state.role == Role::Leader {
            return;
        }

        match traceforge::recv_msg_timed::<NodeMsg>(WaitTime::Finite(W)) {
            Some(msg) => {
                let mut reply = None;
                handle_msg(&mut state, msg, &peers, majority, &mut reply);
                if let Some((tid, m)) = reply {
                    traceforge::send_msg(tid, m);
                }
            }
            None => {
                // Election timer fired. Figure 2 Rules for Followers /
                // Candidates: "If election timeout elapses … start new
                // election." We bump the term and send RVs; if R is
                // exhausted, give up.
                if state.current_term + 1 > rounds {
                    return;
                }
                if mode == Mode::Baseline {
                    // In baseline we don't model election-timeout
                    // randomization explicitly; nondet decides whether
                    // *we* are the one whose timer fires this tick.
                    if !traceforge::nondet() {
                        return;
                    }
                }
                start_election(&mut state, &peers, majority);
            }
        }
    }
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

fn print_compare(num_nodes: usize, rounds: u64) {
    let (s_base, d_base) = run(Mode::Baseline, num_nodes, DELTA, rounds);
    let (s_temp, d_temp) = run(Mode::Timed, num_nodes, DELTA, rounds);

    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(
        out,
        "=== Raft leader election (multi-term, R={rounds}): MUST vs MUST-τ ==="
    )
    .unwrap();
    writeln!(
        out,
        "N={num_nodes}  delta={DELTA}  rounds={rounds}  W={W}  majority={}",
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
        "baseline", s_base.execs, s_base.block, d_base
    )
    .unwrap();
    writeln!(
        out,
        "{:<10} {:>10} {:>10} {:>11.2?}",
        "timed", s_temp.execs, s_temp.block, d_temp
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Election Safety (≤ 1 leader per term in 1..={rounds}) is asserted on every"
    )
    .unwrap();
    writeln!(out, "AppendEntries receive — no central monitor thread.").unwrap();
    print!("{}", out);
}

fn compare_n3_r2() {
    print_compare(3, 2);
}

fn main() {
    compare_n3_r2();
}
