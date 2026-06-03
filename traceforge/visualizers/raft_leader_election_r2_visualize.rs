//! Visualization helper for the multi-term Raft protocol used in
//!
//! [traceforge/tests/raft_leader_election_r2_compare.rs]
//! ## Run
//!
//! ```bash
//! cargo run --release --example raft_leader_election_r2_visualize

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_NODES: usize = 3;
const DELTA: u64 = 2;
const ROUNDS: u64 = 2;
const W: u64 = 1;

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

fn start_election(state: &mut NodeState, peers: &[ThreadId], majority: u64) {
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

fn node(me: usize, main_tid: ThreadId) {
    let Init { peers, me: _ } = traceforge::recv_tagged_msg_block::<_, Init>(
        move |sender, _tag| sender == main_tid,
    );

    let majority = (NUM_NODES as u64) / 2 + 1;
    let mut state = NodeState::new(me);

    // Staggered initial sleep.
    traceforge::sleep((me as u64 + 1) * DELTA);

    loop {
        if state.current_term > ROUNDS {
            return;
        }
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
                if state.current_term + 1 > ROUNDS {
                    return;
                }
                start_election(&mut state, &peers, majority);
            }
        }
    }
}

struct DotSnapshotter {
    src: PathBuf,
    dir: PathBuf,
}

impl ExecutionObserver for DotSnapshotter {
    fn before(&mut self, _eid: ExecutionId) {
        let _ = fs::write(&self.src, b"");
    }
    fn after(&mut self, eid: ExecutionId, _ec: &EndCondition, _c: CoverageInfo) {
        let dst = self.dir.join(format!("exec_{:03}.dot", eid));
        let _ = fs::copy(&self.src, &dst);
    }
}

fn main() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join("raft_leader_election_r2");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    let _ = fs::write(&prune_log, b"");

    let description = format!(
        "Raft leader election R={ROUNDS}, N={NUM_NODES}, timed mode"
    );

    let mut labels = serde_json::Map::new();
    labels.insert("t0".into(), json!("main"));
    let mut order = vec![];
    for i in 0..NUM_NODES {
        labels.insert(format!("t{}", i + 1), json!(format!("n{}", i)));
        order.push(format!("t{}", i + 1));
    }
    order.push("t0".to_string());

    let sd = (NUM_NODES as u64).saturating_sub(1) * DELTA;
    let meta_json = json!({
        "kind":        "pipeline",
        "title":       format!("Raft leader election — R={ROUNDS} multi-term (N={NUM_NODES})"),
        "description": description,
        "labels":      serde_json::Value::Object(labels),
        "order":       order,
        "l": 0, "u": 1, "sd": sd, "wait": W,
    })
    .to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_timed(0, 1, sd)
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_progress_report(usize::MAX)
        .with_dot_out(src.to_str().unwrap())
        .with_dot_out_blocked(true)
        .with_prune_log(prune_log.to_str().unwrap())
        .with_callback(Box::new(DotSnapshotter {
            src: src.clone(),
            dir: dir.clone(),
        }))
        .build();

    traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let mut handles = Vec::with_capacity(NUM_NODES);
        for i in 0..NUM_NODES {
            handles.push(thread::spawn(move || node(i, main_tid)));
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

    let _ = fs::remove_file(src);
}
