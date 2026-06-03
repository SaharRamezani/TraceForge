//! Visualization helper that captures explored executions of the
//! communication-closed leader-election protocol
//!
//! ## Run
//!
//! cargo run --release --example comm_closed_leader_election_visualize
//! python3 viz_out/build_html.py
//! firefox viz_out/index.html

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

/// Set to `true` by `assert_log_consistency` when the just-finished
/// execution recorded at least one `LogEntry`. The snapshotter reads
/// this in `after` to mark electing executions in `elected.txt`.
/// Reset to `false` in `before`. Safe under sequential verification
/// (no `with_parallel(true)`).
static LAST_ELECTED: AtomicBool = AtomicBool::new(false);

use serde_json::json;
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_NODES: usize = 3;
const BALLOTS: u64 = 1;
const W: u64 = 1;
const L: u64 = 1;
const U: u64 = 1;
const SD: u64 = 0;

/// Cap how many per-execution `.dot` files are written to disk
const MAX_EXECS_TO_DUMP: u64 = 100;

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

fn run_leader_round(node: &mut Node, w: u64) {
    node.ballot += 1;
    let ballot = node.ballot;
    node.leader = node.me;
    broadcast(node, ballot, Phase::NewBallot, node.me);
    phase_ack_ballot(node, ballot, w);
}

fn run_follower_round(node: &mut Node, w: u64) {
    node.ballot += 1;
    let ballot = node.ballot;
    let proposals = collect_phase(ballot, Phase::NewBallot, 1, 1, w);
    if let Some(proposal) = proposals.first() {
        node.ballot = proposal.ballot;
        node.leader = proposal.sender;
        phase_ack_ballot(node, node.ballot, w);
    }
}

fn step(node: &mut Node, w: u64) {
    if traceforge::nondet() {
        run_leader_round(node, w);
    } else {
        run_follower_round(node, w);
    }
}

fn node(ballots: u64, w: u64, me: usize, n: usize, main_tid: ThreadId) -> Vec<LogEntry> {
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
    if logs.iter().any(|log| !log.is_empty()) {
        LAST_ELECTED.store(true, Ordering::Relaxed);
    }
}

struct DotSnapshotter {
    src: PathBuf,
    dir: PathBuf,
    cap: u64,
    count: u64,
}

impl ExecutionObserver for DotSnapshotter {
    fn before(&mut self, _eid: ExecutionId) {
        LAST_ELECTED.store(false, Ordering::Relaxed);
        let _ = fs::write(&self.src, b"");
    }
    fn after(&mut self, eid: ExecutionId, _ec: &EndCondition, _c: CoverageInfo) {
        self.count += 1;
        if self.count > self.cap {
            return;
        }
        let dst = self.dir.join(format!("exec_{:03}.dot", eid));
        let _ = fs::copy(&self.src, &dst);
        if LAST_ELECTED.load(Ordering::Relaxed) {
            let manifest = self.dir.join("elected.txt");
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&manifest)
            {
                let _ = writeln!(f, "exec_{:03}.dot", eid);
            }
        }
    }
}

fn main() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join("comm_closed_leader_election");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    let _ = fs::write(&prune_log, b"");

    let description = format!(
        "Communication-closed leader election (CAV'19 Figure 3, N={NUM_NODES}, \
         ballots={BALLOTS}, timed mode)."
    );

    let mut labels = serde_json::Map::new();
    labels.insert("t0".into(), json!("main"));
    let mut order = vec![];
    for i in 0..NUM_NODES {
        labels.insert(format!("t{}", i + 1), json!(format!("n{i}")));
        order.push(format!("t{}", i + 1));
    }
    order.push("t0".to_string());

    let meta_json = json!({
        "kind":        "pipeline",
        "nondet_role": "role",
        "title":       format!("Comm-closed leader election (N={NUM_NODES}, timed, l=u=w=1)"),
        "description": description,
        "labels":      serde_json::Value::Object(labels),
        "order":       order,
        "l": L, "u": U, "sd": SD, "wait": W,
    })
    .to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_timed(L, U, SD)
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_progress_report(usize::MAX)
        .with_dot_out(src.to_str().unwrap())
        .with_dot_out_blocked(true)
        .with_prune_log(prune_log.to_str().unwrap())
        .with_callback(Box::new(DotSnapshotter {
            src: src.clone(),
            dir: dir.clone(),
            cap: MAX_EXECS_TO_DUMP,
            count: 0,
        }))
        .build();

    traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let mut handles = Vec::with_capacity(NUM_NODES);
        for i in 0..NUM_NODES {
            handles.push(thread::spawn(move || {
                node(BALLOTS, W, i, NUM_NODES, main_tid)
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

    let _ = fs::remove_file(src);
}
