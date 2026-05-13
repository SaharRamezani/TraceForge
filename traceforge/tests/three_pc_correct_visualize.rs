//! Visualization-only test that captures every explored execution of
//! the **full Skeen '81 Three-Phase Commit** (including termination
//! protocol) into `viz_out/three_pc_correct/`.
//!
//! Configuration is identical to the N=2 row of the participants sweep
//! in `tests/three_pc_compare.rs`, so the execution graphs rendered
//! here are exactly the executions counted in that row.
//!
//! Bound choice (Skeen '81 + BHG '87 §7):
//!   N = 2 (smallest non-trivial; matches compare-test N=2 row)
//!   L = 0, U = 1, W = 2·U = 2, DELTA = W + 1 = 3, sd = 0
//!
//! Configurable rounds via the `THREE_PC_ROUNDS` env var (default 1).
//! Each round is one independent commit transaction. Keep at R=1 for
//! human-inspectable graphs — at R=2 the visualizer would snapshot
//! ~518k DOT files at N=2 (and the verification itself takes ~minute+),
//! which is impractical for hand-inspection. Use the compare test or
//! the main example for multi-round measurements.
//!
//! Run:
//!   cargo test --release --test three_pc_correct_visualize
//!   THREE_PC_ROUNDS=2 cargo test --release --test three_pc_correct_visualize   # impractical at this N
//!   python3 viz_out/build_html.py
//! Then open `viz_out/index.html`.

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_PARTICIPANTS: u32 = 2;
const U: u64 = 1;
const W: u64 = 2 * U;          // Skeen termination wait
const DELTA: u64 = W + 1;      // minimum stagger for cross-mapping pruning

/// Number of independent commit rounds. Read from the
/// `THREE_PC_ROUNDS` env var so the caller can override without
/// recompiling. Default 1.
///
///     THREE_PC_ROUNDS=2 cargo test --release --test three_pc_correct_visualize
fn rounds_from_env() -> u32 {
    std::env::var("THREE_PC_ROUNDS").ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

// ==== Protocol types (mirrored from compare test) =====================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState { Initial, Wait, Prepared, Committed, Aborted }

#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    Prepare { coord: ThreadId, peers: Vec<ThreadId> },
    PreCommit, Commit, Abort,
    StateRequest(ThreadId),
    StateReply(PState),
    Decide(Decision),
    Probe(ThreadId),
    ProbeAck,
}

#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    Init { peers: Vec<ThreadId> },
    Yes, No, Ack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision { Commit, Abort }

#[derive(Clone, Copy, Debug)]
struct Bounds { u: u64, w: u64, delta: u64 }

// ==== Coordinator =====================================================

fn coordinator(b: Bounds) {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CMsg>() {
        CMsg::Init { peers } => peers, _ => panic!()
    };
    let me = thread::current().id();
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Prepare { coord: me, peers: ps.clone() });
    }
    let mut yes = 0usize; let mut got = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
            Some(CMsg::Yes) => { got += 1; yes += 1; }
            Some(CMsg::No)  => got += 1,
            _ => {} // None timeout or HB-inconsistent rf
        }
    }
    if got != ps.len() || yes != ps.len() {
        for id in &ps { traceforge::send_msg(*id, PMsg::Abort); }
        return;
    }
    for id in &ps { traceforge::send_msg(*id, PMsg::PreCommit); }
    let mut acks = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
            Some(CMsg::Ack) => acks += 1,
            _ => {} // None or off-variant
        }
    }
    if acks != ps.len() { return; }
    for id in &ps { traceforge::send_msg(*id, PMsg::Commit); }
}

// ==== Participant + termination =======================================

struct ParticipantCtx {
    b: Bounds, my_index: u32, num_ps: u32,
    coord_id: ThreadId, peer_ids: Vec<ThreadId>,
    state: PState, voted_yes: bool,
}

impl ParticipantCtx {
    fn me(&self) -> ThreadId { self.peer_ids[self.my_index as usize] }
    fn recv(&self) -> Option<PMsg> {
        traceforge::recv_msg_timed(WaitTime::Finite(self.b.w))
    }
    fn handle_term(&self, msg: PMsg) {
        match msg {
            PMsg::StateRequest(s) => traceforge::send_msg(s, PMsg::StateReply(self.state)),
            PMsg::Probe(s) => traceforge::send_msg(s, PMsg::ProbeAck),
            _ => {}
        }
    }
    fn apply_decide(&mut self, d: Decision) -> Decision {
        self.state = match d { Decision::Commit => PState::Committed, _ => PState::Aborted };
        if d == Decision::Commit { traceforge::assert(self.voted_yes); }
        d
    }
    fn drain_until_decision(&mut self) {
        loop {
            match self.recv() {
                Some(PMsg::Abort | PMsg::Commit | PMsg::Decide(_)) => return,
                Some(o) => self.handle_term(o),
                None => return,
            }
        }
    }
    fn run_after_prepare(&mut self) -> Decision {
        traceforge::sleep(self.b.delta * (self.my_index as u64 + 1));
        self.voted_yes = traceforge::nondet();
        if self.voted_yes {
            traceforge::send_msg(self.coord_id, CMsg::Yes);
            self.state = PState::Wait;
        } else {
            traceforge::send_msg(self.coord_id, CMsg::No);
            self.state = PState::Aborted;
            self.drain_until_decision();
            return Decision::Abort;
        }
        loop {
            match self.recv() {
                Some(PMsg::Abort) => { self.state = PState::Aborted; return Decision::Abort; }
                Some(PMsg::PreCommit) => { self.state = PState::Prepared; break; }
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(o) => self.handle_term(o),
                None => return self.terminate(),
            }
        }
        traceforge::sleep(self.b.delta * self.num_ps as u64);
        traceforge::send_msg(self.coord_id, CMsg::Ack);
        loop {
            match self.recv() {
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return Decision::Commit;
                }
                Some(PMsg::Abort) => { self.state = PState::Aborted; return Decision::Abort; }
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(o) => self.handle_term(o),
                None => return self.terminate(),
            }
        }
    }
    fn terminate(&mut self) -> Decision {
        for j in 0..self.my_index {
            traceforge::send_msg(self.peer_ids[j as usize], PMsg::Probe(self.me()));
        }
        let mut any_lower = false;
        for _ in 0..self.my_index {
            traceforge::sleep(self.b.delta);
            match self.recv() {
                Some(PMsg::ProbeAck) => any_lower = true,
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(o) => self.handle_term(o),
                None => {}
            }
        }
        if any_lower { return self.wait_for_decide(); }
        self.run_termination()
    }
    fn wait_for_decide(&mut self) -> Decision {
        loop {
            match self.recv() {
                Some(PMsg::Decide(d)) => return self.apply_decide(d),
                Some(o) => self.handle_term(o),
                None => return Decision::Abort,
            }
        }
    }
    fn run_termination(&mut self) -> Decision {
        for (j, p) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                traceforge::send_msg(*p, PMsg::StateRequest(self.me()));
            }
        }
        let mut states = vec![self.state];
        for _ in 0..self.peer_ids.len() - 1 {
            traceforge::sleep(self.b.delta);
            match self.recv() {
                Some(PMsg::StateReply(s)) => states.push(s),
                Some(o) => self.handle_term(o),
                None => {}
            }
        }
        let d = if states.iter().any(|s| *s == PState::Committed) { Decision::Commit }
            else if states.iter().any(|s| *s == PState::Aborted) { Decision::Abort }
            else if states.iter().any(|s| *s == PState::Prepared) { Decision::Commit }
            else { Decision::Abort };
        for (j, p) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                traceforge::send_msg(*p, PMsg::Decide(d));
            }
        }
        self.state = match d { Decision::Commit => PState::Committed, _ => PState::Aborted };
        if d == Decision::Commit { traceforge::assert(self.voted_yes); }
        d
    }
}

fn participant(b: Bounds, num_ps: u32, index: u32, rounds: u32) {
    let mut ctx_opt: Option<ParticipantCtx> = None;
    for _r in 0..rounds {
        let (coord_id, peers) = loop {
            match traceforge::recv_msg_block::<PMsg>() {
                PMsg::Prepare { coord, peers } => break (coord, peers),
                PMsg::StateRequest(s) => {
                    let st = ctx_opt.as_ref().map(|c| c.state).unwrap_or(PState::Initial);
                    traceforge::send_msg(s, PMsg::StateReply(st));
                }
                PMsg::Probe(s) => traceforge::send_msg(s, PMsg::ProbeAck),
                _ => {}
            }
        };
        let ctx = ctx_opt.get_or_insert(ParticipantCtx {
            b, my_index: index, num_ps, coord_id, peer_ids: peers.clone(),
            state: PState::Initial, voted_yes: false,
        });
        ctx.coord_id = coord_id;
        ctx.peer_ids = peers;
        ctx.state = PState::Initial;
        ctx.voted_yes = false;
        let _ = ctx.run_after_prepare();
    }
}

// ==== Test entry =======================================================

struct DotSnapshotter { src: PathBuf, dir: PathBuf }

impl ExecutionObserver for DotSnapshotter {
    fn before(&mut self, _eid: ExecutionId) { let _ = fs::write(&self.src, b""); }
    fn after(&mut self, eid: ExecutionId, _ec: &EndCondition, _c: CoverageInfo) {
        let dst = self.dir.join(format!("exec_{:03}.dot", eid));
        let _ = fs::copy(&self.src, &dst);
    }
}

#[test]
fn visualize_correct_3pc() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest).join("..").join("viz_out").join("three_pc_correct");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    let _ = fs::write(&prune_log, b"");

    let description = format!(
        "Full Skeen 3PC (N={}, with termination sub-protocol). Includes election + state-collection + decision rule. \
         Bounds: L=0, U=1, W=2 (= 2·U, Skeen termination bound), DELTA=3. Each rendered execution is one schedule \
         the model checker explored under MUST-τ — including those where a participant times out and runs \
         termination on behalf of the group.",
        NUM_PARTICIPANTS
    );
    let labels: Vec<(String, String)> = (0..NUM_PARTICIPANTS)
        .map(|i| (format!("t{}", i + 2), format!("p{}", i)))
        .collect();
    let labels_json = labels.iter().fold(serde_json::Map::new(), |mut m, (k, v)| {
        m.insert(k.clone(), serde_json::Value::String(v.clone()));
        m
    });
    let order: Vec<String> = std::iter::once("t1".into())
        .chain(labels.iter().map(|(k, _)| k.clone()))
        .chain(std::iter::once("t0".into()))
        .collect();
    let mut labels_full = labels_json;
    labels_full.insert("t0".to_string(), serde_json::Value::String("main".to_string()));
    labels_full.insert("t1".to_string(), serde_json::Value::String("coordinator".to_string()));
    let meta_json = json!({
        "kind": "vote",
        "title": format!("Three-Phase Commit (full Skeen, termination, N={})", NUM_PARTICIPANTS),
        "description": description,
        "labels": labels_full,
        "order": order,
        "l": 0, "u": U, "sd": 0,
        "wait": format!("coord vote/ack={} (= 2·U, Skeen termination), participant={}", W, W),
    }).to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_temporal(0, U, 0)
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_progress_report(usize::MAX)
        .with_dot_out(src.to_str().unwrap())
        .with_dot_out_blocked(true)
        .with_prune_log(prune_log.to_str().unwrap())
        .with_callback(Box::new(DotSnapshotter { src: src.clone(), dir: dir.clone() }))
        .build();

    let b = Bounds { u: U, w: W, delta: DELTA };
    let r_rounds = rounds_from_env();
    traceforge::verify(cfg, move || {
        let mut handles = Vec::new();
        for i in 0..NUM_PARTICIPANTS {
            handles.push(thread::spawn(move || participant(b, NUM_PARTICIPANTS, i, r_rounds)));
        }
        let peers: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        for _r in 0..r_rounds {
            let peers_for_coord = peers.clone();
            let c = thread::spawn(move || coordinator(b));
            traceforge::send_msg(c.thread().id(), CMsg::Init { peers: peers_for_coord });
        }
    });

    let _ = fs::remove_file(src);
}
