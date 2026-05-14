//! Visualization-only test that captures every explored execution of
//! the **full Skeen '81 3PC** (with Section 5 termination protocol —
//! preassigned-ranking election, two-phase backup, reentrant on backup
//! failure) into `viz_out/three_pc_correct/`.
//!
//! Configuration is identical to the N=2 row of the participants sweep
//! in `tests/three_pc_compare.rs`.
//!
//! Bound choice (Skeen '81 §5):
//!   N = 2 (smallest non-trivial)
//!   L = 0, U = 1, W = 2·U = 2, DELTA = W + 1 = 3, sd = 0
//!
//! Configurable rounds via `THREE_PC_ROUNDS` (default 1). Each round =
//! fresh threads (true clean-slate restart).
//!
//! Configurable failures via `THREE_PC_CRASHES=1`: enables fail-stop
//! crash points at every send/recv boundary on every site (coord and
//! every slave, including when acting as backup). Default OFF.
//!
//! Run:
//!   cargo test --release --test three_pc_correct_visualize
//!   THREE_PC_ROUNDS=2 THREE_PC_CRASHES=1 cargo test --release --test three_pc_correct_visualize
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
const W: u64 = 2 * U;
const DELTA: u64 = W + 1;

fn rounds_from_env() -> u32 {
    std::env::var("THREE_PC_ROUNDS").ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

fn crashes_from_env() -> bool {
    std::env::var("THREE_PC_CRASHES").ok()
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ==== Protocol types ==================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PState { Initial, Wait, Prepared, Committed, Aborted }

#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    Prepare { coord: ThreadId, peers: Vec<ThreadId> },
    PreCommit, Commit, Abort,
    MoveTo { backup: ThreadId, state: PState },
    BackupAck { from: ThreadId },
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

fn maybe_crash(crashes: bool) -> bool {
    crashes && traceforge::nondet()
}

// ==== Coordinator =====================================================

fn coordinator(b: Bounds, crashes: bool) {
    let ps: Vec<ThreadId> = loop {
        match traceforge::recv_msg_block::<CMsg>() {
            CMsg::Init { peers } => break peers,
            _ => {}
        }
    };
    let me = thread::current().id();
    if maybe_crash(crashes) { return; }

    for id in &ps {
        traceforge::send_msg(*id, PMsg::Prepare { coord: me, peers: ps.clone() });
        if maybe_crash(crashes) { return; }
    }

    let mut yes = 0usize; let mut got = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
            Some(CMsg::Yes) => { got += 1; yes += 1; }
            Some(CMsg::No)  => got += 1,
            _ => {}
        }
        if maybe_crash(crashes) { return; }
    }
    if got != ps.len() || yes != ps.len() {
        for id in &ps {
            if maybe_crash(crashes) { return; }
            traceforge::send_msg(*id, PMsg::Abort);
        }
        return;
    }
    if maybe_crash(crashes) { return; }
    for id in &ps {
        traceforge::send_msg(*id, PMsg::PreCommit);
        if maybe_crash(crashes) { return; }
    }
    let mut acks = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(b.delta);
        match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
            Some(CMsg::Ack) => acks += 1,
            _ => {}
        }
        if maybe_crash(crashes) { return; }
    }
    if acks != ps.len() { return; }
    if maybe_crash(crashes) { return; }
    for id in &ps {
        traceforge::send_msg(*id, PMsg::Commit);
        if maybe_crash(crashes) { return; }
    }
}

// ==== Slave + Skeen §5 termination ====================================

struct SlaveCtx {
    b: Bounds, my_index: u32, num_ps: u32,
    coord_id: ThreadId, peer_ids: Vec<ThreadId>,
    state: PState, voted_yes: bool, crashes: bool,
}

enum BackupOutcome { Decision(Decision), CurrentDead }

impl SlaveCtx {
    fn me(&self) -> ThreadId { self.peer_ids[self.my_index as usize] }
    fn recv(&self) -> Option<PMsg> {
        traceforge::recv_msg_timed(WaitTime::Finite(self.b.w))
    }
    fn decision_rule(&self) -> Decision {
        match self.state {
            PState::Prepared | PState::Committed => Decision::Commit,
            _ => Decision::Abort,
        }
    }
    fn handle_move_to(&mut self, backup: ThreadId, target: PState) {
        if self.state != PState::Committed && self.state != PState::Aborted {
            self.state = target;
        }
        traceforge::send_msg(backup, PMsg::BackupAck { from: self.me() });
    }
    fn run_after_prepare(&mut self) -> Decision {
        if maybe_crash(self.crashes) { return Decision::Abort; }
        traceforge::sleep(self.b.delta * (self.my_index as u64 + 1));
        self.voted_yes = traceforge::nondet();
        if self.voted_yes {
            if maybe_crash(self.crashes) { return Decision::Abort; }
            traceforge::send_msg(self.coord_id, CMsg::Yes);
            self.state = PState::Wait;
        } else {
            if maybe_crash(self.crashes) { return Decision::Abort; }
            traceforge::send_msg(self.coord_id, CMsg::No);
            self.state = PState::Aborted;
            return Decision::Abort;
        }
        if maybe_crash(self.crashes) { return Decision::Abort; }
        loop {
            match self.recv() {
                Some(PMsg::Abort) => { self.state = PState::Aborted; return Decision::Abort; }
                Some(PMsg::PreCommit) => { self.state = PState::Prepared; break; }
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return Decision::Commit;
                }
                Some(PMsg::MoveTo { backup, state }) => self.handle_move_to(backup, state),
                Some(_) => {}
                None => return self.terminate(),
            }
        }
        if maybe_crash(self.crashes) { return Decision::Abort; }
        traceforge::sleep(self.b.delta * self.num_ps as u64);
        traceforge::send_msg(self.coord_id, CMsg::Ack);
        if maybe_crash(self.crashes) { return Decision::Abort; }
        loop {
            match self.recv() {
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return Decision::Commit;
                }
                Some(PMsg::Abort) => { self.state = PState::Aborted; return Decision::Abort; }
                Some(PMsg::MoveTo { backup, state }) => self.handle_move_to(backup, state),
                Some(_) => {}
                None => return self.terminate(),
            }
        }
    }
    fn terminate(&mut self) -> Decision {
        let mut current = 0u32;
        while current < self.num_ps {
            if current == self.my_index {
                return self.run_backup_protocol();
            }
            match self.wait_for_backup(current) {
                BackupOutcome::Decision(d) => return d,
                BackupOutcome::CurrentDead => current += 1,
            }
        }
        let d = self.decision_rule();
        self.state = match d { Decision::Commit => PState::Committed, _ => PState::Aborted };
        if d == Decision::Commit { traceforge::assert(self.voted_yes); }
        d
    }
    fn wait_for_backup(&mut self, current_idx: u32) -> BackupOutcome {
        let current_peer = self.peer_ids[current_idx as usize];
        loop {
            match self.recv() {
                Some(PMsg::MoveTo { backup, state }) => {
                    if backup == current_peer { self.handle_move_to(backup, state); }
                }
                Some(PMsg::Commit) => {
                    self.state = PState::Committed;
                    traceforge::assert(self.voted_yes);
                    return BackupOutcome::Decision(Decision::Commit);
                }
                Some(PMsg::Abort) => {
                    self.state = PState::Aborted;
                    return BackupOutcome::Decision(Decision::Abort);
                }
                Some(_) => {}
                None => return BackupOutcome::CurrentDead,
            }
        }
    }
    fn run_backup_protocol(&mut self) -> Decision {
        let decision = self.decision_rule();
        let need_phase1 = self.state != PState::Committed && self.state != PState::Aborted;
        if need_phase1 {
            let my_state = self.state;
            let me = self.me();
            for (j, p) in self.peer_ids.iter().enumerate() {
                if j as u32 != self.my_index {
                    if maybe_crash(self.crashes) { return decision; }
                    traceforge::send_msg(*p, PMsg::MoveTo { backup: me, state: my_state });
                }
            }
            let needed = self.peer_ids.len() - 1;
            for _ in 0..needed {
                if maybe_crash(self.crashes) { return decision; }
                traceforge::sleep(self.b.delta);
                match self.recv() {
                    Some(PMsg::BackupAck { .. }) => {}
                    Some(PMsg::Commit) => {
                        self.state = PState::Committed;
                        traceforge::assert(self.voted_yes);
                        return Decision::Commit;
                    }
                    Some(PMsg::Abort) => {
                        self.state = PState::Aborted;
                        return Decision::Abort;
                    }
                    Some(PMsg::MoveTo { backup, state }) => self.handle_move_to(backup, state),
                    Some(_) => {}
                    None => break,
                }
            }
        }
        if maybe_crash(self.crashes) { return decision; }
        let msg = match decision {
            Decision::Commit => PMsg::Commit,
            Decision::Abort => PMsg::Abort,
        };
        for (j, p) in self.peer_ids.iter().enumerate() {
            if j as u32 != self.my_index {
                if maybe_crash(self.crashes) { return decision; }
                traceforge::send_msg(*p, msg.clone());
            }
        }
        self.state = match decision {
            Decision::Commit => PState::Committed,
            Decision::Abort => PState::Aborted,
        };
        if decision == Decision::Commit { traceforge::assert(self.voted_yes); }
        decision
    }
}

fn slave(b: Bounds, num_ps: u32, index: u32, crashes: bool) {
    if maybe_crash(crashes) { return; }
    let (coord_id, peers) = loop {
        match traceforge::recv_msg_block::<PMsg>() {
            PMsg::Prepare { coord, peers } => break (coord, peers),
            _ => {}
        }
    };
    let mut ctx = SlaveCtx {
        b, my_index: index, num_ps, coord_id, peer_ids: peers,
        state: PState::Initial, voted_yes: false, crashes,
    };
    let _ = ctx.run_after_prepare();
}

// ==== Test entry ======================================================

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

    let crashes = crashes_from_env();
    let description = format!(
        "Full Skeen '81 3PC (N={}, with §5 termination protocol). \
         Election by preassigned ranking (lowest-index alive); reentrant \
         on backup failure. Backup runs two-phase: MoveTo(my state) + acks, \
         then Commit/Abort by Skeen decision rule on backup's own state. \
         Bounds: L=0, U=1, W=2 (= 2·U), DELTA=3. Crashes: {}.",
        NUM_PARTICIPANTS, crashes
    );
    // Spawn order in the verifier closure is: N slaves first (t1..tN),
    // then the coord (t{N+1}). t0 is main. Keep label mapping in sync
    // with that order — otherwise the rendered "→ coordinator" arrows
    // point at slave p0 instead of the actual coord thread.
    let coord_tid = format!("t{}", NUM_PARTICIPANTS + 1);
    let labels: Vec<(String, String)> = (0..NUM_PARTICIPANTS)
        .map(|i| (format!("t{}", i + 1), format!("p{}", i)))
        .collect();
    let labels_json = labels.iter().fold(serde_json::Map::new(), |mut m, (k, v)| {
        m.insert(k.clone(), serde_json::Value::String(v.clone()));
        m
    });
    let order: Vec<String> = std::iter::once(coord_tid.clone())
        .chain(labels.iter().map(|(k, _)| k.clone()))
        .chain(std::iter::once("t0".into()))
        .collect();
    let mut labels_full = labels_json;
    labels_full.insert("t0".to_string(), serde_json::Value::String("main".to_string()));
    labels_full.insert(coord_tid, serde_json::Value::String("coordinator".to_string()));
    // nondet_role tells the renderer whether NONDET events are votes
    // (single nondet per thread, legacy 2PC/3PC-without-crashes) or
    // crash decisions (one nondet per send/recv boundary, 3PC-with-
    // crashes). Without this flag the renderer falls back to a
    // per-execution count heuristic that mislabels threads that
    // crashed before reaching their second nondet.
    let nondet_role = if crashes { "crash" } else { "vote" };
    let meta_json = json!({
        "kind": "vote",
        "nondet_role": nondet_role,
        "title": format!("Three-Phase Commit (Skeen '81 + §5 termination, N={})", NUM_PARTICIPANTS),
        "description": description,
        "labels": labels_full,
        "order": order,
        "l": 0, "u": U, "sd": 0,
        "wait": format!("coord vote/ack={}, slave={}", W, W),
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
        for _r in 0..r_rounds {
            let mut handles = Vec::new();
            for i in 0..NUM_PARTICIPANTS {
                handles.push(thread::spawn(move || slave(b, NUM_PARTICIPANTS, i, crashes)));
            }
            let peers: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
            let c = thread::spawn(move || coordinator(b, crashes));
            traceforge::send_msg(c.thread().id(), CMsg::Init { peers });
            for h in handles { let _ = h.join(); }
            let _ = c.join();
        }
    });

    let _ = fs::remove_file(src);
}
