//! Visualization-only test that captures every explored execution of
//! the basic 3PC (Skeen Figure 7, no termination/backup protocol)
//!
//! Configurable rounds via `THREE_PC_ROUNDS` (default 1).
//!
//! Configurable failures via `THREE_PC_CRASHES=1`: enables fail-stop
//! crash points at every send/recv boundary on the coord and every
//! participant. Default OFF.
//!
//! Run:
//!   cargo run --release --example three_pc_visualize
//!   THREE_PC_ROUNDS=2 THREE_PC_CRASHES=1 cargo run --release --example three_pc_visualize
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

#[derive(Clone, Debug, PartialEq)]
enum PMsg {
    Prepare   { round: u32, coord: ThreadId },
    PreCommit { round: u32 },
    Commit    { round: u32 },
    Abort     { round: u32 },
}

#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    Init { peers: Vec<ThreadId> },
    Yes  { round: u32 },
    No   { round: u32 },
    Ack  { round: u32 },
}

#[derive(Clone, Copy, Debug)]
struct Bounds { u: u64, w: u64, delta: u64 }

fn maybe_crash(crashes: bool) -> bool {
    crashes && traceforge::nondet()
}

// ==== Coordinator =====================================================

fn coordinator(b: Bounds, crashes: bool, rounds: u32) {
    let ps: Vec<ThreadId> = loop {
        match traceforge::recv_msg_block::<CMsg>() {
            CMsg::Init { peers } => break peers,
            _ => {}
        }
    };
    let me = thread::current().id();

    for round in 0..rounds {
        if maybe_crash(crashes) { return; }

        for id in &ps {
            traceforge::send_msg(*id, PMsg::Prepare { round, coord: me });
            if maybe_crash(crashes) { return; }
        }

        let mut yes = 0usize;
        let mut got = 0usize;
        for _ in 0..ps.len() {
            traceforge::sleep(b.delta);
            let vote = loop {
                match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
                    Some(CMsg::Yes { round: r }) if r == round => break Some(true),
                    Some(CMsg::No  { round: r }) if r == round => break Some(false),
                    Some(_) => {}
                    None => break None,
                }
            };
            match vote {
                Some(true)  => { got += 1; yes += 1; }
                Some(false) => { got += 1; }
                None => {}
            }
            if maybe_crash(crashes) { return; }
        }
        if got != ps.len() || yes != ps.len() {
            for id in &ps {
                if maybe_crash(crashes) { return; }
                traceforge::send_msg(*id, PMsg::Abort { round });
            }
            continue;
        }
        if maybe_crash(crashes) { return; }

        for id in &ps {
            traceforge::send_msg(*id, PMsg::PreCommit { round });
            if maybe_crash(crashes) { return; }
        }

        let mut acks = 0usize;
        for _ in 0..ps.len() {
            traceforge::sleep(b.delta);
            let ack = loop {
                match traceforge::recv_msg_timed::<CMsg>(WaitTime::Finite(b.w)) {
                    Some(CMsg::Ack { round: r }) if r == round => break Some(()),
                    Some(_) => {}
                    None => break None,
                }
            };
            if ack.is_some() { acks += 1; }
            if maybe_crash(crashes) { return; }
        }
        if acks != ps.len() { continue; }
        if maybe_crash(crashes) { return; }

        for id in &ps {
            traceforge::send_msg(*id, PMsg::Commit { round });
            if maybe_crash(crashes) { return; }
        }
    }
}

// ==== Participant =====================================================

fn participant(b: Bounds, num_ps: u32, index: u32, crashes: bool, rounds: u32) {
    if maybe_crash(crashes) { return; }
    let mut dead = false;
    for round in 0..rounds {
        if dead { continue; }

        let coord_id = loop {
            match traceforge::recv_msg_timed::<PMsg>(WaitTime::Finite(b.w)) {
                Some(PMsg::Prepare { round: r, coord }) if r == round => break Some(coord),
                Some(_) => {}
                None => break None,
            }
        };
        let coord_id = match coord_id {
            Some(c) => c,
            None => { dead = true; continue; }
        };

        if maybe_crash(crashes) { return; }
        traceforge::sleep(b.delta * (index as u64 + 1));

        let voted_yes: bool = traceforge::nondet();
        let vote = if voted_yes { CMsg::Yes { round } } else { CMsg::No { round } };
        if maybe_crash(crashes) { return; }
        traceforge::send_msg(coord_id, vote);
        if !voted_yes { continue; }

        if maybe_crash(crashes) { return; }

        let phase2 = loop {
            match traceforge::recv_msg_timed::<PMsg>(WaitTime::Finite(b.w)) {
                Some(PMsg::PreCommit { round: r }) if r == round => break Some(true),
                Some(PMsg::Abort     { round: r }) if r == round => break Some(false),
                Some(_) => {}
                None => break None,
            }
        };
        match phase2 {
            Some(true) => {}
            Some(false) => continue,
            None => { dead = true; continue; }
        }

        if maybe_crash(crashes) { return; }
        traceforge::sleep(b.delta * num_ps as u64);
        traceforge::send_msg(coord_id, CMsg::Ack { round });
        if maybe_crash(crashes) { return; }

        let phase3 = loop {
            match traceforge::recv_msg_timed::<PMsg>(WaitTime::Finite(b.w)) {
                Some(PMsg::Commit { round: r }) if r == round => break Some(true),
                Some(PMsg::Abort  { round: r }) if r == round => break Some(false),
                Some(_) => {}
                None => break None,
            }
        };
        match phase3 {
            Some(true) => { traceforge::assert(voted_yes); }
            Some(false) => {}
            None => { dead = true; continue; }
        }
    }
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

fn main() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest).join("..").join("viz_out").join("three_pc_correct");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    let _ = fs::write(&prune_log, b"");

    let crashes = crashes_from_env();
    let r_rounds = rounds_from_env();
    let description = format!(
        "Basic 3PC (N={}, R={}). Threads are long-lived: the coordinator \
         and N participants are spawned once and loop over R rounds. Every \
         message carries a round id; each recv applies a round-id filter so \
         cross-round arrivals are silently dropped. Bounds: L=0, U=1, W=2 \
         (= 2·U), DELTA=3. Crashes: {}.",
        NUM_PARTICIPANTS, r_rounds, crashes
    );
    // Spawn order in the verifier closure is: N participants first
    // (t1..tN), then the coord (t{N+1}). t0 is main. Keep label mapping
    // in sync with that order; otherwise the rendered "→ coordinator"
    // arrows point at participant p0 instead of the actual coord thread.
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
    let nondet_role = if crashes { "crash" } else { "vote" };
    let meta_json = json!({
        "kind": "vote",
        "nondet_role": nondet_role,
        "title": format!("Three-Phase Commit (basic, N={}, R={})", NUM_PARTICIPANTS, r_rounds),
        "description": description,
        "labels": labels_full,
        "order": order,
        "l": 0, "u": U, "sd": 0,
        "wait": format!("coord vote/ack={}, participant={}", W, W),
    }).to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_timed(0, U, 0)
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_progress_report(usize::MAX)
        .with_dot_out(src.to_str().unwrap())
        .with_dot_out_blocked(true)
        .with_prune_log(prune_log.to_str().unwrap())
        .with_callback(Box::new(DotSnapshotter { src: src.clone(), dir: dir.clone() }))
        .build();

    let b = Bounds { u: U, w: W, delta: DELTA };
    traceforge::verify(cfg, move || {
        let mut handles = Vec::new();
        for i in 0..NUM_PARTICIPANTS {
            handles.push(thread::spawn(move || participant(b, NUM_PARTICIPANTS, i, crashes, r_rounds)));
        }
        let peers: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        let c = thread::spawn(move || coordinator(b, crashes, r_rounds));
        traceforge::send_msg(c.thread().id(), CMsg::Init { peers });
        for h in handles { let _ = h.join(); }
        let _ = c.join();
    });

    let _ = fs::remove_file(src);
}
