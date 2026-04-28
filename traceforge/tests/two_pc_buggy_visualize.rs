//! Visualization-only helper test that captures every explored
//! execution of the *buggy* 2PC into a viz_out/two_pc_buggy/ directory.
//!
//! The bug mirrors `examples/two_pc_temporal_buggy.rs`: the coordinator
//! commits whenever a *majority* of votes are Yes (instead of *all*),
//! so any participant that voted No but received Commit trips
//! `traceforge::assert(yes)`.
//!
//! With N=3 + temporal mode + `keep_going_after_error(true)` the model
//! checker explores exactly 2^N = 8 vote configurations; 3 of them are
//! bug-witnesses. Temporal pruning collapses the N! ordering factor so
//! each exec is a clean "this vote configuration commits/aborts" graph
//! instead of one of N! interleavings of the same vote configuration.
//!
//! Output layout (relative to the cargo workspace root):
//!   viz_out/two_pc_buggy/exec_NNN.dot     one per explored execution
//!   viz_out/two_pc_buggy/meta.json        viewer header / description
//!
//! After running, regenerate the HTML viewer with:
//!   python3 viz_out/build_html.py
//! Then open viz_out/index.html. Failing executions are flagged with a
//! red `FAIL` pill in the sidebar.

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_PARTICIPANTS: u32 = 3;
const DELTA: u64 = 5;

#[derive(Clone, Debug, PartialEq)]
enum CoordinatorMsg {
    Init(Vec<ThreadId>),
    Yes,
    No,
}

#[derive(Clone, Debug, PartialEq)]
enum ParticipantMsg {
    Prepare(ThreadId),
    Commit,
    Abort,
}

fn coordinator() {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CoordinatorMsg>() {
        CoordinatorMsg::Init(ids) => ids,
        _ => panic!("expected Init"),
    };
    let me = thread::current().id();
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Prepare(me));
    }
    let mut yes_count = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(DELTA);
        let v: CoordinatorMsg = traceforge::recv_msg_block_timed();
        match v {
            CoordinatorMsg::Yes => yes_count += 1,
            CoordinatorMsg::No => (),
            _ => panic!("expected vote"),
        }
    }
    // *** BUG ***  majority instead of unanimity.
    let decision = if yes_count > ps.len() / 2 {
        ParticipantMsg::Commit
    } else {
        ParticipantMsg::Abort
    };
    for id in &ps {
        traceforge::send_msg(*id, decision.clone());
    }
}

fn participant(index: u32) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };
    // Stagger so temporal pruning forces a unique vote-arrival order at
    // the coordinator. Each exec then captures exactly one Y/N
    // configuration instead of N! permutations of it.
    traceforge::sleep(DELTA * (index as u64 + 1));
    let yes = traceforge::nondet();
    traceforge::send_msg(
        cid,
        if yes {
            CoordinatorMsg::Yes
        } else {
            CoordinatorMsg::No
        },
    );
    let action: ParticipantMsg = traceforge::recv_msg_block();
    if matches!(action, ParticipantMsg::Commit) {
        // Bug-witness assertion. With keep_going_after_error this blocks
        // the thread (BLK Assert in the graph) and search continues.
        traceforge::assert(yes);
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

#[test]
fn visualize_buggy_2pc() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join("two_pc_buggy");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    // Touch the prune log so the artifact always exists even when the
    // run records zero rejections (TraceForge only opens this file lazily
    // on the first prune).
    let _ = fs::write(&prune_log, b"");

    // Self-describing meta.json: build_html.py reads `kind`, `labels`,
    // `order` from here, so adding more 2PC variants (or entirely
    // different protocols) doesn't require any Python changes.
    let description = format!(
        "Buggy 2PC (N={}, temporal mode): coordinator commits on majority instead of \
         unanimity. Temporal pruning collapses the N! ordering factor, so each \
         exec is one of the 2^N vote configurations. Failing execs (the participant \
         voted No but received Commit) are flagged with a red FAIL pill.",
        NUM_PARTICIPANTS
    );
    let meta_json = json!({
        "kind":        "vote",
        "title":       format!("Two-Phase Commit: buggy (N={})", NUM_PARTICIPANTS),
        "description": description,
        "labels": {
            "t0": "main",
            "t1": "coordinator",
            "t2": "p0",
            "t3": "p1",
            "t4": "p2",
        },
        "order": ["t1", "t2", "t3", "t4", "t0"],
        "l": 0, "u": 1, "sd": 0, "wait": "inf",
    })
    .to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_temporal(0, 1, 0)
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_dot_out(src.to_str().unwrap())
        // Capture the dot for blocked execs too so the viewer can show
        // the partial graph at the moment the run got stuck (otherwise
        // those slots would show "no graph captured" placeholders).
        .with_dot_out_blocked(true)
        // Emit one JSONL line per rejected rf (empty interval or symmetric
        // duplicate). build_html.py reads this to populate the Pruned tab.
        .with_prune_log(prune_log.to_str().unwrap())
        .with_callback(Box::new(DotSnapshotter {
            src: src.clone(),
            dir: dir.clone(),
        }))
        .build();

    traceforge::verify(cfg, move || {
        let c = thread::spawn(coordinator);
        let mut ps = Vec::new();
        for i in 0..NUM_PARTICIPANTS {
            ps.push(thread::spawn(move || participant(i)));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    });

    let _ = fs::remove_file(src);
}
