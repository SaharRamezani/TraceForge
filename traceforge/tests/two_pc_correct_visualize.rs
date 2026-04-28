//! Visualization-only helper test that captures every explored
//! execution of the *correct* 2PC into a viz_out/two_pc_correct/
//! directory.
//!
//! This is the structural twin of `two_pc_buggy_visualize.rs`: same
//! protocol skeleton, same temporal staggering except the
//! coordinator's decision rule is the *correct* unanimity check
//! (`yes_count == ps.len()` ⇒ Commit, otherwise Abort). With the
//! correct rule no `traceforge::assert(yes)` should ever fire, so the
//! viewer should show 8 passing execs and 0 FAIL pills, in contrast
//! to the buggy run's 3 FAILs at the same vote configurations.
//!
//! Output layout (relative to the cargo workspace root):
//!   viz_out/two_pc_correct/exec_NNN.dot     one per explored execution
//!   viz_out/two_pc_correct/meta.json        viewer header / labels
//!
//! After running, regenerate the HTML viewer with:
//!   python3 viz_out/build_html.py
//! Then open viz_out/index.html. The "Two-Phase Commit: correct" tab
//! sits next to the buggy one; clicking through the same vote
//! configurations side-by-side makes the bug very explicit.

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
    // CORRECT: commit iff *all* participants voted Yes (unanimity).
    let decision = if yes_count == ps.len() {
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
        // Same safety property as the buggy version. With the correct
        // unanimity rule above, this should never fire.
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
fn visualize_correct_2pc() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join("two_pc_correct");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    // Touch the prune log so the artifact always exists even when the
    // run records zero rejections (TraceForge only opens this file lazily
    // on the first prune).
    let _ = fs::write(&prune_log, b"");

    let description = format!(
        "Correct 2PC (N={}, temporal mode): coordinator commits iff every participant \
         voted Yes (unanimity). Same protocol skeleton, same temporal staggering as the \
         buggy run. Only the decision rule differs. Expect zero FAIL pills.",
        NUM_PARTICIPANTS
    );
    let meta_json = json!({
        "kind":        "vote",
        "title":       format!("Two-Phase Commit: correct (N={})", NUM_PARTICIPANTS),
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
        // Even though no assert should fire here, keep_going_after_error
        // is harmless and matches the buggy run's config so the two are
        // exactly comparable side-by-side.
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_dot_out(src.to_str().unwrap())
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
