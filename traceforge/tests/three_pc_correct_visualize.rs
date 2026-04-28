//! Visualization-only helper test that captures every explored
//! execution of the *correct* 3PC into a viz_out/three_pc_correct/
//! directory. Mirrors the timeout-aware structure of the original 3PC
//! reference: every recv that can legitimately fail to deliver uses
//! `recv_msg_timed(WaitTime::Finite(W))` and treats `None` as a
//! timeout, so the viewer captures both "successful read" and "timeout"
//! branches the model checker explores.
//!
//! Structural twin of `three_pc_buggy_visualize.rs`: same protocol
//! skeleton, same temporal staggering, same wait windows; the only
//! difference is the coordinator's decision rule (unanimity vs.
//! majority).
//!
//! Output layout (relative to the cargo workspace root):
//!   viz_out/three_pc_correct/exec_NNN.dot     one per explored execution
//!   viz_out/three_pc_correct/meta.json        viewer header / labels
//!   viz_out/three_pc_correct/prunes.jsonl     temporal prune log
//!
//! Run end-to-end:
//!   cargo test --release --test three_pc_correct_visualize
//!   python3 viz_out/build_html.py
//! Then open viz_out/index.html.

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_PARTICIPANTS: u32 = 3;
const DELTA: u64 = 5;
// Coord vote/ack receive wait. Must be < DELTA so cross-mappings
// (recv k reading from participant j's send, j != k+1) get pruned by
// the empty-interval rule.
const VOTE_WAIT: u64 = DELTA - 1;
// Participants use `recv_msg_block_timed` (W_r=∞). Finite-wait
// participant recvs would advance the participant's local clock to the
// actual PreCommit arrival time, which collapses the (i+1)*DELTA
// staggering and makes ALL acks land at the coordinator at the same
// clock — every ack rf becomes admissible for every recv, so cross-
// mappings stop getting pruned and the bug-witness path stops being
// reachable. With W_r=∞, the participant's local clock stays at its
// predicate clock, the staggered acks sail through, and the buggy
// coordinator's all-acks-received → Commit branch surfaces.

#[derive(Clone, Debug, PartialEq)]
enum CoordinatorMsg {
    Init(Vec<ThreadId>),
    Yes,
    No,
    Ack,
}

#[derive(Clone, Debug, PartialEq)]
enum ParticipantMsg {
    Prepare(ThreadId),
    PreCommit,
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
    let mut received_count = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(DELTA);
        let v: Option<CoordinatorMsg> = traceforge::recv_msg_timed(WaitTime::Finite(VOTE_WAIT));
        match v {
            Some(CoordinatorMsg::Yes) => {
                received_count += 1;
                yes_count += 1;
            }
            Some(CoordinatorMsg::No) => received_count += 1,
            None => {} // vote timed out — treated as "not yes" below
            Some(_) => panic!("expected vote"),
        }
    }

    // CORRECT: PreCommit iff every participant voted Yes AND every vote
    // arrived. A timeout on any recv → not unanimous → abort.
    let unanimous = received_count == ps.len() && yes_count == ps.len();
    if !unanimous {
        for id in &ps {
            traceforge::send_msg(*id, ParticipantMsg::Abort);
        }
        return;
    }

    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::PreCommit);
    }

    let mut acks_received = 0usize;
    for _ in 0..ps.len() {
        traceforge::sleep(DELTA);
        let v: Option<CoordinatorMsg> = traceforge::recv_msg_timed(WaitTime::Finite(VOTE_WAIT));
        match v {
            Some(CoordinatorMsg::Ack) => acks_received += 1,
            None => {} // ack timed out
            Some(_) => panic!("expected ack"),
        }
    }

    if acks_received != ps.len() {
        // Missing ack: don't commit. Participants past PreCommit will
        // time out waiting for Commit and exit cleanly.
        return;
    }

    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Commit);
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

    // Plain `recv_msg_block` (untimed) — deliberately *not*
    // `recv_msg_block_timed`. The block_timed variant advances the
    // participant's local clock to the actual rf-window time on
    // completion, which converges all participants to the same
    // PreCommit-arrival τ and erases the (i+1)*DELTA staggering. The
    // untimed variant leaves the clock alone, so each participant's
    // ack-send τ stays at the canonical (N+i+1)*DELTA value the
    // coordinator's ack receives are tuned for.
    let action: ParticipantMsg = traceforge::recv_msg_block();

    match action {
        ParticipantMsg::Abort => return,
        ParticipantMsg::PreCommit => (),
        _ => panic!("expected PreCommit or Abort"),
    }

    // Coord's k-th ack recv runs at predicate clock (N+k)*DELTA, so each
    // participant must sleep N*DELTA here (not (i+1)*DELTA) to land at
    // (N+i+1)*DELTA, matching the canonical k = i+1 pairing.
    traceforge::sleep(DELTA * NUM_PARTICIPANTS as u64);
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    // Plain `recv_msg_block` (untimed) — deliberately *not*
    // `recv_msg_block_timed`. The block_timed variant advances the
    // participant's local clock to the actual rf-window time on
    // completion, which converges all participants to the same
    // PreCommit-arrival τ and erases the (i+1)*DELTA staggering. The
    // untimed variant leaves the clock alone, so each participant's
    // ack-send τ stays at the canonical (N+i+1)*DELTA value the
    // coordinator's ack receives are tuned for.
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
fn visualize_correct_3pc() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join("three_pc_correct");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    // Touch the prune log so the artifact always exists even when the
    // run records zero rejections (TraceForge only opens this file lazily
    // on the first prune).
    let _ = fs::write(&prune_log, b"");

    let description = format!(
        "Correct 3PC (N={}, temporal mode, with timeouts): every recv that can \
         legitimately fail uses recv_msg_timed(WaitTime::Finite). The coordinator \
         requires both unanimity AND all-arrived to PreCommit; a missing ack \
         leaves the round uncommitted. Expect zero FAIL pills.",
        NUM_PARTICIPANTS
    );
    let meta_json = json!({
        "kind":        "vote",
        "title":       format!("Three-Phase Commit: correct (timeouts, N={})", NUM_PARTICIPANTS),
        "description": description,
        "labels": {
            "t0": "main",
            "t1": "coordinator",
            "t2": "p0",
            "t3": "p1",
            "t4": "p2",
        },
        "order": ["t1", "t2", "t3", "t4", "t0"],
        "l": 0, "u": 1, "sd": 0,
        "wait": format!("coord vote/ack={}, participant=∞", VOTE_WAIT),
    })
    .to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_temporal(0, 1, 0)
        .with_keep_going_after_error(true)
        .with_verbose(1)
        .with_dot_out(src.to_str().unwrap())
        .with_dot_out_blocked(true)
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
