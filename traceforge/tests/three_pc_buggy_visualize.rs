//! Visualization-only helper test that captures every explored
//! execution of a *buggy* 3PC variant (Skeen '81 skeleton, broken
//! decision rule) into viz_out/three_pc_buggy/. Mirrors the
//! timeout-aware structure of the original 3PC reference: every recv
//! that can legitimately fail to deliver uses
//! `recv_msg_timed(WaitTime::Finite(W))` and treats `None` as a
//! timeout.
//!
//! The bug mirrors `examples/three_pc_temporal_buggy.rs`: the
//! coordinator advances to PreCommit (and ultimately Commit) whenever a
//! *majority of received* votes are Yes, instead of requiring *all*
//! votes to be Yes AND all to have arrived. With timeouts this is even
//! more dangerous — Skeen's atomicity argument requires a single missed
//! vote to force abort, but the buggy coordinator may still commit on
//! the survivors.
//!
//! Bound choice: identical to `three_pc_correct_visualize.rs` so the
//! reduction comparison is apples-to-apples. See that file (and
//! `benchmarks_decision.md` § 4.2) for the algebra.
//!
//! Output layout (relative to the cargo workspace root):
//!   viz_out/three_pc_buggy/exec_NNN.dot     one per explored execution
//!   viz_out/three_pc_buggy/meta.json        viewer header / description
//!   viz_out/three_pc_buggy/prunes.jsonl     temporal prune log
//!
//! Run end-to-end:
//!   cargo test --release --test three_pc_buggy_visualize
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
// Skeen '81 termination protocol bounds (see correct-variant
// visualizer for the full algebra).
const U: u64 = 1;
const VOTE_WAIT: u64 = 2 * U; // W = 2·Δ (Skeen termination bound)
const DELTA: u64 = VOTE_WAIT + 1; // minimum stagger for cross-mapping pruning
// Participants use `recv_msg_block_timed` (W_r=∞). Finite-wait
// participant recvs would advance the participant's local clock to the
// actual PreCommit arrival time, which collapses the staggering and
// would defeat the temporal pruning of cross-mapped acks. With W_r=∞,
// the staggered acks survive intact and the buggy coordinator's
// all-acks-received → Commit branch becomes reachable so the bug
// witness fires.

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
            None => {}
            Some(_) => panic!("expected vote"),
        }
    }

    // *** BUG ***  Majority of received votes (ignoring timeouts and
    // ignoring "all received" requirement) instead of strict unanimity.
    let proceed = received_count > 0 && yes_count > received_count / 2;
    if !proceed {
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
            None => {}
            Some(_) => panic!("expected ack"),
        }
    }

    if acks_received != ps.len() {
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
    // Stagger so temporal pruning forces a unique vote-arrival order at
    // the coordinator. Each surviving exec then captures exactly one
    // canonical Y/N pattern (modulo per-recv timeouts).
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

    // Re-stagger for the ack phase so temporal pruning also pins the
    // ack-arrival order. Predicate clock at ack send is (N+i+1)*DELTA.
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
fn visualize_buggy_3pc() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join("three_pc_buggy");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    // Touch the prune log so the artifact always exists even when the
    // run records zero rejections (TraceForge only opens this file lazily
    // on the first prune).
    let _ = fs::write(&prune_log, b"");

    let description = format!(
        "Buggy 3PC (N={}, temporal mode, with timeouts): coordinator commits on \
         majority of received votes (ignoring timeouts and unanimity). Failing \
         execs (a participant voted No but received Commit) are flagged with a \
         red FAIL pill.",
        NUM_PARTICIPANTS
    );
    let meta_json = json!({
        "kind":        "vote",
        "title":       format!("Three-Phase Commit: buggy (timeouts, N={})", NUM_PARTICIPANTS),
        "description": description,
        "labels": {
            "t0": "main",
            "t1": "coordinator",
            "t2": "p0",
            "t3": "p1",
            "t4": "p2",
        },
        "order": ["t1", "t2", "t3", "t4", "t0"],
        "l": 0, "u": U, "sd": 0,
        "wait": format!("coord vote/ack={} (= 2·U, Skeen '81), participant=∞", VOTE_WAIT),
    })
    .to_string();
    fs::write(dir.join("meta.json"), meta_json).unwrap();

    let cfg = Config::builder()
        .with_temporal(0, U, 0)
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
