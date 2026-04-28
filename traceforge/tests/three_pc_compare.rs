//! Side-by-side comparison of the temporal-aware Must-τ algorithm
//! vs. the original (non-temporal) Must algorithm on the 3PC scenario.
//!
//! Builds the same protocol skeleton used by the visualize tests, then
//! runs `traceforge::verify` twice — once with `with_temporal(0, 1, 0)`
//! and once without it — and prints a summary table:
//!
//!   - execs   : number of complete executions explored
//!   - block   : number of blocked executions explored
//!   - wall    : wall-clock seconds for that run
//!
//! Run:
//!   cargo test --release --test three_pc_compare -- --nocapture
//!
//! The exec counts will differ. Without temporal, every Finite-wait recv
//! contributes both an rf=Some(s) and rf=None branch unconditionally;
//! with temporal, the rf candidates whose interval is empty are pruned
//! up front, shrinking the worklist.

use std::time::Instant;

use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_PARTICIPANTS: u32 = 3;
const DELTA: u64 = 5;
const VOTE_WAIT: u64 = DELTA - 1;

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

fn coordinator_correct() {
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
    match action {
        ParticipantMsg::Abort => return,
        ParticipantMsg::PreCommit => (),
        _ => panic!("expected PreCommit or Abort"),
    }

    traceforge::sleep(DELTA * NUM_PARTICIPANTS as u64);
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    if matches!(action, ParticipantMsg::Commit) {
        traceforge::assert(yes);
    }
}

fn build_config(use_temporal: bool) -> Config {
    let mut b = Config::builder()
        .with_keep_going_after_error(true)
        .with_verbose(0);
    if use_temporal {
        b = b.with_temporal(0, 1, 0);
    }
    b.build()
}

fn run_3pc(use_temporal: bool) -> (Stats, std::time::Duration) {
    let cfg = build_config(use_temporal);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let c = thread::spawn(coordinator_correct);
        let mut ps = Vec::new();
        for i in 0..NUM_PARTICIPANTS {
            ps.push(thread::spawn(move || participant(i)));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    });
    (stats, t0.elapsed())
}

#[test]
fn compare_3pc_temporal_vs_original() {
    println!();
    println!(
        "=== 3PC correct, N={} participants, DELTA={}, VOTE_WAIT={} ===",
        NUM_PARTICIPANTS, DELTA, VOTE_WAIT
    );

    let (s_temp, d_temp) = run_3pc(true);
    println!(
        "with_temporal(0,1,0): execs={:>6}  block={:>6}  wall={:>7.2}s",
        s_temp.execs,
        s_temp.block,
        d_temp.as_secs_f64()
    );

    let (s_orig, d_orig) = run_3pc(false);
    println!(
        "without temporal   : execs={:>6}  block={:>6}  wall={:>7.2}s",
        s_orig.execs,
        s_orig.block,
        d_orig.as_secs_f64()
    );

    let exec_ratio = s_orig.execs as f64 / s_temp.execs.max(1) as f64;
    let time_ratio = d_orig.as_secs_f64() / d_temp.as_secs_f64().max(1e-9);
    println!();
    println!(
        "ratio (orig / temporal): execs x{:.2}, wall x{:.2}",
        exec_ratio, time_ratio
    );
}
