//! Side-by-side comparison of MUST-τ vs. MUST on the 3PC scenario,
//! using Skeen-grounded temporal bounds and a sweep over `W/U`.
//!
//! Source for bounds: D. Skeen, "NonBlocking Commit Protocols",
//! SIGMOD '81. The termination protocol bound `W = 2·Δ` is taken
//! verbatim from the paper; `L = 0` and `sd = 0` follow the report's
//! recommendation (`benchmarks_decision.md` § 4.2) — the pruning power
//! comes from `U` being finite, not from `L > 0`.
//!
//! We deliberately *sweep* the `W/U` ratio rather than reporting a
//! single point: a single ratio is suspect ("did you cherry-pick
//! that?"), a curve is not. The eval section in the thesis takes the
//! `× exec` column below as evidence that MUST-τ's reduction scales
//! with timing-constraint strength rather than with a hand-picked
//! parameter.
//!
//! The sweep is `W/U ∈ {2, 3, 5}`. The report recommends `{1.5, 2,
//! 3, 5}`; we drop 1.5 because `W` is integer-valued in the model and
//! `2·U` is the next integer above `1·U` (and is also Skeen's canonical
//! ratio).
//!
//! Run:
//!   cargo test --release --test three_pc_compare -- --nocapture

use std::time::Instant;

use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NUM_PARTICIPANTS: u32 = 3;
/// Upper transit bound. Lower bound `L = 0`. This is Skeen's Δ.
const U: u64 = 1;
/// Skeen ratios swept for honest "show the curve, not the point"
/// reporting. The report recommends `{1.5, 2, 3, 5}`; we drop 1.5
/// because `W` is integer-valued in the model, and add `7` so the
/// sweep crosses the pruning cliff (`W ≥ DELTA`) — the "loose" regime
/// the report explicitly asks us to include so reviewers can see the
/// reduction degrade gracefully rather than being shown only the
/// headline-friendly tight-regime numbers.
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];

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

/// Bound bundle. `delta` is held fixed across the sweep so each
/// ratio is verified on the same encoded scenario; that is what makes
/// the reduction-vs-W curve attributable to the temporal filter
/// rather than to a changing staggering structure (see
/// examples/three_pc_temporal.rs for the algebra).
#[derive(Clone, Copy)]
struct Bounds {
    u: u64,
    w: u64,
    delta: u64,
}

impl Bounds {
    fn with_delta(u: u64, w_ratio: u64, delta: u64) -> Self {
        Self {
            u,
            w: u * w_ratio,
            delta,
        }
    }
}

fn coordinator_correct(b: Bounds) {
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
        traceforge::sleep(b.delta);
        let v: Option<CoordinatorMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
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

    // Skeen's unanimity: every vote is Yes AND every vote arrived.
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
        traceforge::sleep(b.delta);
        let v: Option<CoordinatorMsg> = traceforge::recv_msg_timed(WaitTime::Finite(b.w));
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

fn participant(b: Bounds, num_ps: u32, index: u32) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };
    traceforge::sleep(b.delta * (index as u64 + 1));
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

    traceforge::sleep(b.delta * num_ps as u64);
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    if matches!(action, ParticipantMsg::Commit) {
        traceforge::assert(yes);
    }
}

fn build_config(b: Bounds, use_temporal: bool) -> Config {
    let mut builder = Config::builder()
        .with_keep_going_after_error(true)
        .with_verbose(0);
    if use_temporal {
        builder = builder.with_temporal(0, b.u, 0);
    }
    builder.build()
}

fn run_3pc(b: Bounds, use_temporal: bool) -> (Stats, std::time::Duration) {
    let cfg = build_config(b, use_temporal);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let c = thread::spawn(move || coordinator_correct(b));
        let mut ps = Vec::new();
        for i in 0..NUM_PARTICIPANTS {
            ps.push(thread::spawn(move || participant(b, NUM_PARTICIPANTS, i)));
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
    // Hold DELTA fixed across every row so the reduction-vs-W curve is
    // attributable to the temporal filter, not to a changing staggering
    // structure. We pick DELTA at the middle of the swept ratios so the
    // sweep visibly crosses the pruning cliff (W < DELTA tight regime;
    // W ≥ DELTA loose regime where cross-mappings start surviving).
    // Per report § 4.2, the "loose" rows are what proves the reduction
    // is real and not a cherry-picked headline number.
    let mid_idx = SWEEP_RATIOS.len() / 2;
    let delta = U * SWEEP_RATIOS[mid_idx];

    println!();
    println!("=== 3PC (Skeen '81), N={NUM_PARTICIPANTS}, L=0, U={U}, DELTA={delta} (fixed) ===");
    println!("Sweep over W/U (Skeen termination bound is 2·Δ; sweep keeps it honest)");
    println!();
    println!(
        "{:<6} {:<4} {:<6} {:>8} {:>8} {:>8} {:>8} {:>10} {:>8} {:>8}",
        "ratio", "W", "regime", "tmp.exe", "orig.exe", "tmp.blk", "orig.blk", "wall.tmp", "wall.or", "× exec"
    );

    for &r in SWEEP_RATIOS {
        let b = Bounds::with_delta(U, r, delta);

        let (s_temp, d_temp) = run_3pc(b, true);
        let (s_orig, d_orig) = run_3pc(b, false);

        let regime = if b.w < delta { "tight" } else { "loose" };
        let exec_ratio = s_orig.execs as f64 / s_temp.execs.max(1) as f64;
        println!(
            "{:<6} {:<4} {:<6} {:>8} {:>8} {:>8} {:>8} {:>9.2}s {:>7.2}s {:>7.2}x",
            r,
            b.w,
            regime,
            s_temp.execs,
            s_orig.execs,
            s_temp.block,
            s_orig.block,
            d_temp.as_secs_f64(),
            d_orig.as_secs_f64(),
            exec_ratio,
        );
    }
    println!();
    println!("Source: Skeen '81 termination protocol bound (W = 2·Δ, base case).");
    println!("Sweep variable per benchmark report § 4.2: W/U ∈ {{2, 3, 5, 7}}.");
    println!("DELTA held fixed so the reduction is comparable across rows.");
}
