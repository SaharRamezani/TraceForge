//! Three-Phase Commit (Skeen '81) with timeouts AND a deliberate
//! atomicity bug. Companion to `three_pc_temporal.rs`.
//!
//! Same protocol skeleton, same Skeen-grounded bounds (`L=0`, `U=Δ`,
//! `W = 2·Δ`, `DELTA = W + 1`); see that file's docstring for the
//! full bound algebra and citations.
//!
//! The bug: the coordinator advances to `PreCommit` (and ultimately
//! `Commit`) whenever a *majority* of received votes are Yes — instead
//! of requiring *all* N votes to be Yes AND all to have arrived. With
//! timeouts in play, even a single missing vote should force an abort
//! under correct 3PC; the buggy version proceeds anyway as long as
//! the majority of arrivals were Yes.
//!
//! When the model checker finds an interleaving where some participant
//! voted No but the buggy coordinator still committed, the
//! participant's `assert(yes)` on receiving Commit fires.
//!
//! Usage:
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3 --u 1 --w-ratio 2
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3 --crashes
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3 --replay

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, WaitTime};

const DEFAULT_PARTICIPANTS: u32 = 3;
/// Default Skeen Δ (upper transit bound). Lower bound `L = 0`.
const DEFAULT_U: u64 = 1;
/// Default Skeen ratio: `W = 2·U` (termination protocol bound).
const DEFAULT_W_RATIO: u64 = 2;

const DOT_FILE: &str = "/tmp/3pc_buggy.dot";
const BUG_FILE: &str = "/tmp/3pc_buggy.bug";

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

#[derive(Clone, Copy)]
struct Bounds {
    u: u64,
    w: u64,
    delta: u64,
}

impl Bounds {
    fn from_ratio(u: u64, w_ratio: u64) -> Self {
        assert!(u >= 1, "U must be >= 1 (L=0 < U)");
        assert!(w_ratio >= 2, "W/U ratio must be >= 2 (Skeen)");
        let w = u * w_ratio;
        Self { u, w, delta: w + 1 }
    }
}

fn coordinator(b: Bounds, num_ps: u32, crashes: bool) {
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
            Some(CoordinatorMsg::No) => {
                received_count += 1;
            }
            None => {}
            Some(_) => panic!("expected vote"),
        }
    }

    let _ = num_ps;

    // *** BUG ***
    // Correct (Skeen '81): PreCommit iff every participant voted Yes
    // AND every vote arrived. Equivalent to:
    //     received_count == ps.len() && yes_count == ps.len()
    // Buggy: PreCommit if a *majority* of received votes were Yes,
    // even if some votes timed out.
    let proceed = received_count > 0 && yes_count > received_count / 2;
    if !proceed {
        for id in &ps {
            traceforge::send_msg(*id, ParticipantMsg::Abort);
        }
        return;
    }

    if crashes && traceforge::nondet() {
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

fn participant(b: Bounds, num_ps: u32, index: u32, crashes: bool) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };

    if crashes && traceforge::nondet() {
        return;
    }

    traceforge::sleep(b.delta * (index as u64 + 1));

    let yes = traceforge::nondet();
    let vote = if yes {
        CoordinatorMsg::Yes
    } else {
        CoordinatorMsg::No
    };
    traceforge::send_msg(cid, vote);

    let action: ParticipantMsg = traceforge::recv_msg_block();

    match action {
        ParticipantMsg::Abort => return,
        ParticipantMsg::PreCommit => (),
        _ => panic!("expected PreCommit or Abort"),
    }

    if crashes && traceforge::nondet() {
        return;
    }

    traceforge::sleep(b.delta * num_ps as u64);
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    match action {
        // Atomicity: a participant that voted No must never observe
        // Commit. With the bug above this assertion can fire.
        ParticipantMsg::Commit => traceforge::assert(yes),
        _ => panic!("expected Commit"),
    }
}

fn parse_args() -> (u32, u64, u64, bool, bool) {
    let mut args = std::env::args().skip(1);
    let mut n = DEFAULT_PARTICIPANTS;
    let mut u = DEFAULT_U;
    let mut w_ratio = DEFAULT_W_RATIO;
    let mut crashes = false;
    let mut replay = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--participants" => {
                let v = args.next().expect("--participants requires a value");
                n = v.parse().expect("invalid --participants");
            }
            "--u" => {
                let v = args.next().expect("--u requires a value");
                u = v.parse().expect("invalid --u");
            }
            "--w-ratio" => {
                let v = args.next().expect("--w-ratio requires a value");
                w_ratio = v.parse().expect("invalid --w-ratio");
            }
            "--crashes" => crashes = true,
            "--replay" => replay = true,
            other => panic!("unknown argument: {other}"),
        }
    }
    (n, u, w_ratio, crashes, replay)
}

fn scenario(b: Bounds, num_ps: u32, crashes: bool) -> impl Fn() + Send + Sync + 'static {
    move || {
        let c = thread::spawn(move || coordinator(b, num_ps, crashes));
        let mut ps = Vec::new();
        for i in 0..num_ps {
            ps.push(thread::spawn(move || participant(b, num_ps, i, crashes)));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    }
}

fn main() {
    let (num_ps, u, w_ratio, crashes, replay) = parse_args();
    assert!(num_ps >= 2, "need >= 2 participants for the bug to fire");
    let b = Bounds::from_ratio(u, w_ratio);

    if replay {
        println!("Replaying captured counterexample from {BUG_FILE}");
        traceforge::replay(scenario(b, num_ps, crashes), BUG_FILE);
        return;
    }

    println!("Running buggy 3PC verification (Skeen '81 skeleton, broken decision rule)");
    println!("  N           = {num_ps}");
    println!("  L=0  U={}  W={} (= {}·U)  DELTA={}", b.u, b.w, w_ratio, b.delta);
    println!("  source: Skeen '81 termination bound; report § 4.2");
    println!("Coordinator commits on majority instead of unanimity — bug witness fires.");
    if crashes {
        println!("--crashes: coordinator/participants may crash mid-protocol.");
    }
    println!("Artifacts on counterexample:");
    println!("  DOT graph  -> {DOT_FILE}");
    println!("  replay log -> {BUG_FILE}");
    println!();

    let cfg = Config::builder()
        .with_temporal(0, b.u, 0)
        .with_dot_out(DOT_FILE)
        .with_error_trace(BUG_FILE)
        .with_verbose(1)
        .build();

    traceforge::verify(cfg, scenario(b, num_ps, crashes));
}
