//! Three-Phase Commit with timeouts AND a deliberate decision-rule bug.
//!
//! Same skeleton as `three_pc_temporal.rs`: receives that may
//! legitimately fail to deliver use `recv_msg_timed(WaitTime::Finite)`
//! and treat `None` as a timeout. Crashes are gated on `--crashes`.
//!
//! The bug: the coordinator advances to `PreCommit` (and ultimately
//! `Commit`) whenever a *majority* of received votes are Yes — instead
//! of requiring *all* N votes to be Yes. With timeouts in play, even a
//! single missing vote should force an abort under correct 3PC; the
//! buggy version proceeds anyway as long as the majority of arrivals
//! were Yes.
//!
//! When the model checker finds an interleaving where some participant
//! voted No but the buggy coordinator still committed, the
//! participant's `assert!(yes)` on receiving Commit fires.
//!
//! Usage:
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3 --crashes
//!     cargo run --release --example three_pc_temporal_buggy -- --participants 3 --replay

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, WaitTime};

const DEFAULT_PARTICIPANTS: u32 = 3;
const DEFAULT_DELTA: u64 = 5;

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

fn vote_wait(delta: u64) -> u64 {
    delta - 1
}

// Participant receives are deliberately *untimed* (`recv_msg_block`)
// — the block_timed variant would advance the participant's local
// clock to the rf-window time and collapse the (i+1)*delta staggering
// the temporal filter relies on. Crash-driven "timeouts" are modeled
// via the `crashes && nondet() return` short-circuit before each
// receive, mirroring the original 3PC reference.

fn coordinator(delta: u64, num_ps: u32, crashes: bool) {
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
        traceforge::sleep(delta);
        let v: Option<CoordinatorMsg> = traceforge::recv_msg_timed(WaitTime::Finite(vote_wait(delta)));
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
    // Correct: PreCommit iff every participant voted Yes (and all votes
    //          arrived). Equivalent to:
    //              received_count == ps.len() && yes_count == ps.len()
    // Buggy:   PreCommit if a *majority* of received votes were Yes,
    //          even if some votes timed out.
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
        traceforge::sleep(delta);
        let v: Option<CoordinatorMsg> = traceforge::recv_msg_timed(WaitTime::Finite(vote_wait(delta)));
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

fn participant(delta: u64, num_ps: u32, index: u32, crashes: bool) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };

    if crashes && traceforge::nondet() {
        return;
    }

    traceforge::sleep(delta * (index as u64 + 1));

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

    traceforge::sleep(delta * num_ps as u64);
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    match action {
        // Safety property: a participant that voted No must never
        // observe Commit. With the bug above this assertion can fire.
        ParticipantMsg::Commit => traceforge::assert(yes),
        _ => panic!("expected Commit"),
    }
}

fn parse_args() -> (u32, u64, bool, bool) {
    let mut args = std::env::args().skip(1);
    let mut n = DEFAULT_PARTICIPANTS;
    let mut delta = DEFAULT_DELTA;
    let mut crashes = false;
    let mut replay = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--participants" => {
                let v = args.next().expect("--participants requires a value");
                n = v.parse().expect("invalid --participants");
            }
            "--delta" => {
                let v = args.next().expect("--delta requires a value");
                delta = v.parse().expect("invalid --delta");
            }
            "--crashes" => crashes = true,
            "--replay" => replay = true,
            other => panic!("unknown argument: {other}"),
        }
    }
    (n, delta, crashes, replay)
}

fn scenario(num_ps: u32, delta: u64, crashes: bool) -> impl Fn() + Send + Sync + 'static {
    move || {
        let c = thread::spawn(move || coordinator(delta, num_ps, crashes));
        let mut ps = Vec::new();
        for i in 0..num_ps {
            ps.push(thread::spawn(move || {
                participant(delta, num_ps, i, crashes)
            }));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    }
}

fn main() {
    let (num_ps, delta, crashes, replay) = parse_args();
    assert!(num_ps >= 2, "need >= 2 participants for the bug to fire");
    assert!(delta >= 2, "delta must be >= 2 for cross-mapping pruning");

    if replay {
        println!("Replaying captured counterexample from {BUG_FILE}");
        traceforge::replay(scenario(num_ps, delta, crashes), BUG_FILE);
        return;
    }

    println!("Running buggy 3PC verification with N = {num_ps} participants.");
    println!("Receives are timed (W_r finite); coordinator commits on majority.");
    if crashes {
        println!("--crashes: coordinator/participants may crash mid-protocol.");
    }
    println!("Artifacts on counterexample:");
    println!("  DOT graph  -> {DOT_FILE}");
    println!("  replay log -> {BUG_FILE}");
    println!();

    let cfg = Config::builder()
        .with_temporal(0, 1, 0)
        .with_dot_out(DOT_FILE)
        .with_error_trace(BUG_FILE)
        .with_verbose(1)
        .build();

    traceforge::verify(cfg, scenario(num_ps, delta, crashes));
}
