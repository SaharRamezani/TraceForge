//! Two-Phase Commit with a deliberate bug, to demonstrate how TraceForge
//! surfaces protocol violations.
//!
//! The bug: the coordinator commits whenever a *majority* of votes are
//! Yes, instead of *all*. This is the textbook wrong implementation of
//! 2PC: it breaks atomicity because a participant that voted No can
//! still receive a `Commit` message.
//!
//! When the model checker explores an interleaving where some
//! participant voted No but the majority voted Yes, the participant's
//! `assert!(yes)` on receiving `Commit` fires. TraceForge then:
//!
//!   - prints the offending execution graph to stdout,
//!   - writes a Graphviz DOT graph to /tmp/2pc_buggy.dot,
//!   - writes a JSON replay trace to /tmp/2pc_buggy.bug,
//!   - panics out of `verify`.
//!
//! Replay the captured trace deterministically with `traceforge::replay`
//! using the same closure passed to `verify`.
//!
//! Usage:
//!     cargo run --release --example two_pc_temporal_buggy -- --participants 3

use traceforge::thread::{self, ThreadId};
use traceforge::Config;

const DEFAULT_PARTICIPANTS: u32 = 3;

const DOT_FILE: &str = "/tmp/2pc_buggy.dot";
const BUG_FILE: &str = "/tmp/2pc_buggy.bug";

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
        let v: CoordinatorMsg = traceforge::recv_msg_block();
        match v {
            CoordinatorMsg::Yes => yes_count += 1,
            CoordinatorMsg::No => (),
            _ => panic!("expected vote"),
        }
    }

    // *** BUG ***
    // Correct 2PC: commit iff *every* participant voted Yes.
    //     let decision = if yes_count == ps.len() { Commit } else { Abort };
    // Buggy:  commit on majority. Any participant that voted No will see
    // Commit and trip its `assert!(yes)`.
    let decision = if yes_count > ps.len() / 2 {
        ParticipantMsg::Commit
    } else {
        ParticipantMsg::Abort
    };
    for id in &ps {
        traceforge::send_msg(*id, decision.clone());
    }
}

fn participant() {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };

    let yes = traceforge::nondet();
    let vote = if yes {
        CoordinatorMsg::Yes
    } else {
        CoordinatorMsg::No
    };
    traceforge::send_msg(cid, vote);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    match action {
        // This is the safety property. If the coordinator's decision
        // logic is wrong, this assertion catches it. We use
        // `traceforge::assert` (not the std `assert!`) so that on
        // failure the model checker prints the offending execution
        // graph and persists the replay log before panicking.
        ParticipantMsg::Commit => traceforge::assert(yes),
        ParticipantMsg::Abort => (),
        _ => panic!("expected decision"),
    }
}

fn parse_args() -> (u32, bool) {
    let mut args = std::env::args().skip(1);
    let mut n = DEFAULT_PARTICIPANTS;
    let mut replay = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--participants" => {
                let v = args.next().expect("--participants requires a value");
                n = v.parse().expect("invalid --participants");
            }
            "--replay" => replay = true,
            other => panic!("unknown argument: {other}"),
        }
    }
    (n, replay)
}

fn scenario(num_ps: u32) -> impl Fn() + Send + Sync + 'static {
    move || {
        let c = thread::spawn(coordinator);
        let mut ps = Vec::new();
        for _ in 0..num_ps {
            ps.push(thread::spawn(participant));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    }
}

fn main() {
    let (num_ps, replay) = parse_args();
    assert!(num_ps >= 2, "need >= 2 participants for the bug to fire");

    if replay {
        println!("Replaying captured counterexample from {BUG_FILE}");
        println!("(re-runs only the failing schedule; no exploration)");
        println!();
        traceforge::replay(scenario(num_ps), BUG_FILE);
        return;
    }

    println!("Running buggy 2PC verification with N = {num_ps} participants.");
    println!("Bug: coordinator commits on majority instead of unanimity.");
    println!("Artifacts on counterexample:");
    println!("  DOT graph  -> {DOT_FILE}");
    println!("  replay log -> {BUG_FILE}");
    println!();

    let cfg = Config::builder()
        .with_dot_out(DOT_FILE)
        .with_error_trace(BUG_FILE)
        .with_verbose(1)
        .build();

    // `verify` will panic out when `traceforge::assert(yes)` fires inside
    // a participant. Before panicking, TraceForge prints the offending
    // graph and persists BUG_FILE so the same schedule can be replayed
    // deterministically with `--replay`.
    traceforge::verify(cfg, scenario(num_ps));
}
