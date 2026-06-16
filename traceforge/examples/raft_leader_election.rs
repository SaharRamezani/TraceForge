//! Raft leader election (paper-faithful, multi-term, R-bounded).
//!
//! Implements §5.2 + Figure 2 of Ongaro & Ousterhout "In Search of an
//! Understandable Consensus Algorithm" (saved at `docs/raft.pdf`). Log
//! replication (§5.3, §5.4) is out of scope: logs are uniformly empty,
//! so the §5.4.1 up-to-date check is trivially satisfied and is omitted
//! from the wire format. The only payload an `AppendEntries` carries
//! is the heartbeat fields `(term, leaderId)`.
//!
//! ## Why R?
//!
//! The paper's election loop is *unbounded*: a server that times out
//! without hearing from a leader increments its term, becomes a
//! candidate, sends `RequestVote` to every peer, and *retries
//! indefinitely* until it (or someone else) succeeds — possibly through
//! many split-vote rounds. For a stateless model checker that has to
//! exhaustively enumerate executions, "indefinitely" is a non-starter.
//!
//! `R` (number of rounds / max term) bounds it: every node refuses to
//! advance its own `currentTerm` past `R`. Verification is then a sound
//! check of Election Safety for terms `1..=R` *at this protocol model*.
//! Reusing R = ∞ would be the real protocol; R = 2 catches every
//! single-term *and* one-retry scenario; R = 3 is enough to see two
//! successive retries.
//!
//! ## Servers, RPCs, state — verbatim from Figure 2
//!
//! Server state (per node):
//!   * `currentTerm: u64` — monotonically increasing, persisted.
//!   * `votedFor: Option<usize>` — candidateId that received our vote
//!     this term, or `None`.
//!   * `role` ∈ {Follower, Candidate, Leader}.
//!
//! `RequestVote(term, candidate)` receiver rules:
//!   1. If `term < currentTerm`, reply `granted = false`.
//!   2. If `term > currentTerm`: step down (`role = Follower`,
//!      `currentTerm = term`, `votedFor = None`).
//!   3. Grant iff `votedFor ∈ {None, Some(candidate)}` and the
//!      candidate's log is at least as up-to-date (trivially true
//!      here — empty logs).
//!
//! `AppendEntries(term, leader)` receiver rules:
//!   1. If `term < currentTerm`, reply `success = false`.
//!   2. If `term ≥ currentTerm`: step down to Follower, set
//!      `votedFor = Some(leader)`, reply `success = true`.
//!
//! Rules for all servers (§5.1):
//!   * If any RPC carries a term `T > currentTerm`, set
//!     `currentTerm = T` and convert to Follower.
//!
//! Rules for candidates:
//!   * On conversion: `currentTerm += 1`; `votedFor = Some(self)`;
//!     reset election timer; send `RequestVote` to all peers.
//!   * If votes from a majority arrive: become Leader, immediately send
//!     a heartbeat round.
//!   * If `AppendEntries` arrives from a new leader: step down.
//!   * If election timer fires again: start a new election (term + 1).
//!
//! Rules for leaders:
//!   * Send empty `AppendEntries` heartbeats to all peers immediately
//!     upon election, and repeat in idle periods.
//!
//! ## Election Safety check — *distributed*, no monitor thread
//!
//! Figure 3's Election Safety property — "at most one leader can be
//! elected in a given term" — is checked **locally** on every
//! `AppendEntries` receive: each node remembers the first `(term,
//! leader)` it sees, and any subsequent AE for the same term must carry
//! the same leader id, otherwise `traceforge::assert` fires. By quorum
//! overlap, two leaders in the same term would each broadcast AEs to
//! every peer, so at least one node would witness both. No central
//! observer is needed and there is no thread that does not appear in
//! the paper.
//!
//! ## Two run modes
//!
//!   * `--mode baseline` — untimed MUST. Each node calls
//!     `traceforge::nondet()` at each "would-the-timer-fire-now" point.
//!     The model checker enumerates every subset of nodes that times
//!     out at each step, for every term up to `R`.
//!
//!   * `--mode timed` — MUST-τ with `with_timed(0, 1, sd)` and
//!     staggered `sleep((me+1)*delta)` before each term's election.
//!     Models the realistic common case where randomized timeouts
//!     spread out and one candidate fires first per term.
//!
//! ## Run
//!
//! From the workspace root:
//!
//! ```bash
//! # default (compare baseline vs timed, N=3, R=2, delta=2)
//! cargo run --release --example raft_leader_election
//!
//! # explicit flags
//! cargo run --release --example raft_leader_election -- \
//!     --mode compare --nodes 3 --rounds 2 --delta 2
//!
//! # one mode at a time
//! cargo run --release --example raft_leader_election -- --mode baseline --nodes 3 --rounds 2
//! cargo run --release --example raft_leader_election -- --mode timed --nodes 3 --rounds 1
//!
//! # bigger cluster (slow!)
//! cargo run --release --example raft_leader_election -- --mode baseline --nodes 5 --rounds 1
//! ```
//!
//! ## Parameters
//!
//! | flag         | default | meaning                                                  |
//! | ------------ | ------- | -------------------------------------------------------- |
//! | `--mode`     | compare | `baseline` (MUST), `timed` (MUST-τ), or `compare`     |
//! | `--nodes N`  | 3       | cluster size; must be ≥ 3. State space grows fast in N.  |
//! | `--rounds R` | 2       | max term explored. R=1 = single term; R=2 = one retry.   |
//! | `--delta D`  | 2       | per-round stagger unit used by timed mode (D ≥ 1).    |
//!
//! Notes:
//!   * Timed mode is intended for R=1. At R ≥ 2 the per-round
//!     storage-delay windows overlap and the state space explodes.
//!   * Combining `--nodes 5` with `--rounds ≥ 2` is impractical at this
//!     protocol granularity; use baseline mode and small R.

use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DEFAULT_NODES: usize = 3;
const DEFAULT_DELTA: u64 = 2;
const DEFAULT_ROUNDS: u64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Bootstrapping message — separate type from `NodeMsg` so the
/// init-wait `recv_msg_block::<Init>()` only matches Init messages.
/// Any `NodeMsg` (RV / RVR / AE / AER) that arrives in the mailbox
/// before Init does **stays queued** for the main loop to process,
/// instead of being silently discarded by an `_ => {}` arm.
#[derive(Clone, Debug, PartialEq)]
struct Init {
    peers: Vec<ThreadId>,
    me: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum NodeMsg {
    RequestVote { term: u64, candidate: usize },
    RequestVoteResponse { term: u64, granted: bool, voter: usize },
    AppendEntries { term: u64, leader: usize },
    AppendEntriesResponse { term: u64, success: bool, from: usize },
}

struct NodeState {
    me: usize,
    current_term: u64,
    voted_for: Option<usize>,
    role: Role,
    votes_received: u64,
    // Distributed Election-Safety witness. The first (term, leader) we
    // record for any term is locked in; subsequent AEs for that term
    // must agree or `traceforge::assert` fires.
    seen_leader: Option<(u64, usize)>,
}

impl NodeState {
    fn new(me: usize) -> Self {
        Self {
            me,
            current_term: 0,
            voted_for: None,
            role: Role::Follower,
            votes_received: 0,
            seen_leader: None,
        }
    }
}

fn peers_index_to_tid(peers: &[ThreadId], target: usize, self_idx: usize) -> ThreadId {
    let i = if target < self_idx { target } else { target - 1 };
    peers[i]
}

fn record_leader(state: &mut NodeState, term: u64, leader: usize) {
    match state.seen_leader {
        Some((t, l)) if t == term => {
            // Election Safety (Figure 3): at most one leader per term.
            traceforge::assert(l == leader);
        }
        _ => state.seen_leader = Some((term, leader)),
    }
}

fn send_heartbeats(state: &NodeState, peers: &[ThreadId]) {
    for &peer in peers {
        traceforge::send_msg(
            peer,
            NodeMsg::AppendEntries {
                term: state.current_term,
                leader: state.me,
            },
        );
    }
}

fn become_leader(state: &mut NodeState, peers: &[ThreadId]) {
    state.role = Role::Leader;
    record_leader(state, state.current_term, state.me);
    // Figure 2, Rules for Leaders: "Upon election: send initial empty
    // AppendEntries RPCs (heartbeat) to each server."
    send_heartbeats(state, peers);
}

/// Per-role test for "this term's work is done":
///   - Leader   → we just sent our AE round, the protocol's job is done.
///   - Follower → we've recorded a leader for this term (saw its AE).
///   - Candidate → still trying — keep recv-ing.
///
/// This is the inner-loop exit condition: a node keeps reading
/// messages until *this* predicate fires, exactly mirroring Raft's
/// Figure-2 server rules ("run forever until your role's exit
/// condition triggers") within the bound of a single term.
fn term_completed(state: &NodeState, round: u64) -> bool {
    match state.role {
        Role::Leader => true,
        Role::Follower => matches!(state.seen_leader, Some((t, _)) if t >= round),
        Role::Candidate => false,
    }
}

fn handle_msg(
    state: &mut NodeState,
    msg: NodeMsg,
    peers: &[ThreadId],
    majority: u64,
    reply_target: &mut Option<(ThreadId, NodeMsg)>,
) {
    // Figure 2, "Rules for All Servers": if RPC term > currentTerm,
    // set currentTerm = T and convert to follower. We apply that
    // *before* the per-RPC handler.
    let incoming_term = match &msg {
        NodeMsg::RequestVote { term, .. }
        | NodeMsg::RequestVoteResponse { term, .. }
        | NodeMsg::AppendEntries { term, .. }
        | NodeMsg::AppendEntriesResponse { term, .. } => *term,
    };
    if incoming_term > state.current_term {
        state.current_term = incoming_term;
        state.voted_for = None;
        state.role = Role::Follower;
        state.votes_received = 0;
    }

    match msg {
        NodeMsg::RequestVote { term, candidate } => {
            let mut granted = false;
            if term >= state.current_term {
                // §5.4.1 up-to-date check is vacuously true (empty logs).
                let can_grant = state.voted_for.is_none() || state.voted_for == Some(candidate);
                if can_grant {
                    state.voted_for = Some(candidate);
                    granted = true;
                }
            }
            *reply_target = Some((
                peers_index_to_tid(peers, candidate, state.me),
                NodeMsg::RequestVoteResponse {
                    term: state.current_term,
                    granted,
                    voter: state.me,
                },
            ));
        }
        NodeMsg::RequestVoteResponse { term, granted, .. } => {
            // Only meaningful as Candidate in the same term.
            if state.role == Role::Candidate && term == state.current_term && granted {
                state.votes_received += 1;
                if state.votes_received >= majority && state.role != Role::Leader {
                    become_leader(state, peers);
                }
            }
        }
        NodeMsg::AppendEntries { term, leader } => {
            // Distributed Election-Safety witness.
            record_leader(state, term, leader);
            let success = term >= state.current_term;
            if success {
                // Figure 2: step down on valid AE.
                state.role = Role::Follower;
                state.voted_for = Some(leader);
                state.votes_received = 0;
            }
            *reply_target = Some((
                peers_index_to_tid(peers, leader, state.me),
                NodeMsg::AppendEntriesResponse {
                    term: state.current_term,
                    success,
                    from: state.me,
                },
            ));
        }
        NodeMsg::AppendEntriesResponse { .. } => {
            // Single-term election doesn't act on AERs beyond the
            // term-bump handled above. With logs, leaders use AERs to
            // advance nextIndex/matchIndex (§5.3) — out of scope here.
        }
    }
}

/// Per-node loop. Each iteration of the outer `for round` is one
/// election term attempt — at most `R` attempts in total. Within a
/// round we run the optimised single-term protocol: stagger →
/// single-peek drain → candidacy decision → role-bounded main loop
/// using `recv_msg_block_timed` (no None branch in the per-message
/// recvs, so the state space doesn't compound across rounds).
///
/// Election timeouts are *not* modelled as `recv_msg_timed(Finite(W))`
/// in the main loop — that would put a binary Some/None split on every
/// recv and the state space blows up at R≥1 already. Instead, the
/// per-round structure encodes the timeout *between* rounds: if a
/// node finishes a round still as a Candidate (didn't reach majority,
/// didn't get an AE), we treat that as the timer expiring and retry
/// with the next term up to `R`.
fn node(mode: Mode, delta: u64, rounds: u64, me: usize, num_nodes: usize, main_tid: ThreadId) {
    // Receive Init via a sender-filtered tagged recv. `recv_msg_block`
    // would take any queued NodeMsg first (and panic on the type
    // mismatch since Init is its own type); filtering by `sender ==
    // main_tid` skips past any racing RPCs from peers and matches
    // only the Init that main sent us. Non-matching messages stay
    // in the mailbox for the main loop to handle.
    let Init { peers, me: _ } = traceforge::recv_tagged_msg_block::<_, Init>(
        move |sender, _tag| sender == main_tid,
    );

    let majority = (num_nodes as u64) / 2 + 1;
    let mut state = NodeState::new(me);

    for round in 1..=rounds {
        // ---- (1) Election-timer stagger (timed mode only) ----
        // Re-stagger every round so a previously-non-candidate node
        // doesn't sit forever holding a stale vote.
        if mode == Mode::Timed {
            traceforge::sleep((me as u64 + 1) * delta);
        }

        // ---- (2) Drain peek: did anyone reach us before our timer
        // fired? Single peek — see the project notes for why a
        // `while let` would over-consume the leader's future heartbeat.
        let saw_message_in_peek = match mode {
            Mode::Baseline => false,
            Mode::Timed => {
                if let Some(msg) =
                    traceforge::recv_msg_timed::<NodeMsg>(WaitTime::Finite(0))
                {
                    let mut reply = None;
                    handle_msg(&mut state, msg, &peers, majority, &mut reply);
                    if let Some((tid, m)) = reply {
                        traceforge::send_msg(tid, m);
                    }
                    true
                } else {
                    false
                }
            }
        };

        // ---- (3) Decide whether *we* time out this round and start
        // an election. Per Figure 2, a follower converts to candidate
        // on its election timeout; here that's:
        //   - baseline: nondet (model checker enumerates all subsets);
        //   - timed: deterministic — if our peek already settled
        //     us as Follower/Leader for this round we skip.
        let should_start_election = match mode {
            Mode::Baseline => {
                state.role != Role::Leader
                    && state.current_term < round
                    && traceforge::nondet()
            }
            Mode::Timed => {
                state.role == Role::Follower
                    && state.voted_for.is_none()
                    && !saw_message_in_peek
            }
        };

        if should_start_election {
            // Figure 2, Rules for Candidates: increment currentTerm,
            // vote for self, send RequestVote to all peers. We *set*
            // current_term to `round` (rather than `+=1`) so a node
            // that lagged behind earlier rounds catches up correctly.
            if round > state.current_term {
                state.voted_for = None; // per-term votedFor reset
            }
            state.current_term = round;
            state.role = Role::Candidate;
            state.voted_for = Some(me);
            state.votes_received = 1;
            if state.votes_received >= majority {
                become_leader(&mut state, &peers);
            } else {
                for &peer in &peers {
                    traceforge::send_msg(
                        peer,
                        NodeMsg::RequestVote {
                            term: state.current_term,
                            candidate: me,
                        },
                    );
                }
            }
        }

        // ---- (4) Main loop: keep receiving until this term is
        // visibly complete for us (Figure-2 servers run forever; we
        // stop as soon as the paper would consider our role's exit
        // condition for this term met). No iteration budget — `R`
        // (number of terms) is the only bound on the system. If no
        // leader emerges in this term, the candidate's recv blocks
        // and TraceForge reports the exec as `blocked` — a genuine
        // split-vote stall, not a model-artifact.
        while !term_completed(&state, round) {
            let msg = traceforge::recv_msg_block_timed::<NodeMsg>();
            let mut reply = None;
            handle_msg(&mut state, msg, &peers, majority, &mut reply);
            if let Some((tid, m)) = reply {
                traceforge::send_msg(tid, m);
            }
        }

        // ---- (5) End-of-round. The `while` predicate guarantees
        // `state.role` is now Leader, or Follower with this term's
        // leader recorded; either way this term's work is done.
        return;
    }
    let _ = delta;
}

fn build_config(mode: Mode, num_nodes: usize, delta: u64) -> Config {
    match mode {
        Mode::Baseline => Config::builder()
            .with_progress_report(100_000)
            .with_verbose(0)
            .build(),
        Mode::Timed => {
            // sd ≥ (N-1)·delta lets node N-1's drain at τ=N·delta still
            // see node 0's RV sent at τ=delta. With sd=0 every late peek
            // misses everything, the cancellation rule never fires, and
            // no leader is ever elected.
            //
            // NB: at R ≥ 2 the timed feasibility windows from round 1
            // and round 2 overlap (storage delay outlives the round
            // boundary), which lets the model checker explore very many
            // cross-round message orderings. Timed mode is intended
            // for R=1; for R≥2 use baseline.
            let sd = (num_nodes as u64).saturating_sub(1) * delta;
            Config::builder()
                .with_timed(0, 1, sd)
                .with_progress_report(100_000)
                .with_verbose(0)
                .build()
        }
    }
}

fn run(mode: Mode, num_nodes: usize, delta: u64, rounds: u64) -> (Stats, Duration) {
    let cfg = build_config(mode, num_nodes, delta);
    let t0 = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let mut handles = Vec::with_capacity(num_nodes);
        for i in 0..num_nodes {
            handles.push(thread::spawn(move || {
                node(mode, delta, rounds, i, num_nodes, main_tid)
            }));
        }
        let all_ids: Vec<ThreadId> = handles.iter().map(|h| h.thread().id()).collect();
        for (me, h) in handles.iter().enumerate() {
            let peers: Vec<ThreadId> = all_ids
                .iter()
                .enumerate()
                .filter_map(|(i, id)| if i == me { None } else { Some(*id) })
                .collect();
            traceforge::send_msg(h.thread().id(), Init { peers, me });
        }
    });
    (stats, t0.elapsed())
}

fn print_one(label: &str, num_nodes: usize, rounds: u64, stats: &Stats, dur: Duration) {
    println!(
        "{label:<10} nodes={n} R={R}  execs={execs:<8} blocked={blk:<6} time={dur:?}",
        n = num_nodes,
        R = rounds,
        execs = stats.execs,
        blk = stats.block,
    );
}

fn print_compare(
    num_nodes: usize,
    rounds: u64,
    baseline: (Stats, Duration),
    timed: (Stats, Duration),
) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;

    println!();
    println!("Raft Leader Election (paper-faithful, R-bounded): Must vs Must-τ");
    println!("================================================================");
    println!(
        "nodes = {num_nodes}  (majority = {})  rounds = {rounds}",
        num_nodes / 2 + 1
    );
    println!();
    println!(
        "{:<10} {:>10} {:>10} {:>14}",
        "mode", "execs", "blocked", "time"
    );
    println!(
        "{:<10} {:>10} {:>10} {:>14?}",
        "baseline", b_stats.execs, b_stats.block, b_dur
    );
    println!(
        "{:<10} {:>10} {:>10} {:>14?}",
        "timed", t_stats.execs, t_stats.block, t_dur
    );
    println!();
    let total_b = b_stats.execs + b_stats.block;
    let total_t = t_stats.execs + t_stats.block;
    let ratio = total_b as f64 / total_t.max(1) as f64;
    println!("(execs+blocked) ratio: {ratio:.2}x  (= baseline / timed)");
    println!();
    println!("Election Safety (≤ 1 leader / term) is asserted on every");
    println!("AppendEntries receive — no central monitor thread.");
}

fn parse_args() -> (String, usize, u64, u64) {
    let mut mode = String::from("compare");
    let mut nodes = DEFAULT_NODES;
    let mut delta = DEFAULT_DELTA;
    let mut rounds = DEFAULT_ROUNDS;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = args
                    .next()
                    .unwrap_or_else(|| panic!("--mode requires a value"));
            }
            "--nodes" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--nodes requires a value"));
                nodes = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --nodes: {v}"));
            }
            "--delta" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--delta requires a value"));
                delta = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --delta: {v}"));
            }
            "--rounds" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--rounds requires a value"));
                rounds = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --rounds: {v}"));
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: raft_leader_election [--mode baseline|timed|compare] \
                     [--nodes N] [--delta D] [--rounds R]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, nodes, delta, rounds)
}

fn main() {
    let (mode_str, num_nodes, delta, rounds) = parse_args();
    assert!(num_nodes >= 3, "need at least 3 nodes for a meaningful majority");
    assert!(delta >= 1, "delta must be >= 1");
    assert!(rounds >= 1, "rounds must be >= 1");

    match mode_str.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, num_nodes, delta, rounds);
            print_one("baseline", num_nodes, rounds, &s, d);
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, num_nodes, delta, rounds);
            print_one("timed", num_nodes, rounds, &s, d);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, num_nodes, delta, rounds);
            let timed = run(Mode::Timed, num_nodes, delta, rounds);
            print_compare(num_nodes, rounds, baseline, timed);
        }
        other => panic!("invalid --mode: {other} (expected baseline|timed|compare)"),
    }
}
