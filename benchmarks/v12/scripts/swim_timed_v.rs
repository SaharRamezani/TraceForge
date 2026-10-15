//! SWIM failure-detector suspicion subprotocol: false-positive death.
//!
//! In SWIM (Das et al. 2002, Section 4.2), a prober that misses an Ack
//! within its probe window suspects the target and disseminates
//! Suspect messages. The target refutes a suspicion by broadcasting
//! Alive carrying a higher incarnation number; every member that
//! received the Suspect runs its OWN suspicion timer on its own copy,
//! with no coordination. Lifeguard (Dadgar et al. 2017) starts from
//! SWIM's motivating defect: if the refutation is processed after the
//! suspicion timeout, a perfectly healthy member is declared dead, a
//! false positive.
//!
//! In this model the target NEVER crashes (the static TARGET_CRASHED
//! stays false forever), so ANY dead declaration is a false positive
//! by construction.
//!
//! ## Actors and rounds
//!
//! Threads are long-lived (the three_pc_timed idiom): spawned once,
//! processing R protocol periods. The prober (member 0) drives the
//! periods: each round it pings the target (member 1), waits W_PROBE
//! for the Ack, and on timeout suspects the target with its current
//! known incarnation, sending the Suspect to the target itself first
//! (the edge the refutation path depends on) and disseminating copies
//! to the bystanders (members 2..N-1).
//!
//! The target is purely reactive, exactly as in the paper: it acks
//! every Ping and refutes every admissible suspicion (inc >= its
//! incarnation) by broadcasting Alive{inc + 1}; it has no timers and
//! no timeout branches (blocking receives only). Bystanders are also
//! reactive: they idle until a Suspect arrives, then run their own
//! W_SUSPECT timer against the matching refutation. The only timers in
//! the model are therefore SWIM's own two: the probe window and the
//! suspicion timeout, both at suspecting members.
//!
//! Rounds are matched with round-scoped tags: receive predicates see
//! only (sender, tag), never the payload, so Ack and Alive, the two
//! messages that timed waits target, encode their period in the tag
//! (ack_tag/alive_tag); Ping and Suspect carry
//! `round` in the payload. The prober's per-round Suspect carries its
//! current known incarnation, so incarnations rise across rounds and
//! the SWIM refutation rule (refute iff inc >= incarnation, bump to
//! inc + 1) is exercised for real from round 2 on.
//!
//! There is no bootstrap or release message. The bystanders and the
//! target are spawned first and the prober last, so it captures their
//! ids at spawn; the target learns the prober's id from the `from`
//! field of the Ping and Suspect it receives (the 3PC `Prepare {
//! coord }` idiom), and the bystanders filter by message-kind tag, not
//! by sender.
//!
//! No thread is a daemon: every thread ends in a run in which no
//! message is lost or expires. The prober ends after its R periods.
//! The target and the bystanders cannot be bounded by a count (a
//! Suspect is sent only when a probe times out), so one HARNESS
//! message ends each of them: after its last period the prober sends a
//! Stop to the target, waits for the target to end (a join, not a
//! message), sends a Stop to the first bystander, waits for it, and so
//! on. The Stops come from the prober, so FIFO delivers each one after
//! every Suspect the prober sent to that bystander, and a member leaves
//! its loop on reading it. The Stop is lossy like every send: a lost
//! Stop leaves its reader blocked, the prober blocked at the join, and
//! the later Stops unsent, so a run that ends blocked without a failed
//! assertion is a run in which a message was lost or expired. A
//! prober that fails its assertion sends no Stop (the run is a
//! violation).
//!
//! ## Deviations from the paper (deliberate, defect-first model)
//!
//! No indirect ping-req probing (the paper's k intermediary probers),
//! no infection-style piggybacked dissemination (Suspects are sent
//! directly), fixed prober/target roles, no Confirm dissemination
//! after a verdict, and the target cannot crash in this version. The
//! modeled mechanism is exactly the suspicion subprotocol whose false
//! positive motivates Lifeguard.
//!
//! ## No harness assumptions (since 2026-10-12)
//!
//! The false-positive assertion sits on timeout branches. A timeout
//! branch is a real execution exactly where the checker's timeout miss
//! condition (C6b) admits it: every message the receive could have
//! taken missed its whole wait, judged when the execution is complete.
//! A message misses a wait that began at t_p and ended at t_e when it
//! arrives at or after t_e (an arrival exactly at the deadline may lose
//! the race to the timer) or was already discarded when the wait began
//! (arrival + sd < t_p; with sd = 0 a message is readable only at its
//! arrival instant). A dropped send is never pending, so a lost message
//! never constrains a timeout.
//! Until 2026-10-12 this file additionally followed each
//! property-relevant timeout with a BLOCKING read of the very message
//! the timer awaited (assume_ack_was_late, assume_refutation_was_late),
//! so that a timeout counted only if that message was eventually read.
//! What those reads filtered out, once (C6b) handled the timing, was
//! every execution in which the Ack or the Alive was LOST or EXPIRED
//! unread (sd = 0), since such a message is never read. Under the
//! model's semantics every send may be lost, a dead declaration caused
//! by a lost message is a false positive like any other, and nothing is
//! filtered: the reads are gone, every finite wait is SWIM's own, and the
//! law that depended on them is withdrawn (see Expected verdicts).
//!
//! ## Safety properties (per-node assertions)
//!
//!   dead declaration  => TARGET_CRASHED     (at prober and bystanders;
//!       TARGET_CRASHED is never set, so any firing IS the SWIM false
//!       positive)
//!   refutation bumps incarnation strictly   (at the target, guarded
//!       by the paper's inc >= incarnation admissibility rule)
//!
//! ## CLI parameters (ratios are over U, like three_pc_timed)
//!
//!   --mode baseline|timed|compare   verification mode (default timed:
//!                                   baseline always FIREs, so compare
//!                                   aborts in its baseline leg)
//!   --nodes N                       members: prober + target +
//!                                   (N-2) bystanders (default 3)
//!   --rounds R                      probe periods (default 1)
//!   --u U                           global transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U) (default 0.0)
//!   --sd-ratio SR                   sd = round(SR * U), storage lifetime
//!                                   past arrival (default 0.0)
//!   --w-probe-ratio R               W_PROBE = round(R * U) (default 1.0)
//!   --w-suspect-ratio R             W_SUSPECT = round(R * U) (default 4.0)
//!   --keep-going                    explore the whole state space after a
//!                                   violation and report the number of
//!                                   false positives (violations=) instead
//!                                   of aborting at the first one
//!   --dot-out PATH                  dump the execution graph (the
//!                                   counterexample prefix on a FIRE) as
//!                                   Graphviz dot, tau intervals in timed
//!                                   mode
//!   --parallel none|shared|partitioned
//!                                   exploration strategy (default none)
//!
//! CLI misuse exits 2, distinct from a property violation's 101, so
//! exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
//!
//! ## Expected verdicts (re-measured 2026-10-13, after the first-arrival ruling)
//!
//!   baseline: FIRE (exit 101) at any parameters.
//!   timed:    FIRE at any parameters, R >= 1. Every send may be lost, so
//!       no choice of parameters rules out a false positive: a lost Ack
//!       makes the prober suspect at the probe deadline, and a lost
//!       refuting Alive lets a suspicion timer run out, so the target is
//!       declared dead although it never crashed. A failure detector
//!       cannot tell a lost message from a crash; under links that may
//!       lose anything, no-false-positive is unsatisfiable, and the FIRE
//!       is the semantics' answer. A FIRE does not even need a loss: a
//!       delivered Ack or Alive can miss its wait by timing alone (see
//!       the witnesses below).
//!
//!   Measured 2026-10-13, without --keep-going: all 1,794 timed cells
//!   exit 101 with a certified witness, and all 52 baseline cells exit
//!   101. The cells reach N = 8, R = 6, U = 10, with L from 0 to U, sd up
//!   to 5U, W_PROBE up to 10U and W_SUSPECT up to 12U; N = 3, R = 1 is a
//!   full grid over U in 1..3, L in 0..U, sd in {0, 1, U, 2U}, W_PROBE in
//!   {1, U, 2U, 4U} and W_SUSPECT in {1, U, 2U, 3U, 4U, 8U}.
//!
//!   What the certified witness shows. The checker stops at the first
//!   violating execution it completes and prints its graph; the timeline
//!   is the causal prefix of the last assertion that failed in it (every
//!   member that declared the target dead has a failed assertion in the
//!   graph). N = 3, R = 1, U = 1, L = 0, sd = 0, W_PROBE = 1 and
//!   W_SUSPECT = 4 unless noted:
//!     defaults: the Ack is delivered but arrives at or after the probe
//!         deadline, time 1 (its round trip is up to 2U); the refuting
//!         Alive to the prober is [dropped]; the Alive to the bystander
//!         is delivered at time 1, before the bystander's own Suspect
//!         arrives (time 2, the whole transit U), so it expired unread,
//!         and the bystander's wait [2, 6] finds nothing. Both members
//!         declare the target dead; the printed timeline is the
//!         bystander's.
//!     W_PROBE = W_SUSPECT = 1: nothing is dropped. The Ack and both
//!         Alives are delivered and arrive at or after the deadline of
//!         the wait that expects them (ties at the deadline admitted).
//!     W_PROBE = W_SUSPECT = 4: the Ack and the prober's Alive are
//!         [dropped] (4 exceeds the longest Ack round trip, 2U, so a
//!         delivered Ack is never late); the bystander's Alive arrives
//!         at 4, before its Suspect (5), and expires unread.
//!     L = U = 1: every transit takes exactly 1, so the Ack is late
//!         (round trip 2 > W_PROBE = 1) and an Alive (two hops) cannot
//!         overtake a Suspect (one hop); both refutations are [dropped].
//!
//!   Over all 1,794 timed cells the printed witness drops nothing in 999
//!   and, where it drops, only Acks and Alives (never a Ping or a
//!   Suspect).
//!
//!   First arrival (2026-10-13: a receive that reads s takes the message
//!   that arrived first among those pending at it) changes nothing in
//!   this model: no receive ever has pending matching messages from two
//!   different senders. Every timed wait names its sender and its
//!   period, the bystander's idle wait names the prober, and the
//!   target's untagged receive hears only the prober, over one FIFO
//!   channel, so there is never a second channel to order. The verdicts,
//!   graphs and timelines of all the cells above, and the exhaustive
//!   counts of 404 timed cells (--keep-going), are identical before and
//!   after the ruling.
//!
//!   Exhaustive explored executions, execs/blocked with --keep-going, the
//!   other parameters at their defaults ("old" is the model before
//!   2026-10-12, with the two validation reads; it held at these cells):
//!       N  R         old              now
//!       3  1       4/143           29/205
//!       3  2      16/637         494/2521
//!       4  1     10/1465        197/3201
//!       5  1    28/19571      1373/57193
//!       4  2   100/24249   16772/168687   (3 to 4 minutes)
//!
//!   Until 2026-10-12 the two blocking validation reads (see above)
//!   discarded every execution with a lost or expired Ack or Alive, and
//!   the law "timed FIRE iff W_SUSPECT <= 2U + 2sd - min(L, max(0, 2L -
//!   W_PROBE))" was reported for the executions that remained, its
//!   anchor term coming from the validation read itself. That law
//!   described the filtered model and is withdrawn.
//!
//! Timed FIREs are certified: the checker validates every reported
//! counterexample against a single consistent timeline and prints the
//! witness timestamps next to the graph.
//!
//! A hold with SUSPECTS_RAISED > 0 and REFUTED_IN_TIME = 0 would mean
//! that no suspicion's outcome was ever judged; the binary prints a
//! WARNING. Runs with execs = 0 verified nothing and also WARN. The
//! SUSPECTS_RAISED test does not look at dead, so with --keep-going it
//! also prints at violating cells where no refutation can arrive within
//! the wait (e.g. L = U = 2, W_SUSPECT = 1: refuted = 0, dead > 0);
//! there it is not a vacuous hold.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

// Outcome counters: event counts accumulated across all explored
// executions of one verify run (revisited prefixes may re-count, so
// treat them as zero/nonzero evidence, not exact per-exec tallies).
// Reset before each verify, read after.
static ACKS_IN_TIME: AtomicUsize = AtomicUsize::new(0);
static SUSPECTS_RAISED: AtomicUsize = AtomicUsize::new(0);
static REFUTED_IN_TIME: AtomicUsize = AtomicUsize::new(0);
static DEAD_DECLARED: AtomicUsize = AtomicUsize::new(0);

// Never set true in this version: the target cannot crash, so every
// dead declaration is a false positive. A later version may add real
// fail-stop crashes and set this when T dies.
static TARGET_CRASHED: AtomicBool = AtomicBool::new(false);

const DEFAULT_NODES: u32 = 3;
const DEFAULT_ROUNDS: u32 = 1;
const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 0.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const DEFAULT_W_PROBE_RATIO: f64 = 1.0;
const DEFAULT_W_SUSPECT_RATIO: f64 = 4.0;

// Round-scoped tags (see the doc header): receive predicates can only
// see (sender, tag), so the Ack and the Alive of period r are
// identified by tag, keeping every timed wait pinned to its own
// period's message.
const TAG_KINDS: u32 = 3;

fn ack_tag(round: u32) -> u32 {
    round * TAG_KINDS + 1
}

fn alive_tag(round: u32) -> u32 {
    round * TAG_KINDS + 2
}

/// Tag of the Suspect the prober disseminates to a bystander. Its kind
/// (tag mod TAG_KINDS == 0) is what a bystander's idle wait matches.
fn suspect_tag(round: u32) -> u32 {
    round * TAG_KINDS + 3
}

/// HARNESS: tag of the Stop; kind 0, so that a bystander's idle wait
/// matches it (the target's untagged wait matches any tag).
const STOP_TAG: u32 = 0;

/// Member-to-member protocol traffic.
#[derive(Clone, Debug, PartialEq)]
enum MMsg {
    /// `from` is the prober (the sender's own address, as in 3PC's
    /// Prepare { coord }): the target replies to it.
    Ping { round: u32, from: ThreadId },
    Ack,
    Suspect { inc: u64, round: u32, from: ThreadId },
    Alive { inc: u64 },
    /// HARNESS: end of run, sent by the prober to each reactor in turn.
    Stop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    l: u64,
    sd: u64,
    w_probe: u64,
    w_suspect: u64,
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

impl Bounds {
    fn from_ratios(
        u: u64,
        l_ratio: f64,
        sd_ratio: f64,
        w_probe_ratio: f64,
        w_suspect_ratio: f64,
    ) -> Self {
        if u < 1 {
            cli_bail("U must be >= 1");
        }
        let scale = |r: f64| (r * u as f64).round() as u64;
        let l = scale(l_ratio);
        let sd = scale(sd_ratio);
        if l > u {
            cli_bail("transit lower bound L must be <= U (check --l-ratio)");
        }
        let w_probe = scale(w_probe_ratio);
        let w_suspect = scale(w_suspect_ratio);
        if w_probe < 1 {
            cli_bail("W_PROBE must round to >= 1 (check --w-probe-ratio)");
        }
        if w_suspect < 1 {
            cli_bail("W_SUSPECT must round to >= 1 (check --w-suspect-ratio)");
        }
        Self { u, l, sd, w_probe, w_suspect }
    }
}

// =====================================================================
// Prober (member 0): drives the R protocol periods
// =====================================================================

fn prober(
    b: Bounds,
    rounds: u32,
    target: ThreadId,
    bystanders: Vec<ThreadId>,
    // HARNESS: join handles of the target, then of the bystanders in
    // spawn order (the order in which the Stops go out).
    reactors: Vec<thread::JoinHandle<()>>,
) {
    let me = thread::current_id();

    // The prober's current belief of the target's incarnation; rises
    // with every refutation it accepts (SWIM's incarnation ordering).
    let mut known_inc: u64 = 0;

    for round in 0..rounds {
        traceforge::send_msg(target, MMsg::Ping { round, from: me });

        // Probe window: wait W_PROBE for this period's Ack.
        let acked = loop {
            match traceforge::recv_tagged_msg_timed::<_, MMsg>(
                move |sender, tag| sender == target && tag == Some(ack_tag(round)),
                WaitTime::Finite(b.w_probe),
            ) {
                Some(MMsg::Ack) => break true,
                // Unreachable: only the period's Ack carries its tag.
                Some(_) => {}
                None => break false,
            }
        };
        if acked {
            ACKS_IN_TIME.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        // Probe timed out: suspect the target and disseminate. The
        // Suspects go out first, at the probe deadline, so
        // dissemination timing stays faithful to SWIM.
        traceforge::send_msg(target, MMsg::Suspect { inc: known_inc, round, from: me });
        for m in &bystanders {
            traceforge::send_tagged_msg(
                *m,
                suspect_tag(round),
                MMsg::Suspect { inc: known_inc, round, from: me },
            );
        }

        SUSPECTS_RAISED.fetch_add(1, Ordering::Relaxed);

        // Suspicion timer: wait W_SUSPECT for a refuting Alive. P
        // records its own verdict; per-node locality means it does NOT
        // tell the bystanders the outcome, they run their own timers
        // on their own Suspect copies.
        let refuted = loop {
            match traceforge::recv_tagged_msg_timed::<_, MMsg>(
                move |sender, tag| sender == target && tag == Some(alive_tag(round)),
                WaitTime::Finite(b.w_suspect),
            ) {
                Some(MMsg::Alive { inc }) if inc > known_inc => break Some(inc),
                // Unreachable (the period's refutation strictly bumps);
                // drop and reissue defensively.
                Some(_) => {}
                None => break None,
            }
        };
        match refuted {
            Some(inc) => {
                REFUTED_IN_TIME.fetch_add(1, Ordering::Relaxed);
                known_inc = inc;
            }
            None => {
                DEAD_DECLARED.fetch_add(1, Ordering::Relaxed);
                // Safety: a member declares T dead only if T actually
                // crashed. TARGET_CRASHED is never true here, so this
                // firing IS the SWIM false positive, whether the Ack and
                // the refutation were late or lost.
                traceforge::assert(TARGET_CRASHED.load(Ordering::Relaxed));
                // SWIM removes a dead-declared member; stop probing.
                // (Unreachable in practice: the assert above fires.)
                return;
            }
        }
    }

    // HARNESS: end of run. After its last period and its last
    // refutation wait the prober sends one Stop to each reactor, the
    // target first. It joins a reactor before it sends the next Stop,
    // so a reactor whose Stop was lost keeps the prober blocked and
    // the later Stops are never sent: the fates of the Stops are
    // dependent and the executions grow by one per reactor, not by a
    // factor of two. The Stop comes from the prober, so FIFO orders it
    // after the Suspects it sent to the same bystander.
    let mut previous: Option<thread::JoinHandle<()>> = None;
    for (member, handle) in std::iter::once(target)
        .chain(bystanders.iter().copied())
        .zip(reactors)
    {
        if let Some(p) = previous.take() {
            let _ = p.join();
        }
        traceforge::send_tagged_msg(member, STOP_TAG, MMsg::Stop);
        previous = Some(handle);
    }
}

// =====================================================================
// Target (member 1): purely reactive, never crashes. Acks every Ping,
// refutes every admissible Suspect (the paper's rule); blocking
// receives only, so it contributes no timeout branches.
// =====================================================================

fn target(bystanders: Vec<ThreadId>) {
    let mut incarnation: u64 = 0;

    loop {
        match traceforge::recv_msg_block_timed::<MMsg>() {
            MMsg::Ping { round, from: prober } => {
                traceforge::send_tagged_msg(prober, ack_tag(round), MMsg::Ack);
            }
            MMsg::Suspect { inc, round, from: prober } => {
                // SWIM refutation rule: a suspicion is admissible only
                // at or above our current incarnation; refuting bumps
                // to inc + 1. (The prober tracks refutations, so in
                // this model inc == incarnation always; the guard is
                // the paper's, kept for fidelity.)
                if inc >= incarnation {
                    let new_inc = inc + 1;
                    // Incarnation strictly increases (SWIM's ordering
                    // invariant), guarded live from round 2 on.
                    traceforge::assert(new_inc > incarnation);
                    incarnation = new_inc;
                    traceforge::send_tagged_msg(
                        prober,
                        alive_tag(round),
                        MMsg::Alive { inc: incarnation },
                    );
                    for m in &bystanders {
                        traceforge::send_tagged_msg(
                            *m,
                            alive_tag(round),
                            MMsg::Alive { inc: incarnation },
                        );
                    }
                }
            }
            // HARNESS: end of run.
            MMsg::Stop => return,
            // Unreachable: only the prober sends to the target, and it
            // sends only Ping, Suspect and Stop.
            m => panic!("target: unexpected {m:?}"),
        }
    }
}

// =====================================================================
// Bystander (members 2..N-1): reactive, fully local decision
// =====================================================================

fn bystander(b: Bounds) {
    loop {
        // Idle until the prober disseminates something (SWIM sends
        // bystanders nothing in a healthy period). Only the prober's
        // Suspect carries the suspect kind of tag (the target's Alive
        // does not), and FIFO from the prober delivers Suspects in
        // period order. HARNESS: the Stop (kind 0 too) comes from the
        // prober after its last Suspect, so FIFO delivers it last.
        match traceforge::recv_tagged_msg_block_timed::<_, MMsg>(move |_s, tag| {
            tag.is_some_and(|t| t % TAG_KINDS == 0)
        }) {
            MMsg::Suspect { inc: s_inc, round, .. } => {
                // Own suspicion timer, no coordination with P or the
                // other bystanders: wait W_SUSPECT for an Alive of
                // this period with an incarnation above the suspected
                // one.
                let refuted = loop {
                    match traceforge::recv_tagged_msg_timed::<_, MMsg>(
                        move |_sender, tag| tag == Some(alive_tag(round)),
                        WaitTime::Finite(b.w_suspect),
                    ) {
                        Some(MMsg::Alive { inc }) if inc > s_inc => break true,
                        // Non-refuting Alive (stale inc): drop, reissue.
                        Some(_) => {}
                        None => break false,
                    }
                };
                if refuted {
                    REFUTED_IN_TIME.fetch_add(1, Ordering::Relaxed);
                } else {
                    DEAD_DECLARED.fetch_add(1, Ordering::Relaxed);
                    // Same per-node safety property as the prober's.
                    traceforge::assert(TARGET_CRASHED.load(Ordering::Relaxed));
                    // SWIM removes a dead-declared member; stop.
                    // (Unreachable in practice: the assert fires.)
                    return;
                }
            }
            // HARNESS: end of run.
            MMsg::Stop => return,
            // Unreachable: the prober sends bystanders only Suspect and
            // Stop.
            m => panic!("bystander: unexpected {m:?}"),
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

/// Exploration strategy chosen on the command line (`--parallel`):
/// `none` (single-threaded, the default; keeps the exact exit-code
/// semantics of an aborting assertion), `shared` (the shared work-queue
/// pool, count-identical to sequential exploration; pool size follows
/// MUST_PARALLEL_WORKERS) or `partitioned`.
static PARALLEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn apply_parallel(builder: traceforge::ConfigBuilder) -> traceforge::ConfigBuilder {
    match PARALLEL.get().map(|s| s.as_str()).unwrap_or("none") {
        "none" => builder,
        "shared" => builder.with_parallel(true),
        "partitioned" => builder.with_partitioned_parallelization(true),
        other => cli_bail(&format!("invalid --parallel: {other} (expected none|shared|partitioned)")),
    }
}

fn build_config(mode: Mode, b: Bounds, dot_out: Option<&str>, keep_going: bool) -> Config {
    let mut builder = apply_parallel(Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX));
    if std::env::var_os("TF_VERBOSE").is_some() { builder = builder.with_verbose(1).with_pretty_graph_printing(false); }
    if keep_going {
        // Explore the whole state space even after a violation, so the
        // untimed baseline (which always FIREs on this benchmark) yields
        // a complete execution count comparable with the timed run; the
        // number of false positives is then `dead=` (= violations=).
        builder = builder.with_keep_going_after_error(true);
    }
    // Debug/visualization aid: dump the execution graph (the
    // counterexample's causal prefix on a FIRE) as Graphviz dot; in
    // timed mode every node carries its tau interval. See viz_out/.
    if let Some(path) = dot_out {
        builder = builder.with_dot_out(path);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
    }
}

fn run(mode: Mode, nodes: u32, b: Bounds, rounds: u32, dot_out: Option<&str>, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, b, dot_out, keep_going);
    ACKS_IN_TIME.store(0, Ordering::Relaxed);
    SUSPECTS_RAISED.store(0, Ordering::Relaxed);
    REFUTED_IN_TIME.store(0, Ordering::Relaxed);
    DEAD_DECLARED.store(0, Ordering::Relaxed);
    TARGET_CRASHED.store(false, Ordering::Relaxed);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        // Members are spawned before the prober so it captures their
        // ids; no bootstrap or release message exists. No daemons: the
        // prober also gets the join handles, target first (HARNESS).
        let mut by_ids: Vec<ThreadId> = Vec::new();
        let mut reactors = Vec::new();
        for _ in 0..nodes.saturating_sub(2) {
            let h = thread::spawn(move || bystander(b));
            by_ids.push(h.thread().id());
            reactors.push(h);
        }
        let t_handle = {
            let bys = by_ids.clone();
            thread::spawn(move || target(bys))
        };
        let t_id = t_handle.thread().id();
        reactors.insert(0, t_handle);
        let p = thread::spawn(move || prober(b, rounds, t_id, by_ids, reactors));
        let _ = p.join();
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    acks: usize,
    suspects: usize,
    refuted: usize,
    dead: usize,
}

fn read_counts() -> Counts {
    Counts {
        acks: ACKS_IN_TIME.load(Ordering::Relaxed),
        suspects: SUSPECTS_RAISED.load(Ordering::Relaxed),
        refuted: REFUTED_IN_TIME.load(Ordering::Relaxed),
        dead: DEAD_DECLARED.load(Ordering::Relaxed),
    }
}

/// Vacuity guards (see the doc header): a run with zero complete
/// executions verified nothing, and a run whose suspicions were never
/// refuted NOR declared dead resolved no suspicion at all. Neither
/// exit-0 outcome is a hold.
fn warn_if_vacuous(label: &str, execs: usize, blocked: usize, c: Counts) {
    // A violating execution is aborted by the assertion, so the checker
    // files it under `blocked`, not `execs`. Warning on execs == 0 alone
    // would tell the reader to discard a run that found counterexamples.
    if execs == 0 && c.dead == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}) and no violation: \
             every interleaving blocked, this run verified nothing; treat it as no data, not \
             as a hold."
        );
    }
    // refuted == 0 with suspects > 0 and no dead declaration means no
    // suspicion ever reached an outcome (every suspecting run ended
    // blocked, e.g. waiting for a lost message).
    if c.suspects > 0 && c.refuted == 0 {
        println!(
            "WARNING ({label}): a suspicion was never refuted and never declared dead: no \
             suspicion outcome was ever judged; this hold is likely vacuous, treat it as no data."
        );
    }
}

// =====================================================================
// Reporting
// =====================================================================

fn print_one(label: &str, nodes: u32, b: Bounds, rounds: u32, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<10} N={nodes} R={rounds}  L={l} U={u} Wp={wp} Ws={ws} sd={sd}  execs={execs:<6} \
         blocked={block:<6} acks={acks:<6} suspects={suspects:<6} refuted={refuted:<6} \
         dead={dead:<6} violations={dead:<6} time={dur:?}",
        l = b.l, u = b.u, wp = b.w_probe, ws = b.w_suspect, sd = b.sd,
        execs = stats.execs, block = stats.block,
        acks = c.acks, suspects = c.suspects, refuted = c.refuted, dead = c.dead, dur = dur,
    );
}

fn print_compare(nodes: u32, b: Bounds, rounds: u32, baseline: (Stats, Duration),
                 timed: (Stats, Duration), bc: Counts, tc: Counts) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("SWIM suspicion subprotocol: MUST vs MUST-timed");
    println!("======================================================");
    println!(
        "N = {nodes}    R = {rounds}    L = {}    U = {}    Wp = {}    Ws = {}    sd = {}",
        b.l, b.u, b.w_probe, b.w_suspect, b.sd
    );
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
    println!();
    // Executions EXPLORED is execs + block: a violating execution is aborted
    // by its assertion and an evicted one blocks, so both are filed under
    // `block`. Dividing complete executions alone overstates the reduction.
    let b_explored = b_stats.execs + b_stats.block;
    let t_explored = t_stats.execs + t_stats.block;
    let exec_ratio = b_explored as f64 / t_explored.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "explored reduction: {exec_ratio:.2}x  ({b_explored} vs {t_explored} executions explored, execs+blocked)"
    );
    println!("time  speedup  : {time_ratio:.2}x");
    println!(
        "acks/suspects/refuted/dead: baseline {}/{}/{}/{}   timed {}/{}/{}/{}",
        bc.acks, bc.suspects, bc.refuted, bc.dead, tc.acks, tc.suspects, tc.refuted, tc.dead
    );
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    nodes: u32,
    rounds: u32,
    u: u64,
    l_ratio: f64,
    sd_ratio: f64,
    w_probe_ratio: f64,
    w_suspect_ratio: f64,
    dot_out: Option<String>,
    keep_going: bool,
    parallel: String,
}

fn next_val(args: &mut std::env::Args, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
}

fn parse_num<T: std::str::FromStr>(v: String, flag: &str) -> T {
    v.parse()
        .unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
}

fn parse_args() -> Args {
    // Default mode is `timed`, not the template's `compare`: baseline
    // always FIREs for this benchmark, so compare's first leg aborts
    // the process before any table is printed (see the doc header).
    let mut a = Args {
        mode: String::from("timed"),
        nodes: DEFAULT_NODES,
        rounds: DEFAULT_ROUNDS,
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        w_probe_ratio: DEFAULT_W_PROBE_RATIO,
        w_suspect_ratio: DEFAULT_W_SUSPECT_RATIO,
        dot_out: None,
        keep_going: false,
        parallel: String::from("none"),
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--nodes" => a.nodes = parse_num(next_val(&mut args, "--nodes"), "--nodes"),
            "--rounds" => a.rounds = parse_num(next_val(&mut args, "--rounds"), "--rounds"),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--w-probe-ratio" => {
                a.w_probe_ratio = parse_num(next_val(&mut args, "--w-probe-ratio"), "--w-probe-ratio")
            }
            "--w-suspect-ratio" => {
                a.w_suspect_ratio =
                    parse_num(next_val(&mut args, "--w-suspect-ratio"), "--w-suspect-ratio")
            }
            "--dot-out" => a.dot_out = Some(next_val(&mut args, "--dot-out")),
            "--keep-going" => a.keep_going = true,
            "--parallel" => a.parallel = next_val(&mut args, "--parallel"),
            "--help" | "-h" => {
                eprintln!(
                    "Usage: swim_timed [--mode MODE] [--nodes N] [--rounds R] [--u U] \
                     [--l-ratio LR] [--sd-ratio SR] [--w-probe-ratio R] [--w-suspect-ratio R] \
                     [--dot-out PATH] [--keep-going] [--parallel none|shared|partitioned]\n\
                     --keep-going explores the whole state space after a violation and reports \
                     the number of false positives (violations=) instead of aborting.\n\
                     --dot-out dumps the execution graph (counterexample prefix on a FIRE) as \
                     Graphviz dot with tau intervals in timed mode.\n\
                     Modes: baseline | timed | compare (default timed; compare's baseline leg \
                     always FIREs and aborts for this benchmark)\n\
                     Defaults: N=3, R=1, U=1, L/U=0, sd/U=0, Wp/U=1, Ws/U=4.\n\
                     Exit 0 = no false positive over the explored state space; exit 101 = a \
                     healthy target was declared dead (SWIM false positive); exit 2 = CLI misuse."
                );
                std::process::exit(0);
            }
            other => cli_bail(&format!("unknown argument: {other}")),
        }
    }
    a
}

fn main() {
    let a = parse_args();
    PARALLEL.set(a.parallel.clone()).expect("PARALLEL set once");
    if a.nodes < 2 {
        cli_bail("need at least 2 members (prober + target)");
    }
    if a.rounds < 1 {
        cli_bail("need at least 1 round");
    }
    let b = Bounds::from_ratios(a.u, a.l_ratio, a.sd_ratio, a.w_probe_ratio, a.w_suspect_ratio);
    match a.mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, a.nodes, b, a.rounds, a.dot_out.as_deref(), a.keep_going);
            print_one("baseline", a.nodes, b, a.rounds, &s, d, read_counts());
            warn_if_vacuous("baseline", s.execs, s.block, read_counts());
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, a.nodes, b, a.rounds, a.dot_out.as_deref(), a.keep_going);
            print_one("timed", a.nodes, b, a.rounds, &s, d, read_counts());
            warn_if_vacuous("timed", s.execs, s.block, read_counts());
        }
        "compare" => {
            let baseline = run(Mode::Baseline, a.nodes, b, a.rounds, a.dot_out.as_deref(), a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, a.nodes, b, a.rounds, a.dot_out.as_deref(), a.keep_going);
            let tc = read_counts();
            let (b_vac, t_vac) = (
                (baseline.0.execs, baseline.0.block),
                (timed.0.execs, timed.0.block),
            );
            print_compare(a.nodes, b, a.rounds, baseline, timed, bc, tc);
            warn_if_vacuous("baseline", b_vac.0, b_vac.1, bc);
            warn_if_vacuous("timed", t_vac.0, t_vac.1, tc);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
}
