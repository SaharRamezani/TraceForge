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
//! messages that timed waits and validation reads target, encode
//! their period in the tag (ack_tag/alive_tag); Ping and Suspect carry
//! `round` in the payload. The prober's per-round Suspect carries its
//! current known incarnation, so incarnations rise across rounds and
//! the SWIM refutation rule (refute iff inc >= incarnation, bump to
//! inc + 1) is exercised for real from round 2 on.
//!
//! MMsg::Done is verification harness, not SWIM: a bounded model needs
//! the reactive threads to terminate, so the prober releases them when
//! all R periods are resolved.
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
//! ## Timeout-validation assumptions (verification harness, NOT SWIM)
//!
//! The false-positive assertion sits on timeout branches, and the
//! timeout branch (rf = None) of a finite-wait receive is by design
//! always explorable, in either mode: taking it only means "the
//! awaited message had not arrived by the deadline IN THIS BRANCH".
//! Without further constraint the assertion would fire trivially at
//! any parameters and the timed mode would have no content. The
//! assume_* functions below therefore follow each property-relevant
//! timeout with a BLOCKING timed receive for the very message the
//! timer awaited: an assume("the message really was late here"). The
//! read is feasible exactly when the message could genuinely arrive
//! or be read after the deadline, so spurious timeouts become blocked
//! (discarded) executions instead of false counterexamples. SWIM's
//! prober performs no such read; this is checker scaffolding, kept
//! out of the protocol logic. Costs: hold cells report blocked > 0 by
//! design, and the suspicion timer anchors at the validation read,
//! not the probe deadline (visible in the L = U boundary).
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
//!
//! CLI misuse exits 2, distinct from a property violation's 101, so
//! exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
//!
//! ## Expected verdicts (matrix-verified; R-independent)
//!
//!   baseline: FIRE (exit 101) at any parameters; no-false-positive is
//!       inherently a timing property, an untimed checker cannot hold it.
//!   timed:    FIRE iff W_SUSPECT <= 2U + 2sd - min(L, max(0, 2L - W_PROBE)),
//!       hold above, at EVERY round count R (verified at R in {1,2,3};
//!       at L = 0 this is the familiar hold-from 2U + 2sd + 1, checked
//!       for U in {1,2}, sd in {0,1}). The max(0, 2L - W_PROBE) term is
//!       the validation-read anchor: the Ack cannot be read before
//!       ping_send + 2L, so with W_PROBE < 2L the suspicion timer
//!       starts 2L - W_PROBE past the probe deadline (verified at
//!       L = U for U in {1,2}; at U=4, L=3, Wp=4: hold from Ws=7; at
//!       U=3, L=2, Wp=3: hold from Ws=6). The min(L, ...) cap was found
//!       by the 2026-08-09 audit: for W_PROBE < L and N >= 3, a
//!       BYSTANDER's suspicion timer anchors at its Suspect read (not
//!       the validation read), losing at most L of anchor delay, so the
//!       penalty saturates at L. The earlier formula without the cap
//!       silently over-claimed holds in W_PROBE < L cells. Note that
//!       L = U collapses transit nondeterminism, so holds there explore
//!       very few executions.
//!
//! Timed FIREs are certified: the checker validates every reported
//! counterexample against a single consistent timeline and prints the
//! witness timestamps next to the graph. The historical multi-round
//! artifact band (spurious FIREs up to ~2R(U + sd) from the interval
//! walker's per-event relaxation, P1 feedback item 6) is gone under
//! the exact difference-constraint engine, the only engine on this
//! branch (the legacy walker survives only in pre-timed-exact branch
//! history; the --timed-exact flag was removed 2026-07-27).
//!
//! A hold with SUSPECTS_RAISED > 0 and REFUTED_IN_TIME = 0 is
//! suspicious (every validated suspicion's outcome was discarded); the
//! binary prints a WARNING. Runs with execs = 0 verified nothing and
//! also WARN. Under the exact engine DEAD_DECLARED stays 0 on a hold
//! (the infeasible dead branch is pruned before the counter).

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
// identified by tag, keeping every timed wait and validation read
// pinned to its own period's message.
const TAG_KINDS: u32 = 2;

fn ack_tag(round: u32) -> u32 {
    round * TAG_KINDS + 1
}

fn alive_tag(round: u32) -> u32 {
    round * TAG_KINDS + 2
}

/// Member-to-member protocol traffic.
#[derive(Clone, Debug, PartialEq)]
enum MMsg {
    Ping { round: u32 },
    Ack,
    Suspect { inc: u64, round: u32 },
    Alive { inc: u64 },
    /// Verification harness, not SWIM: releases the reactive threads
    /// once the prober has resolved all R periods.
    Done,
}

/// Bootstrap message, a separate type from MMsg (raft idiom) so the
/// init-wait matches only Init and protocol traffic that races ahead
/// of it stays queued.
#[derive(Clone, Debug, PartialEq)]
struct Init {
    prober: ThreadId,
    target: ThreadId,
    bystanders: Vec<ThreadId>,
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
// Timeout-validation assumptions (verification harness, NOT SWIM;
// see the doc header). SWIM's prober never re-reads the Ack after
// suspecting; these blocking reads encode assume("the message really
// was late in this branch") so that spurious timeout branches become
// blocked executions instead of trivial counterexamples.
// =====================================================================

/// Assume period `round`'s Ack was genuinely late: block until it is
/// readable at or after the current (post-deadline) local time. Timed
/// infeasible, so the execution blocks and is discarded, when the Ack
/// provably arrived inside the probe window. Also consumes the late
/// Ack so it cannot linger in front of later traffic.
fn assume_ack_was_late(target: ThreadId, round: u32) {
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, MMsg>(move |sender, tag| {
            sender == target && tag == Some(ack_tag(round))
        }) {
            MMsg::Ack => return,
            // Unreachable: only period r's Ack carries ack_tag(r).
            _ => {}
        }
    }
}

/// Assume period `round`'s refuting Alive (incarnation above `floor`)
/// was genuinely late: block until it is readable at or after the
/// current (post-deadline) local time. Timed-infeasible, so the
/// execution blocks and is discarded, when the refutation provably
/// arrived inside the suspicion window.
fn assume_refutation_was_late(target: ThreadId, floor: u64, round: u32) {
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, MMsg>(move |sender, tag| {
            sender == target && tag == Some(alive_tag(round))
        }) {
            MMsg::Alive { inc } if inc > floor => return,
            // Unreachable: period r's refutation always carries
            // inc = suspect inc + 1 > floor; skip defensively.
            _ => {}
        }
    }
}

// =====================================================================
// Prober (member 0): drives the R protocol periods
// =====================================================================

fn prober(b: Bounds, rounds: u32, main_tid: ThreadId) {
    let Init { target, bystanders, .. } =
        traceforge::recv_tagged_msg_block::<_, Init>(move |sender, _tag| sender == main_tid);

    // The prober's current belief of the target's incarnation; rises
    // with every refutation it accepts (SWIM's incarnation ordering).
    let mut known_inc: u64 = 0;

    for round in 0..rounds {
        traceforge::send_msg(target, MMsg::Ping { round });

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
        traceforge::send_msg(target, MMsg::Suspect { inc: known_inc, round });
        for m in &bystanders {
            traceforge::send_msg(*m, MMsg::Suspect { inc: known_inc, round });
        }

        // Harness assumption, not SWIM (doc header): keep only the
        // executions in which this probe timeout was temporally real,
        // BEFORE the suspects counter.
        assume_ack_was_late(target, round);

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
                // Harness assumption: keep only the
                // executions in which the refutation was genuinely
                // late or unreadable in the window.
                assume_refutation_was_late(target, known_inc, round);
                DEAD_DECLARED.fetch_add(1, Ordering::Relaxed);
                // Safety: a member declares T dead only if T actually
                // crashed. TARGET_CRASHED is never true here, so this
                // firing IS the SWIM false positive.
                traceforge::assert(TARGET_CRASHED.load(Ordering::Relaxed));
                // SWIM removes a dead-declared member; stop probing.
                // (Unreachable in practice: the assert above fires.)
                return;
            }
        }
    }

    // All periods resolved: release the reactive threads (harness).
    traceforge::send_msg(target, MMsg::Done);
    for m in &bystanders {
        traceforge::send_msg(*m, MMsg::Done);
    }
}

// =====================================================================
// Target (member 1): purely reactive, never crashes. Acks every Ping,
// refutes every admissible Suspect (the paper's rule); blocking
// receives only, so it contributes no timeout branches.
// =====================================================================

fn target(main_tid: ThreadId) {
    let Init { prober, bystanders, .. } =
        traceforge::recv_tagged_msg_block::<_, Init>(move |sender, _tag| sender == main_tid);
    let mut incarnation: u64 = 0;

    loop {
        match traceforge::recv_msg_block_timed::<MMsg>() {
            MMsg::Ping { round } => {
                traceforge::send_tagged_msg(prober, ack_tag(round), MMsg::Ack);
            }
            MMsg::Suspect { inc, round } => {
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
            MMsg::Done => return,
            // Unreachable: only the prober sends to the target, and it
            // sends only Ping, Suspect and Done.
            m => panic!("target: unexpected {m:?}"),
        }
    }
}

// =====================================================================
// Bystander (members 2..N-1): reactive, fully local decision
// =====================================================================

fn bystander(b: Bounds, main_tid: ThreadId) {
    let Init { prober, target, .. } =
        traceforge::recv_tagged_msg_block::<_, Init>(move |sender, _tag| sender == main_tid);

    loop {
        // Idle until the prober disseminates something (SWIM sends
        // bystanders nothing in a healthy period). FIFO from the
        // prober delivers Suspects in period order.
        match traceforge::recv_tagged_msg_block_timed::<_, MMsg>(move |s, _tag| s == prober) {
            MMsg::Suspect { inc: s_inc, round } => {
                // Own suspicion timer, no coordination with P or the
                // other bystanders: wait W_SUSPECT for an Alive of
                // this period with an incarnation above the suspected
                // one.
                let refuted = loop {
                    match traceforge::recv_tagged_msg_timed::<_, MMsg>(
                        move |sender, tag| sender == target && tag == Some(alive_tag(round)),
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
                    // Harness assumption (doc header).
                    assume_refutation_was_late(target, s_inc, round);
                    DEAD_DECLARED.fetch_add(1, Ordering::Relaxed);
                    // Same per-node safety property as the prober's.
                    traceforge::assert(TARGET_CRASHED.load(Ordering::Relaxed));
                    // SWIM removes a dead-declared member; stop.
                    // (Unreachable in practice: the assert fires.)
                    return;
                }
            }
            MMsg::Done => return,
            // Unreachable: the prober sends bystanders only Suspect
            // and Done.
            m => panic!("bystander: unexpected {m:?}"),
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, b: Bounds, dot_out: Option<&str>) -> Config {
    let mut builder = Config::builder().with_progress_report(usize::MAX);
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

fn run(mode: Mode, nodes: u32, b: Bounds, rounds: u32, dot_out: Option<&str>) -> (Stats, Duration) {
    let cfg = build_config(mode, b, dot_out);
    ACKS_IN_TIME.store(0, Ordering::Relaxed);
    SUSPECTS_RAISED.store(0, Ordering::Relaxed);
    REFUTED_IN_TIME.store(0, Ordering::Relaxed);
    DEAD_DECLARED.store(0, Ordering::Relaxed);
    TARGET_CRASHED.store(false, Ordering::Relaxed);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let t = thread::spawn(move || target(main_tid));
        let p = thread::spawn(move || prober(b, rounds, main_tid));
        let mut bys = Vec::new();
        for _ in 0..nodes.saturating_sub(2) {
            bys.push(thread::spawn(move || bystander(b, main_tid)));
        }
        let t_id = t.thread().id();
        let p_id = p.thread().id();
        let by_ids: Vec<ThreadId> = bys.iter().map(|h| h.thread().id()).collect();
        let init = Init { prober: p_id, target: t_id, bystanders: by_ids.clone() };
        // Send every Init before joining anyone.
        traceforge::send_msg(t_id, init.clone());
        traceforge::send_msg(p_id, init.clone());
        for id in &by_ids {
            traceforge::send_msg(*id, init.clone());
        }
        let _ = t.join();
        let _ = p.join();
        for h in bys {
            let _ = h.join();
        }
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
/// executions verified nothing, and a run whose validated suspicions
/// were never refuted NOR declared dead resolved no suspicion at all
/// (every outcome branch was discarded at a validation read). Neither
/// exit-0 outcome is a hold.
fn warn_if_vacuous(label: &str, execs: usize, blocked: usize, c: Counts) {
    if execs == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}): every interleaving \
             blocked, this run verified nothing; treat it as no data, not as a hold."
        );
    }
    // refuted == 0 alone means no suspicion outcome survived: under
    // the exact engine dead branches are pruned before their counter,
    // and under the legacy walker suppressed artifacts end blocked.
    if c.suspects > 0 && c.refuted == 0 {
        println!(
            "WARNING ({label}): a validated suspicion was never refuted and never declared \
             dead: every suspicion outcome was discarded at a validation read; this hold is \
             likely vacuous, treat it as no data."
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
         dead={dead:<6} time={dur:?}",
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
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("execs reduction: {exec_ratio:.2}x");
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
            "--help" | "-h" => {
                eprintln!(
                    "Usage: swim_timed [--mode MODE] [--nodes N] [--rounds R] [--u U] \
                     [--l-ratio LR] [--sd-ratio SR] [--w-probe-ratio R] [--w-suspect-ratio R] \
                     [--dot-out PATH]\n\
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
    if a.nodes < 2 {
        cli_bail("need at least 2 members (prober + target)");
    }
    if a.rounds < 1 {
        cli_bail("need at least 1 round");
    }
    let b = Bounds::from_ratios(a.u, a.l_ratio, a.sd_ratio, a.w_probe_ratio, a.w_suspect_ratio);
    match a.mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, a.nodes, b, a.rounds, a.dot_out.as_deref());
            print_one("baseline", a.nodes, b, a.rounds, &s, d, read_counts());
            warn_if_vacuous("baseline", s.execs, s.block, read_counts());
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, a.nodes, b, a.rounds, a.dot_out.as_deref());
            print_one("timed", a.nodes, b, a.rounds, &s, d, read_counts());
            warn_if_vacuous("timed", s.execs, s.block, read_counts());
        }
        "compare" => {
            let baseline = run(Mode::Baseline, a.nodes, b, a.rounds, a.dot_out.as_deref());
            let bc = read_counts();
            let timed = run(Mode::Timed, a.nodes, b, a.rounds, a.dot_out.as_deref());
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
