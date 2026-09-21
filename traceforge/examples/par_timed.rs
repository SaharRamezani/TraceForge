//! PAR (Positive Acknowledgement with Retransmission): the Bosnacki-Dams
//! discrete-time Promela model, ported to TraceForge.
//!
//! ## The idea in plain words
//!
//! A sender passes a stream of messages to a receiver over a wire that may
//! lose them. Each message carries a one-bit sequence number that
//! alternates 0, 1, 0, 1, ... The receiver hands a message with the
//! expected bit to its user, flips the bit it expects, and answers with an
//! acknowledgement (an "ack"); a message with the other bit is an old copy,
//! which it answers again but does not hand over. The sender waits for an
//! ack; if none comes within its timeout, it sends the same message again.
//!
//! The ack carries NO sequence number: any ack counts. That makes the
//! timeout critical. If it is shorter than the round trip (message there,
//! processing, ack back), the sender resends too early, two acks come back
//! for one message, and the second ack is taken for the next message. If
//! that next message was lost on the wire, it is never sent again: the
//! receiver's user silently misses it, and the receiver's assertion fails
//! when a later message arrives in its place.
//!
//! ## Source (transliterated)
//!
//! D. Bosnacki and D. Dams, "Integrating Real Time into Spin: A Prototype
//! Implementation", FORTE/PSTV 1998, pp. 423-439; reprinted in D. Bosnacki,
//! "Enhancing State Space Reduction Techniques for Model Checking", PhD
//! thesis, TU Eindhoven 2001, Section 3.2 (pp. 62-66: the prose, the
//! scenario, and the complete Promela listing) and Table 2 (p. 70). Their
//! rule (p. 63): the time-out period "should be longer than the sum of the
//! delays through the channels and the message processing time by the
//! receiver", i.e. To > dK + dL + dR. Their Table 2 verifies the model only
//! at To = 9, 90, 900 with (dK, dL, dR) = (3, 3, 1), (30, 30, 10),
//! (300, 300, 100), all on the safe side.
//!
//! The listing, statement by statement, and where each statement lives here:
//!
//!   Sender   R_h: udelay(sc)            sleep(d), d chosen in 0..=UD
//!                 mt = (mt+1) % MAX      same (MAX = 8)
//!            S_f: A!mt,sn; set(sc,To)   send_lossy_msg_timed(frame, dK, dK)
//!            W_s: D?_ -> accept:         recv (any ack) within Finite(To);
//!                   delay(sc,1); flip       accept: sleep(1), flip sn
//!                 D?_ -> ACKerr: S_f       ACKerr (nondet): resend at once
//!                 expire(sc) -> S_f        timeout: resend
//!   Receiver W_f: B?mr,rsn               blocking receive of a frame
//!                 rsn == esn -> S_h      same
//!                 rsn != esn -> S_a      same
//!                 MSGerr -> W_f          MSGerr (nondet): discard the frame
//!            S_h: assert(mr == me)      traceforge::assert(mr == me)
//!                 delay(rc,dR); flip     sleep(dR); flip esn; me += 1
//!            S_a: C!1; delay(rc,1)      send_lossy_msg_timed(ack, dL, dL);
//!                                         sleep(1)
//!   K, L     take a copy, delay dK      the frame (ack) send's exact
//!            (dL), deliver or lose       transit dK (dL); lossy
//!
//! ## Deviations (the channel is the only one that affects behaviour)
//!
//! * Channels. In the listing K and L are one-place processes behind
//!   zero-capacity (rendezvous) channels A, B, C, D: a copy that finds its
//!   reader busy waits inside K or L until the reader is ready, and while K
//!   (L) is busy the writer's A! (C!) blocks. TraceForge has no rendezvous
//!   and its timeouts and receives cannot be made to wait for one exactly
//!   (a timeout is always available and a receive may complete up to sd
//!   late, so a waiting copy would also be readable late, which the
//!   listing's urgent semantics forbids). Here every copy travels on its
//!   own (unbounded capacity) with exact transit dK or dL and storage
//!   lifetime sd = 0: it is read at the instant it arrives if its reader is
//!   waiting, and is lost otherwise. A copy the listing would hold is thus
//!   lost here, and a retransmission never waits for K. The effect is
//!   measured, not assumed: `oracle/par_e1_dt_template.pml` is the listing
//!   with exactly this channel change, and DT-Spin gives it the listing's
//!   verdict on 977 of 1,039 cells; the other 62 are exactly characterized
//!   and are all violations of the listing that this encoding misses (a held
//!   duplicate), never added ones (RESULTS 2). Expressing the holding would
//!   need a receive that must take an already stored message at once, which
//!   TraceForge's always-available timeout and late receive do not give.
//! * Bounded runs. The listing loops forever (payloads modulo MAX = 8, any
//!   number of retransmissions, losses and idle ticks). Here K messages are
//!   sent, each at most R times resent (a timeout or an ACKerr counts), with
//!   at most B lost copies (`with_lossy(B)`; a copy that finds its reader
//!   busy is lost on top of B), and the idle time before a new message is
//!   at most UD ticks. After R resends of one message the sender stops
//!   (verification scaffolding, not a verdict). A FIRE is therefore a real
//!   violation of the listing; a HOLD holds for these bounds.
//! * The assertion needs 4 messages to fail (message 2 lost and its place
//!   taken by message 4), so K >= 4 is needed for a FIRE.
//! * Harness. A Done message (not lossy, transit dK + dR + 2, so it reaches
//!   an idle receiver) ends the reactive receiver. The Init bootstrap from
//!   main is an untimed receive, transparent for timing.
//!
//! Integer time: the listing is a discrete-time model, and TraceForge's
//! timed engine uses integer instants, so the two share one time domain.
//! Events at the same instant may be ordered either way in both (DT-Spin
//! interleaves the actions of one time slice; TraceForge orders equal
//! instants both ways), which is what makes the tie To = dK + dL + dR fail.
//!
//! ## Properties
//!
//!   P1 (the listing's only property): at S_h, assert(mr == me).
//!
//! Outcome counters (event counts over all explored executions, read as
//! zero/nonzero evidence): delivered, duplicates (old copies re-acked),
//! timeouts, ackerr, msgerr, confused (the sender accepted an ack that
//! answers an older message: the PAR mix-up), gave_up, p1_fails.
//!
//! ## CLI
//!
//!   --mode baseline|timed|compare   default timed
//!   --dk N --dl N --dr N --to N     time parameters (default 3 3 1 9, the
//!                                   listing's)
//!   --messages K                    messages sent (default 4)
//!   --retries R                     resends per message before the sender
//!                                   stops (default 1)
//!   --retries-first R1              resend bound for message 1 only (default
//!                                   R; at To = 1 message 1 alone needs about
//!                                   dK + dL + dR resends before its ack returns)
//!   --lossy B                       lost-copy budget (default 1)
//!   --udelay UD                     max idle ticks before a new message
//!                                   (default 0)
//!   --no-errs                       drop the ACKerr and MSGerr branches
//!   --keep-going                    explore past a violation, count it
//!   --parallel none|shared|partitioned
//!
//! Exit codes: 0 hold (for the bounds), 101 P1 violated, 2 CLI misuse.
//! With --keep-going the exit code is 0 and the verdict is p1_fails.
//!
//! ## RESULTS
//!
//! Data, oracles and drivers: docs/research/ta_problems/need-from-user-papers/
//! par-b02/ (README.md there). RT = dK + dL + dR.
//!
//! 1. Oracles. Spin 6.5.2 on the verbatim listing and DT-Spin 4.1.1 on the
//!    same listing with local timers agree with the rule "FIRE iff To <= RT"
//!    on all 544 cells of dK, dL in 1..4, dR in 0..3, To in 1..RT+2 (tie
//!    included), DT-Spin on 495 more cells with dR up to 8: 1,039 cells, no
//!    exception. The rule is exact for the listing; Bosnacki and Dams checked
//!    only To = 9, 90, 900.
//! 2. The channel encoding. The listing with only its channels replaced by
//!    this file's (oracle/par_e1_dt_template.pml) has the listing's verdict on
//!    977 of the 1,039 cells. The 62 others are exactly the cells with
//!    To * floor(RT / To) <= dR, all "listing FIRE, encoding HOLD": every
//!    retransmission sent before the first ack returns reaches a receiver
//!    still processing the first copy; the listing's one-place K holds it
//!    until the receiver is free, this encoding loses it. The encoding never
//!    adds a violation.
//! 3. This file. 515 cells (the rule's neighbourhood for every (dK, dL, dR)
//!    of the grid, plus every difference cell of 2 and its neighbours): the
//!    verdict equals the encoding oracle's on 515 / 515 (325 FIRE, 190 HOLD),
//!    hence the listing's except on the 62 cells of 2. All 64 tie cells FIRE.
//!    A FIRE needs (R1 + 1) * To >= RT (the sender must still be waiting when
//!    the first ack returns); the To = 1 cells need --retries-first about RT.
//!    Witness at the published (3, 3, 1) and To = 7 (the tie), --no-errs:
//!    timeout at 7, the first ack read at 7 after the resend, frame 2 dropped
//!    at 8, the duplicate's ack taken for message 2 at 13, frame 3 taken as a
//!    duplicate, frame 4 read at 31 in place of message 2: assert(4 == 2)
//!    fails. Bosnacki and Dams' scenario step by step.
//! 4. Precision and cost. The same program untimed reports violations at
//!    every To, including the listing's safe settings: at (3, 3, 1, 9), 133
//!    false counterexamples among 18,036 explored executions, against a timed
//!    hold over 3,579. Timed explores 2x to 17.5x fewer executions over the
//!    cells measured and never reports a false counterexample (untimed: 4 to
//!    36,463), but costs about 9x to 11x more per execution, so it is faster
//!    in wall-clock only where the pruning exceeds that: K 4, R 2 is 762 s
//!    timed against 1,195 s untimed (1.6x), while the smaller cells are 0.1x
//!    to 0.9x. Both modes are flat in the magnitude of the constants (the same
//!    counts at (3, 3, 1, 9), (30, 30, 10, 90) and (300, 300, 100, 900)).
//! 5. Side findings on the source (not TraceForge's): DT-Spin shows the
//!    listing can deadlock (all four processes in a circular rendezvous
//!    wait) in 76 of the 544 cells, only with To < dL and never at a safe To;
//!    plain Spin hides it (its Timers process ticks forever). Table 2's state
//!    growth with the constants is single-tick unfolding: DT-Spin 4.1.1 gives
//!    853 / 3,625 / 31,345 states single-tick and 641 at all three rows in its
//!    default multiple-tick mode (published 1,318 / 7,447 / 68,737), so there
//!    is no scale claim here.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

static DELIVERED: AtomicUsize = AtomicUsize::new(0);
static DUPLICATES: AtomicUsize = AtomicUsize::new(0);
static TIMEOUTS: AtomicUsize = AtomicUsize::new(0);
static ACKERRS: AtomicUsize = AtomicUsize::new(0);
static MSGERRS: AtomicUsize = AtomicUsize::new(0);
static CONFUSED: AtomicUsize = AtomicUsize::new(0);
static GAVE_UP: AtomicUsize = AtomicUsize::new(0);
static P1_FAILS: AtomicUsize = AtomicUsize::new(0);

/// MAX in the listing: payloads cycle modulo 8.
const MAX: u8 = 8;

/// Sender to receiver.
#[derive(Clone, Debug, PartialEq)]
enum SMsg {
    /// The listing's `A!mt,sn`.
    Frame { mt: u8, sn: u8 },
    /// Harness: ends the receiver. Not lossy.
    Done,
}

/// Receiver to sender: the listing's `C!1`. `answers` is ghost data (the
/// payload of the frame being acknowledged), read only by the CONFUSED
/// counter, never by a protocol decision.
#[derive(Clone, Debug, PartialEq)]
struct Ack {
    answers: u8,
}

/// Bootstrap from main.
#[derive(Clone, Debug, PartialEq)]
struct Init {
    sender: ThreadId,
    receiver: ThreadId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug)]
struct Params {
    dk: u64,
    dl: u64,
    dr: u64,
    to: u64,
    messages: u32,
    retries: u32,
    /// Resend bound for the first message only (defaults to `retries`).
    retries_first: Option<u32>,
    lossy: usize,
    udelay: u64,
    errs: bool,
}

fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

// =====================================================================
// Sender
// =====================================================================

fn sender(p: Params, main_tid: ThreadId) {
    let Init { receiver, .. } =
        traceforge::recv_tagged_msg_block::<_, Init>(move |s, _tag| s == main_tid);

    let mut mt: u8 = 0;
    let mut sn: u8 = 0;
    for i in 0..p.messages {
        let bound = if i == 0 { p.retries_first.unwrap_or(p.retries) } else { p.retries };
        // R_h: udelay(sc), an idle time of any number of ticks (here at
        // most UD), then fetch the next message.
        let mut d = 0;
        while d < p.udelay && traceforge::nondet() {
            d += 1;
        }
        if d > 0 {
            traceforge::sleep(d);
        }
        mt = (mt + 1) % MAX;

        let mut resends: u32 = 0;
        // S_f: A!mt,sn; set(sc,To)
        traceforge::send_lossy_msg_timed(receiver, SMsg::Frame { mt, sn }, p.dk, p.dk);
        loop {
            // W_s: D?_ (any ack) or expire(sc) at To.
            match traceforge::recv_tagged_msg_timed::<_, Ack>(
                move |s, _tag| s == receiver,
                WaitTime::Finite(p.to),
            ) {
                Some(Ack { answers }) => {
                    if p.errs && traceforge::nondet() {
                        // ACKerr: the ack is taken as corrupt, goto S_f.
                        ACKERRS.fetch_add(1, Ordering::Relaxed);
                    } else {
                        if answers != mt {
                            CONFUSED.fetch_add(1, Ordering::Relaxed);
                        }
                        // delay(sc,1); sn = 1 - sn; goto R_h
                        traceforge::sleep(1);
                        sn = 1 - sn;
                        break;
                    }
                }
                None => {
                    // expire(sc): goto S_f
                    TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Back at S_f: resend, unless this message was already resent R
            // times (bounded-run scaffolding: the sender stops).
            if resends == bound {
                GAVE_UP.fetch_add(1, Ordering::Relaxed);
                send_done(p, receiver);
                return;
            }
            resends += 1;
            traceforge::send_lossy_msg_timed(receiver, SMsg::Frame { mt, sn }, p.dk, p.dk);
        }
    }
    send_done(p, receiver);
}

/// Harness: Done arrives dK + dR + 2 after it is sent. Every frame was sent
/// no later, so it arrives at least dR + 2 after the last frame, when the
/// receiver (busy at most dR + 1 after a frame) is waiting again.
fn send_done(p: Params, receiver: ThreadId) {
    let t = p.dk + p.dr + 2;
    traceforge::send_msg_timed(receiver, SMsg::Done, t, t);
}

// =====================================================================
// Receiver
// =====================================================================

fn receiver(p: Params, main_tid: ThreadId) {
    let Init { sender, .. } =
        traceforge::recv_tagged_msg_block::<_, Init>(move |s, _tag| s == main_tid);

    let mut me: u8 = 1;
    let mut esn: u8 = 0;
    loop {
        // W_f: B?mr,rsn
        let (mr, rsn) =
            match traceforge::recv_tagged_msg_block_timed::<_, SMsg>(move |s, _tag| s == sender) {
                SMsg::Frame { mt, sn } => (mt, sn),
                SMsg::Done => return,
            };
        if p.errs && traceforge::nondet() {
            // MSGerr: the frame is taken as corrupt, goto W_f.
            MSGERRS.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if rsn == esn {
            // S_h: assert(mr == me); delay(rc,dR); flip esn; me += 1
            if mr != me {
                P1_FAILS.fetch_add(1, Ordering::Relaxed);
            }
            traceforge::assert(mr == me);
            DELIVERED.fetch_add(1, Ordering::Relaxed);
            if p.dr > 0 {
                traceforge::sleep(p.dr);
            }
            esn = 1 - esn;
            me = (me + 1) % MAX;
        } else {
            DUPLICATES.fetch_add(1, Ordering::Relaxed);
        }
        // S_a: C!1 (through L); delay(rc,1); goto W_f
        traceforge::send_lossy_msg_timed(sender, Ack { answers: mr }, p.dl, p.dl);
        traceforge::sleep(1);
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

static PARALLEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn build_config(mode: Mode, p: Params, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_progress_report(usize::MAX).with_lossy(p.lossy);
    builder = match PARALLEL.get().map(|s| s.as_str()).unwrap_or("none") {
        "none" => builder,
        "shared" => builder.with_parallel(true),
        "partitioned" => builder.with_partitioned_parallelization(true),
        other => cli_bail(&format!("invalid --parallel: {other}")),
    };
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    // Every protocol send carries its own exact transit; the global window
    // only times main's Init sends, which are read untimed. sd = 0: a copy
    // is readable only at the instant it arrives (see Deviations).
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(0, 0, 0).build(),
    }
}

fn reset_counts() {
    for c in [&DELIVERED, &DUPLICATES, &TIMEOUTS, &ACKERRS, &MSGERRS, &CONFUSED, &GAVE_UP, &P1_FAILS] {
        c.store(0, Ordering::Relaxed);
    }
}

fn run(mode: Mode, p: Params, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, p, keep_going);
    reset_counts();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let r = thread::spawn(move || receiver(p, main_tid));
        let s = thread::spawn(move || sender(p, main_tid));
        let init = Init { sender: s.thread().id(), receiver: r.thread().id() };
        traceforge::send_msg(r.thread().id(), init.clone());
        traceforge::send_msg(s.thread().id(), init);
        let _ = r.join();
        let _ = s.join();
    });
    (stats, start.elapsed())
}

fn report(label: &str, stats: &Stats, elapsed: Duration) -> usize {
    let c = |a: &AtomicUsize| a.load(Ordering::Relaxed);
    let p1 = c(&P1_FAILS);
    println!(
        "{label}: execs={} blocked={} explored={} time={:.3}s p1_fails={} delivered={} duplicates={} \
         timeouts={} ackerr={} msgerr={} confused={} gave_up={}",
        stats.execs,
        stats.block,
        stats.execs + stats.block,
        elapsed.as_secs_f64(),
        p1,
        c(&DELIVERED),
        c(&DUPLICATES),
        c(&TIMEOUTS),
        c(&ACKERRS),
        c(&MSGERRS),
        c(&CONFUSED),
        c(&GAVE_UP),
    );
    if stats.execs == 0 && p1 == 0 {
        println!("WARNING ({label}): no complete execution and no violation: this run verified nothing.");
    }
    if p1 == 0 && c(&DUPLICATES) == 0 {
        println!("NOTE ({label}): no duplicate frame ever reached the receiver: no retransmission was exercised.");
    }
    p1
}

fn main() {
    let mut p = Params {
        dk: 3,
        dl: 3,
        dr: 1,
        to: 9,
        messages: 4,
        retries: 1,
        retries_first: None,
        lossy: 1,
        udelay: 0,
        errs: true,
    };
    let mut mode = String::from("timed");
    let mut keep_going = false;
    let mut args = std::env::args().skip(1);
    let num = |v: Option<String>, flag: &str| -> u64 {
        v.unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
            .parse()
            .unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
    };
    while let Some(a) = args.next() {
        match a.as_str() {
            "--mode" => mode = args.next().unwrap_or_else(|| cli_bail("--mode needs a value")),
            "--dk" => p.dk = num(args.next(), "--dk"),
            "--dl" => p.dl = num(args.next(), "--dl"),
            "--dr" => p.dr = num(args.next(), "--dr"),
            "--to" => p.to = num(args.next(), "--to"),
            "--messages" => p.messages = num(args.next(), "--messages") as u32,
            "--retries" => p.retries = num(args.next(), "--retries") as u32,
            "--retries-first" => p.retries_first = Some(num(args.next(), "--retries-first") as u32),
            "--lossy" => p.lossy = num(args.next(), "--lossy") as usize,
            "--udelay" => p.udelay = num(args.next(), "--udelay"),
            "--no-errs" => p.errs = false,
            "--keep-going" => keep_going = true,
            "--parallel" => {
                let v = args.next().unwrap_or_else(|| cli_bail("--parallel needs a value"));
                let _ = PARALLEL.set(v);
            }
            other => cli_bail(&format!("unknown argument {other}")),
        }
    }
    if p.to < 1 {
        cli_bail("--to must be >= 1");
    }
    if p.messages < 1 || p.messages > 200 {
        cli_bail("--messages must be in 1..=200");
    }
    let rt = p.dk + p.dl + p.dr;
    let r1 = p.retries_first.unwrap_or(p.retries);
    // The rule's verdict is for the LISTING with unbounded retransmissions. A
    // run of this file can only reach a violation if the sender is still
    // waiting when the first ack returns, i.e. (R1 + 1) * To >= RT; below that
    // a hold is a bound artifact, not a verdict.
    let reachable = (u64::from(r1) + 1) * p.to >= rt;
    println!(
        "par (Bosnacki-Dams): dK={} dL={} dR={} To={} (rule To > dK+dL+dR = {}: listing {}) \
         K={} R={} R1={} B={} UD={} errs={}{}",
        p.dk,
        p.dl,
        p.dr,
        p.to,
        rt,
        if p.to > rt { "HOLD" } else { "FIRE" },
        p.messages,
        p.retries,
        r1,
        p.lossy,
        p.udelay,
        p.errs,
        if p.to <= rt && !reachable {
            "  [(R1+1)*To < RT: no violation is reachable at these bounds, a hold here is a bound artifact]"
        } else {
            ""
        }
    );
    let modes: Vec<Mode> = match mode.as_str() {
        "timed" => vec![Mode::Timed],
        "baseline" => vec![Mode::Baseline],
        "compare" => vec![Mode::Baseline, Mode::Timed],
        other => cli_bail(&format!("invalid --mode: {other}")),
    };
    let mut violated = false;
    let mut explored = Vec::new();
    for m in modes {
        let (stats, elapsed) = run(m, p, keep_going);
        let label = if m == Mode::Timed { "timed" } else { "baseline" };
        let p1 = report(label, &stats, elapsed);
        violated |= p1 > 0;
        explored.push((label, stats.execs + stats.block));
    }
    if explored.len() == 2 {
        let (b, t) = (explored[0].1, explored[1].1);
        println!(
            "explored reduction: {:.2}x ({} vs {} executions explored, execs+blocked)",
            b as f64 / t.max(1) as f64,
            b,
            t
        );
    }
    if violated {
        println!("VIOLATION: P1 (assert(mr == me)) failed in some explored execution");
    }
}
