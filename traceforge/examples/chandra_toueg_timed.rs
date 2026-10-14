//! Chandra-Toueg eventually perfect failure detector (JACM 43(2), 1996,
//! Fig. 10 on p. 256), in the form Tran, Konnov and Widder formalized and
//! verified ("A Case Study on Parametric Verification of Failure Detectors",
//! LMCS 19(1):17, 2023, Algorithm 1; their TLA+ spec `tlap/spec-inv/sa_fnc.tla`
//! from the FORTE 2021 artifact, Zenodo 10.5281/zenodo.4687714). Property:
//! Strong Accuracy (a correct process is never suspected) of the two-process
//! cutoff instance, under partial synchrony: bounds Delta (message delay) and
//! Phi (relative speed) that hold from time 1. TKW's model has UNKNOWN bounds
//! with GST = 1; their experiments (Sec. 9) and this port fix Delta and Phi per
//! run. Phi is in the sense of sa_fnc.tla (a step at least every Phi rounds,
//! rTimer < Phi - 1), one round tighter than a literal reading of their TC2.
//!
//! THE ALGORITHM (CT Fig. 10). Every process p: Task 1 repeatedly sends
//! "p-is-alive"; Task 2 suspects q if it hears nothing from q during the last
//! timeout ticks ("counting the number of steps that it takes"); Task 3, on a
//! message from a suspected q, trusts q again and increases timeout by one.
//! The initial timeout (TKW's TODefault) is a free parameter.
//!
//! TKW'S MODEL, AND WHERE EACH PART IS HERE. Global rounds; each round runs
//! sub-rounds SSched, RSched, IncMsgAge, SSnd, RNoSnd, RRcv, RComp in order.
//!   SSched (sTimer)      -> `sender`: self-Tick with window [3, 3*Phi]. The
//!                           first Send round and every gap are in [3, 3*Phi].
//!   RSched (rTimer, rPC) -> `Clock`: self-Tick with window [1, Phi]; one
//!                           receiver step per active round, cycling
//!                           NoSnd -> Rcv -> Comp from rPC = RComp.
//!   SSnd + IncMsgAge     -> Alive with transit [0, Delta - 1], read at its
//!                           arrival (storage lifetime sd = 0). A message sent in
//!                           round s is old (must be delivered) at the first RRcv
//!                           in a round >= s + Delta, and optional before.
//!   the buffer           -> the receiver's count of Alive messages read since
//!                           its last step (TKW's existsMsgOfAge): RRcv delivers
//!                           everything that arrived before its Tick; one that
//!                           arrives at the Tick's instant is read before or
//!                           after it (both orders are explored).
//!   RNoSnd/RRcv/RComp    -> `Detector::step`, line for line (wt + 1 only while
//!                           wt < timeout; RRcv resets wt on a delivery and, if
//!                           suspected, trusts again with timeout + 1 (Task 3);
//!                           RComp suspects when wt >= timeout (Task 2)).
//!   StrongAccuracy       -> the assertion in `Detector::step`, at the Comp
//!                           step that suspects (TKW's invariant ~suspected,
//!                           violated there and then).
//! One time unit is one round. Every constraint is an interval on a send, so
//! timed TraceForge handles it symbolically and enumerates only event ORDERS.
//! The horizon H: main's EndOfRun arrives at exactly H; a step at H is kept or
//! cut by the tie order. The sender is bounded by COUNT (floor(H/3) Sends, the
//! most TKW's sender can make by round H) and the receiver by H ticks (never
//! binding before H in timed mode); untimed, these caps are what bounds the
//! run (same program in both modes).
//!
//! THE SEMANTICS THIS PORT LIVES IN (since 2026-10-12; first-arrival receives
//! since 2026-10-13). Every send may be lost (`with_all_sends_lossy`, the
//! model's rule), and a message is readable only at its arrival instant (sd =
//! 0: Alive, EndOfRun and the two self-Ticks alike). A receive reads the
//! message that ARRIVED FIRST among those pending at it (a tie leaves both
//! orders explored; a message that arrived before the wait began is gone), so
//! a waiting receiver never reads a later arrival while an earlier one of
//! another sender expires unread. The port never sleeps and handles every
//! message in zero time, so no message arrives while its receiver is busy. A
//! lost Alive is a silence the receiver cannot tell from a slow sender, and so
//! is an Alive that arrives at the instant of a Receive step and is read after
//! that step's Tick (it counts at the next Receive step). A sender whose Tick
//! is lost stalls for good (TKW's sender never stalls); a receiver whose Tick
//! or EndOfRun is lost ends. No execution is filtered: there is no drain, no
//! `assume!`, no reliability condition and no harness message the verdict
//! waits for. The two waits are finite (`recv_msg_timed(Finite(3*Phi))` at the
//! sender, `Finite(Phi)` at the receiver), as long as the largest transit of
//! the Tick awaited. The engine admits a timeout only when nothing arrives
//! before the deadline (C6b, half-open): the Tick was lost, or (in a wait that
//! began when it was armed) it arrives exactly at the deadline and loses the
//! race to the timer, so a stall needs no lost message. No thread ever waits
//! for ever: the only blocked endings are the failed assertions.
//!
//! WHAT THIS GIVES. Under links that may lose anything, Strong Accuracy is
//! unsatisfiable: a stalled sender (which needs no lost message, see above),
//! or enough lost Alives, and the receiver suspects a process that never
//! crashed. The verdict is therefore VIOLATED at every TODefault at which the
//! receiver can take enough steps within H (3*ceil((to + 1)/3) steps; the
//! binary prints a NOTE when H is too short, a HOLD there is vacuous), in both
//! modes. Swept at Delta 1, Phi 1, H 8 (to = 1..5) and at Delta 2, Phi 2, H 10
//! (to = 1..8): VIOLATED in both modes at every to, and a vacuous HOLD at the
//! next one (6, 9). Each FIRE prints a counterexample graph; in timed mode it
//! carries a certified witness timeline. The timing cases (a slow sender, late
//! delivery) are still explored but no longer separate in the verdict.
//! Chandra and Toueg assume reliable links and prove only EVENTUAL accuracy,
//! so nothing here is a bug in their algorithm.
//!
//! HISTORY. Until 2026-10-12 this file carried a harness: sequence numbers on
//! Alive with `assume!` guards (`check_seq`, `check_done`), a receiver tick
//! drain and a buffer thread, judged at the end of the run. Written against
//! sd = 0 cross-channel eviction (a receive reading a later message of another
//! channel while an earlier one expired unread, which the first-arrival rule
//! of 2026-10-13 now excludes), the guards also discarded every run with a
//! lost Alive once all sends became lossy (2026-09-30), i.e. they re-created
//! reliable links, and on that filtered model the port reproduced TLC's least
//! safe TODefault on TKW's spec (thr = 3*Phi + Delta - [Delta mod 3 == 2] for
//! Phi >= 2; 13 at TKW's Table 1 cell, where they verified 26). That
//! threshold is a fact about TKW's reliable-link model, established by TLC and
//! still reproduced by `--mode rounds` (TKW's own round model has no messages
//! to lose); it is no longer a claim of this port, and no boundary is. Facts
//! about TKW's artifact found with TLC, not TraceForge, still stand: their
//! Table 1 rows 5-6 printed (Delta 4, Phi 5) match (4, 6) exactly (44,733
//! states, depth 267); fd-paras/fd_paras.tla parses `x'[Delta] = a \/ b` as
//! `(x'[Delta] = a) \/ b`, so as shipped it VIOLATES Strong Accuracy at their
//! own 6*Phi+Delta (Delta = Phi = 1, N = 2) and holds with the parentheses;
//! their published tables use the two-process specs, which have them.
//!
//! RESULTS (re-measured 2026-10-13 after the first-arrival ruling, one worker,
//! --keep-going; "old" is the harness version at HEAD 0cbe2c7 on the same
//! cells; thr is TLC's least safe TODefault on TKW's reliable-link spec, which
//! the old port reproduced):
//!
//!   Delta Phi to  H   old timed              new timed               new baseline
//!   1     1   2   8   VIOLATED 2 of 602      VIOLATED 28 of 156      VIOLATED 62 of 914
//!   1     1   3   8   HOLDS (thr = 3)        VIOLATED 14 of 436      VIOLATED 62 of 1,919
//!   2     2   6   10  VIOLATED 3 of 6,597    VIOLATED 67 of 9,012    VIOLATED 226 of 19,675
//!   2     2   7   10  HOLDS (thr = 7)        VIOLATED 36 of 9,080    VIOLATED 88 of 20,392
//!   2     4   9   13  VIOLATED 11 of 39,698  VIOLATED 365 of 108,471 VIOLATED 736 of 216,574
//!   2     4   12  13  HOLDS, vacuous in both (15 steps needed; new: 109,908 explored, 0 blocked)
//!   2     4   12  15  HOLDS (H < TLC's 16)   VIOLATED, no count (*)  VIOLATED 2,224 of 1,842,172
//!   2     4   12  16  VIOLATED 3 of 170,291  VIOLATED, no count (*)  VIOLATED 2,224 of 2,353,201
//!   1     1   3   8   --mode rounds (TKW's model, no messages): HOLDS in both; new: to 2 VIOLATED
//!   2     2   7   10  --mode rounds: HOLDS (5,975 executions), to 6 VIOLATED (4 of 5,975)
//!   2     4   13  16  --mode rounds: HOLDS (27,445,895 executions), to 12 VIOLATED
//!
//! (*) The timed --keep-going run did not finish within 15 min, so there is no
//! count; the verdict is the exit 101 of a run without --keep-going.
//!
//! "n of m": violating endings (each the first suspecting Comp step of its
//! run) of all endings explored (execs + blocked + impossible). In the new
//! version blocked equals the violation count in every cell: a failed
//! assertion ends its run as blocked, and nothing else ever blocks. The old
//! explored counts were mostly guard-blocked runs (e.g. 600 of the 602).
//!
//! Before the first-arrival rule the timed legs of the same cells gave 28 of
//! 156, 24 of 496, 106 of 13,963, 72 of 14,131 and 604 of 224,979 (and 229,219
//! explored at H 13): the rule removes the executions in which a waiting
//! receive read a later arrival while an earlier one of another sender
//! expired unread. The untimed leg has no clock, so it is unchanged, and no
//! verdict that was known has moved.
//!
//! Certified witness at Delta 1, Phi 1, to 3, H 8 (exit 101 without
//! --keep-going): the sender's first Alive is sent at 3 and lost (the printed
//! graph marks the send `[dropped]`). The receiver steps at every time unit; its
//! sixth step, at 6, is a Comp step with wt = 3 = to (wt reached 3 at step 3,
//! and the Receive step 5 delivered nothing): suspected. The second Alive,
//! sent at 6, arrives at 6 and is read just before that Tick (a tie, both
//! orders are explored), but a Comp step does not consult what was read. In
//! TKW's model the first message is delivered at the Receive step of round 5
//! and no suspicion follows. Before the first-arrival rule this witness had
//! the first Alive arrive at 3 and not be lost: the receiver read its own
//! Tick instead, and the Alive, readable only at 3 (sd = 0), expired unread.
//! That execution is gone.
//!
//! Other certified witnesses. Below thr they lose no send: at Delta 1, Phi 1,
//! to 2 the first Alive, arriving at 3, is read just before the Tick of step 3,
//! a Comp step with wt = 2 = to; at Delta 2, Phi 2, to 6 (and Delta 2, Phi 4,
//! to 12, H 16) the first Alive is delivered at once, the sender's second Send
//! comes late (at 8, a gap of 5 in [3, 6]; at 14, a gap of 11 in [3, 12]) and
//! its Alive takes the full transit Delta - 1, arriving at the instant of a
//! Receive step and read after that step's Tick, so the next Comp step has
//! wt = to. At thr and above (to 3..5 at Delta 1, Phi 1, H 8; to 7, 8 at
//! Delta 2, Phi 2, H 9..10), and at Delta 2, Phi 4, to 12 with H 15, one short
//! of TLC's counterexample depth, the first Alive is lost. In some of these
//! graphs the sender's last wait times out, after the events that matter.
//!
//! Cost (machine loaded, seconds indicative): at Delta 2, Phi 4, H 13 the new
//! version explores 109,908 timed executions in 106 s (229,219 in 106 s before
//! the first-arrival rule; old: 39,698, of which 39,067 guard-blocked, 24 s)
//! and 221,895 untimed ones in 14 s. At H 15 and H 16 the untimed leg alone
//! explores 1,842,172 executions in 202 s and 2,353,201 in 216 s (VIOLATED
//! 2,224 both times); the timed --keep-going run finished at neither within
//! 15 min. No efficiency claim is made here.
//!
//! Modes (same `--delta/--phi/--to/--horizon` everywhere):
//!   timed     the port above under timed TraceForge (symbolic time)
//!   baseline  the SAME port under untimed TraceForge (windows ignored)
//!   rounds    TKW's own round model (sa_fnc.tla transliterated sub-round by
//!             sub-round into one Rust thread, each disjunction a nondet()
//!             choice) under untimed TraceForge: a stateless enumeration of
//!             explicit-time rounds with no messages, so nothing is lost and
//!             TLC's thr is reproduced (TLC, which hashes states, is far
//!             cheaper on this finite model)
//!   compare   baseline, then timed;  all  baseline, timed, rounds

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, ConsType, Stats, WaitTime};

// =====================================================================
// Messages
// =====================================================================

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// Self-timer. Sender: its next Send round. Receiver: its next active round.
    Tick,
    /// "p-is-alive" (CT Fig. 10, Task 1).
    Alive,
    /// From main, arriving at exactly H: stop taking protocol steps.
    EndOfRun,
}

/// What one receiver run observed. Observation only: the protocol never
/// reads it, and the verdict does not depend on it (see `Detector::step`).
#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct Summary {
    ever_suspected: bool,
    /// Active rounds (receiver steps), Receive steps, delivering ones.
    steps: usize,
    receives: usize,
    deliveries: usize,
}

/// The receiver's program counter `rPC` (TKW: RNoSnd, RRcv, RComp).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    NoSnd,
    Rcv,
    Comp,
}

impl Phase {
    /// TKW's RSched: an active round advances rPC cyclically.
    fn next(self) -> Self {
        match self {
            Phase::NoSnd => Phase::Rcv,
            Phase::Rcv => Phase::Comp,
            Phase::Comp => Phase::NoSnd,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
    Rounds,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Baseline => "baseline",
            Mode::Timed => "timed",
            Mode::Rounds => "rounds",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Params {
    /// TKW's Delta: a message of age >= Delta rounds must be delivered at
    /// the receiver's next Receive step. Delta >= 1 for the port.
    delta: u64,
    /// TKW's Phi: every process takes a step at least every Phi rounds.
    phi: u64,
    /// TKW's TODefault: the initial timeout.
    to: u64,
    /// Horizon in rounds: main's EndOfRun arrives at exactly H; the round
    /// model runs rounds 1..=H.
    h: u64,
}

impl Params {
    /// The sender's k-th Send is at time >= 3k, so floor(H/3) Sends are all
    /// the Sends TKW's sender can make by round H; later ones could only be
    /// delivered after the receiver's last step. Untimed, it bounds the run.
    fn sender_sends(&self) -> u64 {
        self.h / 3
    }
    /// The receiver's k-th Tick arrives at time >= k, so H ticks never bind
    /// before H in timed mode; untimed, the cap bounds the run.
    fn receiver_ticks(&self) -> u64 {
        self.h
    }
    /// Steps a receiver needs to suspect with nothing ever delivered: the
    /// j-th Comp step (step 3j) compares wt = 3j - 1 against the timeout.
    fn steps_to_suspect(&self) -> u64 {
        3 * (self.to + 1).div_ceil(3)
    }
    /// Default-horizon helper: the round of TLC's shortest counterexample
    /// at TODefault = thr - 1 in TKW's reliable-link model, as fitted on
    /// their grid (Delta <= 8, Phi <= 6). Only a default for H; no claim.
    fn h_min(&self) -> u64 {
        if self.phi == 1 {
            3 + 3 * self.delta.saturating_sub(2).div_ceil(3)
        } else {
            3 * self.phi + 3 * self.delta.div_ceil(3) + 1 - u64::from(self.delta % 3 == 0)
        }
    }
}

// =====================================================================
// Outcome counters (observation only; the verdict is the inline assertion)
// =====================================================================

const ZERO: AtomicUsize = AtomicUsize::new(0);

/// Receiver runs that reached EndOfRun, and runs with a suspicion (each
/// counted once, at the first Comp step that suspects). Counters are raised
/// by program code on every ending the search enumerates, so with
/// --keep-going `SUSPECTED` is a count of violating endings, not a verdict:
/// the verdict is the engine's (exit 101 with a certified witness).
static COMPLETED: AtomicUsize = AtomicUsize::new(0);
static SUSPECTED: AtomicUsize = AtomicUsize::new(0);
/// Finite waits that timed out, which the engine admits only when nothing
/// arrived before the deadline: the awaited message was lost, or it arrives
/// exactly at the deadline and loses the race to the timer. A sender that
/// timed out stops sending; a receiver that timed out stops.
static STALLED_SENDERS: AtomicUsize = AtomicUsize::new(0);
static RECEIVER_TIMEOUTS: AtomicUsize = AtomicUsize::new(0);
/// [min, max] of steps, receives, deliveries (over receiver runs that
/// ended) and of Alive sends (over sender runs that ended).
const PAIR: [AtomicUsize; 2] = [ZERO; 2];
static RANGES: [[AtomicUsize; 2]; 4] = [PAIR; 4];

fn reset_counts() {
    for c in [&COMPLETED, &SUSPECTED, &STALLED_SENDERS, &RECEIVER_TIMEOUTS] {
        c.store(0, Ordering::Relaxed);
    }
    for r in &RANGES {
        r[0].store(usize::MAX, Ordering::Relaxed);
        r[1].store(0, Ordering::Relaxed);
    }
}

fn range_add(i: usize, v: usize) {
    RANGES[i][0].fetch_min(v, Ordering::Relaxed);
    RANGES[i][1].fetch_max(v, Ordering::Relaxed);
}

/// Record one receiver run that ended (observation only).
fn record(s: Summary) {
    COMPLETED.fetch_add(1, Ordering::Relaxed);
    for (i, v) in [s.steps, s.receives, s.deliveries].into_iter().enumerate() {
        range_add(i, v);
    }
}

// =====================================================================
// Receiver protocol state: CT Fig. 10 Tasks 2 and 3, TKW's three steps
// =====================================================================

/// TKW's rPC, waitingTime, timeout, suspected.
struct Detector {
    phase: Phase,
    waiting_time: u64,
    timeout: u64,
    suspected: bool,
    obs: Summary,
}

impl Detector {
    /// TKW Proc_Init. rPC = "RComp", so the first active round is NoSnd.
    fn new(p: &Params) -> Self {
        Self { phase: Phase::Comp, waiting_time: 0, timeout: p.to, suspected: false, obs: Summary::default() }
    }

    /// One active round (TKW RSched with rTimer' = 0): advance rPC and run
    /// that step. `deliver` is consulted only by the Receive step and says
    /// whether it delivered at least one message.
    fn step(&mut self, deliver: impl FnOnce() -> bool) {
        self.obs.steps += 1;
        self.phase = self.phase.next();
        match self.phase {
            // RNoSnd.
            Phase::NoSnd => {
                if self.waiting_time < self.timeout {
                    self.waiting_time += 1;
                }
            }
            // RRcv: MsgDeliver if anything is delivered, else NoMsgDeliver.
            Phase::Rcv => {
                self.obs.receives += 1;
                if deliver() {
                    self.obs.deliveries += 1;
                    self.waiting_time = 0;
                    if self.suspected {
                        // CT Task 3: trust q again, increase the timeout.
                        self.suspected = false;
                        self.timeout += 1;
                    }
                } else if self.waiting_time < self.timeout {
                    self.waiting_time += 1;
                }
            }
            // RComp: CT Task 2.
            Phase::Comp => {
                if self.waiting_time < self.timeout {
                    self.waiting_time += 1;
                } else {
                    // Suspect. Strong Accuracy (TKW's invariant: ~suspected
                    // in every state) is violated here and now. The sender
                    // never crashes in this model, so every suspicion is a
                    // false one, whatever caused the silence: slow steps,
                    // late delivery, or heartbeats that were lost, which
                    // the model's semantics allow for every send (including
                    // the sender's own timer).
                    if !self.obs.ever_suspected {
                        SUSPECTED.fetch_add(1, Ordering::Relaxed);
                    }
                    self.suspected = true;
                    self.obs.ever_suspected = true;
                    traceforge::assert(false);
                }
            }
        }
    }
}

// =====================================================================
// The port: sender and receiver (timed and baseline modes)
// =====================================================================

/// Process 1 of TKW's cutoff instance: CT Fig. 10, Task 1. Bounded by
/// COUNT, not by EndOfRun: it makes every Send TKW's sender can make by
/// round H and stops. Its timer is a self-sent message like any other and
/// may be lost. The wait for it is finite: a Tick that arrives at all
/// arrives within 3*Phi, so the timeout branch, which the engine admits
/// only when no Tick arrives before the deadline, is the lost-Tick case or
/// a Tick arriving exactly at the deadline (its largest transit), and the
/// sender then stops for good.
fn sender(p: Params, receiver: ThreadId) {
    let me = thread::current().id();
    let mut sent = 0usize;
    for _ in 0..p.sender_sends() {
        // TKW sTimer: the first Send round is in [3, 3*Phi], so is every gap.
        traceforge::send_msg_timed(me, Msg::Tick, 3, 3 * p.phi);
        match traceforge::recv_msg_timed::<Msg>(WaitTime::Finite(3 * p.phi)) {
            Some(Msg::Tick) => {}
            Some(m) => panic!("sender: unexpected {m:?}"),
            None => {
                STALLED_SENDERS.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        // SSnd with sTimer = 0: send "p-is-alive". Old at age Delta means
        // arrival at most Delta - 1 rounds after the Send round.
        traceforge::send_msg_timed(receiver, Msg::Alive, 0, p.delta - 1);
        sent += 1;
    }
    range_add(3, sent);
}

/// The receiver's self-timer (TKW rTimer): the first active round is in
/// [1, Phi], and so is every gap. At most H ticks (see `receiver_ticks`).
struct Clock {
    me: ThreadId,
    phi: u64,
    left: u64,
}

impl Clock {
    fn start(p: &Params) -> Self {
        let mut c = Self { me: thread::current().id(), phi: p.phi, left: p.receiver_ticks() };
        c.arm();
        c
    }
    fn arm(&mut self) {
        if self.left > 0 {
            self.left -= 1;
            traceforge::send_msg_timed(self.me, Msg::Tick, 1, self.phi);
        }
    }
}

/// Process 2 of TKW's cutoff instance: steps on its own clock (TKW rTimer)
/// and keeps the Alive messages read since its last step as a count, TKW's
/// message buffer (their RRcv delivers everything that has arrived). The
/// wait is finite: a Tick that arrives at all arrives within Phi of its
/// arming, and EndOfRun (at exactly H) has arrived by the time the tick cap
/// is reached (the k-th Tick is at time >= k), so the timeout branch, which
/// the engine admits only when nothing arrives before the deadline, is the
/// lost-Tick or lost-EndOfRun case or a Tick arriving exactly at the
/// deadline (its largest transit); the receiver then stops.
fn receiver(p: Params) {
    let mut d = Detector::new(&p);
    let mut clock = Clock::start(&p);
    let mut held: u32 = 0;
    loop {
        match traceforge::recv_msg_timed::<Msg>(WaitTime::Finite(p.phi)) {
            Some(Msg::Alive) => held += 1,
            Some(Msg::Tick) => {
                d.step(|| std::mem::take(&mut held) > 0);
                clock.arm();
            }
            Some(Msg::EndOfRun) => break,
            None => {
                RECEIVER_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
    record(d.obs);
}

fn port(p: Params) {
    let r = thread::spawn(move || receiver(p));
    let rt = r.thread().id();
    let s = thread::spawn(move || sender(p, rt));
    // The horizon, arriving at exactly H. Like every message it may be
    // lost; the receiver then runs to its tick cap and times out.
    traceforge::send_msg_timed(rt, Msg::EndOfRun, p.h, p.h);
    let _ = s.join();
    let _ = r.join();
}

// =====================================================================
// TKW's own round model (sa_fnc.tla), transliterated (rounds mode)
// =====================================================================

/// One behaviour of TKW's Next relation for rounds 1..=H, with every
/// disjunction of the spec a nondet() choice. Sub-rounds in their order:
/// SSched, RSched, IncMsgAge, SSnd, then the receiver's step (RNoSnd, RRcv
/// or RComp; at most one of them acts per round).
fn rounds_model(p: Params) {
    let d = p.delta as usize;
    let mut s_timer: u64 = 0;
    let mut r_timer: u64 = 0;
    // existsMsgOfAge[0..=Delta]
    let mut msgs = vec![false; d + 1];
    let mut det = Detector::new(&p);
    let mut sent = 0usize;
    for _round in 1..=p.h {
        // SSched: reset allowed iff sTimer >= 2, increment iff < 3*Phi - 1.
        let reset_ok = s_timer >= 2;
        let inc_ok = s_timer < 3 * p.phi - 1;
        let reset = if reset_ok && inc_ok { traceforge::nondet() } else { reset_ok };
        s_timer = if reset { 0 } else { s_timer + 1 };
        // RSched: active always allowed, inactive iff rTimer < Phi - 1.
        let active = if r_timer < p.phi - 1 { traceforge::nondet() } else { true };
        r_timer = if active { 0 } else { r_timer + 1 };
        // IncMsgAge.
        if d > 0 {
            let mut aged = vec![false; d + 1];
            aged[1..d].copy_from_slice(&msgs[0..d - 1]);
            aged[d] = msgs[d] || msgs[d - 1];
            msgs = aged;
        }
        // SSnd.
        if s_timer == 0 {
            msgs[0] = true;
            sent += 1;
        }
        // The receiver's step. RRcv: MsgDeliver or NoMsgDeliver. Both are
        // "existsMsgOfAge' = a subset of existsMsgOfAge without age Delta":
        // old messages go, each young one stays or goes by choice; the
        // subset equal to the old set is NoMsgDeliver (needs no old one).
        if active {
            det.step(|| {
                let mut any = false;
                for k in 0..=d {
                    if msgs[k] && (k == d || traceforge::nondet()) {
                        msgs[k] = false;
                        any = true;
                    }
                }
                any
            });
        }
    }
    range_add(3, sent);
    record(det.obs);
}

// =====================================================================
// Verifier setup and reporting
// =====================================================================

fn build_config(mode: Mode, keep_going: bool, workers: usize, max_execs: Option<u64>) -> Config {
    // FIFO per (sender, destination) channel, pinned (it is the default).
    let mut b = Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX).with_cons_type(ConsType::FIFO);
    if keep_going {
        b = b.with_keep_going_after_error(true);
    }
    if workers > 1 {
        b = b.with_parallel(true).with_parallel_workers(workers);
    }
    if let Some(n) = max_execs {
        b = b.with_max_iterations(n);
    }
    match mode {
        Mode::Baseline | Mode::Rounds => b.build(),
        // Every send carries its own window; sd = 0 everywhere.
        Mode::Timed => b.with_timed(0, 0, 0).build(),
    }
}

fn run(mode: Mode, p: Params, keep_going: bool, workers: usize, max_execs: Option<u64>) -> (Stats, Duration) {
    let cfg = build_config(mode, keep_going, workers, max_execs);
    reset_counts();
    let start = Instant::now();
    let stats = match mode {
        Mode::Rounds => traceforge::verify(cfg, move || rounds_model(p)),
        _ => traceforge::verify(cfg, move || port(p)),
    };
    (stats, start.elapsed())
}

fn range(i: usize) -> String {
    let lo = RANGES[i][0].load(Ordering::Relaxed);
    let hi = RANGES[i][1].load(Ordering::Relaxed);
    if lo == usize::MAX { String::from("-") } else { format!("[{lo},{hi}]") }
}

fn print_one(mode: Mode, p: &Params, stats: &Stats, dur: Duration, max_execs: Option<u64>) {
    let runs = COMPLETED.load(Ordering::Relaxed);
    let susp = SUSPECTED.load(Ordering::Relaxed);
    let verdict = if susp > 0 { "VIOLATED" } else { "HOLDS" };
    println!(
        "{:<8} delta={} phi={} to={} H={}  execs={} blocked={} impossible={} \
         explored={} receiver_runs={runs} violations={susp} stalled_senders={} receiver_timeouts={} \
         steps={} receives={} deliveries={} sent={} verdict={verdict} time={dur:?}",
        mode.name(),
        p.delta,
        p.phi,
        p.to,
        p.h,
        stats.execs,
        stats.block,
        stats.timeline_impossible,
        stats.execs + stats.block + stats.timeline_impossible,
        STALLED_SENDERS.load(Ordering::Relaxed),
        RECEIVER_TIMEOUTS.load(Ordering::Relaxed),
        range(0),
        range(1),
        range(2),
        range(3),
    );
    // The counters are raised by PROGRAM code, so they also see endings that
    // the (C6b) timeout-miss condition later judges to admit no timeline
    // (those run to completion first). A count of zero is sound; a count
    // above zero is an upper bound. The verdict is the engine's: exit 101
    // with a certified witness, without --keep-going.
    if stats.timeline_impossible > 0 && susp > 0 {
        println!(
            "NOTE: {} of the {} endings explored admit no timeline and were counted above too; \
             violations={susp} is an upper bound. Re-run without --keep-going (exit 101 = the \
             engine certified a counterexample) before quoting it.",
            stats.timeline_impossible,
            stats.execs + stats.block + stats.timeline_impossible
        );
    }
    if runs == 0 && susp == 0 {
        println!("WARNING ({}): no receiver run ended and nothing was suspected: no data, not a hold.", mode.name());
    }
    if let Some(n) = max_execs {
        if (stats.execs + stats.block) as u64 >= n {
            println!(
                "NOTE ({}): stopped at the --max-execs budget ({n}): counts are a prefix of the \
                 exploration; a HOLD here is not a verdict.",
                mode.name()
            );
        }
    }
    if p.h < p.steps_to_suspect() {
        println!(
            "NOTE ({}): H = {} < {}, the steps a receiver needs to suspect at to = {}: no run can \
             suspect within this horizon, so a HOLD here is vacuous.",
            mode.name(),
            p.h,
            p.steps_to_suspect(),
            p.to
        );
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut mode = String::from("timed");
    let (mut delta, mut phi, mut to, mut h) = (2u64, 4u64, None, None);
    let mut keep_going = false;
    let mut workers = 1usize;
    let mut max_execs = None;
    while let Some(a) = args.next() {
        let mut val = |f: &str| args.next().unwrap_or_else(|| panic!("{f} needs a value"));
        match a.as_str() {
            "--mode" => mode = val("--mode"),
            "--delta" => delta = val("--delta").parse().expect("--delta"),
            "--phi" => phi = val("--phi").parse().expect("--phi"),
            "--to" => to = Some(val("--to").parse().expect("--to")),
            "--horizon" => h = Some(val("--horizon").parse().expect("--horizon")),
            "--keep-going" => keep_going = true,
            "--workers" => workers = val("--workers").parse().expect("--workers"),
            "--max-execs" => max_execs = Some(val("--max-execs").parse().expect("--max-execs")),
            _ => panic!(
                "unknown flag {a}. Usage: chandra_toueg_timed [--mode timed|baseline|rounds|compare|all] \
                 [--delta D>=1] [--phi P>=1] [--to T] [--horizon H] \
                 [--keep-going] [--workers N] [--max-execs N]"
            ),
        }
    }
    assert!(delta >= 1, "Delta = 0 is not representable exactly by the port (see header)");
    assert!(phi >= 1, "Phi >= 1");
    let to = to.unwrap_or(6 * phi + delta);
    let mut p = Params { delta, phi, to, h: 0 };
    // Default H: the TLC diameter bound is not known here; h_min + 3*Phi is
    // past the boundary counterexample with one more sender period of slack.
    p.h = h.unwrap_or(p.h_min() + 3 * phi);
    let modes: Vec<Mode> = match mode.as_str() {
        "baseline" => vec![Mode::Baseline],
        "timed" => vec![Mode::Timed],
        "rounds" => vec![Mode::Rounds],
        "compare" => vec![Mode::Baseline, Mode::Timed],
        "all" => vec![Mode::Baseline, Mode::Timed, Mode::Rounds],
        m => panic!("unknown mode {m}"),
    };
    for m in modes {
        let (s, d) = run(m, p, keep_going, workers, max_execs);
        print_one(m, &p, &s, d, max_execs);
    }
}
