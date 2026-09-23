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
//! The initial timeout (TKW's TODefault) is a free parameter: too small, and a
//! correct sender is suspected. TKW verify it with TODefault = 6*Phi + Delta
//! (Table 1, HOLDS) and TODefault = Delta + 1 (Table 2, VIOLATED); they do
//! not say where between the two the property starts to hold.
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
//!   the buffer           -> `buffer` thread (TKW's existsMsgOfAge): holds
//!                           arrived messages; the receiver touches it only in its
//!                           Receive step (Query/Answer, transit exactly 0).
//!   RNoSnd/RRcv/RComp    -> `Detector::step`, line for line (wt + 1 only while
//!                           wt < timeout; RRcv resets wt on a delivery and, if
//!                           suspected, trusts again with timeout + 1 (Task 3);
//!                           RComp suspects when wt >= timeout (Task 2)).
//!   StrongAccuracy       -> `judge`: assert that no Comp step ever suspected.
//! One time unit is one round. Every constraint is an interval on a send, so
//! timed TraceForge handles it symbolically and enumerates only event ORDERS.
//!
//! WHY THE PORT IS EXACT (Delta >= 1). A message sent in round s is delivered
//! at a Receive step at time r iff it arrived by r (read before the Query in a
//! same-instant tie, both orders are explored). Arrival in [s, s + Delta - 1]
//! makes delivery possible at every Receive step in [s, s + Delta - 1] and
//! forced at the first one at or after s + Delta: TKW's choice set. Delivering
//! everything that arrived loses nothing: the receiver observes only whether a
//! Receive step delivered at least one message, and any young message TKW
//! keeps can arrive later here. FIFO arrival loses nothing: messages are
//! interchangeable and deadlines grow with send time, so sorting TKW's
//! delivery rounds into send order keeps every step's observation. Delta = 0
//! would need a send to precede a same-round Receive strictly; it is refused.
//! The horizon H: main's EndOfRun arrives at exactly H; a step at H is kept or
//! cut by the tie order. So every JUDGED run is a TKW prefix (sound; blocked
//! runs may carry eviction artifacts, which the guards cut before any
//! judgement), and every H-round TKW behaviour has a judged run with the same
//! receiver steps and the same delivered/not-delivered outcome at every
//! Receive step (complete up to H for Strong Accuracy).
//!
//! HARNESS (sd = 0 hazards, each closed BEFORE the judgement). At sd = 0 a
//! receive may read a later message from another channel and silently evict
//! an earlier one (TraceForge constrains skips only within one channel), and a
//! timed assertion is certified on timeline feasibility even if another thread
//! blocks afterwards. Unguarded, an eviction fakes a silence and a FALSE
//! violation gets certified (an earlier draft whose sender stopped on a
//! cross-channel EndOfRun did exactly that: its own pending Tick was evicted,
//! i.e. a Send never made, and the receiver's suspicion was reported).
//! Every input channel of every thread, and what closes an eviction on it:
//!   sender   <- sender   Tick      none possible: the sender is bounded by
//!                                  COUNT (floor(H/3) Sends, each after its
//!                                  own Tick) and receives nothing else
//!   receiver <- receiver Tick      `Clock::drain` blocks (a step never taken)
//!   receiver <- main     EndOfRun  the tick cap H stops re-arming; the
//!                                  receiver then waits for EndOfRun forever
//!   receiver <- buffer   Answer    read by a receive tagged with the buffer,
//!                                  right after its own Query: nothing to skip
//!   buffer   <- sender   Alive     `check_seq` (sequence gap), `check_done`
//!                                  (an Alive lost after the last one read)
//!   buffer   <- sender   Done      the buffer loop never exits without it
//!   buffer   <- receiver Query     the receiver blocks waiting for its Answer
//!   buffer   <- receiver Stop      the buffer loop never exits without it
//! The buffer judges only after the sender's Done and the receiver's Stop, so
//! every guard of every thread is causally before the assertion. The caps
//! never bind early in an eviction-free timed run (the sender's k-th Send is
//! at time >= 3k, the receiver's k-th step at >= k); untimed, they are what
//! bounds the run (same program in both modes).
//!
//! RESULTS. Data, drivers and the TLC ground truth are in
//! docs/research/ta_problems/need-from-user-papers/tkw-forte2021-fd-artifact/sweep
//! (README.md there). Numbers are for Delta = 2, Phi = 4 unless stated.
//!
//! Ground truth. TLC on TKW's spec with TODefault a parameter: 1350 cells
//! (Delta 0..8, Phi 1..6, TODefault 1..6*Phi+Delta), 0 errors, the verdict
//! monotone in TODefault in every (Delta, Phi). The least safe TODefault is
//!     thr = 3*Phi + Delta - [Delta mod 3 == 2]      (Phi >= 2)
//!     thr = 3 + 3*ceil(max(Delta - 2, 0) / 3)       (Phi = 1)
//! on all 1350 cells. It was derived by hand from the two worst silences, the
//! start-up one (first Send as late as 3*Phi, wt counting from time 0) and the
//! steady one (a Send delivered in its own round, the next forced 3*Phi +
//! Delta later), both rounded to the receiver's 3-step cycle, and then
//! checked blind on Delta 5..8 and Phi 6; it is fitted, not proven beyond the
//! grid. At TKW's Table 1 cell it is 13, not the verified 6*Phi+Delta = 26.
//! TKW state no least value; their report (App. D.2) says only that below
//! 6*Phi+Delta the receiver "might have a wrong suspicion". Delta+1 is not
//! always unsafe either: it is the threshold itself at Phi = 1, Delta = 2, 5, 8.
//!
//! 1. The timed port gives TLC's verdicts. One exploration per (Delta, Phi)
//!    at a horizon from TLC's depths (R_diam; for Phi = 5 the round of TLC's
//!    shortest counterexample plus 3) gives W*, and W* + 1 equals TLC's
//!    threshold on all 36 pairs run (Phi 1..4 with Delta 1..8, Phi 5 with
//!    Delta 1..4), each W* strictly below the most the horizon allows, and the
//!    explored count identical across TODefault within each pair. 725 cells
//!    compared (346 run, 379 derived from W*): 0 disagreements with TLC, 0
//!    errors (65 of the runs cannot suspect within H and hold by W*). At
//!    H = 25 = TLC's diameter here:
//!      to = 12  VIOLATED  (943 of 58,533 judged runs suspect)
//!      to = 13  HOLDS     explored 235,230 = 58,533 judged + 176,697 blocked
//!      to = 26  HOLDS     (TKW's Table 1)
//!    and `--control crashed-sender` is VIOLATED. The unbounded HOLD is TLC's;
//!    TraceForge's bounded verdicts coincide with it on every cell.
//! 2. Untimed, the same program cannot express partial synchrony: its W* is
//!    3*floor(H/3) - 1, the most H steps allow (8, 11, 14, 17 at H = 10, 13,
//!    16, 19), so it reports a violation at every TODefault it can reach, e.g.
//!    13 at H = 16 where TLC holds. Timed W* equals it up to H = 13 and stays
//!    at 12 = thr - 1 from H = 16 on.
//! 3. Cost, explored executions (single worker, loaded machine; seconds are
//!    indicative only):
//!      H    timed            untimed, same program   TKW's rounds, untimed
//!      10   671 (1.0 s)      370 (0.1 s)              37,722 (1.4 s)
//!      13   2,204 (3.7 s)    1,377 (0.4 s)            1,012,229 (30 s)
//!      16   7,198 (13 s)     5,138 (1.7 s)            27,445,895 (846 s)
//!      19   23,102 (51 s)    19,294 (5.1 s)           over 1 h
//!      22   73,681 (206 s)   72,918 (20 s)            -
//!      25   235,230 (580 s)  277,121 (61 s)           -
//!    On the same program untimed is wrong, and faster in wall-clock at every
//!    H (a timed execution costs about 10x more); in executions it grows x3.8
//!    per 3 rounds against timed's x3.2, so timed explores fewer from H = 25
//!    on. Against a stateless
//!    enumeration of TKW's explicit-time rounds, timed explores 56x to 3,800x
//!    fewer executions (x3.3 per 3 rounds against x27). TLC, which hashes
//!    states, is cheaper than both: 10,158 states, about 3 s, unbounded. 70
//!    to 75% of timed "explored" are runs the sd = 0 guards block.
//! 4. The adaptive timeout (TKW report, Formula 36: timeout <= 6*Phi+Delta
//!    from a start below it): TLC gives timeout <= max(TODefault, thr) on all
//!    28 runs over 10 (Delta, Phi) pairs.
//! What it is NOT: not a bug in Chandra and Toueg (they prove EVENTUAL
//! accuracy; a suspicion below thr is allowed and raises the timeout), and thr
//! belongs to TKW's two-process model: starting the receiver's cycle one phase
//! later (rPC = RNoSnd) moves (2,4) from 13 to 14 under TLC.
//! Side results from checking TKW's artifact with TLC, not TraceForge: their
//! Table 1 rows 5-6 (printed Delta 4, Phi 5: 44.7K states, depth 267) match
//! Delta 4, Phi 6 exactly (44,733, 267), not Phi 5 (28,492, 225); and their
//! N-process spec fd-paras/fd_paras.tla parses `x'[Delta] = a \/ b` as
//! `(x'[Delta] = a) \/ b`, so old messages can vanish: as shipped it VIOLATES
//! Strong Accuracy at their own 6*Phi+Delta (Delta = Phi = 1, N = 2), and holds
//! with the parentheses added. Their published tables use the two-process
//! specs, which have the parentheses.
//!
//! Modes (same `--delta/--phi/--to/--horizon` everywhere):
//!   timed     the port above under timed TraceForge (symbolic time)
//!   baseline  the SAME port under untimed TraceForge (windows ignored)
//!   rounds    TKW's own round model (sa_fnc.tla transliterated sub-round by
//!             sub-round into one Rust thread, each disjunction a nondet()
//!             choice) under untimed TraceForge: a stateless enumeration of
//!             explicit-time rounds (no state hashing; TLC, which hashes
//!             states, is far cheaper on this finite model)
//!   compare   baseline, then timed;  all  baseline, timed, rounds
//! `--encoding direct` is an ablation: no buffer thread, the receiver reads
//! Alive messages itself between steps (exact too, more interleavings).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, ConsType, Stats};

// =====================================================================
// Messages
// =====================================================================

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// Self-timer. Sender: its next Send round. Receiver: its next active round.
    Tick,
    /// "p-is-alive" (CT Fig. 10, Task 1). `seq` is the eviction guard.
    Alive { seq: u32 },
    /// From main, arriving at exactly H: stop taking protocol steps.
    EndOfRun,
    /// Sender to its Alive destination after its drain: how many it sent.
    Done { sent: u32 },
    /// Buffer encoding: the receiver's Receive step asks the buffer to
    /// deliver (TKW RRcv), and the buffer answers whether it delivered any.
    Query { reply_to: ThreadId },
    Answer { delivered: bool },
    /// Buffer encoding: the receiver is past H and drained; its summary.
    Stop { summary: Summary },
}

/// What one receiver run observed, judged after every guard has passed.
/// Observation only: the protocol never reads it.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct Summary {
    ever_suspected: bool,
    /// Largest waitingTime compared against the timeout at a Comp step
    /// (None: the run took no Comp step).
    max_wt: Option<u64>,
    /// Active rounds (receiver steps), Receive steps, delivering ones.
    steps: usize,
    receives: usize,
    deliveries: usize,
    /// Alive messages the sender sent (filled in by whoever checks Done).
    sent: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Encoding {
    /// TKW's message buffer is its own thread; the receiver touches it only
    /// in its Receive step, as in TKW's RRcv.
    Buffer,
    /// Ablation: the receiver reads Alive messages itself between steps and
    /// keeps the undelivered ones as a local count.
    Direct,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Control {
    None,
    /// The sender never sends Alive. Must FIRE whenever the receiver can
    /// take 3*ceil((to+1)/3) steps within H (checks the property can fail).
    CrashedSender,
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
    encoding: Encoding,
    control: Control,
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
    /// Round of TLC's shortest counterexample at TODefault = threshold - 1,
    /// as fitted on every (Delta, Phi) of the TLC grid (Delta <= 8, Phi <= 6):
    /// a horizon below it can under-report the threshold.
    fn h_min(&self) -> u64 {
        if self.phi == 1 {
            3 + 3 * self.delta.saturating_sub(2).div_ceil(3)
        } else {
            3 * self.phi + 3 * self.delta.div_ceil(3) + 1 - u64::from(self.delta % 3 == 0)
        }
    }
}

// =====================================================================
// Outcome counters (over executions that reach the judgement)
// =====================================================================

const ZERO: AtomicUsize = AtomicUsize::new(0);
const MAXW: usize = 512;

static COMPLETED: AtomicUsize = AtomicUsize::new(0);
static SUSPECTED: AtomicUsize = AtomicUsize::new(0);
/// Histogram of the per-execution max_wt. W* (its largest bin) is the
/// largest waiting time any execution compares at a Comp step: the verdict
/// is VIOLATED iff W* >= TODefault, and at TODefault > W* the threshold
/// W* + 1 comes out of ONE run (control flow never reads wt or to).
static WHIST: [AtomicUsize; MAXW + 1] = [ZERO; MAXW + 1];
/// [min, max] of steps, receives, deliveries, sent over judged executions.
const PAIR: [AtomicUsize; 2] = [ZERO; 2];
static RANGES: [[AtomicUsize; 2]; 4] = [PAIR; 4];
/// Guard hits, as EVENT counts (a blocked execution is not observable
/// otherwise): a sequence gap at an Alive, a count mismatch at Done.
static GUARD_SEQ: AtomicUsize = AtomicUsize::new(0);
static GUARD_DONE: AtomicUsize = AtomicUsize::new(0);

fn reset_counts() {
    COMPLETED.store(0, Ordering::Relaxed);
    SUSPECTED.store(0, Ordering::Relaxed);
    GUARD_SEQ.store(0, Ordering::Relaxed);
    GUARD_DONE.store(0, Ordering::Relaxed);
    for b in &WHIST {
        b.store(0, Ordering::Relaxed);
    }
    for r in &RANGES {
        r[0].store(usize::MAX, Ordering::Relaxed);
        r[1].store(0, Ordering::Relaxed);
    }
}

/// Record one judged execution and check TKW's StrongAccuracy == ~suspected
/// over every state up to H. Called only after every eviction guard passed.
fn judge(s: Summary) {
    COMPLETED.fetch_add(1, Ordering::Relaxed);
    if let Some(w) = s.max_wt {
        WHIST[(w as usize).min(MAXW)].fetch_add(1, Ordering::Relaxed);
    }
    for (r, v) in RANGES.iter().zip([s.steps, s.receives, s.deliveries, s.sent]) {
        r[0].fetch_min(v, Ordering::Relaxed);
        r[1].fetch_max(v, Ordering::Relaxed);
    }
    if s.ever_suspected {
        SUSPECTED.fetch_add(1, Ordering::Relaxed);
    }
    traceforge::assert(!s.ever_suspected);
}

/// The eviction guard on every Alive channel (sd = 0 lets a receive skip a
/// message that arrived earlier on another channel; see header).
fn check_seq(seq: u32, expected: &mut u32) {
    if seq != *expected {
        GUARD_SEQ.fetch_add(1, Ordering::Relaxed);
    }
    traceforge::assume!(seq == *expected);
    *expected += 1;
}

fn check_done(sent: u32, expected: u32) {
    if sent != expected {
        GUARD_DONE.fetch_add(1, Ordering::Relaxed);
    }
    traceforge::assume!(sent == expected);
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
                self.obs.max_wt = Some(self.obs.max_wt.map_or(self.waiting_time, |m| m.max(self.waiting_time)));
                if self.waiting_time < self.timeout {
                    self.waiting_time += 1;
                } else {
                    self.suspected = true;
                    self.obs.ever_suspected = true;
                }
            }
        }
    }
}

// =====================================================================
// The port: sender, receiver, buffer (timed and baseline modes)
// =====================================================================

/// Process 1 of TKW's cutoff instance: CT Fig. 10, Task 1. `dest` is where
/// Alive and Done go: the buffer thread, or the receiver (direct encoding).
/// Bounded by COUNT, not by EndOfRun: it makes every Send TKW's sender can
/// make by round H and stops. It receives only its own Ticks, one at a time,
/// so no receive of it can skip (and evict) anything.
fn sender(p: Params, dest: ThreadId) {
    let me = thread::current().id();
    let mut sent = 0u32;
    for _ in 0..p.sender_sends() {
        // TKW sTimer: the first Send round is in [3, 3*Phi], so is every gap.
        traceforge::send_msg_timed(me, Msg::Tick, 3, 3 * p.phi);
        let m: Msg = traceforge::recv_msg_block_timed();
        assert_eq!(m, Msg::Tick, "sender: only its own Tick can arrive");
        // SSnd with sTimer = 0: send "p-is-alive". Old at age Delta means
        // arrival at most Delta - 1 rounds after the Send round.
        if p.control != Control::CrashedSender {
            traceforge::send_msg_timed(dest, Msg::Alive { seq: sent }, 0, p.delta - 1);
            sent += 1;
        }
    }
    // Arrives no earlier than any Alive (same FIFO channel).
    traceforge::send_msg_timed(dest, Msg::Done { sent }, p.delta - 1, p.delta - 1);
}

/// The receiver's self-timer (TKW rTimer): the first active round is in
/// [1, Phi], and so is every gap. At most H ticks (see `receiver_ticks`).
struct Clock {
    me: ThreadId,
    phi: u64,
    left: u64,
    armed: bool,
}

impl Clock {
    fn start(p: &Params) -> Self {
        let mut c = Self { me: thread::current().id(), phi: p.phi, left: p.receiver_ticks(), armed: false };
        c.arm();
        c
    }
    fn arm(&mut self) {
        if self.left > 0 {
            self.left -= 1;
            traceforge::send_msg_timed(self.me, Msg::Tick, 1, self.phi);
            self.armed = true;
        }
    }
    /// Harness: a Tick evicted for EndOfRun is a step never taken; waiting
    /// for it blocks such a run before it can be judged.
    fn drain(&mut self) {
        if self.armed {
            let m: Msg = traceforge::recv_msg_block_timed();
            assert_eq!(m, Msg::Tick, "receiver drain");
            self.armed = false;
        }
    }
}

/// Buffer encoding, process 2: steps on its own clock and touches the
/// buffer only in its Receive step.
fn receiver_buffered(p: Params, buffer: ThreadId) {
    let me = thread::current().id();
    let mut d = Detector::new(&p);
    let mut clock = Clock::start(&p);
    loop {
        match traceforge::recv_msg_block_timed::<Msg>() {
            Msg::Tick => {
                clock.armed = false;
                d.step(|| {
                    traceforge::send_msg_timed(buffer, Msg::Query { reply_to: me }, 0, 0);
                    match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == buffer) {
                        Msg::Answer { delivered } => delivered,
                        m => panic!("receiver: unexpected {m:?} from the buffer"),
                    }
                });
                clock.arm();
            }
            Msg::EndOfRun => break,
            m => panic!("receiver: unexpected {m:?}"),
        }
    }
    clock.drain();
    traceforge::send_msg_timed(buffer, Msg::Stop { summary: d.obs }, 0, 0);
}

/// Buffer encoding, TKW's message buffer (their `existsMsgOfAge`): holds
/// every Alive message that has arrived, and on a Query delivers all of
/// them. It judges the run once the sender's Done and the receiver's Stop
/// are in: causally after every eviction guard of every thread.
fn buffer() {
    let mut held: u32 = 0;
    let mut expected: u32 = 0;
    let mut done = false;
    let mut stop: Option<Summary> = None;
    while !(done && stop.is_some()) {
        match traceforge::recv_msg_block_timed::<Msg>() {
            Msg::Alive { seq } => {
                check_seq(seq, &mut expected);
                held += 1;
            }
            Msg::Query { reply_to } => {
                traceforge::send_msg_timed(reply_to, Msg::Answer { delivered: held > 0 }, 0, 0);
                held = 0;
            }
            Msg::Done { sent } => {
                check_done(sent, expected);
                done = true;
            }
            Msg::Stop { summary } => stop = Some(summary),
            m => panic!("buffer: unexpected {m:?}"),
        }
    }
    let mut s = stop.unwrap();
    s.sent = expected as usize;
    judge(s);
}

/// Direct encoding (ablation), process 2: reads Alive messages itself
/// between steps and keeps the undelivered ones as a local count.
fn receiver_direct(p: Params) {
    let mut d = Detector::new(&p);
    let mut clock = Clock::start(&p);
    let mut held: u32 = 0;
    let mut expected: u32 = 0;
    let mut done = false;
    let mut ended = false;
    // One order-agnostic loop: at sd = 0 a fixed drain order would kill
    // whichever message it does not wait for first.
    while !(ended && done && !clock.armed) {
        match traceforge::recv_msg_block_timed::<Msg>() {
            Msg::Alive { seq } => {
                check_seq(seq, &mut expected);
                held += 1;
            }
            Msg::Done { sent } => {
                check_done(sent, expected);
                done = true;
            }
            Msg::Tick => {
                clock.armed = false;
                if !ended {
                    d.step(|| std::mem::take(&mut held) > 0);
                    clock.arm();
                }
            }
            Msg::EndOfRun => ended = true,
            m => panic!("receiver: unexpected {m:?}"),
        }
    }
    let mut s = d.obs;
    s.sent = expected as usize;
    judge(s);
}

fn port(p: Params) {
    let (mut handles, rt, dest) = match p.encoding {
        Encoding::Buffer => {
            let b = thread::spawn(buffer);
            let bt = b.thread().id();
            let r = thread::spawn(move || receiver_buffered(p, bt));
            let rt = r.thread().id();
            (vec![b, r], rt, bt)
        }
        Encoding::Direct => {
            let r = thread::spawn(move || receiver_direct(p));
            let rt = r.thread().id();
            (vec![r], rt, rt)
        }
    };
    handles.push(thread::spawn(move || sender(p, dest)));
    // After the spawns: nothing main sends precedes an Alive in any order.
    traceforge::send_msg_timed(rt, Msg::EndOfRun, p.h, p.h);
    for h in handles {
        let _ = h.join();
    }
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
        if s_timer == 0 && p.control != Control::CrashedSender {
            msgs[0] = true;
            det.obs.sent += 1;
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
    judge(det.obs);
}

// =====================================================================
// Verifier setup and reporting
// =====================================================================

fn build_config(mode: Mode, keep_going: bool, workers: usize, max_execs: Option<u64>) -> Config {
    // FIFO per (sender, destination) channel, pinned (it is the default).
    let mut b = Config::builder().with_progress_report(usize::MAX).with_cons_type(ConsType::FIFO);
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
    let comp = COMPLETED.load(Ordering::Relaxed);
    let susp = SUSPECTED.load(Ordering::Relaxed);
    let wstar = (0..=MAXW).rev().find(|&w| WHIST[w].load(Ordering::Relaxed) > 0);
    let w_at = wstar.map_or(0, |w| WHIST[w].load(Ordering::Relaxed));
    let verdict = if susp > 0 { "VIOLATED" } else { "HOLDS" };
    let enc = match (mode, p.encoding) {
        (Mode::Rounds, _) => "rounds",
        (_, Encoding::Buffer) => "buffer",
        (_, Encoding::Direct) => "direct",
    };
    let control = match p.control {
        Control::None => "none",
        Control::CrashedSender => "crashed-sender",
    };
    println!(
        "{:<8} enc={enc} control={control} delta={} phi={} to={} H={}  execs={} blocked={} \
         impossible={} explored={} completed={comp} violations={susp} W*={} (attained by {w_at}) steps={} \
         receives={} deliveries={} sent={} guard_seq_events={} guard_done_events={} \
         verdict={verdict} time={dur:?}",
        mode.name(),
        p.delta,
        p.phi,
        p.to,
        p.h,
        stats.execs,
        stats.block,
        stats.timeline_impossible,
        stats.execs + stats.block + stats.timeline_impossible,
        wstar.map_or(String::from("-"), |w| w.to_string()),
        range(0),
        range(1),
        range(2),
        range(3),
        GUARD_SEQ.load(Ordering::Relaxed),
        GUARD_DONE.load(Ordering::Relaxed),
    );
    // Every counter above is raised by PROGRAM code, so it also sees endings
    // that the (C6b) timeout-miss condition later judges to admit no timeline:
    // those endings run to completion, raise the counters, and are only then
    // discarded by the engine. Both the verdict and W* are therefore UPPER
    // bounds. A counter-based HOLDS is sound (nothing suspected anywhere);
    // a counter-based VIOLATED, and any threshold derived from W*, are not.
    if stats.timeline_impossible > 0 {
        println!(
            "NOTE: {} of the {} endings explored admit no timeline, and the counters above were \
             raised on them too. violations={susp} and W* are upper bounds: re-run without \
             --keep-going (exit 101 = the engine certified a counterexample) before quoting a \
             VIOLATED verdict or a W*-derived threshold.",
            stats.timeline_impossible,
            stats.execs + stats.block + stats.timeline_impossible
        );
    }
    // Depending on the mode, the engine files a violating execution under
    // execs or under blocked; `completed` counts every judged execution.
    if comp == 0 {
        println!("WARNING ({}): no execution reached the judgement: no data, not a hold.", mode.name());
    }
    // The verdict comes from the harness counters. In the port modes every
    // judged execution must also be one the engine counts as complete; a
    // mismatch would mean a judged run the engine did not certify.
    if mode != Mode::Rounds && comp != stats.execs {
        println!(
            "WARNING ({}): {comp} judged executions but the engine counts {} complete: the \
             harness verdict may include runs the engine did not certify; confirm without --keep-going.",
            mode.name(),
            stats.execs
        );
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
    if wstar.is_none() && susp > 0 {
        println!("INTERNAL ERROR ({}): a suspicion without any Comp step", mode.name());
    }
    if let Some(w) = wstar {
        if (w as u64 >= p.to) != (susp > 0) {
            println!("INTERNAL ERROR ({}): W* = {w} but verdict {verdict} at to = {}", mode.name(), p.to);
        }
        if susp == 0 {
            println!(
                "         W* + 1 = {}: every TODefault >= {} holds and every smaller one is violated, \
                 at this H (one run; control flow never reads wt or to).",
                w + 1,
                w + 1
            );
        }
    }
    if p.h < p.steps_to_suspect() {
        println!(
            "NOTE ({}): H = {} < {} steps a receiver needs to suspect at to = {}: no behaviour can \
             suspect within H, so this run alone shows nothing at this to; its HOLD follows from \
             W* (every to > W* holds at this H).",
            mode.name(),
            p.h,
            p.steps_to_suspect(),
            p.to
        );
    }
    if mode != Mode::Baseline && p.control == Control::None && p.h < p.h_min() {
        println!(
            "NOTE ({}): H = {} < {}, the round of TLC's shortest counterexample at the threshold \
             boundary (fitted on the TLC grid): the threshold may be under-reported.",
            mode.name(),
            p.h,
            p.h_min()
        );
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut mode = String::from("timed");
    let (mut delta, mut phi, mut to, mut h) = (2u64, 4u64, None, None);
    let mut keep_going = false;
    let mut encoding = Encoding::Buffer;
    let mut control = Control::None;
    let mut workers = 1usize;
    let mut max_execs = None;
    while let Some(a) = args.next() {
        let mut val = |f: &str| args.next().unwrap_or_else(|| panic!("{f} needs a value"));
        match a.as_str() {
            "--mode" => mode = val("--mode"),
            "--encoding" => {
                encoding = match val("--encoding").as_str() {
                    "buffer" => Encoding::Buffer,
                    "direct" => Encoding::Direct,
                    e => panic!("unknown encoding {e}"),
                }
            }
            "--control" => {
                control = match val("--control").as_str() {
                    "none" => Control::None,
                    "crashed-sender" => Control::CrashedSender,
                    c => panic!("unknown control {c}"),
                }
            }
            "--delta" => delta = val("--delta").parse().expect("--delta"),
            "--phi" => phi = val("--phi").parse().expect("--phi"),
            "--to" => to = Some(val("--to").parse().expect("--to")),
            "--horizon" => h = Some(val("--horizon").parse().expect("--horizon")),
            "--keep-going" => keep_going = true,
            "--workers" => workers = val("--workers").parse().expect("--workers"),
            "--max-execs" => max_execs = Some(val("--max-execs").parse().expect("--max-execs")),
            _ => panic!(
                "unknown flag {a}. Usage: chandra_toueg_timed [--mode timed|baseline|rounds|compare|all] \
                 [--delta D>=1] [--phi P>=1] [--to T] [--horizon H] [--encoding buffer|direct] \
                 [--control none|crashed-sender] [--keep-going] [--workers N] [--max-execs N]"
            ),
        }
    }
    assert!(delta >= 1, "Delta = 0 is not representable exactly by the port (see header)");
    assert!(phi >= 1, "Phi >= 1");
    let to = to.unwrap_or(6 * phi + delta);
    let mut p = Params { delta, phi, to, h: 0, encoding, control };
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
