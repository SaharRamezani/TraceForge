//! Alternating bit protocol: passing a list of messages over a wire that loses some.
//!
//! ## The idea in plain words
//!
//! Two programs, a sender and a receiver, want to pass a short list of
//! messages across a wire that now and then throws a message away
//! without telling anyone. The receiver must hand every message to its
//! user exactly once, in the original order, with nothing missing and
//! nothing repeated.
//!
//! The sender works on one message at a time. It puts one extra mark on
//! the message, 0 or 1, and the mark flips from one message to the
//! next: the first message is marked 0, the second 1, the third 0
//! again, and so on. After sending, the sender waits a little while for
//! a reply from the receiver (an "acknowledgement": a short note saying
//! "I got it"). If a reply comes back, the sender moves on to the next
//! message. If the wait runs out first, the sender assumes something was
//! thrown away and sends the same message again, with the same mark. So
//! that this example ends, the sender gives up after a fixed number of
//! repeats.
//!
//! The receiver remembers which mark it expects next. A message with
//! that mark is new: the receiver hands it to its user, starts expecting
//! the other mark, and replies. A message with the other mark is an
//! extra copy of something already handed over: it is not handed over
//! again, but the receiver still replies, because its earlier reply may
//! be exactly what got lost. In the alternating bit protocol every reply
//! repeats the mark of the message it answers, and the sender ignores a
//! reply whose mark is not the one it is waiting for.
//!
//! There is one timer: how long the sender waits for a reply before it
//! sends again. Everything else about time is outside anyone's control:
//! a message and a reply each spend some time on the wire, and the
//! receiver may spend a moment handling a new message before it replies.
//!
//! What can go wrong is a wait that is too short. The sender then sends
//! an extra copy while the first reply is still on its way, and two
//! replies come back for one message. With marks on the replies this is
//! harmless: by the time the second reply arrives the sender is waiting
//! for the other mark, so it ignores it. That is why the alternating bit
//! protocol stays correct however short the wait is, as long as the
//! wire never swaps the order of two messages.
//!
//! The file also contains a cousin, called "par", that is the same in
//! every way except that replies carry no mark: any reply counts. Now a
//! short wait is dangerous. The sender takes the first reply as the
//! answer to message 0 and sends message 1. If the wire throws message 1
//! away, the second reply to message 0 turns up a moment later and the
//! sender takes it as the answer to message 1. It moves on and never
//! sends message 1 again, so the receiver's user silently never gets it.
//! This can only happen when the wait is no longer than a full trip
//! there and back: the time the message spends on the wire, plus how
//! long it may lie waiting after it arrives before the receiver picks it
//! up, plus the receiver's handling time, plus the same two times for
//! the reply on its way back. The tool used here tries every possible
//! order and timing of events, and so finds exactly how short the wait
//! must be for this to happen (see "Expected verdicts" below).
//!
//! The marking idea comes from K. A. Bartlett, R. A. Scantlebury and
//! P. T. Wilkinson, "A Note on Reliable Full-Duplex Transmission over
//! Half-Duplex Links", Communications of the ACM 12(5):260-261, 1969.
//! Their version lets data flow both ways over a line that carries only
//! one direction at a time, with the two ends taking turns. It resends a
//! message when an error is detected, and it recovers from lost messages
//! the same way as an earlier scheme by W. C. Lynch, which their note
//! does not spell out. The one-way form with a resend timer used here is
//! the later textbook form. The unmarked-reply cousin and its
//! too-short-wait failure come from D. Bosnacki and D. Dams,
//! "Integrating Real Time into Spin: A Prototype Implementation",
//! FORTE/PSTV 1998, Kluwer, pp. 423-439 (with the companion tool paper "Discrete-Time Promela and Spin", FTRTFT 1998,
//! LNCS 1486, pp. 307-310), who write that the wait "should be longer
//! than the sum of the delays through the channels and the message
//! processing time by the receiver". A timed benchmark version of the
//! protocol is the AlternatingBit model of F. M. Bonneland, P. G. Jensen,
//! K. G. Larsen, M. Muniz and J. Srba, "Start Pruning When Time Gets
//! Urgent: Partial Order Reduction for Timed Systems", CAV 2018, LNCS
//! 10981, pp. 527-546.
//!
//! ## How the protocol maps onto TraceForge
//!
//! Two protocol threads (sender, receiver) plus main, which spawns
//! both, sends each an `Init` naming both ThreadIds, and joins them (an
//! Init may be dropped like every send; that thread then blocks). The
//! `Init` reads are untimed blocking receives filtered on main's id (the
//! swim_timed.rs bootstrap idiom): an untimed receive is transparent
//! for timing, so both protocol threads start their clocks together and
//! no frame can reach the receiver before it listens.
//!
//! Sender, for payload i in 0..K with bit = i % 2:
//!
//!   send_lossy_msg(receiver, Frame{bit, payload: i})
//!   loop recv_tagged_msg_timed(from receiver, Finite(W)):
//!     Some(Ack{bit: a})  abp: accept iff a == Some(bit), else discard
//!                             (STALE_ACKS) and wait a fresh W
//!                        par: accept any Ack
//!     None               retransmit the same frame (lossy) if fewer
//!                        than R retransmissions of message i so far,
//!                        otherwise give up (GAVE_UP)
//!   after K accepted Acks: Done{completed: true}; after giving up:
//!   Done{completed: false}. Done may be dropped like every send; the
//!   receiver then blocks and the execution counts as blocked.
//!
//! The sender reads ANY acknowledgement from the receiver and discards a
//! wrong bit in code. It must not select the current bit with the
//! receive predicate: a skipped stale Ack would stay readable in the
//! untimed baseline and be taken for a fresh Ack one cycle later, a
//! reordering made by the harness, not by the wire.
//!
//! Receiver, expecting bit e (initially 0) and payload `next`
//! (initially 0), blocking timed receive from the sender:
//!
//!   Frame{bit == e, payload}  deliver: P1, DELIVERED, sleep(dR) when
//!                             dR > 0, flip e, send_lossy_msg Ack
//!   Frame{bit != e}           duplicate: DUPLICATES, send_lossy_msg Ack
//!   Done{completed}           P2 when completed, then return
//!
//! Ack is `Ack{bit: Some(frame bit)}` for abp and `Ack{bit: None}` for
//! par (DT-Spin's PAR acknowledgement is the constant ACK and its sender
//! reads it with `in?_`). Frames are identical in both variants: PAR's
//! frames carry the alternating sequence bit and its receiver discards
//! duplicates exactly as ABP's does; only the acknowledgement check
//! differs. Acks also carry `answers`, the payload of the frame they
//! acknowledge. It is ghost data for the CONFUSED_ACKS counter (the
//! sender accepted an Ack that answers an older frame) and no protocol
//! decision reads it.
//!
//! Time: both directions use the global transit window [L, U] of
//! `with_timed(L, U, sd)`; sd is the storage lifetime (how long a
//! message stays readable after it arrives). W is the sender's timeout,
//! anchored at the (re)send, and dR the receiver's processing time on
//! the new-frame path (DT-Spin's `delay(rc, dR)` at S_h). Loss: every
//! send may be dropped, with no budget, in BOTH modes (timed configs do
//! this by default, baseline via `with_all_sends_lossy()`). The channel
//! is TraceForge's default, order-preserving per sender and receiver pair, which is the
//! setting in which ABP is correct at every timeout. Baseline and timed
//! verify the identical program; the only difference is the Config.
//!
//! PAR here means the positive acknowledgement with retransmission
//! protocol; Bosnacki and Dams spell the acronym "Parallel
//! Acknowledgment with Retransmission".
//!
//! ## Deviations from the paper (deliberate)
//!
//! * Simplex, timer-driven ABP instead of Bartlett et al.'s full-duplex
//!   scheme over a half-duplex line (messages alternate direction). Their
//!   note resends on a detected error and says only that the recovery
//!   from message drops is "the same for both schemes" (theirs and
//!   Lynch's), without describing it. Timers, and therefore anything for
//!   a timed checker to decide, are explicit only in the later simplex
//!   form (the DT-Spin PAR model and the TAPAAL/UPPAAL AlternatingBit
//!   net).
//! * Any send may be dropped, with no budget, including the harness
//!   messages Init and Done. Detected corruption (DT-Spin's MSGerr and ACKerr branches) is folded
//!   into loss; DT-Spin's "resend at once on a corrupt ack" path is not
//!   modelled.
//! * No channel processes: transit is folded into the sends and both
//!   directions share [L, U]. TraceForge channels are unbounded queues,
//!   DT-Spin's are one-place, so more frames can be in flight here.
//! * A frame that reaches a busy receiver can also be lost here by
//!   expiring in storage. In DT-Spin the data channel hands a frame over
//!   by rendezvous (`chan B = [0]`), so while the receiver is in
//!   `delay(rc, dR)` the frame waits inside channel K and is lost only
//!   through K's explicit loss branch. Here, with dR > 0, a stored frame
//!   can expire during the receiver's processing sleep (side effect (b)
//!   below).
//! * K messages, then stop, with at most R retransmissions per message.
//!   TAPAAL instead bounds in-flight tokens and DT-Spin cycles payloads
//!   modulo MAX = 8 forever. Payloads here are 0..K-1.
//! * Order-preserving channels instead of TAPAAL's unordered media
//!   (places are multisets). In TAPAAL, ABP's safety leans on timing;
//!   here it does not. The two are not the same verification problem.
//! * One fixed timeout W instead of TAPAAL's resend window [5, 6], and
//!   an exact processing sleep dR instead of TAPAAL's ack deadline
//!   [0, 2] (a TraceForge receive has one wait value, sleep is exact).
//!   dR is applied only when a new frame is delivered, as in DT-Spin.
//! * DT-Spin's one-tick delays after accepting and after sending an ack,
//!   and its unbounded start delay, are omitted (modelling artefacts
//!   that only shift constants).
//! * After a stale (wrong-bit) Ack, the abp sender waits a fresh W: a
//!   TraceForge program cannot read the clock, so it cannot resume a
//!   partly elapsed timer. This only delays a retransmission.
//! * The property is data-level (DT-Spin's own `assert(mr == me)` plus
//!   an end-of-run gap check) rather than TAPAAL's phase query over net
//!   places.
//!
//! ## Timeout-validation assumptions (verification harness, NOT the protocol)
//!
//! None are added, deliberately, and this section explains why the
//! swim_timed.rs idiom does not apply. A timeout branch is always
//! explorable in both modes and carries no evidence that the awaited
//! Ack was really late. That makes an assertion ON a timeout branch
//! worthless, and swim_timed.rs repairs it with a blocking read of the
//! awaited message. Here no assertion sits on a timeout branch: a
//! timeout only triggers a retransmission, and both assertions live at
//! the receiver. The one way a premature timeout can lead to a violation
//! is the PAR confusion, and that path runs through the sender's own
//! later read of the Ack it timed out on: that protocol read happens
//! after the deadline, so the timed engine constrains it at least as
//! strictly as a validation read would. It is feasible only if the Ack
//! can still be read at or after the deadline, and, being a bounded
//! read, it must also land before the sender gives up at (R + 1) W,
//! which is the source of the lower edge 2L + dR <= (R + 1) W (a
//! blocking validation read would not impose that). Adding a validation
//! read would be wrong twice over: it would consume that late Ack and so
//! delete the very bug under study, and it would block every timeout
//! caused by a dropped frame or Ack (a dropped message can never be
//! read), discarding the loss recovery the protocol exists for. The
//! claim is checked on the certified witness of every timed par FIRE
//! reported below (the sender's first Ack read is at or after its first
//! deadline) and by par holds just above the boundary with
//! DUPLICATES > 0.
//!
//! Two side effects of timed storage are accepted and documented. (a)
//! A message not read within sd of its arrival expires. A timeout
//! branch taken while an Ack was readable can therefore let that Ack
//! expire unread: an Ack lost by expiry rather than by a drop, harmless for
//! both properties (losing Acks only causes retransmissions), and the
//! reason timed can explore MORE executions than baseline when sd is
//! small. (b) With dR > 0, a frame stored while the receiver processes
//! an earlier one can expire unread (the receiver may read a frame up
//! to sd late and then sleep dR), a frame lost by expiry; an
//! expired Done shows up as blocked executions. With dR = 0 no frame
//! can expire: every later frame arrives no earlier than the one just
//! read, and the receiver is ready again at once. The binary prints a
//! NOTE for timed runs with dR > 0.
//!
//! ## Properties checked (assertions at the receiver)
//!
//!   P1 (every delivery, both variants): payload == next expected
//!       payload. DT-Spin's `assert(mr == me)`; catches a duplicate
//!       delivery and a gap in the middle of the sequence.
//!   P2 (on Done{completed: true}): delivered == K. Catches the silent
//!       gap at the tail: the sender believes all K messages were
//!       acknowledged but the receiver is missing some. This is the
//!       Bosnacki-Dams "never resends it" outcome; with K = 2 it is the
//!       only place the PAR bug can show.
//!
//! No assertion on Done{completed: false}: a sender that gave up because
//! every copy of a frame or of its Ack was lost is not a violation
//! (counted in GAVE_UP; P1 still covered the prefix it delivered).
//! "Every message is eventually delivered" is deliberately not asserted:
//! it is false under loss and inconclusive in a bounded model.
//!
//! Outcome counters accumulate over all explored executions (revisited
//! prefixes may re-count, so read them as zero/nonzero evidence):
//! DELIVERED, DUPLICATES (receiver re-acked an old frame), TIMEOUTS
//! (sender retransmissions), STALE_ACKS (abp sender discarded a
//! wrong-bit Ack), CONFUSED_ACKS (sender accepted an Ack answering an
//! older frame), COMPLETED, GAVE_UP, END_CHECKS (P2 evaluated),
//! P1_FAILS and P2_FAILS (counted before the assertion so
//! --keep-going can report them).
//!
//! ## CLI parameters (ratios are over U, like swim_timed)
//!
//!   --mode baseline|timed|compare   verification mode (default compare;
//!                                   par with R >= 1 FIREs in its
//!                                   baseline leg, so compare aborts
//!                                   there unless --keep-going: pass
//!                                   --mode timed to see par's timed
//!                                   verdict)
//!   --variant abp|par               acks carry the bit (abp) or not (par)
//!                                   (default abp)
//!   --messages K                    messages to transfer (default 2)
//!   --retries R                     retransmissions per message before
//!                                   the sender gives up (default 1;
//!                                   verification scaffolding)
//!   --u U                           transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U) (default 0.0)
//!   --sd-ratio SR                   sd = round(SR * U) (default 0.0)
//!   --w-ratio WR                    sender timeout W = round(WR * U)
//!                                   (default 3.0; must round to >= 1)
//!   --proc-ratio PR                 receiver processing dR = round(PR * U)
//!                                   (default 0.0)
//!   --keep-going                    explore past a violation and report
//!                                   p1_fails= and p2_fails= (plus a
//!                                   VIOLATION line when either is
//!                                   nonzero) instead of aborting; the
//!                                   exit code is then 0 either way
//!   --parallel none|shared|partitioned  exploration strategy (default none)
//!
//! DT-Spin's PAR rows (dK, dL, dR, To) = (3, 3, 1, 9), (30, 30, 10, 90),
//! (300, 300, 100, 900) are `--l-ratio 1 --proc-ratio 0.333 --w-ratio 3`
//! at `--u 3`, `--u 30` and `--u 300`.
//!
//! Exit codes: 0 = hold over the explored state space, 101 = P1 or P2
//! violated, 2 = CLI misuse (distinct from 101, so exit-code-based sweep
//! harnesses cannot mistake a typo for a FIRE). With --keep-going the
//! exploration does not abort and the exit code is 0 even when a
//! property was violated: the verdict is then p1_fails/p2_fails and the
//! printed VIOLATION line, never the exit code.
//!
//! ## Expected verdicts (re-measured 2026-09-30, every send lossy)
//!
//! Every line below comes from a run of this binary. Unless stated:
//! K = 2, R = 1, L = 0, sd = 0, dR = 0. A "hold" is exit 0 with
//! end_checks > 0 and dups > 0 (both properties evaluated, duplicate
//! frames explored); "no completion" marks exit-0 cells where no
//! execution completed (end_checks = 0, WARNING printed), which check P1
//! only and are NOT holds of P2.
//!
//!   abp, baseline: HOLD at every W tried (U = 1: execs 122, blocked 132,
//!       stale 58, confused 0); K=3 R=2 W=3: execs 107388, blocked 107406.
//!
//!   abp, timed: HOLD at every cell tried: U = 1, W = 1..4 (execs/blocked
//!       140/150, 104/114, 34/44, 34/44); sd = 1 at W = 1..3; DT-Spin row
//!       (L = U = 3, dR = 1) at W = 4 (34/44) and U = 30 at W = 90 (25/35);
//!       K=3 R=2 W=3 (1142/1160). The premature-timeout path really runs:
//!       stale = 58 at U=1 W=1, 47 at sd=1 W=3, 0 at W=3 sd=0.
//!       confused = 0 in every abp run.
//!
//!   par, baseline: FIRE (exit 101). With --keep-going: execs 105,
//!       p2_fails 2. The counterexample (read on the 2026-09-16 witness)
//!       is the Bosnacki-Dams scenario: frame 0, timeout, frame 0 again,
//!       first Ack accepted, frame 1 dropped, second Ack accepted for
//!       frame 1, Done{completed: true} after one delivery (P2). HOLD with
//!       R = 0 (execs 7, blocked 13; timed 6/12).
//!
//!   par, timed: FIRE iff  W <= 2U + sd + dR  and  2L + dR <= (R + 1) W.
//!       The tie W = 2U + sd + dR FIREs. Since the (C6b) timeout-miss rule
//!       a timeout may not pass over a readable Ack, so the late Ack must
//!       arrive after the deadline; one sd of the old 2U + 2sd + dR
//!       (measured 2026-09-16) is gone. Upper edge, measured:
//!         U=1             FIRE W=1,2    hold W=3,4
//!         U=2             FIRE W=4      hold W=5
//!         U=1 sd=1        FIRE W=3      hold W=4
//!         U=1 sd=2        FIRE W=3,4    hold W=5,6
//!         U=2 sd=1        FIRE W=5      hold W=6
//!         U=1 dR=1        FIRE W=3      hold W=4
//!         U=2 sd=1 dR=2   FIRE W=6,7    hold W=8
//!         U=2 L=1 sd=1    FIRE W=5      hold W=6
//!         K=3, and R=2:   FIRE W=2      hold W=3
//!         K=3 R=2 sd=1    FIRE W=2,3    hold W=4
//!       Lower edge: when (R + 1) W < 2L + dR no Ack can come back before
//!       the sender gives up, so nothing completes.
//!         U=L=2           no completion W=1   FIRE W=2,4   hold W=5
//!       DT-Spin rows (L = U, dR = round(0.333 U)):
//!         U=3   R=1   no completion W=3      FIRE W=4,7      hold W=8,9
//!         U=3   R=2   no completion W=2      FIRE W=3        hold W=8
//!         U=30  R=1   no completion W=34     FIRE W=35,70    hold W=71,90
//!         U=300 R=1   no completion W=349    FIRE W=350,700  hold W=701
//!       So Bosnacki and Dams' "To > dK + dL + dR" is exactly the
//!       measured upper edge at L = U, sd = 0 (dR on the delivery path).
//!       Holds above the boundary are not empty: dups > 0, timeouts > 0,
//!       end_checks > 0, confused = 0 (U=1 W=3: execs 34, blocked 44,
//!       end_checks 12; DT-Spin rows above the edge: execs 25, blocked
//!       35, end_checks 9). On 2026-09-16 all 35 timed FIRE witnesses
//!       then found were checked by script (11 also read by hand): each
//!       shows the sender's first Ack read at or after a timeout deadline,
//!       a retransmitted copy, and the next frame dropped.
//!
//! Cost. Above the round trip timing prunes: at U = 1, W = 3, abp timed
//! explores 34 execs + 44 blocked (baseline 122 + 132); K=3 R=2 W=3:
//! 1142 + 1160 in 1.4 s (baseline 107388 + 107406 in 19 s). At W = 1
//! timed explores MORE than baseline (140 + 150 vs 122 + 132) because
//! unread messages expire (side effect (a)); with sd = 20 the gap
//! nearly closes (125 + 135). Scale: on the DT-Spin rows above the edge
//! both variants explore 25 executions in about 20 ms at U = 3, 30 and
//! 300. WITHDRAWN 2026-09-19:
//! the comparison with Table 2's 1318 / 7447 / 68737 states, even as a
//! shape. That growth is single-tick unfolding of the 1998 prototype;
//! DT-Spin 4.1.1 on the same listing gives 641 states at all three rows
//! in its default multiple-tick mode, and this file's UNTIMED mode is as
//! flat as its timed mode. See the B02 section of the benchmark survey
//! and par_timed.rs (the transliteration of the DT-Spin listing).
//! Times are single-threaded wall clock.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

// Outcome counters: event counts accumulated across all explored
// executions of one verify run (revisited prefixes may re-count, so
// treat them as zero/nonzero evidence, not exact per-exec tallies).
// Reset before each verify, read after.
static DELIVERED: AtomicUsize = AtomicUsize::new(0);
static DUPLICATES: AtomicUsize = AtomicUsize::new(0);
static TIMEOUTS: AtomicUsize = AtomicUsize::new(0);
static STALE_ACKS: AtomicUsize = AtomicUsize::new(0);
static CONFUSED_ACKS: AtomicUsize = AtomicUsize::new(0);
static COMPLETED: AtomicUsize = AtomicUsize::new(0);
static GAVE_UP: AtomicUsize = AtomicUsize::new(0);
static END_CHECKS: AtomicUsize = AtomicUsize::new(0);
/// P1 and P2 failures, counted BEFORE the assertion so that
/// `--keep-going` runs report how many explored executions break each.
static P1_FAILS: AtomicUsize = AtomicUsize::new(0);
static P2_FAILS: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_MESSAGES: u32 = 2;
const DEFAULT_RETRIES: u32 = 1;
const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 0.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const DEFAULT_W_RATIO: f64 = 3.0;
const DEFAULT_PROC_RATIO: f64 = 0.0;

/// Sender to receiver.
#[derive(Clone, Debug, PartialEq)]
enum SMsg {
    /// A data frame: the alternating bit plus the payload (DT-Spin's
    /// `out!mt,sn`). Identical in both variants.
    Frame { bit: u8, payload: u32 },
    /// Verification harness, not the protocol: ends the reactive
    /// receiver and enables the end-of-run gap check (P2). May be dropped
    /// like every send; the receiver then blocks (a blocked execution).
    Done { completed: bool },
}

/// Receiver to sender.
#[derive(Clone, Debug, PartialEq)]
enum RMsg {
    /// `bit` is Some(frame bit) for abp and None for par. `answers` is
    /// ghost data (the payload of the acknowledged frame), read only by
    /// the CONFUSED_ACKS counter, never by a protocol decision.
    Ack { bit: Option<u8>, answers: u32 },
}

/// Bootstrap message from main, a separate type so the init-wait
/// (filtered on main's id) matches only Init.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    /// Alternating bit protocol: acks repeat the frame's bit.
    Abp,
    /// Positive acknowledgement with retransmission: acks carry no bit.
    Par,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Variant::Abp => "abp",
            Variant::Par => "par",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    l: u64,
    sd: u64,
    /// Sender retransmission timeout.
    w: u64,
    /// Receiver processing time on the new-frame path.
    proc_delay: u64,
}

#[derive(Clone, Copy, Debug)]
struct Params {
    variant: Variant,
    messages: u32,
    retries: u32,
    b: Bounds,
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

impl Bounds {
    fn from_ratios(u: u64, l_ratio: f64, sd_ratio: f64, w_ratio: f64, proc_ratio: f64) -> Self {
        if u < 1 {
            cli_bail("U must be >= 1");
        }
        for (r, flag) in [
            (l_ratio, "--l-ratio"),
            (sd_ratio, "--sd-ratio"),
            (w_ratio, "--w-ratio"),
            (proc_ratio, "--proc-ratio"),
        ] {
            if !(r >= 0.0) || !r.is_finite() {
                cli_bail(&format!("{flag} must be a finite ratio >= 0"));
            }
        }
        let scale = |r: f64| (r * u as f64).round() as u64;
        let l = scale(l_ratio);
        if l > u {
            cli_bail("transit lower bound L must be <= U (check --l-ratio)");
        }
        let sd = scale(sd_ratio);
        let w = scale(w_ratio);
        if w < 1 {
            cli_bail("W must round to >= 1 (check --w-ratio)");
        }
        let proc_delay = scale(proc_ratio);
        Self { u, l, sd, w, proc_delay }
    }

    /// 2U + 2sd + dR: the latest time, after a send, at which the Ack
    /// to that frame can still be read. Printed for reference only; the
    /// verdicts come from the checker.
    fn round_trip(self) -> u64 {
        2 * self.u + 2 * self.sd + self.proc_delay
    }

    /// 2U + sd + dR: the latest deadline at which a timeout can still be
    /// followed by a read of the Ack (the timeout may not pass over a
    /// readable Ack, so the Ack must arrive after it).
    fn premature_edge(self) -> u64 {
        2 * self.u + self.sd + self.proc_delay
    }
}

// =====================================================================
// Sender
// =====================================================================

fn sender(p: Params, main_tid: ThreadId) {
    let Init { receiver, .. } =
        traceforge::recv_tagged_msg_block_timed::<_, Init>(move |s, _tag| s == main_tid);

    for i in 0..p.messages {
        let bit = (i % 2) as u8;
        let mut retransmissions: u32 = 0;
        traceforge::send_lossy_msg(receiver, SMsg::Frame { bit, payload: i });

        loop {
            // Read ANY acknowledgement from the receiver (see the doc
            // header: a bit-selective predicate would leave stale Acks
            // readable in baseline).
            match traceforge::recv_tagged_msg_timed::<_, RMsg>(
                move |s, _tag| s == receiver,
                WaitTime::Finite(p.b.w),
            ) {
                Some(RMsg::Ack { bit: ack_bit, answers }) => {
                    let accept = match p.variant {
                        Variant::Par => true,
                        Variant::Abp => ack_bit == Some(bit),
                    };
                    if accept {
                        if answers != i {
                            // Ghost observation only: this Ack answers
                            // an older frame (the PAR confusion).
                            CONFUSED_ACKS.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    }
                    // abp: wrong bit, an Ack for the previous message.
                    // Ignore it and keep waiting (a fresh W).
                    STALE_ACKS.fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    if retransmissions == p.retries {
                        // Bounded-model scaffolding: give up on message i.
                        GAVE_UP.fetch_add(1, Ordering::Relaxed);
                        traceforge::send_msg(receiver, SMsg::Done { completed: false });
                        return;
                    }
                    retransmissions += 1;
                    TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                    traceforge::send_lossy_msg(receiver, SMsg::Frame { bit, payload: i });
                }
            }
        }
    }

    // The sender believes all K messages were acknowledged.
    COMPLETED.fetch_add(1, Ordering::Relaxed);
    traceforge::send_msg(receiver, SMsg::Done { completed: true });
}

// =====================================================================
// Receiver: purely reactive (blocking receives only, no timers)
// =====================================================================

fn receiver(p: Params, main_tid: ThreadId) {
    let Init { sender, .. } =
        traceforge::recv_tagged_msg_block_timed::<_, Init>(move |s, _tag| s == main_tid);

    let mut expected_bit: u8 = 0;
    let mut next: u32 = 0;
    let mut delivered: u32 = 0;
    let ack_bit = |bit: u8| match p.variant {
        Variant::Abp => Some(bit),
        Variant::Par => None,
    };

    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, SMsg>(move |s, _tag| s == sender) {
            SMsg::Frame { bit, payload } if bit == expected_bit => {
                // New frame: hand it to the user.
                if payload != next {
                    P1_FAILS.fetch_add(1, Ordering::Relaxed);
                }
                // P1: the delivered sequence is exactly 0, 1, 2, ...
                traceforge::assert(payload == next);
                DELIVERED.fetch_add(1, Ordering::Relaxed);
                delivered += 1;
                next = payload + 1;
                if p.b.proc_delay > 0 {
                    // Processing time (DT-Spin dR), unconditional in
                    // both modes; a no-op in baseline.
                    traceforge::sleep(p.b.proc_delay);
                }
                expected_bit ^= 1;
                traceforge::send_lossy_msg(sender, RMsg::Ack { bit: ack_bit(bit), answers: payload });
            }
            SMsg::Frame { bit, payload } => {
                // Extra copy of a frame already delivered: acknowledge
                // again (the earlier Ack may have been lost), do not
                // deliver.
                DUPLICATES.fetch_add(1, Ordering::Relaxed);
                traceforge::send_lossy_msg(sender, RMsg::Ack { bit: ack_bit(bit), answers: payload });
            }
            SMsg::Done { completed } => {
                if completed {
                    END_CHECKS.fetch_add(1, Ordering::Relaxed);
                    if delivered != p.messages {
                        P2_FAILS.fetch_add(1, Ordering::Relaxed);
                    }
                    // P2: a sender that believes every message was
                    // acknowledged leaves no silent gap at the tail.
                    traceforge::assert(delivered == p.messages);
                }
                return;
            }
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

/// Exploration strategy chosen on the command line (`--parallel`):
/// `none` (single-threaded, the default; keeps the exact exit-code
/// semantics of an aborting assertion), `shared` (the shared work-queue
/// pool) or `partitioned`.
static PARALLEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn apply_parallel(builder: traceforge::ConfigBuilder) -> traceforge::ConfigBuilder {
    match PARALLEL.get().map(|s| s.as_str()).unwrap_or("none") {
        "none" => builder,
        "shared" => builder.with_parallel(true),
        "partitioned" => builder.with_partitioned_parallelization(true),
        other => cli_bail(&format!("invalid --parallel: {other} (expected none|shared|partitioned)")),
    }
}

fn build_config(mode: Mode, p: Params, keep_going: bool) -> Config {
    // Every send may be dropped, no budget, in both modes; with_timed is
    // the only difference between baseline and timed.
    let mut builder =
        apply_parallel(Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX));
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(p.b.l, p.b.u, p.b.sd).build(),
    }
}

fn reset_counts() {
    for c in [
        &DELIVERED,
        &DUPLICATES,
        &TIMEOUTS,
        &STALE_ACKS,
        &CONFUSED_ACKS,
        &COMPLETED,
        &GAVE_UP,
        &END_CHECKS,
        &P1_FAILS,
        &P2_FAILS,
    ] {
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
        // Send both Inits before joining anyone.
        traceforge::send_msg(r.thread().id(), init.clone());
        traceforge::send_msg(s.thread().id(), init);
        let _ = r.join();
        let _ = s.join();
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    delivered: usize,
    duplicates: usize,
    timeouts: usize,
    stale: usize,
    confused: usize,
    completed: usize,
    gave_up: usize,
    end_checks: usize,
    p1_fails: usize,
    p2_fails: usize,
}

fn read_counts() -> Counts {
    Counts {
        delivered: DELIVERED.load(Ordering::Relaxed),
        duplicates: DUPLICATES.load(Ordering::Relaxed),
        timeouts: TIMEOUTS.load(Ordering::Relaxed),
        stale: STALE_ACKS.load(Ordering::Relaxed),
        confused: CONFUSED_ACKS.load(Ordering::Relaxed),
        completed: COMPLETED.load(Ordering::Relaxed),
        gave_up: GAVE_UP.load(Ordering::Relaxed),
        end_checks: END_CHECKS.load(Ordering::Relaxed),
        p1_fails: P1_FAILS.load(Ordering::Relaxed),
        p2_fails: P2_FAILS.load(Ordering::Relaxed),
    }
}

/// Vacuity guards. A run with zero complete executions verified
/// nothing; a run where P2 was never evaluated checked only P1; a run
/// where no duplicate frame ever reached the receiver never exercised
/// retransmission of a delivered frame, the path both variants exist to
/// handle. None of these exit-0 outcomes is a full hold.
fn warn_if_vacuous(label: &str, mode: Mode, p: Params, execs: usize, blocked: usize, c: Counts) {
    // A P1 violation is aborted by the assertion, so the checker files it
    // under `blocked`, not `execs`. Warning on execs == 0 alone would tell
    // the reader to discard a run that found counterexamples.
    if execs == 0 && c.p1_fails + c.p2_fails == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}) and no violation: \
             every execution blocked, this run verified nothing; treat it as no data, not a hold."
        );
    }
    if c.delivered == 0 {
        println!("WARNING ({label}): no frame was ever delivered; P1 was never evaluated.");
    }
    if c.end_checks == 0 {
        println!(
            "WARNING ({label}): no execution in which the sender completed all K messages \
             (end_checks=0); P2 was never evaluated, treat this hold as partial."
        );
    }
    if p.retries >= 1 && c.duplicates == 0 {
        println!(
            "WARNING ({label}): no duplicate frame ever reached the receiver; the \
             retransmission path was not exercised, this hold is likely vacuous."
        );
    }
    // A premature timeout followed by a read of the late Ack is timed
    // feasible iff W <= 2U + sd + dR and 2L + dR <= (R + 1) W (the
    // measured par boundary, see the doc header); untimed it is always
    // feasible.
    let premature_possible = mode == Mode::Baseline
        || (p.b.w <= p.b.premature_edge()
            && (u64::from(p.retries) + 1) * p.b.w >= 2 * p.b.l + p.b.proc_delay);
    if p.variant == Variant::Abp
        && p.messages >= 2
        && p.retries >= 1
        && premature_possible
        && c.stale == 0
    {
        println!(
            "WARNING ({label}): the abp sender never discarded a stale Ack although a \
             premature timeout is possible here; the premature-timeout path was not explored."
        );
    }
    if mode == Mode::Timed && p.b.proc_delay > 0 {
        println!(
            "NOTE ({label}): dR={} > 0: a frame (or Done) stored while the receiver processes an \
             earlier frame can expire unread, a loss the network did not choose that baseline \
             does not have (an expired Done shows up as blocked executions).",
            p.b.proc_delay
        );
    }
}

/// Only reachable with `--keep-going` (otherwise the first violation
/// aborts with exit 101): the process still exits 0, so say it loudly.
fn warn_if_violated(label: &str, c: Counts) {
    if c.p1_fails > 0 || c.p2_fails > 0 {
        println!(
            "VIOLATION ({label}): p1_fails={} p2_fails={}; --keep-going explored past it, so \
             the exit code is 0 but this is a FIRE, not a hold.",
            c.p1_fails, c.p2_fails
        );
    }
}

// =====================================================================
// Reporting
// =====================================================================

fn print_one(label: &str, p: Params, stats: &Stats, dur: Duration, c: Counts) {
    let b = p.b;
    println!(
        "{label:<9} variant={v} K={k} R={r}  L={l} U={u} sd={sd} W={w} dR={dr} \
         (2U+2sd+dR={rt})  execs={execs:<7} blocked={block:<6} delivered={del:<6} \
         dups={dups:<6} timeouts={to:<6} stale={stale:<6} confused={conf:<6} \
         completed={comp:<6} gave_up={gu:<6} end_checks={ec:<6} p1_fails={p1:<4} p2_fails={p2:<4} time={dur:?}",
        v = p.variant.name(), k = p.messages, r = p.retries,
        l = b.l, u = b.u, sd = b.sd, w = b.w, dr = b.proc_delay, rt = b.round_trip(),
        execs = stats.execs, block = stats.block, del = c.delivered, dups = c.duplicates,
        to = c.timeouts, stale = c.stale, conf = c.confused, comp = c.completed,
        gu = c.gave_up, ec = c.end_checks, p1 = c.p1_fails, p2 = c.p2_fails, dur = dur,
    );
}

fn print_compare(p: Params, baseline: &(Stats, Duration), timed: &(Stats, Duration), bc: Counts, tc: Counts) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    let b = p.b;
    println!();
    println!("Alternating bit / PAR: MUST vs MUST-timed");
    println!("======================================================");
    println!(
        "variant = {}    K = {}    R = {}    L = {}    U = {}    sd = {}    W = {}    dR = {}",
        p.variant.name(), p.messages, p.retries, b.l, b.u, b.sd, b.w, b.proc_delay
    );
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
    println!();
    // Executions EXPLORED is execs + block: a violating execution is aborted
    // by its assertion and an evicted one blocks, so both are filed under
    // `block`. Dividing complete executions alone overstates the reduction
    // (sensor network defaults: 7702x that way, 215x explored).
    let b_explored = b_stats.execs + b_stats.block;
    let t_explored = t_stats.execs + t_stats.block;
    let exec_ratio = b_explored as f64 / t_explored.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "explored reduction: {exec_ratio:.2}x  ({b_explored} vs {t_explored} executions explored, execs+blocked)"
    );
    println!("time  speedup  : {time_ratio:.2}x");
    println!(
        "delivered/dups/timeouts/stale/confused: baseline {}/{}/{}/{}/{}   timed {}/{}/{}/{}/{}",
        bc.delivered, bc.duplicates, bc.timeouts, bc.stale, bc.confused,
        tc.delivered, tc.duplicates, tc.timeouts, tc.stale, tc.confused
    );
    println!(
        "completed/gave_up/end_checks/p1_fails/p2_fails: baseline {}/{}/{}/{}/{}   timed {}/{}/{}/{}/{}",
        bc.completed, bc.gave_up, bc.end_checks, bc.p1_fails, bc.p2_fails,
        tc.completed, tc.gave_up, tc.end_checks, tc.p1_fails, tc.p2_fails
    );
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    variant: Variant,
    messages: u32,
    retries: u32,
    u: u64,
    l_ratio: f64,
    sd_ratio: f64,
    w_ratio: f64,
    proc_ratio: f64,
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
    let mut a = Args {
        mode: String::from("compare"),
        variant: Variant::Abp,
        messages: DEFAULT_MESSAGES,
        retries: DEFAULT_RETRIES,
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        w_ratio: DEFAULT_W_RATIO,
        proc_ratio: DEFAULT_PROC_RATIO,
        keep_going: false,
        parallel: String::from("none"),
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--variant" => {
                let v = next_val(&mut args, "--variant");
                a.variant = match v.as_str() {
                    "abp" => Variant::Abp,
                    "par" => Variant::Par,
                    other => cli_bail(&format!("invalid --variant: {other} (expected abp|par)")),
                };
            }
            "--messages" => a.messages = parse_num(next_val(&mut args, "--messages"), "--messages"),
            "--retries" => a.retries = parse_num(next_val(&mut args, "--retries"), "--retries"),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--w-ratio" => a.w_ratio = parse_num(next_val(&mut args, "--w-ratio"), "--w-ratio"),
            "--proc-ratio" => {
                a.proc_ratio = parse_num(next_val(&mut args, "--proc-ratio"), "--proc-ratio")
            }
            "--keep-going" => a.keep_going = true,
            "--parallel" => a.parallel = next_val(&mut args, "--parallel"),
            "--help" | "-h" => {
                eprintln!(
                    "Usage: alternating_bit_timed [--mode baseline|timed|compare] [--variant abp|par] \
                     [--messages K] [--retries R] [--u U] [--l-ratio LR] \
                     [--sd-ratio SR] [--w-ratio WR] [--proc-ratio PR] [--keep-going] \
                     [--parallel none|shared|partitioned]\n\
                     Defaults: mode=compare, variant=abp, K=2, R=1, U=1, L/U=0, sd/U=0, \
                     W/U=3, dR/U=0.\n\
                     abp: acks repeat the frame bit; par: acks carry no bit (the Bosnacki-Dams \
                     premature-timeout bug). par with R >= 1 FIREs in baseline, so compare aborts \
                     in its baseline leg unless --keep-going; pass --mode timed for par.\n\
                     --keep-going explores past a violation and reports p1_fails=/p2_fails= (and a \
                     VIOLATION line) instead of aborting; the exit code is then 0 either way.\n\
                     Exit codes: 0 = hold, 101 = FIRE (wrong or missing delivery), 2 = CLI misuse."
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
    if a.messages < 1 {
        cli_bail("need at least 1 message (--messages)");
    }
    if a.messages > 1_000 {
        cli_bail("--messages above 1000 is not a sensible bounded model");
    }
    let b = Bounds::from_ratios(a.u, a.l_ratio, a.sd_ratio, a.w_ratio, a.proc_ratio);
    let p = Params {
        variant: a.variant,
        messages: a.messages,
        retries: a.retries,
        b,
    };
    match a.mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, p, a.keep_going);
            let c = read_counts();
            print_one("baseline", p, &s, d, c);
            warn_if_vacuous("baseline", Mode::Baseline, p, s.execs, s.block, c);
            warn_if_violated("baseline", c);
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, p, a.keep_going);
            let c = read_counts();
            print_one("timed", p, &s, d, c);
            warn_if_vacuous("timed", Mode::Timed, p, s.execs, s.block, c);
            warn_if_violated("timed", c);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, p, a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, p, a.keep_going);
            let tc = read_counts();
            print_compare(p, &baseline, &timed, bc, tc);
            warn_if_vacuous("baseline", Mode::Baseline, p, baseline.0.execs, baseline.0.block, bc);
            warn_if_vacuous("timed", Mode::Timed, p, timed.0.execs, timed.0.block, tc);
            warn_if_violated("baseline", bc);
            warn_if_violated("timed", tc);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
}
