//! Bounded Retransmission Protocol, as modelled and verified by D'Argenio,
//! Katoen, Ruys and Tretmans, "The Bounded Retransmission Protocol must be on
//! time!" (TACAS 1997, LNCS 1217; full version CTIT TR 97-03): the timed
//! automata of Sec. 3.2 (Fig. 1 sender S, Fig. 2 receiver R) in the UPPAAL
//! form of Sec. 4.1 (Fig. 4), and the two timing properties of Sec. 4.2:
//!   (2)  A[] (R.first_safe_frame => rb1 = 1): the receiver never takes a
//!        non-first frame for the first frame of a new file;
//!   (4)  A[] ((S.error and x = SYNC) => R.new_file): when the sender ends its
//!        post-failure wait, the receiver has already given the file up.
//! DKRT state that both hold if (constraint (5), as printed, non-strict)
//!   T1 > 2 TD   and   SYNC >= TR >= 2 MAX T1 + 3 TD.
//! They found it with UPPAAL runs at chosen values (TD = 1, T1 = 3) and argue
//! the TR side tight with the trace of their Fig. 7.
//!
//! THE PROTOCOL (Fig. 4, transliterated). S sends each chunk of a file as a
//! frame (first, last, alternating bit) and waits for an ack while x < T1; at
//! x = T1 it resends (at most MAX times) or gives up (I_NOK, or I_DK on the
//! last chunk), waits SYNC and restarts the alternating bit. R reports a frame
//! whose bit is the expected one (I_FST / I_INC / I_OK) and acks every frame;
//! if no NEW frame comes for TR (a duplicate does not reset z) it gives up
//! (I_NOK unless the last frame was the last chunk) and waits for a new file,
//! whose first frame sets the expected bit. Channels K and L lose any message
//! and deliver the rest with delay in (0, TD]; acks carry no bit.
//!
//! ENCODING.
//!   * Time scale k: every DKRT constant is multiplied by k and the transit
//!     (0, TD] becomes [1, k*TD], i.e. resolution 1/k. At k = 1 this is DKRT's
//!     automaton in integer time; at k = 2 (minimal transit 1/2 standing in
//!     for u > 0) it gave DKRT's dense verdict on every (2) cell tested. Both
//!     are checked against IMITATOR (see DEVIATIONS and RESULTS).
//!   * Strict guards. x < T1: S waits Finite(k*T1 - 1) for an ack, then
//!     sleep(1) is the x = T1 transition (a timeout taken when the ack arrives
//!     exactly at the deadline is that ack lost on L, which DKRT allow).
//!     z < TR: R arms its timer at k*TR - 1 and, on the live one, sleep(1) is
//!     the z = TR transition; a frame arriving at z = TR is read after it, in
//!     new_file.
//!   * Loss: every send may be dropped, with no budget, in both modes, since
//!     DKRT's lines lose without bound; at sd = 0 a frame or ack that expires
//!     unread (it arrived while its receiver was busy) is another loss, also
//!     DKRT-legal.
//!   * Receive discipline (timed mode, ruling of 2026-10-13): a receive that
//!     reads message s reads the one that ARRIVED FIRST among the matching
//!     messages pending at it; every other pending matching message arrives
//!     no earlier than s (a tie leaves both orders explored) or had expired
//!     when the wait began (arrival + sd before the instant the wait began). A
//!     waiting S or R never passes over an earlier arrival, whoever sent it; a
//!     message expires unread only while its receiver is busy (sleeping, or
//!     handling an earlier message); a timeout is possible only when nothing
//!     readable arrived in the wait (an arrival exactly at the deadline may
//!     lose the race: both worlds exist). The baseline mode has no clock and
//!     is not affected.
//!   * The sender client offers the next file after any wait in [0, DC],
//!     DC >= TR (DKRT's client has no deadline; an eager client with a
//!     bounded number of files would report false holds of (2)).
//!   * Properties are ghost observations asserted where they fail: (2) a
//!     frame with rb1 = 0 taken in new_file (asserted at that frame); (4) a
//!     zero-transit Probe from S at x = SYNC that finds R in idle, including
//!     the tie with R's own z = TR instant (a Finite(0) probe read after the
//!     sleep; asserted at the probe); client: the one-file analogue of DKRT
//!     Table 2 (1) and (2) on their Sec. 3.2 channels, the run ends with one
//!     client in ok and the other in nok (see client_violation; refused for
//!     more than one file). The client property relates both processes: S
//!     sends R a zero-transit SoutOk when SC enters ok (as the Probe does),
//!     and R asserts on its own state when RC is in nok at that instant or at
//!     its next z = TR. main also judges the observations of the runs in
//!     which both processes end; a `join` is no message (see BOUND).
//!
//! DEVIATIONS FROM FIG. 4 (each harmless when T1 > 2 TD, DKRT's premise):
//!   * Channels are TraceForge's unbounded lossy FIFO queues (DKRT Sec. 3.2),
//!     not Fig. 4's one-place K and L. With T1 > 2 TD at most one frame and
//!     one ack are ever in transit (DKRT Sec. 4.2 (1); IMITATOR confirms their
//!     BAD states are unreachable iff T1 > 2 TD), and frame contents fixed at
//!     send equal Fig. 4's read-at-delivery because S changes b1/bN/ab only
//!     after an ack, when K is empty. Below T1 <= 2 TD the two differ: Fig. 4
//!     deadlocks, this port shows the client-level bug reported below.
//!   * UPPAAL's committed and urgent locations are zero-time handler code.
//!   * No Fig. 8 client automata: the clients are ghost indications.
//!   * Bounded files F and client wait DC. IMITATOR shows two files with a
//!     free client (DC >= TR) give the unbounded-files regions for (2), and
//!     one file for (4); an eager client would report false holds.
//!   * The integer grid of step 1/k: k = 1 is DKRT's automaton in integer
//!     time, which misses the dense violations on the line SYNC = TR + TD -
//!     T1 - 1 (IMITATOR's integer model predicts exactly this); k = 2 gave the
//!     dense verdict on every (2) cell tested, and IMITATOR's half-grid and
//!     dense regions coincide at every integer point checked (n = 2, MAX <= 3),
//!     but that is established empirically, not proven.
//!
//! NO HARNESS FILTER (since 2026-10-12). Every send may be lost, and a message
//! that reaches a busy receiver expires unread at sd = 0: for frames and acks
//! that is DKRT's lossy line, but loss holds for R's self-sent timer and for
//! S's Probe as well (the Probe can also expire while R sleeps). Until
//! 2026-10-12 R drained every armed timer and `assume!`d every Probe read
//! before the judgement, so an execution with a lost or expired timer or Probe
//! was discarded. Under the model's semantics those are executions like any
//! other (a lost timer is a receiver without a clock, and (4) then fails at
//! S's next probe); they are now explored and judged. Under the receive
//! discipline above R's wait cannot pass over its Timer for a later message
//! (a frame, the Probe), and R is never busy when its Timer arrives (it
//! sleeps only right after reading the live one), so R's clock fails only when
//! the Timer send itself is lost.
//!
//! BOUND (since 2026-10-14; no Done, no Terminate). The model is bounded by
//! rounds: S runs F files of n chunks with at most MAX resends per chunk, and
//! R serves the same F files: it ends at the timeout z = TR that follows the
//! last chunk (bN) of the last file (file F - 1). A run that loses and
//! expires nothing ends every thread (each frame is read, so is each Timer it
//! arms, and the live one is that timeout); a run that loses a frame R needs,
//! or its last Timer, leaves R at its receive: blocked, a message was lost (in
//! the baseline mode a timeout taken while an ack was readable is that ack
//! lost on L, so S giving up before the last chunk is such a run).
//! No message stops R and none carries a verdict: (2), (4) and the client
//! property are asserted at R where they fail, main only joins. R's state
//! after that timeout is the same whether a copy of the last chunk is still
//! to come or not, so the bound does not read such a copy, nor a Probe that
//! arrives after it. In timed mode no copy can arrive after it when
//! TR >= MAX T1 + TD (a NOTE is printed below it) and a Probe that does finds
//! R in new_file, so (2) and (4) are exact there (only probe_nontrivial_runs
//! reads fewer runs). The baseline mode has no clock: R's Timer can fire
//! before a copy still in transit and that copy is not read (62% to 82% of
//! the (2) executions of the p2 cells tried; the violation is still found
//! from the earlier timeouts).
//!
//! MODELS. `--model dkrt` is the above. `--model spin` is DKRT's App. C
//! untimed Promela model: lines Line_K / Line_L deliver or tell S of a loss
//! through ChunkTimeout (so S times out only after a real loss: assumption
//! (A1)), and a double SyncWait handshake replaces R's timer and S's SYNC wait
//! (assumption (A2)). It has no time parameters at all. DKRT searched this
//! model with Spin's bitstate hashing (about 98% coverage) and verified an
//! optimized equivalent (their C.7) exhaustively. Here n is fixed per run,
//! while their environment draws n per file.
//!
//! RESULTS. Re-measured 2026-10-13, after the first-arrival ruling (see
//! "Receive discipline" under ENCODING), on the file before BOUND (it had
//! Done and Terminate, so its counts include the loss branches of those two
//! sends, and the tables below are not re-measured except where a bound
//! column says so): one
//! worker, counts with --keep-going, a certified witness from the same cell
//! run without it (exit 101; the search is deterministic, the same witness on
//! 30 repeats at TR 8, SYNC 8 to 11 and the T1 = 2 TD cell). The IMITATOR
//! oracle (oracle/*.res), the drivers (brp_xval.py; BRP_BIN selects the
//! binary) and a README are in
//! docs/research/ta_problems/need-from-user-papers/brp-b04; the *_v2* logs and
//! final_chain.meta there are the 2026-09-19 build, not the numbers below.
//!
//! The 11 cells of the 2026-10-12 harness removal (MAX 1, TD 1, T1 3, k 1, one
//! file), verdict and violating runs. old = HEAD 0cbe2c7 (timer drain, Probe
//! `assume!`, earlier receive rule); 10-12 = this file with the earlier
//! receive rule; now = the file before BOUND; bound = this file (2026-10-14):
//!                                     old            10-12          now            bound
//!   (2) TR 8,  SYNC 13                VIOLATED 12    VIOLATED 8     VIOLATED 4     VIOLATED 2
//!   (2) TR 9,  SYNC 14                HOLDS          HOLDS          HOLDS          HOLDS
//!   (2) TR 10, SYNC 15                HOLDS          HOLDS          HOLDS          HOLDS
//!   (4) TR 11, SYNC 8                 VIOLATED 10    VIOLATED 464   VIOLATED 188   VIOLATED 94
//!   (4) TR 11, SYNC 9                 VIOLATED 10    VIOLATED 500   VIOLATED 188   VIOLATED 94
//!   (4) TR 11, SYNC 10                HOLDS          VIOLATED 464   VIOLATED 152   VIOLATED 76
//!   (4) TR 11, SYNC 11                HOLDS          VIOLATED 464   VIOLATED 152   VIOLATED 76
//!   client, T1 3, TR = SYNC = 11      HOLDS          HOLDS          HOLDS          HOLDS
//!   client, T1 2, TR = SYNC = 7       VIOLATED 1     VIOLATED 1     VIOLATED 1     VIOLATED 1
//!   baseline, TR 9, SYNC 14, p2       VIOLATED 528   VIOLATED 186   VIOLATED 186   VIOLATED 35
//!   --model spin, 50 executions       HOLDS          HOLDS          HOLDS          HOLDS
//! now to bound: the violating runs of (2) and (4) are half (the loss branch
//! of Done repeated each of them: R parked at its assert, S still sent it),
//! the client ones and the spin model's executions are the same, and the
//! baseline's fall to 35 for the reason BOUND gives (116 of its 186 came
//! after the bound, (186 - 116) / 2).
//! Counts, old to now: the executions explored (completed + blocked) fall 2.8x
//! to 3.2x in the timed cells (blocked 3.3x to 3.8x: no drains, no summaries,
//! no passed-over arrivals), completed ones rise 1.2x to 2.4x, user CPU falls
//! 1.7x to 2.2x (baseline 2.6x, spin 1.2x). The ruling alone (10-12 to now)
//! cuts the explored executions 1.5x to 1.7x and the violating runs 2.0x (TR 8)
//! and 2.5x to 3.1x ((4)); the baseline mode and the spin model are not
//! affected.
//!
//! Witnesses (scaled time, k = 1):
//!   (2) TR 8 (2 MAX T1 + 3 TD = 9): R reads frame 1 at 1, acks, and arms its
//!       Timer (due 8). R's first ack arrives at S's deadline 2 and loses the
//!       race (an ack lost on L); S resends at 3 and reads R's second ack at
//!       its deadline 5; its first copy of chunk 2 (sent at 5) is [dropped],
//!       its second (sent at 8) arrives at 9, the instant R, which read the
//!       Timer at 8, reaches z = TR: R has given up (I_NOK) and takes the
//!       frame in new_file with b1 = 0. That is DKRT's Fig. 7. Before the
//!       ruling the first copy needed no loss: it arrived at 6 and R's wait
//!       passed over it for the later Timer (8). Loss does not move (2): it
//!       needs R to give up and then take a late frame, and a lost timer
//!       removes the giving up.
//!   (4) at SYNC 10, 11: 152 violating runs, the same at SYNC 12, 15 and 20.
//!       There SYNC + T1 > TR + TD (the oracle's hold side, see Oracle below):
//!       a live Timer fires before the Probe (z = TR comes strictly before it,
//!       and R's wait must read the Timer first), so every violating run has
//!       R's Timer lost. Witness at SYNC 11: R reads frame 1 at 1, acks, and
//!       arms its Timer (due 11), whose send is [dropped]; R's acks, sent at
//!       1 and 4, arrive at S's deadlines 2 and 5 and lose the race; S gives
//!       up at 6 and probes at 17; R, with no clock, reads the duplicate at 4
//!       and the Probe at 17, still idle. The same witness at SYNC 8, 9, 10
//!       with the probe at 14, 15, 16. Before the ruling R's clock could also
//!       fail without a loss: the Timer arrived at 11 and expired unread while
//!       R's wait took the later Probe (464 runs then). SYNC 8 and 9 have 36
//!       runs more: they include DKRT's own violation (R's Timer live, the
//!       Probe not later than R's z = TR). A receiver whose clock can fail
//!       satisfies (4) at no SYNC; IMITATOR's region assumes a clock that
//!       cannot, so this port claims no region for (4).
//!   client, T1 = 2 TD (TR = SYNC = 7): R reads frame 1 at 1 and acks; S
//!       times out at 1, resends at 2 and reads the first ack, which arrived
//!       at 2 = x = T1, as the ack of the copy it just sent; it goes on to
//!       chunk 2 (both copies [dropped]) and reads R's second ack (arrives 4)
//!       as the ack of chunk 2: SC ends I_OK at 4. R never gets chunk 2, its
//!       Timer (due 7) fires and RC ends I_NOK. The same witness before the
//!       ruling.
//!
//! Oracle: IMITATOR 3.4.0 on a transliteration of Fig. 4 (it reproduces the
//! IMITATOR library's published BRPAAPP21 region when its channel is relaxed
//! to [0, TD]; it marks DKRT's committed locations urgent, which only adds
//! interleavings, so a HOLD carries over to DKRT's model). Exact regions,
//! dense time, n = 2, MAX in 1..3, T1 > 2 TD assumed (not tested):
//!   (2) holds iff TR >= 2 MAX T1 + 3 TD  and  SYNC + T1 >= TR + TD
//!   (4) holds iff SYNC + T1 > TR + TD
//! (At n = 1, (2) holds for every TR: every frame has rb1 = 1.) So SYNC >= TR
//! is sound but not needed: with the TR bound, (2) and (4) both hold iff
//! TR - SYNC < T1 - TD. What is already published, and what is not:
//!   * The SYNC premise missing from (2) ("holds whenever TR >= 2 MAX T1 +
//!     3 TD" in both DKRT versions): Hune, Romijn, Stoelinga, Vaandrager
//!     (report CSI-R0102, Jan 2001, p. 30; JLAP 52-53, 2002, Sec. 5.3) derived
//!     "TR - 2 <= SYNC" at fixed values (our conjunct if they used DKRT's TD 1,
//!     T1 3) and report that DKRT acknowledged the error. Andre, Arias,
//!     Petrucci, van de Pol (TACAS 2021, p. 324) give the exact symbolic
//!     "SYNC + TS >= TR + TD" (MAX 2, confirmed to MAX 20, on a [0, TD]
//!     channel, so their TR bound is strict) and call DKRT's constraint
//!     "strictly stronger". Ours only confirms it, on DKRT's (0, TD] channel.
//!   * In no source we found: any region for (4) (Hune et al. could not check
//!     it, AAPP21 did not analyse it, TReX assumed Synch > Tr), and so the
//!     refutation of TACAS'97 p. 10: "(A2) is only fulfilled if this condition
//!     on the values SYNC and TR is respected" and "(A1) and (A2) are
//!     fulfilled only if the following constraints hold" (TACAS (4) = the full
//!     version's (5)). The full version says "if" but still calls the
//!     conditions "tight".
//! Witness at DKRT's own setting MAX 2, TD 1, T1 3, TR 15: SYNC = 14 satisfies
//! (2) and (4); SYNC = 13 violates (4) at the tie SYNC + T1 = TR + TD. The
//! regions are IMITATOR's computation; the sources are listed in the README.
//!
//! 1. Cross-validation, both sides of every edge (TR edge and (4) with one
//!    file at MAX 1..3, SYNC edge with two files at MAX 1; TD 1..2, two T1
//!    each, k = 1 and 2): 200 runs against the IMITATOR oracle of the same
//!    time semantics (k = 1 integer, k = 2 half grid). (2), the 72 TR-edge and
//!    32 SYNC-edge runs: 104/104 agree. Against the dense model the only
//!    disagreements are the 4 predicted k = 1 cells at SYNC = TR + TD - T1 - 1
//!    (integer time holds; dense time violates, the new file's first frame
//!    needing a transit below one time unit). (4), 96 runs: VIOLATED in all;
//!    the 48 on the oracle's violation side (SYNC + T1 <= TR + TD) agree, the
//!    48 on its hold side do not, because R's Timer can be lost (NO HARNESS
//!    FILTER). DKRT's own cell MAX 2, TD 1, T1 3, TR 15 at k = 2: (4)
//!    VIOLATED at SYNC = 13, 14 and 15 (266, 236 and 236 runs), so the port
//!    no longer reproduces by execution the refutation of TACAS'97's claim
//!    that (A2) holds only if SYNC >= TR; that refutation rests on IMITATOR's
//!    region. (2) at the same cell with two files and k = 2 (SYNC = 12, 13,
//!    14) was not re-run: about 12M executions each in the 2026-09-19 build.
//! 2. The two mechanisms (classified witnesses; the counters p2_tr_side and
//!    p2_sync_side split every violation, TR-edge cells show only the first,
//!    SYNC-edge cells only the second): TR side = DKRT's Fig. 7 (a premature
//!    receiver timeout, then a late copy of chunk 2 taken as a first frame:
//!    the TR 8 witness above); SYNC side = a new file started while R still
//!    holds the old one: its first frame is taken as a duplicate, R times out,
//!    chunk 2 enters first_safe_frame and RC ends with I_OK for a file whose
//!    first chunk it never got. Witness (MAX 1, TD 1, T1 3, TR 11, SYNC 7,
//!    k 1, two files): file 0's first frame is [dropped], its copy (sent at 3)
//!    arrives at 4 and R arms its Timer (due 14); R's ack, arriving at S's
//!    deadline 5, loses the race, S gives up at 6 and offers file 1 at
//!    6 + SYNC = 13; its first frame arrives at 14, the instant of R's Timer,
//!    and R reads it first: still idle and the bit is not the expected one, so
//!    a duplicate (acked only); then the Timer, R gives up at 15 and takes
//!    chunk 2 of file 1 (arrives 16) in new_file with b1 = 0.
//! 3. Outside (5), at T1 = 2 TD, TR = 2 MAX T1 + 3 TD, SYNC = TR, with the
//!    unbounded lossy channels of DKRT Sec. 3.2: SC ends with I_OK and RC with
//!    I_NOK in the certified witness of all 8 cells tested (MAX 1..2, TD 1..2,
//!    k 1..2), and every cell at T1 = 2 TD + 1 holds. An ack arriving exactly
//!    at x = T1 is consumed after the retransmission and taken for the next
//!    chunk (acks carry no bit). DKRT's one-place Fig. 4 is not deadlock-free
//!    there (IMITATOR).
//! 4. Precision: this program run untimed (--mode baseline) reports
//!    violations of (2), (4) and the client property at MAX 1 (5 settings of
//!    the time constants) and of (2) and the client property at MAX 2 (3
//!    settings); the counts are identical across the settings, they do not
//!    depend on the time constants. DKRT's App. C model (--model spin) has no
//!    time parameters and satisfies (2) in every (n, MAX, F) run and the
//!    client property in the F = 1 runs, in 50 / 226 / 1,250 / 25,538
//!    executions for (2,1,1) / (2,2,1) / (2,1,2) / (2,2,2) (158 / 12,482 /
//!    31,250 for (3,1,1) / (3,1,2) / (2,1,3)): twice the hand count of App.
//!    C's choice tree (25 / 113 / 625 / 12,769), the factor 2 being the last
//!    Stop, which Line_L forwards to a sender that has ended (a lossy send
//!    that no one reads: delivered or lost). Its tricks build in the
//!    assumptions whose region the timed model computes.
//! 5. Efficiency, same program, single worker, median of 3 (the counts are
//!    identical across repeats; times are user CPU seconds on a shared
//!    machine; cex = violating runs, counted where they fail; blocked runs are
//!    executions in which a thread waits for ever for a lost message: an Init,
//!    Done, R's Timer, the monitor's Terminate). Explored = completed + blocked:
//!      cell (DKRT's safe settings)            untimed                     timed
//!      (2)  MAX 2 TD 1 T1 3 TR=SYNC=15 F=1 k=1  86,489 (7,416 cex) 9.6 s    6,026 proof 7.6 s
//!      (4)  same                                325,522 (91,060 cex) 38.7 s 10,118 VIOLATED 15.3 s
//!      client, same                             195,455 (144 cex) 23.4 s     6,026 proof 7.6 s
//!    14x to 32x fewer executions and 1.3x to 3.1x faster, with a proof where
//!    untimed reports false alarms ((2) and the client property; (4) is
//!    VIOLATED in both modes, in the timed one by a lost Timer: 1,622 runs).
//!    The two-file cell of the 2026-09-19 table (MAX 1, TD 1, T1 3, TR 9,
//!    SYNC 7, F = 2) was not re-measured. IMITATOR synthesizes each whole
//!    parametric region in 0.3 to 9 s: no speed claim against timed-automata
//!    tools.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, ConsType, Stats, WaitTime};

// =====================================================================
// Messages
// =====================================================================

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// Bootstrap from main, SPIN MODEL ONLY (untimed, transparent for
    /// timing). The DKRT model has none: R is spawned first and S last, so S
    /// captures R's id, and R learns S's from the `from` of the frames.
    Init { peer: ThreadId },
    /// Channel K, S -> R: F!(b1, bN, ab). `file` and `chunk` are ghost data
    /// for the observations; no protocol decision reads them (R's bound reads
    /// `file`, see BOUND). `from` is S's own address, the reply address of the
    /// ack (as in 3PC's Prepare).
    Frame { b1: bool, bn: bool, ab: bool, file: u32, chunk: u32, from: ThreadId },
    /// Channel L, R -> S. DKRT's ack carries no bit; `file` and `chunk` are
    /// ghost data (which frame R answered), read only by the observations.
    Ack { file: u32, chunk: u32 },
    /// S's own client (SC, Fig. 8): the next file is offered (Sin).
    Sin,
    /// R's timer z, armed at every z := 0, read one time unit before z = TR
    /// (see "Strict guards" under ENCODING in the header). Stale epochs are ignored.
    Timer { epoch: u32 },
    /// Property (4) monitor: S at x = SYNC in state error, zero transit.
    /// `file` (ghost) is the file S just gave up.
    Probe { file: u32 },
    /// Property client: S -> R at the instant SC enters ok, zero transit.
    /// Sent only when the client property is judged (see `client_check`).
    SoutOk,
    /// DKRT App. C (Promela) model only: loss notification from a line to S,
    /// the SyncWait double handshake, and the end of the run.
    ChunkTimeout,
    SyncWait,
    Stop,
}

const PROBE_TAG: u32 = 1;

/// The sender client's indication (Sout) and the receiver client's (Rout).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sout {
    Ok,
    Nok,
    Dk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rout {
    Fst,
    Inc,
    Ok,
    Nok,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct SenderObs {
    files: u32,
    ok: u32,
    nok: u32,
    dk: u32,
    retransmissions: u32,
    /// The last indication the sending client got.
    last_sout: Option<Sout>,
    /// A file was started after a failed one (the (2) SYNC side needs it).
    restart_after_fail: bool,
    /// Acks accepted that answer another frame than the one being sent.
    confused_acks: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct ReceiverObs {
    /// Property (2) and (4) violations (monitors, not protocol state), and
    /// the kind of (2) violation: a frame of the file R had just given up
    /// (TR side, a premature timeout) or of a later file (SYNC side).
    p2: bool,
    p2_tr_side: bool,
    p2_sync_side: bool,
    p4: bool,
    /// The client property was found to fail at R (S's I_OK with RC in nok).
    client: bool,
    /// The last indication the receiving client got.
    last_rout: Option<Rout>,
    frames: u32,
    duplicates: u32,
    new_files: u32,
    timeouts: u32,
    /// A first frame taken in new_file after a timeout: the only place
    /// where (2) is tested non-trivially.
    first_safe_after_timeout: u32,
    /// Probes that arrived when R had seen a frame of the failed file.
    probes_nontrivial: u32,
    fst: u32,
    inc: u32,
    ok: u32,
    nok: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Model {
    /// DKRT's timed automata (Fig. 1, 2, 4).
    Dkrt,
    /// DKRT's untimed Promela model (App. C): timers replaced by "tricks"
    /// that build in their assumptions (A1) and (A2).
    Spin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Property {
    P2,
    P4,
    /// DKRT Table 2 (1) and (2): for the same file the two clients never get
    /// I_OK on one side and I_NOK on the other.
    Client,
    /// (2), (4) and the client properties together.
    All,
}

impl Property {
    fn checks_p2(self) -> bool {
        matches!(self, Property::P2 | Property::All)
    }
    fn checks_p4(self) -> bool {
        matches!(self, Property::P4 | Property::All)
    }
    /// The client property is judged on final states, exact for one file.
    fn checks_client(self, files: u32) -> bool {
        matches!(self, Property::Client) || (matches!(self, Property::All) && files == 1)
    }
}

/// DKRT Table 2 (1) and (2), with one file: the run ends with SC in ok and
/// RC in nok, or SC in nok and RC in ok. With one file this final-state check
/// is exact for DKRT's state properties A[] (File.same => not(SC.ok and
/// RC.nok)) and its mirror: once S has its I_OK, R has received the last
/// chunk, so every later frame has bN = 1 and no later I_NOK can come, and a
/// report after an I_NOK moves RC out of nok (checked against DKRT's RClient
/// automaton during review). With more files it is not, so it is refused.
fn client_violation(so: &SenderObs, ro: &ReceiverObs) -> bool {
    matches!(
        (so.last_sout, ro.last_rout),
        (Some(Sout::Ok), Some(Rout::Nok)) | (Some(Sout::Nok), Some(Rout::Ok))
    )
}

/// DKRT's constants, and the harness bounds. Every time constant is used
/// multiplied by `k` (the time scale; see ENCODING in the header).
#[derive(Clone, Copy, Debug)]
struct Params {
    model: Model,
    /// Chunks per file (n) and the retransmission bound (MAX).
    n: u32,
    max: u32,
    /// DKRT constants in their own time unit.
    td: u64,
    t1: u64,
    tr: u64,
    sync: u64,
    /// Time scale: every constant times k, transit [1, k*TD] for (0, TD].
    k: u64,
    /// Files the sender client offers (F), and the client's longest wait
    /// before offering the next file (DC, in DKRT units).
    files: u32,
    dc: u64,
    property: Property,
}

impl Params {
    fn td(&self) -> u64 {
        self.k * self.td
    }
    /// Channel transit (0, TD] of K and L at resolution 1/k: [1, k*TD].
    fn lo(&self) -> u64 {
        1
    }
    fn t1(&self) -> u64 {
        self.k * self.t1
    }
    fn tr(&self) -> u64 {
        self.k * self.tr
    }
    fn sync(&self) -> u64 {
        self.k * self.sync
    }
    fn dc(&self) -> u64 {
        self.k * self.dc
    }
}

// =====================================================================
// Counters. (2), (4) and the client property are counted by R at the step
// where they fail (once per run); the others by main's `judge`, over the runs
// in which both processes ended.
// =====================================================================

static JUDGED: AtomicUsize = AtomicUsize::new(0);
static P2_RUNS: AtomicUsize = AtomicUsize::new(0);
static P2_TR_RUNS: AtomicUsize = AtomicUsize::new(0);
static P2_SYNC_RUNS: AtomicUsize = AtomicUsize::new(0);
static P4_RUNS: AtomicUsize = AtomicUsize::new(0);
static CLIENT_RUNS: AtomicUsize = AtomicUsize::new(0);
static FAILED_FILE_RUNS: AtomicUsize = AtomicUsize::new(0);
static TIMEOUT_RUNS: AtomicUsize = AtomicUsize::new(0);
static RESTART_AFTER_FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);
static FIRST_SAFE_AFTER_TIMEOUT_RUNS: AtomicUsize = AtomicUsize::new(0);
static PROBE_NONTRIVIAL_RUNS: AtomicUsize = AtomicUsize::new(0);
static CONFUSED_RUNS: AtomicUsize = AtomicUsize::new(0);
/// Judged runs whose client assertion fails.
static ASSERT_FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

const COUNTERS: [&AtomicUsize; 13] = [
    &JUDGED,
    &P2_RUNS,
    &P2_TR_RUNS,
    &P2_SYNC_RUNS,
    &P4_RUNS,
    &CLIENT_RUNS,
    &FAILED_FILE_RUNS,
    &TIMEOUT_RUNS,
    &RESTART_AFTER_FAIL_RUNS,
    &FIRST_SAFE_AFTER_TIMEOUT_RUNS,
    &PROBE_NONTRIVIAL_RUNS,
    &CONFUSED_RUNS,
    &ASSERT_FAIL_RUNS,
];

fn reset_counts() {
    for c in COUNTERS {
        c.store(0, Ordering::Relaxed);
    }
}

fn count(flag: bool, c: &AtomicUsize) {
    if flag {
        c.fetch_add(1, Ordering::Relaxed);
    }
}

// =====================================================================
// Sender S (Fig. 1 / Fig. 4) with its client SC
// =====================================================================

/// Returns its observation: main reads it through `join` (no message).
fn sender(p: Params, receiver: ThreadId) -> SenderObs {
    let me = thread::current().id();
    let mut obs = SenderObs::default();
    // Fig. 1: ab := 0 initially.
    let mut ab = false;
    let mut failed_before = false;
    for file in 0..p.files {
        // idle: SC offers the next file (Sin) after any wait (Fig. 8's
        // send_req has no invariant); the first file at time 0.
        let wait = if file == 0 { 0 } else { p.dc() };
        traceforge::send_msg_timed(me, Msg::Sin, 0, wait);
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == me) {
            Msg::Sin => {}
            m => panic!("sender idle: unexpected {m:?}"),
        }
        obs.files += 1;
        obs.restart_after_fail |= failed_before;
        // Sin?: i := 1, b1 := 1, x := 0.
        let mut i = 1u32;
        let failed = loop {
            // next_frame (urgent): bN := (i == n), rc := 0, then F!.
            let (b1, bn) = (i == 1, i == p.n);
            let mut rc = 0u32;
            let frame = Msg::Frame { b1, bn, ab, file, chunk: i, from: me };
            traceforge::send_lossy_msg_timed(receiver, frame.clone(), p.lo(), p.td());
            // wait_ack (x <= T1): an ack is accepted while x < T1, i.e. at
            // most k*T1 - 1 scaled units after the (re)send; at x = T1 the
            // timeout. A timeout taken while an ack was readable is the ack
            // being lost on L, which DKRT allow without bound.
            let acked = loop {
                let got = traceforge::recv_tagged_msg_timed::<_, Msg>(
                    move |s, _| s == receiver,
                    WaitTime::Finite(p.t1() - 1),
                );
                match got {
                    Some(Msg::Ack { file: af, chunk: ac }) => {
                        if (af, ac) != (file, i) {
                            obs.confused_acks += 1;
                        }
                        break true;
                    }
                    Some(m) => panic!("sender: unexpected {m:?}"),
                    None => {
                        traceforge::sleep(1); // now x = T1
                        if rc < p.max {
                            // x == T1, rc < MAX: F!, x := 0, rc := rc + 1.
                            rc += 1;
                            obs.retransmissions += 1;
                            traceforge::send_lossy_msg_timed(receiver, frame.clone(), p.lo(), p.td());
                        } else {
                            break false;
                        }
                    }
                }
            };
            if acked {
                // x < T1, B?: x := 0, ab := 1 - ab, success (urgent).
                ab = !ab;
                if i < p.n {
                    i += 1; // b1 := 0
                    continue;
                }
                obs.ok += 1; // Sout I_OK
                obs.last_sout = Some(Sout::Ok);
                if p.property.checks_client(p.files) {
                    traceforge::send_msg_timed(receiver, Msg::SoutOk, 0, 0);
                }
                break false;
            }
            // x == T1, rc == MAX: I_DK if i == n else I_NOK, x := 0, error.
            if i == p.n {
                obs.dk += 1;
                obs.last_sout = Some(Sout::Dk);
            } else {
                obs.nok += 1;
                obs.last_sout = Some(Sout::Nok);
            }
            break true;
        };
        if failed {
            // error (x <= SYNC): at x == SYNC, ab := 0, idle. The Probe is
            // the property (4) monitor reading R's location at this instant.
            traceforge::sleep(p.sync());
            if p.property.checks_p4() {
                traceforge::send_tagged_msg_timed(receiver, PROBE_TAG, Msg::Probe { file }, 0, 0);
            }
            ab = false;
        }
        failed_before |= failed;
    }
    obs
}

// =====================================================================
// Receiver R (Fig. 2 / Fig. 4) with its client RC
// =====================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RLoc {
    NewFile,
    Idle,
}

/// R's protocol state (Fig. 4) plus the ghost observations.
struct RState {
    loc: RLoc,
    exp_ab: bool,
    /// rbN of the last frame K delivered (Fig. 4: assigned at every G!),
    /// and (ghost) the file and chunk that frame belongs to.
    rbn: bool,
    last_file: u32,
    last_chunk: u32,
    seen_any: bool,
    /// Ghost: the file of the last frame R reported (accepted as new).
    last_reported_file: Option<u32>,
    /// Which ghost properties are asserted where they fail (the others are
    /// only counted).
    property: Property,
    files: u32,
    /// S's I_OK has reached R (property client).
    sout_ok: bool,
    obs: ReceiverObs,
}

impl RState {
    fn new(property: Property, files: u32) -> Self {
        Self {
            loc: RLoc::NewFile,
            exp_ab: false,
            rbn: false,
            last_file: 0,
            last_chunk: 0,
            seen_any: false,
            last_reported_file: None,
            property,
            files,
            sout_ok: false,
            obs: ReceiverObs::default(),
        }
    }

    /// Property client, asserted where it fails: SC in ok (S's SoutOk has
    /// arrived) and RC in nok, checked at the SoutOk and at every z = TR. A
    /// lost or expired SoutOk can only hide a violation in its run, as a lost
    /// Probe does (its twin with the SoutOk read is explored).
    fn client_check(&mut self) {
        if self.sout_ok && self.obs.last_rout == Some(Rout::Nok) {
            count(!self.obs.client, &CLIENT_RUNS);
            self.obs.client = true;
            if self.property.checks_client(self.files) {
                traceforge::assert(false);
            }
        }
    }

    /// G?(rb1, rbN, rab) in new_file or idle. Returns (ack, report): whether
    /// R sends an ack, and whether it reported (then exp_ab flips, z := 0).
    fn frame(&mut self, b1: bool, bn: bool, ab: bool, file: u32, chunk: u32) -> (bool, bool) {
        self.obs.frames += 1;
        self.rbn = bn;
        self.last_file = file;
        self.last_chunk = chunk;
        self.seen_any = true;
        match self.loc {
            RLoc::NewFile => {
                // new_file, G?: z := 0; first_safe_frame: exp_ab := rab.
                self.obs.new_files += 1;
                if self.obs.timeouts > 0 {
                    self.obs.first_safe_after_timeout += 1;
                }
                if !b1 {
                    // Property (2) fails here and now. A frame of the file R
                    // last accepted: R gave it up too early (TR side). Of a
                    // later file: the new file started while R still held
                    // the old one (SYNC side).
                    let tr_side = self.last_reported_file == Some(file);
                    count(!self.obs.p2, &P2_RUNS);
                    count(tr_side && !self.obs.p2_tr_side, &P2_TR_RUNS);
                    count(!tr_side && !self.obs.p2_sync_side, &P2_SYNC_RUNS);
                    self.obs.p2 = true;
                    if tr_side {
                        self.obs.p2_tr_side = true;
                    } else {
                        self.obs.p2_sync_side = true;
                    }
                    if self.property.checks_p2() {
                        traceforge::assert(false);
                    }
                }
                self.exp_ab = ab;
                // frame_received: rab == exp_ab, so report.
                self.report(b1, bn);
                (true, true)
            }
            RLoc::Idle => {
                if ab == self.exp_ab {
                    self.report(b1, bn);
                    (true, true)
                } else {
                    // A repeated frame: A!, back to idle, z NOT reset.
                    self.obs.duplicates += 1;
                    (true, false)
                }
            }
        }
    }

    /// report + frame_reported: the Rout indication; the caller sends A!;
    /// exp_ab flips; idle.
    fn report(&mut self, b1: bool, bn: bool) {
        self.last_reported_file = Some(self.last_file);
        match (b1, bn) {
            (_, true) => {
                self.obs.ok += 1;
                self.obs.last_rout = Some(Rout::Ok);
            }
            (true, false) => {
                self.obs.fst += 1;
                self.obs.last_rout = Some(Rout::Fst);
            }
            (false, false) => {
                self.obs.inc += 1;
                self.obs.last_rout = Some(Rout::Inc);
            }
        }
        self.exp_ab = !self.exp_ab;
        self.loc = RLoc::Idle;
    }

    /// z == TR (or App. C's SyncWait): I_NOK iff rbN == 0, then new_file.
    fn give_up(&mut self) {
        self.obs.timeouts += 1;
        if !self.rbn {
            self.obs.nok += 1;
            self.obs.last_rout = Some(Rout::Nok);
        }
        self.loc = RLoc::NewFile;
        self.client_check();
    }

    fn probe(&mut self, file: u32) {
        if self.seen_any && self.last_file == file {
            self.obs.probes_nontrivial += 1;
        }
        if self.loc == RLoc::Idle {
            // Property (4) fails here and now: S ends its post-failure wait
            // while R still holds the file.
            count(!self.obs.p4, &P4_RUNS);
            self.obs.p4 = true;
            if self.property.checks_p4() {
                traceforge::assert(false);
            }
        }
    }

    fn ack(&self) -> Msg {
        Msg::Ack { file: self.last_file, chunk: self.last_chunk }
    }

    /// R's bound (see BOUND in the header): the last frame K delivered is
    /// the last chunk (bN) of the last of the `files` files.
    fn took_last_chunk(&self, files: u32) -> bool {
        self.rbn && self.last_file + 1 == files
    }
}

struct Receiver {
    me: ThreadId,
    /// S's address, learned from the first frame that arrives (R sends
    /// nothing and times nothing out before it).
    sender: Option<ThreadId>,
    p: Params,
    st: RState,
    /// The live timer epoch; stale timers are ignored.
    epoch: u32,
}

impl Receiver {
    /// z := 0: arm the timer for the next z = TR. A self-send, which may be
    /// lost like any other message (R then never times out in that run).
    fn reset_z(&mut self) {
        self.epoch += 1;
        traceforge::send_msg_timed(self.me, Msg::Timer { epoch: self.epoch }, self.p.tr() - 1, self.p.tr() - 1);
    }

    fn frame(&mut self, b1: bool, bn: bool, ab: bool, file: u32, chunk: u32, from: ThreadId) {
        self.sender = Some(from);
        let (ack, reported) = self.st.frame(b1, bn, ab, file, chunk);
        if ack {
            let sender = self.sender.expect("set by the frame just read");
            traceforge::send_lossy_msg_timed(sender, self.st.ack(), self.p.lo(), self.p.td());
        }
        if reported {
            self.reset_z();
        }
    }

    /// The live timer, read at z = TR - 1 (scaled): wait to z = TR, catch a
    /// Probe sent at exactly this instant (R is still idle), then time out.
    fn timeout(&mut self) {
        traceforge::sleep(1);
        if self.p.property.checks_p4() {
            let probe = traceforge::recv_tagged_msg_timed::<_, Msg>(
                move |_s, tag| tag == Some(PROBE_TAG),
                WaitTime::Finite(0),
            );
            match probe {
                Some(Msg::Probe { file }) => self.st.probe(file),
                Some(m) => panic!("receiver: unexpected {m:?}"),
                None => {}
            }
        }
        self.st.give_up();
    }
}

/// R serves the F files and ends at the timeout (z = TR) that follows the
/// last chunk of the last file (see BOUND in the header); in a run that loses
/// nothing every frame and every Timer it armed is read, so it ends there,
/// with no message to stop it. A run that loses a frame it needs (or its last
/// Timer) leaves R at this receive: that run is blocked. Returns its
/// observation, which main reads through `join`.
fn receiver(p: Params) -> ReceiverObs {
    let me = thread::current().id();
    let mut r = Receiver { me, sender: None, p, st: RState::new(p.property, p.files), epoch: 0 };
    loop {
        match traceforge::recv_msg_block_timed::<Msg>() {
            Msg::Frame { b1, bn, ab, file, chunk, from } => r.frame(b1, bn, ab, file, chunk, from),
            Msg::Timer { epoch } => {
                if epoch == r.epoch && r.st.loc == RLoc::Idle {
                    r.timeout();
                    if r.st.took_last_chunk(p.files) {
                        break;
                    }
                }
            }
            Msg::Probe { file } => r.st.probe(file),
            Msg::SoutOk => {
                r.st.sout_ok = true;
                r.st.client_check();
            }
            m => panic!("receiver: unexpected {m:?}"),
        }
    }
    r.st.obs
}

// =====================================================================
// DKRT App. C: the untimed Promela model (Sender, Receiver, Line_K, Line_L)
// =====================================================================
//
// No clocks. A line either delivers a message or tells the sender through
// ChunkTimeout that it was lost, so the sender times out only after a real
// loss (their assumption (A1)); after a failure, sender and receiver meet in
// a double SyncWait handshake before a new file starts (assumption (A2)).
// DKRT searched it with Spin (bitstate) and verified an optimized form exhaustively.

fn spin_sender(p: Params, main_tid: ThreadId) -> SenderObs {
    let (line_k, receiver) = match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == main_tid) {
            Msg::Init { peer: r } => (peer, r),
            m => panic!("spin sender: expected Init, got {m:?}"),
        },
        m => panic!("spin sender: expected Init, got {m:?}"),
    };
    let mut obs = SenderObs::default();
    let mut ab = false; // start: ab = 0
    let mut failed_before = false;
    for file in 0..p.files {
        obs.files += 1; // idle: Sin?REQ
        obs.restart_after_fail |= failed_before;
        let mut i = 1u32;
        let failed = loop {
            // next_frame: F!(i==1, i==n, ab, d[i]); rc = 0.
            let frame = Msg::Frame { b1: i == 1, bn: i == p.n, ab, file, chunk: i, from: thread::current().id() };
            traceforge::send_msg(line_k, frame.clone());
            let mut rc = 0u32;
            let acked = loop {
                // wait_ack: B?ACK or ChunkTimeout?SHAKE.
                match traceforge::recv_msg_block_timed::<Msg>() {
                    Msg::Ack { file: af, chunk: ac } => {
                        if (af, ac) != (file, i) {
                            obs.confused_acks += 1;
                        }
                        break true;
                    }
                    Msg::ChunkTimeout if rc < p.max => {
                        rc += 1;
                        obs.retransmissions += 1;
                        traceforge::send_msg(line_k, frame.clone());
                    }
                    Msg::ChunkTimeout => break false,
                    m => panic!("spin sender: unexpected {m:?}"),
                }
            };
            if acked {
                ab = !ab;
                if i < p.n {
                    i += 1;
                    continue;
                }
                obs.ok += 1;
                obs.last_sout = Some(Sout::Ok);
                break false;
            }
            if i == p.n {
                obs.dk += 1;
                obs.last_sout = Some(Sout::Dk);
            } else {
                obs.nok += 1;
                obs.last_sout = Some(Sout::Nok);
            }
            break true;
        };
        if failed {
            // error: SyncWait!SHAKE; SyncWait?SHAKE; ab = 0.
            traceforge::send_msg(receiver, Msg::SyncWait);
            match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == receiver) {
                Msg::SyncWait => {}
                m => panic!("spin sender: expected SyncWait, got {m:?}"),
            }
            ab = false;
        }
        failed_before |= failed;
    }
    traceforge::send_msg(line_k, Msg::Stop);
    obs
}

/// Line_K (S -> R) or Line_L (R -> S): deliver, or lose and tell S.
fn spin_line(main_tid: ThreadId) {
    let (dest, sender) = match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == main_tid) {
            Msg::Init { peer: s } => (peer, s),
            m => panic!("spin line: expected Init, got {m:?}"),
        },
        m => panic!("spin line: expected Init, got {m:?}"),
    };
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s != main_tid) {
            Msg::Stop => {
                traceforge::send_msg(dest, Msg::Stop);
                return;
            }
            m => {
                if traceforge::nondet() {
                    traceforge::send_msg(dest, m);
                } else {
                    traceforge::send_msg(sender, Msg::ChunkTimeout);
                }
            }
        }
    }
}

fn spin_receiver(p: Params, main_tid: ThreadId) -> ReceiverObs {
    let (line_l, sender) = match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == main_tid) {
            Msg::Init { peer: s } => (peer, s),
            m => panic!("spin receiver: expected Init, got {m:?}"),
        },
        m => panic!("spin receiver: expected Init, got {m:?}"),
    };
    let mut st = RState::new(p.property, p.files);
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s != main_tid) {
            Msg::Frame { b1, bn, ab, file, chunk, .. } => {
                let (ack, _) = st.frame(b1, bn, ab, file, chunk);
                if ack {
                    traceforge::send_msg(line_l, st.ack());
                }
            }
            Msg::SyncWait => {
                // new_file or idle: SyncWait?SHAKE; (idle: I_NOK iff !bN);
                // SyncWait!SHAKE; new_file.
                if st.loc == RLoc::Idle {
                    st.give_up();
                }
                traceforge::send_msg(sender, Msg::SyncWait);
            }
            Msg::Stop => {
                traceforge::send_msg(line_l, Msg::Stop);
                break;
            }
            m => panic!("spin receiver: unexpected {m:?}"),
        }
    }
    st.obs
}

// =====================================================================
// Harness: main spawns the processes and judges the client property
// =====================================================================

/// S and R return their observations when they end and main reads them
/// through `join`, which is no message. A run in which a process cannot end
/// (R waits for a frame or a Timer a run lost) is blocked at that join and is
/// not judged here; the client property is asserted at R as well (see
/// `client_check`), for it must also fail in those runs, where S ended in ok
/// on a stale ack and R never got the last chunk. This also tallies the
/// counters that need both sides.
fn judge(p: &Params, so: &SenderObs, ro: &ReceiverObs) {
    JUDGED.fetch_add(1, Ordering::Relaxed);
    let client = client_violation(so, ro);
    count(client, &CLIENT_RUNS);
    count(so.nok + so.dk > 0, &FAILED_FILE_RUNS);
    count(ro.timeouts > 0, &TIMEOUT_RUNS);
    count(so.restart_after_fail, &RESTART_AFTER_FAIL_RUNS);
    count(ro.first_safe_after_timeout > 0, &FIRST_SAFE_AFTER_TIMEOUT_RUNS);
    count(ro.probes_nontrivial > 0, &PROBE_NONTRIVIAL_RUNS);
    count(so.confused_acks > 0, &CONFUSED_RUNS);
    if p.property.checks_client(p.files) && client {
        // SC ended in ok and RC in nok (or the converse) for the same file.
        ASSERT_FAIL_RUNS.fetch_add(1, Ordering::Relaxed);
        traceforge::assert(false);
    }
}

fn run_once(p: Params) {
    let main_tid = thread::current().id();
    let (so, ro) = match p.model {
        Model::Dkrt => {
            // No bootstrap message: R first, S last (S captures R's id).
            let r = thread::spawn(move || receiver(p));
            let rt = r.thread().id();
            let s = thread::spawn(move || sender(p, rt));
            let so = s.join().unwrap();
            let ro = r.join().unwrap();
            (so, ro)
        }
        Model::Spin => {
            let s = thread::spawn(move || spin_sender(p, main_tid));
            let r = thread::spawn(move || spin_receiver(p, main_tid));
            let k = thread::spawn(move || spin_line(main_tid));
            let l = thread::spawn(move || spin_line(main_tid));
            let (st, rt, kt, lt) = (s.thread().id(), r.thread().id(), k.thread().id(), l.thread().id());
            for (to, a, b) in [(st, kt, rt), (rt, lt, st), (kt, rt, st), (lt, st, st)] {
                traceforge::send_msg(to, Msg::Init { peer: a });
                traceforge::send_msg(to, Msg::Init { peer: b });
            }
            let so = s.join().unwrap();
            let ro = r.join().unwrap();
            let _ = k.join();
            let _ = l.join();
            (so, ro)
        }
    };
    // Every process has ended: judge.
    judge(&p, &so, &ro);
}

fn build_config(mode: Mode, keep_going: bool, workers: usize) -> Config {
    let mut b = Config::builder().with_all_sends_lossy()
        .with_progress_report(usize::MAX)
        .with_cons_type(ConsType::FIFO);
    if keep_going {
        b = b.with_keep_going_after_error(true);
    }
    if workers > 1 {
        b = b.with_parallel(true).with_parallel_workers(workers);
    }
    match mode {
        Mode::Baseline => b.build(),
        // Every protocol send carries its own window; sd = 0 everywhere.
        Mode::Timed => b.with_timed(0, 0, 0).build(),
    }
}

fn run(mode: Mode, p: Params, keep_going: bool, workers: usize) -> (Stats, Duration) {
    let cfg = build_config(mode, keep_going, workers);
    reset_counts();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || run_once(p));
    (stats, start.elapsed())
}

fn load(c: &AtomicUsize) -> usize {
    c.load(Ordering::Relaxed)
}

fn print_one(mode: Mode, p: &Params, stats: &Stats, dur: Duration) {
    let j = load(&JUDGED);
    let (v2, v4, vc) = (load(&P2_RUNS), load(&P4_RUNS), load(&CLIENT_RUNS));
    let viol = match p.property {
        Property::P2 => v2,
        Property::P4 => v4,
        Property::Client => vc,
        Property::All => v2 + v4 + vc,
    };
    println!(
        "{:<8} model={:?} n={} MAX={} TD={} T1={} TR={} SYNC={} k={} F={} DC={} prop={:?}  \
         execs={} blocked={} impossible={} explored={} judged={j} p2_runs={v2} p2_tr_side={} p2_sync_side={} \
         p4_runs={v4} client_runs={vc} failed_file_runs={} timeout_runs={} restart_after_fail_runs={} \
         first_safe_after_timeout_runs={} probe_nontrivial_runs={} confused_ack_runs={} verdict={} time={dur:?}",
        if mode == Mode::Timed { "timed" } else { "baseline" },
        p.model,
        p.n,
        p.max,
        p.td,
        p.t1,
        p.tr,
        p.sync,
        p.k,
        p.files,
        p.dc,
        p.property,
        stats.execs,
        stats.block,
        stats.timeline_impossible,
        stats.execs + stats.block + stats.timeline_impossible,
        load(&P2_TR_RUNS),
        load(&P2_SYNC_RUNS),
        load(&FAILED_FILE_RUNS),
        load(&TIMEOUT_RUNS),
        load(&RESTART_AFTER_FAIL_RUNS),
        load(&FIRST_SAFE_AFTER_TIMEOUT_RUNS),
        load(&PROBE_NONTRIVIAL_RUNS),
        load(&CONFUSED_RUNS),
        if viol > 0 { "VIOLATED" } else { "HOLDS" },
    );
    if j == 0 && viol == 0 {
        println!("WARNING: no execution reached the judgement and nothing failed: no data, not a hold.");
    }
    // The verdict comes from these counters, and the counters are raised by
    // PROGRAM code, which runs on every ending the search enumerates. Since
    // the (C6b) timeout-miss condition is judged only once the graph is whole,
    // an ending that admits no timeline still executes and still raises these
    // counters before the engine discards it, so `viol > 0` is an UPPER bound
    // on the violations. A counter-only hold is sound (no ending at all
    // violated, feasible or not); a counter-only VIOLATED is not, and must be
    // confirmed by the engine.
    if viol > 0 && stats.timeline_impossible > 0 {
        println!(
            "NOTE: verdict=VIOLATED comes from program counters, and {} of the {} endings explored \
             admit no timeline. Some or all of the {viol} violating endings may be among them. Re-run \
             this cell WITHOUT --keep-going: exit 101 means the engine certified a counterexample, \
             exit 0 means the violation existed only in impossible worlds.",
            stats.timeline_impossible,
            stats.execs + stats.block + stats.timeline_impossible
        );
    }
    if viol == 0 {
        // A hold is informative only if the situation the property guards
        // against actually occurred in some judged execution.
        let p2 = matches!(p.property, Property::P2 | Property::All);
        if p2 && load(&FIRST_SAFE_AFTER_TIMEOUT_RUNS) == 0 {
            println!(
                "NOTE: no frame reached the receiver after it timed out. With one file that is exactly \
                 what (2) needs (compare the adjacent FIRE cell); with more files it means (2) was barely tested."
            );
        }
        if p2 && p.files >= 2 && load(&RESTART_AFTER_FAIL_RUNS) == 0 {
            println!("NOTE: no file ever started after a failed one: the (2) SYNC side was not exercised.");
        }
        if p.property.checks_p4() && load(&PROBE_NONTRIVIAL_RUNS) == 0 {
            println!("NOTE: no probe met a receiver that had seen the failed file: a (4) hold here is trivial.");
        }
        if p2 && p.n == 1 {
            println!("NOTE: n = 1: no frame has rb1 = 0, so (2) cannot fail (vacuous).");
        }
    }
    if p.model == Model::Dkrt && p.t1 <= 2 * p.td {
        println!("NOTE: T1 <= 2 TD: outside DKRT's one-place channel assumption; not comparable with their Fig. 4 model.");
    }
    if p.model == Model::Dkrt && mode == Mode::Baseline {
        println!(
            "NOTE: no clock: R's Timer can fire before a copy of the last chunk (or the Probe) still in transit, \
             and R's bound (BOUND in the header) does not read it."
        );
    } else if p.model == Model::Dkrt && p.tr < p.max as u64 * p.t1 + p.td {
        println!(
            "NOTE: TR < MAX T1 + TD: a copy of the last chunk can reach R after the timeout its bound ends on \
             (BOUND in the header); such runs are not read."
        );
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut mode = String::from("timed");
    let mut p = Params {
        model: Model::Dkrt,
        n: 2,
        max: 2,
        td: 1,
        t1: 3,
        tr: 15,
        sync: 15,
        k: 2,
        files: 1,
        dc: 0,
        property: Property::P2,
    };
    let mut dc = None;
    let mut keep_going = false;
    let mut workers = 1usize;
    while let Some(a) = args.next() {
        let mut val = |f: &str| args.next().unwrap_or_else(|| panic!("{f} needs a value"));
        match a.as_str() {
            "--mode" => mode = val("--mode"),
            "--model" => {
                p.model = match val("--model").as_str() {
                    "dkrt" => Model::Dkrt,
                    "spin" => Model::Spin,
                    x => panic!("unknown model {x}"),
                }
            }
            "--n" => p.n = val("--n").parse().expect("--n"),
            "--max" => p.max = val("--max").parse().expect("--max"),
            "--td" => p.td = val("--td").parse().expect("--td"),
            "--t1" => p.t1 = val("--t1").parse().expect("--t1"),
            "--tr" => p.tr = val("--tr").parse().expect("--tr"),
            "--sync" => p.sync = val("--sync").parse().expect("--sync"),
            "--k" => p.k = val("--k").parse().expect("--k"),
            "--files" => p.files = val("--files").parse().expect("--files"),
            "--dc" => dc = Some(val("--dc").parse().expect("--dc")),
            "--property" => {
                p.property = match val("--property").as_str() {
                    "p2" => Property::P2,
                    "p4" => Property::P4,
                    "client" => Property::Client,
                    "all" => Property::All,
                    x => panic!("unknown property {x}"),
                }
            }
            "--keep-going" => keep_going = true,
            "--workers" => workers = val("--workers").parse().expect("--workers"),
            _ => panic!(
                "unknown flag {a}. Usage: brp_timed [--mode timed|baseline|compare] [--model dkrt|spin] \
                 [--n N] [--max M] [--td D] [--t1 T] [--tr R] [--sync S] [--k K] [--files F] [--dc DC] \
                 [--property p2|p4|client|all] [--keep-going] [--workers W]"
            ),
        }
    }
    assert!(p.n >= 1 && p.files >= 1 && p.k >= 1 && p.td >= 1);
    assert!(
        p.property != Property::Client || p.files == 1,
        "the client property is judged on final states, which is exact for one file only (--files 1)"
    );
    assert!(p.t1 * p.k >= 2 && p.tr * p.k >= 2, "k*T1 and k*TR must be at least 2");
    // The client's wait before a later file: long enough for R to time out
    // (DC >= TR removes the bounded-client false holds; see DEVIATIONS).
    p.dc = dc.unwrap_or(p.tr + p.td);
    let mut modes = match mode.as_str() {
        "timed" => vec![Mode::Timed],
        "baseline" => vec![Mode::Baseline],
        "compare" => vec![Mode::Baseline, Mode::Timed],
        m => panic!("unknown mode {m}"),
    };
    if p.model == Model::Spin {
        // App. C has no clocks: it is an untimed model by construction.
        modes = vec![Mode::Baseline];
    }
    for m in modes {
        let (s, d) = run(m, p, keep_going, workers);
        print_one(m, &p, &s, d);
    }
}
