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
//!     for u > 0) it gave DKRT's dense verdict on every cell tested. Both are
//!     checked against IMITATOR (see DEVIATIONS and RESULTS).
//!   * Strict guards. x < T1: S waits Finite(k*T1 - 1) for an ack, then
//!     sleep(1) is the x = T1 transition (a timeout taken while an ack is
//!     readable is that ack lost on L, which DKRT allow). z < TR: R arms its
//!     timer at k*TR - 1 and, on the live one, sleep(1) is the z = TR
//!     transition; a frame arriving at z = TR is read after it, in new_file.
//!   * Loss: every lossy send may be dropped (budget = number of lossy sends),
//!     since DKRT's lines lose without bound; at sd = 0 an evicted frame or ack
//!     is another loss, also DKRT-legal.
//!   * The sender client offers the next file after any wait in [0, DC],
//!     DC >= TR (DKRT's client has no deadline; an eager client with a
//!     bounded number of files would report false holds of (2)).
//!   * Properties are ghost observations, judged once in main after both
//!     threads' guards: (2) a frame with rb1 = 0 taken in new_file; (4) a
//!     zero-transit Probe from S at x = SYNC that finds R in idle, including
//!     the tie with R's own z = TR instant (a Finite(0) probe read after the
//!     sleep); client: the one-file analogue of DKRT Table 2 (1) and (2) on
//!     their Sec. 3.2 channels, the run ends with one client in ok and the
//!     other in nok (see client_violation; refused for more than one file).
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
//!     dense verdict on every cell tested, and IMITATOR's half-grid and dense
//!     regions coincide at every integer point checked (n = 2, MAX <= 3), but
//!     that is established empirically, not proven.
//!
//! HARNESS (sd = 0 eviction, as in chandra_toueg_timed). A receive may skip a
//! message from another channel; for frames and acks that is a loss (legal),
//! for R's timer it is a timeout never taken (illegal). Every armed timer is
//! read before R reports (count drain), every Probe must be read (count
//! assume), R keeps running after Done until its pending timeout (its I_NOK is
//! a client indication), Done travels on the frame channel with transit k*TD,
//! and the only assert is in main after both summaries.
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
//! RESULTS. Data, oracle, drivers and a README are in
//! docs/research/ta_problems/need-from-user-papers/brp-b04 (final build:
//! files *_v2*, final_chain.meta has the source md5).
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
//!    each, k = 1 and 2): 200 runs, 200/200 agree with the IMITATOR oracle of
//!    the same time semantics (k = 1 integer, k = 2 half grid). Against the
//!    dense model the only disagreements are the 4 predicted k = 1 cells at
//!    SYNC = TR + TD - T1 - 1 (integer time holds; dense time violates, the
//!    new file's first frame needing a transit below one time unit).
//!    DKRT's own cell MAX 2, TD 1, T1 3, TR 15 at k = 2: (4) VIOLATED at
//!    SYNC = 13 (28 runs, the tie) and HOLDS at SYNC = 14 < TR (756 runs with
//!    a non-trivial probe), refuting TACAS'97's claim that (A2) holds only
//!    if SYNC >= TR, on the executable port. (2) at the same cell with two files,
//!    k = 2: SYNC = 12 VIOLATED (102 runs, all SYNC side), 13 HOLDS (the exact
//!    edge), 14 HOLDS, 11.9M executions each (787K restarts after a failure,
//!    1.1M first frames after a receiver timeout; ~44 min on 12 workers).
//! 2. The two mechanisms (classified witnesses): TR side = DKRT's Fig. 7 (a
//!    premature receiver timeout, then a late copy of chunk 2 taken as a first
//!    frame); SYNC side = a new file started while R still holds the old one:
//!    its first frame is taken as a duplicate, R times out, chunk 2 enters
//!    first_safe_frame and RC ends with I_OK for a file whose first chunk it
//!    never got.
//! 3. Outside (5), at T1 = 2 TD, TR = 2 MAX T1 + 3 TD, SYNC = TR, with the
//!    unbounded lossy channels of DKRT Sec. 3.2: SC ends with I_OK and RC with
//!    I_NOK in all 8 cells tested (MAX 1..2, TD 1..2, k 1..2), and every cell
//!    at T1 = 2 TD + 1 holds. An ack arriving exactly at x = T1 is consumed
//!    after the retransmission and taken for the next chunk (acks carry no
//!    bit). DKRT's one-place Fig. 4 is not deadlock-free there (IMITATOR).
//! 4. Precision: this program run untimed reports violations of (2), (4) and
//!    the client property for every parameter value (its counts do not depend
//!    on the time constants). DKRT's App. C model (--model spin) has no time
//!    parameters and satisfies (2) and the client property for every (n, MAX,
//!    F) run, in 25 / 113 / 625 / 12,769 executions for (2,1,1) / (2,2,1) /
//!    (2,1,2) / (2,2,2), which equal the hand count of App. C's choice tree:
//!    its tricks build in the assumptions whose region the timed model computes.
//! 5. Efficiency, same program, single worker, median of 3 (the counts are
//!    identical across repeats; untimed "counterexamples" are its spurious
//!    violations, filed as blocked; timed blocked runs are sd = 0 guards):
//!      cell (DKRT's safe settings)            untimed                     timed
//!      (2)  MAX 2 TD 1 T1 3 TR=SYNC=15 F=1 k=1  41,515 (24,912 cex) 3.2 s   2,200 proof 1.8 s
//!      (4)  same                                91,491 (60,884 cex) 7.6 s   2,200 proof 2.3 s
//!      (2)  MAX 1 TD 1 T1 3 TR 9 SYNC 7 F=2 k=1  2,994,579 (1,878,828) 286 s  66,397 proof 88 s
//!    19x to 45x fewer executions and 1.8x to 3.3x faster, with a proof where
//!    untimed reports false alarms. IMITATOR synthesizes each whole parametric
//!    region in 0.3 to 9 s: no speed claim against timed-automata tools.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, ConsType, Stats, WaitTime};

// =====================================================================
// Messages
// =====================================================================

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// Bootstrap from main (untimed, transparent for timing).
    Init { peer: ThreadId },
    /// Channel K, S -> R: F!(b1, bN, ab). `file` and `chunk` are ghost data
    /// for the observations; no protocol decision reads them.
    Frame { b1: bool, bn: bool, ab: bool, file: u32, chunk: u32 },
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
    /// S -> R after the last file: stop, and how many Probes were sent.
    Done { probes: u32 },
    /// To main: what each process observed (ghost data).
    SenderSummary { s: SenderObs },
    ReceiverSummary { r: ReceiverObs },
    /// DKRT App. C (Promela) model only: loss notification from a line to S,
    /// the SyncWait double handshake, and the end of the run.
    ChunkTimeout,
    SyncWait,
    Stop,
}

const PROBE_TAG: u32 = 1;
/// Storage lifetime at main (summaries only): longer than any run.
const MAIN_SD: u64 = 1 << 40;

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
    fn checks_p4(self) -> bool {
        matches!(self, Property::P4 | Property::All)
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
    /// Loss budget (lossy sends that may be dropped per execution).
    loss: usize,
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
// Counters (over executions that reach the judgement)
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
/// Judged runs whose assertion fails (the engine files them as blocked).
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

fn sender(p: Params, main_tid: ThreadId) {
    let me = thread::current().id();
    let receiver = match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => peer,
        m => panic!("sender: expected Init, got {m:?}"),
    };
    let mut obs = SenderObs::default();
    // Fig. 1: ab := 0 initially.
    let mut ab = false;
    let mut probes = 0u32;
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
            let frame = Msg::Frame { b1, bn, ab, file, chunk: i };
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
                probes += 1;
            }
            ab = false;
        }
        failed_before |= failed;
    }
    // Harness: after the last file, on the same FIFO channel as every frame.
    traceforge::send_msg_timed(receiver, Msg::Done { probes }, p.td(), p.td());
    traceforge::send_msg_timed(main_tid, Msg::SenderSummary { s: obs }, 0, 0);
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
    obs: ReceiverObs,
}

impl RState {
    fn new() -> Self {
        Self {
            loc: RLoc::NewFile,
            exp_ab: false,
            rbn: false,
            last_file: 0,
            last_chunk: 0,
            seen_any: false,
            last_reported_file: None,
            obs: ReceiverObs::default(),
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
                    // A frame of the file R last accepted: R gave it up too
                    // early (TR side). Of a later file: the new file started
                    // while R still held the old one (SYNC side).
                    self.obs.p2 = true;
                    if self.last_reported_file == Some(file) {
                        self.obs.p2_tr_side = true;
                    } else {
                        self.obs.p2_sync_side = true;
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
    }

    fn probe(&mut self, file: u32) {
        if self.loc == RLoc::Idle {
            self.obs.p4 = true;
        }
        if self.seen_any && self.last_file == file {
            self.obs.probes_nontrivial += 1;
        }
    }

    fn ack(&self) -> Msg {
        Msg::Ack { file: self.last_file, chunk: self.last_chunk }
    }
}

struct Receiver {
    me: ThreadId,
    sender: ThreadId,
    p: Params,
    st: RState,
    /// Timer bookkeeping: the live epoch, and how many timers were armed
    /// and read (every armed timer must be read before the judgement).
    epoch: u32,
    armed: u32,
    read: u32,
    probes_read: u32,
}

impl Receiver {
    /// z := 0: arm the timer for the next z = TR.
    fn reset_z(&mut self) {
        self.epoch += 1;
        self.armed += 1;
        traceforge::send_msg_timed(self.me, Msg::Timer { epoch: self.epoch }, self.p.tr() - 1, self.p.tr() - 1);
    }

    fn frame(&mut self, b1: bool, bn: bool, ab: bool, file: u32, chunk: u32) {
        let (ack, reported) = self.st.frame(b1, bn, ab, file, chunk);
        if ack {
            traceforge::send_lossy_msg_timed(self.sender, self.st.ack(), self.p.lo(), self.p.td());
        }
        if reported {
            self.reset_z();
        }
    }

    /// The live timer, read at z = TR - 1 (scaled): wait to z = TR, catch a
    /// Probe sent at exactly this instant (R is still idle), then time out.
    fn timeout(&mut self) {
        traceforge::sleep(1);
        let sender = self.sender;
        if self.p.property.checks_p4() {
            let probe = traceforge::recv_tagged_msg_timed::<_, Msg>(
                move |s, tag| s == sender && tag == Some(PROBE_TAG),
                WaitTime::Finite(0),
            );
            match probe {
                Some(Msg::Probe { file }) => {
                    self.probes_read += 1;
                    self.st.probe(file);
                }
                Some(m) => panic!("receiver: unexpected {m:?}"),
                None => {}
            }
        }
        self.st.give_up();
    }
}

fn receiver(p: Params, main_tid: ThreadId) {
    let me = thread::current().id();
    let sender = match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => peer,
        m => panic!("receiver: expected Init, got {m:?}"),
    };
    let mut r = Receiver { me, sender, p, st: RState::new(), epoch: 0, armed: 0, read: 0, probes_read: 0 };
    let probes_sent = loop {
        match traceforge::recv_msg_block_timed::<Msg>() {
            Msg::Frame { b1, bn, ab, file, chunk } => r.frame(b1, bn, ab, file, chunk),
            Msg::Timer { epoch } => {
                r.read += 1;
                if epoch == r.epoch && r.st.loc == RLoc::Idle {
                    r.timeout();
                }
            }
            Msg::Probe { file } => {
                r.probes_read += 1;
                r.st.probe(file);
            }
            Msg::Done { probes } => break probes,
            m => panic!("receiver: unexpected {m:?}"),
        }
    };
    // Nothing else can come on the S -> R channel after Done (FIFO), so R's
    // remaining DKRT behaviour is its pending timeout (idle has invariant
    // z <= TR): let it happen, since its I_NOK is a client indication.
    while r.st.loc == RLoc::Idle {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == me) {
            Msg::Timer { epoch } => {
                r.read += 1;
                if epoch == r.epoch {
                    r.timeout();
                }
            }
            m => panic!("receiver after Done: unexpected {m:?}"),
        }
    }
    // Harness: every armed timer must be read. A timer evicted at sd = 0 (a
    // later frame read first) is a timeout R never took, which DKRT's
    // invariant z <= TR forbids; such a run blocks here, before judgement.
    while r.read < r.armed {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == me) {
            Msg::Timer { .. } => r.read += 1,
            m => panic!("receiver drain: unexpected {m:?}"),
        }
    }
    // A Probe evicted unread is a check never made: discard the run.
    traceforge::assume!(r.probes_read == probes_sent);
    traceforge::send_msg_timed(main_tid, Msg::ReceiverSummary { r: r.st.obs }, 0, 0);
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

fn spin_sender(p: Params, main_tid: ThreadId) {
    let (line_k, receiver) = match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
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
            let frame = Msg::Frame { b1: i == 1, bn: i == p.n, ab, file, chunk: i };
            traceforge::send_msg(line_k, frame.clone());
            let mut rc = 0u32;
            let acked = loop {
                // wait_ack: B?ACK or ChunkTimeout?SHAKE.
                match traceforge::recv_msg_block::<Msg>() {
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
            match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == receiver) {
                Msg::SyncWait => {}
                m => panic!("spin sender: expected SyncWait, got {m:?}"),
            }
            ab = false;
        }
        failed_before |= failed;
    }
    traceforge::send_msg(line_k, Msg::Stop);
    traceforge::send_msg(main_tid, Msg::SenderSummary { s: obs });
}

/// Line_K (S -> R) or Line_L (R -> S): deliver, or lose and tell S.
fn spin_line(main_tid: ThreadId) {
    let (dest, sender) = match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
            Msg::Init { peer: s } => (peer, s),
            m => panic!("spin line: expected Init, got {m:?}"),
        },
        m => panic!("spin line: expected Init, got {m:?}"),
    };
    loop {
        match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s != main_tid) {
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

fn spin_receiver(main_tid: ThreadId) {
    let (line_l, sender) = match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
        Msg::Init { peer } => match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s == main_tid) {
            Msg::Init { peer: s } => (peer, s),
            m => panic!("spin receiver: expected Init, got {m:?}"),
        },
        m => panic!("spin receiver: expected Init, got {m:?}"),
    };
    let mut st = RState::new();
    loop {
        match traceforge::recv_tagged_msg_block::<_, Msg>(move |s, _| s != main_tid) {
            Msg::Frame { b1, bn, ab, file, chunk } => {
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
    traceforge::send_msg(main_tid, Msg::ReceiverSummary { r: st.obs });
}

// =====================================================================
// Harness: main spawns the processes, then judges once all are done
// =====================================================================

fn judge(p: &Params, so: SenderObs, ro: ReceiverObs) {
    JUDGED.fetch_add(1, Ordering::Relaxed);
    let client = client_violation(&so, &ro);
    count(ro.p2, &P2_RUNS);
    count(ro.p2_tr_side, &P2_TR_RUNS);
    count(ro.p2_sync_side, &P2_SYNC_RUNS);
    count(ro.p4, &P4_RUNS);
    count(client, &CLIENT_RUNS);
    count(so.nok + so.dk > 0, &FAILED_FILE_RUNS);
    count(ro.timeouts > 0, &TIMEOUT_RUNS);
    count(so.restart_after_fail, &RESTART_AFTER_FAIL_RUNS);
    count(ro.first_safe_after_timeout > 0, &FIRST_SAFE_AFTER_TIMEOUT_RUNS);
    count(ro.probes_nontrivial > 0, &PROBE_NONTRIVIAL_RUNS);
    count(so.confused_acks > 0, &CONFUSED_RUNS);
    let ok = match p.property {
        Property::P2 => !ro.p2,
        Property::P4 => !ro.p4,
        Property::Client => !client,
        Property::All => !ro.p2 && !ro.p4 && (p.files > 1 || !client),
    };
    count(!ok, &ASSERT_FAIL_RUNS);
    traceforge::assert(ok);
}

/// The two summaries, in a fixed order (reading them in either order would
/// double every execution). Main's storage lifetime is long (see
/// build_config), so the summary that arrives first waits to be read.
fn collect(st: ThreadId, rt: ThreadId) -> (SenderObs, ReceiverObs) {
    let so = match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == st) {
        Msg::SenderSummary { s } => s,
        m => panic!("main: unexpected {m:?}"),
    };
    let ro = match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _| s == rt) {
        Msg::ReceiverSummary { r } => r,
        m => panic!("main: unexpected {m:?}"),
    };
    (so, ro)
}

fn run_once(p: Params) {
    let main_tid = thread::current().id();
    match p.model {
        Model::Dkrt => {
            let s = thread::spawn(move || sender(p, main_tid));
            let r = thread::spawn(move || receiver(p, main_tid));
            let (st, rt) = (s.thread().id(), r.thread().id());
            traceforge::send_msg(st, Msg::Init { peer: rt });
            traceforge::send_msg(rt, Msg::Init { peer: st });
            let (so, ro) = collect(st, rt);
            let _ = s.join();
            let _ = r.join();
            judge(&p, so, ro);
        }
        Model::Spin => {
            let s = thread::spawn(move || spin_sender(p, main_tid));
            let r = thread::spawn(move || spin_receiver(main_tid));
            let k = thread::spawn(move || spin_line(main_tid));
            let l = thread::spawn(move || spin_line(main_tid));
            let (st, rt, kt, lt) = (s.thread().id(), r.thread().id(), k.thread().id(), l.thread().id());
            for (to, a, b) in [(st, kt, rt), (rt, lt, st), (kt, rt, st), (lt, st, st)] {
                traceforge::send_msg(to, Msg::Init { peer: a });
                traceforge::send_msg(to, Msg::Init { peer: b });
            }
            let (so, ro) = collect(st, rt);
            for h in [s, r, k, l] {
                let _ = h.join();
            }
            judge(&p, so, ro);
        }
    }
}

fn build_config(mode: Mode, p: &Params, keep_going: bool, workers: usize) -> Config {
    let mut b = Config::builder()
        .with_progress_report(usize::MAX)
        .with_cons_type(ConsType::FIFO)
        .with_lossy(p.loss);
    if keep_going {
        b = b.with_keep_going_after_error(true);
    }
    if workers > 1 {
        b = b.with_parallel(true).with_parallel_workers(workers);
    }
    match mode {
        Mode::Baseline => b.build(),
        // Every protocol send carries its own window; sd = 0 at S and R.
        // Main only collects the two summaries: it keeps them until read.
        Mode::Timed => b.with_timed(0, 0, 0).with_node_sd(thread::main_thread_id(), MAIN_SD).build(),
    }
}

fn run(mode: Mode, p: Params, keep_going: bool, workers: usize) -> (Stats, Duration) {
    let cfg = build_config(mode, &p, keep_going, workers);
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
        "{:<8} model={:?} n={} MAX={} TD={} T1={} TR={} SYNC={} k={} F={} DC={} loss={} prop={:?}  \
         execs={} blocked={} explored={} judged={j} p2_runs={v2} p2_tr_side={} p2_sync_side={} \
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
        p.loss,
        p.property,
        stats.execs,
        stats.block,
        stats.execs + stats.block,
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
    if j == 0 {
        println!("WARNING: no execution reached the judgement: no data, not a hold.");
    }
    // The verdict comes from these counters. Every judged run is either a
    // complete execution or one whose assertion failed (filed as blocked);
    // anything else would be a judged run the engine did not certify.
    let fails = load(&ASSERT_FAIL_RUNS);
    if j != stats.execs + fails {
        println!(
            "WARNING: {j} judged runs but {} complete + {fails} failing: the verdict may count a run the \
             engine did not certify; confirm without --keep-going.",
            stats.execs
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
        loss: 0,
        property: Property::P2,
    };
    let mut dc = None;
    let mut loss = None;
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
            "--loss" => loss = Some(val("--loss").parse().expect("--loss")),
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
                 [--loss B] [--property p2|p4|client|all] [--keep-going] [--workers W]"
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
    // Every lossy send may be dropped: DKRT's channels lose without bound.
    p.loss = loss.unwrap_or((2 * p.n as usize) * (p.max as usize + 1) * p.files as usize);
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
