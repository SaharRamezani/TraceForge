//! TTL lease lock with fencing tokens (split-brain safety).
//!
//! Motivation (Kleppmann 2016, "How to do distributed locking"; the
//! Jepsen etcd analyses): a client that acquires a TTL lease and then
//! pauses (GC pause, scheduling stall) for longer than the TTL can
//! resume believing it still holds the lock and write to shared
//! storage while another client legitimately holds the lease: mutual
//! exclusion is lost. Fencing tokens (a monotonically increasing token
//! issued with each grant and checked by the storage) restore safety
//! without any timing assumption. An untimed checker cannot analyze
//! the unfenced protocol meaningfully, because its safety depends on
//! the pause length relative to the TTL, a relation only a timed
//! semantics can express.
//!
//! ## Actors and message flow (R rounds, C clients; client 0 is the
//! pausing holder, all other clients never pause)
//!
//! Threads are long-lived (the three_pc_timed idiom): each is spawned
//! once and loops over R rounds internally. Every round:
//!
//!   LockService ---- RoundStart{r} ----> every client
//!   client i    ---- Acquire{i,r} ----> LockService    (tag ACQ)
//!   LockService: drains all C Acquires into a local FIFO queue,
//!                then serves the queue one holder at a time:
//!   LockService ---- Grant{token} ----> next queued client
//!   client i: sleep(PAUSE_i)                           (the GC pause)
//!   client i ---- Write{token, i} ----> Storage
//!   Storage  ---- WriteAck ----> client i
//!   client i ---- Release{token} ----> LockService     (tag REL+token)
//!
//! Clients demand the lock concurrently, exactly as in Kleppmann's
//! scenario: there is NO staggering and NO retry loop, and the only
//! sleep in the file is the GC pause under study (unconditional, both
//! modes). The up-front drain models what every real lock service
//! does: accept and queue concurrent lock requests immediately, then
//! grant the lock as it frees. All C! drain (= grant) orders are
//! explored. Because the post-expiry grant is served from the
//! service's LOCAL queue, it involves no late message read and cannot
//! be timed-infeasible: the vacuity trap of the earlier staggered
//! design (which had to keep a queued Acquire readable TTL time units
//! after its send) is structurally gone, and with it the --spread-ratio
//! parameter.
//!
//! After sending a Grant the service waits for that holder's Release
//! with a token-scoped finite-wait receive of W_r = TTL: Some(Release)
//! means the lease was freed early (RELEASED), None means the TTL
//! elapsed and the lease is considered EXPIRED; either way the next
//! queued client is granted token + 1. Tokens increase monotonically
//! across rounds (round r grants tokens r*C+1 ..= (r+1)*C). The
//! Release tag encodes the token (rel_tag), so a stale Release from an
//! expired ex-holder, which stays in the mailbox forever, can never be
//! confused for the current holder's Release, in any round. The
//! Storage receives exactly C*R writes and always acks. RoundStart r+1
//! is broadcast only after every round-r lease is resolved, so rounds
//! never overlap at the service; a round at the service is the direct
//! analog of a 3PC round at the coordinator.
//!
//! ## Failure scenario modeled
//!
//! In any round, client A (client 0) is granted token t and
//! immediately pauses for PAUSE time units. If PAUSE exceeds the TTL,
//! the service's Release wait times out, the lease expires, and the
//! next queued client B is granted token t+1. B writes to storage. A
//! then resumes, still believing it holds the lease, and its stale
//! token-t write reaches the storage AFTER B's token-(t+1) write: the
//! classic split-brain lost-update. Fencing makes the storage reject
//! the stale write; without fencing the write is applied and the
//! safety assertion fires.
//!
//! ## Safety properties (all are per-node SAFETY assertions)
//!
//! At the Storage, at apply time, for every applied write:
//!
//!     traceforge::assert(token > last_applied_token)
//!
//! that is, applied tokens are strictly increasing. This is precisely
//! "a write from a stale holder is never applied after a newer
//! holder's write" and "an expired token is never accepted past a
//! newer one". With fencing off every write takes the apply path, so
//! the assert IS the mutual-exclusion property. With fencing on the
//! storage rejects token < last_applied writes (counted in
//! STALE_REJECTED) and the same assert documents that the fencing
//! comparison is exactly the needed check.
//!
//! Liveness properties ("every client eventually gets the lease",
//! "every write is eventually acked") are deliberately NOT asserted:
//! under arbitrary message delay/loss they are simply false, and in a
//! bounded model an inconclusive run supports no liveness claim. Every
//! checked property is a safety assertion evaluated locally at one
//! node with no global observer.
//!
//! ## Known limitation: multi-round pauser re-entry
//!
//! A client paused past its lease also misses the START of the next
//! round: with R >= 2 and PAUSE far above TTL, the pauser can reach
//! its RoundStart read only after the message's readability window
//! (send_hi + U + sd) has closed, and the execution blocks. This
//! mirrors reality (a long-paused client misses the next epoch), but
//! it means R >= 2 sweeps should keep PAUSE near the boundary, and a
//! run that completes zero executions verified nothing (the binary
//! prints a loud WARNING in that case; sweep harnesses must treat
//! execs = 0 as "no data", never as a hold).
//!
//! ## CLI parameters (ratios are over U, like three_pc_timed)
//!
//!   --mode baseline|timed|compare   verification mode (default compare)
//!   --clients C                     number of clients (default 2;
//!                                   client 0 is the pauser)
//!   --rounds R                      lease rounds, long-lived threads
//!                                   (default 1)
//!   --u U                           global transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U), transit lower bound
//!                                   (default 0.0; L <= U enforced)
//!   --sd-ratio SR                   sd = round(SR * U), message storage
//!                                   lifetime past arrival (default 0.0)
//!   --ttl-ratio TR                  TTL = round(TR * U), lease TTL and the
//!                                   service's Release wait (default 6.0)
//!   --pause-ratio PR                PAUSE = round(PR * U), client 0's GC
//!                                   pause after Grant (default 0.0)
//!   --fencing on|off                storage checks fencing tokens
//!                                   (default on)
//!
//! CLI misuse (unknown flag, malformed value, invalid combination)
//! exits with code 2, distinct from a property violation's 101, so
//! exit-code-based harnesses cannot mistake a typo for a FIRE.
//!
//! ## Expected verdict matrix (defaults unless stated)
//!
//!   fencing on,  baseline                    : hold (exit 0), fencing
//!       restores safety with NO timing assumptions; run with
//!       --pause-ratio 10 to see STALE_REJECTED > 0 (the stale
//!       interleaving explored and defused).
//!   fencing on,  timed                       : hold; under safe
//!       timings no stale write can even occur, so also run
//!       --pause-ratio 10 to see STALE_REJECTED > 0.
//!   fencing off, baseline                    : FIRE (exit 101), the
//!       untimed checker admits pause > TTL unconditionally; it can
//!       never verify a lease protocol.
//!   fencing off, timed (PAUSE small vs TTL)  : hold, non-vacuous
//!       (EXPIRED > 0 in the sub-boundary cells with PAUSE close to
//!       the boundary shows the expiry branch is explored: safety
//!       holds DESPITE expiry because the stale write provably lands
//!       before the new holder's write): the timed checker PROVES the
//!       unfenced protocol safe under a bounded pause.
//!   fencing off, timed, --pause-ratio 10     : FIRE with the genuine
//!       Kleppmann counterexample (A granted token t, pauses past TTL,
//!       B granted token t+1 and writes, A's stale write applied
//!       after).
//!
//! Note that with --fencing off, --mode compare exits 101 during its
//! baseline leg, before the comparison table is printed; use the
//! single-mode invocations to study the unfenced protocol.
//!
//! ## Hold/FIRE boundary (fencing off, timed; matrix-verified)
//!
//! The checker FIREs exactly when
//!
//!     PAUSE >= TTL - 2 * (U - L + sd)
//!
//! at EVERY round count R (verified at R in {1,2} for U in {1,2},
//! L in {0,U}, sd in {0,1}). The two slack hops are the pauser's write
//! path: its write is readable until grant_send + PAUSE + 2(U + sd),
//! while the next holder's write cannot be read before
//! grant_send + TTL + 2L; the split-brain is realizable iff the first
//! reaches past the second. Timed FIREs are certified: the checker
//! validates every reported counterexample against one consistent
//! timeline and prints the witness timestamps next to the graph.
//!
//! Historical note: the interval walker (the pre-exact engine) FIREd
//! from PAUSE = TTL - 4(U - L + sd) at R = 1 and at EVERY pause at
//! R >= 2; those extra FIREs were per-event interval relaxation
//! artifacts (P1 feedback item 6), realized by no timeline. They are
//! gone under the exact difference-constraint engine (the only engine
//! on this branch; the interval walker lives in the pre-timed-exact
//! history for A/B comparisons).
//!
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

// Outcome counters: reset before each verify, read after. verify runs
// many executions; the counters accumulate across all of them
// (including partially-explored blocked executions).
static GRANTS: AtomicUsize = AtomicUsize::new(0);
static EXPIRED: AtomicUsize = AtomicUsize::new(0);
static RELEASED: AtomicUsize = AtomicUsize::new(0);
static APPLIED: AtomicUsize = AtomicUsize::new(0);
static STALE_REJECTED: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 0.0;
const DEFAULT_TTL_RATIO: f64 = 6.0;
const DEFAULT_PAUSE_RATIO: f64 = 0.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const DEFAULT_CLIENTS: u32 = 2;
const DEFAULT_ROUNDS: u32 = 1;

// Message tags used for content-level filtering at the service (the
// recv predicates can only see sender and tag, not the payload).
// Releases are token-scoped: rel_tag(t) identifies the Release of
// grant t uniquely across all rounds, so lingering stale Releases can
// never satisfy the current holder's Release wait.
const TAG_ACQ: u32 = 1;
const TAG_INIT: u32 = 3;
const TAG_REL_BASE: u32 = 1000;

fn rel_tag(token: u64) -> u32 {
    TAG_REL_BASE + token as u32
}

/// Messages received by the LockService.
#[derive(Clone, Debug, PartialEq)]
enum SMsg {
    Init { clients: Vec<ThreadId> },
    Acquire { client: ThreadId, round: u32 },
    Release { token: u64 },
}

/// Messages received by a client.
#[derive(Clone, Debug, PartialEq)]
enum CMsg {
    Init { service: ThreadId, storage: ThreadId },
    RoundStart { round: u32 },
    Grant { token: u64 },
    WriteAck,
}

/// Messages received by the Storage.
#[derive(Clone, Debug, PartialEq)]
struct WMsg {
    token: u64,
    writer: ThreadId,
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
    ttl: u64,
    pause: u64,
    sd: u64,
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

impl Bounds {
    fn from_ratios(u: u64, l_ratio: f64, ttl_ratio: f64, pause_ratio: f64, sd_ratio: f64) -> Self {
        if u < 1 {
            cli_bail("U must be >= 1");
        }
        let l = (l_ratio * u as f64).round() as u64;
        if l > u {
            cli_bail("transit lower bound L must be <= U (check --l-ratio)");
        }
        let ttl = (ttl_ratio * u as f64).round() as u64;
        if ttl < 1 {
            cli_bail("TTL must round to >= 1 (check --ttl-ratio)");
        }
        let pause = (pause_ratio * u as f64).round() as u64;
        let sd = (sd_ratio * u as f64).round() as u64;
        Self { u, l, ttl, pause, sd }
    }
}

// =====================================================================
// LockService
// =====================================================================

fn lock_service(main_tid: ThreadId, ttl: u64, rounds: u32) {
    // Setup handshake, sender-filtered so a racing Acquire cannot be
    // read in its place (the raft_leader_election Init idiom).
    let clients = match traceforge::recv_tagged_msg_block::<_, SMsg>(move |s, _tag| s == main_tid)
    {
        SMsg::Init { clients } => clients,
        m => panic!("service: expected Init, got {m:?}"),
    };

    let mut token: u64 = 1;
    for round in 0..rounds {
        // (a) Open the round. RoundStart r+1 is only sent after every
        // round-r lease resolved, so rounds never overlap here.
        for c in &clients {
            traceforge::send_msg(*c, CMsg::RoundStart { round });
        }

        // (b) Drain: accept and queue this round's C concurrent
        // Acquires immediately, the way a real lock service queues
        // waiters. Every Acquire is read close to its arrival, so no
        // read here depends on long message retention (no sd or
        // staggering needed), and all C! grant orders are explored.
        let mut queue: Vec<ThreadId> = Vec::new();
        for _ in 0..clients.len() {
            match traceforge::recv_tagged_msg_block_timed::<_, SMsg>(|_s, tag| {
                tag == Some(TAG_ACQ)
            }) {
                SMsg::Acquire { client, round: r } => {
                    // Clients acquire only after reading RoundStart{r},
                    // and all round r-1 Acquires were drained in round
                    // r-1, so a cross-round Acquire cannot appear.
                    if r != round {
                        panic!("service: round-{r} Acquire drained in round {round}");
                    }
                    queue.push(client);
                }
                m => panic!("service: expected Acquire, got {m:?}"),
            }
        }

        // (c) Serve the queue: grant, then wait for THIS grant's
        // Release for at most TTL. The post-expiry grant comes from
        // the local queue, not from a message read, so the expiry path
        // can never be timed-infeasible.
        for holder in queue {
            traceforge::send_msg(holder, CMsg::Grant { token });
            GRANTS.fetch_add(1, Ordering::Relaxed);

            let outcome: Option<SMsg> = traceforge::recv_tagged_msg_timed(
                move |s, tag| s == holder && tag == Some(rel_tag(token)),
                WaitTime::Finite(ttl),
            );
            match outcome {
                Some(SMsg::Release { .. }) => {
                    RELEASED.fetch_add(1, Ordering::Relaxed);
                }
                Some(m) => panic!("service: expected Release, got {m:?}"),
                None => {
                    // TTL elapsed: the lease is considered expired and
                    // the next grant gets a larger token. The ex-holder
                    // may still believe it holds the lease.
                    EXPIRED.fetch_add(1, Ordering::Relaxed);
                }
            }
            token += 1;
        }
    }
}

// =====================================================================
// Client (shared by the pauser and the contenders)
// =====================================================================

fn client(main_tid: ThreadId, pause: u64, rounds: u32) {
    let (service, storage_tid) =
        match traceforge::recv_tagged_msg_block::<_, CMsg>(move |s, _tag| s == main_tid) {
            CMsg::Init { service, storage } => (service, storage),
            m => panic!("client: expected Init, got {m:?}"),
        };
    let me = thread::current().id();

    for round in 0..rounds {
        // The service channel strictly alternates RoundStart, Grant
        // (FIFO), so each blocking read below sees exactly the
        // expected variant.
        match traceforge::recv_tagged_msg_block_timed::<_, CMsg>(move |s, _tag| s == service) {
            CMsg::RoundStart { round: r } => {
                if r != round {
                    panic!("client: RoundStart{{{r}}} in round {round}");
                }
            }
            m => panic!("client: expected RoundStart, got {m:?}"),
        }

        // Demand is concurrent: every client acquires as soon as it
        // sees the round open (Kleppmann's contending clients). No
        // staggering, no retry.
        traceforge::send_tagged_msg(service, TAG_ACQ, SMsg::Acquire { client: me, round });

        // The Grant always arrives eventually (the service grants
        // every queued Acquire), so a blocking timed recv is safe: the
        // client starts waiting before the Grant is sent.
        let token = match traceforge::recv_tagged_msg_block_timed::<_, CMsg>(move |s, _tag| {
            s == service
        }) {
            CMsg::Grant { token } => token,
            m => panic!("client: expected Grant, got {m:?}"),
        };

        // The GC pause under study (unconditional, both modes). After
        // it the client still believes it holds the lease, TTL or not.
        if pause > 0 {
            traceforge::sleep(pause);
        }

        traceforge::send_msg(storage_tid, WMsg { token, writer: me });
        match traceforge::recv_tagged_msg_block_timed::<_, CMsg>(move |s, _tag| s == storage_tid)
        {
            CMsg::WriteAck => {}
            m => panic!("client: expected WriteAck, got {m:?}"),
        }

        traceforge::send_tagged_msg(service, rel_tag(token), SMsg::Release { token });
    }
}

// =====================================================================
// Storage
// =====================================================================

fn storage(total_writes: u32, fencing: bool) {
    let mut last_applied: u64 = 0;
    for _ in 0..total_writes {
        // Every granted client always writes, so exactly C*R writes
        // arrive and a blocking timed recv is safe.
        let w: WMsg = traceforge::recv_msg_block_timed();
        if fencing && w.token < last_applied {
            // Fencing check: reject stale tokens. (Tokens are unique,
            // so token >= last_applied is equivalent to strictly
            // greater here.)
            STALE_REJECTED.fetch_add(1, Ordering::Relaxed);
        } else {
            // THE safety property: applied tokens strictly increase.
            traceforge::assert(w.token > last_applied);
            APPLIED.fetch_add(1, Ordering::Relaxed);
            last_applied = w.token;
        }
        traceforge::send_msg(w.writer, CMsg::WriteAck);
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, b: Bounds) -> Config {
    let builder = Config::builder().with_progress_report(usize::MAX);
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
    }
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    grants: usize,
    expired: usize,
    released: usize,
    applied: usize,
    stale: usize,
}

fn read_counts() -> Counts {
    Counts {
        grants: GRANTS.load(Ordering::Relaxed),
        expired: EXPIRED.load(Ordering::Relaxed),
        released: RELEASED.load(Ordering::Relaxed),
        applied: APPLIED.load(Ordering::Relaxed),
        stale: STALE_REJECTED.load(Ordering::Relaxed),
    }
}

fn run(
    mode: Mode,
    num_clients: u32,
    b: Bounds,
    fencing: bool,
    rounds: u32,
) -> (Stats, Duration) {
    let cfg = build_config(mode, b);
    GRANTS.store(0, Ordering::Relaxed);
    EXPIRED.store(0, Ordering::Relaxed);
    RELEASED.store(0, Ordering::Relaxed);
    APPLIED.store(0, Ordering::Relaxed);
    STALE_REJECTED.store(0, Ordering::Relaxed);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let total_writes = num_clients * rounds;
        let storage_h = thread::spawn(move || storage(total_writes, fencing));
        let storage_tid = storage_h.thread().id();
        let service_h = thread::spawn(move || lock_service(main_tid, b.ttl, rounds));
        let service_tid = service_h.thread().id();
        let mut clients = Vec::new();
        for i in 0..num_clients {
            let pause = if i == 0 { b.pause } else { 0 };
            clients.push(thread::spawn(move || client(main_tid, pause, rounds)));
        }
        let client_ids: Vec<ThreadId> = clients.iter().map(|h| h.thread().id()).collect();
        // Send every Init before joining anything.
        traceforge::send_tagged_msg(service_tid, TAG_INIT, SMsg::Init { clients: client_ids });
        for h in &clients {
            traceforge::send_msg(
                h.thread().id(),
                CMsg::Init { service: service_tid, storage: storage_tid },
            );
        }
        for h in clients {
            let _ = h.join();
        }
        let _ = service_h.join();
        let _ = storage_h.join();
    });
    (stats, start.elapsed())
}

// =====================================================================
// Reporting
// =====================================================================

fn fencing_str(fencing: bool) -> &'static str {
    if fencing { "on" } else { "off" }
}

fn print_one(
    label: &str,
    num_clients: u32,
    b: Bounds,
    fencing: bool,
    rounds: u32,
    stats: &Stats,
    dur: Duration,
    c: Counts,
) {
    println!(
        "{label:<10} C={num_clients} R={rounds} fencing={f}  L={l} U={u} TTL={ttl} PAUSE={p} sd={sd}  execs={execs:<6} \
         blocked={block:<6} grants={grants:<6} expired={expired:<6} released={released:<6} \
         applied={applied:<6} stale={stale:<6} time={dur:?}",
        f = fencing_str(fencing), l = b.l, u = b.u, ttl = b.ttl, p = b.pause, sd = b.sd,
        execs = stats.execs, block = stats.block,
        grants = c.grants, expired = c.expired, released = c.released,
        applied = c.applied, stale = c.stale, dur = dur,
    );
}

fn print_compare(
    num_clients: u32,
    b: Bounds,
    fencing: bool,
    rounds: u32,
    baseline: (Stats, Duration),
    timed: (Stats, Duration),
    bc: Counts,
    tc: Counts,
) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("TTL lease lock with fencing tokens: MUST vs MUST-τ");
    println!("======================================================");
    println!(
        "C = {num_clients}    R = {rounds}    fencing = {}    L = {}    U = {}    TTL = {}    PAUSE = {}    sd = {}",
        fencing_str(fencing), b.l, b.u, b.ttl, b.pause, b.sd
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
        "grants/released/expired: baseline {}/{}/{}   timed {}/{}/{}",
        bc.grants, bc.released, bc.expired, tc.grants, tc.released, tc.expired
    );
    println!(
        "applied/stale_rejected: baseline {}/{}   timed {}/{}",
        bc.applied, bc.stale, tc.applied, tc.stale
    );
}

/// Vacuity guard: a run in which every interleaving blocked completed
/// zero executions and verified NOTHING; its exit 0 must not be read
/// as a hold (see the multi-round pauser re-entry note in the doc
/// header for the known way to hit this).
fn warn_if_vacuous(label: &str, execs: usize, blocked: usize) {
    if execs == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}): every interleaving \
             blocked, this run verified nothing; treat it as no data, not as a hold. \
             See the multi-round pauser re-entry note in the doc header."
        );
    }
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    u: u64,
    l_ratio: f64,
    ttl_ratio: f64,
    pause_ratio: f64,
    sd_ratio: f64,
    fencing: bool,
    clients: u32,
    rounds: u32,
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
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        ttl_ratio: DEFAULT_TTL_RATIO,
        pause_ratio: DEFAULT_PAUSE_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        fencing: true,
        clients: DEFAULT_CLIENTS,
        rounds: DEFAULT_ROUNDS,
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--clients" => a.clients = parse_num(next_val(&mut args, "--clients"), "--clients"),
            "--rounds" => a.rounds = parse_num(next_val(&mut args, "--rounds"), "--rounds"),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--ttl-ratio" => {
                a.ttl_ratio = parse_num(next_val(&mut args, "--ttl-ratio"), "--ttl-ratio")
            }
            "--pause-ratio" => {
                a.pause_ratio = parse_num(next_val(&mut args, "--pause-ratio"), "--pause-ratio")
            }
            "--fencing" => {
                let v = next_val(&mut args, "--fencing");
                a.fencing = match v.as_str() {
                    "on" => true,
                    "off" => false,
                    other => cli_bail(&format!("invalid --fencing: {other} (expected on|off)")),
                };
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: lease_timed [--mode baseline|timed|compare] [--clients C] [--rounds R] \
                     [--u U] [--l-ratio LR] [--sd-ratio SR] [--ttl-ratio TR] [--pause-ratio PR] \
                     [--fencing on|off]\n\
                     Defaults: C=2, R=1, U=1, L/U=0, sd/U=0, TTL/U=6, PAUSE/U=0, fencing=on.\n\
                     Client 0 is the pauser; clients i>0 never pause. Clients demand the lock\n\
                     concurrently (no staggering); the service queues and serves them in every\n\
                     explored order. See the doc header for the verdict matrix.\n\
                     Exit codes: 0 = hold, 101 = FIRE (assertion violation), 2 = CLI misuse."
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
    if a.clients < 1 {
        cli_bail("need at least 1 client");
    }
    if a.rounds < 1 {
        cli_bail("need at least 1 round");
    }
    if (a.clients as u64) * (a.rounds as u64) > 100_000 {
        cli_bail("clients * rounds too large for the token tag scheme");
    }
    let b = Bounds::from_ratios(a.u, a.l_ratio, a.ttl_ratio, a.pause_ratio, a.sd_ratio);
    match a.mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, a.clients, b, a.fencing, a.rounds);
            print_one("baseline", a.clients, b, a.fencing, a.rounds, &s, d, read_counts());
            warn_if_vacuous("baseline", s.execs, s.block);
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, a.clients, b, a.fencing, a.rounds);
            print_one("timed", a.clients, b, a.fencing, a.rounds, &s, d, read_counts());
            warn_if_vacuous("timed", s.execs, s.block);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, a.clients, b, a.fencing, a.rounds);
            let bc = read_counts();
            let timed = run(Mode::Timed, a.clients, b, a.fencing, a.rounds);
            let tc = read_counts();
            let (b_vac, t_vac) = (
                (baseline.0.execs, baseline.0.block),
                (timed.0.execs, timed.0.block),
            );
            print_compare(a.clients, b, a.fencing, a.rounds, baseline, timed, bc, tc);
            warn_if_vacuous("baseline", b_vac.0, b_vac.1);
            warn_if_vacuous("timed", t_vac.0, t_vac.1);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
}
