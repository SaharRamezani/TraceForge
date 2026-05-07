//! Three-Phase Commit (3PC) — Skeen '81, ported with timeouts and
//! optional crash failures.
//!
//! Protocol (from D. Skeen, "NonBlocking Commit Protocols", SIGMOD '81;
//! TLA+ form: tlaplus/Examples/specifications/acp):
//!
//!   Phase 1  CanCommit / Vote
//!     coord  ── Prepare ──▶ all participants
//!     each p ── Yes/No  ──▶ coord
//!     coord aborts unless every vote is Yes (and every vote arrived)
//!
//!   Phase 2  PreCommit / Ack
//!     coord  ── PreCommit ──▶ all participants
//!     each p ── Ack       ──▶ coord
//!     coord refuses to commit unless every Ack arrived
//!
//!   Phase 3  DoCommit
//!     coord  ── Commit ──▶ all participants
//!
//! Receives that may legitimately fail to deliver (because the coord or
//! a participant is slow/crashed) use `recv_msg_timed(WaitTime::Finite(W))`
//! and treat `None` as a timeout — exactly the resilience design the
//! original 3PC paper specifies (a missing vote → abort; a missing ack →
//! don't commit; etc.).
//!
//! ## Bound selection
//!
//! Following the methodology used by MUST-τ — see the benchmark report
//! `benchmarks_decision.md` (§ 4.2) — we expose three first-class
//! parameters and pick their defaults from Skeen's own analysis:
//!
//!   * `U` — upper bound on per-message transit time. This is Skeen's
//!     Δ. Lower bound `L = 0`: the report's recommendation is that the
//!     pruning power comes from `U` being finite, not from `L > 0`, and
//!     real networks have negligible lower-bound transit.
//!   * `W` — per-receive wait window. Skeen's termination protocol
//!     bound is `W = 2·Δ` (two single-direction transits — one for the
//!     coord's broadcast, one for the reply). We expose `--w-ratio` so
//!     the eval can sweep `W/U ∈ {2, 3, 5}` and report the reduction
//!     curve, never a single hand-picked point.
//!   * `sd = 0` — participants do not buffer beyond receive; the
//!     storage-delay term is zero by Skeen's deployment assumption.
//!
//! Two pitfalls the report flags and we deliberately avoid:
//!
//!   * `L = U` (zero uncertainty) trivially maximises pruning but is a
//!     degenerate timed automaton — we always keep `L < U`.
//!   * `W < U` (timeout shorter than worst-case delivery) creates a
//!     configuration bug that MUST-τ would *correctly* report; that is
//!     a separate experiment, not a headline number, and the assertion
//!     in `main` rejects it for the canonical run.
//!
//! The participant staggering `DELTA = W + 1` is the *minimum* gap
//! that makes every cross-mapping (recv k reading from participant
//! j ≠ k) fall outside the recv window under Skeen's bounds:
//!
//!   send i  has arrival window  [i·DELTA, i·DELTA + U]
//!   recv k  has accept window   [k·DELTA, k·DELTA + W]
//!   for j = k+1: empty iff DELTA > W   ⇒  DELTA ≥ W + 1
//!   for j = k-1: empty iff DELTA > U   ⇒  DELTA ≥ U + 1   (subsumed)
//!
//! This is *not* a parameter of the protocol — it is a proof-side
//! choice that makes the canonical participant-to-recv pairing the
//! unique survivor under temporal pruning. The graph-count delta we
//! report is therefore an honest measurement of MUST-τ's filter, not a
//! by-product of widening DELTA past the necessary minimum.
//!
//! ## Modelling shortcut: untimed participant receives
//!
//! Participant receives use `recv_msg_block` (untimed) rather than
//! `recv_msg_block_timed`. The block_timed variant advances the
//! participant's local clock to the rf-window time on completion,
//! which converges all participants to the same broadcast-arrival τ
//! and erases the (i+1)·DELTA staggering — and with it the cross-
//! mapping pruning that gives MUST-τ its headline reduction.
//!
//! Crash-driven "timeout" semantics on the participant side are
//! modelled the way Skeen models them in the original protocol: as a
//! `nondet()`-gated short-circuit before the recv (only under
//! `--crashes`). Both runs (baseline and temporal) explore the same
//! crash branches, so the comparison is apples-to-apples.
//!
//! ## Properties checked
//!
//! Atomicity (no participant decides COMMIT while another decides
//! ABORT) is enforced at the participant by `assert(yes)` on receiving
//! `Commit`: the unanimity rule guarantees a Yes-voter cannot be
//! committed against without all other voters also having said Yes.
//! The buggy companion example (`three_pc_temporal_buggy.rs`) drops
//! this rule and the assertion fires.
//!
//! Skeen's full *non-blocking termination protocol* (electing a new
//! coordinator on suspected coord crash) and *coordinator-recovery
//! soundness* (recovered coord never overrides PRE_COMMIT) are not
//! modelled in this skeleton; they are listed in the benchmark report
//! as future work, since they sit on top of the same temporal-bound
//! framework as the headline reduction story.
//!
//! Usage:
//!
//!     # Single point (Skeen defaults: U=1, W=2·U=2, DELTA=W+1=3)
//!     cargo run --release --example three_pc_temporal -- --mode compare
//!     cargo run --release --example three_pc_temporal -- --mode temporal
//!
//!     # Override bounds (Skeen ratio = W/U)
//!     cargo run --release --example three_pc_temporal -- --u 1 --w-ratio 3
//!     cargo run --release --example three_pc_temporal -- --u 2 --w-ratio 2
//!
//!     # Sweep W/U ∈ {2, 3, 5} per the report's recommended axis
//!     cargo run --release --example three_pc_temporal -- --mode sweep
//!
//!     # Optional crash branches (gate them off the headline number)
//!     cargo run --release --example three_pc_temporal -- --mode compare --crashes

use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

const DEFAULT_PARTICIPANTS: u32 = 3;
/// Default upper transit bound (Skeen's Δ). Lower bound `L = 0`.
const DEFAULT_U: u64 = 1;
/// Default Skeen ratio: `W = 2·U`. Sweep range used by `--mode sweep`
/// is `{2, 3, 5, 7}`; see report (§ 4.2, "Specific picks").
const DEFAULT_W_RATIO: u64 = 2;
/// W/U ratios swept by `--mode sweep`. The report recommends
/// `{1.5, 2, 3, 5}`; we drop 1.5 because `W` is an integer here, and
/// add 7 so the sweep crosses the pruning cliff (`W ≥ DELTA`) — the
/// "loose" regime that the report explicitly asks us to include so
/// reviewers can see the reduction degrade gracefully.
const SWEEP_RATIOS: &[u64] = &[2, 3, 5, 7];

#[derive(Clone, Debug, PartialEq)]
enum CoordinatorMsg {
    Init(Vec<ThreadId>),
    Yes,
    No,
    Ack,
}

#[derive(Clone, Debug, PartialEq)]
enum ParticipantMsg {
    Prepare(ThreadId),
    PreCommit,
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Temporal,
}

/// Bound bundle.
///
/// * `u`     — Skeen's Δ (transit upper bound).
/// * `w`     — per-receive wait (Skeen default `2·u`).
/// * `delta` — participant-stagger sleep used by the model. For a
///   single-point run we pick the *minimum* `delta = w + 1` (tight
///   pruning); for a sweep we hold `delta` fixed at the largest swept
///   `w + 1` so the same encoded scenario is verified across every
///   `W/U` ratio — that's what makes the reduction-vs-W curve an
///   honest comparison rather than three different scenarios.
#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    w: u64,
    delta: u64,
}

impl Bounds {
    /// Single-point bounds: tight stagger `delta = w + 1`.
    fn from_ratio(u: u64, w_ratio: u64) -> Self {
        assert!(u >= 1, "U must be >= 1 (L=0 < U)");
        assert!(
            w_ratio >= 2,
            "W/U ratio must be >= 2 (Skeen's termination protocol bound is 2·Δ); use a separate \
             misconfiguration experiment if you want W < 2·U"
        );
        let w = u.checked_mul(w_ratio).expect("W overflow");
        let delta = w.checked_add(1).expect("DELTA overflow");
        Self { u, w, delta }
    }

    /// Sweep-point bounds: caller-provided `delta` (typically held
    /// fixed across the sweep so each ratio uses the same scenario).
    fn with_delta(u: u64, w_ratio: u64, delta: u64) -> Self {
        assert!(u >= 1, "U must be >= 1 (L=0 < U)");
        let w = u.checked_mul(w_ratio).expect("W overflow");
        Self { u, w, delta }
    }
}

fn coordinator(mode: Mode, b: Bounds, num_ps: usize, crashes: bool) {
    let ps: Vec<ThreadId> = match traceforge::recv_msg_block::<CoordinatorMsg>() {
        CoordinatorMsg::Init(ids) => ids,
        _ => panic!("expected Init"),
    };
    let me = thread::current().id();

    // Phase 1: CanCommit -> Vote
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Prepare(me));
    }

    let mut yes_count = 0usize;
    let mut received_count = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(b.delta);
        }
        let v: Option<CoordinatorMsg> = match mode {
            Mode::Baseline => Some(traceforge::recv_msg_block()),
            Mode::Temporal => traceforge::recv_msg_timed(WaitTime::Finite(b.w)),
        };
        match v {
            Some(CoordinatorMsg::Yes) => {
                received_count += 1;
                yes_count += 1;
            }
            Some(CoordinatorMsg::No) => {
                received_count += 1;
            }
            // Timeout: vote did not arrive within W. Skeen's rule says
            // unanimity must include all-arrived, so a single timeout
            // forces abort.
            None => {}
            Some(_) => panic!("expected vote"),
        }
    }

    let _ = num_ps;
    let unanimous = received_count == ps.len() && yes_count == ps.len();

    if !unanimous {
        for id in &ps {
            traceforge::send_msg(*id, ParticipantMsg::Abort);
        }
        return;
    }

    // Coord may crash here, before broadcasting PreCommit. Mirrors the
    // 3PC reference's `crashes_enabled && nondet() return` branch.
    if crashes && traceforge::nondet() {
        return;
    }

    // Phase 2: PreCommit -> Ack
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::PreCommit);
    }

    let mut acks_received = 0usize;
    for _ in 0..ps.len() {
        if mode == Mode::Temporal {
            traceforge::sleep(b.delta);
        }
        let v: Option<CoordinatorMsg> = match mode {
            Mode::Baseline => Some(traceforge::recv_msg_block()),
            Mode::Temporal => traceforge::recv_msg_timed(WaitTime::Finite(b.w)),
        };
        match v {
            Some(CoordinatorMsg::Ack) => acks_received += 1,
            None => {} // ack timed out — leaves the round uncommitted
            Some(_) => panic!("expected ack"),
        }
    }

    if acks_received != ps.len() {
        // Missing ack(s): cannot safely commit. Don't send Commit.
        // Participants past PreCommit will block waiting for Commit and
        // the model checker captures the resulting blocked execution.
        return;
    }

    // Phase 3: DoCommit
    for id in &ps {
        traceforge::send_msg(*id, ParticipantMsg::Commit);
    }
}

fn participant(mode: Mode, b: Bounds, num_ps: u32, index: u32, crashes: bool) {
    let cid = match traceforge::recv_msg_block::<ParticipantMsg>() {
        ParticipantMsg::Prepare(id) => id,
        _ => panic!("expected Prepare"),
    };

    if crashes && traceforge::nondet() {
        return;
    }

    if mode == Mode::Temporal {
        traceforge::sleep(b.delta * (index as u64 + 1));
    }

    let yes = traceforge::nondet();
    let vote = if yes {
        CoordinatorMsg::Yes
    } else {
        CoordinatorMsg::No
    };
    traceforge::send_msg(cid, vote);

    // Untimed blocking recv: see the "Modelling shortcut" comment at
    // the top of the file. Untimed recv preserves the staggering that
    // the cross-mapping pruning relies on; participant timeout
    // semantics are folded into the `--crashes` short-circuit above.
    let action: ParticipantMsg = traceforge::recv_msg_block();

    match action {
        ParticipantMsg::Abort => return,
        ParticipantMsg::PreCommit => (),
        _ => panic!("expected PreCommit or Abort"),
    }

    if crashes && traceforge::nondet() {
        return;
    }

    if mode == Mode::Temporal {
        traceforge::sleep(b.delta * num_ps as u64);
    }
    traceforge::send_msg(cid, CoordinatorMsg::Ack);

    let action: ParticipantMsg = traceforge::recv_msg_block();
    match action {
        // Atomicity: a participant that voted No must never observe
        // Commit. Holds under Skeen's unanimity rule.
        ParticipantMsg::Commit => assert!(yes),
        _ => panic!("expected Commit"),
    }
}

fn build_config(mode: Mode, b: Bounds) -> Config {
    match mode {
        Mode::Baseline => Config::builder().build(),
        Mode::Temporal => Config::builder().with_temporal(0, b.u, 0).build(),
    }
}

fn run(mode: Mode, num_ps: u32, b: Bounds, crashes: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, b);
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let c = thread::spawn(move || coordinator(mode, b, num_ps as usize, crashes));

        let mut ps = Vec::new();
        for i in 0..num_ps {
            ps.push(thread::spawn(move || {
                participant(mode, b, num_ps, i, crashes)
            }));
        }
        traceforge::send_msg(
            c.thread().id(),
            CoordinatorMsg::Init(ps.iter().map(|h| h.thread().id()).collect()),
        );
    });
    (stats, start.elapsed())
}

fn print_one(label: &str, num_ps: u32, b: Bounds, stats: &Stats, dur: Duration, crashes: bool) {
    let crash_tag = if crashes { " (crashes)" } else { "" };
    println!(
        "{label:<10}{crash_tag} N={num_ps}  L=0 U={u} W={w} (W/U={r})  execs={execs:<6} \
         blocked={block:<6} time={dur:?}",
        u = b.u,
        w = b.w,
        r = b.w / b.u,
        execs = stats.execs,
        block = stats.block,
        dur = dur,
    );
}

fn print_compare(
    num_ps: u32,
    b: Bounds,
    crashes: bool,
    baseline: (Stats, Duration),
    temporal: (Stats, Duration),
) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = temporal;

    println!();
    println!(
        "Three-Phase Commit (Skeen '81, with timeouts{}): MUST vs MUST-τ",
        if crashes { " + crashes" } else { "" }
    );
    println!("======================================================");
    println!("N = {num_ps}    L = 0    U = {}    W = {} (= {}·U)", b.u, b.w, b.w / b.u);
    println!("source: Skeen '81 termination protocol bound (W = 2·Δ; see report § 4.2)");
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
        "temporal", t_stats.execs, t_stats.block, t_dur
    );
    println!();
    let exec_ratio = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!("execs reduction: {exec_ratio:.2}x  (= baseline / temporal)");
    println!("time  speedup  : {time_ratio:.2}x");
}

fn print_sweep(
    num_ps: u32,
    u: u64,
    delta: u64,
    crashes: bool,
    rows: &[(u64, Bounds, Stats, Duration, Stats, Duration)],
) {
    println!();
    println!(
        "Three-Phase Commit (Skeen '81): MUST vs MUST-τ across W/U sweep{}",
        if crashes { " + crashes" } else { "" }
    );
    println!("====================================================================");
    println!("N = {num_ps}    L = 0    U = {u}    DELTA = {delta} (held fixed across the sweep)");
    println!("sweep variable: W/U ∈ {SWEEP_RATIOS:?}    source: report § 4.2");
    println!("note: ratios with W < DELTA are pruning-active; W ≥ DELTA crosses the cliff");
    println!();
    println!(
        "{:<6} {:<6} {:<8} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "ratio", "W", "regime", "base.exec", "temp.exec", "base.blk", "temp.blk", "× exec"
    );
    for (ratio, b, b_stats, _b_dur, t_stats, _t_dur) in rows {
        let regime = if b.w < delta { "tight" } else { "loose" };
        let r = b_stats.execs as f64 / t_stats.execs.max(1) as f64;
        println!(
            "{:<6} {:<6} {:<8} {:>10} {:>10} {:>10} {:>10} {:>7.2}x",
            ratio, b.w, regime, b_stats.execs, t_stats.execs, b_stats.block, t_stats.block, r
        );
    }
    println!();
    println!("Read-out: the reduction curve (× exec column) is what the eval");
    println!("section plots — the slope, not the headline number, is the");
    println!("evidence that MUST-τ's pruning scales with timing-constraint");
    println!("strength rather than with a cherry-picked single point.");
}

fn parse_args() -> (String, u32, u64, u64, bool) {
    let mut mode = String::from("compare");
    let mut num_ps = DEFAULT_PARTICIPANTS;
    let mut u = DEFAULT_U;
    let mut w_ratio = DEFAULT_W_RATIO;
    let mut crashes = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = args
                    .next()
                    .unwrap_or_else(|| panic!("--mode requires a value"));
            }
            "--participants" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--participants requires a value"));
                num_ps = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --participants: {v}"));
            }
            "--u" => {
                let v = args.next().unwrap_or_else(|| panic!("--u requires a value"));
                u = v.parse().unwrap_or_else(|_| panic!("invalid --u: {v}"));
            }
            "--w-ratio" => {
                let v = args
                    .next()
                    .unwrap_or_else(|| panic!("--w-ratio requires a value"));
                w_ratio = v
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid --w-ratio: {v}"));
            }
            "--crashes" => crashes = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: three_pc_temporal [--mode baseline|temporal|compare|sweep] \
                     [--participants N] [--u U] [--w-ratio R] [--crashes]\n\
                     \n\
                     Defaults: U=1, W/U ratio=2 (Skeen's termination protocol bound).\n\
                     Sweep mode runs W/U ∈ {{2, 3, 5}} per the benchmark report § 4.2."
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (mode, num_ps, u, w_ratio, crashes)
}

fn main() {
    let (mode_str, num_ps, u, w_ratio, crashes) = parse_args();
    assert!(num_ps >= 1, "need at least 1 participant");

    match mode_str.as_str() {
        "baseline" => {
            let b = Bounds::from_ratio(u, w_ratio);
            let (s, d) = run(Mode::Baseline, num_ps, b, crashes);
            print_one("baseline", num_ps, b, &s, d, crashes);
        }
        "temporal" => {
            let b = Bounds::from_ratio(u, w_ratio);
            let (s, d) = run(Mode::Temporal, num_ps, b, crashes);
            print_one("temporal", num_ps, b, &s, d, crashes);
        }
        "compare" => {
            let b = Bounds::from_ratio(u, w_ratio);
            let baseline = run(Mode::Baseline, num_ps, b, crashes);
            let temporal = run(Mode::Temporal, num_ps, b, crashes);
            print_compare(num_ps, b, crashes, baseline, temporal);
        }
        "sweep" => {
            // Hold DELTA fixed across every row so the reduction-vs-W
            // curve is attributable to the temporal filter, not to a
            // changing staggering structure. We pick DELTA at the
            // middle of the swept ratios so the sweep visibly crosses
            // the pruning cliff (`W < DELTA`, tight regime; `W ≥ DELTA`,
            // loose regime where cross-mappings start surviving). The
            // report explicitly asks for both regimes — the "loose"
            // rows are what proves the reduction is real and not a
            // cherry-picked headline number.
            let mid_idx = SWEEP_RATIOS.len() / 2;
            let delta = u * SWEEP_RATIOS[mid_idx];
            let mut rows = Vec::new();
            for &r in SWEEP_RATIOS {
                let b = Bounds::with_delta(u, r, delta);
                let baseline = run(Mode::Baseline, num_ps, b, crashes);
                let temporal = run(Mode::Temporal, num_ps, b, crashes);
                rows.push((r, b, baseline.0, baseline.1, temporal.0, temporal.1));
            }
            print_sweep(num_ps, u, delta, crashes, &rows);
        }
        other => panic!("invalid --mode: {other} (expected baseline|temporal|compare|sweep)"),
    }
}
