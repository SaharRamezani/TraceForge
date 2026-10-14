//! Differential check of the first-arrival reception discipline
//! (2026-10-13) against an independent brute-force reference.
//!
//! The programs are receive-only: one receiver (an initial sleep, then
//! 1-3 receives, blocking or with a finite wait) and 1-3 senders, each
//! sleeping and sending a few messages to it. Every send is lossy, the
//! transit window is `[l, u]`, the storage lifetime `sd`.
//!
//! The reference enumerates every drop pattern and every integer
//! arrival assignment (FIFO per sender), runs the receiver of each
//! timeline by depth-first search over the readings the paper's Recv
//! and Timeout rules allow, and collects the set of distinct
//! executions (drop pattern, what each receive returned, whether it
//! ended blocked). The engine must explore exactly that many
//! executions, each once, and none without a timeline.
//!
//! `TF_FA_RULE=old` selects the reference of the semantics before the
//! ruling (a receive passes over only the messages ordered before the
//! one it reads on the same channel), which is how the reference itself
//! was validated against the pre-ruling engine. The default is the
//! first-arrival rule: a receive reading `s` also passes over every
//! pending message that arrived earlier than `s`, and each passed-over
//! message must have been discarded when the wait began.
//!
//! `first_arrival_reference_smoke` runs a small batch in the suite;
//! `first_arrival_reference_campaign` is the long run (env TF_FA_SEED,
//! TF_FA_N), run with --ignored --nocapture.
use std::collections::BTreeSet;

use traceforge::thread;
use traceforge::{Config, ConsType, SchedulePolicy, WaitTime};

#[derive(Clone, Copy, Debug)]
enum Rcv {
    Block,
    Wait(u64),
}

#[derive(Clone, Debug)]
struct Prog {
    /// FIFO, Causal (no relays here: the same order as FIFO) or Bag
    /// (nothing is ordered: no arrival coupling, no ordered pass-over).
    cons: ConsType,
    l: u64,
    u: u64,
    sd: u64,
    d0: u64,
    rcvs: Vec<Rcv>,
    /// Per sender: the sleeps before each of its sends.
    senders: Vec<Vec<u64>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Rd {
    Msg(usize),
    Timeout,
    Blocked,
}

struct M {
    sender: usize,
    sigma: u64,
}

fn messages(p: &Prog) -> Vec<M> {
    let mut out = Vec::new();
    for (j, sl) in p.senders.iter().enumerate() {
        let mut t = 0;
        for &d in sl {
            t += d;
            out.push(M { sender: j, sigma: t });
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn explore(
    p: &Prog,
    msgs: &[M],
    alpha: &[Option<u64>],
    k: usize,
    tau: u64,
    consumed: &mut Vec<bool>,
    reads: &mut Vec<Rd>,
    out: &mut BTreeSet<Vec<Rd>>,
    first_arrival: bool,
) {
    if k == p.rcvs.len() {
        out.insert(reads.clone());
        return;
    }
    let n = msgs.len();
    let (inf, w) = match p.rcvs[k] {
        Rcv::Block => (true, 0),
        Rcv::Wait(w) => (false, w),
    };
    let mut any_read = false;
    for s in 0..n {
        if consumed[s] {
            continue;
        }
        let Some(a) = alpha[s] else { continue };
        let lo = tau.max(a);
        let hi = if inf { a + p.sd } else { (a + p.sd).min(tau + w) };
        if lo > hi {
            continue;
        }
        // Messages the read passes over must have been discarded when
        // the wait began.
        let mut ok = true;
        for b in 0..n {
            if b == s || consumed[b] {
                continue;
            }
            let Some(ab) = alpha[b] else { continue };
            let ordered_before =
                !matches!(p.cons, ConsType::Bag) && msgs[b].sender == msgs[s].sender && b < s;
            let arrived_first = first_arrival && ab < a;
            if (ordered_before || arrived_first) && ab + p.sd >= tau {
                ok = false;
                break;
            }
        }
        if !ok {
            continue;
        }
        any_read = true;
        consumed[s] = true;
        reads.push(Rd::Msg(s));
        for t2 in lo..=hi {
            explore(p, msgs, alpha, k + 1, t2, consumed, reads, out, first_arrival);
        }
        reads.pop();
        consumed[s] = false;
    }
    if inf {
        if !any_read {
            reads.push(Rd::Blocked);
            out.insert(reads.clone());
            reads.pop();
        }
    } else {
        // Timeout: no pending message is readable at any instant of
        // [tau, tau + w) (half-open: an arrival at the deadline may lose
        // the race to the timer).
        let miss = (0..n).all(|b| {
            consumed[b]
                || match alpha[b] {
                    None => true,
                    Some(ab) => ab + p.sd < tau || ab >= tau + w,
                }
        });
        if miss {
            reads.push(Rd::Timeout);
            explore(p, msgs, alpha, k + 1, tau + w, consumed, reads, out, first_arrival);
            reads.pop();
        }
    }
}

/// Distinct executions (drop pattern, readings) of `p`.
fn reference(p: &Prog, first_arrival: bool) -> BTreeSet<(Vec<bool>, Vec<Rd>)> {
    let msgs = messages(p);
    let n = msgs.len();
    let mut graphs: BTreeSet<(Vec<bool>, Vec<Rd>)> = BTreeSet::new();
    for mask in 0u32..(1 << n) {
        let delivered: Vec<bool> = (0..n).map(|i| mask >> i & 1 == 1).collect();
        // Arrival assignments: per message in [sigma + l, sigma + u],
        // non-decreasing along each sender.
        let mut alpha: Vec<Option<u64>> = vec![None; n];
        fn assign(
            p: &Prog,
            msgs: &[M],
            delivered: &[bool],
            i: usize,
            alpha: &mut Vec<Option<u64>>,
            graphs: &mut BTreeSet<(Vec<bool>, Vec<Rd>)>,
            first_arrival: bool,
        ) {
            if i == msgs.len() {
                let mut consumed = vec![false; msgs.len()];
                let mut reads = Vec::new();
                let mut out = BTreeSet::new();
                explore(
                    p,
                    msgs,
                    alpha,
                    0,
                    p.d0,
                    &mut consumed,
                    &mut reads,
                    &mut out,
                    first_arrival,
                );
                for r in out {
                    graphs.insert((delivered.to_vec(), r));
                }
                return;
            }
            if !delivered[i] {
                alpha[i] = None;
                assign(p, msgs, delivered, i + 1, alpha, graphs, first_arrival);
                return;
            }
            // FIFO: no earlier than the nearest delivered predecessor of
            // the same sender (a dropped one does not couple); a bag
            // couples nothing.
            let prev = if matches!(p.cons, ConsType::Bag) {
                0
            } else {
                (0..i)
                    .rev()
                    .filter(|&j| msgs[j].sender == msgs[i].sender)
                    .find_map(|j| alpha[j])
                    .unwrap_or(0)
            };
            for a in (msgs[i].sigma + p.l)..=(msgs[i].sigma + p.u) {
                if a < prev {
                    continue;
                }
                alpha[i] = Some(a);
                assign(p, msgs, delivered, i + 1, alpha, graphs, first_arrival);
            }
            alpha[i] = None;
        }
        assign(p, &msgs, &delivered, 0, &mut alpha, &mut graphs, first_arrival);
    }
    graphs
}

fn engine(p: &Prog, policy: SchedulePolicy, seed: u64) -> (usize, usize, usize) {
    let prog = p.clone();
    let stats = traceforge::verify(
        Config::builder()
            .with_policy(policy)
            .with_seed(seed)
            .with_cons_type(p.cons)
            .with_max_iterations(200_000)
            .with_timed(p.l, p.u, p.sd)
            .build(),
        move || {
            let rcvs = prog.rcvs.clone();
            let d0 = prog.d0;
            let receiver = thread::spawn(move || {
                if d0 > 0 {
                    traceforge::sleep(d0);
                }
                for r in rcvs {
                    match r {
                        Rcv::Block => {
                            let _: u32 = traceforge::recv_msg_block_timed();
                        }
                        Rcv::Wait(w) => {
                            let _: Option<u32> =
                                traceforge::recv_msg_timed(WaitTime::Finite(w));
                        }
                    }
                }
            });
            let r = receiver.thread().id();
            for (j, sl) in prog.senders.iter().enumerate() {
                let sl = sl.clone();
                thread::spawn(move || {
                    for (k, d) in sl.into_iter().enumerate() {
                        if d > 0 {
                            traceforge::sleep(d);
                        }
                        traceforge::send_msg(r, (10 * j + k) as u32);
                    }
                });
            }
        },
    );
    (stats.execs, stats.block, stats.timeline_impossible)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_prog(r: &mut Rng) -> Prog {
    const GRID: [(u64, u64, u64); 5] = [(0, 0, 0), (0, 1, 0), (0, 2, 1), (1, 3, 0), (0, 0, 2)];
    let (l, u, sd) = GRID[r.below(GRID.len() as u64) as usize];
    let cons = [ConsType::FIFO, ConsType::FIFO, ConsType::Bag, ConsType::Causal][r.below(4) as usize];
    let d0 = [0, 0, 1, 3, 6][r.below(5) as usize];
    let nr = 1 + r.below(3) as usize;
    let rcvs = (0..nr)
        .map(|_| {
            if r.below(3) == 0 {
                Rcv::Block
            } else {
                Rcv::Wait([1, 2, 5, 9][r.below(4) as usize])
            }
        })
        .collect();
    let ns = 1 + r.below(3) as usize;
    let mut senders: Vec<Vec<u64>> = Vec::new();
    let mut total = 0;
    for _ in 0..ns {
        if total >= 4 {
            break;
        }
        let m = (1 + r.below(2) as usize).min(4 - total);
        total += m;
        senders.push((0..m).map(|_| [0, 1, 2, 4, 6][r.below(5) as usize]).collect());
    }
    Prog { cons, l, u, sd, d0, rcvs, senders }
}

fn campaign(seed: u64, n: u64) {
    let old = std::env::var("TF_FA_RULE").map(|v| v == "old").unwrap_or(false);
    let mut rng = Rng(seed);
    let mut bad: Vec<String> = Vec::new();
    let mut shapes = 0usize;
    let mut differ = 0usize;
    for i in 0..n {
        let p = gen_prog(&mut rng);
        let refg = reference(&p, !old);
        if reference(&p, old) != refg {
            differ += 1; // the ruling changes this program's executions
        }
        let blocked = refg
            .iter()
            .filter(|(_, r)| r.last() == Some(&Rd::Blocked))
            .count();
        let want = (refg.len() - blocked, blocked, 0);
        for (policy, s) in [(SchedulePolicy::LTR, 0), (SchedulePolicy::Arbitrary, 13)] {
            let got = engine(&p, policy, s);
            if got != want {
                bad.push(format!(
                    "#{i} {policy:?}/{s}: engine (execs, block, impossible) = {got:?}, \
                     reference = {want:?}\n    {p:?}"
                ));
            }
        }
        shapes += 1;
    }
    println!(
        "first-arrival reference: {shapes} programs ({differ} on which the two rules differ), \
         {} disagreements",
        bad.len()
    );
    for b in &bad {
        println!("{b}");
    }
    assert!(bad.is_empty(), "{} disagreements with the reference", bad.len());
}

#[test]
fn first_arrival_reference_smoke() {
    let seed = std::env::var("TF_FA_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or(7);
    let n = std::env::var("TF_FA_N").ok().and_then(|v| v.parse().ok()).unwrap_or(40);
    campaign(seed, n);
}

#[test]
#[ignore]
fn first_arrival_reference_campaign() {
    let seed = std::env::var("TF_FA_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    let n = std::env::var("TF_FA_N").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    campaign(seed, n);
}
