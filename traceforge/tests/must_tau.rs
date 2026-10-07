//! Must-tau (2026-09-29): Must's Algorithm 1 with the full timed system
//! as its consistency check. Every send of a timed program may be
//! dropped (no budget), a dropped send is the canonical outcome of a
//! send, and a receive's canonical outcome is judged in Must's Previous
//! set. Expected: no explored graph lacks a timeline
//! (`timeline_impossible == 0`), and every scheduler finds the same
//! executions, each once.
//!
//! Each program is run under LTR and several Arbitrary seeds. The
//! pinned counts were cross-checked graph by graph against the union,
//! over schedulers, of the pre-Must-tau exploration of the same lossy
//! program.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::{EndCondition, ExecutionEnd, Monitor, MonitorResult};
use traceforge::thread;
use traceforge::thread::ThreadId;
use traceforge::{Config, ConsType, CoverageInfo, ExecutionId, Nondet, SchedulePolicy, Stats, Val, WaitTime};

type Outcomes = BTreeMap<String, usize>;

struct Collector(Arc<Mutex<Vec<String>>>);

impl ExecutionObserver for Collector {
    fn after(&mut self, _eid: ExecutionId, cond: &EndCondition, c: CoverageInfo) {
        let mut goals: Vec<String> = c.coverage.keys().cloned().collect();
        goals.sort();
        self.0.lock().unwrap().push(format!("{cond:?} {goals:?}"));
    }
}

fn run_one<F>(
    policy: SchedulePolicy,
    seed: u64,
    timing: (u64, u64, u64),
    cons: ConsType,
    f: F,
) -> (Stats, Outcomes)
where
    F: Fn() + Send + Sync + 'static,
{
    run_cfg(policy, seed, timing, cons, false, f)
}

/// `keep_going`: a failed `traceforge::assert` is recorded and the
/// exploration goes on, instead of ending the run.
fn run_cfg<F>(
    policy: SchedulePolicy,
    seed: u64,
    timing: (u64, u64, u64),
    cons: ConsType,
    keep_going: bool,
    f: F,
) -> (Stats, Outcomes)
where
    F: Fn() + Send + Sync + 'static,
{
    let sink = Arc::new(Mutex::new(Vec::new()));
    let verbose = std::env::var("TF_MT_VERBOSE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(timing.0, timing.1, timing.2)
            .with_cons_type(cons)
            .with_policy(policy)
            .with_seed(seed)
            .with_verbose(verbose)
            .with_keep_going_after_error(keep_going)
            .with_callback(Box::new(Collector(Arc::clone(&sink))))
            .build(),
        f,
    );
    let mut outcomes = Outcomes::new();
    for o in sink.lock().unwrap().iter() {
        *outcomes.entry(o.clone()).or_insert(0) += 1;
    }
    (stats, outcomes)
}

/// Runs `f` under LTR and `seeds` Arbitrary seeds and checks that every
/// run counts `expected` (executions, blocked), explores no graph
/// without a timeline, and reports the same outcomes.
fn check<F>(name: &str, timing: (u64, u64, u64), cons: ConsType, expected: (usize, usize), f: F)
where
    F: Fn() + Clone + Send + Sync + 'static,
{
    check_cfg(name, timing, cons, expected, false, f)
}

fn check_cfg<F>(
    name: &str,
    timing: (u64, u64, u64),
    cons: ConsType,
    expected: (usize, usize),
    keep_going: bool,
    f: F,
) where
    F: Fn() + Clone + Send + Sync + 'static,
{
    let seeds: u64 = std::env::var("TF_MT_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    let mut runs = vec![(SchedulePolicy::LTR, 0u64)];
    runs.extend((0..seeds).map(|s| (SchedulePolicy::Arbitrary, s)));
    let mut first: Option<Outcomes> = None;
    for (policy, seed) in runs {
        let (st, outcomes) = run_cfg(policy, seed, timing, cons, keep_going, f.clone());
        println!(
            "{name} {policy:?} seed={seed}: execs={} block={} timeline_impossible={} pruned={}",
            st.execs, st.block, st.timeline_impossible, st.pruned
        );
        assert_eq!(
            (st.execs, st.block),
            expected,
            "{name} {policy:?} seed={seed}: (execs, block)"
        );
        assert_eq!(st.timeline_impossible, 0, "{name} {policy:?} seed={seed}: a graph without a timeline was explored");
        match &first {
            None => first = Some(outcomes),
            Some(o) => assert_eq!(o, &outcomes, "{name} {policy:?} seed={seed}: outcomes differ from LTR"),
        }
    }
}

// ---------------------------------------------------------------------
// The retransmission counterexample of the timeout kill (2026-09-29).
// L=0 U=2 sd=1 FIFO. R: sleep 3; blocking recv DATA; send ACK to S.
// S: send f to R; recv ACK, wait 6; on timeout send the retransmission
// x to R. The execution "f delivered but gone before R listens, S times
// out, R reads x" is launched by R <- x; with a DELIVERED canonical ack
// its only launcher had the ack inside S's wait (no timeline), so the
// kill lost it. With the dropped canonical send the launcher is live.

const ID: u32 = 7;
const DATA: u32 = 1;
const ACK: u32 = 2;
const Y: u32 = 3;

fn retransmission(r_first: bool) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let body_r = |sid: traceforge::thread::ThreadId| {
            traceforge::sleep(3);
            let v: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(DATA));
            traceforge::cover!(format!("R={v}"));
            traceforge::send_tagged_msg(sid, ACK, 99u32);
        };
        let body_s = |rid: traceforge::thread::ThreadId| {
            traceforge::send_tagged_msg(rid, DATA, 1u32);
            let a: Option<u32> =
                traceforge::recv_tagged_msg_timed(|_, t| t == Some(ACK), WaitTime::Finite(6));
            traceforge::cover!(format!("S={a:?}"));
            if a.is_none() {
                traceforge::send_tagged_msg(rid, DATA, 2u32);
            }
        };
        let take_id = || -> traceforge::thread::ThreadId {
            traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(ID))
        };
        let (first, second) = if r_first {
            let r = thread::spawn(move || body_r(take_id()));
            let s = thread::spawn(move || body_s(take_id()));
            (r, s)
        } else {
            let s = thread::spawn(move || body_s(take_id()));
            let r = thread::spawn(move || body_r(take_id()));
            (s, r)
        };
        let (a, b) = (first.thread().id(), second.thread().id());
        traceforge::send_tagged_msg_timed(a, ID, b, 0, 0);
        traceforge::send_tagged_msg_timed(b, ID, a, 0, 0);
        let _ = first.join();
        let _ = second.join();
    }
}

/// The retransmission program plus a thread Q waiting for a Y message:
/// main sends it y0, and S sends it y on its timeout besides x. "Q reads
/// y" is launched by a revisit that can delete R's events.
fn retransmission_with_q(r_first: bool) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let body_r = |sid: traceforge::thread::ThreadId| {
            traceforge::sleep(3);
            let v: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(DATA));
            traceforge::cover!(format!("R={v}"));
            traceforge::send_tagged_msg(sid, ACK, 99u32);
        };
        let body_s = |(rid, qid): (traceforge::thread::ThreadId, traceforge::thread::ThreadId)| {
            traceforge::send_tagged_msg(rid, DATA, 1u32);
            let a: Option<u32> =
                traceforge::recv_tagged_msg_timed(|_, t| t == Some(ACK), WaitTime::Finite(6));
            if a.is_none() {
                traceforge::send_tagged_msg(rid, DATA, 2u32);
                traceforge::send_tagged_msg(qid, Y, 20u32);
            }
        };
        let q = thread::spawn(move || {
            let v: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(Y));
            traceforge::cover!(format!("Q={v}"));
        });
        let (r, s) = if r_first {
            let r = thread::spawn(move || {
                body_r(traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(ID)))
            });
            let s = thread::spawn(move || {
                body_s(traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(ID)))
            });
            (r, s)
        } else {
            let s = thread::spawn(move || {
                body_s(traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(ID)))
            });
            let r = thread::spawn(move || {
                body_r(traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(ID)))
            });
            (r, s)
        };
        let (rid, sid, qid) = (r.thread().id(), s.thread().id(), q.thread().id());
        traceforge::send_tagged_msg_timed(rid, ID, sid, 0, 0);
        traceforge::send_tagged_msg_timed(sid, ID, (rid, qid), 0, 0);
        traceforge::send_tagged_msg(qid, Y, 10u32);
        let _ = r.join();
        let _ = s.join();
        let _ = q.join();
    }
}

/// A GC refusal as the only launcher (adversarial review, 2026-09-29).
/// L=0 U=2 sd=1 FIFO. A: send u to S. B: send y to S, window [0,0].
/// S: recv (u or y); send b to R; recv; sleep 4; send s to R.
/// R: sleep 5; recv (b or s). "S reads u then y, R reads s" is launched
/// by R <- s from the world where R refuses b (reading y forces b dead
/// before R listens); the pre-Must-tau exploration never let a refusal
/// launch and found that execution only under some schedulers.
fn refusal_launcher() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            traceforge::sleep(5);
            let v: u32 = traceforge::recv_msg_block_timed();
            traceforge::cover!(format!("R={v}"));
        });
        let rid = r.thread().id();
        let s = thread::spawn(move || {
            let a: u32 = traceforge::recv_msg_block_timed();
            traceforge::send_msg(rid, 10u32);
            let b: u32 = traceforge::recv_msg_block_timed();
            traceforge::cover!(format!("S={a},{b}"));
            traceforge::sleep(4);
            traceforge::send_msg(rid, 20u32);
        });
        let sid = s.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg(sid, 1u32));
        let _ = thread::spawn(move || traceforge::send_msg_timed(sid, 2u32, 0, 0));
    }
}

/// A blocking receive whose only candidate is readable at its visit but
/// not in Previous of a later revisit (P's timeout forces c early), so
/// its canonical outcome there is the refusal. L=0 U=2 sd=0 FIFO.
fn refusal_in_previous(order: u8) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let c_body = || {
            traceforge::sleep(4);
            let v: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(0));
            traceforge::cover!(format!("C={v}"));
        };
        let q_body = || {
            let v: Option<u32> =
                traceforge::recv_tagged_msg_timed(|_, t| t == Some(3), WaitTime::Finite(10));
            traceforge::cover!(format!("Q={v:?}"));
        };
        let (c, q) = if order % 2 == 0 {
            let c = thread::spawn(c_body);
            let q = thread::spawn(q_body);
            (c, q)
        } else {
            let q = thread::spawn(q_body);
            let c = thread::spawn(c_body);
            (c, q)
        };
        let (cid, qid) = (c.thread().id(), q.thread().id());
        let p = thread::spawn(move || {
            let _g: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(2));
            traceforge::send_tagged_msg(cid, 0, 1u32);
            let a: Option<u32> =
                traceforge::recv_tagged_msg_timed(|_, t| t == Some(1), WaitTime::Finite(1));
            traceforge::cover!(format!("P={a:?}"));
            if a.is_none() {
                traceforge::send_tagged_msg(qid, 3, 3u32);
            }
        });
        let pid = p.thread().id();
        if order / 2 == 0 {
            let _ = thread::spawn(move || traceforge::send_tagged_msg(pid, 2, 2u32));
            let _ = thread::spawn(move || traceforge::send_tagged_msg_timed(pid, 1, 9u32, 2, 2));
        } else {
            let _ = thread::spawn(move || traceforge::send_tagged_msg_timed(pid, 1, 9u32, 2, 2));
            let _ = thread::spawn(move || traceforge::send_tagged_msg(pid, 2, 2u32));
        }
    }
}

/// The 2026-09-28 drawing: R waits [0,5]; S_b arrives in [0,4], S_s is
/// sent after sleep(2) and arrives in [2,6]. L=0 U=4 sd=0.
fn drawing(b_first: bool) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("R={v:?}"));
        });
        let rid = r.thread().id();
        let sb = move || traceforge::send_msg(rid, 1u32);
        let ss = move || {
            traceforge::sleep(2);
            traceforge::send_msg(rid, 2u32);
        };
        let (t1, t2) = if b_first {
            (thread::spawn(sb), thread::spawn(ss))
        } else {
            (thread::spawn(ss), thread::spawn(sb))
        };
        let _ = r.join();
        let _ = t1.join();
        let _ = t2.join();
    }
}

/// Two receives, each waiting [0,5] (L=0 U=4 sd=0): r may read s (sent
/// at 0) or the late x; r2 may read the late y or time out.
fn two_receives() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("r={v:?}"));
        });
        let r2 = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("r2={v:?}"));
        });
        let rid = r.thread().id();
        let r2id = r2.thread().id();
        let x = thread::spawn(move || {
            traceforge::sleep(2);
            traceforge::send_msg(rid, 10u32);
        });
        let y = thread::spawn(move || {
            traceforge::sleep(2);
            traceforge::send_msg(r2id, 20u32);
        });
        let s = thread::spawn(move || traceforge::send_msg(rid, 11u32));
        let _ = r.join();
        let _ = r2.join();
        let _ = x.join();
        let _ = y.join();
        let _ = s.join();
    }
}

/// Five senders with windows [0,1] .. [4,5] to a receiver waiting [0,5]
/// (L=0 U=4 sd=0): each delivered one alone rules the timeout out.
fn five_candidates() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("R={v:?}"));
        });
        let rid = r.thread().id();
        let senders: Vec<_> = (0..5u64)
            .map(|i| thread::spawn(move || traceforge::send_msg_timed(rid, i as u32, i, i + 1)))
            .collect();
        let _ = r.join();
        for t in senders {
            let _ = t.join();
        }
    }
}

/// FIFO, one sender: m1 (window [3,8]) then m2 (window [0,4]); R waits
/// [0,5]. Coupling a_m1 <= a_m2 <= 4 rules out R missing both when both
/// are delivered. L=0 U=4 sd=0.
fn fifo_two_sends() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("R={v:?}"));
        });
        let rid = r.thread().id();
        let s = thread::spawn(move || {
            traceforge::send_msg_timed(rid, 1u32, 3, 8);
            traceforge::send_msg_timed(rid, 2u32, 0, 4);
        });
        let _ = r.join();
        let _ = s.join();
    }
}

/// A timeout made impossible by a later READ choice, not by a send (fuzz
/// program 612). L=0 U=2 sd=1 FIFO. R: r0 = recv_timed(2), then a
/// blocking r1. S: m1 at 0, sleep 3, m2 at 3.
fn choice_killed() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let a: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(2));
            let b: u32 = traceforge::recv_msg_block_timed();
            traceforge::cover!(format!("r0={a:?} r1={b}"));
        });
        let rid = r.thread().id();
        let s = thread::spawn(move || {
            traceforge::send_msg(rid, 1u32);
            traceforge::sleep(3);
            traceforge::send_msg(rid, 2u32);
        });
        let _ = r.join();
        let _ = s.join();
    }
}

/// Reads are not eager across senders (a documented property of the
/// timed system, not of Must-tau): R waits from 0, x (one sender)
/// arrives at 1 and stays until 6, s (another sender) arrives at 3.
/// "R reads s" is an execution; a timeout is not (x was readable).
/// L=0 U=0 sd=5 FIFO.
fn cross_sender(blocking: bool) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(move || {
            if blocking {
                let v: u32 = traceforge::recv_msg_block_timed();
                traceforge::cover!(format!("R={v}"));
            } else {
                let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
                traceforge::cover!(format!("R={v:?}"));
            }
        });
        let rid = r.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg_timed(rid, 1u32, 1, 1));
        let _ = thread::spawn(move || {
            traceforge::sleep(3);
            traceforge::send_msg_timed(rid, 2u32, 0, 0);
        });
    }
}

// ---------------------------------------------------------------------
// Pinned counts (executions, blocked). Every send is lossy, so blocked
// endings include threads that never receive a lost message (e.g. a
// lost thread-id handshake). Each pin equals, graph for graph, the
// union over LTR and 12 Arbitrary seeds of the pre-Must-tau exploration
// of the same lossy program, which also explored and discarded 2 to 30
// timeline-impossible endings per run on these programs.

#[test]
fn retransmission_r_first() {
    check("retransmission(R first)", (0, 2, 1), ConsType::FIFO, EXPECT_RETRANS, retransmission(true));
}

#[test]
fn retransmission_s_first() {
    check("retransmission(S first)", (0, 2, 1), ConsType::FIFO, EXPECT_RETRANS, retransmission(false));
}

#[test]
fn retransmission_with_q_both_orders() {
    for r_first in [true, false] {
        check("retransmission_with_q", (0, 2, 1), ConsType::FIFO, EXPECT_Q, retransmission_with_q(r_first));
    }
}

#[test]
fn refusal_launcher_every_scheduler() {
    check("refusal_launcher", (0, 2, 1), ConsType::FIFO, EXPECT_REFUSAL_LAUNCHER, refusal_launcher());
}

#[test]
fn refusal_in_previous_all_orders() {
    for order in 0..4u8 {
        check("refusal_in_previous", (0, 2, 0), ConsType::FIFO, EXPECT_REFUSAL_PREVIOUS, refusal_in_previous(order));
    }
}

#[test]
fn drawing_both_orders() {
    for b_first in [true, false] {
        check("drawing", (0, 4, 0), ConsType::FIFO, EXPECT_DRAWING, drawing(b_first));
    }
}

#[test]
fn two_receives_pinned() {
    check("two_receives", (0, 4, 0), ConsType::FIFO, EXPECT_TWO_RECEIVES, two_receives());
}

#[test]
fn five_candidates_pinned() {
    check("five_candidates", (0, 4, 0), ConsType::FIFO, EXPECT_FIVE, five_candidates());
}

#[test]
fn fifo_two_sends_pinned() {
    check("fifo_two_sends", (0, 4, 0), ConsType::FIFO, EXPECT_FIFO_TWO, fifo_two_sends());
}

#[test]
fn choice_killed_pinned() {
    check("choice_killed", (0, 2, 1), ConsType::FIFO, EXPECT_CHOICE_KILLED, choice_killed());
}

#[test]
fn cross_sender_reads_not_eager() {
    check("cross_sender(finite)", (0, 0, 5), ConsType::FIFO, EXPECT_CROSS_FINITE, cross_sender(false));
    check("cross_sender(blocking)", (0, 0, 5), ConsType::FIFO, EXPECT_CROSS_BLOCKING, cross_sender(true));
}

const EXPECT_RETRANS: (usize, usize) = (7, 8);
const EXPECT_Q: (usize, usize) = (25, 41);
const EXPECT_REFUSAL_LAUNCHER: (usize, usize) = (4, 13);
const EXPECT_REFUSAL_PREVIOUS: (usize, usize) = (3, 12);
const EXPECT_DRAWING: (usize, usize) = (6, 0);
const EXPECT_TWO_RECEIVES: (usize, usize) = (18, 0);
const EXPECT_FIVE: (usize, usize) = (82, 0);
const EXPECT_FIFO_TWO: (usize, usize) = (5, 0);
const EXPECT_CHOICE_KILLED: (usize, usize) = (4, 2);
const EXPECT_CROSS_FINITE: (usize, usize) = (5, 0);
const EXPECT_CROSS_BLOCKING: (usize, usize) = (4, 1);

// ---------------------------------------------------------------------
// The shared-pool explorer (`with_parallel(true)`) hands the graph of a
// backward revisit to another worker, so that graph never passes through
// the test `try_revisit` makes on a graph it installs. Until 2026-10-05
// it was not tested at all: the worker explored it to its end, and the
// ending reached the observers although it counted as nothing
// (`timeline_impossible` 1 on both programs below).

/// R: r1 = recv_timed(5); r2 = recv_timed(5). S: send e to R.
/// L=U=0 sd=10 FIFO. When both receives time out before e is added, the
/// revisit "r2 reads e" has no timeline: e arrives at 0 and is stored
/// until 10, so r1 could not have timed out at 5.
fn two_timeouts_one_send() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let a: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            let b: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("r1={a:?} r2={b:?}"));
        });
        let rid = r.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg(rid, 1u32));
    }
}

/// The same shape with a send that exists only after the second timeout.
/// R: r1 = recv_timed(3); r2 = recv_timed(8), on a timeout send to main.
/// S: send e to R. L=1 U=2 sd=1 FIFO.
fn timeout_then_report() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let main_id = thread::current_id();
        let r = thread::spawn(move || {
            let a: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(3));
            let b: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(8));
            traceforge::cover!(format!("r1={a:?} r2={b:?}"));
            if b.is_none() {
                traceforge::send_msg(main_id, 9u32);
            }
        });
        let rid = r.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg(rid, 1u32));
    }
}

/// Runs `f` in the shared pool and checks it against the sequential
/// exploration: same counts, same outcomes, no graph without a timeline.
fn check_pool<F>(name: &str, timing: (u64, u64, u64), expected: (usize, usize), f: F)
where
    F: Fn() + Clone + Send + Sync + 'static,
{
    let (seq, seq_outcomes) = run_one(SchedulePolicy::LTR, 0, timing, ConsType::FIFO, f.clone());
    assert_eq!((seq.execs, seq.block), expected, "{name} sequential: (execs, block)");
    for workers in [1usize, 2, 4] {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let st = traceforge::verify(
            Config::builder()
                .with_timed(timing.0, timing.1, timing.2)
                .with_cons_type(ConsType::FIFO)
                .with_parallel(true)
                .with_parallel_workers(workers)
                .with_callback(Box::new(Collector(Arc::clone(&sink))))
                .build(),
            f.clone(),
        );
        let mut outcomes = Outcomes::new();
        for o in sink.lock().unwrap().iter() {
            *outcomes.entry(o.clone()).or_insert(0) += 1;
        }
        println!(
            "{name} pool workers={workers}: execs={} block={} timeline_impossible={} pruned={}",
            st.execs, st.block, st.timeline_impossible, st.pruned
        );
        assert_eq!((st.execs, st.block), expected, "{name} pool workers={workers}: (execs, block)");
        assert_eq!(st.timeline_impossible, 0, "{name} pool workers={workers}: a graph without a timeline was explored");
        assert_eq!(outcomes, seq_outcomes, "{name} pool workers={workers}: outcomes differ from the sequential run");
        assert_eq!(st.pruned, seq.pruned, "{name} pool workers={workers}: rejected children");
    }
}

#[test]
fn pool_tests_a_revisit_graph_before_handing_it_over() {
    check_pool("two_timeouts_one_send", (0, 0, 10), EXPECT_TWO_TIMEOUTS, two_timeouts_one_send());
    check_pool("timeout_then_report", (1, 2, 1), EXPECT_TIMEOUT_REPORT, timeout_then_report());
}

const EXPECT_TWO_TIMEOUTS: (usize, usize) = (2, 0);
const EXPECT_TIMEOUT_REPORT: (usize, usize) = (4, 0);

// ---------------------------------------------------------------------
// A send whose delivered child has no timeline. Until 2026-10-05 that
// child was tested after the send had been added to the running
// execution: the execution was abandoned, but the sending thread ran on
// until its next scheduling point. A failing `traceforge::assert` or a
// channel creation right after the send landed on the position the
// abandon had filled, and the run died with "Incorrect TraceForge
// Program". The execution now goes on as the dropped child, so no
// program code runs in a graph without a timeline.

/// B: recv_timed(5). A: send to B, then `after`. L=U=0 sd=10 FIFO. When
/// B has timed out before A's send is added, the delivered child has no
/// timeline: the message is stored over [0, 10], inside B's wait.
fn send_then<G>(after: G) -> impl Fn() + Clone + Send + Sync + 'static
where
    G: Fn() + Clone + Send + Sync + 'static,
{
    move || {
        let b = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("B={v:?}"));
        });
        let bid = b.thread().id();
        let after = after.clone();
        let _ = thread::spawn(move || {
            traceforge::send_msg(bid, 1u32);
            after();
        });
    }
}

/// The same with an assertion that fails in some executions only.
/// A: m = recv_timed(1); send to B; assert(m is some). C: send to A.
/// B: recv_timed(5). L=U=0 sd=10 FIFO. It fails when C's message is lost.
fn guarded_send_then_assert() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let b = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("B={v:?}"));
        });
        let bid = b.thread().id();
        let a = thread::spawn(move || {
            let m: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(1));
            traceforge::cover!(format!("A={m:?}"));
            traceforge::send_msg(bid, 1u32);
            traceforge::assert(m.is_some());
        });
        let aid = a.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg(aid, 7u32));
    }
}

#[test]
#[should_panic(expected = "certified timed counterexample")]
fn assert_after_a_rejected_send_is_certified() {
    run_one(SchedulePolicy::LTR, 0, (0, 0, 10), ConsType::FIFO, send_then(|| traceforge::assert(false)));
}

#[test]
fn code_after_a_rejected_send_runs_in_the_dropped_child() {
    check_cfg("send; assert", (0, 0, 10), ConsType::FIFO, EXPECT_SEND_THEN, true, send_then(|| traceforge::assert(false)));
    check_cfg("recv; send; assert", (0, 0, 10), ConsType::FIFO, EXPECT_GUARDED_SEND, true, guarded_send_then_assert());
    check("send; channel", (0, 0, 10), ConsType::FIFO, EXPECT_SEND_THEN, send_then(|| {
        let _ = traceforge::channel::Builder::<u32>::new().build();
    }));
}

const EXPECT_SEND_THEN: (usize, usize) = (2, 0);
const EXPECT_GUARDED_SEND: (usize, usize) = (4, 0);

/// A rejection that does not depend on the schedule. R: blocking receive.
/// S: send with window [2, 2]; send with window [0, 0]; assert(false).
/// L=U=0 sd=10 FIFO. With the first send delivered, the second cannot
/// be: it would arrive before the first one on the same channel.
fn overtaking_send_then_assert() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let v: u32 = traceforge::recv_msg_block_timed();
            traceforge::cover!(format!("R={v}"));
        });
        let rid = r.thread().id();
        let _ = thread::spawn(move || {
            traceforge::send_msg_timed(rid, 1u32, 2, 2);
            traceforge::send_msg_timed(rid, 2u32, 0, 0);
            traceforge::assert(false);
        });
    }
}

#[test]
#[should_panic(expected = "certified timed counterexample")]
fn assert_after_an_overtaking_send_is_certified() {
    run_one(SchedulePolicy::LTR, 0, (0, 0, 10), ConsType::FIFO, overtaking_send_then_assert());
}

/// Three sends rejected one after the other in one execution, with a
/// channel creation after each. B: three recv_timed(5). L=U=0 sd=10 FIFO.
fn three_rejected_sends() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let b = thread::spawn(|| {
            let v1: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            let v2: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            let v3: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::cover!(format!("B={v1:?},{v2:?},{v3:?}"));
        });
        let bid = b.thread().id();
        let _ = thread::spawn(move || {
            for v in 1..=3u32 {
                traceforge::send_msg(bid, v);
                let _ = traceforge::channel::Builder::<u32>::new().build();
            }
        });
    }
}

#[test]
fn several_rejected_sends_in_one_execution() {
    check("three rejected sends", (0, 0, 10), ConsType::FIFO, EXPECT_THREE_REJECTED, three_rejected_sends());
}

const EXPECT_THREE_REJECTED: (usize, usize) = (8, 0);

/// Every execution the explorer starts is one it counts: a rejected
/// child, delivered send or revisit graph, starts none. `before` runs
/// once per started execution, `after` once per counted one, and the
/// program's closure once per execution that runs any code.
#[test]
fn every_started_execution_is_counted() {
    struct Calls(Arc<AtomicUsize>, Arc<AtomicUsize>);
    impl ExecutionObserver for Calls {
        fn before(&mut self, _eid: ExecutionId) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn after(&mut self, _eid: ExecutionId, _cond: &EndCondition, _c: CoverageInfo) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    // One rejected delivered send, and two rejected revisit graphs.
    let programs: [(&str, Arc<dyn Fn() + Send + Sync>); 2] = [
        ("send_then", Arc::new(send_then(|| {}))),
        ("two_timeouts_one_send", Arc::new(two_timeouts_one_send())),
    ];
    for (name, prog) in programs {
        let (before, after, entered) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let e = Arc::clone(&entered);
        let st = traceforge::verify(
            Config::builder()
                .with_timed(0, 0, 10)
                .with_cons_type(ConsType::FIFO)
                .with_policy(SchedulePolicy::LTR)
                .with_callback(Box::new(Calls(Arc::clone(&before), Arc::clone(&after))))
                .build(),
            move || {
                e.fetch_add(1, Ordering::Relaxed);
                prog()
            },
        );
        let counted = st.execs + st.block;
        assert!(st.pruned >= 1, "{name}: no child was rejected, the program does not test this");
        assert_eq!(before.load(Ordering::Relaxed), counted, "{name}: executions started");
        assert_eq!(after.load(Ordering::Relaxed), counted, "{name}: executions reported");
        assert_eq!(entered.load(Ordering::Relaxed), counted, "{name}: executions that ran program code");
    }
}

// ---------------------------------------------------------------------
// The partitioned explorer ships saved states to other tasks. A task
// whose revisits are all rejected counts no execution; its rejected
// children still belong to the totals (they were dropped before
// 2026-10-05). A violation found by the root worker, which explores
// inside the scope itself, must end the run like any other.

fn run_partitioned<F>(timing: (u64, u64, u64), f: F) -> (Stats, Outcomes)
where
    F: Fn() + Send + Sync + 'static,
{
    let sink = Arc::new(Mutex::new(Vec::new()));
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(timing.0, timing.1, timing.2)
            .with_cons_type(ConsType::FIFO)
            .with_partitioned_parallelization(true)
            .with_partitioned_num_threads(2)
            .with_warmup(1)
            .with_iterations_until_split(1)
            .with_callback(Box::new(Collector(Arc::clone(&sink))))
            .build(),
        f,
    );
    let mut outcomes = Outcomes::new();
    for o in sink.lock().unwrap().iter() {
        *outcomes.entry(o.clone()).or_insert(0) += 1;
    }
    (stats, outcomes)
}

#[test]
fn partitioned_explorer_counts_like_the_sequential_one() {
    let programs: [(&str, (u64, u64, u64), Arc<dyn Fn() + Send + Sync>); 3] = [
        ("send_then", (0, 0, 10), Arc::new(send_then(|| {}))),
        ("two_timeouts_one_send", (0, 0, 10), Arc::new(two_timeouts_one_send())),
        ("three_rejected_sends", (0, 0, 10), Arc::new(three_rejected_sends())),
    ];
    for (name, timing, prog) in programs {
        let p = Arc::clone(&prog);
        let (seq, seq_outcomes) = run_one(SchedulePolicy::LTR, 0, timing, ConsType::FIFO, move || p());
        let p = Arc::clone(&prog);
        let (st, outcomes) = run_partitioned(timing, move || p());
        println!(
            "{name} partitioned: execs={} block={} timeline_impossible={} pruned={} (sequential pruned={})",
            st.execs, st.block, st.timeline_impossible, st.pruned, seq.pruned
        );
        assert_eq!((st.execs, st.block), (seq.execs, seq.block), "{name} partitioned: (execs, block)");
        assert_eq!(st.timeline_impossible, 0, "{name} partitioned: a graph without a timeline was explored");
        assert_eq!(outcomes, seq_outcomes, "{name} partitioned: outcomes differ from the sequential run");
        assert_eq!(st.pruned, seq.pruned, "{name} partitioned: rejected children");
    }
}

#[test]
#[should_panic(expected = "certified timed counterexample")]
fn partitioned_explorer_reports_a_violation_of_its_root_worker() {
    traceforge::verify(
        Config::builder()
            .with_timed(0, 0, 10)
            .with_cons_type(ConsType::FIFO)
            .with_partitioned_parallelization(true)
            .with_partitioned_num_threads(2)
            .build(),
        send_then(|| traceforge::assert(false)),
    );
}

// ---------------------------------------------------------------------
// The canonical value of a range choice is its first value (2026-10-05;
// it was the last). Any single value gives the same graphs, each once:
// what the convention fixes is which of the graphs that differ in the
// value of a deleted choice launches the backward revisit.

/// R: r = recv_timed(5); x = (0..3).nondet(). S: send to R. L=U=0 sd=10.
/// The send is dropped (r times out) or delivered (r reads it: stored
/// over [0, 10], it cannot be missed): 2 x 3 values = 6 graphs.
fn choice_after_receive() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let a: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            let x = (0usize..3).nondet();
            traceforge::cover!(format!("r={a:?} x={x}"));
        });
        let rid = r.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg(rid, 1u32));
    }
}

#[test]
fn a_deleted_range_choice_is_explored_once() {
    check("choice_after_receive", (0, 0, 10), ConsType::FIFO, EXPECT_CHOICE_AFTER_RECEIVE, choice_after_receive());
}

const EXPECT_CHOICE_AFTER_RECEIVE: (usize, usize) = (6, 0);

/// Under LTR the receive times out first and x takes its first value,
/// 0. With the first value canonical, the revisit "r reads the message"
/// is launched from that very graph, so the endings where r reads start
/// right after the first ending; with the last value canonical they
/// would start only after x had gone through all its values.
#[test]
fn the_launch_world_of_a_deleted_choice_holds_its_first_value() {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let st = traceforge::verify(
        Config::builder()
            .with_timed(0, 0, 10)
            .with_cons_type(ConsType::FIFO)
            .with_policy(SchedulePolicy::LTR)
            .with_callback(Box::new(Collector(Arc::clone(&sink))))
            .build(),
        choice_after_receive(),
    );
    assert_eq!((st.execs, st.block), EXPECT_CHOICE_AFTER_RECEIVE);
    let order: Vec<String> = sink.lock().unwrap().clone();
    assert!(order[0].contains("r=None x=0"), "endings in exploration order: {order:#?}");
    assert!(order[1].contains("r=Some(1) x=0"), "endings in exploration order: {order:#?}");
}

// ---------------------------------------------------------------------
// A rejected revisit graph used to start an empty execution: `before`
// ran, no program code did, and `on_stop` was then called on the monitor
// left by the previous execution when that one had ended on a failed
// assumption (which skips `on_stop`), with AllThreadsCompleted. A monitor
// that checks an end-of-run property raised a false alarm.

struct EndMonitor {
    reached_end: Arc<AtomicBool>,
}

impl Monitor for EndMonitor {
    fn on_stop(&mut self, end: &ExecutionEnd) -> MonitorResult {
        if end.condition == EndCondition::AllThreadsCompleted && !self.reached_end.load(Ordering::Relaxed) {
            return Err("all threads completed but R did not reach its end".to_string());
        }
        Ok(())
    }
}

/// R: r1 = recv_timed(5); r2 = recv_timed(5); assume(r1 timed out); set
/// the monitor's flag. S: send to R. L=U=0 sd=10 FIFO. Every execution in
/// which all threads complete has the flag set.
fn monitored_two_timeouts() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let reached_end = Arc::new(AtomicBool::new(false));
        let monitor: Arc<Mutex<dyn Monitor>> =
            Arc::new(Mutex::new(EndMonitor { reached_end: Arc::clone(&reached_end) }));
        let _: thread::JoinHandle<MonitorResult> = traceforge::spawn_monitor(
            || {
                let _never: u64 = traceforge::recv_msg_block();
                Ok(())
            },
            |_: ThreadId, _: ThreadId, _: Val| None,
            |_: ThreadId, _: ThreadId, _: Val| false,
            monitor,
        );
        let r = thread::spawn(move || {
            let a: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            let _b: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            traceforge::assume!(a.is_none());
            reached_end.store(true, Ordering::Relaxed);
        });
        let rid = r.thread().id();
        let _ = thread::spawn(move || traceforge::send_msg(rid, 1u32));
    }
}

#[test]
fn a_rejected_revisit_graph_does_not_stop_a_stale_monitor() {
    let (st, _) = run_one(SchedulePolicy::LTR, 0, (0, 0, 10), ConsType::FIFO, monitored_two_timeouts());
    assert_eq!(st.timeline_impossible, 0);
    assert!(st.pruned >= 1, "no revisit graph was rejected, the program does not test this");
}

/// One verbose run for graph-level cross-checks: PROG, POL (ltr or arbN),
/// and TF_MT_VERBOSE (2 prints counted and blocked graphs).
#[test]
#[ignore = "driver for graph-level cross-checks; run with --ignored --nocapture"]
fn must_tau_print() {
    let prog = std::env::var("PROG").unwrap_or_default();
    let pol = std::env::var("POL").unwrap_or_else(|_| "ltr".into());
    let (policy, seed) = if pol == "ltr" {
        (SchedulePolicy::LTR, 0)
    } else {
        (SchedulePolicy::Arbitrary, pol[3..].parse().unwrap())
    };
    let (st, _) = match prog.as_str() {
        "retrans1" => run_one(policy, seed, (0, 2, 1), ConsType::FIFO, retransmission(true)),
        "retrans0" => run_one(policy, seed, (0, 2, 1), ConsType::FIFO, retransmission(false)),
        "q1" => run_one(policy, seed, (0, 2, 1), ConsType::FIFO, retransmission_with_q(true)),
        "q0" => run_one(policy, seed, (0, 2, 1), ConsType::FIFO, retransmission_with_q(false)),
        "reflaunch" => run_one(policy, seed, (0, 2, 1), ConsType::FIFO, refusal_launcher()),
        "refprev0" => run_one(policy, seed, (0, 2, 0), ConsType::FIFO, refusal_in_previous(0)),
        "refprev3" => run_one(policy, seed, (0, 2, 0), ConsType::FIFO, refusal_in_previous(3)),
        "drawing1" => run_one(policy, seed, (0, 4, 0), ConsType::FIFO, drawing(true)),
        "drawing0" => run_one(policy, seed, (0, 4, 0), ConsType::FIFO, drawing(false)),
        "two" => run_one(policy, seed, (0, 4, 0), ConsType::FIFO, two_receives()),
        "five" => run_one(policy, seed, (0, 4, 0), ConsType::FIFO, five_candidates()),
        "fifo2" => run_one(policy, seed, (0, 4, 0), ConsType::FIFO, fifo_two_sends()),
        "choice" => run_one(policy, seed, (0, 2, 1), ConsType::FIFO, choice_killed()),
        "crossf" => run_one(policy, seed, (0, 0, 5), ConsType::FIFO, cross_sender(false)),
        "crossb" => run_one(policy, seed, (0, 0, 5), ConsType::FIFO, cross_sender(true)),
        other => panic!("unknown PROG {other}"),
    };
    println!("STATS execs={} block={} timeline_impossible={}", st.execs, st.block, st.timeline_impossible);
}
