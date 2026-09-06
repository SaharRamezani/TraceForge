//! Hand-checked completeness probe for the DEFAULT (LTR) schedule policy on
//! tiny inbox programs.
//!
//! Every program here has ONE send per sender thread, so all sends to the
//! collector are sb-incomparable: no FIFO/same-sender ordering constraint can
//! rule any subset out, and the ground truth is a plain binomial count that can
//! be enumerated on paper (see each fn's comment). Case (a) is additionally
//! cross-checked against the equivalent plain-blocking-receive program, which
//! the docs call policy-independent.
//!
//! Run with:
//!   CARGO_INCREMENTAL=0 cargo test -p traceforge --test inbox_ltr_completeness -- --nocapture

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::{cover, thread, Config, CoverageInfo, ExecutionId, SchedulePolicy};

#[derive(Clone, Debug, PartialEq)]
struct Msg {
    id: u32,
}

/// The set of cover!-goals fired in one execution. Each collector fires one
/// goal naming the sorted id-set it read, so the goal set identifies the
/// inbox's rf-set. Sets (not counts) because a prefix replay after a backward
/// revisit can re-fire a goal inside one execution.
type Fingerprint = BTreeSet<String>;

struct OutcomeCollector {
    sink: Arc<Mutex<Vec<(String, Fingerprint)>>>,
}

impl ExecutionObserver for OutcomeCollector {
    fn after(&mut self, _eid: ExecutionId, cond: &EndCondition, c: CoverageInfo) {
        let goals: Fingerprint = c.coverage.keys().cloned().collect();
        self.sink.lock().unwrap().push((format!("{cond:?}"), goals));
    }
}

#[derive(Debug)]
struct RunResult {
    execs: usize,
    block: usize,
    /// class -> multiplicity, over FINISHED executions only.
    classes: BTreeMap<String, usize>,
}

fn run(policy: SchedulePolicy, prog: Arc<dyn Fn() + Send + Sync>) -> RunResult {
    let sink: Arc<Mutex<Vec<(String, Fingerprint)>>> = Arc::new(Mutex::new(Vec::new()));
    let cfg = Config::builder()
        .with_policy(policy)
        .with_seed(0)
        .with_callback(Box::new(OutcomeCollector {
            sink: Arc::clone(&sink),
        }))
        .build();
    let stats = traceforge::verify(cfg, move || prog());
    let mut classes: BTreeMap<String, usize> = BTreeMap::new();
    for (cond, goals) in sink.lock().unwrap().iter() {
        // Blocked executions end in `Deadlock`; only completed ones count
        // as explored executions (matching Stats::execs vs Stats::block).
        if cond == "AllThreadsCompleted" {
            // One class per execution: the goals of all its inboxes joined,
            // so a multi-inbox program's class records the whole tuple of
            // rf-sets, not just which sets appeared somewhere.
            let key = goals.iter().cloned().collect::<Vec<_>>().join("|");
            *classes.entry(key).or_insert(0) += 1;
        }
    }
    RunResult {
        execs: stats.execs,
        block: stats.block,
        classes,
    }
}

/// n senders, each sending exactly one distinguishable message to one
/// collector thread running a single untimed `inbox_with_bounds(min, max)`.
fn inbox_prog(senders: u32, min: usize, max: Option<usize>) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        let collector = thread::spawn(move || {
            let msgs = traceforge::inbox_with_bounds(min, max);
            let mut ids: Vec<u32> = msgs
                .into_iter()
                .flatten()
                .map(|v| {
                    v.as_any_ref()
                        .downcast_ref::<Msg>()
                        .expect("inbox should yield Msg")
                        .id
                })
                .collect();
            ids.sort_unstable();
            cover!(format!("read{ids:?}"));
        });
        let tid = collector.thread().id();
        let mut senders_v = Vec::new();
        for id in 0..senders {
            let t = tid.clone();
            senders_v.push(thread::spawn(move || {
                traceforge::send_msg(t, Msg { id });
            }));
        }
        for s in senders_v {
            let _ = s.join();
        }
        let _ = collector.join();
    })
}

/// Reference program: same senders, but the collector does `k` sequential
/// plain BLOCKING receives instead of one inbox. Recv-only programs are
/// documented as policy-independent, so this is an independent oracle.
fn recv_prog(senders: u32, k: usize) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        let collector = thread::spawn(move || {
            let mut ids: Vec<u32> = Vec::new();
            for _ in 0..k {
                let m: Msg = traceforge::recv_msg_block();
                ids.push(m.id);
            }
            ids.sort_unstable();
            cover!(format!("read{ids:?}"));
        });
        let tid = collector.thread().id();
        let mut senders_v = Vec::new();
        for id in 0..senders {
            let t = tid.clone();
            senders_v.push(thread::spawn(move || {
                traceforge::send_msg(t, Msg { id });
            }));
        }
        for s in senders_v {
            let _ = s.join();
        }
        let _ = collector.join();
    })
}

fn report(name: &str, r: &RunResult, expected_execs: usize, expected: &[&str]) -> bool {
    let got: BTreeSet<&str> = r.classes.keys().map(|s| s.as_str()).collect();
    let want: BTreeSet<&str> = expected.iter().copied().collect();
    let missing: Vec<&&str> = want.difference(&got).collect();
    let extra: Vec<&&str> = got.difference(&want).collect();
    println!("--- {name}");
    println!("    execs={} block={} (hand-count execs={expected_execs})", r.execs, r.block);
    println!("    classes explored: {:?}", r.classes);
    println!("    hand-count classes: {expected:?}");
    if !missing.is_empty() {
        println!("    MISSING (under-exploration): {missing:?}");
    }
    if !extra.is_empty() {
        println!("    EXTRA (over-exploration): {extra:?}");
    }
    missing.is_empty() && extra.is_empty() && r.execs == expected_execs
}

// ---------------------------------------------------------------------
// (a) two senders, inbox min=1 max=1.
// Ground truth: the inbox must take exactly one of the two incomparable
// sends. Read-sets: {0}, {1}. => 2 executions, 0 blocked.
// Reference: one plain blocking recv with two available sends = 2 execs.
// ---------------------------------------------------------------------
#[test]
fn case_a_two_senders_min1_max1() {
    let r = run(SchedulePolicy::LTR, inbox_prog(2, 1, Some(1)));
    let refr = run(SchedulePolicy::LTR, recv_prog(2, 1));
    println!("=== case (a): 2 senders, inbox(min=1,max=1)");
    let ok = report("inbox", &r, 2, &["read[0]", "read[1]"]);
    let ok_ref = report("plain blocking recv oracle", &refr, 2, &["read[0]", "read[1]"]);
    assert!(ok_ref, "recv oracle itself disagrees with the hand count");
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// ---------------------------------------------------------------------
// (b) two senders, inbox min=1 max=2.
// Ground truth: nonempty subsets of {0,1}: {0}, {1}, {0,1}. => 3 executions.
// ---------------------------------------------------------------------
#[test]
fn case_b_two_senders_min1_max2() {
    let r = run(SchedulePolicy::LTR, inbox_prog(2, 1, Some(2)));
    println!("=== case (b): 2 senders, inbox(min=1,max=2)");
    let ok = report("inbox", &r, 3, &["read[0]", "read[1]", "read[0, 1]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// ---------------------------------------------------------------------
// (c) two senders, inbox min=2 max=2.
// Ground truth: only subset of size 2: {0,1}. => 1 execution, 0 blocked
// (both sends always happen, so the min is always eventually satisfiable).
// ---------------------------------------------------------------------
#[test]
fn case_c_two_senders_min2_max2() {
    let r = run(SchedulePolicy::LTR, inbox_prog(2, 2, Some(2)));
    println!("=== case (c): 2 senders, inbox(min=2,max=2)");
    let ok = report("inbox", &r, 1, &["read[0, 1]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// ---------------------------------------------------------------------
// (d) three senders, inbox min=2 max=2.
// Ground truth: 2-subsets of {0,1,2}: {0,1}, {0,2}, {1,2}. => 3 executions.
// Reference: two sequential plain blocking receives explore the ORDERED
// pairs (6 = 3 * 2!), which collapse to the same 3 unordered classes.
// ---------------------------------------------------------------------
#[test]
fn case_d_three_senders_min2_max2() {
    let r = run(SchedulePolicy::LTR, inbox_prog(3, 2, Some(2)));
    let refr = run(SchedulePolicy::LTR, recv_prog(3, 2));
    println!("=== case (d): 3 senders, inbox(min=2,max=2)");
    let want = ["read[0, 1]", "read[0, 2]", "read[1, 2]"];
    let ok = report("inbox", &r, 3, &want);
    let ok_ref = report("2x plain blocking recv oracle", &refr, 6, &want);
    assert!(ok_ref, "recv oracle itself disagrees with the hand count");
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// =====================================================================
// Second grid: shapes where the documented antichain rule bites, i.e. a
// sender that sends TWICE (FIFO), and sequential inboxes.
//
// Documented semantics (traceforge/src/lib.rs, "Single-sender batches"):
// the inbox member pool is the sb-minimal ANTICHAIN of the channel, so at
// most ONE message per sender is a candidate for one inbox call; a second
// message from the same sender is not a distinct member and only becomes
// a candidate once its FIFO predecessor has been consumed.
// =====================================================================

/// `sends[i]` = how many messages sender i sends (ids are globally distinct,
/// allocated in sender order); the collector runs `inboxes` in sequence.
fn multi_prog(sends: Vec<u32>, inboxes: Vec<(usize, Option<usize>)>) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        let boxes = inboxes.clone();
        let collector = thread::spawn(move || {
            for (i, (min, max)) in boxes.iter().enumerate() {
                let msgs = traceforge::inbox_with_bounds(*min, *max);
                let mut ids: Vec<u32> = msgs
                    .into_iter()
                    .flatten()
                    .map(|v| {
                        v.as_any_ref()
                            .downcast_ref::<Msg>()
                            .expect("inbox should yield Msg")
                            .id
                    })
                    .collect();
                ids.sort_unstable();
                cover!(format!("i{i}{ids:?}"));
            }
        });
        let tid = collector.thread().id();
        let mut handles = Vec::new();
        let mut next_id = 0u32;
        for n in sends.iter() {
            let base = next_id;
            next_id += *n;
            let t = tid.clone();
            let n = *n;
            handles.push(thread::spawn(move || {
                for k in 0..n {
                    traceforge::send_msg(t.clone(), Msg { id: base + k });
                }
            }));
        }
        for h in handles {
            let _ = h.join();
        }
        let _ = collector.join();
    })
}

// (e) ONE sender with two FIFO sends (ids 0,1); inbox min=1 max=1.
// Antichain at the inbox = {0} only (1 sits behind 0 in the FIFO). min=1
// forces exactly one member => the single class {0}. 1 execution, 0 blocked.
#[test]
fn case_e_one_sender_two_sends_min1_max1() {
    let r = run(SchedulePolicy::LTR, multi_prog(vec![2], vec![(1, Some(1))]));
    println!("=== case (e): 1 sender x 2 sends, inbox(min=1,max=1)");
    let ok = report("inbox", &r, 1, &["i0[0]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (f) flagship shape: sender A sends id 0; sender B sends ids 1,2 (FIFO).
// Antichain at the inbox = {0, 1} (2 is behind 1). inbox min=1 max=None
// => the nonempty subsets {0}, {1}, {0,1}: 3 executions, 0 blocked.
#[test]
fn case_f_two_senders_one_and_two_sends_min1() {
    let r = run(SchedulePolicy::LTR, multi_prog(vec![1, 2], vec![(1, None)]));
    println!("=== case (f): senders 1x1 and 1x2 sends, inbox(min=1,max=None)");
    let ok = report("inbox", &r, 3, &["i0[0]", "i0[1]", "i0[0, 1]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (g) mirror shape: ONE sender with two FIFO sends; inbox min=0 max=None
// (the non-blocking untimed inbox). Antichain = {0}; admissible read-sets
// are the subsets of that antichain: {} and {0} => 2 executions, 0 blocked.
// Reading {1} or {0,1} would read past / batch a same-sender FIFO
// predecessor and is NOT a legal outcome.
#[test]
fn case_g_one_sender_two_sends_min0() {
    let r = run(SchedulePolicy::LTR, multi_prog(vec![2], vec![(0, None)]));
    // Independent oracle: same sender, but the collector does ONE
    // non-blocking plain receive. Recv-only programs are policy-independent
    // and FIFO forbids reading id 1 before id 0, so this must be {} / {0}.
    let oracle: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {
        let collector = thread::spawn(|| {
            let m: Option<Msg> = traceforge::recv_msg();
            let ids: Vec<u32> = m.into_iter().map(|m| m.id).collect();
            cover!(format!("i0{ids:?}"));
        });
        let tid = collector.thread().id();
        let s = thread::spawn(move || {
            traceforge::send_msg(tid.clone(), Msg { id: 0 });
            traceforge::send_msg(tid, Msg { id: 1 });
        });
        let _ = s.join();
        let _ = collector.join();
    });
    let refr = run(SchedulePolicy::LTR, oracle);
    println!("=== case (g): 1 sender x 2 sends, inbox(min=0,max=None)");
    let ok = report("inbox", &r, 2, &["i0[]", "i0[0]"]);
    let ok_ref = report("non-blocking plain recv oracle", &refr, 2, &["i0[]", "i0[0]"]);
    assert!(ok_ref, "recv oracle itself disagrees with the hand count");
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// =====================================================================
// Third grid: the inbox is followed by a SEND, so the exploration cannot
// stay on the pure forward path (post-inbox events must be revisited).
// =====================================================================

#[derive(Clone, Debug, PartialEq)]
struct Ack(u32);

/// n senders (one send each) -> collector runs one inbox, then acks main;
/// main blocking-receives the ack.
fn ack_prog(senders: u32, min: usize, max: Option<usize>) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        let main_tid = thread::main_thread_id();
        let collector = thread::spawn(move || {
            let msgs = traceforge::inbox_with_bounds(min, max);
            let mut ids: Vec<u32> = msgs
                .into_iter()
                .flatten()
                .map(|v| v.as_any_ref().downcast_ref::<Msg>().unwrap().id)
                .collect();
            ids.sort_unstable();
            cover!(format!("i0{ids:?}"));
            traceforge::send_msg(main_tid, Ack(ids.len() as u32));
        });
        let tid = collector.thread().id();
        let mut hs = Vec::new();
        for id in 0..senders {
            let t = tid.clone();
            hs.push(thread::spawn(move || traceforge::send_msg(t, Msg { id })));
        }
        let _: Ack = traceforge::recv_msg_block();
        for h in hs {
            let _ = h.join();
        }
        let _ = collector.join();
    })
}

// (k) 2 senders, inbox(min=1,max=1) then ack. The ack is deterministic
// (one send, one blocking recv), so the classes are still exactly the two
// singleton read-sets {0}, {1}: 2 executions, 0 blocked.
#[test]
fn case_k_inbox_then_send_min1_max1() {
    let r = run(SchedulePolicy::LTR, ack_prog(2, 1, Some(1)));
    println!("=== case (k): 2 senders, inbox(min=1,max=1) then ack");
    let ok = report("inbox", &r, 2, &["i0[0]", "i0[1]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (m) same, inbox(min=1,max=None): {0}, {1}, {0,1} => 3 executions.
#[test]
fn case_m_inbox_then_send_min1_unbounded() {
    let r = run(SchedulePolicy::LTR, ack_prog(2, 1, None));
    println!("=== case (m): 2 senders, inbox(min=1,max=None) then ack");
    let ok = report("inbox", &r, 3, &["i0[0]", "i0[1]", "i0[0, 1]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (n) 2 senders (one send each), collector runs TWO inbox(min=1,max=None).
// Hand enumeration:
//   inbox1 = {0} -> inbox2 must take the only leftover {1}            (exec)
//   inbox1 = {1} -> inbox2 must take {0}                              (exec)
//   inbox1 = {0,1} -> nothing is left, inbox2 (min=1) blocks forever  (blocked)
// => 2 executions and 1 blocked class.
#[test]
fn case_n_two_unbounded_inboxes_blocked_class() {
    let r = run(
        SchedulePolicy::LTR,
        multi_prog(vec![1, 1], vec![(1, None), (1, None)]),
    );
    println!("=== case (n): 2 senders, two sequential inbox(min=1,max=None)");
    let ok = report("inbox", &r, 2, &["i0[0]|i1[1]", "i0[1]|i1[0]"]);
    println!("    hand-count blocked = 1, tool blocked = {}", r.block);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
    assert_eq!(r.block, 1, "blocked-class count disagrees with the hand count");
}

// (l) relay chain: senders A,B -> collector C (inbox 1,1) -> D (inbox 1,1).
// C picks one of the two incomparable messages; D's inbox has exactly one
// possible member (C's relay). => 2 executions, 0 blocked.
#[test]
fn case_l_relay_chain() {
    let prog: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {
        let d = thread::spawn(|| {
            let msgs = traceforge::inbox_with_bounds(1, Some(1));
            let ids: Vec<u32> = msgs
                .into_iter()
                .flatten()
                .map(|v| v.as_any_ref().downcast_ref::<Msg>().unwrap().id)
                .collect();
            cover!(format!("d{ids:?}"));
        });
        let d_tid = d.thread().id();
        let c = thread::spawn(move || {
            let msgs = traceforge::inbox_with_bounds(1, Some(1));
            let mut ids: Vec<u32> = msgs
                .into_iter()
                .flatten()
                .map(|v| v.as_any_ref().downcast_ref::<Msg>().unwrap().id)
                .collect();
            ids.sort_unstable();
            cover!(format!("c{ids:?}"));
            traceforge::send_msg(d_tid, Msg { id: 9 });
        });
        let c_tid = c.thread().id();
        let mut hs = Vec::new();
        for id in 0..2u32 {
            let t = c_tid.clone();
            hs.push(thread::spawn(move || traceforge::send_msg(t, Msg { id })));
        }
        for h in hs {
            let _ = h.join();
        }
        let _ = c.join();
        let _ = d.join();
    });
    let r = run(SchedulePolicy::LTR, prog);
    println!("=== case (l): A,B -> C(inbox 1,1) -> D(inbox 1,1)");
    let ok = report("inbox", &r, 2, &["c[0]|d[9]", "c[1]|d[9]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (h) two sequential inboxes, each min=1 max=1, two senders one send each.
// Every inbox consumes exactly one of the two incomparable messages, so the
// executions are the ORDERED pairs: (0 then 1) and (1 then 0). 2 executions.
#[test]
fn case_h_two_inboxes_two_senders() {
    let r = run(
        SchedulePolicy::LTR,
        multi_prog(vec![1, 1], vec![(1, Some(1)), (1, Some(1))]),
    );
    println!("=== case (h): 2 senders x 1 send, two sequential inbox(1,1)");
    let ok = report("inbox", &r, 2, &["i0[0]|i1[1]", "i0[1]|i1[0]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (i) ONE sender with two FIFO sends, two sequential inboxes min=1 max=1.
// First inbox can only take the FIFO front {0}; once consumed, {1} becomes
// the front for the second inbox. Exactly 1 execution, 0 blocked.
#[test]
fn case_i_one_sender_two_sends_two_inboxes() {
    let r = run(
        SchedulePolicy::LTR,
        multi_prog(vec![2], vec![(1, Some(1)), (1, Some(1))]),
    );
    println!("=== case (i): 1 sender x 2 sends, two sequential inbox(1,1)");
    let ok = report("inbox", &r, 1, &["i0[0]|i1[1]"]);
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

// (j) three senders, one send each, inbox min=1 max=None: every nonempty
// subset of {0,1,2} => 7 executions.
#[test]
fn case_j_three_senders_min1_unbounded() {
    let r = run(SchedulePolicy::LTR, multi_prog(vec![1, 1, 1], vec![(1, None)]));
    println!("=== case (j): 3 senders x 1 send, inbox(min=1,max=None)");
    let ok = report(
        "inbox",
        &r,
        7,
        &[
            "i0[0]",
            "i0[1]",
            "i0[2]",
            "i0[0, 1]",
            "i0[0, 2]",
            "i0[1, 2]",
            "i0[0, 1, 2]",
        ],
    );
    assert!(ok, "LTR inbox exploration disagrees with the hand count");
}

/// Formula sweep. With n senders sending ONE message each, every send is
/// sb-incomparable, so the whole channel is the antichain and the legal
/// read-sets of one `inbox_with_bounds(min, max)` are exactly the subsets of
/// size k for min <= k <= min(max, n):  sum_k C(n, k).  If that sum is 0 the
/// inbox can never be satisfied and the single class is a blocked execution.
/// This is the same hand argument as cases (a)-(d), applied on a grid.
#[test]
fn sweep_one_send_per_sender_binomial_formula() {
    fn c(n: usize, k: usize) -> usize {
        if k > n {
            return 0;
        }
        (0..k).fold(1usize, |a, i| a * (n - i) / (i + 1))
    }
    let mut bad: Vec<String> = Vec::new();
    for n in 1..=4usize {
        for min in 0..=3usize {
            for max in [Some(1usize), Some(2), Some(3), None] {
                let hi = max.unwrap_or(n).min(n);
                let hand: usize = (min..=hi).map(|k| c(n, k)).sum();
                let r = run(
                    SchedulePolicy::LTR,
                    multi_prog(vec![1; n], vec![(min, max)]),
                );
                let hand_block = if hand == 0 { 1 } else { 0 };
                let tag = format!("n={n} min={min} max={max:?}");
                println!(
                    "{tag}: hand execs={hand} block={hand_block} | tool execs={} block={}",
                    r.execs, r.block
                );
                if r.execs != hand || r.block != hand_block {
                    bad.push(format!(
                        "{tag}: hand=({hand},{hand_block}) tool=({},{}) classes={:?}",
                        r.execs, r.block, r.classes
                    ));
                }
            }
        }
    }
    assert!(bad.is_empty(), "formula mismatches:\n{}", bad.join("\n"));
}

/// Characterizes the case-(g) family: min=0 inboxes facing a sender that
/// sends more than once. Hand truth = subsets of the sb-minimal antichain
/// (one message per sender), i.e. 2^(#senders). Never asserts.
#[test]
fn diagnostic_min0_multi_send_family() {
    for (name, sends, hand) in [
        ("1 sender x 2 sends", vec![2u32], 2usize),
        ("1 sender x 3 sends", vec![3], 2),
        ("2 senders: 1 and 2 sends", vec![1, 2], 4),
        ("2 senders x 2 sends", vec![2, 2], 4),
        // control: no sender sends twice, so the antichain is everything
        ("2 senders x 1 send", vec![1, 1], 4),
    ] {
        let r = run(SchedulePolicy::LTR, multi_prog(sends, vec![(0, None)]));
        println!(
            "min=0, {name}: hand={hand} tool execs={} block={} classes={:?}",
            r.execs, r.block, r.classes
        );
    }
}

// ---------------------------------------------------------------------
// Diagnostic only (never asserts): the same four programs under Arbitrary,
// to show whether the two policies agree on these shapes.
// ---------------------------------------------------------------------
#[test]
fn diagnostic_arbitrary_policy_side_by_side() {
    for (name, prog, hand) in [
        ("a: 2 senders min=1 max=1", inbox_prog(2, 1, Some(1)), 2usize),
        ("b: 2 senders min=1 max=2", inbox_prog(2, 1, Some(2)), 3),
        ("c: 2 senders min=2 max=2", inbox_prog(2, 2, Some(2)), 1),
        ("d: 3 senders min=2 max=2", inbox_prog(3, 2, Some(2)), 3),
        ("e: 1 sender x2 sends min=1 max=1", multi_prog(vec![2], vec![(1, Some(1))]), 1),
        ("f: senders 1,2 sends min=1", multi_prog(vec![1, 2], vec![(1, None)]), 3),
        ("g: 1 sender x2 sends min=0", multi_prog(vec![2], vec![(0, None)]), 2),
        (
            "h: 2 senders, two inbox(1,1)",
            multi_prog(vec![1, 1], vec![(1, Some(1)), (1, Some(1))]),
            2,
        ),
        (
            "i: 1 sender x2 sends, two inbox(1,1)",
            multi_prog(vec![2], vec![(1, Some(1)), (1, Some(1))]),
            1,
        ),
        ("j: 3 senders min=1 unbounded", multi_prog(vec![1, 1, 1], vec![(1, None)]), 7),
    ] {
        let l = run(SchedulePolicy::LTR, Arc::clone(&prog));
        let a = run(SchedulePolicy::Arbitrary, prog);
        println!(
            "{name}: hand={hand} | LTR execs={} block={} {:?} | ARB execs={} block={} {:?}",
            l.execs, l.block, l.classes, a.execs, a.block, a.classes
        );
    }
}
