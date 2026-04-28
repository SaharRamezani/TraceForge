//! Visualization-only helper test that captures per-execution dot graphs
//! and per-rejection prune logs for several temporal scenarios.
//!
//! Output layout (relative to the cargo workspace root):
//!   viz_out/loose/exec_NNN.dot            4-thread pipeline, loose bounds
//!   viz_out/tight/exec_NNN.dot            4-thread pipeline, tight bounds
//!   viz_out/backward_stack/exec_NNN.dot   1 send + many timed recvs
//!   viz_out/infinite_waits/exec_NNN.dot   concurrent senders + ∞-wait recvs
//!
//! Each subdirectory also holds a `prunes.jsonl` with the rejected rfs and
//! their empty-interval witnesses.
//!
//! Run end-to-end:
//!   cargo test --test temporal_visualize
//!   python3 viz_out/build_html.py
//! Then open viz_out/index.html.
//!
//! Blocked execs (the prunings) capture their partial graph because
//! `make_config` enables `with_dot_out_blocked(true)`. Without that,
//! their slots in the viewer would show "no graph captured" placeholders.

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread::{self, ThreadId};
use traceforge::*;

struct DotSnapshotter {
    src: PathBuf,
    dir: PathBuf,
}

impl ExecutionObserver for DotSnapshotter {
    fn before(&mut self, _eid: ExecutionId) {
        // Truncate the source file before each execution so we never copy
        // stale tail bytes left over from a previous, larger graph.
        let _ = fs::write(&self.src, b"");
    }

    fn after(&mut self, eid: ExecutionId, _ec: &EndCondition, _c: CoverageInfo) {
        let dst = self.dir.join(format!("exec_{:03}.dot", eid));
        let _ = fs::copy(&self.src, &dst);
    }
}

/// Per-scenario meta info written to <viz_out>/<slug>/meta.json so the
/// HTML viewer (`viz_out/build_html.py`) can render this run without any
/// hardcoded knowledge of the protocol it represents.
///
/// `wait` is the recv wait-time (in ticks for finite recvs, or "inf").
/// `labels` maps the raw thread ids ("t0", "t1", ...) onto friendly
/// names; `order` is the desired sidebar/summary thread order.
struct Meta<'a> {
    l: u64,
    u: u64,
    sd: u64,
    wait: &'a str,
    title: &'a str,
    description: &'a str,
    labels: &'a [(&'a str, &'a str)],
    order: &'a [&'a str],
}

fn make_config(label: &str, meta: &Meta) -> (PathBuf, Config) {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = PathBuf::from(manifest)
        .join("..")
        .join("viz_out")
        .join(label);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let src = dir.join("_latest.dot");
    let prune_log = dir.join("prunes.jsonl");
    // Touch the prune log so the artifact always exists, even for
    // scenarios that record zero rejections. TraceForge's writer only
    // creates the file lazily on the first prune; without this, build_html.py
    // sees "no file" and reports 0 prunings — visually indistinguishable
    // from "logging not configured".
    let _ = fs::write(&prune_log, b"");

    // Drop a meta.json next to the dot files so build_html.py can render
    // this run with its protocol-specific labels and thread ordering.
    let labels_obj = serde_json::Map::from_iter(
        meta.labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), serde_json::Value::from(*v))),
    );
    let order_arr: Vec<_> = meta.order.iter().map(|s| serde_json::Value::from(*s)).collect();
    let meta_json = json!({
        "kind":        "pipeline",
        "title":       meta.title,
        "description": meta.description,
        "labels":      labels_obj,
        "order":       order_arr,
        "l":  meta.l,
        "u":  meta.u,
        "sd": meta.sd,
        "wait": meta.wait,
    })
    .to_string();
    let _ = fs::write(dir.join("meta.json"), meta_json);

    let cfg = Config::builder()
        .with_temporal(meta.l, meta.u, meta.sd)
        .with_verbose(1) // required to trigger print_graph_dot per execution
        .with_dot_out(src.to_str().unwrap())
        // Capture the dot for blocked execs too (the prunings) so the
        // viewer can show the partial graph at the moment the rf-mapping
        // got rejected. Without this, those slots would render as
        // "no graph captured" placeholders.
        .with_dot_out_blocked(true)
        .with_prune_log(prune_log.to_str().unwrap())
        .with_callback(Box::new(DotSnapshotter {
            src: src.clone(),
            dir: dir.clone(),
        }))
        .build();
    (src, cfg)
}

// =====================================================================
// 4-thread cascading pipeline (matches the baselined test
// `four_thread_pipeline_temporal_pruning` in temporal.rs).
// =====================================================================
fn run_pipeline(label: &str, global_u: u64, wait_ns: u64) {
    let wait_str = wait_ns.to_string();
    let meta = Meta {
        l: 0,
        u: global_u,
        sd: 0,
        wait: &wait_str,
        title: "4-thread cascading pipeline",
        description: "Each consumer recv has a finite wait; loose=admits both branches, tight=prunes timeouts.",
        labels: &[("t0", "main"), ("t1", "c"), ("t2", "b"), ("t3", "a")],
        order: &["t3", "t2", "t1", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let main_id = thread::current().id();
        let c = thread::spawn(move || {
            for _ in 0..2 {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                if let Some(v) = v {
                    traceforge::sleep(1);
                    traceforge::send_msg(main_id, v + 1000);
                }
            }
        });
        let c_id = c.thread().id();
        let b = thread::spawn(move || {
            for _ in 0..2 {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                if let Some(v) = v {
                    traceforge::sleep(1);
                    traceforge::send_msg(c_id, v + 100);
                }
            }
        });
        let b_id = b.thread().id();
        let a = thread::spawn(move || {
            for _ in 0..2 {
                let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
                if let Some(v) = v {
                    traceforge::sleep(1);
                    traceforge::send_msg(b_id, v + 10);
                }
            }
        });
        let a_id = a.thread().id();
        for i in 0..2 {
            traceforge::sleep(1);
            traceforge::send_msg(a_id, i);
            let _v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(wait_ns));
        }
    });
    let _ = fs::remove_file(src);
}

// =====================================================================
// backward-revisit stack.
//
// One consumer thread does 20 timed receives in succession. Main sleeps then sends
// some messages to the consumer.
// =====================================================================
fn run_backward_stack(label: &str) {
    const RECVS: usize = 5;
    const SENDS: usize = 3;
    const RECV_WAIT: u64 = 10;
    const SLEEP_BEFORE_SEND: u64 = 10;
    const U: u64 = 30;
    let wait_str = RECV_WAIT.to_string();
    let desc = format!(
        "consumer with {} timed recvs (wait={}) + main with {} sends after sleep({})",
        RECVS, RECV_WAIT, SENDS, SLEEP_BEFORE_SEND
    );
    let meta = Meta {
        l: 0,
        u: U,
        sd: 0,
        wait: &wait_str,
        title: "Backward-revisit stack",
        description: &desc,
        labels: &[("t0", "main"), ("t1", "consumer")],
        order: &["t1", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let consumer = thread::spawn(move || {
            for _ in 0..RECVS {
                let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(RECV_WAIT));
            }
        });
        let h_id = consumer.thread().id();
        traceforge::sleep(SLEEP_BEFORE_SEND);
        for _ in 0..SENDS {
            traceforge::send_msg(h_id, 42i32);
        }
    });
    let _ = fs::remove_file(src);
}

// =====================================================================
// infinite waits with concurrent senders.
//
// The consumer does 2 blocking (W_r = +∞) timed receives; each must
// read from a real send (no ⊥ branch). Two concurrent senders each emit
// one message at staggered times.
//
// Expect: 1 final execution, 1 pruned rf.
// =====================================================================
fn run_infinite(label: &str) {
    // 2 blocking infinite-wait receives + 2 concurrent senders.
    const RECVS: usize = 2;
    const U: u64 = 1;
    let desc = format!(
        "consumer with {} blocking infinite-wait recvs + 2 concurrent senders at sleep(2), sleep(4)",
        RECVS
    );
    let meta = Meta {
        l: 0,
        u: U,
        sd: 0,
        wait: "inf",
        title: "Concurrent senders + ∞-wait recvs",
        description: &desc,
        labels: &[("t0", "main"), ("t1", "consumer"), ("t2", "s1"), ("t3", "s2")],
        order: &["t1", "t2", "t3", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let consumer = thread::spawn(move || {
            for _ in 0..RECVS {
                let _: i32 = traceforge::recv_msg_block_timed();
            }
        });
        let h_id = consumer.thread().id();
        let _s1 = thread::spawn(move || {
            traceforge::sleep(2);
            traceforge::send_msg(h_id, 100i32);
        });
        let _s2 = thread::spawn(move || {
            traceforge::sleep(4);
            traceforge::send_msg(h_id, 200i32);
        });
    });
    let _ = fs::remove_file(src);
}

// =====================================================================
// Mirrors `temporal::sleep_advances_lower_bound_both_outcomes`.
//
// L=0, U=1000, sd=0; consumer waits 100 ticks; main sleeps 10 then
// sends. Both rf=send and rf=⊥ branches are temporally consistent, so
// 2 executions, 0 prunings.
// =====================================================================
fn run_sleep_advances(label: &str) {
    let meta = Meta {
        l: 0,
        u: 1000,
        sd: 0,
        wait: "100",
        title: "Sleep advances lower bound",
        description:
            "Wide transit window: rf=send and rf=⊥ are both temporally consistent. \
             Expect 2 execs, 0 prunings.",
        labels: &[("t0", "main"), ("t1", "consumer")],
        order: &["t1", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let consumer = thread::spawn(|| {
            let _: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(100));
        });
        traceforge::sleep(10);
        traceforge::send_msg(consumer.thread().id(), 42i32);
    });
    let _ = fs::remove_file(src);
}

// =====================================================================
// Mirrors `temporal::finite_wait_forces_timeout`.
//
// L=0, U=0, sd=0; consumer wait=10; main sleeps 100 then sends. The
// rf=send window is empty (send arrives at 100 but recv only waits to
// 10), so only the timeout branch survives. Expect 1 exec, 1 pruning.
// =====================================================================
fn run_finite_wait_forces_timeout(label: &str) {
    let meta = Meta {
        l: 0,
        u: 0,
        sd: 0,
        wait: "10",
        title: "Finite wait forces timeout",
        description:
            "Send arrives strictly after the recv's wait expires; rf-from-send window is \
             empty and pruned. Only the ⊥ branch survives. Expect 1 exec, 1 pruning.",
        labels: &[("t0", "main"), ("t1", "consumer")],
        order: &["t1", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let consumer = thread::spawn(|| {
            let v: Option<i32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
            assert!(v.is_none());
        });
        traceforge::sleep(100);
        traceforge::send_msg(consumer.thread().id(), 42i32);
    });
    let _ = fs::remove_file(src);
}

// =====================================================================
// Mirrors `temporal::infinite_wait_prunes_timeout`.
//
// L=0, U=10, sd=0; producer sleeps 5 then sends; consumer does a
// blocking timed recv (W_r = +∞). The rf=send window is non-empty, but
// the ⊥ branch is inadmissible for a blocking recv, so it is pruned.
// Expect 1 exec, 1 pruning (the ⊥ candidate).
// =====================================================================
fn run_infinite_wait_prunes_timeout(label: &str) {
    let meta = Meta {
        l: 0,
        u: 10,
        sd: 0,
        wait: "inf",
        title: "Infinite wait prunes ⊥",
        description:
            "Blocking timed recv: ⊥ is inadmissible, only rf=send survives. \
             Expect 1 exec, 1 pruning.",
        labels: &[("t0", "main"), ("t1", "consumer")],
        order: &["t1", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let consumer = thread::spawn(|| {
            let v: i32 = traceforge::recv_msg_block_timed();
            assert_eq!(v, 42);
        });
        traceforge::sleep(5);
        traceforge::send_msg(consumer.thread().id(), 42i32);
    });
    let _ = fs::remove_file(src);
}

// =====================================================================
// Mirrors `temporal::predicate_timed_recv`.
//
// L=0, U=100, sd=0; two senders each emit one message after a handoff
// from main. Main does a tagged timed recv (wait=50) whose predicate
// only accepts s1's id. The s2 message is filtered by the predicate;
// the s1 send is temporally consistent and admits both rf=s1 and ⊥.
// Expect 2 execs.
// =====================================================================
fn run_predicate_timed_recv(label: &str) {
    let meta = Meta {
        l: 0,
        u: 100,
        sd: 0,
        wait: "50",
        title: "Predicate timed recv",
        description:
            "Two senders, but the recv predicate only matches s1. s2's send is filtered \
             out by the predicate (not by the temporal filter). Expect 2 execs.",
        labels: &[("t0", "main"), ("t1", "s1"), ("t2", "s2")],
        order: &["t1", "t2", "t0"],
    };
    let (src, cfg) = make_config(label, &meta);
    traceforge::verify(cfg, move || {
        let main_id = thread::current().id();
        let s1 = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block();
            traceforge::send_msg(main_id, 1i32);
        });
        let s2 = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block();
            traceforge::send_msg(main_id, 2i32);
        });
        let s1_id = s1.thread().id();
        traceforge::send_msg(s1_id, 0);
        traceforge::send_msg(s2.thread().id(), 0);

        let v: Option<i32> = traceforge::recv_tagged_msg_timed(
            move |tid: ThreadId, _tag| tid == s1_id,
            WaitTime::Finite(50),
        );
        let _ = v;
    });
    let _ = fs::remove_file(src);
}

#[test]
fn visualize_all_scenarios() {
    run_pipeline("loose", 50, 50);
    run_pipeline("tight", 2, 2);
    run_backward_stack("backward_stack");
    run_infinite("infinite_waits");
    // First four `temporal::*` tests, captured for the viewer.
    run_sleep_advances("sleep_advances");
    run_finite_wait_forces_timeout("finite_wait_timeout");
    run_infinite_wait_prunes_timeout("infinite_wait_prunes");
    run_predicate_timed_recv("predicate_timed");
}
