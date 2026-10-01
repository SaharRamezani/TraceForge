//! Regression (2026-09-30): the dodge probe of a candidate read used to
//! drop every front past its fold cap (12 fronts), which made the
//! candidate list looser than (C7). Under causal delivery the first
//! candidate is installed without a further check, so with 13 or more
//! fronts an inconsistent graph was visited (timeline_impossible = 1) and
//! the canonical read judged on the same list could be one that no
//! timeline admits. The probe now searches the fronts exactly past the
//! cap.
//!
//! R waits for START (sent at 5 by L), then blocks for a B message. H
//! sends n-1 B messages with transit [0, 10], sleeps 5, sends the n-th
//! (it arrives at 5 or later, so it is never dead before R's wait), then
//! GO to L, which sends s = 99 and START. Reading s past the n-th front
//! is inconsistent, so the first execution must read b1, and no ending
//! may lack a timeline.

use std::sync::{Arc, Mutex};
use traceforge::coverage::ExecutionObserver;
use traceforge::monitor_types::EndCondition;
use traceforge::thread;
use traceforge::{Config, ConsType, CoverageInfo, ExecutionId, SchedulePolicy};

const B: u32 = 1;
const GO: u32 = 2;
const START: u32 = 3;

struct Collector(Arc<Mutex<Vec<String>>>);
impl ExecutionObserver for Collector {
    fn after(&mut self, _eid: ExecutionId, _cond: &EndCondition, c: CoverageInfo) {
        let mut goals: Vec<String> = c.coverage.keys().cloned().collect();
        goals.sort();
        self.0.lock().unwrap().push(goals.join(","));
    }
}

fn prog(nfronts: u32) -> impl Fn() + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let _: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(START));
            let v: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(B));
            traceforge::cover!(format!("R={v}"));
        });
        let rid = r.thread().id();
        let l = thread::spawn(move || {
            let _: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(GO));
            traceforge::send_tagged_msg_timed(rid, B, 99u32, 0, 10);
            traceforge::send_tagged_msg_timed(rid, START, 0u32, 0, 0);
        });
        let lid = l.thread().id();
        let h = thread::spawn(move || {
            for i in 1..nfronts {
                traceforge::send_tagged_msg_timed(rid, B, i, 0, 10);
            }
            traceforge::sleep(5);
            traceforge::send_tagged_msg_timed(rid, B, nfronts, 0, 10);
            traceforge::send_tagged_msg_timed(lid, GO, 0u32, 0, 0);
        });
        let _ = r.join();
        let _ = l.join();
        let _ = h.join();
    }
}

#[test]
fn candidate_probe_is_exact_past_the_fold_cap() {
    for n in [12u32, 13, 14] {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let st = traceforge::verify(
            Config::builder()
                .with_timed(0, 10, 0)
                .with_cons_type(ConsType::Causal)
                .with_policy(SchedulePolicy::LTR)
                .with_max_iterations(3)
                .with_callback(Box::new(Collector(Arc::clone(&sink))))
                .build(),
            prog(n),
        );
        assert_eq!(st.timeline_impossible, 0, "{n} fronts: an explored ending had no timeline");
        let first = sink.lock().unwrap().first().cloned();
        assert_eq!(first.as_deref(), Some("R=1"), "{n} fronts: first execution");
    }
}
