use traceforge::{
    cover, recv_msg_block, send_msg,
    thread::{self, ThreadId},
    Config,
};
use rand::distr::{Bernoulli, Uniform};

//mod utils;

#[test]
fn sample_basic() {
    let n = 10;
    let stats = traceforge::verify(Config::builder().build(), move || {
        let s: u32 = traceforge::sample(Uniform::new(0, 100).unwrap(), n);
        println!("HERE {s}");
    });
    println!("Stats: {} {}", stats.execs, stats.block);
    assert_eq!(stats.execs, n);
}
#[test]
fn sample_with_nondet_choice() {
    let n = 10;
    let stats = traceforge::verify(Config::builder().with_verbose(1).build(), move || {
        if traceforge::nondet() {
            let s: u32 = traceforge::sample(Uniform::new(0, 100).unwrap(), n);
            println!("HERE {s}");
        }
    });
    println!("Stats: {} {}", stats.execs, stats.block);
    assert_eq!(stats.execs, n + 1);
}
#[test]
fn sample_with_comm() {
    let n = 10;
    let stats = traceforge::verify(Config::builder().with_verbose(1).build(), move || {
        let tid = thread::spawn(|| {
            let r: u32 = recv_msg_block();
            let t: u32 = traceforge::sample(Uniform::new(0, 1).unwrap(), r as usize);
            println!("t = {t}");
        })
        .thread()
        .id();
        let s: u32 = traceforge::sample(Uniform::new(1, 6).unwrap(), n);
        send_msg(tid, s);
    });
    println!("Stats: {} {}", stats.execs, stats.block);
    // assert_eq!(stats.execs, n);
}

#[test]
fn sample_with_bernoulli() {
    let n = 10;
    let stats = traceforge::verify(Config::builder().with_verbose(1).build(), move || {
        let tid = thread::spawn(|| {
            let r: bool = recv_msg_block();
            cover!("R", r);
        })
        .thread()
        .id();
        let s: bool = traceforge::sample(Bernoulli::new(0.6).unwrap(), n);
        send_msg(tid, s);
    });
    println!(
        "Stats: {} {} r={}",
        stats.execs,
        stats.block,
        stats.coverage.covered("R".to_owned())
    );
    assert_eq!(stats.execs, n);
}

/// T1: send(t2); recv(); send(T3);
/// T2: recv(); send(T1); send(T3);
/// T3: recv(); recv();
///
/// The state space is 2: in which order T3 receives its messages
#[test]
fn estimate_srs_rss_rr() {
    fn foo() {
        let t1 = thread::spawn(move || {
            let _: u32 = traceforge::recv_msg_block();
            let _: u32 = traceforge::recv_msg_block();
        });
        let t1id = t1.thread().id();
        let t2 = thread::spawn(move || {
            let m: (ThreadId, u32) = traceforge::recv_msg_block();
            traceforge::send_msg(m.0, 2u32);
            traceforge::send_msg(t1id.clone(), 3u32);
        });
        let t2id = t2.thread().id();
        let t3 = thread::spawn(move || {
            traceforge::send_msg(t2id, (thread::current().id(), 1u32));
            let _: u32 = traceforge::recv_msg_block();
            traceforge::send_msg(t1id, 4u32);
        });
        let _ = t1.join();
        let _ = t2.join();
        let _ = t3.join();
    }
    let states = traceforge::estimate_execs_with_samples(foo, 10);
    assert_eq!(states, 2.0);
}

#[test]
fn estimate_sr_ncopies() {
    fn foo(n_test: usize) {
        let mut tids = Vec::new();
        for _i in 0..n_test {
            let tr = thread::spawn(move || {
                let _: u32 = traceforge::recv_msg_block();
            });
            let tid = tr.thread().id();
            let ts = thread::spawn(move || {
                let _ = traceforge::send_msg(tid, 1u32);
            });
            tids.push(ts);
            tids.push(tr);
        }
        for tid in tids {
            let _ = tid.join();
        }
    }
    let n_test = 5;
    let states = traceforge::estimate_execs_with_samples(move || foo(n_test), 10);
    assert_eq!(states, 1.0);
}

#[test]
fn estimate_ns_nseqr() {
    fn foo(n_test: usize) {
        let mut tids = Vec::new();
        let receiver = thread::spawn(move || {
            for _i in 0..n_test {
                let _: u32 = traceforge::recv_msg_block();
            }
        });
        let receiver_id = receiver.thread().id();
        tids.push(receiver);
        for _i in 0..n_test {
            let rid = receiver_id.clone();
            let ts = thread::spawn(move || {
                let _ = traceforge::send_msg(rid, 1u32);
            });
            tids.push(ts);
        }
        for tid in tids {
            let _ = tid.join();
        }
    }
    let n_test = 5;
    let states = traceforge::estimate_execs_with_samples(move || foo(n_test), 1);
    // The number of execs is 1 under FIFO and n_test! under MO
    assert_eq!(states, 120.0);
}

#[test]
fn estimate_a_nstepsb() {
    fn foo(n_test: usize) {
        let mut tids = Vec::new();

        let sink = thread::Builder::new()
            .name("sink".to_owned())
            .spawn(|| {
                let _: u32 = traceforge::recv_msg_block();
                let _: u32 = traceforge::recv_msg_block();
            })
            .expect("Could not create sink");
        let sink_id = sink.thread().id();
        tids.push(sink);

        let receiver = thread::Builder::new()
            .name("receiver".to_owned())
            .spawn(move || {
                for _i in 0..n_test {
                    let _: u32 = traceforge::recv_msg_block();
                }
            })
            .expect("Could not create receiver");
        let receiver_id = receiver.thread().id();
        tids.push(receiver);

        let a = thread::Builder::new()
            .name("sender-a".to_owned())
            .spawn(move || {
                for _i in 0..n_test {
                    traceforge::send_msg(receiver_id, 0u32);
                }
                traceforge::send_msg(sink_id, 1u32);
            })
            .expect("Could not create sender-a");
        tids.push(a);

        let b = thread::Builder::new()
            .name("sender-b".to_owned())
            .spawn(move || {
                traceforge::send_msg(sink_id, 2u32);
            })
            .expect("Could not create sender-b");
        tids.push(b);

        for tid in tids {
            let _ = tid.join();
        }
    }
    let n_test = 5;
    let states = traceforge::estimate_execs_with_samples(move || foo(n_test), 5);
    assert_eq!(states, 2.0);
    let stats = traceforge::verify(Config::builder().build(), move || foo(n_test));
    println!("Stats = {}, {}", stats.execs, stats.block);
    assert_eq!(stats.execs, 2);
}

// =====================================================================
// Estimation-mode inbox support: visit_inbox_rfs now
// records the forward outcome fan-out (subsets + the applicable empty)
// as an EXECS_EST factor, samples one outcome, and pushes no forward
// revisits. Estimation cannot sample inbox BACKWARD revisits, so every
// shape below synchronizes (joins + a go-token) to have all sends
// committed before the inbox visit: the estimate is then exact.
// =====================================================================

// k=1 over three committed senders: 3 singleton subsets + timeout = 4.
// (Was 7 when the inbox still took a [min, max] range: the 3 size-2
// subsets are no longer outcomes now that k is exact.)
// Pre-fix this returned 1.0 (no factor) while each "sample" re-ran the
// whole subtree via pushed revisits.
#[test]
fn estimate_timed_inbox_k1() {
    let est = traceforge::estimate_execs_with_config(
        Config::builder().with_timed(0, 0, 1000).build(),
        || {
            let c = thread::spawn(|| {
                let _: u32 = traceforge::recv_tagged_msg_block(|_, t| t == Some(9));
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    1,
                    traceforge::WaitTime::Finite(10),
                );
            });
            let cid = c.thread().id();
            let senders: Vec<_> = (0u32..3)
                .map(|v| {
                    let cid = cid.clone();
                    thread::spawn(move || traceforge::send_tagged_msg(cid, 1, v))
                })
                .collect();
            for s in senders {
                let _ = s.join();
            }
            traceforge::send_tagged_msg(cid, 9, 0u32);
        },
        5,
    );
    assert!((est - 4.0).abs() < 1e-9, "estimate {est} != 4.0");
}

// Untimed non-blocking (min=0) inbox over one committed sender:
// one subset + the immediate empty = 2.
#[test]
fn estimate_untimed_inbox_min0() {
    let est = traceforge::estimate_execs_with_config(
        Config::builder().build(),
        || {
            let c = thread::spawn(|| {
                let _: u32 = traceforge::recv_tagged_msg_block(|_, t| t == Some(9));
                let _ = traceforge::inbox_with_tag_and_bounds(|_, t| t == Some(1), 0, None);
            });
            let cid = c.thread().id();
            let s = {
                let cid = cid.clone();
                thread::spawn(move || traceforge::send_tagged_msg(cid, 1, 1u32))
            };
            let _ = s.join();
            traceforge::send_tagged_msg(cid, 9, 0u32);
        },
        5,
    );
    assert!((est - 2.0).abs() < 1e-9, "estimate {est} != 2.0");
}

// Infinite-wait min=2 with a single sender: the estimation walk must
// terminate blocked (no subsets, no empty fallback) without hanging or
// pushing revisits. The pinned value is whatever the estimator reports
// for an all-blocked walk; the load-bearing assertion is termination
// with a finite estimate.
#[test]
fn estimate_infinite_inbox_blocks() {
    let est = traceforge::estimate_execs_with_config(
        Config::builder().with_timed(0, 0, 1000).build(),
        || {
            let c = thread::spawn(|| {
                let _ = traceforge::inbox_with_tag_timed(
                    |_, t| t == Some(1),
                    2,
                    traceforge::WaitTime::Infinite,
                );
            });
            traceforge::send_tagged_msg(c.thread().id(), 1, 1u32);
        },
        5,
    );
    assert!(est.is_finite(), "estimate {est} not finite");
}
