// Probes for the timeout kill (`with_kill_dead_timeouts`), each run
// under the reference (explore-then-discard) and the kill, comparing
// the number of counted executions. L=0, U=4 unless stated.
//
// six_lossy: the 2026-09-28 drawing. R = recv_msg_timed(Finite(5))
// waits [0,5]; S_b lossy, arrives in [0,4] (unmissable when
// delivered); S_s lossy, sent after sleep(2), arrives in [2,6]
// (missable). Both may be lost (budget 2). Expected executions:
//   R reads S_b, S_s delivered      R reads S_b, S_s lost
//   R reads S_s, S_b delivered      R reads S_s, S_b lost
//   R timeout, S_b lost, S_s lost   R timeout, S_b lost, S_s delivered
// and never "R timeout with S_b delivered".

use traceforge::thread;
use traceforge::{Config, SchedulePolicy, Stats, WaitTime};

struct Printer;
impl log::Log for Printer {
    fn enabled(&self, m: &log::Metadata) -> bool {
        m.level() <= log::Level::Info
    }
    fn log(&self, r: &log::Record) {
        if self.enabled(r.metadata()) {
            let s = format!("{}", r.args());
            if s.contains("[kill") {
                println!("LOG {s}");
            }
        }
    }
    fn flush(&self) {}
}
static PRINTER: Printer = Printer;

fn run<F>(name: &str, kill: bool, lossy: usize, verbose: usize, f: F) -> Stats
where
    F: Fn() + Send + Sync + 'static,
{
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    println!("==== {name} kill={kill} ====");
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 4, 0)
            .with_lossy(lossy)
            .with_kill_dead_timeouts(kill)
            .with_policy(SchedulePolicy::LTR)
            .with_verbose(verbose)
            .build(),
        f,
    );
    println!(
        "STATS {name} kill={kill} execs={} block={} timeline_impossible={} killed={}",
        stats.execs, stats.block, stats.timeline_impossible, stats.killed
    );
    stats
}

/// Same program under both modes: the counted executions must agree,
/// and the kill must leave no dead ending to discard.
fn both<F>(name: &str, lossy: usize, expected: usize, f: F)
where
    F: Fn() + Send + Sync + Clone + 'static,
{
    let reference = run(name, false, lossy, 0, f.clone());
    let killed = run(name, true, lossy, 0, f);
    assert_eq!(reference.execs, expected, "{name}: reference count");
    assert_eq!(killed.execs, expected, "{name}: kill count");
    assert_eq!(killed.timeline_impossible, 0, "{name}: kill left dead endings");
    assert_eq!(reference.block + killed.block, 0, "{name}: blocked endings");
}

fn six_lossy(b_first: bool) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
        });
        let rid = r.thread().id();
        let sb = move || traceforge::send_lossy_msg(rid, 1u32);
        let ss = move || {
            traceforge::sleep(2);
            traceforge::send_lossy_msg(rid, 2u32);
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

#[test]
fn six_lossy_b_first_verbose() {
    let s = run("six_lossy_b_first", true, 2, 1, six_lossy(true));
    assert_eq!((s.execs, s.timeline_impossible), (6, 0));
}

#[test]
fn six_lossy_both_orders() {
    both("six_lossy_b_first", 2, 6, six_lossy(true));
    both("six_lossy_s_first", 2, 6, six_lossy(false));
}

/// The P2 program without loss: R reads S_b or S_s, never times out.
fn p2(b_first: bool) -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
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

#[test]
fn p2_both_orders() {
    both("p2_b_first", 0, 2, p2(true));
    both("p2_s_first", 0, 2, p2(false));
}

/// Two receives: r (killed by s, may read the missable x) and r2 (may
/// read the missable y or time out). Four executions.
fn two_receives() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
        });
        let r2 = thread::spawn(|| {
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
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

#[test]
fn two_receives_four() {
    both("two_receives", 0, 4, two_receives());
}

/// Five senders with windows [0,1], [1,2], [2,3], [3,4], [4,5] to a
/// receiver waiting [0,5]: each one alone kills the timeout. Five
/// executions, no timeout.
fn five_candidates() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
        });
        let rid = r.thread().id();
        let senders: Vec<_> = (0..5u64)
            .map(|i| {
                thread::spawn(move || traceforge::send_msg_timed(rid, i as u32, i, i + 1))
            })
            .collect();
        let _ = r.join();
        for t in senders {
            let _ = t.join();
        }
    }
}

#[test]
fn five_candidates_five() {
    both("five_candidates", 0, 5, five_candidates());
}

/// Receiver waiting [2,5]; A arrives in [0,3], B in [3,10]. The
/// timeout is feasible (A early, B late), so no kill fires: three
/// executions in both modes and `killed` stays 0.
fn late_windows() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            traceforge::sleep(2);
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(3));
        });
        let rid = r.thread().id();
        let a = thread::spawn(move || traceforge::send_msg_timed(rid, 1u32, 0, 3));
        let b = thread::spawn(move || traceforge::send_msg_timed(rid, 2u32, 3, 10));
        let _ = r.join();
        let _ = a.join();
        let _ = b.join();
    }
}

/// One seed of the Arbitrary policy, verbose, for reading the tree.
/// SEED and KILL (1/0) come from the environment.
#[test]
fn arbitrary_seed_verbose() {
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    let seed: u64 = std::env::var("SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let kill: bool = std::env::var("KILL").map(|s| s == "1").unwrap_or(true);
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 4, 0)
            .with_lossy(2)
            .with_kill_dead_timeouts(kill)
            .with_policy(SchedulePolicy::Arbitrary)
            .with_seed(seed)
            .with_verbose(3)
            .build(),
        six_lossy(true),
    );
    println!(
        "STATS six_lossy arbitrary seed={seed} kill={kill} execs={} timeline_impossible={} killed={}",
        stats.execs, stats.timeline_impossible, stats.killed
    );
}

/// Scheduler independence: the kill under the Arbitrary policy, several
/// seeds, must count the same executions as under LTR.
#[test]
fn arbitrary_policy_counts() {
    let _ = log::set_logger(&PRINTER);
    for seed in 0..8u64 {
        for (name, expected, lossy, f) in [
            ("six_lossy", 6usize, 2usize, Box::new(six_lossy(true)) as Box<dyn Fn() + Send + Sync>),
            ("two_receives", 4, 0, Box::new(two_receives())),
            ("five_candidates", 5, 0, Box::new(five_candidates())),
        ] {
            let stats = traceforge::verify(
                Config::builder()
                    .with_timed(0, 4, 0)
                    .with_lossy(lossy)
                    .with_kill_dead_timeouts(true)
                    .with_policy(SchedulePolicy::Arbitrary)
                    .with_seed(seed)
                    .build(),
                f,
            );
            println!(
                "STATS {name} arbitrary seed={seed} execs={} timeline_impossible={} killed={}",
                stats.execs, stats.timeline_impossible, stats.killed
            );
            assert_eq!(stats.execs, expected, "{name} seed {seed}");
            assert_eq!(stats.timeline_impossible, 0, "{name} seed {seed}");
        }
    }
}

#[test]
fn late_windows_three_no_kill() {
    both("late_windows", 0, 3, late_windows());
    let s = run("late_windows", true, 0, 0, late_windows());
    assert_eq!(s.killed, 0);
}

/// One run for the external sweep driver: PROG, BFIRST (1/0), POLICY
/// (ltr/arb), SEED, KILL (1/0) from the environment; prints every
/// counted execution (verbose 1) and the STATS line.
#[test]
fn sweep_one() {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let prog = env("PROG", "six_lossy");
    let b_first = env("BFIRST", "1") == "1";
    let arb = env("POLICY", "ltr") == "arb";
    let seed: u64 = env("SEED", "0").parse().unwrap();
    let kill = env("KILL", "1") == "1";
    let (lossy, f): (usize, Box<dyn Fn() + Send + Sync>) = match prog.as_str() {
        "six_lossy" => (2, Box::new(six_lossy(b_first))),
        "p2" => (0, Box::new(p2(b_first))),
        "two_receives" => (0, Box::new(two_receives())),
        "five_candidates" => (0, Box::new(five_candidates())),
        "late_windows" => (0, Box::new(late_windows())),
        other => panic!("unknown PROG {other}"),
    };
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 4, 0)
            .with_lossy(lossy)
            .with_kill_dead_timeouts(kill)
            .with_policy(if arb { SchedulePolicy::Arbitrary } else { SchedulePolicy::LTR })
            .with_seed(seed)
            .with_verbose(1)
            .build(),
        f,
    );
    println!(
        "STATS execs={} block={} timeline_impossible={} killed={}",
        stats.execs, stats.block, stats.timeline_impossible, stats.killed
    );
}

/// FIFO channel, two sends from the same thread: m1 (window [3,8]) then
/// m2 (window [0,4]). R waits [0,5].
///
/// At R's visit only m1 has been sent: m1 alone does not kill (some
/// timeline has a_m1 = 7 > 5, R legitimately times out).
///
/// When m2 is sent, FIFO couples a_m1 <= a_m2 <= 4, so a_m1 <= 4 too:
/// no timeline now lets R miss BOTH, so m2 kills the timeout. But m2 is
/// not sb-minimal while m1 (sb-before m2, unread) is still a candidate:
/// forcing "R reads m2" directly is not a legal read. The correct base
/// after the kill is m1 (FIFO's actual next message), not m2.
fn fifo_two_sends() -> impl Fn() + Clone + Send + Sync + 'static {
    move || {
        let r = thread::spawn(|| {
            let v: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
            println!("ENDING R got {v:?}");
        });
        let rid = r.thread().id();
        let s = thread::spawn(move || {
            traceforge::send_msg_timed(rid, 1u32, 3, 8); // m1
            traceforge::send_msg_timed(rid, 2u32, 0, 4); // m2
        });
        let _ = r.join();
        let _ = s.join();
    }
}

#[test]
fn fifo_two_sends_verbose() {
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 4, 0)
            .with_cons_type(traceforge::ConsType::FIFO)
            .with_kill_dead_timeouts(true)
            .with_policy(SchedulePolicy::LTR)
            .with_verbose(1)
            .build(),
        fifo_two_sends(),
    );
    println!(
        "STATS execs={} block={} timeline_impossible={} killed={}",
        stats.execs, stats.block, stats.timeline_impossible, stats.killed
    );
}

/// Reference (kill off) on the same FIFO program: the timeout world is
/// explored to its end and must be judged timeline-impossible there,
/// independently of the kill machinery.
#[test]
fn fifo_two_sends_reference() {
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 4, 0)
            .with_cons_type(traceforge::ConsType::FIFO)
            .with_kill_dead_timeouts(false)
            .with_policy(SchedulePolicy::LTR)
            .with_verbose(2)
            .build(),
        fifo_two_sends(),
    );
    println!(
        "STATS execs={} block={} timeline_impossible={} killed={}",
        stats.execs, stats.block, stats.timeline_impossible, stats.killed
    );
}

/// Same program WITHOUT m2: now a_m1 ranges over [3,8] freely, the
/// timeout (a_m1 > 5) is a real behaviour, and the kill must NOT fire.
#[test]
fn fifo_one_send_timeout_survives() {
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    for kill in [false, true] {
        let stats = traceforge::verify(
            Config::builder()
                .with_timed(0, 4, 0)
                .with_cons_type(traceforge::ConsType::FIFO)
                .with_kill_dead_timeouts(kill)
                .with_policy(SchedulePolicy::LTR)
                .build(),
            move || {
                let r = thread::spawn(|| {
                    let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                });
                let rid = r.thread().id();
                let s = thread::spawn(move || {
                    traceforge::send_msg_timed(rid, 1u32, 3, 8); // m1 only
                });
                let _ = r.join();
                let _ = s.join();
            },
        );
        println!(
            "STATS kill={kill} execs={} timeline_impossible={} killed={}",
            stats.execs, stats.timeline_impossible, stats.killed
        );
    }
}

/// The two-send FIFO program plus a thread T that branches (two coin
/// flips) AFTER the sender. In the reference, T's whole fan-out is
/// re-explored inside the dead timeout world (each leaf discarded); the
/// kill cuts that world at m2's send, before T ever runs there.
#[test]
fn fifo_two_sends_with_fanout() {
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    for kill in [false, true] {
        let stats = traceforge::verify(
            Config::builder()
                .with_timed(0, 4, 0)
                .with_cons_type(traceforge::ConsType::FIFO)
                .with_kill_dead_timeouts(kill)
                .with_policy(SchedulePolicy::LTR)
                .build(),
            move || {
                let r = thread::spawn(|| {
                    let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(5));
                });
                let rid = r.thread().id();
                let s = thread::spawn(move || {
                    traceforge::send_msg_timed(rid, 1u32, 3, 8); // m1
                    traceforge::send_msg_timed(rid, 2u32, 0, 4); // m2
                });
                let t = thread::spawn(|| {
                    let _: bool = traceforge::nondet();
                    let _: bool = traceforge::nondet();
                });
                let _ = r.join();
                let _ = s.join();
                let _ = t.join();
            },
        );
        println!(
            "STATS kill={kill} execs={} timeline_impossible={} killed={}",
            stats.execs, stats.timeline_impossible, stats.killed
        );
    }
}

/// Choice-killed timeout, distilled from fuzz program 612. Timed
/// (L=0, U=2, sd=1), FIFO. R runs r0 = recv_timed(2) then r1 =
/// blocking recv. S sends m1 at t=0 (arrival in [0,2]) and m2 at t=3
/// (arrival in [3,5]).
///
/// r0's timeout needs the deadline tie a_m1 = 2 (m1 arrives exactly at
/// the deadline and loses the race). No send ever kills that timeout:
/// m1 allows it, m2 misses the window anyway. The dead branch is
/// "r0 TIMEOUT, r1 reads m2": skipping m1 needs m1 dead before r1's
/// wait (a_m1 + 1 < 2, i.e. a_m1 < 1), contradicting a_m1 = 2. The
/// contradiction is created by r1's READ CHOICE, not by a send, so the
/// kill has no moment to fire at and both modes explore-and-discard it.
#[test]
fn choice_killed_timeout() {
    let _ = log::set_logger(&PRINTER);
    log::set_max_level(log::LevelFilter::Info);
    for kill in [false, true] {
        let stats = traceforge::verify(
            Config::builder()
                .with_timed(0, 2, 1)
                .with_cons_type(traceforge::ConsType::FIFO)
                .with_kill_dead_timeouts(kill)
                .with_policy(SchedulePolicy::LTR)
                .build(),
            move || {
                let r = thread::spawn(|| {
                    let a: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(2));
                    let b: u32 = traceforge::recv_msg_block_timed();
                    println!("ENDING r0={a:?} r1={b}");
                });
                let rid = r.thread().id();
                let s = thread::spawn(move || {
                    traceforge::send_msg(rid, 1u32); // m1 at t=0
                    traceforge::sleep(3);
                    traceforge::send_msg(rid, 2u32); // m2 at t=3
                });
                let _ = r.join();
                let _ = s.join();
            },
        );
        println!(
            "STATS kill={kill} execs={} timeline_impossible={} killed={}",
            stats.execs, stats.timeline_impossible, stats.killed
        );
    }
}

/// The 2026-09-28 drawing of the choice-killed program: L=0, U=2,
/// sd=1, FIFO, both sends lossy. R: r0 = recv_timed(2), then r1 =
/// blocking recv. S: m1 at t=0, sleep(3), m2. verbose 2 prints every
/// counted, blocked and discarded ending.
#[test]
fn choice_killed_lossy_drawing() {
    let kill = std::env::var("KILL").map(|s| s == "1").unwrap_or(false);
    let stats = traceforge::verify(
        Config::builder()
            .with_timed(0, 2, 1)
            .with_lossy(2)
            .with_cons_type(traceforge::ConsType::FIFO)
            .with_kill_dead_timeouts(kill)
            .with_policy(SchedulePolicy::LTR)
            .with_verbose(2)
            .build(),
        move || {
            let r = thread::spawn(|| {
                let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(2));
                let _: u32 = traceforge::recv_msg_block_timed();
            });
            let rid = r.thread().id();
            let s = thread::spawn(move || {
                traceforge::send_lossy_msg(rid, 1u32);
                traceforge::sleep(3);
                traceforge::send_lossy_msg(rid, 2u32);
            });
            let _ = r.join();
            let _ = s.join();
        },
    );
    println!(
        "STATS kill={kill} execs={} block={} timeline_impossible={} killed={}",
        stats.execs, stats.block, stats.timeline_impossible, stats.killed
    );
}
