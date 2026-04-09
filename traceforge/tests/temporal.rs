/// Tests for Must-τ algorithm
///
/// These tests verify:
/// 1. Sleep events are correctly recorded in the execution graph
/// 2. Temporal consistency checking prunes infeasible executions
/// 3. Without temporal parameters, behavior is identical to original Must
/// 4. Transit bounds (L, U), storage delay (sd), and wait time (W) work correctly

use traceforge::thread;
use traceforge::*;

mod utils;

// ============================================================
// Basic sleep tests: sleep events should not affect non-temporal mode
// ============================================================

/// A simple program with a sleep event and a blocking receive.
/// Without temporal config, sleep is a no-op. One send, one blocking receive = 1 execution.
#[test]
fn sleep_noop_without_temporal_config() {
    let stats = traceforge::verify(Config::default(), || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::sleep(100);
        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Sleep in the receiver thread, non-temporal mode.
#[test]
fn sleep_in_receiver_noop() {
    let stats = traceforge::verify(Config::default(), || {
        let h = thread::spawn(move || {
            traceforge::sleep(50);
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Multiple sleeps in non-temporal mode: same execution count as without sleeps.
#[test]
fn multiple_sleeps_noop() {
    let stats = traceforge::verify(Config::default(), || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::sleep(10);
        traceforge::sleep(20);
        traceforge::send_msg(h.thread().id(), 1);
    });
    assert_eq!(stats.execs, 1);
}

// ============================================================
// Temporal consistency tests: transit bounds filter executions
// ============================================================

/// With very wide temporal bounds, all executions should remain feasible.
#[test]
fn temporal_wide_bounds_all_feasible() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(u64::MAX / 2)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Receiver's earliest recv time: 0 (from sleep(0)).
/// Sender's earliest send time: 100 (from sleep(100)).
/// rf lower bound: recv >= send + L = 100.
/// Wait time (per recv): recv <= prev_po(recv) + W = 0 + 10 = 10.
/// Contradiction: recv >= 100 but recv <= 10 → infeasible.
///
/// Blocking recv with no feasible rf → deadlock (0 execs, 1 blocked).
#[test]
fn temporal_tight_wait_prunes_late_send() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(200)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            traceforge::sleep(0);
            let _: i32 = traceforge::recv_msg_block_with_wait(10);
        });

        traceforge::sleep(100);
        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 0);
}

/// Same scenario but with a wide enough wait time → feasible.
#[test]
fn temporal_wide_wait_allows_late_send() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(200)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block_with_wait(200);
        });

        traceforge::sleep(200);
        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Upper bound on transit time prunes expired messages.
///
/// Sender sends at time 0. Receiver sleeps 200, then receives at earliest 200.
/// With U=50, sd=0: recv <= send + U + sd = 50. But recv earliest = 200 > 50 → infeasible.
#[test]
fn temporal_upper_bound_prunes_expired_message() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .with_storage_delay(0)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            traceforge::sleep(200);
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 0);
}

/// Storage delay extends message availability, making it feasible.
///
/// Sender sends at time 0. Receiver sleeps 200, then receives at earliest 200.
/// With U=50, sd=200: recv <= send + U + sd = 250. recv earliest = 200 <= 250 → feasible.
#[test]
fn temporal_storage_delay_extends_availability() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .with_storage_delay(200)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            traceforge::sleep(200);
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Lower bound on transit time enforces minimum transit.
///
/// Both threads start immediately. L=100, U=200, recv W=50.
/// recv >= send + L = 100. recv <= prev_po(recv) + W = 0 + 50 = 50.
/// Contradiction → infeasible.
#[test]
fn temporal_lower_bound_enforces_minimum_transit() {
    let conf = Config::builder()
        .with_transit_lower_bound(100)
        .with_transit_upper_bound(200)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block_with_wait(50);
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 0);
}

/// With a sufficient wait time, the lower bound is satisfied.
///
/// L=100, U=200, recv W=200. recv >= 100, recv <= 200. 100 <= recv <= 200 → feasible.
#[test]
fn temporal_lower_bound_with_sufficient_wait() {
    let conf = Config::builder()
        .with_transit_lower_bound(100)
        .with_transit_upper_bound(200)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block_with_wait(200);
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

// ============================================================
// Multiple sends: temporal bounds reduce the set of feasible rf options
// ============================================================

/// Two sends to the same receiver. Without temporal constraints and blocking recv,
/// the receiver can read from either → 2 executions.
#[test]
fn no_temporal_two_sends_two_executions() {
    let stats = traceforge::verify(Config::default(), || {
        let recv_h = thread::spawn(move || {
            traceforge::sleep(40);
            let _: i32 = traceforge::recv_msg_block();
        });

        let recv_id = recv_h.thread().id();

        traceforge::send_msg(recv_id, 1);

        thread::spawn(move || {
            traceforge::sleep(50);
            traceforge::send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 2);
}

/// With temporal constraints, only one of two sends is feasible.
///
/// Sender A sends at time 0. Sender B: sleep(50), sends at earliest 50.
/// Receiver: sleep(40), then recv with W=5 at earliest 40.
/// L=0, U=100, sd=0:
///   rf from B: recv <= 40 + 5 = 45. recv >= 50. 50 > 45 → infeasible!
///   rf from A: recv <= 40 + 5 = 45. recv >= 0. recv = 40, 40 <= 45 → feasible.
#[test]
fn temporal_filters_one_of_two_sends() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(100)
        .with_storage_delay(0)
        .build();

    let stats = traceforge::verify(conf, || {
        let recv_h = thread::spawn(move || {
            traceforge::sleep(40);
            let _: i32 = traceforge::recv_msg_block_with_wait(5);
        });

        let recv_id = recv_h.thread().id();

        // Sender A: sends immediately
        traceforge::send_msg(recv_id, 1);

        // Sender B: sleeps then sends
        thread::spawn(move || {
            traceforge::sleep(50);
            traceforge::send_msg(recv_id, 2);
        });
    });
    // Only rf from A is feasible
    assert_eq!(stats.execs, 1);
}

// ============================================================
// Backward revisit filtering by temporal consistency
// ============================================================

/// Backward revisits are also filtered by temporal consistency.
///
/// Main sends at time 0. Thread B: sleep(200), sends.
/// Receiver: blocking recv with W=10. L=0, U=50.
///
/// rf from main: recv >= 0, recv <= 50, recv <= 0+10=10. recv in [0,10]
/// rf from B: recv >= 200, recv <= 250, recv <= 0+10=10. 200 > 10 → infeasible.
#[test]
fn temporal_filters_backward_revisit() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block_with_wait(10);
        });

        let recv_id = h.thread().id();
        traceforge::send_msg(recv_id, 1);

        thread::spawn(move || {
            traceforge::sleep(200);
            traceforge::send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 1);
}

// ============================================================
// Non-blocking receive with temporal constraints
// ============================================================

/// Non-blocking receive with temporal constraints.
/// The timeout (rf=None) case is always feasible.
/// One send that is temporally infeasible: only timeout executes.
#[test]
fn temporal_nonblocking_timeout_always_feasible() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .with_storage_delay(0)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            traceforge::sleep(200);
            let _: Option<i32> = traceforge::recv_msg();
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Non-blocking receive without temporal constraints gives both read and timeout.
#[test]
fn no_temporal_nonblocking_two_executions() {
    let stats = traceforge::verify(Config::default(), || {
        let h = thread::spawn(move || {
            traceforge::sleep(200);
            let _: Option<i32> = traceforge::recv_msg();
        });

        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 2);
}

// ============================================================
// Config builder tests
// ============================================================

#[test]
fn config_builder_network_temporal_params() {
    let conf = Config::builder()
        .with_transit_lower_bound(10)
        .with_transit_upper_bound(100)
        .with_storage_delay(5)
        .build();

    assert!(conf.is_temporal());
    assert_eq!(conf.transit_l(), 10);
    assert_eq!(conf.transit_u(), 100);
    assert_eq!(conf.sd(), 5);
}

#[test]
fn config_default_not_temporal() {
    let conf = Config::default();
    assert!(!conf.is_temporal());
}

#[test]
fn config_single_param_enables_temporal() {
    let conf = Config::builder().with_transit_lower_bound(5).build();
    assert!(conf.is_temporal());
    assert_eq!(conf.transit_l(), 5);
    assert_eq!(conf.transit_u(), u64::MAX);
    assert_eq!(conf.sd(), 0);
}

// ============================================================
// Per-receive wait time: defaults to unbounded
// ============================================================

/// A plain `recv_msg_block` (no wait specified) defaults to W = ∞, so
/// even in temporal mode it will block forever for the message to arrive
/// rather than time out. This test ensures that a scenario which would
/// be pruned with a tight W is still feasible without one.
#[test]
fn no_wait_defaults_to_unbounded() {
    // Same scenario as `temporal_tight_wait_prunes_late_send`, but the
    // receive uses plain `recv_msg_block` (W = ∞). It must remain feasible.
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(200)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            traceforge::sleep(0);
            let _: i32 = traceforge::recv_msg_block();
        });

        traceforge::sleep(100);
        traceforge::send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// recv1 has a tight W=5, recv2 has a wide W=1000. recv1 must read from
/// send_a (immediate), recv2 must read from send_b (after sleep(100)).
/// L=0, U=1000.
#[test]
fn different_wait_times_per_receive() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(1000)
        .build();

    let stats = traceforge::verify(conf, || {
        let h = thread::spawn(move || {
            let _: i32 = traceforge::recv_msg_block_with_wait(5);
            traceforge::sleep(100);
            let _: i32 = traceforge::recv_msg_block_with_wait(1000);
        });

        let recv_id = h.thread().id();

        // Immediate send for the first recv
        traceforge::send_msg(recv_id, 1);

        // Late send for the second recv
        thread::spawn(move || {
            traceforge::sleep(150);
            traceforge::send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 1);
}
