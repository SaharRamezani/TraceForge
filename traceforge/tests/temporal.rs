//! Tests for Must-τ (`temporal_*` API).
//!
//! These tests exercise the *new, additive* temporal API:
//!   - `temporal_sleep`
//!   - `temporal_recv_msg` / `temporal_recv_msg_with_wait`
//!   - `temporal_recv_msg_block` / `temporal_recv_msg_block_with_wait`
//!   - `Config::builder().with_transit_{lower,upper}_bound` / `with_storage_delay`
//!
//! The legacy receive primitives (`recv_msg`, `recv_msg_block`) are deliberately
//! NOT modified. If you want timestamp semantics on a specific receive, you
//! must use one of the `temporal_*` primitives below.

use traceforge::thread;
use traceforge::*;

mod utils;

// =====================================================================
// Sleep events: no-ops when temporal mode is off
// =====================================================================

/// One send + one blocking temporal receive = 1 execution. The sleep event is
/// recorded but inert because the config is not temporal.
#[test]
fn sleep_noop_without_temporal_config() {
    let stats = verify(Config::default(), || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block();
        });
        temporal_sleep(100);
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Sleep in the receiver thread, non-temporal mode.
#[test]
fn sleep_in_receiver_noop() {
    let stats = verify(Config::default(), || {
        let h = thread::spawn(|| {
            temporal_sleep(50);
            let _: i32 = temporal_recv_msg_block();
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Multiple sleeps in non-temporal mode: same execution count as without them.
#[test]
fn multiple_sleeps_noop() {
    let stats = verify(Config::default(), || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block();
        });
        temporal_sleep(10);
        temporal_sleep(20);
        send_msg(h.thread().id(), 1);
    });
    assert_eq!(stats.execs, 1);
}

// =====================================================================
// Legacy receives keep plain Must semantics under temporal mode
// =====================================================================

/// With a temporal [`Config`] but a legacy `recv_msg_block`, the receive has
/// `W = +∞`, so the "late send" scenario that would be pruned via a tight
/// `temporal_recv_msg_block_with_wait` is still feasible.
#[test]
fn legacy_recv_ignores_wait_time_under_temporal_mode() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(200)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = recv_msg_block();
        });
        temporal_sleep(100);
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

// =====================================================================
// Temporal consistency: transit bounds and wait time prune executions
// =====================================================================

/// Very wide bounds: all executions remain feasible.
#[test]
fn temporal_wide_bounds_all_feasible() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(u64::MAX / 2)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block();
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Sender sleeps 100 before sending; receiver's recv has `W = 10` and starts
/// ready at time 0. Then `recv ≥ 100` (rf lower bound) collides with
/// `recv ≤ 10` (wait time) → infeasible.
#[test]
fn temporal_tight_wait_prunes_late_send() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(200)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block_with_wait(10);
        });
        temporal_sleep(100);
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 0);
}

/// Same scenario but with a wait time wide enough to cover the delayed send.
#[test]
fn temporal_wide_wait_allows_late_send() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(300)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block_with_wait(300);
        });
        temporal_sleep(100);
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Upper bound on transit time prunes "expired" messages.
///
/// Send at 0, receiver sleeps 200, then receives. With `U = 50, sd = 0`,
/// `recv ≤ 0 + 50 = 50` but `recv ≥ 200` → infeasible.
#[test]
fn temporal_upper_bound_prunes_expired_message() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .with_storage_delay(0)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            temporal_sleep(200);
            let _: i32 = temporal_recv_msg_block();
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 0);
}

/// Storage delay widens message availability, rescuing feasibility:
/// `U = 50, sd = 200` → `recv ≤ 250`, and `recv = 200` fits.
#[test]
fn temporal_storage_delay_extends_availability() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .with_storage_delay(200)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            temporal_sleep(200);
            let _: i32 = temporal_recv_msg_block();
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Lower bound forces a minimum transit time: `L = 100` but `W = 50` → infeasible.
#[test]
fn temporal_lower_bound_enforces_minimum_transit() {
    let conf = Config::builder()
        .with_transit_lower_bound(100)
        .with_transit_upper_bound(200)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block_with_wait(50);
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 0);
}

/// Same lower bound, now with a wait time that satisfies it.
#[test]
fn temporal_lower_bound_with_sufficient_wait() {
    let conf = Config::builder()
        .with_transit_lower_bound(100)
        .with_transit_upper_bound(200)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block_with_wait(200);
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

// =====================================================================
// Rf filtering with multiple sends
// =====================================================================

/// Two sends, no temporal mode → 2 executions (either rf).
#[test]
fn no_temporal_two_sends_two_executions() {
    let stats = verify(Config::default(), || {
        let recv_h = thread::spawn(|| {
            temporal_sleep(40);
            let _: i32 = temporal_recv_msg_block();
        });
        let recv_id = recv_h.thread().id();
        send_msg(recv_id, 1);
        thread::spawn(move || {
            temporal_sleep(50);
            send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 2);
}

/// Under temporal mode the late sender is infeasible:
///  - sender A: send at 0
///  - sender B: sleep(50), then send (earliest at 50)
///  - receiver: sleep(40), recv with W = 5, so recv ≤ 45
/// rf from A is feasible (recv = 40 ≤ 45); rf from B is not (50 > 45).
#[test]
fn temporal_filters_one_of_two_sends() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(100)
        .with_storage_delay(0)
        .build();

    let stats = verify(conf, || {
        let recv_h = thread::spawn(|| {
            temporal_sleep(40);
            let _: i32 = temporal_recv_msg_block_with_wait(5);
        });
        let recv_id = recv_h.thread().id();
        send_msg(recv_id, 1);
        thread::spawn(move || {
            temporal_sleep(50);
            send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 1);
}

/// Backward revisits are pruned by the temporal filter as well.
#[test]
fn temporal_filters_backward_revisit() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block_with_wait(10);
        });
        let recv_id = h.thread().id();
        send_msg(recv_id, 1);
        thread::spawn(move || {
            temporal_sleep(200);
            send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 1);
}

// =====================================================================
// Non-blocking temporal receive
// =====================================================================

/// Non-blocking `temporal_recv_msg` + a send that is temporally infeasible:
/// only the timeout execution survives.
#[test]
fn temporal_nonblocking_timeout_always_feasible() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(50)
        .with_storage_delay(0)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            temporal_sleep(200);
            let _: Option<i32> = temporal_recv_msg();
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// Non-blocking without temporal mode: both timeout and read branches explored.
#[test]
fn no_temporal_nonblocking_two_executions() {
    let stats = verify(Config::default(), || {
        let h = thread::spawn(|| {
            temporal_sleep(200);
            let _: Option<i32> = temporal_recv_msg();
        });
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 2);
}

// =====================================================================
// Config builder
// =====================================================================

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
    assert_eq!(conf.transit_l(), 0);
    assert_eq!(conf.transit_u(), u64::MAX);
    assert_eq!(conf.sd(), 0);
}

#[test]
fn config_single_param_enables_temporal() {
    let conf = Config::builder().with_transit_lower_bound(5).build();
    assert!(conf.is_temporal());
    assert_eq!(conf.transit_l(), 5);
    assert_eq!(conf.transit_u(), u64::MAX);
    assert_eq!(conf.sd(), 0);
}

// =====================================================================
// Per-receive wait times
// =====================================================================

/// `temporal_recv_msg_block` defaults to `W = +∞`, so the tight-wait scenario
/// above becomes feasible when the wait bound is dropped.
#[test]
fn no_wait_defaults_to_unbounded() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(200)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block();
        });
        temporal_sleep(100);
        send_msg(h.thread().id(), 42);
    });
    assert_eq!(stats.execs, 1);
}

/// recv1 has a tight `W = 5`, recv2 has a wide `W = 1000`. Under `L = 0, U = 1000`
/// exactly one pairing is feasible: recv1 ← send_a (immediate),
/// recv2 ← send_b (after sleep(150)).
#[test]
fn different_wait_times_per_receive() {
    let conf = Config::builder()
        .with_transit_lower_bound(0)
        .with_transit_upper_bound(1000)
        .build();

    let stats = verify(conf, || {
        let h = thread::spawn(|| {
            let _: i32 = temporal_recv_msg_block_with_wait(5);
            temporal_sleep(100);
            let _: i32 = temporal_recv_msg_block_with_wait(1000);
        });
        let recv_id = h.thread().id();
        send_msg(recv_id, 1);
        thread::spawn(move || {
            temporal_sleep(150);
            send_msg(recv_id, 2);
        });
    });
    assert_eq!(stats.execs, 1);
}
