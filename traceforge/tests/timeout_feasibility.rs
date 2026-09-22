//! (C6') Timeout feasibility: a finite-wait receive may return `None`
//! only in timelines where every message it could have consumed missed
//! its wait window `[t0, t0 + W]`.
//!
//! Before this constraint the `rf = None` branch was unconstrained, so
//! a receive could time out while a matching message sat readable in
//! its window. That produced CERTIFIED counterexamples for worlds that
//! cannot happen (the witness in `test_no_timeout_when_message_must_be_readable`
//! is the one Sahar reported on 2026-09-16).
//!
//! The constraint is enforced by the completion/certification oracle
//! only, never during pruning, so the exploration tree is unchanged:
//! an impossible timeout is still explored and then dropped at the end
//! (it stops being counted in `execs`, and any assert on it is
//! suppressed instead of reported).
//!
//! Every expected count below is derived by hand from the semantics,
//! not read off the implementation.

use traceforge::*;

fn cfg(l: u64, u: u64, sd: u64) -> Config {
    Config::builder().with_timed(l, u, sd).build()
}

/// L=0, U=1, sd=0, W=10: the message arrives by 1 and the receive
/// waits until 10, so no timeline lets it miss. The timeout branch is
/// still explored but must be dropped at completion: one execution,
/// and the assert must NOT fire.
#[test]
fn test_no_timeout_when_message_must_be_readable() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(got.is_some());
    });
    assert_eq!(
        stats.execs, 1,
        "only the read is a real world; the timeout has no timeline"
    );
}

/// Same program with U = 20 > W = 10: the message may genuinely arrive
/// after the deadline, so the timeout is a real world and the assert
/// is a real violation. This is the control that the fix did not just
/// delete the branch.
#[test]
#[should_panic(expected = "assertion failed")]
fn test_timeout_survives_when_message_can_be_late() {
    let _ = verify(cfg(0, 20, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(got.is_some());
    });
}

/// The tie, which pins the HALF-OPEN form of the constraint: with
/// L = U = W the message arrives exactly at the deadline, and the
/// timer may win that race. Both worlds stay: read (a_b = t_e is still
/// inside the storage window) and timeout.
///
/// With the strict form `a_b > t_e` the timeout would vanish here, and
/// every PAR tie cell `To = dK + dL + dR` would turn into a false hold.
#[test]
fn test_arrival_exactly_at_deadline_keeps_both_worlds() {
    let stats = verify(cfg(5, 5, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let _ = recv_msg_timed::<u32>(WaitTime::Finite(5));
    });
    assert_eq!(
        stats.execs, 2,
        "arrival exactly at the deadline races the timer: read and timeout"
    );
}

/// A message that died before the wait began cannot be blamed for the
/// timeout: sd = 0 and L = U = 0 put the whole storage window at time
/// 0, while the receive starts at 5 after a sleep. Only the timeout
/// survives (the read is impossible), so the assert fires.
#[test]
#[should_panic(expected = "assertion failed")]
fn test_message_dead_before_the_wait_began_allows_timeout() {
    let _ = verify(cfg(0, 0, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        sleep(5);
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(got.is_some());
    });
}

/// Two matching messages, both certain to be readable in the window:
/// dodging one is not enough, so the timeout has no timeline at all.
#[test]
fn test_every_candidate_must_miss_the_window() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
            send_msg(me, 2u32);
        });
        let got = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(got.is_some());
    });
    assert_eq!(
        stats.execs, 1,
        "FIFO: only the first message is readable first; no timeout world"
    );
}

/// A timeout followed by a blocking read of the same message (the
/// timeout-validation idiom). The constraint now does the work the
/// idiom used to do by hand: the timeout forces the message late, and
/// the later read picks it up. The world stays feasible, so this must
/// not regress into a blocked or dropped execution.
#[test]
fn test_timeout_then_late_read_stays_feasible() {
    let stats = verify(cfg(0, 20, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        if recv_msg_timed::<u32>(WaitTime::Finite(10)).is_none() {
            // Only reachable when the message was still in flight.
            let late = recv_msg_block::<u32>();
            assert(late == 1);
        }
    });
    assert!(
        stats.execs >= 2,
        "both the in-window read and the late read must survive, got {}",
        stats.execs
    );
}

/// A message consumed by an earlier receive of the same thread is gone
/// and cannot forbid a later timeout.
#[test]
fn test_message_consumed_earlier_does_not_forbid_timeout() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let first = recv_msg_block::<u32>();
        assert(first == 1);
        let second = recv_msg_timed::<u32>(WaitTime::Finite(10));
        assert(second.is_none());
    });
    assert_eq!(stats.execs, 1, "nothing is left to read: the timeout stands");
}

/// No matching message at all: the timeout is unconditional.
#[test]
fn test_timeout_with_no_sender_is_unconstrained() {
    let stats = verify(cfg(0, 5, 0), || {
        let got = recv_msg_timed::<u32>(WaitTime::Finite(3));
        assert(got.is_none());
    });
    assert_eq!(stats.execs, 1);
}

/// A collector of one is a receive by another name: with the message
/// certain to arrive inside the window, its empty (timeout) result has
/// no timeline either. This also covers the vouching hazard: an inbox
/// visit must not vouch for a graph whose inbox timed out, or the
/// stricter completion check would be skipped.
#[test]
fn test_inbox_of_one_timeout_obeys_the_same_rule() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = inbox_timed(1, WaitTime::Finite(10));
        assert(!got.is_empty());
    });
    assert_eq!(
        stats.execs, 1,
        "a readable message completes a batch of one: no empty world"
    );
}

/// The same collector when the message may genuinely be late: the
/// empty result is a real world, so the assert is a real violation.
#[test]
#[should_panic(expected = "assertion failed")]
fn test_inbox_of_one_timeout_survives_when_late() {
    let _ = verify(cfg(0, 20, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = inbox_timed(1, WaitTime::Finite(10));
        assert(!got.is_empty());
    });
}

/// A collector of two with only one message available times out for a
/// reason the constraint deliberately does not model (a cardinality
/// condition a difference system cannot express), so that branch stays
/// unconstrained. Pinned so the exemption is a decision, not a drift.
#[test]
fn test_inbox_of_two_timeout_stays_unconstrained() {
    let stats = verify(cfg(0, 1, 0), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || {
            send_msg(me, 1u32);
        });
        let got = inbox_timed(2, WaitTime::Finite(10));
        assert(got.is_empty());
    });
    assert_eq!(
        stats.execs, 1,
        "one message can never complete a batch of two: only the timeout"
    );
}
