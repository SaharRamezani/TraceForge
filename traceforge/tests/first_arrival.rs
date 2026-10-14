//! First-arrival reception discipline of the timed semantics
//! (2026-10-13, Sahar's ruling: "whichever arrived first must be read
//! and we do not wait to see the other one").
//!
//! A receive that reads `s` takes the message that ARRIVED FIRST among
//! those pending at it: every other matching message `b` either
//! arrives no earlier than `s` (a tie leaves both orders to the
//! exploration) or was already discarded when the wait began
//! (`a_b + sd < t_p`). Before the ruling only messages ordered before
//! `s` on the same channel were protected; a waiting receive could
//! read a later arrival of ANOTHER sender and let the earlier one
//! expire unread. Those executions are not behaviours of the
//! semantics, so no execution below contains one.
//!
//! Every count is hand-derived from the delivered subsets of the
//! (lossy) sends. "HEAD" is the commit before the ruling, whose counts
//! are given where they differ.

use traceforge::thread;
use traceforge::*;

fn run(l: u64, u: u64, sd: u64, body: fn()) -> Stats {
    traceforge::verify(Config::builder().with_timed(l, u, sd).build(), body)
}

fn counts(s: &Stats) -> (usize, usize, usize) {
    (s.execs, s.block, s.timeline_impossible)
}

/// m1 arrives at 0, m2 at 5; the receiver waits from 0. Delivered
/// {m1, m2}: the receive reads m1 at 0 and only then (HEAD: also m2 at
/// 5, with m1 expiring unread, which is the world the ruling removes:
/// execs 4). {m1}, {m2}: one read each; {}: the receive blocks.
#[test]
fn later_arrival_of_another_sender_is_not_read_over_an_earlier_one() {
    let stats = run(0, 0, 0, || {
        let receiver = thread::spawn(|| {
            let _: u32 = traceforge::recv_msg_block_timed();
        });
        let r = receiver.thread().id();
        thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_msg(r, 2u32);
        });
        traceforge::send_msg(r, 1u32);
    });
    assert_eq!(counts(&stats), (3, 1, 0));
}

/// Same shape with a finite wait: the receive returns the first
/// arrival (HEAD: 5 executions, the extra one reading m2 at 5 with m1
/// expired).
#[test]
fn finite_wait_reads_the_first_arrival() {
    let stats = run(0, 0, 0, || {
        let receiver = thread::spawn(|| {
            let _: Option<u32> = traceforge::recv_msg_timed(WaitTime::Finite(10));
        });
        let r = receiver.thread().id();
        thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_msg(r, 2u32);
        });
        thread::spawn(move || {
            traceforge::sleep(2);
            traceforge::send_msg(r, 1u32);
        });
    });
    // {m1, m2}: m1; {m1}: m1; {m2}: m2; {}: timeout.
    assert_eq!(counts(&stats), (4, 0, 0));
}

/// Both messages arrive at the same instant: nothing arrived first,
/// both orders are explored (unchanged by the ruling).
#[test]
fn simultaneous_arrivals_keep_both_orders() {
    let stats = run(0, 0, 0, || {
        let receiver = thread::spawn(|| {
            let _: u32 = traceforge::recv_msg_block_timed();
        });
        let r = receiver.thread().id();
        thread::spawn(move || traceforge::send_msg(r, 2u32));
        traceforge::send_msg(r, 1u32);
    });
    // {m1, m2}: two reads; {m1}, {m2}: one each; {}: blocked.
    assert_eq!(counts(&stats), (4, 1, 0));
}

/// A message that died before the wait began is not passed over (the
/// GC rule of 2026-08-08): m1 arrives at 0 and is gone at 10, when the
/// receive starts; m2 arrives at 15. Unchanged by the ruling.
#[test]
fn a_message_dead_before_the_wait_is_not_passed_over() {
    let stats = run(0, 0, 0, || {
        let receiver = thread::spawn(|| {
            traceforge::sleep(10);
            let v: u32 = traceforge::recv_msg_block_timed();
            traceforge::assert(v == 2);
        });
        let r = receiver.thread().id();
        thread::spawn(move || {
            traceforge::sleep(15);
            traceforge::send_msg(r, 2u32);
        });
        traceforge::send_msg(r, 1u32);
    });
    // m2 delivered: it is read whatever happened to m1; m2 dropped:
    // m1 is dead, the receive blocks.
    assert_eq!(counts(&stats), (2, 2, 0));
}

/// Stored messages are read in arrival order: with sd = 10 both are
/// alive when the receiver wakes at 8, and the first receive must take
/// m1 (arrived 0), not m2 (arrived 5). HEAD reads m2 first in one of
/// the executions and the assertion fires.
#[test]
fn stored_messages_are_read_in_arrival_order() {
    let stats = run(0, 0, 10, || {
        let receiver = thread::spawn(|| {
            traceforge::sleep(8);
            let a: u32 = traceforge::recv_msg_block_timed();
            let b: u32 = traceforge::recv_msg_block_timed();
            traceforge::assert(a < b);
        });
        let r = receiver.thread().id();
        thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_msg(r, 2u32);
        });
        traceforge::send_msg(r, 1u32);
    });
    // {m1, m2}: one execution; {m1}, {m2}: blocked at the second read;
    // {}: blocked at the first.
    assert_eq!(counts(&stats), (1, 3, 0));
}

/// The receive of another tag is no candidate: a tagged receive that
/// waits for tag 7 does not have to read the earlier tag-9 message
/// (selection by tag is not passing over).
#[test]
fn a_message_of_another_tag_is_not_a_candidate() {
    let stats = run(0, 0, 0, || {
        let receiver = thread::spawn(|| {
            let _: u32 = traceforge::recv_tagged_msg_block_timed(|_, t| t == Some(7));
        });
        let r = receiver.thread().id();
        thread::spawn(move || {
            traceforge::sleep(5);
            traceforge::send_tagged_msg(r, 7, 2u32);
        });
        traceforge::send_tagged_msg(r, 9, 1u32);
    });
    // The tag-9 message never matters: delivered or not, the read is the
    // tag-7 message, or the receive blocks when it is dropped.
    assert_eq!(counts(&stats), (2, 2, 0));
}
