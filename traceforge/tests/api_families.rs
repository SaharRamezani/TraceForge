//! A program is either timed or untimed: under `Config::with_timed`
//! every receive and inbox must use a `*_timed` variant. Sends need no
//! variant (a send without explicit bounds takes the configured
//! `[L, U]`), and monitor threads are exempt (their untimed reads
//! observe copies without constraining the program's timelines).

use traceforge::thread;
use traceforge::{Config, WaitTime, inbox_timed, recv_msg, recv_msg_block, recv_msg_block_timed, recv_msg_timed, send_msg, verify};

fn timed() -> Config {
    Config::builder().with_timed(0, 1, 0).build()
}

fn untimed() -> Config {
    Config::builder().build()
}

fn one_sender_program<R: Fn() -> Option<u32> + Send + Sync + 'static>(recv: R) -> impl Fn() + Send + Sync + 'static {
    let recv = std::sync::Arc::new(recv);
    move || {
        let me = thread::current().id();
        let _s = thread::spawn(move || send_msg(me, 7u32));
        let _ = recv();
    }
}

#[test]
#[should_panic(expected = "untimed receive (recv_msg_block / recv_tagged_msg_block / select_msg_block) in a timed configuration")]
fn untimed_blocking_receive_is_rejected_under_a_timed_configuration() {
    verify(timed(), one_sender_program(|| Some(recv_msg_block::<u32>())));
}

#[test]
#[should_panic(expected = "untimed receive (recv_msg / recv_tagged_msg / select_msg) in a timed configuration")]
fn untimed_non_blocking_receive_is_rejected_under_a_timed_configuration() {
    verify(timed(), one_sender_program(|| recv_msg::<u32>()));
}

#[test]
#[should_panic(expected = "untimed receive (inbox / inbox_with_*) in a timed configuration")]
fn untimed_inbox_is_rejected_under_a_timed_configuration() {
    verify(timed(), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || send_msg(me, 7u32));
        let _ = traceforge::inbox();
    });
}

#[test]
fn timed_receives_are_fine_under_a_timed_configuration() {
    let s = verify(timed(), one_sender_program(|| Some(recv_msg_block_timed::<u32>())));
    assert_eq!(s.execs, 1);
    let s = verify(timed(), || {
        let me = thread::current().id();
        let _s = thread::spawn(move || send_msg(me, 7u32));
        let _ = inbox_timed(1, WaitTime::Infinite);
    });
    assert_eq!(s.execs, 1);
}

/// The same-program idiom: the timed API runs unchanged under an
/// untimed configuration, where its timing information is ignored.
#[test]
fn timed_receives_are_fine_under_an_untimed_configuration() {
    let s = verify(untimed(), one_sender_program(|| Some(recv_msg_block_timed::<u32>())));
    assert_eq!(s.execs, 1);
    let s = verify(untimed(), one_sender_program(|| recv_msg_timed::<u32>(WaitTime::Finite(3))));
    assert_eq!(s.execs, 2, "read, and the timeout with the message unread");
}

#[test]
fn untimed_receives_are_fine_under_an_untimed_configuration() {
    let s = verify(untimed(), one_sender_program(|| Some(recv_msg_block::<u32>())));
    assert_eq!(s.execs, 1);
    let s = verify(untimed(), one_sender_program(|| recv_msg::<u32>()));
    assert_eq!(s.execs, 2, "read, and the empty non-blocking read");
}

/// The non-blocking form of a timed program is `recv_msg_timed` with a
/// zero wait. Under the untimed configuration it counts exactly like the
/// untimed non-blocking receive (one read, one empty).
#[test]
fn zero_wait_timed_receive_matches_the_untimed_non_blocking_receive() {
    let a = verify(untimed(), one_sender_program(|| recv_msg::<u32>()));
    let b = verify(untimed(), one_sender_program(|| recv_msg_timed::<u32>(WaitTime::Finite(0))));
    assert_eq!((a.execs, a.block), (b.execs, b.block));
    let c = verify(timed(), one_sender_program(|| recv_msg_timed::<u32>(WaitTime::Finite(0))));
    assert_eq!(c.execs + c.timeline_impossible, 2, "the timeout world is explored; with U = 1 it may or may not have a timeline");
}
