//! The three classes of a keep-going exploration are disjoint:
//! complete executions without a violation (`execs`), executions that
//! ended at a failed `assert` (`violating`, counted inside `block`),
//! and every other blocked execution (`block - violating`).
//!
//! Before this file `assert` did not switch after recording its block,
//! so a thread that returned right after a failed assert got an END
//! label behind the block and its execution was filed under `execs`.

use traceforge::thread;
use traceforge::*;

/// t1 asserts a choice, t2 assumes one: four executions.
///   (true , true ): valid
///   (false, true ): violating (t1 asserts and returns at once)
///   (true , false): blocked by the assume
///   (false, false): violating AND assume-blocked; filed as violating
fn program() {
    let t1 = thread::spawn(|| {
        let b: bool = traceforge::nondet();
        traceforge::assert(b);
    });
    let t2 = thread::spawn(|| {
        let c: bool = traceforge::nondet();
        traceforge::assume!(c);
    });
    let _ = t1.join();
    let _ = t2.join();
}

fn classes(s: &Stats) -> (usize, usize, usize) {
    (s.execs, s.block - s.violating, s.violating)
}

#[test]
fn untimed_keep_going_separates_the_three_classes() {
    let stats = traceforge::verify(
        Config::builder().with_keep_going_after_error(true).build(),
        program,
    );
    assert_eq!(classes(&stats), (1, 1, 2));
}

#[test]
fn timed_keep_going_separates_the_three_classes() {
    let stats = traceforge::verify(
        Config::builder().with_keep_going_after_error(true).with_timed(0, 1, 0).build(),
        program,
    );
    assert_eq!(classes(&stats), (1, 1, 2));
}

/// The run without a violation has none counted.
#[test]
fn no_violation_counts_none() {
    let stats = traceforge::verify(
        Config::builder().with_keep_going_after_error(true).build(),
        || {
            let t = thread::spawn(|| {
                let b: bool = traceforge::nondet();
                traceforge::assert(b || !b);
            });
            let _ = t.join();
        },
    );
    assert_eq!(classes(&stats), (2, 0, 0));
}
