//! YARN job scheduler with deadlines: how long must a deadline be?
//!
//! ## The idea in plain words
//!
//! A big computing cluster is shared by many users. Apache Hadoop YARN
//! (Vinod Kumar Vavilapalli et al., "Apache Hadoop YARN: Yet Another
//! Resource Negotiator", ACM Symposium on Cloud Computing, SoCC 2013)
//! splits the work between one central boss, the resource manager, and
//! helpers called application masters. In real YARN every job gets its
//! own application master, which asks the manager for room on the
//! cluster's machines. This file follows a small model of that setup built by Ehsan
//! Khamespanah, Ramtin Khosravi and Marjan Sirjani ("An efficient TCTL
//! model checking algorithm and a reduction technique for verification
//! of timed actor models", Science of Computer Programming 153, 2018,
//! Section 6.2), written in the Timed Rebeca modelling language. Their
//! model simplifies things: a fixed set of helpers stays around, each
//! runs one job at a time, and each receives its next job from the
//! manager's waiting line.
//!
//! The manager keeps a short waiting line of jobs, served first come,
//! first served. Every job has a deadline: how many time units it may
//! still spend waiting and running before it is too late.
//!
//! Who takes part:
//!   - a clock that ticks once every time unit;
//!   - the manager, who owns the waiting line and knows which helpers
//!     are free;
//!   - one or more helpers, each able to run one job at a time.
//!
//! What happens on every tick: the manager gives the job at the front of
//! the line to each helper that is free, and tells that helper how much
//! time the job has left. For every job handed out, a new job joins the
//! back of the line, so the line is always full: the cluster never gets
//! a break. Then every job still waiting loses one unit of its remaining
//! time. A job whose time runs out while it is still waiting has
//! expired: it is thrown away and a fresh job takes its place.
//!
//! What the helpers say back: a helper that receives a job compares the
//! work the job needs (2 time units in the original model; the authors
//! left a switched-off alternative in which the work is either 2 or 5)
//! with the time the job has left. If the work fits, the helper does it
//! and then tells the manager "done". If it does not fit, the helper
//! gives up exactly when the time runs out and tells the manager
//! "missed". Either message makes the helper free again.
//!
//! The timers: the clock tick (1 unit) drives the manager; every new job
//! starts with the same deadline (3 units in the original, 5 with four
//! helpers); a helper waits for the work time before saying "done", or
//! for the job's remaining time before saying "missed". In the original
//! model every message arrives instantly. Here the job hand-off and the
//! helper's answer may take a little while to travel, anywhere between a
//! smallest and a largest travel time, and the checker tries every
//! possibility, including every order of things that happen at the same
//! moment.
//!
//! What can go wrong:
//!   - a job waits in line so long that its deadline expires;
//!   - a job reaches a helper with so little time left that the helper
//!     has to give up on it;
//!   - a helper decides a job fits, because it only counts its own work
//!     time, but the manager reads its "done" message after the deadline
//!     has already passed, because the job and the answer spent time
//!     travelling, or because a message sat for a moment before anyone
//!     read it.
//!
//! Because the line is always full, the question this file asks is: for
//! a given number of helpers, line length, work time and travel time, how
//! long does a deadline have to be before none of this can ever happen?
//! That question is this file's own; the original authors did not ask
//! it. They asked something else: can a helper start five jobs that fit
//! their deadline within ten time units? (Their paper asks it for the
//! second helper, the property file shipped with their model for the
//! first.) Their paper reports only how much work the check took, not
//! the answer. That question is available here too.
//!
//! ## How the model maps onto TraceForge
//!
//! Source of truth: the four Timed Rebeca files of the case study
//! (yarn-deadline-fifo-{1,2,3,4}AMs.rebeca, identical except for the
//! number of AMs, QUEUE_SIZE = k+1 and DEFAULT_DEADLINE = 3, 3, 3, 5) and
//! yarn.property. The SCP 2018 paper prints the 3-AM file as Listing 4.
//! The SoCC 2013 YARN paper has no deadline scheduler: the deadline
//! queue is the Rebeca authors' abstraction of the RM's scheduling loop.
//!
//! Threads (spawned once, long-lived):
//!
//!   Clock      for i in 0..=H: send Tick{i} to RM with transit (0,0),
//!              sleep(1). Replaces the RM self-message
//!              `self.checkQueue() after(1)`.
//!   RM         one blocking receive loop, the Rebeca actor shape:
//!              Tick{i}   => checkQueue() verbatim (see below);
//!              Update{am, job, miss} => mark that AM free, record the
//!                           outcome. After Tick{H} no more dispatch; the
//!                           RM keeps reading until every busy AM has
//!                           reported, then judges the property and sends
//!                           Done to every AM.
//!   AM i       (i = 0..k-1; am1 of the source is AM 0) blocking receive
//!              of RunJob{job, dline, tick, rm}; completion c is chosen
//!              from the work-time set (default {2}, see CLI); if
//!              c > dline: sleep(dline), send
//!              Update{miss: true} (the source's self-preemption); else
//!              doneJobs++, sleep(c), send Update{miss: false}.
//!   Monitor    only with --property throughput (see below).
//!
//! checkQueue() is ported line by line: for each AM in index order, if
//! it is free, mark it busy, send it RunJob(queue[0]), shift the queue
//! left and append a fresh DEFAULT_DEADLINE job; then for each slot I:
//! queue[I]--, and if it hits 0 count a queue miss, shift the tail left
//! from I and append a fresh job, then I++. The last step preserves a
//! quirk of the source: after a removal, the job shifted into slot I
//! skips this tick's decrement, and the fresh back job is decremented in
//! the same tick. The doneJobs wrap (> 5 becomes 1, as in the code; the
//! paper text says "set to 0") is preserved too.
//!
//! Messages: Tick (Clock -> RM, transit (0,0) so ticks stay exact),
//! RunJob (RM -> AM) and Update (AM -> RM) with the global transit
//! window [L, U], Done (RM -> AM, harness). Deadlines are data: the RM
//! keeps, for each busy AM, the tick the job was handed out at and its
//! dline, so its absolute deadline is D = tick + dline in tick units.
//! Nothing in the model reads the clock.
//!
//! ## Deviations from the paper (deliberate)
//!
//!   - RunJob and Update take between L and U time units to travel. The
//!     source has zero network delay; L = U = 0 (the default) is the
//!     Rebeca model.
//!   - The periodic self-message becomes a separate Clock thread that
//!     sleeps 1 and sends exact Ticks up to a horizon H (default 10, the
//!     TCTL time bound). A sleep inside the RM would stop it from reading
//!     reports between ticks, and a finite-wait poll would put
//!     always-explorable timeout branches in front of every report.
//!   - Bounded horizon instead of unbounded time: after Tick{H} the RM
//!     hands out no more jobs and only collects outstanding reports.
//!   - `send ... after(d)` at the AM becomes sleep(d) followed by an
//!     ordinary send. Equivalent here: a busy AM gets no other work.
//!   - The source line is `int completion = 2;//?(2, 5);`. In Timed
//!     Rebeca `?(e1, ..., en)` picks ONE of the listed values (SCP 2018,
//!     Sect. 5.3.1, nondeterministic assignment), so the commented-out
//!     expression means "either 2 or 5", not a range. The default stays
//!     2, the active code; --completion-choice 2,5 restores the
//!     commented-out choice exactly. --completion-lo/--completion-hi give
//!     an interval instead, which for 2..5 WIDENS the source (it also
//!     allows 3 and 4). Nothing in the paper says which the modellers
//!     meant to use: its text only says the AMs were simplified "by
//!     setting 2 as the completion time of all jobs".
//!   - The per-message instrumentation flags (m_queue_misses,
//!     m_update_miss, m_job_complete, reset on every message) become
//!     cumulative first-violation records, asserted once when the RM has
//!     collected every report (see the next section).
//!   - The properties expiry, preempt and miss (the default) are this
//!     example's own questions. They are built on the source's
//!     instrumentation flags, but no published property reads those
//!     flags: yarn.property and the paper check only the reachability
//!     formula prop1 (see throughput), and the paper reports state counts
//!     for it (Table 2), not a verdict. So none of the verdicts below
//!     reproduces a published result. Note also that the paper describes
//!     the AMs as simplified "to perform their assigned jobs
//!     successfully", yet the model's own constants already lose jobs at
//!     zero delay (K=1: a queue expiry at tick 2 and a give-up at time 3,
//!     see Expected verdicts).
//!   - New property late-report, which cannot happen at zero delay.
//!   - Rebeca mailbox capacities (5)/(6) dropped: they bound the Rebeca
//!     tool's state space, not the protocol.
//!   - Absolute integer CLI (L, U, sd, deadline, completion, horizon)
//!     instead of the house ratio-over-U style: the source's tick of 1
//!     fixes the time unit, and ratios would rescale its constants.
//!
//! ## Timeout-validation assumptions (verification harness, NOT the protocol)
//!
//! The protocol has no finite-wait receive at all (every Rebeca message
//! server simply reacts), so the swim_timed assume_* idiom has nothing
//! to attach to. (The one finite-wait receive in this file is the
//! throughput monitor's, and it asserts only on the Some branch, never
//! on the timeout, so the trivially-explorable timeout cannot fire it.)
//! The analogous hazard in this model is a message that
//! is left unread: TraceForge lets a blocking receive read a later
//! message on another channel while an earlier one expires unread (with
//! sd = 0 a message is readable only at the instant it arrives, and a
//! skipped one is evicted). A skipped Update would leave its AM busy
//! forever in the RM's table and fake queue expiries; a skipped Tick
//! would desynchronise the RM's time base. Countermeasures, all
//! verification scaffolding:
//!
//!   1. assume!(index == expected) on every Tick: an execution that
//!      skipped a tick is discarded (counted blocked) at the next tick.
//!   2. The RM only finishes once every AM it believes busy has
//!      reported. A skipped Update can never be read later, so that
//!      execution blocks instead of finishing.
//!   3. Every property assertion is deferred until that point. The RM
//!      keeps first-violation records in locals and publishes counters
//!      only on finishing, so counters describe finished executions.
//!   4. Done (RM -> AM) lets the AMs terminate. With --property
//!      throughput the RM also sends Finished to the monitor, which
//!      asserts only after reading it (same deferral).
//!
//! Costs: holds report blocked > 0 by design (the discarded skip
//! branches plus the checker's own blocked classes); a violation that
//! precedes a skip in the same execution is discarded with it, which is
//! sound because the same violation survives in the sibling execution
//! without the skip.
//!
//! Both pieces were checked by temporarily removing them (runs
//! performed, then the file restored):
//!
//!   - without item 2 (RM finishes right after Tick{H}), K=1 QS=2 DD=8
//!     U=0 H=10 FIREs FALSELY: the certified witness never reads job 0's
//!     report, the AM stays busy in the RM's table and the queue
//!     expires. Item 2 is load-bearing for verdicts.
//!   - without item 1, the same verdicts at DD=8 U=0 H=10 and at DD=12
//!     U=1 H=14, but 16 -> 111 and 339 -> 22,366 executions (4.6 s ->
//!     100 s). Item 1 is load-bearing for cost. (A skipped tick also
//!     skips one aging step, which delays misses rather than creating
//!     them.)
//!
//! ## Properties checked (--property)
//!
//!   The first three are this example's own questions (see Deviations):
//!   they observe the events the source's instrumentation flags mark,
//!   but no published property checks them.
//!
//!   expiry       no waiting job's deadline reaches zero in the queue at
//!                a tick <= H (the event the source flags in
//!                m_queue_misses).
//!   preempt      no job handed out at a tick <= H is given up by its AM
//!                (the event the source flags in m_update_miss). A give-up
//!                after H is still counted: the RM collects every
//!                outstanding report.
//!   miss         expiry OR preempt, i.e. every deadline miss the source
//!                instruments (default).
//!   late-report  no "done" report is read by the RM after it has already
//!                read Tick{D+1}, D = dispatch tick + dline. Sound without
//!                a clock read (Tick{D+1} is read at time >= D+1, and reads
//!                on one thread are time-ordered, so the report is read
//!                after its deadline). At sd = 0 a message is read at its
//!                arrival instant, so the check is also exact in
//!                TraceForge's integer-valued timelines: a report arriving
//!                later than D arrives at >= D+1, together with or after
//!                Tick{D+1}, and both read orders of a tie are explored.
//!                With sd > 0 it ALSO flags reports that arrived by D but
//!                were read after Tick{D+1} (read lag, not travel): e.g.
//!                U=0 sd=1 DD=8 H=10 FIREs with zero travel time (see
//!                Expected verdicts). Judged only for jobs with D + 1 <= H.
//!   throughput   the source's TCTL prop1, EU(time <= 10, !doneJobs1,
//!                doneJobs1) with doneJobs1 = am1.doneJobs > 4: CAN am1
//!                start its fifth fitting job by time H? INVERTED exit
//!                code: am1 sends Fifth (transit (0,0)) to a monitor that
//!                waits Finite(H) from time 0 and asserts false when it
//!                reads it, so exit 101 = REACHABLE (the TCTL formula is
//!                true) and exit 0 = unreachable. The assertion is on the
//!                Some branch, never on the timeout, so it is exact: Some
//!                is feasible iff a timeline has the fifth start at a
//!                time <= H. (The paper checks am2; the property file,
//!                used here, checks am1.) The paper reports no verdict for
//!                this formula, only state counts, so the throughput
//!                verdicts below are not a reproduction either.
//!
//! ## CLI parameters
//!
//!   --mode baseline|timed|compare   default timed (compare aborts in its
//!                                   baseline leg whenever baseline FIREs)
//!   --ams K                         application masters, 1..4 (default 1)
//!   --queue-size QS                 queue slots (default K+1, as in all
//!                                   four source files)
//!   --deadline DD                   DEFAULT_DEADLINE (default 3; 5 when
//!                                   K = 4, as in the source)
//!   --completion-lo C, --completion-hi C
//!                                   work time interval (default 2, 2)
//!   --completion-choice C1,C2,...   work time is ONE of the listed values,
//!                                   like Rebeca's ?(C1, C2, ...); 2,5 is
//!                                   the source's commented-out choice.
//!                                   Overrides the interval flags; at most
//!                                   8 values, each >= 1
//!   --horizon H                     last tick (default 10). Capped so that
//!                                   no thread can exceed TraceForge's
//!                                   1,000-event per-thread limit (past it
//!                                   the checker stops the whole search
//!                                   early and would exit 0 having
//!                                   verified little): H <= 248, 198, 141,
//!                                   109 for K = 1, 2, 3, 4; larger values
//!                                   exit 2. Every such H is far beyond
//!                                   what finishes anyway.
//!   --l L, --u U, --sd SD           transit window and storage lifetime,
//!                                   absolute (default 0, 0, 0)
//!   --property P                    miss|expiry|preempt|late-report|
//!                                   throughput (default miss)
//!   --keep-going                    explore past the first violation
//!
//! Exit 0 = hold (for throughput: unreachable), 101 = violation (for
//! throughput: reachable), 2 = CLI misuse, so exit-code-based sweep
//! harnesses cannot mistake a typo for a FIRE. With --keep-going the
//! run exits 0 and prints a VIOLATIONS line instead.
//!
//! ## Printed counters (finished executions only, except where noted)
//!
//!   finished            executions in which the RM collected every report
//!   dispatched, done, preempted, expired, late
//!                       jobs handed out, "done" reports, give-ups, queue
//!                       expiries, late "done" reports (late-report rule)
//!   judged              "done" reports whose deadline lies inside H
//!   min_queue_left      smallest remaining deadline of a waiting job after
//!                       aging; 1 on an expiry hold = tightest case explored
//!   min_dispatch_dline  smallest dline handed out; == c_hi on a miss or
//!                       preempt hold = tightest case explored
//!   min_report_slack    smallest D - (last tick read) over judged on-time
//!                       reports; 0 on a late-report hold = tightest case
//!   fifth=a/b           a: monitor read am1's fifth start by time H;
//!                       b: am1 started a fifth fitting job at any time
//!                       (counted at am1, may include discarded executions)
//!   tick_skips          executions discarded by harness item 1
//!
//! The binary WARNs when a run verified nothing (0 executions, nothing
//! dispatched), when no miss can fit inside the horizon whatever the
//! timing (DD > H + 1 for expiry, DD >= H + c_hi for preempt: a
//! horizon-trivial hold), and when late-report judged no report.
//!
//! ## Expected verdicts (every cell below was run; nothing is predicted)
//!
//! Defaults unless stated: timed, K=1, QS=2, C=2..2, L=0, sd=0. "FIRE a /
//! hold b" means exit 101 at DD=a and exit 0 at DD=b. Every hold listed
//! is non-trivial for its horizon and shows the tightest-case counter
//! (min_dispatch_dline == c_hi, the largest work time, for miss/preempt,
//! min_queue_left == 1 for
//! expiry, min_report_slack == 0 for late-report). Timeouts (120 s) are
//! listed as no data.
//!
//! HEADLINE (sd = 0). Every measured boundary fits, with
//!
//!     P = ceil(QS / K) * (c_hi + 2U + 1)   (longest wait in the queue)
//!
//!   expiry holds iff DD >= P + 1
//!   miss and preempt hold iff DD >= P + c_hi
//!   late-report holds at U = 0; for U >= 1 iff DD >= P + c_hi + 2U
//!
//! and no measured boundary depended on L or on c_lo. With QS = K + 1,
//! ceil(QS/K) = 2, so the source's own constants (DD = 3; DD = 5 at
//! K = 4) are far below the miss boundary 3*c_hi + 4U + 2 = 8 at U = 0.
//! The formula was measured, not proved, on these cells:
//!
//!   miss      U=0:                 FIRE 7 / hold 8    (H=10 and H=14)
//!   miss      U=1:                 FIRE 11 (H=10, 14) / hold 12 (H=14, 16)
//!   miss      L=1 U=1:             FIRE 11 (H=10) / hold 12 (H=14)
//!   miss      U=2, L in {0,1,2}:   FIRE 15 (H=14) / hold 16 (H=16)
//!   miss      QS=1 U=0:            FIRE 4 / hold 5    (H=10)
//!   miss      QS=3 U=0:            FIRE 10 / hold 11  (H=12)
//!   miss      K=2 QS=3 U=0:        FIRE 7 / hold 8    (H=10, hold 9 s)
//!   miss      K=2 QS=3 U=1:        FIRE 11 (H=10, 39 s); hold side NO
//!                                  DATA (H=10 is horizon-trivial at
//!                                  DD=12; H=11 timed out)
//!   miss      K=2 QS=3 L=1 U=1:    FIRE 11 (H=10) / hold 12 (H=12)
//!   miss      K=2 QS=5 U=0:        FIRE 10 / hold 11  (H=11, 21 s)
//!   miss      K=3 QS=4 U=0:        FIRE 7 (H=7, H=9) / hold 8 (H=7, 72 s;
//!                                  H=9 timed out)
//!   miss      C=2..3 U=0:          FIRE 10 / hold 11  (H=10)
//!   miss      C={2,5} U=0:         FIRE 16 / hold 17  (H=13, 3 s; the
//!                                  source's commented-out choice, via
//!                                  --completion-choice 2,5)
//!   miss      C=2..5 U=0:          FIRE 16 / hold 17  (H=13, 16 s; the
//!                                  WIDENED interval, also allows 3 and 4)
//!   miss      C=5..5 U=0:          FIRE 16 / hold 17  (H=13)
//!   expiry    U=0:                 FIRE 6 (H=10) / hold 7 (H=10, 14)
//!   expiry    U=1:                 FIRE 10 / hold 11  (H=12)
//!   preempt   U=0:                 FIRE 7 / hold 8    (H=10)
//!   late-rep. U=0:                 hold at DD=3 and DD=8 (H=10, judged > 0)
//!   late-rep. U=1, L in {0,1}:     FIRE 13 / hold 14  (H=16)
//!   late-rep. L=2 U=2:             FIRE 19 / hold 20  (H=21)
//!   late-rep. K=2 QS=3 L=1 U=1:    FIRE 13 / hold 14  (H=16)
//!
//! sd > 0 (no formula claimed): miss at sd=1 U=0: FIRE 12 (H=12) and
//! 13 (H=14) / hold 14 (H=14 and H=15). The witness at DD=12 is genuine
//! read lag, every read inside its [arrival, arrival + 1] window: the RM
//! reads ticks and reports late and the AM reads its job late, so a job
//! cycle stretches from 3 to up to 6 units. late-report at sd=1 U=0 DD=8
//! H=10 FIREs (QS=2 and QS=1; hold side not measured) although nothing
//! travels: in the QS=2 witness job 3 is handed out for Tick 5 (read at
//! 6) with dline 4, so D = 9; the AM reads it at 7 and its report arrives
//! at 9, on time; the RM reads Tick 9 and Tick 10 at 10 and only then the
//! report. That is the read-lag case described under late-report.
//!
//! The source's constants, timed, U=0, H=10: FIRE for K = 1, 2, 3, 4
//! (K=4 takes about 60 s), and at K=1 also with C={2,5}. The K=1 witness is the modelled overload:
//! queue expiry at tick 2 and job 1 handed out at tick 2 with dline 1,
//! given up at time 3. The DD=7 witness at U=0 shows the tie-order
//! starvation behind the boundary: report and tick arrive together, the
//! tick is read first, so the next job waits an extra tick each cycle
//! (dispatches at ticks 0, 2, 4, 7, 10 with dline 7, 5, 3, 2, 1). The
//! late-report witness at U=1, DD=13 is the transit hazard: job 3 handed
//! out at tick 12 with dline 3 (deadline 15), received at 13, done at
//! 15, report read at 16 right after Tick 16.
//!
//! baseline: FIRE (exit 101) in every cell run where a miss fits inside
//! the horizon (miss DD=3, 8, 11 at H=10 and 16 at H=16; expiry and
//! preempt DD=11 H=10; K=2 DD=8 H=10 and DD=12 H=12; late-report DD=3
//! H=10, DD=14 and 15 at H=16): ticks race ahead of reports, so an
//! untimed checker can never prove a deadline safe. It holds only when
//! horizon-trivial (DD=12 H=10, with the WARNING; 1,024 executions
//! against 16 timed).
//!
//! throughput (INVERTED: FIRE = reachable), DD=3:
//!   timed U=0:        unreachable at H=7, 8; reachable at H=9, 10 (witness:
//!                     fitting starts at 0, 3, 5, 7, 9, job 1 at tick 2
//!                     was given up)
//!   timed L=0 U=1:    reachable at H=9, 10
//!   timed L=1 U=1:    unreachable at H=10
//!   baseline:         unreachable at H=5, 6; reachable at H=7, 8, 10, and
//!                     at L=1 U=1 H=10 (untimed, so it over-claims)
//!   timed L=1 U=1 DD=12: unreachable at H=16 with fifth=0/1 (am1 does
//!                     start a fifth job, at time 17: the time bound does
//!                     the work); reachable at H=17.
//!
//! CLI: --horizon 249 (K=1), 199 (K=2), 142 (K=3), 110 (K=4) and
//! 4294967295 exit 2. The caps themselves are accepted: baseline DD=1
//! FIREs at K=3 H=141 and K=4 H=109, and 20 s timed runs with C=1..1
//! sd=1 DD=1000 at K=1 H=248, K=2 H=198, K=4 H=109 printed no "Large
//! thread size" warning before the 20 s cut (no verdict, as expected).
//! (Before the cap, --horizon 500 ran 0 executions and exited 0.)

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Nondet, Stats, WaitTime};

// Outcome counters: accumulated across all explored executions of one
// verify run and reset before each run. The RM publishes its counters
// only when it finishes (every report collected), so executions that
// are later discarded by the harness never pollute them. Revisited
// prefixes may re-count: treat them as zero/nonzero evidence.
static RUNS_FINISHED: AtomicUsize = AtomicUsize::new(0);
static DISPATCHED: AtomicUsize = AtomicUsize::new(0);
static DONE_REPORTS: AtomicUsize = AtomicUsize::new(0);
static JUDGED_REPORTS: AtomicUsize = AtomicUsize::new(0);
static PREEMPTED: AtomicUsize = AtomicUsize::new(0);
static QUEUE_EXPIRED: AtomicUsize = AtomicUsize::new(0);
static LATE_REPORTS: AtomicUsize = AtomicUsize::new(0);
static FIFTH_IN_TIME: AtomicUsize = AtomicUsize::new(0);
// am1 started its fifth fitting job at SOME time (throughput only;
// counted at am1, so it may include later-discarded executions).
static FIFTH_STARTS: AtomicUsize = AtomicUsize::new(0);
static VIOLATIONS: AtomicUsize = AtomicUsize::new(0);
// Harness discards: executions in which the RM let a tick expire unread
// (counted just before the assume! that discards them).
static TICK_SKIPS: AtomicUsize = AtomicUsize::new(0);
// Tightness evidence (minimum over finished executions): how close the
// explored executions came to each kind of miss. A hold whose minimum
// sits exactly at the edge (queue left 1, dispatch dline == c_hi,
// report slack 0) shows the tightest case was actually explored.
static MIN_QUEUE_LEFT: AtomicU64 = AtomicU64::new(u64::MAX);
static MIN_DISPATCH_DLINE: AtomicU64 = AtomicU64::new(u64::MAX);
static MIN_REPORT_SLACK: AtomicU64 = AtomicU64::new(u64::MAX);

const TAG_FIFTH: u32 = 1;
const TAG_FINISHED: u32 = 2;

/// Messages received by the ResourceManager.
#[derive(Clone, Debug, PartialEq)]
enum RmMsg {
    /// Clock tick number `index`, sent at time `index` (source:
    /// `self.checkQueue() after(1)`).
    Tick { index: u32 },
    /// Source: `rm.update(deadline_miss)`. `am` stands in for Rebeca's
    /// `sender`; `job` is bookkeeping.
    Update { am: usize, job: u32, miss: bool },
}

/// Messages received by an ApplicationMaster.
#[derive(Clone, Debug, PartialEq)]
enum AmMsg {
    /// Source: `amI.runJob(fifo_queue[0])`. `job` and `tick` are
    /// bookkeeping; `rm` lets the AM answer without an Init message.
    RunJob { job: u32, dline: u64, tick: u32, rm: ThreadId },
    /// Verification harness, not the protocol: lets the AM terminate.
    Done,
}

/// Messages received by the throughput monitor (harness only).
#[derive(Clone, Debug, PartialEq)]
enum MonMsg {
    /// am1's doneJobs just became 5 (tag TAG_FIFTH).
    Fifth,
    /// The RM collected every report (tag TAG_FINISHED).
    Finished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Property {
    Miss,
    Expiry,
    Preempt,
    LateReport,
    Throughput,
}

impl Property {
    fn name(self) -> &'static str {
        match self {
            Property::Miss => "miss",
            Property::Expiry => "expiry",
            Property::Preempt => "preempt",
            Property::LateReport => "late-report",
            Property::Throughput => "throughput",
        }
    }
}

/// Most values --completion-choice accepts.
const MAX_CHOICES: usize = 8;

/// The work time (completion) of a job.
#[derive(Clone, Copy, Debug)]
enum Work {
    /// Any integer in lo..=hi (--completion-lo/--completion-hi).
    Interval { lo: u64, hi: u64 },
    /// One of vals[..len], sorted and distinct, like Rebeca's
    /// `?(e1, ..., en)` (--completion-choice).
    Choice { vals: [u64; MAX_CHOICES], len: usize },
}

impl Work {
    /// The largest possible work time (drives every boundary).
    fn hi(self) -> u64 {
        match self {
            Work::Interval { hi, .. } => hi,
            Work::Choice { vals, len } => vals[len - 1],
        }
    }

    /// Picks the work time of one job (a branching point when more than
    /// one value is possible).
    fn pick(self) -> u64 {
        match self {
            Work::Interval { lo, hi } if lo == hi => lo,
            Work::Interval { lo, hi } => (lo as usize..=hi as usize).nondet() as u64,
            Work::Choice { vals, len: 1 } => vals[0],
            Work::Choice { vals, len } => vals[(0..len).nondet()],
        }
    }

    fn label(self) -> String {
        match self {
            Work::Interval { lo, hi } => format!("{lo}..{hi}"),
            Work::Choice { vals, len } => {
                let list: Vec<String> = vals[..len].iter().map(u64::to_string).collect();
                format!("{{{}}}", list.join(","))
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Params {
    ams: usize,
    queue_size: usize,
    deadline: u64,
    work: Work,
    horizon: u32,
    l: u64,
    u: u64,
    sd: u64,
    property: Property,
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

// =====================================================================
// Clock: the periodic `self.checkQueue() after(1)` of the source
// =====================================================================

fn clock(rm: ThreadId, horizon: u32) {
    for index in 0..=horizon {
        // Transit (0,0): tick i is readable exactly at time i.
        traceforge::send_msg_timed(rm, RmMsg::Tick { index }, 0, 0);
        if index < horizon {
            traceforge::sleep(1);
        }
    }
}

// =====================================================================
// ResourceManager
// =====================================================================

/// The job an AM is running, as far as the RM knows.
#[derive(Clone, Copy, Debug)]
struct Running {
    job: u32,
    tick: u32,
    dline: u64,
}

/// First-violation records and tallies of one execution, published to
/// the global counters only when the RM finishes.
#[derive(Debug)]
struct Records {
    first_expiry_tick: Option<u32>,
    first_preempt_job: Option<u32>,
    first_late_job: Option<u32>,
    dispatched: usize,
    done: usize,
    judged: usize,
    preempted: usize,
    expired: usize,
    late: usize,
    min_queue_left: u64,
    min_dispatch_dline: u64,
    min_report_slack: u64,
}

impl Records {
    fn new() -> Self {
        Records {
            first_expiry_tick: None,
            first_preempt_job: None,
            first_late_job: None,
            dispatched: 0,
            done: 0,
            judged: 0,
            preempted: 0,
            expired: 0,
            late: 0,
            min_queue_left: u64::MAX,
            min_dispatch_dline: u64::MAX,
            min_report_slack: u64::MAX,
        }
    }

    fn publish(&self) {
        RUNS_FINISHED.fetch_add(1, Ordering::Relaxed);
        DISPATCHED.fetch_add(self.dispatched, Ordering::Relaxed);
        DONE_REPORTS.fetch_add(self.done, Ordering::Relaxed);
        JUDGED_REPORTS.fetch_add(self.judged, Ordering::Relaxed);
        PREEMPTED.fetch_add(self.preempted, Ordering::Relaxed);
        QUEUE_EXPIRED.fetch_add(self.expired, Ordering::Relaxed);
        LATE_REPORTS.fetch_add(self.late, Ordering::Relaxed);
        MIN_QUEUE_LEFT.fetch_min(self.min_queue_left, Ordering::Relaxed);
        MIN_DISPATCH_DLINE.fetch_min(self.min_dispatch_dline, Ordering::Relaxed);
        MIN_REPORT_SLACK.fetch_min(self.min_report_slack, Ordering::Relaxed);
    }
}

fn resource_manager(p: Params, ams: Vec<ThreadId>, monitor: Option<ThreadId>) {
    let me = thread::current().id();
    let k = ams.len();
    // fifo_queue: remaining deadline of each waiting job, front first.
    let mut queue: Vec<u64> = vec![p.deadline; p.queue_size];
    // appMasterI == BUSY  <=>  running[i].is_some()
    let mut running: Vec<Option<Running>> = vec![None; k];
    let mut expected_tick: u32 = 0;
    let mut ticks_done = false;
    let mut next_job: u32 = 0;
    let mut rec = Records::new();

    loop {
        // Harness (doc header, item 2): finish only once every busy AM
        // has reported.
        if ticks_done && running.iter().all(Option::is_none) {
            break;
        }
        match traceforge::recv_msg_block_timed::<RmMsg>() {
            RmMsg::Tick { index } => {
                // Harness (doc header, item 1): a skipped tick discards
                // the execution instead of desynchronising the RM.
                if index != expected_tick {
                    TICK_SKIPS.fetch_add(1, Ordering::Relaxed);
                }
                traceforge::assume!(index == expected_tick);
                expected_tick += 1;

                // checkQueue(), step 1: hand the front job to every free
                // AM, in AM order, refilling the back of the queue.
                for (i, slot) in running.iter_mut().enumerate() {
                    if slot.is_none() {
                        let dline = queue[0];
                        let job = next_job;
                        next_job += 1;
                        *slot = Some(Running { job, tick: index, dline });
                        traceforge::send_msg(ams[i], AmMsg::RunJob { job, dline, tick: index, rm: me });
                        rec.dispatched += 1;
                        rec.min_dispatch_dline = rec.min_dispatch_dline.min(dline);
                        queue.remove(0);
                        queue.push(p.deadline);
                    }
                }

                // checkQueue(), step 2: age every waiting job by one unit;
                // a job that hits zero expired. Verbatim loop, including
                // the source's skip of the slot shifted into I.
                let mut i = 0;
                while i < queue.len() {
                    queue[i] -= 1;
                    if queue[i] == 0 {
                        rec.expired += 1;
                        rec.first_expiry_tick.get_or_insert(index);
                        queue.remove(i);
                        queue.push(p.deadline);
                    }
                    i += 1;
                }
                if let Some(&m) = queue.iter().min() {
                    rec.min_queue_left = rec.min_queue_left.min(m);
                }

                if index == p.horizon {
                    ticks_done = true;
                }
            }
            RmMsg::Update { am, job, miss } => {
                // update(deadline_miss): appMasterI = FREE.
                let run = match running.get_mut(am).and_then(Option::take) {
                    Some(run) if run.job == job => run,
                    // Unreachable: an AM reports exactly once per job.
                    other => panic!("rm: unexpected Update am={am} job={job}, running {other:?}"),
                };
                if miss {
                    rec.preempted += 1;
                    rec.first_preempt_job.get_or_insert(job);
                } else {
                    rec.done += 1;
                    // Absolute deadline in tick units. Every Update is
                    // read after the Tick that dispatched its job, so
                    // expected_tick >= 1 here.
                    let deadline_tick = u64::from(run.tick) + run.dline;
                    let last_tick = u64::from(expected_tick - 1);
                    if deadline_tick < u64::from(p.horizon) {
                        rec.judged += 1;
                        if last_tick > deadline_tick {
                            rec.late += 1;
                            rec.first_late_job.get_or_insert(job);
                        } else {
                            rec.min_report_slack = rec.min_report_slack.min(deadline_tick - last_tick);
                        }
                    }
                }
            }
        }
    }

    // Harness (doc header, item 3): judge only now, with every report
    // collected.
    rec.publish();
    let ok = match p.property {
        Property::Miss => rec.first_expiry_tick.is_none() && rec.first_preempt_job.is_none(),
        Property::Expiry => rec.first_expiry_tick.is_none(),
        Property::Preempt => rec.first_preempt_job.is_none(),
        Property::LateReport => rec.first_late_job.is_none(),
        Property::Throughput => true,
    };
    if !ok {
        VIOLATIONS.fetch_add(1, Ordering::Relaxed);
    }
    traceforge::assert(ok);

    for am in &ams {
        traceforge::send_msg(*am, AmMsg::Done);
    }
    if let Some(m) = monitor {
        traceforge::send_tagged_msg(m, TAG_FINISHED, MonMsg::Finished);
    }
}

// =====================================================================
// ApplicationMaster
// =====================================================================

fn app_master(index: usize, p: Params, monitor: Option<ThreadId>) {
    let mut done_jobs: u32 = 0;
    let mut fifth_sent = false;
    loop {
        match traceforge::recv_msg_block_timed::<AmMsg>() {
            AmMsg::RunJob { job, dline, rm, .. } => {
                // Source: int completion = 2; //?(2, 5);
                let completion = p.work.pick();
                if completion > dline {
                    // Self-preemption: stop when the deadline passes.
                    traceforge::sleep(dline);
                    traceforge::send_msg(rm, RmMsg::Update { am: index, job, miss: true });
                } else {
                    done_jobs += 1;
                    if done_jobs == 5 && !fifth_sent {
                        if let Some(m) = monitor {
                            FIFTH_STARTS.fetch_add(1, Ordering::Relaxed);
                            // Harness: tell the throughput monitor, at
                            // the instant doneJobs1 becomes true.
                            traceforge::send_tagged_msg_timed(m, TAG_FIFTH, MonMsg::Fifth, 0, 0);
                            fifth_sent = true;
                        }
                    }
                    if done_jobs > 5 {
                        done_jobs = 1;
                    }
                    traceforge::sleep(completion);
                    traceforge::send_msg(rm, RmMsg::Update { am: index, job, miss: false });
                }
            }
            AmMsg::Done => return,
        }
    }
}

// =====================================================================
// Throughput monitor (harness; --property throughput only)
// =====================================================================

fn throughput_monitor(horizon: u32) {
    // Waits from time 0 for at most H: the Some branch is feasible iff
    // am1's fifth fitting job can start at a time <= H.
    let fifth = traceforge::recv_tagged_msg_timed::<_, MonMsg>(
        |_s, tag| tag == Some(TAG_FIFTH),
        WaitTime::Finite(u64::from(horizon)),
    );
    if fifth.is_some() {
        // Deferred like the RM's assertions: only once the RM finished.
        let _ = traceforge::recv_tagged_msg_block_timed::<_, MonMsg>(|_s, tag| {
            tag == Some(TAG_FINISHED)
        });
        FIFTH_IN_TIME.fetch_add(1, Ordering::Relaxed);
        VIOLATIONS.fetch_add(1, Ordering::Relaxed);
        // INVERTED: firing means the TCTL EU formula is TRUE (reachable).
        traceforge::assert(false);
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

/// TraceForge stops the WHOLE search (after a warning) once one thread
/// grows past this many events. This is its default; it is pinned here
/// because main() rejects horizons that could reach it.
const THREAD_EVENT_LIMIT: u32 = 1000;

/// Upper bound on the events of the busiest thread (TraceForge counts
/// begin, end, every send, receive, sleep and choice, and a final block).
/// Work times are >= 1, so each AM runs at most one job per tick.
///   Clock: begin + (H+1) sends + H sleeps + end.
///   RM:    begin + (H+1) tick reads + <= K(H+1) dispatches + <= K(H+1)
///          report reads + K Done + Finished + end.
///   AM:    begin + <= (H+1) jobs * (read, choice, sleep, send) + Fifth +
///          Done read + end.
fn max_thread_events(ams: usize, horizon: u32) -> u64 {
    let ticks = u64::from(horizon) + 1;
    let k = ams as u64;
    let clock = 2 * ticks + 1;
    let rm = ticks * (2 * k + 1) + k + 3;
    let am = 4 * ticks + 4;
    clock.max(rm).max(am)
}

fn build_config(mode: Mode, p: Params, keep_going: bool) -> Config {
    let mut builder = Config::builder()
        .with_progress_report(usize::MAX)
        .with_thread_threshold(THREAD_EVENT_LIMIT);
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(p.l, p.u, p.sd).build(),
    }
}

fn reset_counters() {
    for c in [
        &RUNS_FINISHED,
        &DISPATCHED,
        &DONE_REPORTS,
        &JUDGED_REPORTS,
        &PREEMPTED,
        &QUEUE_EXPIRED,
        &LATE_REPORTS,
        &FIFTH_IN_TIME,
        &FIFTH_STARTS,
        &VIOLATIONS,
        &TICK_SKIPS,
    ] {
        c.store(0, Ordering::Relaxed);
    }
    for m in [&MIN_QUEUE_LEFT, &MIN_DISPATCH_DLINE, &MIN_REPORT_SLACK] {
        m.store(u64::MAX, Ordering::Relaxed);
    }
}

fn run(mode: Mode, p: Params, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, p, keep_going);
    reset_counters();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let monitor = if p.property == Property::Throughput {
            Some(thread::spawn(move || throughput_monitor(p.horizon)))
        } else {
            None
        };
        let monitor_id = monitor.as_ref().map(|h| h.thread().id());
        let mut ams = Vec::new();
        for i in 0..p.ams {
            let m = if i == 0 { monitor_id } else { None };
            ams.push(thread::spawn(move || app_master(i, p, m)));
        }
        let am_ids: Vec<ThreadId> = ams.iter().map(|h| h.thread().id()).collect();
        let rm = thread::spawn(move || resource_manager(p, am_ids, monitor_id));
        let rm_id = rm.thread().id();
        let clk = thread::spawn(move || clock(rm_id, p.horizon));
        let _ = clk.join();
        let _ = rm.join();
        for h in ams {
            let _ = h.join();
        }
        if let Some(h) = monitor {
            let _ = h.join();
        }
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    finished: usize,
    dispatched: usize,
    done: usize,
    judged: usize,
    preempted: usize,
    expired: usize,
    late: usize,
    fifth: usize,
    fifth_starts: usize,
    violations: usize,
    tick_skips: usize,
    min_queue_left: u64,
    min_dispatch_dline: u64,
    min_report_slack: u64,
}

fn read_counts() -> Counts {
    Counts {
        finished: RUNS_FINISHED.load(Ordering::Relaxed),
        dispatched: DISPATCHED.load(Ordering::Relaxed),
        done: DONE_REPORTS.load(Ordering::Relaxed),
        judged: JUDGED_REPORTS.load(Ordering::Relaxed),
        preempted: PREEMPTED.load(Ordering::Relaxed),
        expired: QUEUE_EXPIRED.load(Ordering::Relaxed),
        late: LATE_REPORTS.load(Ordering::Relaxed),
        fifth: FIFTH_IN_TIME.load(Ordering::Relaxed),
        fifth_starts: FIFTH_STARTS.load(Ordering::Relaxed),
        tick_skips: TICK_SKIPS.load(Ordering::Relaxed),
        violations: VIOLATIONS.load(Ordering::Relaxed),
        min_queue_left: MIN_QUEUE_LEFT.load(Ordering::Relaxed),
        min_dispatch_dline: MIN_DISPATCH_DLINE.load(Ordering::Relaxed),
        min_report_slack: MIN_REPORT_SLACK.load(Ordering::Relaxed),
    }
}

fn show_min(v: u64) -> String {
    if v == u64::MAX {
        String::from("-")
    } else {
        v.to_string()
    }
}

// =====================================================================
// Reporting
// =====================================================================

fn print_one(label: &str, p: Params, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<9} prop={prop} K={k} QS={qs} DD={dd} C={c} H={h} L={l} U={u} sd={sd}  \
         execs={execs} blocked={block} finished={fin} dispatched={disp} done={ot} \
         judged={jd} preempted={pre} expired={exp} late={late} fifth={fifth}/{fs} \
         min_queue_left={mq} min_dispatch_dline={md} min_report_slack={ms} \
         tick_skips={ts} violations={viol} time={dur:?}",
        prop = p.property.name(), k = p.ams, qs = p.queue_size, dd = p.deadline,
        c = p.work.label(), h = p.horizon, l = p.l, u = p.u, sd = p.sd,
        execs = stats.execs, block = stats.block, fin = c.finished, disp = c.dispatched,
        ot = c.done, jd = c.judged, pre = c.preempted, exp = c.expired, late = c.late,
        fifth = c.fifth, fs = c.fifth_starts, mq = show_min(c.min_queue_left), md = show_min(c.min_dispatch_dline),
        ms = show_min(c.min_report_slack), ts = c.tick_skips, viol = c.violations, dur = dur,
    );
}

fn print_compare(p: Params, baseline: (Stats, Duration), timed: (Stats, Duration), bc: Counts, tc: Counts) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("YARN deadline scheduler: MUST vs MUST-timed");
    println!("======================================================");
    println!(
        "prop = {}   K = {}   QS = {}   DD = {}   C = {}   H = {}   L = {}   U = {}   sd = {}",
        p.property.name(), p.ams, p.queue_size, p.deadline, p.work.label(), p.horizon, p.l, p.u, p.sd
    );
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
    println!();
    // Executions EXPLORED is execs + block: a violating execution is aborted
    // by its assertion and an evicted one blocks, so both are filed under
    // `block`. Dividing complete executions alone overstates the reduction
    // (sensor network defaults: 7702x that way, 215x explored).
    let b_explored = b_stats.execs + b_stats.block;
    let t_explored = t_stats.execs + t_stats.block;
    let exec_ratio = b_explored as f64 / t_explored.max(1) as f64;
    let time_ratio = b_dur.as_secs_f64() / t_dur.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "explored reduction: {exec_ratio:.2}x  ({b_explored} vs {t_explored} executions explored, execs+blocked)"
    );
    println!("time  speedup  : {time_ratio:.2}x");
    println!(
        "dispatched/preempted/expired/late: baseline {}/{}/{}/{}   timed {}/{}/{}/{}",
        bc.dispatched, bc.preempted, bc.expired, bc.late, tc.dispatched, tc.preempted, tc.expired, tc.late
    );
}

/// Vacuity guards: an exit 0 that verified nothing, or a hold whose
/// interesting path cannot occur inside the horizon, is not a hold.
fn warn_if_vacuous(label: &str, p: Params, stats: &Stats, c: Counts) {
    if c.violations > 0 {
        // Only reachable with --keep-going (otherwise the first
        // violation aborts): violating executions end in an assertion
        // block, so they count as blocked, not as execs.
        println!(
            "VIOLATIONS ({label}): {} violating executions found (--keep-going continued past \
             them; exit 0 here does NOT mean the property holds).",
            c.violations
        );
    } else if stats.execs == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={}): every execution was \
             discarded, this run verified nothing; treat it as no data, not as a hold.",
            stats.block
        );
    }
    if stats.execs > 0 && (c.finished == 0 || c.dispatched == 0) {
        println!(
            "WARNING ({label}): no finished execution handed out a job (finished={}, \
             dispatched={}); treat it as no data.",
            c.finished, c.dispatched
        );
    }
    let h = u64::from(p.horizon);
    match p.property {
        Property::Miss | Property::Expiry | Property::Preempt => {
            // A queued job can reach zero at a tick <= H only if
            // DD <= H + 1; a job handed out at tick t has dline >= DD - t,
            // so a give-up inside the horizon needs DD < H + c_hi.
            let expiry_possible = p.deadline <= h + 1;
            let preempt_possible = p.deadline < h + p.work.hi();
            let possible = match p.property {
                Property::Expiry => expiry_possible,
                Property::Preempt => preempt_possible,
                _ => expiry_possible || preempt_possible,
            };
            if !possible {
                println!(
                    "WARNING ({label}): DD={} cannot miss inside horizon H={} whatever the \
                     timing; a hold here is horizon-trivial, treat it as no data (raise --horizon).",
                    p.deadline, p.horizon
                );
            }
        }
        Property::LateReport => {
            if c.judged == 0 && c.violations == 0 {
                println!(
                    "WARNING ({label}): no done report had its deadline inside the horizon \
                     (judged=0); this hold checked nothing, raise --horizon."
                );
            }
        }
        Property::Throughput => {
            if c.violations == 0 && c.fifth_starts == 0 {
                println!(
                    "NOTE ({label}): am1 never started a fifth fitting job at any time inside \
                     the horizon (fifth=0/0), so this unreachable verdict does not depend on \
                     the time bound."
                );
            }
        }
    }
}

fn print_verdict_hint(p: Params) {
    if p.property == Property::Throughput {
        println!(
            "note: --property throughput has an INVERTED exit code: exit 0 = am1 can NOT start \
             five fitting jobs by time H (TCTL prop1 false); exit 101 = reachable (prop1 true)."
        );
    }
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    ams: usize,
    queue_size: Option<usize>,
    deadline: Option<u64>,
    c_lo: u64,
    c_hi: u64,
    c_choice: Option<Vec<u64>>,
    horizon: u32,
    l: u64,
    u: u64,
    sd: u64,
    property: Property,
    keep_going: bool,
}

fn next_val(args: &mut std::env::Args, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
}

fn parse_num<T: std::str::FromStr>(v: String, flag: &str) -> T {
    v.parse()
        .unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::from("timed"),
        ams: 1,
        queue_size: None,
        deadline: None,
        c_lo: 2,
        c_hi: 2,
        c_choice: None,
        horizon: 10,
        l: 0,
        u: 0,
        sd: 0,
        property: Property::Miss,
        keep_going: false,
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--ams" => a.ams = parse_num(next_val(&mut args, "--ams"), "--ams"),
            "--queue-size" => {
                a.queue_size = Some(parse_num(next_val(&mut args, "--queue-size"), "--queue-size"))
            }
            "--deadline" => {
                a.deadline = Some(parse_num(next_val(&mut args, "--deadline"), "--deadline"))
            }
            "--completion-lo" => {
                a.c_lo = parse_num(next_val(&mut args, "--completion-lo"), "--completion-lo")
            }
            "--completion-hi" => {
                a.c_hi = parse_num(next_val(&mut args, "--completion-hi"), "--completion-hi")
            }
            "--completion-choice" => {
                let v = next_val(&mut args, "--completion-choice");
                a.c_choice = Some(
                    v.split(',')
                        .map(|s| parse_num(s.trim().to_string(), "--completion-choice"))
                        .collect(),
                );
            }
            "--horizon" => a.horizon = parse_num(next_val(&mut args, "--horizon"), "--horizon"),
            "--l" => a.l = parse_num(next_val(&mut args, "--l"), "--l"),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--sd" => a.sd = parse_num(next_val(&mut args, "--sd"), "--sd"),
            "--property" => {
                let v = next_val(&mut args, "--property");
                a.property = match v.as_str() {
                    "miss" => Property::Miss,
                    "expiry" => Property::Expiry,
                    "preempt" => Property::Preempt,
                    "late-report" => Property::LateReport,
                    "throughput" => Property::Throughput,
                    other => cli_bail(&format!(
                        "invalid --property: {other} (expected miss|expiry|preempt|late-report|throughput)"
                    )),
                };
            }
            "--keep-going" => a.keep_going = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: yarn_scheduler_timed [--mode baseline|timed|compare] [--ams K] \
                     [--queue-size QS] [--deadline DD] [--completion-lo C] [--completion-hi C] \
                     [--completion-choice C1,C2,...] [--horizon H] [--l L] [--u U] [--sd SD] \
                     [--property miss|expiry|preempt|late-report|throughput] [--keep-going]\n\
                     All times are absolute integers (the source's tick is 1 unit).\n\
                     --completion-choice picks ONE listed value per job, like Rebeca's ?(..); \
                     2,5 is the source's commented-out choice (an interval 2..5 also allows 3, 4).\n\
                     --horizon is capped at 248, 198, 141, 109 for K = 1..4 (TraceForge's \
                     per-thread event limit).\n\
                     Defaults: mode=timed, K=1, QS=K+1, DD=3 (5 when K=4), C=2..2, H=10, \
                     L=0, U=0, sd=0, property=miss.\n\
                     Exit 0 = hold, 101 = violation, 2 = CLI misuse. For --property throughput \
                     the meaning is INVERTED: 101 = am1 can start five fitting jobs by time H."
                );
                std::process::exit(0);
            }
            other => cli_bail(&format!("unknown argument: {other}")),
        }
    }
    a
}

fn main() {
    let a = parse_args();
    if a.ams < 1 || a.ams > 4 {
        cli_bail("--ams must be in 1..=4");
    }
    let queue_size = a.queue_size.unwrap_or(a.ams + 1);
    let deadline = a.deadline.unwrap_or(if a.ams == 4 { 5 } else { 3 });
    if queue_size < 1 {
        cli_bail("--queue-size must be >= 1");
    }
    if deadline < 1 {
        cli_bail("--deadline must be >= 1");
    }
    let work = match &a.c_choice {
        None => {
            if a.c_lo < 1 || a.c_lo > a.c_hi {
                cli_bail("need 1 <= --completion-lo <= --completion-hi");
            }
            Work::Interval { lo: a.c_lo, hi: a.c_hi }
        }
        Some(list) => {
            let mut sorted = list.clone();
            sorted.sort_unstable();
            sorted.dedup();
            if sorted.is_empty() || sorted.len() > MAX_CHOICES || sorted[0] < 1 {
                cli_bail(&format!(
                    "--completion-choice needs 1..={MAX_CHOICES} distinct values, each >= 1"
                ));
            }
            let mut vals = [0u64; MAX_CHOICES];
            vals[..sorted.len()].copy_from_slice(&sorted);
            Work::Choice { vals, len: sorted.len() }
        }
    };
    if a.horizon < 1 {
        cli_bail("--horizon must be >= 1");
    }
    if max_thread_events(a.ams, a.horizon) > u64::from(THREAD_EVENT_LIMIT) {
        let cap = (1..a.horizon)
            .take_while(|&h| max_thread_events(a.ams, h) <= u64::from(THREAD_EVENT_LIMIT))
            .last()
            .unwrap_or(0);
        cli_bail(&format!(
            "--horizon {} is too long for K={}: a thread could pass TraceForge's \
             {THREAD_EVENT_LIMIT}-event per-thread limit, which stops the search early; \
             use --horizon <= {cap}",
            a.horizon, a.ams
        ));
    }
    if a.l > a.u {
        cli_bail("transit lower bound --l must be <= --u");
    }
    let p = Params {
        ams: a.ams,
        queue_size,
        deadline,
        work,
        horizon: a.horizon,
        l: a.l,
        u: a.u,
        sd: a.sd,
        property: a.property,
    };
    print_verdict_hint(p);
    match a.mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, p, a.keep_going);
            let c = read_counts();
            print_one("baseline", p, &s, d, c);
            warn_if_vacuous("baseline", p, &s, c);
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, p, a.keep_going);
            let c = read_counts();
            print_one("timed", p, &s, d, c);
            warn_if_vacuous("timed", p, &s, c);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, p, a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, p, a.keep_going);
            let tc = read_counts();
            let (bs, ts) = (baseline.0.clone(), timed.0.clone());
            print_compare(p, baseline, timed, bc, tc);
            warn_if_vacuous("baseline", p, &bs, bc);
            warn_if_vacuous("timed", p, &ts, tc);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
}
