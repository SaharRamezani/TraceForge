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
//!   - the manager, who owns the waiting line, knows which helpers are
//!     free, and wakes itself up once every time unit;
//!   - one or more helpers, each able to run one job at a time.
//!
//! What happens every time the manager wakes up: it gives the job at the
//! front of the line to each helper that is free, and tells that helper
//! how much time the job has left. For every job handed out, a new job
//! joins the back of the line, so the line is always full: the cluster
//! never gets a break. Then every job still waiting loses one unit of its
//! remaining time. A job whose time runs out while it is still waiting
//! has expired: it is thrown away and a fresh job takes its place.
//!
//! What the helpers say back: a helper that receives a job compares the
//! work the job needs (2 time units in the original model; the authors
//! left a switched-off alternative in which the work is either 2 or 5)
//! with the time the job has left. If the work fits, the helper does it
//! and then tells the manager "done". If it does not fit, the helper
//! gives up exactly when the time runs out and tells the manager
//! "missed". Either message makes the helper free again.
//!
//! The timers: the manager's wake-up (1 unit); every new job starts with
//! the same deadline (3 units in the original, 5 with four helpers); a
//! helper waits for the work time before saying "done", or for the job's
//! remaining time before saying "missed". In the original model every
//! message arrives instantly and none is ever lost. Here the job hand-off
//! and the helper's answer may take a little while to travel, anywhere
//! between a smallest and a largest travel time, and any message may be
//! lost on the way: the checker tries every possibility, including every
//! order of things that happen at the same moment.
//!
//! What can go wrong:
//!   - a job waits in line so long that its deadline expires;
//!   - a job reaches a helper with so little time left that the helper
//!     has to give up on it;
//!   - a helper decides a job fits, because it only counts its own work
//!     time, but the manager reads its "done" message after the deadline
//!     has already passed, because the job and the answer spent time
//!     travelling (or, where a message may be kept for a while before it
//!     is read, because it sat for a moment before anyone read it);
//!   - a job hand-off or an answer never arrives. The manager then keeps
//!     that helper marked busy for ever (the model has no retransmission
//!     and no timeout), and the jobs waiting behind it run out of time.
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
//! What the checker finds, in short. A waiting job can expire at every
//! deadline for which an expiry fits into the run at all, because one
//! lost message is enough to leave a helper marked busy for good. With
//! a single helper the other three questions are not decided by loss
//! and keep an answer that depends on the timing: a late "done" report,
//! the five-jobs question, and a helper giving a job up. With two
//! helpers a lost report leaves the other helper to serve the queue
//! alone, and a give-up or a late report then fits at deadlines where
//! one helper is safe. See Results.
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
//! Threads. The RM and the AMs are daemons: Rebeca actors react to their
//! next message for ever, nobody waits for them, and an execution is
//! over when nothing can move any more.
//!
//!   RM         one blocking receive loop, the Rebeca actor shape:
//!              Tick      => checkQueue() verbatim (see below), then the
//!                           source's `self.checkQueue() after(1)`: the RM
//!                           sends itself the next Tick with exact transit
//!                           1, up to tick H;
//!              Update{am, job, miss} => mark that AM free; a "done"
//!                           report is checked against its deadline.
//!              Tick 0 is sent at time 0 with transit 0 (the
//!              constructor's call). After tick H the RM hands out
//!              nothing and keeps reading reports.
//!   AM i       (i = 0..k-1; am1 of the source is AM 0) blocking receive
//!              of RunJob{job, dline, tick, rm}; completion c is chosen
//!              from the work-time set (default {2}, see CLI); if
//!              c > dline: sleep(dline), send
//!              Update{miss: true} (the source's self-preemption); else
//!              doneJobs++, sleep(c), send Update{miss: false}.
//!   Monitor    only with --property throughput (see Properties); an
//!              ordinary thread with one finite wait.
//!
//! The RM's timer is a message like any other, as in Rebeca, so the
//! model's semantics may lose it: the RM's clock then stops for good, it
//! hands out no more jobs, and it only reads reports. That is one of
//! the explored executions.
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
//! Messages: Tick (RM to itself, exact transit), RunJob (RM -> AM) and
//! Update (AM -> RM) with the global transit window [L, U]. No thread
//! needs a bootstrap message: the RM gets the AMs' ids through its spawn
//! closure, and RunJob carries the address the AM answers to. Deadlines
//! are data: the RM keeps, for each busy AM, the tick the job was handed
//! out at and its dline, so its absolute deadline is D = tick + dline in
//! tick units. Nothing in the model reads the clock.
//!
//! ## Deviations from the paper (deliberate)
//!
//!   - RunJob and Update take between L and U time units to travel. The
//!     source has zero network delay; L = U = 0 (the default) is the
//!     Rebeca model's timing.
//!   - Every message may be lost, and a message that is not read within
//!     the storage lifetime sd (default 0) of its arrival expires unread.
//!     This is the model's semantics for every send; Rebeca's message
//!     bags lose nothing. A receive takes the message that arrived
//!     first, a tie leaving both orders, so a reactor cannot take a
//!     later message and let an earlier one expire. Here no receiver is
//!     ever busy when a message reaches it (the RM handles a message in
//!     zero time, an AM is idle again before its next job can arrive),
//!     so at sd = 0 every message is lost or read at its arrival (the
//!     throughput probe excepted: its monitor waits only until H).
//!   - Bounded horizon instead of unbounded time: the RM re-arms its
//!     timer up to tick H (default 10, the TCTL time bound) and hands out
//!     nothing afterwards. The bound is a count kept by the RM; no
//!     message announces the end of the run.
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
//!     assertions at the step where the flagged event happens (see
//!     Properties).
//!   - The properties expiry, preempt and miss (the default) are this
//!     example's own questions. They are built on the source's
//!     instrumentation flags, but no published property reads those
//!     flags: yarn.property and the paper check only the reachability
//!     formula prop1 (see throughput), and the paper reports state counts
//!     for it (Table 2), not a verdict. So none of the verdicts below
//!     reproduces a published result. Note also that the paper describes
//!     the AMs as simplified "to perform their assigned jobs
//!     successfully", yet the model's own constants already lose jobs at
//!     zero delay with nothing lost (K=1: a queue expiry at tick 2 and a
//!     give-up at time 4, the DD=3 cells in Results).
//!   - New property late-report, which cannot happen at zero delay
//!     (L = U = sd = 0).
//!   - Rebeca mailbox capacities (5)/(6) dropped: they bound the Rebeca
//!     tool's state space, not the protocol.
//!   - Absolute integer CLI (L, U, sd, deadline, completion, horizon)
//!     instead of the house ratio-over-U style: the source's tick of 1
//!     fixes the time unit, and ratios would rescale its constants.
//!
//! ## No harness (since 2026-10-13)
//!
//! Until 2026-10-13 this file carried a verification harness around the
//! model: a separate Clock thread that sent numbered ticks, an
//! `assume!(index == expected)` on every tick (an execution in which the
//! RM let a tick go unread was discarded), an exit condition that made
//! the RM keep reading until every AM it believed busy had reported (an
//! execution with a report that could never be read blocked there for
//! ever), every property judged only after that point, a Done message to
//! release each AM and a Finished message the throughput monitor waited
//! for. It was written against sd = 0 eviction (a receive that took a
//! later message and let an earlier one expire; since 2026-10-13 a
//! receive takes the message that arrived first), and once every send
//! became lossy it also discarded every execution with a lost RunJob,
//! Update or tick: in effect it re-created Rebeca's reliable message
//! bags. The old header recorded the telling experiment itself: with the
//! report-collecting exit removed, K=1 DD=8 fired, "falsely", on an
//! execution in which the RM never reads job 0's report.
//!
//! Under the model's semantics that execution is real. A lost report
//! leaves the AM busy in the RM's table, and the queue behind it
//! expires: it is a deadline miss like any other and is now reported.
//! There is no Clock thread (the RM's timer is its own self-sent message,
//! as in the source), no assume!, no drain, no release message and no
//! deferred judgement: each property is asserted at the step where it
//! fails, and the counters are raised there.
//!
//! ## Properties checked (--property)
//!
//!   The first three are this example's own questions (see Deviations):
//!   they observe the events the source's instrumentation flags mark,
//!   but no published property checks them. Each is asserted where the
//!   event happens, and counted whichever property is selected.
//!
//!   expiry       no waiting job's deadline reaches zero in the queue at
//!                a tick <= H (the event the source flags in
//!                m_queue_misses). Asserted by the RM in its aging loop.
//!   preempt      no job handed out at a tick <= H is given up by its AM
//!                (the event the source flags in m_update_miss). Asserted
//!                by the AM at the instant it gives up, before it sends
//!                its report, so the verdict does not depend on that
//!                report being read.
//!   miss         expiry OR preempt, i.e. every deadline miss the source
//!                instruments (default).
//!   late-report  no "done" report is read by the RM after it has already
//!                read tick number D+1, D = dispatch tick + dline. Asserted
//!                by the RM when it reads the report. Sound without a
//!                clock read (tick D+1 is read at time >= D+1, and reads
//!                on one thread are time-ordered, so the report is read
//!                after its deadline). The RM reads its messages in the
//!                order they arrive (a tie goes either way), so a report
//!                is late exactly when it arrives no earlier than tick
//!                D+1 does. With sd > 0 that needs no travel: a job or a
//!                report that waits up to sd before it is read delays
//!                what comes after it (read lag, not travel). Judged
//!                only for jobs with D + 1 <= H. A lost report is never
//!                read, so it is never late.
//!   throughput   the source's TCTL prop1, EU(time <= 10, !doneJobs1,
//!                doneJobs1) with doneJobs1 = am1.doneJobs > 4: CAN am1
//!                start its fifth fitting job by time H? INVERTED exit
//!                code: at the instant its doneJobs becomes 5, am1 sends
//!                one zero-transit Fifth to a monitor thread that waits
//!                Finite(H) from time 0 and asserts false when it reads
//!                it, so exit 101 = REACHABLE (the TCTL formula is true)
//!                and exit 0 = unreachable. Fifth is the one message of
//!                this file that is not the protocol's: the property
//!                relates am1's state to an instant on another clock, so
//!                one probe carries it there. A lost probe is a timeout:
//!                it can hide the start, never invent one. (The paper
//!                checks am2; the property file, used here, checks am1.)
//!                The paper reports no verdict for this formula, only
//!                state counts, so the throughput verdicts below are not
//!                a reproduction either.
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
//!                                   verified little): H <= 248, 165, 123,
//!                                   98 for K = 1, 2, 3, 4; larger values
//!                                   exit 2.
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
//! ## Printed counters
//!
//! Every counter is raised by program code at the step where its event
//! happens and summed over every ending the search explores. They are
//! observation only: the verdict is exit 101 with a certified witness,
//! without --keep-going. Program code also runs on endings the engine
//! then finds to admit no timeline (impossible=), so a count above zero
//! is an upper bound whenever impossible > 0 (it was 0 in every run
//! below); a count of zero is sound.
//!
//!   execs, blocked      the checker's counts: endings without a failed
//!                       assertion, and endings stopped at one (the RM
//!                       and the AMs are daemons, so nothing else blocks).
//!                       The throughput monitor ends right after its
//!                       assertion, and the engine files such an ending
//!                       under execs: for throughput read fifth=a/b
//!   dispatched, done, preempted, expired, late
//!                       jobs handed out, "done" reports read, give-ups,
//!                       queue expiries, late "done" reports
//!   judged              "done" reports whose deadline lies inside H
//!   min_queue_left      smallest remaining deadline of a waiting job after
//!                       aging; 1 on an expiry hold = tightest case explored
//!   min_dispatch_dline  smallest dline handed out; == c_hi on a miss or
//!                       preempt hold = tightest case explored
//!   min_report_slack    smallest D - (last tick read) over judged on-time
//!                       reports; 0 on a late-report hold = tightest case
//!   fifth=a/b           a: the monitor read am1's fifth start by time H;
//!                       b: am1 started a fifth fitting job at any time
//!   violations          failed assertions (events: with --keep-going a
//!                       thread runs on to its next TraceForge step after
//!                       a failed assertion, so one ending can count more
//!                       than one)
//!
//! The binary WARNs when a run verified nothing (0 executions, nothing
//! dispatched), when no miss can fit inside the horizon whatever the
//! timing (DD > H + 1 for expiry, DD >= H + c_hi for preempt: a
//! horizon-trivial hold), and when late-report judged no report.
//!
//! ## Results (re-measured 2026-10-13 after the first-arrival ruling; nothing is predicted)
//!
//! "old" is the harness version (commit 0cbe2c7), "new" this file, on the
//! same cell, one worker, 300 s limit. "new" has the receive rule of
//! 2026-10-13: a timed receive takes the message that arrived first, a
//! tie leaving both orders (the baseline mode has no clock and is not
//! affected by it). Verdicts are from runs WITHOUT
//! --keep-going (FIRE = exit 101, in timed mode with a certified witness;
//! HOLD = exit 0); the "new" counts are execs / blocked / violations of
//! the same cell WITH --keep-going. Cells are K=1, QS=2, C=2, L=0, U=0,
//! sd=0, H=10 unless stated. The command of a row is
//!
//!     yarn_scheduler_timed --mode timed [--keep-going] <flags>
//!
//! with the flags of the row (--deadline DD, --property P, --horizon H,
//! --u U, --l L, --ams K, --queue-size QS).
//!
//!   property     cell            old                        new
//!   miss         DD=3            FIRE  0 / 9,070 / 32       FIRE  7 / 5 / 6
//!   miss         DD=8            HOLD  16 / 8,072 / 0       FIRE  223 / 14 / 14
//!   miss         DD=11           HOLD  16 / 8,072 / 0       FIRE  253 / 6 / 6
//!   miss         DD=12           not run                    HOLD  259 / 0 / 0, horizon-trivial
//!   miss         U=1 H=14 DD=11  FIRE                       FIRE  3,013 / 54 / 54
//!   miss         U=1 H=14 DD=12  no verdict                 FIRE  3,103 / 26 / 26
//!   expiry       DD=8            HOLD  16 / 8,072 / 0       FIRE  223 / 14 / 14
//!   expiry       DD=11           HOLD  16 / 8,072 / 0       FIRE  253 / 6 / 6
//!   expiry       DD=12           not run                    HOLD  259 / 0 / 0, horizon-trivial
//!   expiry       QS=3 DD=8       not run                    FIRE  176 / 25 / 27
//!   preempt      DD=3            FIRE  0 / 9,070 / 32       FIRE  43 / 17 / 17
//!   preempt      DD=7            FIRE                       FIRE  222 / 9 / 9
//!   preempt      DD=8            HOLD  16 / 8,072 / 0       HOLD  259 / 0 / 0
//!   preempt      U=1 H=14 DD=11  not run                    FIRE  3,119 / 12 / 12
//!   preempt      U=1 H=14 DD=12  not run                    HOLD  3,167 / 0 / 0
//!   preempt      K=2 QS=3 DD=8   HOLD  1,262 / 141,835 / 0  FIRE  26,359 / 160 / 160
//!   late-report  DD=3            HOLD  32 / 9,070 / 0       HOLD  457 / 0 / 0
//!   late-report  DD=8            HOLD  16 / 8,072 / 0       HOLD  259 / 0 / 0
//!   late-report  U=1 H=16 DD=13  FIRE                       FIRE  7,463 / 3 / 3
//!   late-report  U=1 H=16 DD=14  no verdict                 HOLD  7,471 / 0 / 0
//!
//! (Counts are execs / blocked / violations with --keep-going; an old
//! FIRE without counts was not re-run with --keep-going, or that run did
//! not finish in 300 s. "no verdict": the old binary did not finish in
//! 300 s; its header, measured before every send became lossy, recorded a
//! hold for both cells. "horizon-trivial": no expiry fits in the horizon,
//! and the binary warns. In the old version almost every ending was a
//! discarded one: 16 complete executions against 8,072 blocked at DD=8.)
//! Every hold above that is not horizon-trivial shows its tightest-case
//! counter (min_dispatch_dline = 2 = c_hi for preempt, min_report_slack
//! = 0 for late-report) and judged > 0 for late-report.
//!
//! What moved, and why.
//!
//!   - expiry and miss now FIRE at DD=8 and DD=11, where the old version
//!     held, and hold only at DD=12, where no expiry fits in the horizon
//!     (H=10; the binary warns). At U=0 they FIRE at every DD from 1 to
//!     11 and hold at 12 and 13; at U=1, H=14 they FIRE at every DD from
//!     8 to 15 and hold at 16. Certified witness at DD=8: the RM hands
//!     job 0 to the AM at time 0; the AM works for 2 and sends "done",
//!     and that send is lost (marked [dropped] in the witness); the RM
//!     reads its own ticks at 1, 2, .. 7 and no report, so the AM stays
//!     busy in the RM's table and the job waiting behind it, aged by one
//!     per tick, runs out at tick 7. A lost RunJob ends the same way (the
//!     RM frees an AM only when it reads that AM's report). The
//!     witnesses at DD=11, at QS=3 DD=8 and at U=1, H=14, DD=11 and 12
//!     lose job 0's report as well. The old boundaries
//!     "expiry holds iff DD >= P + 1" and "miss holds iff DD >= P + c_hi",
//!     with P = ceil(QS / K) * (c_hi + 2U + 1), described the harness
//!     version and are WITHDRAWN, together with the cells of the old
//!     matrix that supported them.
//!   - preempt keeps its old verdicts with ONE helper (FIRE 7 / HOLD 8 at
//!     U=0, FIRE 11 / HOLD 12 at U=1, H=14; DD 1 to 13 at U=0 and DD 8
//!     to 16 at U=1 measured): a helper that never gets its next job
//!     cannot give one up, and the witnesses lose nothing. At DD=7 the
//!     report reaches the RM at the instant of its own tick (at times 2
//!     and 5) and the RM reads the tick first: jobs 0, 1, 2 are handed
//!     out at ticks 0, 3, 6 with 7, 4, 1 units left and the AM gives
//!     job 2 up at time 7. With two helpers it moves: K=2, QS=3, DD=8
//!     held in the old version and FIREs now. Witness: am1's second
//!     report (job 2) is lost on the send, so am1 stays busy for ever in
//!     the RM's table and am2 serves the queue alone; job 6 is handed
//!     out at tick 10 with 1 unit left and am2 gives it up at 11. The
//!     old boundary "preempt holds iff DD >= P + c_hi" is therefore
//!     WITHDRAWN for K >= 2 and stands only as measured above for K = 1.
//!   - late-report keeps every verdict it had on these cells, which have
//!     one helper (a lost message ends all hand-outs, so loss cannot make
//!     a report late): HOLD at U=0 (every DD from 1 to 12 measured), and
//!     FIRE at DD=13 / HOLD at DD=14 for U=1, H=16 (DD 8 to 15 measured),
//!     the boundary DD >= P + c_hi + 2U of the old header at K=1. The
//!     certified witness at DD=13 loses nothing: both transits take their
//!     longest time, job 2 is handed out at tick 10 with 3 units left
//!     (D = 13), the AM is done at 13, and the report arrives at 14, the
//!     instant of tick 14, which the RM reads first. The other old
//!     late-report cells: L=2 U=2, H=21 FIREs at DD=19 (284 / 2 / 2) and
//!     holds at DD=20 (288 / 0 / 0), as the old header recorded; with
//!     two helpers (K=2, QS=3, L=1 U=1, H=16) it FIREs at DD=13 (13,577
//!     / 88 / 88) and at DD=14 (13,993 / 12 / 12), where the old header
//!     recorded a hold and one helper holds: the witness loses am2's
//!     first report and am1 serves the queue alone. It FIREs at DD=15
//!     too, and at DD=16 with H=20; the hold side is not located.
//!
//! The first-arrival rule moved no verdict: every cell above, and every
//! deadline of the sweeps named in the bullets, has the verdict the same
//! file had before it, when a receive could take a later message and let
//! an earlier one expire. The timed cells now explore 8 to 29 percent
//! fewer executions (the baseline cells explore the same number). What
//! changed is the certified witnesses: those that need a stuck helper
//! (expiry and miss at DD >= 8, K=2 preempt) used to rely on a report
//! that was sent and expired unread, and now lose it on the send.
//!
//! The source's own constants (DD=3, K=1) FIRE in both versions, and the
//! new witnesses lose nothing. In each of them job 0's report reaches
//! the RM at time 2, the instant of its own tick 2, and the RM reads the
//! tick first. For miss, the AM still looks busy at tick 2, so the job
//! waiting behind job 0 runs out there; for preempt, job 1 is handed out
//! at tick 3 with 1 unit left and given up at time 4.
//!
//! throughput (INVERTED: FIRE = reachable), DD=3 unless stated, old and
//! new agree on every cell:
//!
//!   timed  U=0:           unreachable at H=7 and 8; reachable at H=9, 10
//!                         (H=5 to 12 measured: unreachable up to 8)
//!   timed  L=0 U=1:       reachable at H=9 and 10 (unreachable at H=6,
//!                         7, 8)
//!   timed  L=1 U=1:       unreachable at H=10
//!   timed  L=1 U=1 DD=12: unreachable at H=16 (fifth=0/4: am1 does start
//!                         a fifth job, after 16: the time bound does the
//!                         work; the old binary did not finish this cell
//!                         in 300 s), reachable at H=17
//!   baseline U=0:         unreachable at H=5 and 6; reachable at H=7, 8,
//!                         10, and at L=1 U=1 H=10 (no clock, so it
//!                         over-claims)
//!
//! Counts move there too (execs / blocked / violations, old then new):
//! timed U=0 at H=7: 24 / 1,087 / 0, now 148 / 0 / 0; H=8: 32 / 2,219 / 0,
//! now 218 / 0 / 0; H=9: 49 / 4,511 / 2, now 321 / 0 / 2; H=10:
//! 68 / 9,116 / 4, now 465 / 0 / 6. The monitor returns after its failed
//! assertion, so a reaching execution is filed under execs.
//!
//! baseline, the same program without a clock: FIRE (exit 101) at miss
//! DD=3 and 8, expiry DD=8, preempt DD=3 and 8 and late-report DD=8, in
//! both versions. Without time, ticks race ahead of reports, so preempt
//! DD=8 and late-report DD=8, which the timed run proves safe, are
//! reported as violations.
//! Counts, old then new: miss DD=3 0 / 23,553 / 1,024, now 11 / 11 / 13;
//! miss DD=8 802 / 23,553 / 222, now 5,101 / 193 / 201; expiry DD=8
//! 893 / 23,553 / 131, now 5,494 / 138 / 138; preempt DD=3
//! 21 / 23,553 / 1,003, now 385 / 138 / 138; preempt DD=8
//! 905 / 23,553 / 119, now 5,639 / 85 / 85; late-report DD=8
//! 965 / 23,553 / 59, now 6,017 / 81 / 81 (the same with and without the
//! first-arrival rule).
//!
//! Cost. The new version explores far less than the old one, whose
//! endings were mostly discarded ones: miss DD=8 timed, 8,088 executions
//! in 11 s before, 237 in about 0.2 s now; preempt K=2 QS=3 DD=8, 143,097
//! executions and 171 s for the old hold, under 1 s to the new FIRE and
//! 26,519 executions in about 45 s for the whole space with
//! --keep-going. The first-arrival rule removes executions but makes
//! each one dearer: the same cells take about 1.5 times as long as
//! without it (K=2: 45 s against 30 s).
//!
//! CLI: --horizon 249 (K=1), 166 (K=2), 124 (K=3) and 99 (K=4) exit 2;
//! one below each is accepted.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Nondet, Stats, WaitTime};

// Outcome counters: observation only, the verdict is the assertion at
// the site. Each is raised by program code at the step where its event
// happens (VIOLATIONS just before the failing assert), summed over every
// ending the search explores, and reset before each verify run. Program
// code also runs on endings the engine then judges to admit no timeline
// (Stats::timeline_impossible), so a count above zero is an upper bound
// whenever that is non-zero; a count of zero is sound. With --keep-going
// a failed assertion stops its own thread only, and what the other
// threads do afterwards is counted too.
static DISPATCHED: AtomicUsize = AtomicUsize::new(0);
static DONE_REPORTS: AtomicUsize = AtomicUsize::new(0);
static JUDGED_REPORTS: AtomicUsize = AtomicUsize::new(0);
static PREEMPTED: AtomicUsize = AtomicUsize::new(0);
static QUEUE_EXPIRED: AtomicUsize = AtomicUsize::new(0);
static LATE_REPORTS: AtomicUsize = AtomicUsize::new(0);
static FIFTH_IN_TIME: AtomicUsize = AtomicUsize::new(0);
// am1 started its fifth fitting job at SOME time (throughput only;
// counted at am1 when its doneJobs becomes 5).
static FIFTH_STARTS: AtomicUsize = AtomicUsize::new(0);
static VIOLATIONS: AtomicUsize = AtomicUsize::new(0);
// Tightness evidence (minimum over every explored ending): how close the
// explored executions came to each kind of miss. A hold whose minimum
// sits exactly at the edge (queue left 1, dispatch dline == c_hi,
// report slack 0) shows the tightest case was actually explored.
static MIN_QUEUE_LEFT: AtomicU64 = AtomicU64::new(u64::MAX);
static MIN_DISPATCH_DLINE: AtomicU64 = AtomicU64::new(u64::MAX);
static MIN_REPORT_SLACK: AtomicU64 = AtomicU64::new(u64::MAX);

const TAG_FIFTH: u32 = 1;

/// Messages received by the ResourceManager.
#[derive(Clone, Debug, PartialEq)]
enum RmMsg {
    /// The RM's own timer (source: `self.checkQueue() after(1)`). The RM
    /// counts the ticks it reads, so the message carries no index.
    Tick,
    /// Source: `rm.update(deadline_miss)`. `am` stands in for Rebeca's
    /// `sender`; `job` is bookkeeping.
    Update { am: usize, job: u32, miss: bool },
}

/// Messages received by an ApplicationMaster.
#[derive(Clone, Debug, PartialEq)]
enum AmMsg {
    /// Source: `amI.runJob(fifo_queue[0])`. `job` and `tick` are
    /// bookkeeping; `rm` is the address the AM answers to, carried by
    /// the packet (Rebeca's known rebec `rm`).
    RunJob { job: u32, dline: u64, tick: u32, rm: ThreadId },
}

/// The throughput probe (--property throughput only), the one message
/// that is not the protocol's: am1's doneJobs just became 5 (tag
/// TAG_FIFTH, zero transit).
#[derive(Clone, Debug, PartialEq)]
enum MonMsg {
    Fifth,
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

    /// A queue expiry is asserted (it is always counted).
    fn asserts_expiry(self) -> bool {
        matches!(self, Property::Miss | Property::Expiry)
    }

    /// A give-up is asserted (it is always counted).
    fn asserts_preempt(self) -> bool {
        matches!(self, Property::Miss | Property::Preempt)
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
// ResourceManager
// =====================================================================

/// The job an AM is running, as far as the RM knows.
#[derive(Clone, Copy, Debug)]
struct Running {
    job: u32,
    tick: u32,
    dline: u64,
}

/// A daemon: the Rebeca actor shape, one blocking receive loop over its
/// own timer and the AMs' reports, for ever. Its timer is a self-sent
/// Tick like any other message: a lost Tick stops the RM's clock for
/// good, and the RM then only reads reports.
fn resource_manager(p: Params, ams: Vec<ThreadId>) {
    let me = thread::current().id();
    let k = ams.len();
    // fifo_queue: remaining deadline of each waiting job, front first.
    let mut queue: Vec<u64> = vec![p.deadline; p.queue_size];
    // appMasterI == BUSY  <=>  running[i].is_some()
    let mut running: Vec<Option<Running>> = vec![None; k];
    // Ticks read so far; the next Tick read is tick number `ticks`.
    let mut ticks: u32 = 0;
    let mut next_job: u32 = 0;

    // Source: the constructor's `self.checkQueue()`, tick 0 at time 0.
    traceforge::send_msg_timed(me, RmMsg::Tick, 0, 0);
    loop {
        match traceforge::recv_msg_block_timed::<RmMsg>() {
            RmMsg::Tick => {
                let index = ticks;
                ticks += 1;

                // checkQueue(), step 1: hand the front job to every free
                // AM, in AM order, refilling the back of the queue.
                for (i, slot) in running.iter_mut().enumerate() {
                    if slot.is_none() {
                        let dline = queue[0];
                        let job = next_job;
                        next_job += 1;
                        *slot = Some(Running { job, tick: index, dline });
                        traceforge::send_msg(ams[i], AmMsg::RunJob { job, dline, tick: index, rm: me });
                        DISPATCHED.fetch_add(1, Ordering::Relaxed);
                        MIN_DISPATCH_DLINE.fetch_min(dline, Ordering::Relaxed);
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
                        // Property expiry: a waiting job's deadline ran
                        // out in the queue, at tick `index` <= H (the
                        // event the source flags in m_queue_misses).
                        QUEUE_EXPIRED.fetch_add(1, Ordering::Relaxed);
                        if p.property.asserts_expiry() {
                            VIOLATIONS.fetch_add(1, Ordering::Relaxed);
                            traceforge::assert(false);
                        }
                        queue.remove(i);
                        queue.push(p.deadline);
                    }
                    i += 1;
                }
                if let Some(&m) = queue.iter().min() {
                    MIN_QUEUE_LEFT.fetch_min(m, Ordering::Relaxed);
                }

                // Source: `self.checkQueue() after(1)`, re-armed up to
                // tick H (the bounded horizon); after tick H the RM hands
                // out nothing and keeps reading reports.
                if index < p.horizon {
                    traceforge::send_msg_timed(me, RmMsg::Tick, 1, 1);
                }
            }
            RmMsg::Update { am, job, miss } => {
                // update(deadline_miss): appMasterI = FREE. A give-up
                // (miss) was judged by the AM itself; nothing to judge.
                let run = match running.get_mut(am).and_then(Option::take) {
                    Some(run) if run.job == job => run,
                    // Unreachable: an AM reports exactly once per job.
                    other => panic!("rm: unexpected Update am={am} job={job}, running {other:?}"),
                };
                if !miss {
                    DONE_REPORTS.fetch_add(1, Ordering::Relaxed);
                    // Absolute deadline in tick units. Every Update is
                    // read after the Tick that dispatched its job, so
                    // ticks >= 1 here.
                    let deadline_tick = u64::from(run.tick) + run.dline;
                    let last_tick = u64::from(ticks - 1);
                    if deadline_tick < u64::from(p.horizon) {
                        JUDGED_REPORTS.fetch_add(1, Ordering::Relaxed);
                        if last_tick > deadline_tick {
                            // Property late-report: this "done" is read
                            // after tick number deadline + 1.
                            LATE_REPORTS.fetch_add(1, Ordering::Relaxed);
                            if p.property == Property::LateReport {
                                VIOLATIONS.fetch_add(1, Ordering::Relaxed);
                                traceforge::assert(false);
                            }
                        } else {
                            MIN_REPORT_SLACK.fetch_min(deadline_tick - last_tick, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }
}

// =====================================================================
// ApplicationMaster
// =====================================================================

/// A daemon: it reacts to every RunJob, for ever.
fn app_master(index: usize, p: Params, monitor: Option<ThreadId>) {
    let mut done_jobs: u32 = 0;
    let mut fifth_sent = false;
    loop {
        let AmMsg::RunJob { job, dline, rm, .. } = traceforge::recv_msg_block_timed::<AmMsg>();
        // Source: int completion = 2; //?(2, 5);
        let completion = p.work.pick();
        if completion > dline {
            // Self-preemption: stop when the deadline passes.
            traceforge::sleep(dline);
            // Property preempt: the job is given up here and now (the
            // event the source flags in m_update_miss when the RM reads
            // the report; the AM does not wait for that read).
            PREEMPTED.fetch_add(1, Ordering::Relaxed);
            if p.property.asserts_preempt() {
                VIOLATIONS.fetch_add(1, Ordering::Relaxed);
                traceforge::assert(false);
            }
            traceforge::send_msg(rm, RmMsg::Update { am: index, job, miss: true });
        } else {
            done_jobs += 1;
            if done_jobs == 5 && !fifth_sent {
                if let Some(m) = monitor {
                    FIFTH_STARTS.fetch_add(1, Ordering::Relaxed);
                    // The throughput probe, at the instant doneJobs1
                    // becomes true.
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
}

// =====================================================================
// Throughput monitor (--property throughput only)
// =====================================================================

/// An ordinary thread: one finite wait, then it ends.
fn throughput_monitor(horizon: u32) {
    // Waits from time 0 for at most H: the Some branch is feasible iff
    // am1's fifth fitting job can start at a time <= H. A lost probe is
    // a timeout: it can hide the start, never invent one.
    let fifth = traceforge::recv_tagged_msg_timed::<_, MonMsg>(
        |_s, tag| tag == Some(TAG_FIFTH),
        WaitTime::Finite(u64::from(horizon)),
    );
    if fifth.is_some() {
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
/// The RM hands an AM at most one job per tick, so each AM runs at most
/// H+1 jobs.
///   RM:    begin + the first Tick send + (H+1) tick reads + H re-arms +
///          <= K(H+1) dispatches + <= K(H+1) report reads + the final
///          block (the receive that waits for ever, or a failed assert).
///   AM:    begin + <= (H+1) jobs * (read, choice, sleep, send) + Fifth +
///          the final block.
fn max_thread_events(ams: usize, horizon: u32) -> u64 {
    let ticks = u64::from(horizon) + 1;
    let k = ams as u64;
    let rm = ticks * (2 * k + 2) + 2;
    let am = 4 * ticks + 3;
    rm.max(am)
}

fn build_config(mode: Mode, p: Params, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_all_sends_lossy()
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
        &DISPATCHED,
        &DONE_REPORTS,
        &JUDGED_REPORTS,
        &PREEMPTED,
        &QUEUE_EXPIRED,
        &LATE_REPORTS,
        &FIFTH_IN_TIME,
        &FIFTH_STARTS,
        &VIOLATIONS,
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
        // The AMs and the RM are daemons: they react for ever and nobody
        // waits for them. Ids go through the spawn closures (the AMs
        // learn the RM's from RunJob), so every thread starts at time 0.
        let mut am_ids: Vec<ThreadId> = Vec::new();
        for i in 0..p.ams {
            let m = if i == 0 { monitor_id } else { None };
            am_ids.push(thread::spawn_daemon(move || app_master(i, p, m)).thread().id());
        }
        thread::spawn_daemon(move || resource_manager(p, am_ids));
        // Only the monitor ends by itself (its one wait is finite).
        if let Some(h) = monitor {
            let _ = h.join();
        }
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    dispatched: usize,
    done: usize,
    judged: usize,
    preempted: usize,
    expired: usize,
    late: usize,
    fifth: usize,
    fifth_starts: usize,
    violations: usize,
    min_queue_left: u64,
    min_dispatch_dline: u64,
    min_report_slack: u64,
}

fn read_counts() -> Counts {
    Counts {
        dispatched: DISPATCHED.load(Ordering::Relaxed),
        done: DONE_REPORTS.load(Ordering::Relaxed),
        judged: JUDGED_REPORTS.load(Ordering::Relaxed),
        preempted: PREEMPTED.load(Ordering::Relaxed),
        expired: QUEUE_EXPIRED.load(Ordering::Relaxed),
        late: LATE_REPORTS.load(Ordering::Relaxed),
        fifth: FIFTH_IN_TIME.load(Ordering::Relaxed),
        fifth_starts: FIFTH_STARTS.load(Ordering::Relaxed),
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
         execs={execs} blocked={block} impossible={imp} dispatched={disp} done={ot} \
         judged={jd} preempted={pre} expired={exp} late={late} fifth={fifth}/{fs} \
         min_queue_left={mq} min_dispatch_dline={md} min_report_slack={ms} \
         violations={viol} time={dur:?}",
        prop = p.property.name(), k = p.ams, qs = p.queue_size, dd = p.deadline,
        c = p.work.label(), h = p.horizon, l = p.l, u = p.u, sd = p.sd,
        execs = stats.execs, block = stats.block, imp = stats.timeline_impossible, disp = c.dispatched,
        ot = c.done, jd = c.judged, pre = c.preempted, exp = c.expired, late = c.late,
        fifth = c.fifth, fs = c.fifth_starts, mq = show_min(c.min_queue_left), md = show_min(c.min_dispatch_dline),
        ms = show_min(c.min_report_slack), viol = c.violations, dur = dur,
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
    // Executions EXPLORED is execs + block: a violating execution ends at
    // its failed assertion and is filed under `block`, so dividing complete
    // executions alone would misstate the reduction.
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
        // Only reachable with --keep-going (otherwise the first violation
        // aborts). A failed assertion stops the RM or an AM, and that
        // execution is filed under `blocked` (nothing else blocks: the
        // reactors are daemons). The monitor's failed assertion is its
        // last step, so its thread still ends and the engine files that
        // execution under `execs`: for throughput read fifth=a/b.
        println!(
            "VIOLATIONS ({label}): {} failed assertions counted (--keep-going continued past \
             them; exit 0 here does NOT mean the property holds).",
            c.violations
        );
    } else if stats.execs == 0 {
        println!(
            "WARNING ({label}): 0 complete executions: this run verified nothing; treat it as \
             no data, not as a hold."
        );
    }
    // The counters are raised by PROGRAM code, which also runs on endings
    // the engine then judges to admit no timeline. A count of zero is
    // sound; a count above zero is an upper bound. The verdict is the
    // engine's: exit 101 with a certified witness, without --keep-going.
    if stats.timeline_impossible > 0 {
        println!(
            "NOTE ({label}): {} endings admit no timeline and were counted too: every counter \
             above is an upper bound.",
            stats.timeline_impossible
        );
    }
    if stats.execs > 0 && c.dispatched == 0 {
        println!("WARNING ({label}): no execution handed out a job (dispatched=0); treat it as no data.");
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
                     --horizon is capped at 248, 165, 123, 98 for K = 1..4 (TraceForge's \
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
