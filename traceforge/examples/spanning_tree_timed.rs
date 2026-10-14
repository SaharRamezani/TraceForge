//! Perlman's spanning tree for bridged networks: can the forward delay be too short?
//!
//! ## The algorithm in plain words
//!
//! Picture several small office networks, each one a cable with computers
//! on it, joined together by boxes called bridges. A bridge sits on two or
//! more cables and copies every message it hears on one cable onto the
//! others. That is all it does. The trouble starts when the cables and
//! bridges form a ring: a message copied around the ring comes back to
//! where it started and is copied again, and again, forever. Nothing in
//! the message lets a bridge notice the repeat, so one ring can flood and
//! stall the whole network. Rings are common anyway: people add spare
//! cables so that the network survives a broken one, or simply plug a
//! cable into the wrong socket.
//!
//! Radia Perlman's algorithm (R. Perlman, "An Algorithm for Distributed
//! Computation of a Spanning Tree in an Extended LAN", Ninth Data
//! Communications Symposium, ACM SIGCOMM 1985, published in Computer
//! Communication Review 15(4), pages 44 to 53) lets the bridges agree, only
//! by talking to each other, on which of their connections to use, so that
//! no ring is left but every cable can still be reached. Every other
//! connection between a bridge and a cable is switched off and kept in
//! reserve.
//!
//! Who takes part: only the bridges. Each has a unique number. The bridge
//! with the smallest number becomes the leader (the paper calls it the
//! root). Every bridge works out how many steps away from the leader it
//! is. On every cable, the bridge closest to the leader becomes that
//! cable's spokesman (the paper says "designated bridge"); if two are
//! equally close, the smaller number wins.
//!
//! What they tell each other: a short hello saying "I believe the leader
//! is bridge X, I am N steps away from it, and I am bridge Y". The leader
//! sends a hello on each of its cables at a steady rhythm. A spokesman
//! passes a fresh hello on to its other cables as soon as one reaches it
//! from the leader's side. A bridge that hears a better offer on a cable
//! goes quiet there. Each bridge keeps copying on the one cable that leads
//! towards the leader and on the cables where it is the spokesman, and
//! switches all the others off.
//!
//! The timers, and what each one is for:
//!
//!   * Hello time: how often the leader repeats its hello.
//!   * Maximum age: how long a bridge trusts the last hello it heard on a
//!     cable. If nothing fresh comes in that long, the bridge assumes
//!     something broke and works out a new plan, which may mean switching
//!     a spare connection on.
//!   * Switch-off delay (the paper's "pre-backup delay"): a connection that
//!     should be switched off keeps copying a little longer, so that the
//!     network is not cut in two while the news is still travelling.
//!   * Switch-on delay (the paper's "pre-forwarding delay", today usually
//!     called the forward delay): a connection that should be switched on
//!     first waits. The wait must be long enough for the news to reach
//!     every bridge and for every connection that has to close to have
//!     closed. Perlman asks for it to be at least the switch-off delay plus
//!     the longest time news can need to cross the network, counting a few
//!     lost hellos.
//!
//! What can go wrong: if the switch-on wait is too short, a new connection
//! starts copying while an old one that should close is still copying. For
//! that moment the cables form a ring and messages go round and round.
//! This is called a transient loop. It heals by itself, but even a short
//! one can flood a network.
//!
//! This example uses the smallest ring there is: three bridges, A (number
//! 1, the leader), B (number 2) and C (number 3), joined by three cables
//! A-B, B-C and C-A. A ring exists exactly when all six bridge-to-cable
//! connections copy at the same moment. It tells two stories.
//!
//! Story 1, "a cable is plugged in" (the default). The A-B cable is
//! plugged in after the other two cables have settled without it. Before
//! that, B reached the leader through C, so C was the spokesman on B-C,
//! and all four existing connections copied. When the new cable comes up,
//! both of its ends start their switch-on wait. The leader's next hello
//! goes from A to B. B learns it is now only one step from the leader,
//! which makes B the better spokesman for cable B-C (as close as C, and a
//! smaller number), so B passes the hello on to C. C now starts switching
//! its B-C connection off: it keeps copying for the switch-off delay, then
//! stops. If cable A-B starts copying before C has stopped, there is a
//! loop.
//!
//! Story 2, "hellos go missing". All three cables have settled and C's
//! B-C connection is off, because B is the spokesman there. Some of B's
//! hellos to C get lost or arrive late. After the maximum age C stops
//! trusting its old information, decides it should be the spokesman on
//! B-C itself, and starts its switch-on wait. If no hello from B gets
//! through before that wait ends, C switches on while B is still copying
//! onto B-C: a loop.
//!
//! The checker tries every order and every allowed delay of the messages
//! within the bounds you give, and reports whether a loop can happen.
//! Every message may also be lost on the way, and a message that has
//! arrived stays readable only for a short storage time (0 by default).
//! A bridge that is waiting takes the message that arrived first, so it
//! never lets a readable one go by (see Losses). The checker explores
//! these runs like any other, and a loop they cause is reported like any
//! other.
//!
//! What the checker finds, in short. A hello that is never read is news
//! that never arrives, and no timer can make up for it. In story 1 a loop
//! is reachable whatever the timers are: there is a run in which no hello
//! gets through to C before the new cable opens. With a clock, in such a
//! run each hello that could reach C earlier is lost on one of its two
//! hops (C, waiting, would read one that reached it), and the others, if
//! delivered, reach it no earlier than the cable opens; without a clock
//! C may also just not pick a delivered one up. In story 2 it is
//! reachable as soon as the run is long enough to hold one maximum age
//! plus one switch-on wait: every hello in that window is lost. Both are
//! reported with and without a clock, in timed mode with a witness
//! timeline. The timer boundaries this file reported before 2026-10-13
//! came from a harness that threw away every run in which the awaited
//! hello was never read; they described that filtered model and are
//! withdrawn (see Results).
//!
//! ## How the protocol maps onto TraceForge
//!
//! Threads: one per bridge (A, B, C) plus `main`, which only spawns them.
//! There is no bootstrap message: `main` spawns C, then B (which gets C's
//! id through its spawn closure), then A (which gets B's id and, in
//! link-up, C's), and every receive selects by tag alone. Every bridge
//! clock therefore starts at 0: the instant the cable comes up (link-up)
//! or the start of observation (lost-hellos). A bridge that only reacts
//! is a daemon thread with a blocking receive: B in both stories (a loop
//! that never returns) and C in link-up (its first wait is blocking). A
//! daemon that waits for ever does not make an execution blocked. The
//! bridges that drive a run are ordinary threads and end by themselves:
//! A, which sleeps and sends R hellos, and C in lost-hellos, which waits
//! only finite times. `main` joins only those.
//! Each point-to-point LAN is a direct channel between its two bridges;
//! only the channels that carry news in the story exist.
//!
//! Messages: `Hello { root, cost, sender, age }` carries the paper's Root
//! ID, path length, transmitting bridge ID and AGE (the link identifier
//! and MAX_AGE fields are constant here and dropped). The only other
//! message is `Msg::ForwardingNotice`, the probe of the link-up property,
//! see below. There is no bootstrap, release or report message.
//! Decisions follow the paper: a bridge is Designated on a LAN unless it
//! heard a better claim there (lower root, else shorter path, else lower
//! bridge ID; `is_designated`), and link state moves by the p.48 table
//! (`port_rule`): FORWARDING/PRE_BACKUP forward data, BACKUP/PRE_FORWARDING
//! do not.
//!
//! Timers (all as ratios over U on the command line):
//!
//!   * HELLO_TIME `H`: A's `sleep` between hellos.
//!   * phase `P` (link-up only): A's first hello on the new cable goes out
//!     `P` after link-up, `0 <= P <= H` (default `H`: the root's previous
//!     hello went out just before the cable came up).
//!   * MAX_AGE `MA` (lost-hellos only): C's finite-wait receive for B's
//!     next hello, started at time 0 (C holds B's information with age 0)
//!     and then at the read of each hello. Relays are immediate, so AGE is
//!     0 at receipt and the wait is the paper's aging.
//!   * PRE_BACKUP_DELAY `Bd` (link-up only): C's finite-wait receive right
//!     after B's hello made it defer on B-C. The paper (p.49) gives two
//!     lower bounds, `Bd >= MAX_AGE + MaxPropTime` and, from its root
//!     failure argument, `Bd >= 2 * MaxPropTime`, and then simplifies to
//!     `2 * MAX_AGE`, which is the default here.
//!   * PRE_FORWARDING_DELAY `Fd`: link-up: A sleeps until `Fd` and opens
//!     A-B (B's end of A-B started its identical timer at the same instant,
//!     so A's expiry stands for both ends). lost-hellos: C's finite-wait
//!     receive after the age-out. Default `3 * MA` (paper).
//!   * MaxDelay: per-hop transit `[L, U]` plus storage lifetime `sd` over
//!     the two hops A to B to C. MaxLostMsgs has no counterpart: the
//!     semantics may lose any number of hellos (see Losses).
//!
//! Loop detection without a clock read (link-up). The property relates
//! two bridges at one instant: when A-B opens, C's B-C end must not
//! forward. It is observed with ONE probe, `Msg::ForwardingNotice`, sent
//! by A to C at time `Fd` with `send_tagged_msg_timed(.., 0, 0)`: zero
//! transit, so it arrives exactly when A-B opens, and it stays readable
//! until `Fd + sd`. C asserts on its own B-C state when it reads the
//! notice, and it reads it only in the two states in which its B-C end
//! still forwards (old FORWARDING before the news, PRE_BACKUP after it).
//! A read at time `r` therefore shows one instant (`r >= Fd`) at which
//! A-B (both ends), B-C (both ends) and C-A (both ends, never touched)
//! all forward: a loop. A never sends C anything else, so no message of
//! its own channel stands before the notice; at C it competes only with
//! B's relays, and the earlier arrival is read first (see Losses). It is
//! the only message in this file that no bridge would send. Like every
//! send it may be lost, or expire unread; that can only hide the loop of
//! that run (the same run with the notice read is explored too), it can
//! never create one. In lost-hellos the check is local and needs no
//! probe: C's own B-C end opens while B (never told otherwise) keeps
//! forwarding onto B-C, and A-B and C-A are untouched.
//!
//! Losses: only the semantics loses messages. Every send may be dropped,
//! with no budget, in both modes (the Config sets `with_all_sends_lossy`).
//! In timed mode a message that is not read within `sd` of its arrival
//! expires, and a receive takes the matching message that arrived first
//! (the first-arrival rule of 2026-10-13; a tie leaves both orders), so a
//! bridge that is waiting never lets a readable message go by. A message
//! therefore goes unread only if it is lost, if its receiver is not
//! waiting for it when it arrives (C has stopped, or waits for the notice
//! alone), or if it ties: it arrives at the very instant at which the
//! receive takes another message or the wait times out. For a bridge
//! these are the same thing: the hello is never read. The program takes
//! no loss decision of its own (no `nondet()`, no budget, no schedule),
//! and all of B's hellos to C carry one tag, so C takes the first of them
//! to arrive.
//!
//! ## The bound of the run
//!
//! The root is bounded to R hellos (`--rounds`). A bounded root that has
//! fallen silent must never be what closes a loop, and no message may
//! announce the end of a run (it could be lost, and its loss would
//! fabricate a loop). The bound is therefore local arithmetic.
//!
//!   * link-up: the check happens at `Fd`, so the root must send every
//!     hello the real root sends up to `Fd`, those at `P + i*H <= Fd`:
//!     `R >= (Fd - P)/H + 1` if `Fd >= P`, else 1. That is the default; a
//!     smaller `--rounds` is refused (exit 2), a larger one is allowed.
//!     C's B-C state can only move from forwarding to not forwarding, and
//!     what it is at `Fd` depends only on hellos sent by then, so a loop
//!     asserted at the read of the notice is a loop of the real root too.
//!   * lost-hellos: hello number R of the real root, the first one the
//!     bounded root does not send, would leave at `R*H` and reach C at
//!     `T = R*H + 2L` at the earliest. A message that can arrive before
//!     `T` comes from one of the R hellos that are sent, so a wait whose
//!     DEADLINE is at most `T` sees exactly what it would see with the
//!     real root (at the deadline itself an arrival may be missed anyway).
//!     C cannot read its clock, but a completed wait lasted at most its
//!     length, so C keeps `left`, `T` minus the lengths of the waits it
//!     has started, and starts a wait only if its length still fits;
//!     otherwise it stops, silently, with no assertion. The default R is
//!     the smallest with `R*H + 2L >= MA + Fd` (at least 1), so that one
//!     full age-out plus one forward delay fits. A smaller `--rounds` is
//!     allowed; no reopening can then be reached, and the binary prints a
//!     NOTE that a hold is vacuous for that reason.
//!
//! ## Deviations from the paper (deliberate)
//!
//!   * Topology: a triangle of point-to-point LANs, the smallest cycle.
//!   * Start state: the model starts from a settled tree and injects one
//!     change (a cable added, or hellos lost), instead of running the
//!     election from power-on. Settled per-link states are hard-coded; in
//!     lost-hellos C starts at time 0 with B-C in BACKUP and B's
//!     information at age 0.
//!     A root failure or a cable failure cannot create a loop in a
//!     triangle (it removes the only cycle), so the hazards modelled are
//!     the two the paper's timers guard: PRE_FORWARDING vs PRE_BACKUP
//!     after a change, and a false MAX_AGE expiry.
//!   * A cable plugged into running bridges: the paper gives the
//!     PRE_FORWARDING start (timer set to PRE_FORWARDING_DELAY) only for a
//!     bridge's own power-on (p.47) and says nothing about a link that
//!     comes up later. The model assumes such a link is set up the same
//!     way at both ends. The p.48 table leads to the same place: a new
//!     link has no stored hello, so the bridge is Designated there, and a
//!     BACKUP link that should forward goes to PRE_FORWARDING with its
//!     timer started at that instant, the link-up.
//!   * Only the traffic that can move the race is sent. link-up: A's
//!     hellos on A-B and B's relay on B-C. lost-hellos: A to B to C.
//!     Steady refresh hellos on unchanged links (C-A, and C to B before
//!     the change) are omitted; they are assumed to arrive well within
//!     MAX_AGE, so they never change a decision.
//!   * Triggered hellos are omitted: a Designated Bridge also answers at
//!     once when it hears a worse claim on its LAN (p.47). link-up: before
//!     B hears A, B may relay the root's previous hello (heard through C)
//!     onto the new cable and A answers immediately; that news path is 4
//!     hops long and beats the root's next periodic hello only when it
//!     lands before `P + U + sd`, roughly when `H > 3(U + sd)` and `P = H`.
//!     lost-hellos: after the age-out C relays the root's next hello onto
//!     B-C and B answers at once. That answer travels three hops from the
//!     root hello (A to C to B to C, each as slow as `U + sd` in the worst
//!     timeline), so it can land no earlier than B's 2-hop periodic relay
//!     of the same root hello (at most `2(U + sd)`), and the latest moment
//!     news reaches C is still set by the relay. Each omitted answer is
//!     one more frame that would have to be lost for the same loop, and
//!     the semantics may lose it like any other send.
//!     In both cases the model can only report MORE loops than a real
//!     bridge; `--phase-ratio` lets you shorten the link-up news delay.
//!   * The root is bounded to R hellos, and in lost-hellos C stops when its
//!     time budget is used up (see "The bound of the run").
//!   * Priorities, link costs, the self-loop rule (p.48), one-way link
//!     detection (p.52), the hello hold-down (p.51), the forwarding
//!     database and non-participating bridges are omitted; none affects
//!     the race in a triangle.
//!   * Ties: when C's B-C end closes at exactly the instant A-B opens (or
//!     B's hello reaches C at exactly the instant C's B-C end opens), the
//!     timed receive may read at its deadline or miss the message there
//!     (both are explored), so a zero-length overlap counts as a loop.
//!   * IEEE 802.1D (secondary source only; the standard was not read) has
//!     no PRE_BACKUP state and a two-stage forward delay. `--backup-delay-
//!     ratio 0` with `Fd = 2 * forward_delay` approximates it.
//!
//! ## No harness (since 2026-10-13)
//!
//! The loop assertions sit where the loop closes: on C's read of the
//! notice (link-up) and on C's forward-delay timeout (lost-hellos). With a
//! clock, a timeout branch is a real execution exactly where the checker's
//! timeout miss condition admits it: no message the receive could have
//! taken can have arrived inside the wait, judged when the execution is
//! complete (without a clock any timeout is possible). Until 2026-10-13
//! this file wrapped the protocol in a verification harness on top of
//! that. It is gone:
//!
//!   * Validation reads. Each property-relevant branch was followed by a
//!     BLOCKING read of the very message that had been awaited
//!     (`assume_hello_was_late` after C's forward-delay timeout,
//!     `assume_news_unprocessed` when C read the notice before any news,
//!     `assume_notice_was_late` after C's switch-off delay), so that the
//!     branch counted only if that message was read later. What those
//!     reads filtered out, once the miss condition handled the timing, was
//!     every execution in which the hello or the notice was LOST or
//!     EXPIRED unread, since such a message is never read. Under the
//!     model's semantics every send may be lost, a loop caused by a hello
//!     that is never read is a loop like any other, and nothing is
//!     filtered. In lost-hellos C also began with a blocking wait for B's
//!     first delivered hello, so a run started only once a hello had got
//!     through; C now starts at time 0 in the settled state.
//!   * The file's own loss model. `--losses K` let the sender of one hop
//!     decide with `traceforge::nondet()` which of its hellos never
//!     arrive, at most K of them, and B's hellos to C carried their
//!     delivery index in the tag so that each wait at C was pinned to one
//!     message. Only the semantics loses messages now, without a budget,
//!     and the default number of hellos no longer follows K.
//!   * Harness messages. `main` sent every bridge an `Init` with the
//!     peers' ids, and `Msg::Done` travelled from A to B (in lost-hellos
//!     on to C) to let the reactive bridges stop. Both were sends like any
//!     other: a dropped one left a bridge waiting for ever, and an Init
//!     took up to U to arrive, so a bridge could start up to U late (in
//!     the old link-up witness under Results, B and C read theirs at time
//!     1). Ids now go through the spawn closures, reactive bridges are
//!     daemons, and the end of a run is local arithmetic (see "The bound
//!     of the run").
//!
//! What is left that no bridge would do is the one probe of the link-up
//! property and, in lost-hellos, the subtraction that bounds C's run.
//!
//! ## Properties checked
//!
//! Every check is a local assertion at C, at the step where the loop
//! closes, with its counter raised just before it:
//!
//!   link-up:     when C reads A's notice (A-B now forwards at both ends),
//!                C's own B-C end must not forward:
//!                  LOOP_BEFORE_NEWS    (C had read no hello from B yet)
//!                  LOOP_IN_PRE_BACKUP  (C was inside its switch-off delay)
//!   lost-hellos: C's B-C end must never reach FORWARDING while B, the
//!                rightful Designated Bridge, forwards onto B-C:
//!                  LOOP_REOPENED
//!
//! A loop is reported whatever kept the news from C: a hello that was
//! lost or late.
//!
//! Evidence counters (observation only): link-up NEWS_HEARD (C entered
//! PRE_BACKUP) and SETTLED_BACKUP (C's switch-off delay ran out before it
//! read the notice); lost-hellos REFRESHED, AGED_OUT and REVERTED (a hello
//! from B cancelled C's PRE_FORWARDING). Counters accumulate over all
//! explored executions, so treat them as zero/nonzero evidence.
//!
//! Soundness of the link-up check: the notice arrives at `Fd` exactly and
//! C reads it at `r >= Fd` while its B-C end forwards, so the instant `r`
//! has all six ends forwarding. Completeness: C reads the notice in both
//! of its forwarding states. Its first wait takes whichever of the news
//! and the notice arrives first (either, when they arrive together), so
//! the notice is read whenever no news came before it, and its switch-off
//! wait takes the notice up to and including its deadline.
//!
//! ## CLI parameters (ratios are over U)
//!
//!   --scenario link-up|lost-hellos  story (default link-up)
//!   --mode baseline|timed|compare   verification mode (default timed;
//!                                   without --keep-going compare aborts
//!                                   at the FIRE of its baseline leg)
//!   --rounds R                      root hellos sent. Default: from the
//!                                   timers (see "The bound of the run").
//!                                   link-up: (Fd - P)/H + 1 if Fd >= P,
//!                                   else 1; fewer is refused. lost-hellos:
//!                                   the smallest R >= 1 with R*H + 2L >=
//!                                   MA + Fd; fewer (at least 1) is allowed,
//!                                   a hold is then vacuous and a NOTE says
//!                                   so
//!   --u U                           per-hop transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U) (default 0.0)
//!   --sd-ratio SR                   sd = round(SR * U), how long a message
//!                                   stays readable after arrival (default 0.0)
//!   --hello-ratio HR                H = round(HR * U), >= 1 (default 2.0)
//!   --phase-ratio PR                P = round(PR * U), 0 <= P <= H
//!                                   (link-up only, refused with
//!                                   lost-hellos; default P = H)
//!   --max-age-ratio MR              MA = round(MR * U), >= 1 (default 6.0
//!                                   for link-up, 3.0 for lost-hellos,
//!                                   where the default R grows with
//!                                   MA + Fd)
//!   --backup-delay-ratio BR         Bd = round(BR * U) (link-up only,
//!                                   refused with lost-hellos; default 2 * MA)
//!   --forward-delay-ratio FR        Fd = round(FR * U) (default 3 * MA)
//!   --keep-going                    explore past a violation and exit 0; the
//!                                   number of loop hits is printed as
//!                                   violations= (read that, not the exit code)
//!   --parallel none|shared|partitioned  exploration strategy (default none)
//!
//! CLI misuse exits 2, distinct from a property violation's 101, so
//! exit-code-based sweep harnesses cannot mistake a typo for a FIRE. That
//! includes a flag the chosen scenario does not use, `--rounds 0`, a
//! link-up `--rounds` below its minimum, an R above 10,000 (also when it
//! is the default for the given timers), and a U or scaled timer above
//! 10^12 ticks (a cap that keeps every derived time, such as `3 * MA` or
//! `P + R * H`, far from integer overflow).
//!
//! ## Results (re-measured 2026-10-13 after the first-arrival rule; "old"
//! is the harness version at HEAD 0cbe2c7 on the same cell, one worker,
//! measured before that rule)
//!
//! Under the model's semantics the news can always be lost. In link-up the
//! run in which C reads no hello and then the notice exists at every
//! setting; in lost-hellos the run in which C reads no hello for `MA + Fd`
//! exists as soon as `R*H + 2L >= MA + Fd`. Both exist with and without a
//! clock, so both modes FIRE, and the verdict no longer depends on the
//! timers (in lost-hellos only on whether the run is that long). Every
//! new row and every further cell below agrees: link-up FIREs in both
//! modes in all of them, and lost-hellos FIREs in every one with
//! `R*H + 2L >= MA + Fd` and holds, vacuously, in the two (R = 2 and
//! R = 5) where it does not.
//!
//! Measured. Counts are from a run with --keep-going, the verdict from a
//! run without it (FIRE = exit 101, in timed mode with a certified witness
//! timeline; HOLD = exit 0). Timers are the defaults (U=1 L=0 sd=0 H=2;
//! link-up P=2 MA=6 Bd=12 Fd=18; lost-hellos MA=3 Fd=9) unless a flag is
//! shown. Command:
//!
//!   spanning_tree_timed [--scenario lost-hellos] [--max-age-ratio 2]
//!       [--rounds R] --mode timed|baseline [--keep-going]
//!
//! with `--rounds R` given whenever R is not the binary's own default
//! (old: 1 in link-up, 2 in lost-hellos; new: 9, 3 and 6 in the three
//! cells).
//!
//!   cell          R  ver  mode      execs   blocked  violations  verdict
//!   link-up       1  old  timed     1       45       0           HOLD
//!   link-up       1  old  baseline  3       45       4           FIRE
//!   link-up       9  old  timed     12,761  174,325  6,568       FIRE
//!   link-up       9  old  baseline  29,523  171,405  39,364      FIRE
//!   link-up       9  new  timed     47,296  0        13,762      FIRE
//!   link-up       9  new  baseline  77,708  0        38,854      FIRE
//!   link-up MA 2  1  old  timed     2       46       2           FIRE
//!   link-up MA 2  1  old  baseline  3       45       4           FIRE
//!   link-up MA 2  3  old  timed     26      310      34          FIRE
//!   link-up MA 2  3  old  baseline  39      297      52          FIRE
//!   link-up MA 2  3  new  timed     73      0        37          FIRE
//!   link-up MA 2  3  new  baseline  92      0        46          FIRE
//!   lost-hellos   2  old  timed     18      88       0           HOLD
//!   lost-hellos   2  old  baseline  17      73       3           FIRE
//!   lost-hellos   2  new  timed     11      0        0           HOLD (vacuous)
//!   lost-hellos   2  new  baseline  14      0        0           HOLD (vacuous)
//!   lost-hellos   6  old  timed     2,165   6,933    3           FIRE
//!   lost-hellos   6  old  baseline  3,281   6,457    1,641       FIRE
//!   lost-hellos   6  new  timed     1,322   0        96          FIRE
//!   lost-hellos   6  new  baseline  2,838   0        729         FIRE
//!
//! ("link-up MA 2" is `--max-age-ratio 2`, i.e. MA=2 Bd=4 Fd=6. The two
//! vacuous holds print the NOTE of "The bound of the run".)
//!
//! Reading the new rows. Nothing can block in this version (A, and C in
//! lost-hellos, never wait for ever; B, and C in link-up, are daemons), and
//! under --keep-going a violating run is filed under execs as well,
//! because C ends right after its failed assertion. So every explored
//! ending is a complete execution, blocked is 0, and violations counts
//! executions: in every cell it equals the engine's own count of failed
//! assertions (one `task-N` line each, which in timed mode the engine
//! prints only for an execution it certified), and no ending was judged
//! timeline-impossible (the printed `impossible=` is 0 in every cell).
//!
//! What the first-arrival rule changed. It orders the reads of a receive
//! that can take messages of two senders, and C's first receive in
//! link-up (B's relays and A's notice) is the only such receive in this
//! file; every other receive has one sender. So only the timed link-up
//! rows moved. Every baseline row (no clock, no rule), every lost-hellos
//! row and every verdict are as they were before the rule; the timed
//! link-up rows lost executions and, in most cells, violations. At the
//! defaults this program measured 66,083 executions and 32,293 violations
//! before it, with loop_before_news 19,683 = 3^9 in timed mode as in
//! baseline, because C could take the notice while relays that had
//! reached it earlier expired unread.
//!
//! Reading the old rows. At its own default R the old timed run holds at
//! the default timers (rows R = 1 and R = 2), as its law said. Given as
//! many hellos as the new default it FIREs as well, because the semantics
//! had been dropping hellos beyond the budget K since every send became
//! lossy (2026-09-30). Its two certified witnesses: link-up, the root's
//! first hello is dropped, C reads the relay of the second at 6 and is
//! still in its switch-off delay when it reads the notice at 18, the
//! deadline tie; lost-hellos, the root's hellos sent at 2 to 8 are
//! dropped, C times out at 3 and at 12, and the validation read takes the
//! relay of the last hello at 12. Most of what the old version explored
//! are blocked runs. At lost-hellos R = 2 the old baseline FIREs and the
//! new one holds: the new program stops C by its time budget in both
//! modes, so a run too short for one age-out plus one forward delay
//! asserts nothing.
//!
//! Certified witness, link-up defaults, timed (exit 101; the same on
//! every schedule seed tried). A's hellos leave at 2, 4, .., 18 and B
//! reads and relays all nine. The relays of the first seven (sent at 2 to
//! 14) are dropped on B to C; those of the last two (sent at 16 and 18)
//! are delivered. C reads no hello: its first receive, waiting since time
//! 0, takes the notice at 18, and A-B opens at 18 while C's B-C end is
//! still FORWARDING: the loop. The seven relays have to be lost: a relay
//! of the hello sent at `t` reaches C within `[t, t + 2]`, so each of
//! those arrives before 18 if it is delivered, and C, waiting, would read
//! it, since the receive takes the message that arrived first. The last
//! two cannot beat the notice: the relay of the hello sent at 16 can only
//! arrive at 18 itself, a tie that leaves both orders, and that of the
//! hello sent at 18 arrives at 18 or later. Before the first-arrival rule
//! C passed over the seven relays and let them expire unread, so none
//! needed to be lost; those runs are gone. The old version discarded this
//! run (its `assume_news_unprocessed` read of B's first relay cannot
//! succeed: that relay is lost). At this cell loop_before_news is 1,152 =
//! 2^7 * 3^2 in timed mode: each of the first seven hellos is dropped on
//! A-B or its relay on B-C (two fates), and each of the last two has a
//! third, the relay delivered; with the 12,610 loop_in_pre_backup that
//! makes the 13,762 violations. In baseline it is 19,683 = 3^9, since
//! without a clock a delivered relay may stay unread for any of the nine
//! hellos.
//!
//! Certified witness, lost-hellos defaults, timed (exit 101; the same on
//! every schedule seed tried). A's hellos leave at 0, 2, .., 10; B reads
//! and relays all six. The first five relays are dropped on B to C. The
//! sixth is delivered and never read: its hello left at 10, so it reaches
//! C by 12, and C's two waits time out at 3 (aging) and at 12 (forward
//! delay), which the engine admits only if nothing arrived in [0, 3) and
//! in [3, 12); so it arrives exactly at 12, the deadline, where it may be
//! missed. At 12 C's B-C end goes FORWARDING while B still forwards onto
//! B-C: the loop. The clock counts here as it does in link-up: a timed
//! timeout needs every hello that could arrive inside the wait to be lost
//! (96 violating runs = 2^5 * 3), while without a clock a delivered relay
//! may also simply not be read in time (729 = 3^6).
//!
//! Further cells, new binary, the flags added to the command above; `v of
//! n` = violating executions of all executions explored, timed / baseline
//! (blocked = 0 and impossible = 0 in each; every FIRE confirmed by exit
//! 101 without --keep-going). link-up:
//!   --max-age-ratio 5 (R 7): 1,522 of 4,924 / 4,246 of 8,492. The old
//!       binary holds this cell at its R = 1 and FIREs with --rounds 7
//!       (timed: 730 violations).
//!   --max-age-ratio 4 (R 6): 809 of 1,943 / 1,394 of 2,788.
//!   --max-age-ratio 2 --sd-ratio 1 (R 3): 37 of 73 / 46 of 92.
//!   --max-age-ratio 2 --l-ratio 1 (R 3): 33 of 60 / 46 of 92.
//!   --max-age-ratio 2 --phase-ratio 0 (R 4): 101 of 227 / 146 of 292.
//!   --max-age-ratio 2 --backup-delay-ratio 0 (R 3): 28 of 74 / 46 of 92.
//!   --max-age-ratio 2 --rounds 4 (more than the minimum): 111 of 219 /
//!       146 of 292.
//!   --max-age-ratio 2 --forward-delay-ratio 1 (Fd 1 < P, R 1): 3 of 6 /
//!       4 of 8.
//!   --u 2 --max-age-ratio 1 --hello-ratio 1 --l-ratio 0.5 --sd-ratio 0.5
//!       (U=2 L=1 sd=1 H=2 MA=2, R 3): 42 of 69 / 46 of 92.
//! lost-hellos (`--scenario lost-hellos`):
//!   --max-age-ratio 2 --forward-delay-ratio 3 (R 3): 12 of 54 / 27 of 73.
//!       The old binary holds this cell at its R = 2 and FIREs with
//!       --rounds 3 (timed: 3 violations).
//!   --l-ratio 1 (R 5): 32 of 243 / 243 of 858.
//!   --sd-ratio 1 (R 6): 96 of 1,637 / 729 of 2,838.
//!   --max-age-ratio 4 --forward-delay-ratio 3 --hello-ratio 5 (R 2): 6 of
//!       12 / 9 of 20.
//!   --rounds 8 (two more than the default): 1,584 of 15,069 / 12,866 of
//!       40,777.
//!   --rounds 5 (one short of the default): HOLD, vacuous, with the NOTE;
//!       411 / 636 executions, reverted = 0.
//!
//! Cost at the link-up defaults with --keep-going, on a shared machine:
//! new timed 47,296 executions in 51 to 68 s and new baseline 77,708 in 12
//! to 15 s (eight runs each); old with --rounds 9, timed 187,086 endings
//! in 106 s and baseline 200,928 in 20 s (one run each). The timed run
//! explores fewer executions than the baseline and takes longer; no
//! efficiency claim is made here.
//!
//! WITHDRAWN. Everything this header reported before 2026-10-13 was
//! measured on 2026-09-16 with the validation reads and with losses
//! limited to the budget K (sends became lossy without a budget on
//! 2026-09-30), i.e. over the executions in which the awaited hello or
//! notice is read in the end. It is no longer claimed: the link-up law
//! (timed FIRE iff Fd <= P + K*H + Bd + 2U + 2sd, and MA <= P + K*H + 2U +
//! 2sd at the paper's simplified settings), the lost-hellos law (timed
//! FIRE iff MA + Fd <= (K+1)*H + 2(U - L) + 2sd), the matrix that located
//! them and its harness checks, and the reading of both laws as
//! corrections to Perlman's PRE_FORWARDING_DELAY rule (the phase term P,
//! the K + 1 hello periods of 802.1D, the false reopening once
//! H >= 6U + 2L + 6sd). The timelines those results described (news that
//! arrives one tick too late, the tie at a deadline) are still executions
//! of this model; they just no longer separate FIRE from HOLD, because a
//! hello that is never read closes the loop at every setting.
//!
//! Timed FIREs are certified: the checker validates every reported
//! counterexample against one consistent timeline and prints it as
//! "Certified witness timeline". A link-up hold with news_heard = 0 or
//! settled_backup = 0, a lost-hellos hold with reverted = 0, and any run
//! with execs = 0 print a WARNING and are no data; a lost-hellos run too
//! short for one age-out plus one forward delay prints the NOTE above.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

// Outcome counters (observation only; the verdict is the inline
// assertion): event counts accumulated across all explored executions of
// one verify run, each raised at the site where the event happens
// (revisited prefixes may re-count, so treat them as zero/nonzero
// evidence, not exact per-exec tallies). Reset before each verify, read
// after.
// link-up
static NEWS_HEARD: AtomicUsize = AtomicUsize::new(0);
static SETTLED_BACKUP: AtomicUsize = AtomicUsize::new(0);
static LOOP_BEFORE_NEWS: AtomicUsize = AtomicUsize::new(0);
static LOOP_IN_PRE_BACKUP: AtomicUsize = AtomicUsize::new(0);
// lost-hellos
static REFRESHED: AtomicUsize = AtomicUsize::new(0);
static AGED_OUT: AtomicUsize = AtomicUsize::new(0);
static REVERTED: AtomicUsize = AtomicUsize::new(0);
static LOOP_REOPENED: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 0.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const DEFAULT_HELLO_RATIO: f64 = 2.0;
const DEFAULT_MAX_AGE_RATIO_LINK_UP: f64 = 6.0;
const DEFAULT_MAX_AGE_RATIO_LOST_HELLOS: f64 = 3.0;
// Cap on U and on every scaled timer. With R <= 10_000 the largest derived
// time (3 * MA, P + R * H, ...) stays near 10^16, far below u64::MAX.
const MAX_TIME: u64 = 1_000_000_000_000;
const MAX_ROUNDS: u32 = 10_000;

// Bridge IDs (the smallest is the Root).
const ID_A: u32 = 1;
const ID_B: u32 = 2;
const ID_C: u32 = 3;

// Tags: receive predicates see only (sender, tag), and every receive in
// this file selects by tag alone, so no bridge needs a sender's id. All of
// B's hellos to C carry the one tag TAG_RELAY: C takes any of them.
const TAG_ROOT_HELLO: u32 = 1;
const TAG_NOTICE: u32 = 2;
const TAG_RELAY: u32 = 3;

/// The paper's HELLO (p.46), minus the constant link identifier and
/// MAX_AGE fields.
#[derive(Clone, Debug, PartialEq)]
struct Hello {
    root: u32,
    cost: u32,
    sender: u32,
    age: u64,
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    Hello(Hello),
    /// Not Perlman (link-up only): the property's one probe, A to C with
    /// zero transit at the instant A-B opens (see the doc header). It is
    /// the only message in this file that no bridge would send.
    ForwardingNotice,
}

/// Per-bridge, per-link state (p.46).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LinkState {
    Forwarding,
    PreBackup,
    Backup,
    PreForwarding,
}

impl LinkState {
    /// p.48: "Data traffic is forwarded to and from links in the
    /// FORWARDING and PRE_BACKUP states."
    fn forwards(self) -> bool {
        matches!(self, LinkState::Forwarding | LinkState::PreBackup)
    }
}

/// p.46: a bridge is Designated Bridge on a LAN unless it heard there a
/// claim with a lower Root ID, or the same root and a shorter path, or the
/// same path length and a lower transmitting bridge ID.
fn is_designated(my_root: u32, my_dist: u32, my_id: u32, heard: Option<&Hello>) -> bool {
    match heard {
        None => true,
        Some(h) => (my_root, my_dist, my_id) < (h.root, h.cost, h.sender),
    }
}

/// p.48 link-state table. `should_forward` is "root link or Designated".
/// Entering PRE_FORWARDING / PRE_BACKUP starts the matching timer, which
/// the caller runs as a receive with that wait.
fn port_rule(cur: LinkState, should_forward: bool) -> LinkState {
    use LinkState::*;
    match (should_forward, cur) {
        (true, Forwarding) | (true, PreBackup) => Forwarding,
        (true, PreForwarding) | (true, Backup) => PreForwarding,
        (false, Backup) | (false, PreForwarding) => Backup,
        (false, PreBackup) | (false, Forwarding) => PreBackup,
    }
}

/// p.46-47 timer pulse: once the PRE_FORWARDING / PRE_BACKUP delay has run
/// out, the link moves on to FORWARDING / BACKUP. Other states run no timer.
fn timer_expired(cur: LinkState) -> LinkState {
    match cur {
        LinkState::PreForwarding => LinkState::Forwarding,
        LinkState::PreBackup => LinkState::Backup,
        other => other,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    LinkUp,
    LostHellos,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::LinkUp => "link-up",
            Scenario::LostHellos => "lost-hellos",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    u: u64,
    l: u64,
    sd: u64,
    hello: u64,
    phase: u64,
    max_age: u64,
    backup_delay: u64,
    forward_delay: u64,
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

impl Bounds {
    #[allow(clippy::too_many_arguments)]
    fn from_ratios(
        u: u64,
        l_ratio: f64,
        sd_ratio: f64,
        hello_ratio: f64,
        phase_ratio: Option<f64>,
        max_age_ratio: f64,
        backup_delay_ratio: Option<f64>,
        forward_delay_ratio: Option<f64>,
    ) -> Self {
        if u < 1 {
            cli_bail("U must be >= 1");
        }
        if u > MAX_TIME {
            cli_bail(&format!("U must be <= {MAX_TIME}"));
        }
        for (name, r) in [
            ("--l-ratio", Some(l_ratio)),
            ("--sd-ratio", Some(sd_ratio)),
            ("--hello-ratio", Some(hello_ratio)),
            ("--phase-ratio", phase_ratio),
            ("--max-age-ratio", Some(max_age_ratio)),
            ("--backup-delay-ratio", backup_delay_ratio),
            ("--forward-delay-ratio", forward_delay_ratio),
        ] {
            if let Some(r) = r {
                if !(r >= 0.0 && r.is_finite()) {
                    cli_bail(&format!("{name} must be a finite value >= 0"));
                }
                if (r * u as f64).round() > MAX_TIME as f64 {
                    cli_bail(&format!("{name} times U must be <= {MAX_TIME}"));
                }
            }
        }
        let scale = |r: f64| (r * u as f64).round() as u64;
        let l = scale(l_ratio);
        let sd = scale(sd_ratio);
        if l > u {
            cli_bail("transit lower bound L must be <= U (check --l-ratio)");
        }
        let hello = scale(hello_ratio);
        if hello < 1 {
            cli_bail("HELLO_TIME must round to >= 1 (check --hello-ratio)");
        }
        let phase = phase_ratio.map(scale).unwrap_or(hello);
        if phase > hello {
            cli_bail("the phase of the root's next hello must be <= HELLO_TIME (check --phase-ratio)");
        }
        let max_age = scale(max_age_ratio);
        if max_age < 1 {
            cli_bail("MAX_AGE must round to >= 1 (check --max-age-ratio)");
        }
        // Paper's simplified settings (p.49-50) unless given explicitly.
        let backup_delay = backup_delay_ratio.map(scale).unwrap_or(2 * max_age);
        let forward_delay = forward_delay_ratio.map(scale).unwrap_or(3 * max_age);
        Self { u, l, sd, hello, phase, max_age, backup_delay, forward_delay }
    }
}

/// Advance the calling bridge's clock from `*now` to absolute time `t`.
fn sleep_until(now: &mut u64, t: u64) {
    if t > *now {
        traceforge::sleep(t - *now);
        *now = t;
    }
}

// =====================================================================
// Story 1: link-up. Cable A-B comes up at time 0.
// =====================================================================

/// Bridge A, the Root. Its A-B end is PRE_FORWARDING from time 0 and
/// opens at Fd; its periodic hellos go out on A-B at P, P + H, ...
/// An ordinary thread: it sends `rounds` hellos and ends by itself.
fn root_link_up(b: Bounds, rounds: u32, b_tid: ThreadId, c_tid: ThreadId) {
    let mut now: u64 = 0;
    let mut ab = LinkState::PreForwarding;

    let open_ab = |now: &mut u64, ab: &mut LinkState| {
        sleep_until(now, b.forward_delay);
        // PRE_FORWARDING_DELAY expired: A-B forwards (at both ends, B's
        // identical timer started at the same instant).
        *ab = LinkState::Forwarding;
        // The property's ONE probe, not Perlman (doc header): a
        // zero-transit notice of that instant. C asserts on its own B-C
        // state when it reads it. Like every send it may be lost, which
        // can only hide a loop (the twin run with the notice delivered is
        // explored too), never create one.
        traceforge::send_tagged_msg_timed(c_tid, TAG_NOTICE, Msg::ForwardingNotice, 0, 0);
    };

    for i in 0..rounds {
        let t = b.phase + u64::from(i) * b.hello;
        if ab != LinkState::Forwarding && b.forward_delay < t {
            open_ab(&mut now, &mut ab);
        }
        sleep_until(&mut now, t);
        traceforge::send_tagged_msg(
            b_tid,
            TAG_ROOT_HELLO,
            Msg::Hello(Hello { root: ID_A, cost: 0, sender: ID_A, age: 0 }),
        );
    }
    if ab != LinkState::Forwarding {
        open_ab(&mut now, &mut ab);
    }
}

/// Bridge B. Settled before the cable: root link B-C at distance 2, with
/// C Designated on B-C. A's hello on A-B makes A-B its root link
/// (distance 1) and B Designated on B-C, so B relays at once.
/// Purely reactive, so a daemon: a blocking receive loop that never
/// returns.
fn bridge_b_link_up(c_tid: ThreadId) {
    // What B last heard on B-C from C before the change.
    let heard_on_bc = Hello { root: ID_A, cost: 1, sender: ID_C, age: 0 };
    let mut dist: u32 = 2;
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(|_s, t| t == Some(TAG_ROOT_HELLO)) {
            Msg::Hello(h) => {
                // Distance = 1 + best distance heard from a Designated
                // Bridge (p.46); A-B's end stays PRE_FORWARDING until Fd.
                dist = dist.min(h.cost + 1);
                if is_designated(h.root, dist, ID_B, Some(&heard_on_bc)) {
                    traceforge::send_tagged_msg(
                        c_tid,
                        TAG_RELAY,
                        Msg::Hello(Hello { root: h.root, cost: dist, sender: ID_B, age: h.age }),
                    );
                } else {
                    // Unreachable in the triangle: distance 1 ties C and
                    // wins on the lower ID.
                    panic!("bridge B: expected to become Designated on B-C");
                }
            }
            // Unreachable: only hellos carry TAG_ROOT_HELLO.
            m => panic!("bridge B: unexpected {m:?}"),
        }
    }
}

/// Bridge C. Root link C-A at distance 1, Designated on B-C (so its B-C
/// end forwards) until B's hello arrives. Reactive, so a daemon: if the
/// news and the notice are both lost it waits for ever in its first
/// receive, which is no blocked execution.
fn bridge_c_link_up(b: Bounds) {
    let my_root = ID_A;
    let my_dist: u32 = 1;
    let mut bc = LinkState::Forwarding;

    // Old state: wait for the news, or observe A-B opening first.
    let first = traceforge::recv_tagged_msg_block_timed::<_, Msg>(|_s, t| {
        t == Some(TAG_RELAY) || t == Some(TAG_NOTICE)
    });
    match first {
        Msg::ForwardingNotice => {
            // A-B forwards now, and C has read no news: its B-C end still
            // forwards, whether B's hellos were lost or reach C no
            // earlier than the notice. The loop is closed, here and now.
            LOOP_BEFORE_NEWS.fetch_add(1, Ordering::Relaxed);
            traceforge::assert(!bc.forwards());
            return;
        }
        Msg::Hello(h) => {
            // B-C is not C's root link; C is Designated there only if its
            // own claim beats B's.
            let designated = is_designated(my_root, my_dist, ID_C, Some(&h));
            bc = port_rule(bc, designated);
            if bc != LinkState::PreBackup {
                panic!("bridge C: expected PRE_BACKUP after B's hello, got {bc:?}");
            }
            NEWS_HEARD.fetch_add(1, Ordering::Relaxed);
        }
    }

    // PRE_BACKUP: keep forwarding for PRE_BACKUP_DELAY.
    match traceforge::recv_tagged_msg_timed::<_, Msg>(
        |_s, t| t == Some(TAG_NOTICE),
        WaitTime::Finite(b.backup_delay),
    ) {
        Some(Msg::ForwardingNotice) => {
            // A-B forwards now, and C's B-C end is still inside its
            // switch-off delay: the loop is closed, here and now.
            LOOP_IN_PRE_BACKUP.fetch_add(1, Ordering::Relaxed);
            traceforge::assert(!bc.forwards());
        }
        Some(m) => panic!("bridge C: unexpected {m:?}"),
        None => {
            // PRE_BACKUP_DELAY expired: B-C goes BACKUP and stops
            // forwarding.
            bc = timer_expired(bc);
            if bc != LinkState::Backup {
                panic!("bridge C: expected BACKUP after PRE_BACKUP_DELAY, got {bc:?}");
            }
            // B-C no longer forwards, so nothing is left to check; a
            // notice that comes later is simply not read.
            SETTLED_BACKUP.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// =====================================================================
// Story 2: lost-hellos. Settled tree with all three cables; C's B-C end
// is BACKUP because B is Designated on B-C.
// =====================================================================

/// Bridge A, the Root: a hello to B every HELLO_TIME, from time 0. An
/// ordinary thread: it sends `rounds` hellos and ends by itself.
fn root_steady(b: Bounds, rounds: u32, b_tid: ThreadId) {
    for i in 0..rounds {
        if i > 0 {
            traceforge::sleep(b.hello);
        }
        traceforge::send_tagged_msg(
            b_tid,
            TAG_ROOT_HELLO,
            Msg::Hello(Hello { root: ID_A, cost: 0, sender: ID_A, age: 0 }),
        );
    }
}

/// Bridge B, Designated on B-C: relays every root hello it reads at once.
/// Purely reactive, so a daemon: a blocking receive loop that never
/// returns.
fn bridge_b_steady(c_tid: ThreadId) {
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(|_s, t| t == Some(TAG_ROOT_HELLO)) {
            Msg::Hello(h) => {
                traceforge::send_tagged_msg(
                    c_tid,
                    TAG_RELAY,
                    Msg::Hello(Hello { root: h.root, cost: h.cost + 1, sender: ID_B, age: h.age }),
                );
            }
            // Unreachable: only hellos carry TAG_ROOT_HELLO.
            m => panic!("bridge B: unexpected {m:?}"),
        }
    }
}

/// The time up to which the bounded root stands for the real one
/// (lost-hellos): the real root's next hello, number R, would leave at
/// R*H and could reach C at R*H + 2L at the earliest (two hops), so a
/// message that can arrive before that instant comes from one of the R
/// hellos the bounded root does send.
fn steady_horizon(b: Bounds, rounds: u32) -> u64 {
    u64::from(rounds) * b.hello + 2 * b.l
}

/// Bridge C: ages B's information on B-C, reopens B-C after MAX_AGE plus
/// PRE_FORWARDING_DELAY of silence. An ordinary thread with finite waits
/// only: it ends by itself.
fn bridge_c_steady(b: Bounds, rounds: u32) {
    let my_root = ID_A;
    let my_dist: u32 = 1;
    let relay = |_s: ThreadId, t: Option<u32>| t == Some(TAG_RELAY);

    // Settled state at time 0: B-C is BACKUP and C holds B's information
    // with age 0, so the first aging wait starts at time 0.
    let mut bc = LinkState::Backup;

    // The bound of the run, as local arithmetic (no message announces it).
    // The root is bounded to R hellos, and the silence after the last one
    // is the end of the model, not something the protocol did. A wait
    // whose DEADLINE is at most T = R*H + 2L (`steady_horizon`) sees
    // exactly what it would see with the real root, whose hello number R
    // cannot arrive before T. C cannot read its clock, but a completed
    // wait lasted at most its length, so `left` (T minus the lengths of
    // the waits so far) is a lower bound of the time that remains before
    // T. C starts a wait only if it fits in `left`; otherwise it stops,
    // silently, with no assertion.
    let mut left = steady_horizon(b, rounds);

    loop {
        if left < b.max_age {
            return;
        }
        left -= b.max_age;
        // Aging: the stored hello expires after MAX_AGE without a newer one.
        match traceforge::recv_tagged_msg_timed::<_, Msg>(relay, WaitTime::Finite(b.max_age)) {
            Some(Msg::Hello(_)) => {
                REFRESHED.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Some(m) => panic!("bridge C: unexpected {m:?}"),
            None => {}
        }
        AGED_OUT.fetch_add(1, Ordering::Relaxed);
        // Information discarded: C now claims Designated on B-C.
        bc = port_rule(bc, is_designated(my_root, my_dist, ID_C, None));
        if bc != LinkState::PreForwarding {
            panic!("bridge C: expected PRE_FORWARDING after the age-out, got {bc:?}");
        }

        if left < b.forward_delay {
            return;
        }
        left -= b.forward_delay;
        // PRE_FORWARDING: newer information reverts to BACKUP at once
        // (p.49-50).
        match traceforge::recv_tagged_msg_timed::<_, Msg>(
            relay,
            WaitTime::Finite(b.forward_delay),
        ) {
            Some(Msg::Hello(h)) => {
                bc = port_rule(bc, is_designated(my_root, my_dist, ID_C, Some(&h)));
                if bc != LinkState::Backup {
                    panic!("bridge C: expected BACKUP after B's hello, got {bc:?}");
                }
                REVERTED.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Some(m) => panic!("bridge C: unexpected {m:?}"),
            None => {}
        }
        // PRE_FORWARDING_DELAY expired with no newer information.
        bc = timer_expired(bc);
        LOOP_REOPENED.fetch_add(1, Ordering::Relaxed);
        // B is Designated on B-C and never stopped forwarding there, so
        // C's end must not forward. bc is FORWARDING on every path that
        // gets here (the p.47 timer rule applied to PRE_FORWARDING), so
        // reaching this line IS the violation, whether B's hellos were
        // late or lost: the loop is closed, here and now.
        traceforge::assert(!bc.forwards());
        return;
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

/// Exploration strategy chosen on the command line (`--parallel`):
/// `none` (single-threaded, the default; keeps the exact exit-code
/// semantics of an aborting assertion), `shared` or `partitioned`.
static PARALLEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn apply_parallel(builder: traceforge::ConfigBuilder) -> traceforge::ConfigBuilder {
    match PARALLEL.get().map(|s| s.as_str()).unwrap_or("none") {
        "none" => builder,
        "shared" => builder.with_parallel(true),
        "partitioned" => builder.with_partitioned_parallelization(true),
        other => cli_bail(&format!("invalid --parallel: {other} (expected none|shared|partitioned)")),
    }
}

fn build_config(mode: Mode, b: Bounds, keep_going: bool) -> Config {
    let mut builder = apply_parallel(Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX));
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    // Identical program in both modes: only the Config differs.
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(b.l, b.u, b.sd).build(),
    }
}

fn reset_counts() {
    for c in [
        &NEWS_HEARD,
        &SETTLED_BACKUP,
        &LOOP_BEFORE_NEWS,
        &LOOP_IN_PRE_BACKUP,
        &REFRESHED,
        &AGED_OUT,
        &REVERTED,
        &LOOP_REOPENED,
    ] {
        c.store(0, Ordering::Relaxed);
    }
}

fn run(
    mode: Mode,
    scenario: Scenario,
    b: Bounds,
    rounds: u32,
    keep_going: bool,
) -> (Stats, Duration) {
    let cfg = build_config(mode, b, keep_going);
    reset_counts();
    let start = Instant::now();
    // No bootstrap message: ids go through the spawn closures, so every
    // bridge clock starts at 0. Spawn order C, B, A: B needs C's id, A
    // needs B's (and C's for the notice). Reactive bridges are daemons;
    // main joins only the threads that always end.
    let stats = traceforge::verify(cfg, move || match scenario {
        Scenario::LinkUp => {
            let hc = thread::spawn_daemon(move || bridge_c_link_up(b));
            let c_tid = hc.thread().id();
            let hb = thread::spawn_daemon(move || bridge_b_link_up(c_tid));
            let b_tid = hb.thread().id();
            let ha = thread::spawn(move || root_link_up(b, rounds, b_tid, c_tid));
            let _ = ha.join();
        }
        Scenario::LostHellos => {
            let hc = thread::spawn(move || bridge_c_steady(b, rounds));
            let c_tid = hc.thread().id();
            let hb = thread::spawn_daemon(move || bridge_b_steady(c_tid));
            let b_tid = hb.thread().id();
            let ha = thread::spawn(move || root_steady(b, rounds, b_tid));
            let _ = ha.join();
            let _ = hc.join();
        }
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    news: usize,
    settled: usize,
    loop_before: usize,
    loop_pre_backup: usize,
    refreshed: usize,
    aged_out: usize,
    reverted: usize,
    reopened: usize,
}

impl Counts {
    fn violations(&self) -> usize {
        self.loop_before + self.loop_pre_backup + self.reopened
    }
}

fn read_counts() -> Counts {
    Counts {
        news: NEWS_HEARD.load(Ordering::Relaxed),
        settled: SETTLED_BACKUP.load(Ordering::Relaxed),
        loop_before: LOOP_BEFORE_NEWS.load(Ordering::Relaxed),
        loop_pre_backup: LOOP_IN_PRE_BACKUP.load(Ordering::Relaxed),
        refreshed: REFRESHED.load(Ordering::Relaxed),
        aged_out: AGED_OUT.load(Ordering::Relaxed),
        reverted: REVERTED.load(Ordering::Relaxed),
        reopened: LOOP_REOPENED.load(Ordering::Relaxed),
    }
}

/// Vacuity guards: a run with zero complete executions verified nothing,
/// and an exit-0 run whose interesting path was never explored is no hold.
fn warn_if_vacuous(label: &str, scenario: Scenario, execs: usize, blocked: usize, c: Counts) {
    // A run that found counterexamples is never "no data", wherever the
    // checker files its violating executions (here under `execs`: C ends
    // right after its failed assertion), so warn only when there is no
    // violation either.
    if execs == 0 && c.violations() == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}): every execution \
             blocked, this run verified nothing; treat it as no data, not as a hold."
        );
    }
    if c.violations() > 0 {
        return;
    }
    match scenario {
        Scenario::LinkUp => {
            if c.news == 0 || c.settled == 0 {
                println!(
                    "WARNING ({label}): news_heard={} settled_backup={}: C never both heard the \
                     news and closed B-C before it read A's notice; this hold is vacuous, treat \
                     it as no data.",
                    c.news, c.settled
                );
            }
        }
        Scenario::LostHellos => {
            if c.reverted == 0 {
                println!(
                    "WARNING ({label}): reverted=0: no execution had C enter PRE_FORWARDING and \
                     then get corrected by B's hello, so the PRE_FORWARDING race was never \
                     exercised; treat this hold as no data for the forward-delay race."
                );
            }
        }
    }
}

/// The counters are raised by program code, so they also see endings that
/// the timeout-miss condition later judges to admit no timeline (those run
/// to completion first). A count of zero is sound; a count above zero is
/// then an upper bound, and the verdict is the engine's (exit 101 with a
/// certified witness, without --keep-going).
fn note_if_upper_bound(label: &str, stats: &Stats, c: Counts) {
    if stats.timeline_impossible > 0 && c.violations() > 0 {
        println!(
            "NOTE ({label}): {} of the {} endings explored admit no timeline and were counted \
             above too; violations={} is an upper bound. Re-run without --keep-going (exit 101 = \
             the engine certified a counterexample) before quoting it.",
            stats.timeline_impossible,
            stats.execs + stats.block + stats.timeline_impossible,
            c.violations()
        );
    }
}

/// lost-hellos only, computed from the parameters: if one age-out plus one
/// forward delay does not fit before the horizon R*H + 2L, C stops before
/// it could reopen B-C, so no loop can be reached and a hold is vacuous.
fn note_if_too_short(scenario: Scenario, b: Bounds, rounds: u32) {
    let horizon = steady_horizon(b, rounds);
    if scenario == Scenario::LostHellos && horizon < b.max_age + b.forward_delay {
        println!(
            "NOTE: R*H + 2L = {horizon} < MA + Fd = {}: C's time budget cannot hold one age-out \
             plus one forward delay, so C stops before it could reopen B-C and a hold here is \
             vacuous (the default --rounds for these timers is {}).",
            b.max_age + b.forward_delay,
            default_rounds(scenario, b)
        );
    }
}

// =====================================================================
// Reporting
// =====================================================================

fn params_line(scenario: Scenario, b: Bounds, rounds: u32) -> String {
    match scenario {
        Scenario::LinkUp => format!(
            "scenario=link-up R={rounds}  L={} U={} sd={} H={} P={} MA={} Bd={} Fd={}",
            b.l, b.u, b.sd, b.hello, b.phase, b.max_age, b.backup_delay, b.forward_delay
        ),
        Scenario::LostHellos => format!(
            "scenario=lost-hellos R={rounds}  L={} U={} sd={} H={} MA={} Fd={}",
            b.l, b.u, b.sd, b.hello, b.max_age, b.forward_delay
        ),
    }
}

fn counts_line(scenario: Scenario, c: Counts) -> String {
    match scenario {
        Scenario::LinkUp => format!(
            "news_heard={} settled_backup={} loop_before_news={} loop_in_pre_backup={} \
             violations={}",
            c.news, c.settled, c.loop_before, c.loop_pre_backup, c.violations()
        ),
        Scenario::LostHellos => format!(
            "refreshed={} aged_out={} reverted={} loop_reopened={} violations={}",
            c.refreshed, c.aged_out, c.reverted, c.reopened, c.violations()
        ),
    }
}

fn print_one(
    label: &str,
    scenario: Scenario,
    b: Bounds,
    rounds: u32,
    stats: &Stats,
    dur: Duration,
    c: Counts,
) {
    println!(
        "{label:<9} {}  execs={:<5} blocked={:<5} impossible={:<3} {} time={dur:?}",
        params_line(scenario, b, rounds),
        stats.execs,
        stats.block,
        stats.timeline_impossible,
        counts_line(scenario, c),
    );
}

fn print_compare(
    scenario: Scenario,
    b: Bounds,
    rounds: u32,
    baseline: (Stats, Duration),
    timed: (Stats, Duration),
    bc: Counts,
    tc: Counts,
) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("Perlman spanning tree ({}): MUST vs MUST-timed", scenario.name());
    println!("======================================================");
    println!("{}", params_line(scenario, b, rounds));
    println!();
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>14}",
        "mode", "execs", "blocked", "impossible", "time"
    );
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>14?}",
        "baseline", b_stats.execs, b_stats.block, b_stats.timeline_impossible, b_dur
    );
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>14?}",
        "timed", t_stats.execs, t_stats.block, t_stats.timeline_impossible, t_dur
    );
    println!();
    println!("baseline: {}", counts_line(scenario, bc));
    println!("timed:    {}", counts_line(scenario, tc));
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    scenario: String,
    mode: String,
    rounds: Option<u32>,
    u: u64,
    l_ratio: f64,
    sd_ratio: f64,
    hello_ratio: f64,
    phase_ratio: Option<f64>,
    max_age_ratio: Option<f64>,
    backup_delay_ratio: Option<f64>,
    forward_delay_ratio: Option<f64>,
    keep_going: bool,
    parallel: String,
}

fn next_val(args: &mut std::env::Args, flag: &str) -> String {
    args.next().unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
}

fn parse_num<T: std::str::FromStr>(v: String, flag: &str) -> T {
    v.parse().unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
}

fn parse_args() -> Args {
    // Default mode is `timed`: both modes FIRE for this benchmark, so
    // without --keep-going compare aborts in its first leg, the baseline
    // (see the doc header).
    let mut a = Args {
        scenario: String::from("link-up"),
        mode: String::from("timed"),
        rounds: None,
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        hello_ratio: DEFAULT_HELLO_RATIO,
        phase_ratio: None,
        max_age_ratio: None,
        backup_delay_ratio: None,
        forward_delay_ratio: None,
        keep_going: false,
        parallel: String::from("none"),
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--scenario" => a.scenario = next_val(&mut args, "--scenario"),
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--rounds" => a.rounds = Some(parse_num(next_val(&mut args, "--rounds"), "--rounds")),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--hello-ratio" => {
                a.hello_ratio = parse_num(next_val(&mut args, "--hello-ratio"), "--hello-ratio")
            }
            "--phase-ratio" => {
                a.phase_ratio =
                    Some(parse_num(next_val(&mut args, "--phase-ratio"), "--phase-ratio"))
            }
            "--max-age-ratio" => {
                a.max_age_ratio =
                    Some(parse_num(next_val(&mut args, "--max-age-ratio"), "--max-age-ratio"))
            }
            "--backup-delay-ratio" => {
                a.backup_delay_ratio = Some(parse_num(
                    next_val(&mut args, "--backup-delay-ratio"),
                    "--backup-delay-ratio",
                ))
            }
            "--forward-delay-ratio" => {
                a.forward_delay_ratio = Some(parse_num(
                    next_val(&mut args, "--forward-delay-ratio"),
                    "--forward-delay-ratio",
                ))
            }
            "--keep-going" => a.keep_going = true,
            "--parallel" => a.parallel = next_val(&mut args, "--parallel"),
            "--help" | "-h" => {
                eprintln!(
                    "Usage: spanning_tree_timed [--scenario link-up|lost-hellos] \
                     [--mode baseline|timed|compare] [--rounds R] [--u U] \
                     [--l-ratio LR] [--sd-ratio SR] [--hello-ratio HR] [--phase-ratio PR] \
                     [--max-age-ratio MR] [--backup-delay-ratio BR] [--forward-delay-ratio FR] \
                     [--keep-going] [--parallel none|shared|partitioned]\n\
                     Ratios are over U. Defaults: link-up, timed, U=1, L/U=0, sd/U=0, H/U=2, \
                     P=H, MA/U=6 (link-up) or 3 (lost-hellos), Bd=2*MA, Fd=3*MA, and R from the \
                     timers: link-up (Fd - P)/H + 1 if Fd >= P, else 1 (fewer is refused); \
                     lost-hellos the smallest R >= 1 with R*H + 2L >= MA + Fd (fewer is allowed, \
                     a hold is then vacuous).\n\
                     --keep-going explores past a violation, exits 0 and prints violations=.\n\
                     Exit 0 = no transient loop over the explored state space; exit 101 = a \
                     transient loop is reachable; exit 2 = CLI misuse."
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
    PARALLEL.set(a.parallel.clone()).expect("PARALLEL set once");
    let scenario = match a.scenario.as_str() {
        "link-up" => Scenario::LinkUp,
        "lost-hellos" => Scenario::LostHellos,
        other => cli_bail(&format!("invalid --scenario: {other} (expected link-up|lost-hellos)")),
    };
    // Refuse knobs the chosen story never reads, instead of ignoring them.
    if scenario == Scenario::LostHellos {
        for (flag, given) in [
            ("--phase-ratio", a.phase_ratio.is_some()),
            ("--backup-delay-ratio", a.backup_delay_ratio.is_some()),
        ] {
            if given {
                cli_bail(&format!("{flag} is link-up only; --scenario lost-hellos does not use it"));
            }
        }
    }
    let b = Bounds::from_ratios(
        a.u,
        a.l_ratio,
        a.sd_ratio,
        a.hello_ratio,
        a.phase_ratio,
        a.max_age_ratio.unwrap_or(match scenario {
            Scenario::LinkUp => DEFAULT_MAX_AGE_RATIO_LINK_UP,
            Scenario::LostHellos => DEFAULT_MAX_AGE_RATIO_LOST_HELLOS,
        }),
        a.backup_delay_ratio,
        a.forward_delay_ratio,
    );
    // The bound of the run is local arithmetic on the timers (doc header).
    let need = default_rounds(scenario, b);
    let rounds = a.rounds.map(u64::from).unwrap_or(need);
    if rounds < 1 {
        cli_bail("--rounds must be >= 1");
    }
    if scenario == Scenario::LinkUp && rounds < need {
        cli_bail(&format!(
            "--rounds must be >= {need} for --scenario link-up with these timers: the root's \
             hellos go out at P + i*H, and with fewer than {need} of them the bounded root would \
             fall silent before A-B opens at Fd = {}, so a FIRE could be an artifact of the \
             bounded run",
            b.forward_delay
        ));
    }
    if rounds > u64::from(MAX_ROUNDS) {
        cli_bail(&format!(
            "--rounds must be <= {MAX_ROUNDS} (got {rounds}{})",
            if a.rounds.is_none() { ", the default for these timers" } else { "" }
        ));
    }
    let rounds = rounds as u32;
    match a.mode.as_str() {
        "baseline" | "timed" => {
            let (mode, label) = if a.mode == "baseline" {
                (Mode::Baseline, "baseline")
            } else {
                (Mode::Timed, "timed")
            };
            let (s, d) = run(mode, scenario, b, rounds, a.keep_going);
            let c = read_counts();
            print_one(label, scenario, b, rounds, &s, d, c);
            warn_if_vacuous(label, scenario, s.execs, s.block, c);
            note_if_upper_bound(label, &s, c);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, scenario, b, rounds, a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, scenario, b, rounds, a.keep_going);
            let tc = read_counts();
            let (b_stats, t_stats) = (baseline.0.clone(), timed.0.clone());
            print_compare(scenario, b, rounds, baseline, timed, bc, tc);
            warn_if_vacuous("baseline", scenario, b_stats.execs, b_stats.block, bc);
            warn_if_vacuous("timed", scenario, t_stats.execs, t_stats.block, tc);
            note_if_upper_bound("baseline", &b_stats, bc);
            note_if_upper_bound("timed", &t_stats, tc);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
    note_if_too_short(scenario, b, rounds);
}

/// The number of root hellos a run needs so that its bound cannot cause a
/// FIRE (doc header, "Bound of the run"). link-up: every hello the real
/// root sends up to Fd, the instant of the check, i.e. those at P + i*H <=
/// Fd. lost-hellos: the smallest R >= 1 with R*H + 2L >= MA + Fd, so that
/// one full age-out plus one forward delay fits before the horizon.
fn default_rounds(scenario: Scenario, b: Bounds) -> u64 {
    match scenario {
        Scenario::LinkUp => {
            if b.forward_delay >= b.phase {
                (b.forward_delay - b.phase) / b.hello + 1
            } else {
                1
            }
        }
        Scenario::LostHellos => {
            let need = (b.max_age + b.forward_delay).saturating_sub(2 * b.l);
            need.div_ceil(b.hello).max(1)
        }
    }
}
