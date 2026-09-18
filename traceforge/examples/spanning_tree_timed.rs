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
//! Sweeping the timers shows the exact settings where a loop first becomes
//! possible.
//!
//! ## How the protocol maps onto TraceForge
//!
//! Threads: one per bridge (A, B, C) plus `main`, which only spawns them
//! and sends each an `Init` with the peers' ThreadIds. Bridges read `Init`
//! with the UNTIMED `recv_tagged_msg_block`, which the timed engine treats
//! as transparent, so every bridge clock starts at 0: the instant the
//! cable comes up (link-up) or the start of observation (lost-hellos).
//! Each point-to-point LAN is a direct channel between its two bridges;
//! only the channels that carry news in the story exist.
//!
//! Messages: `Hello { root, cost, sender, age }` carries the paper's Root
//! ID, path length, transmitting bridge ID and AGE (the link identifier
//! and MAX_AGE fields are constant here and dropped). `Msg::Done`,
//! `Msg::ForwardingNotice` and `Init` are harness messages, see below.
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
//!     next hello, started at the read of the previous one. Relays are
//!     immediate, so AGE is 0 at receipt and the wait is the paper's aging.
//!   * PRE_BACKUP_DELAY `Bd` (link-up only): C's finite-wait receive right
//!     after B's hello made it defer on B-C. The paper (p.49) gives two
//!     lower bounds, `Bd >= MAX_AGE + MaxPropTime` and, from its root
//!     failure argument, `Bd >= 2 * MaxPropTime`, and then simplifies to
//!     `2 * MAX_AGE`, which is the default here.
//!   * PRE_FORWARDING_DELAY `Fd`: link-up: A sleeps until `Fd` and opens
//!     A-B (B's end of A-B started its identical timer at the same instant,
//!     so A's expiry stands for both ends). lost-hellos: C's finite-wait
//!     receive after the age-out. Default `3 * MA` (paper).
//!   * MaxDelay / MaxLostMsgs: per-hop transit `[L, U]` plus storage
//!     lifetime `sd` over the two hops A to B to C, and `--losses K`.
//!
//! Loop detection without a clock read (link-up). The instant A-B opens
//! is signalled by `Msg::ForwardingNotice`, sent by A to C at time `Fd`
//! with `send_tagged_msg_timed(.., 0, 0)`: zero transit, so it arrives
//! exactly when A-B opens, and it stays readable until `Fd + sd`. C reads
//! it only in the two states in which its B-C end still forwards (old
//! FORWARDING before the news, PRE_BACKUP after it). A read at time `r`
//! therefore proves one instant (`r >= Fd`) at which A-B (both ends), B-C
//! (both ends) and C-A (both ends, never touched) all forward: a loop.
//! A never sends C anything else, so the notice has no ordering coupling.
//! It is verification scaffolding, not protocol traffic. In lost-hellos
//! the check is local: C's own B-C end opens while B (never told
//! otherwise) keeps forwarding onto B-C, and A-B and C-A are untouched.
//!
//! Losses: the lossy hop's sender decides, with `traceforge::nondet()`,
//! which of its hellos never arrive, at most `K` of them (A to B in
//! link-up, B to C in lost-hellos). The decision is taken at the sender
//! instead of with `with_lossy`, so that B's hellos to C carry a gap-free
//! delivery index in their tag (`relay_tag`) and every wait at C is
//! pinned to one specific message (see the harness section).
//!
//! ## Deviations from the paper (deliberate)
//!
//!   * Topology: a triangle of point-to-point LANs, the smallest cycle.
//!   * Start state: the model starts from a settled tree and injects one
//!     change (a cable added, or hellos lost), instead of running the
//!     election from power-on. Settled per-link states are hard-coded.
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
//!     news reaches C is still set by the relay. With `K = 0` the omission
//!     therefore changes no verdict, but with `K >= 1` the answer is one
//!     more frame that would have to be lost.
//!     In both cases the model can only report MORE loops than a real
//!     bridge; `--phase-ratio` lets you shorten the link-up news delay.
//!   * Priorities, link costs, the self-loop rule (p.48), one-way link
//!     detection (p.52), the hello hold-down (p.51), the forwarding
//!     database and non-participating bridges are omitted; none affects
//!     the race in a triangle.
//!   * Ties: when C's B-C end closes at exactly the instant A-B opens (or
//!     B's hello reaches C at exactly the instant C's B-C end opens), the
//!     timed receive may read at its deadline, so a zero-length overlap
//!     counts as a loop. Every `<=` below would be `<` under the opposite
//!     convention.
//!   * IEEE 802.1D (secondary source only; the standard was not read) has
//!     no PRE_BACKUP state and a two-stage forward delay. `--backup-delay-
//!     ratio 0` with `Fd = 2 * forward_delay` approximates it.
//!
//! ## Timeout-validation assumptions (verification harness, NOT the protocol)
//!
//! The timeout branch of a finite-wait receive is always explorable, in
//! either mode: it only says the awaited message had not been read by the
//! deadline IN THIS BRANCH. An assertion placed directly on such a branch
//! would fire at any parameters. The `assume_*` functions therefore follow
//! each property-relevant branch with a BLOCKING timed receive for the very
//! message that was awaited: it is feasible exactly when that message could
//! genuinely be read at or after the moment in question, so spurious
//! branches become blocked executions instead of false counterexamples. No
//! bridge performs these reads.
//!
//!   * `assume_hello_was_late` (lost-hellos): after C's PRE_FORWARDING
//!     timeout, read B's next hello (the tag pins its delivery index) at
//!     or after the deadline. BEFORE `LOOP_REOPENED` and the assert. With
//!     no later hello at the end of the run it blocks, which also removes
//!     the end-of-horizon artifact.
//!   * `assume_news_unprocessed` (link-up): the receiver-side order of
//!     messages from DIFFERENT senders is unconstrained, so C could read
//!     A's notice after letting B's hello expire unread in its mailbox,
//!     which amounts to an extra lost message. When C reads the notice
//!     before any news, it must still be able to read B's first hello at
//!     or after that moment. BEFORE `LOOP_BEFORE_NEWS` and the assert.
//!   * `assume_notice_was_late` (link-up): after C's PRE_BACKUP timeout,
//!     read A's notice at or after the deadline. It guards only the
//!     `SETTLED_BACKUP` evidence counter; the link-up loop assertions sit
//!     on message reads, never on a timeout branch.
//!
//! Known costs: holds report blocked > 0 by design where validation reads
//! fail. No timer is started after a validation read (each is followed
//! only by a counter and an assert or a return), so the read resolving
//! later than the deadline never shifts a verdict.
//!
//! ## Properties checked
//!
//! Every check is a local assertion at C, counted BEFORE it fires:
//!
//!   link-up:     when C reads A's notice (A-B now forwards at both ends),
//!                C's own B-C end must not forward:
//!                  LOOP_BEFORE_NEWS    (C had not yet heard B's hello)
//!                  LOOP_IN_PRE_BACKUP  (C was inside its switch-off delay)
//!   lost-hellos: C's B-C end must never reach FORWARDING while B, the
//!                rightful Designated Bridge, forwards onto B-C:
//!                  LOOP_REOPENED
//!
//! Evidence counters: link-up NEWS_HEARD (C entered PRE_BACKUP) and
//! SETTLED_BACKUP (C's switch-off delay ran out and A-B provably opened at
//! or after that); lost-hellos REFRESHED, AGED_OUT (includes spurious,
//! unvalidated age-outs) and REVERTED (a hello from B cancelled C's
//! PRE_FORWARDING). Counters accumulate over all explored executions, so
//! treat them as zero/nonzero evidence. HELLOS_LOST counts loss decisions.
//!
//! Soundness of the link-up check: the notice arrives at `Fd` exactly and
//! C reads it at `r >= Fd` while its B-C end forwards, so the instant `r`
//! has all six ends forwarding. Completeness: if B's first hello can be
//! read at `t` with `Fd <= t + Bd`, then either `Fd <= t` and C can read
//! the notice at `Fd` before the hello, or C reads the hello at `t < Fd`
//! and then the notice at `Fd`, inside its switch-off delay.
//!
//! ## CLI parameters (ratios are over U)
//!
//!   --scenario link-up|lost-hellos  story (default link-up)
//!   --mode baseline|timed|compare   verification mode (default timed:
//!                                   baseline always FIREs, so compare
//!                                   aborts in its baseline leg)
//!   --rounds R                      root hellos sent (default K + 1 for
//!                                   link-up, K + 2 for lost-hellos; fewer
//!                                   is refused)
//!   --losses K                      hellos that may be lost (default 0)
//!   --u U                           per-hop transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U) (default 0.0)
//!   --sd-ratio SR                   sd = round(SR * U), how long a message
//!                                   stays readable after arrival (default 0.0)
//!   --hello-ratio HR                H = round(HR * U), >= 1 (default 2.0)
//!   --phase-ratio PR                P = round(PR * U), 0 <= P <= H
//!                                   (link-up only, refused with
//!                                   lost-hellos; default P = H)
//!   --max-age-ratio MR              MA = round(MR * U), >= 1 (default 6.0
//!                                   for link-up, 3.0 for lost-hellos: at
//!                                   H = 2U a lost-hellos MA of 6U can never
//!                                   really expire, so PRE_FORWARDING would
//!                                   not be exercised)
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
//! includes a flag the chosen scenario does not use, and a U or scaled
//! timer above 10^12 ticks (a cap that keeps every derived time, such as
//! `3 * MA` or `P + R * H`, far from integer overflow).
//!
//! ## Expected verdicts (matrix-verified, 2026-09-16)
//!
//! Every cell below was run (timeout 120 s, none hit it). "a/b" means
//! "FIRE at a, hold at b" for the swept timer, other timers as stated.
//!
//!   baseline: FIRE (exit 101) at any parameters, in both scenarios: an
//!       untimed checker lets A-B open, or C's timers expire, before any
//!       news arrives. Run at the defaults, at --max-age-ratio 100, and at
//!       --losses 1 --max-age-ratio 50, for both scenarios. `--mode
//!       compare` therefore exits 101 in its baseline leg. With
//!       --keep-going: link-up defaults execs=3 violations=2 (timed: 1/0);
//!       lost-hellos MA=3 execs=5 violations=1 (timed: 3/0); lost-hellos
//!       K=1 MA=3 execs=26 violations=6 (timed: 18/0).
//!
//!   timed, link-up:  FIRE iff  Fd <= P + K*H + Bd + 2U + 2sd
//!       independent of L, R and MA. The right-hand side is the latest time
//!       C can read B's first delivered hello, plus C's switch-off delay.
//!       Verified (R = K + 1 unless stated):
//!         U=1 L=0 sd=0 H=2 P=2 Bd=4:  Fd 8/9 (6, 7 FIRE; 10 hold);
//!           also at R=2 (8/9) and at L=1 (8/9)
//!         U=1 P=0 Bd=4: 6/7;   U=1 sd=1 P=2 Bd=4: 10/11 (9 FIRE, 12 hold)
//!         U=1 P=2 Bd=0 (802.1D-like): 4/5 (Fd 0, 1, 3 FIRE; 6 hold)
//!         U=1 H=5 P=3 Bd=3: 8/9
//!         U=2 H=4 P=4 Bd=8: L=0 sd=0 16/17; L=1 sd=1 18/19; L=2 sd=0
//!           16/17;  U=2 H=4 P=1 Bd=8: 13/14
//!         K=1 U=1 H=2 P=2 Bd=4: 10/11 (R=2 and R=3)
//!         K=1 U=1 L=1 sd=1 H=3 P=3 Bd=2: 12/13
//!         K=2 R=3 U=1 sd=1 H=3 P=1 Bd=2: 13/14
//!         K=1 U=2 L=1 H=4 P=4 Bd=8: 20/21
//!       The cells above with an explicit Bd run at the default MA = 6U
//!       (unused by the link-up threads) and deliberately leave the
//!       paper's range for PRE_BACKUP_DELAY (Bd >= MA + MaxPropTime and
//!       Bd >= 2 * MaxPropTime, e.g. Bd=4 at MA=6, and the 802.1D-like
//!       Bd=0): they sweep Bd and Fd freely to locate the Fd - Bd threshold.
//!       Paper's simplified settings (Bd = 2MA, Fd = 3MA): FIRE iff
//!       MA <= P + K*H + 2U + 2sd; verified U=1 H=P=2: MA 2, 3, 4 FIRE,
//!       5 hold; sd=1: MA 6/7.
//!       Holds: execs 1..7, news_heard > 0, settled_backup > 0, both loop
//!       counters 0. --keep-going FIREs show the two paths: Bd=4 Fd=8
//!       loop_in_pre_backup=1; Bd=0 Fd=1 loop_before_news=1.
//!       Witnesses read: MA=3 defaults (A's hello at 2, B relays at 3, C
//!       reads it at 4 and enters PRE_BACKUP until 10, A-B opens at 9 and C
//!       reads the notice at 9) and Bd=4 Fd=8 (same, notice read at 8 =
//!       PRE_BACKUP deadline: the tie).
//!
//!   timed, lost-hellos:  FIRE iff  MA + Fd <= (K+1)*H + 2(U - L) + 2sd
//!       i.e. the largest gap between two reads of B's delivered hellos
//!       (the first read 2L after the root sent it, the next one K + 1
//!       periods later and 2U + 2sd after its root send). Verified as
//!       (MA, Fd) pairs, FIRE / hold:
//!         U=1 L=0 sd=0 H=2 K=0 (bound 4): (1,3)/(1,4) (2,2)/(2,3)
//!           (3,1)/(3,2) (4,0)/(4,1); also (1,2) (2,1) FIRE; R=3 (2,2)/(2,3)
//!         U=1 L=1 (bound 2): (1,1)/(1,2) (2,0)/(2,1)
//!         U=1 sd=1 (bound 6): (3,3)/(3,4) (5,1)/(5,2)
//!         U=1 H=5 (bound 7): (4,3)/(4,4)
//!         U=2 H=4: L=0 sd=0 (bound 8) (3,5)/(3,6); L=1 sd=1 (bound 8)
//!           (3,5)/(3,6); L=2 sd=1 (bound 6) (3,3)/(3,4)
//!         K=1 U=1 H=2 (bound 6): (3,3)/(3,4) (5,1)/(5,2) at R=3, and
//!           (3,3)/(3,4) at R=4
//!         K=2 R=4 U=1 L=1 sd=1 H=3 (bound 11): (5,6)/(5,7)
//!         K=1 U=2 L=1 H=4 (bound 10): (4,6)/(4,7)
//!       Holds: reverted > 0 in every cell above (2..68). Witnesses read:
//!       MA=2 Fd=2 (C reads hello 0 at 0, ages out at 2, opens at 4, hello
//!       1 sent by A at 2 and relayed at 3 arrives at 4) and K=1 MA=3 Fd=3
//!       (B drops the period-1 hello, C ages out at 3, opens at 6, the
//!       period-2 relay arrives at 6).
//!
//!   Harness checks: with `assume_news_unprocessed` disabled, link-up
//!       FIREs at the predicted holds Bd=0 Fd=5, Bd=0 Fd=9 and Bd=4 Fd=9
//!       (C lets B's hello expire unread); with `assume_hello_was_late`
//!       disabled, lost-hellos FIREs at (MA, Fd) = (2, 3) and (6, 18).
//!       Both guards restored, those cells hold (the (6, 18) hold prints
//!       the reverted=0 WARNING: at MA=6, H=2 no age-out can really happen).
//!
//! What this says about the paper (within the model and its deviations).
//! Take MaxPropTime = 2(U + sd) + K*H: two hops of at most U transit plus
//! sd reading lag, and K lost hellos.
//!
//!   * link-up: no loop iff Fd >= Bd + MaxPropTime + P + 1. Perlman's
//!     PRE_FORWARDING_DELAY >= PRE_BACKUP_DELAY + MaxPropTime is short by
//!     the phase P (up to one HELLO_TIME: after a cable is plugged in the
//!     news waits for the root's next hello while both PRE_FORWARDING timers
//!     already run), plus one tick for the zero-length tie. Only Fd - Bd
//!     matters, so this holds for any Bd inside the paper's bounds. With the
//!     simplified settings it becomes MA >= MaxPropTime + P + 1 instead of
//!     MA >= MaxPropTime. L never matters: the race is decided by the
//!     latest possible news. For H > 3(U + sd) the omitted triggered hellos
//!     could bring the news earlier, so the P term is an upper estimate
//!     there.
//!   * lost-hellos: the spacing counts K + 1 hello periods, as in 802.1D's
//!     (lost_msg + 1) * hello, not Perlman's HELLO_TIME * MaxLostMsgs. With
//!     no losses the paper's minimum MA = MaxPropTime and Fd = 3MA still
//!     reopen B-C falsely once H >= 6U + 2L + 6sd: verified U=1 sd=0 H=10
//!     MA 2, 3 FIRE, 4 hold; H=6 MA=2 FIRE (tie), H=5 MA=2 hold; U=2 L=1
//!     H=16 MA 4 FIRE, 5 hold. For K >= 1 the omitted triggered reply makes
//!     these FIREs an over-approximation.
//!
//! Timed FIREs are certified: the checker validates every reported
//! counterexample against one consistent timeline and prints it as
//! "Certified witness timeline". A link-up hold with news_heard = 0 or
//! settled_backup = 0, a lost-hellos hold with reverted = 0, and any run
//! with execs = 0 print a WARNING and are no data.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats, WaitTime};

// Outcome counters: event counts accumulated across all explored
// executions of one verify run (revisited prefixes may re-count, so
// treat them as zero/nonzero evidence, not exact per-exec tallies).
// Reset before each verify, read after.
static HELLOS_LOST: AtomicUsize = AtomicUsize::new(0);
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
const DEFAULT_LOSSES: u32 = 0;
// Cap on U and on every scaled timer. With R <= 10_000 the largest derived
// time (3 * MA, P + R * H, ...) stays near 10^16, far below u64::MAX.
const MAX_TIME: u64 = 1_000_000_000_000;
const MAX_ROUNDS: u32 = 10_000;

// Bridge IDs (the smallest is the Root).
const ID_A: u32 = 1;
const ID_B: u32 = 2;
const ID_C: u32 = 3;

// Tags: receive predicates see only (sender, tag). B's hellos to C carry
// their delivery index so that every wait at C is pinned to one message.
const TAG_ROOT_HELLO: u32 = 1;
const TAG_NOTICE: u32 = 2;
const TAG_DONE: u32 = 3;
const TAG_RELAY_BASE: u32 = 100;

fn relay_tag(index: u32) -> u32 {
    TAG_RELAY_BASE + index
}

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
    /// Verification harness, not Perlman (link-up only): A to C with
    /// zero transit at the instant A-B opens (see the doc header).
    ForwardingNotice,
    /// Verification harness, not Perlman: lets reactive threads stop.
    Done,
}

/// Bootstrap message, a separate type from Msg so the init read matches
/// only Init.
#[derive(Clone, Debug, PartialEq)]
struct Init {
    a: ThreadId,
    b: ThreadId,
    c: ThreadId,
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

fn read_init(main_tid: ThreadId) -> Init {
    // UNTIMED read: transparent for timing, so the clock stays at 0.
    traceforge::recv_tagged_msg_block::<_, Init>(move |s, _tag| s == main_tid)
}

// =====================================================================
// Timeout-validation assumptions (verification harness, NOT Perlman;
// see the doc header). No bridge performs these reads.
// =====================================================================

/// Assume B's hello with delivery index `index` was genuinely late: block
/// until it is readable at or after the current (post-deadline) time.
/// Timed infeasible, so the execution blocks and is discarded, when that
/// hello provably could not be read that late, or does not exist.
fn assume_hello_was_late(b_tid: ThreadId, index: u32) {
    let tag = relay_tag(index);
    match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, t| {
        s == b_tid && t == Some(tag)
    }) {
        Msg::Hello(_) => {}
        // Unreachable: only B's hellos carry relay tags.
        m => panic!("assume_hello_was_late: unexpected {m:?}"),
    }
}

/// Assume B's first hello was still unprocessed when C read A's notice:
/// it must be readable at or after that read. Removes timelines in which
/// the hello expired unread in C's mailbox (an extra, unbudgeted loss).
fn assume_news_unprocessed(b_tid: ThreadId) {
    assume_hello_was_late(b_tid, 0);
}

/// Assume A's notice was genuinely late for C's PRE_BACKUP wait: it must
/// be readable at or after the deadline, i.e. A-B opened no earlier than
/// C's B-C end closed. Guards the SETTLED_BACKUP evidence counter only.
fn assume_notice_was_late(a_tid: ThreadId) {
    match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, t| {
        s == a_tid && t == Some(TAG_NOTICE)
    }) {
        Msg::ForwardingNotice => {}
        m => panic!("assume_notice_was_late: unexpected {m:?}"),
    }
}

// =====================================================================
// Story 1: link-up. Cable A-B comes up at time 0.
// =====================================================================

/// Bridge A, the Root. Its A-B end is PRE_FORWARDING from time 0 and
/// opens at Fd; its periodic hellos go out on A-B at P, P + H, ...
fn root_link_up(b: Bounds, rounds: u32, losses: u32, main_tid: ThreadId) {
    let Init { b: b_tid, c: c_tid, .. } = read_init(main_tid);
    let mut now: u64 = 0;
    let mut losses_left = losses;
    let mut ab = LinkState::PreForwarding;

    let open_ab = |now: &mut u64, ab: &mut LinkState| {
        sleep_until(now, b.forward_delay);
        // PRE_FORWARDING_DELAY expired: A-B forwards (at both ends, B's
        // identical timer started at the same instant).
        *ab = LinkState::Forwarding;
        // Harness: zero-transit notice of that instant (doc header).
        traceforge::send_tagged_msg_timed(c_tid, TAG_NOTICE, Msg::ForwardingNotice, 0, 0);
    };

    for i in 0..rounds {
        let t = b.phase + u64::from(i) * b.hello;
        if ab != LinkState::Forwarding && b.forward_delay < t {
            open_ab(&mut now, &mut ab);
        }
        sleep_until(&mut now, t);
        // Loss on the A to B hop, decided here (doc header).
        if losses_left > 0 && traceforge::nondet() {
            losses_left -= 1;
            HELLOS_LOST.fetch_add(1, Ordering::Relaxed);
        } else {
            traceforge::send_tagged_msg(
                b_tid,
                TAG_ROOT_HELLO,
                Msg::Hello(Hello { root: ID_A, cost: 0, sender: ID_A, age: 0 }),
            );
        }
    }
    // Harness: release B.
    traceforge::send_tagged_msg(b_tid, TAG_DONE, Msg::Done);
    if ab != LinkState::Forwarding {
        open_ab(&mut now, &mut ab);
    }
}

/// Bridge B. Settled before the cable: root link B-C at distance 2, with
/// C Designated on B-C. A's hello on A-B makes A-B its root link
/// (distance 1) and B Designated on B-C, so B relays at once.
fn bridge_b_link_up(main_tid: ThreadId) {
    let Init { a: a_tid, c: c_tid, .. } = read_init(main_tid);
    // What B last heard on B-C from C before the change.
    let heard_on_bc = Hello { root: ID_A, cost: 1, sender: ID_C, age: 0 };
    let mut dist: u32 = 2;
    let mut relays: u32 = 0;
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _t| s == a_tid) {
            Msg::Hello(h) => {
                // Distance = 1 + best distance heard from a Designated
                // Bridge (p.46); A-B's end stays PRE_FORWARDING until Fd.
                dist = dist.min(h.cost + 1);
                if is_designated(h.root, dist, ID_B, Some(&heard_on_bc)) {
                    traceforge::send_tagged_msg(
                        c_tid,
                        relay_tag(relays),
                        Msg::Hello(Hello { root: h.root, cost: dist, sender: ID_B, age: h.age }),
                    );
                    relays += 1;
                } else {
                    // Unreachable in the triangle: distance 1 ties C and
                    // wins on the lower ID.
                    panic!("bridge B: expected to become Designated on B-C");
                }
            }
            Msg::Done => return,
            m => panic!("bridge B: unexpected {m:?}"),
        }
    }
}

/// Bridge C. Root link C-A at distance 1, Designated on B-C (so its B-C
/// end forwards) until B's hello arrives.
fn bridge_c_link_up(b: Bounds, main_tid: ThreadId) {
    let Init { a: a_tid, b: b_tid, .. } = read_init(main_tid);
    let my_root = ID_A;
    let my_dist: u32 = 1;
    let mut bc = LinkState::Forwarding;

    // Old state: wait for the news, or observe A-B opening first.
    let first = traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, t| {
        (s == b_tid && t == Some(relay_tag(0))) || (s == a_tid && t == Some(TAG_NOTICE))
    });
    match first {
        Msg::ForwardingNotice => {
            // Harness assumption BEFORE the counter (doc header).
            assume_news_unprocessed(b_tid);
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
        m => panic!("bridge C: unexpected {m:?}"),
    }

    // PRE_BACKUP: keep forwarding for PRE_BACKUP_DELAY.
    match traceforge::recv_tagged_msg_timed::<_, Msg>(
        move |s, t| s == a_tid && t == Some(TAG_NOTICE),
        WaitTime::Finite(b.backup_delay),
    ) {
        Some(Msg::ForwardingNotice) => {
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
            // Harness assumption before the evidence counter.
            assume_notice_was_late(a_tid);
            SETTLED_BACKUP.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// =====================================================================
// Story 2: lost-hellos. Settled tree with all three cables; C's B-C end
// is BACKUP because B is Designated on B-C.
// =====================================================================

/// Bridge A, the Root: a hello to B every HELLO_TIME, from time 0.
fn root_steady(b: Bounds, rounds: u32, main_tid: ThreadId) {
    let Init { b: b_tid, .. } = read_init(main_tid);
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
    traceforge::send_tagged_msg(b_tid, TAG_DONE, Msg::Done);
}

/// Bridge B, Designated on B-C: relays every root hello at once; the B to
/// C hop may lose up to `losses` of them.
fn bridge_b_steady(losses: u32, main_tid: ThreadId) {
    let Init { a: a_tid, c: c_tid, .. } = read_init(main_tid);
    let mut losses_left = losses;
    let mut delivered: u32 = 0;
    loop {
        match traceforge::recv_tagged_msg_block_timed::<_, Msg>(move |s, _t| s == a_tid) {
            Msg::Hello(h) => {
                if losses_left > 0 && traceforge::nondet() {
                    losses_left -= 1;
                    HELLOS_LOST.fetch_add(1, Ordering::Relaxed);
                } else {
                    traceforge::send_tagged_msg(
                        c_tid,
                        relay_tag(delivered),
                        Msg::Hello(Hello { root: h.root, cost: h.cost + 1, sender: ID_B, age: h.age }),
                    );
                    delivered += 1;
                }
            }
            Msg::Done => {
                // Harness: release C (never lost).
                traceforge::send_tagged_msg(c_tid, TAG_DONE, Msg::Done);
                return;
            }
            m => panic!("bridge B: unexpected {m:?}"),
        }
    }
}

/// Bridge C: ages B's information on B-C, reopens B-C after MAX_AGE plus
/// PRE_FORWARDING_DELAY of silence.
fn bridge_c_steady(b: Bounds, main_tid: ThreadId) {
    let Init { b: b_tid, .. } = read_init(main_tid);
    let my_root = ID_A;
    let my_dist: u32 = 1;
    let pred = move |next: u32| {
        let tag = relay_tag(next);
        move |s: ThreadId, t: Option<u32>| s == b_tid && (t == Some(tag) || t == Some(TAG_DONE))
    };

    // Initial stored information on B-C: B's first delivered hello.
    let mut bc = LinkState::Backup;
    match traceforge::recv_tagged_msg_block_timed::<_, Msg>(pred(0)) {
        Msg::Hello(h) => {
            bc = port_rule(bc, is_designated(my_root, my_dist, ID_C, Some(&h)));
            if bc != LinkState::Backup {
                panic!("bridge C: expected BACKUP on B-C, got {bc:?}");
            }
        }
        Msg::Done => return,
        m => panic!("bridge C: unexpected {m:?}"),
    }
    let mut next: u32 = 1;

    loop {
        // Aging: the stored hello expires after MAX_AGE without a newer one.
        match traceforge::recv_tagged_msg_timed::<_, Msg>(pred(next), WaitTime::Finite(b.max_age)) {
            Some(Msg::Hello(_)) => {
                REFRESHED.fetch_add(1, Ordering::Relaxed);
                next += 1;
                continue;
            }
            Some(Msg::Done) => return,
            Some(m) => panic!("bridge C: unexpected {m:?}"),
            None => {}
        }
        AGED_OUT.fetch_add(1, Ordering::Relaxed);
        // Information discarded: C now claims Designated on B-C.
        bc = port_rule(bc, is_designated(my_root, my_dist, ID_C, None));
        if bc != LinkState::PreForwarding {
            panic!("bridge C: expected PRE_FORWARDING after the age-out, got {bc:?}");
        }

        // PRE_FORWARDING: newer information reverts to BACKUP at once
        // (p.49-50).
        match traceforge::recv_tagged_msg_timed::<_, Msg>(
            pred(next),
            WaitTime::Finite(b.forward_delay),
        ) {
            Some(Msg::Hello(h)) => {
                bc = port_rule(bc, is_designated(my_root, my_dist, ID_C, Some(&h)));
                if bc != LinkState::Backup {
                    panic!("bridge C: expected BACKUP after B's hello, got {bc:?}");
                }
                REVERTED.fetch_add(1, Ordering::Relaxed);
                next += 1;
                continue;
            }
            Some(Msg::Done) => return,
            Some(m) => panic!("bridge C: unexpected {m:?}"),
            None => {}
        }
        // Harness assumption BEFORE the counter (doc header).
        assume_hello_was_late(b_tid, next);
        // PRE_FORWARDING_DELAY expired with no newer information.
        bc = timer_expired(bc);
        LOOP_REOPENED.fetch_add(1, Ordering::Relaxed);
        // B is Designated on B-C and never stopped forwarding there, so
        // C's end must not forward. bc is FORWARDING on every path that
        // gets here (the p.47 timer rule applied to PRE_FORWARDING), so
        // reaching this line with a validated timeout IS the violation.
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
    let mut builder = apply_parallel(Config::builder().with_progress_report(usize::MAX));
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
        &HELLOS_LOST,
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
    losses: u32,
    keep_going: bool,
) -> (Stats, Duration) {
    let cfg = build_config(mode, b, keep_going);
    reset_counts();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let (ha, hb, hc) = match scenario {
            Scenario::LinkUp => (
                thread::spawn(move || root_link_up(b, rounds, losses, main_tid)),
                thread::spawn(move || bridge_b_link_up(main_tid)),
                thread::spawn(move || bridge_c_link_up(b, main_tid)),
            ),
            Scenario::LostHellos => (
                thread::spawn(move || root_steady(b, rounds, main_tid)),
                thread::spawn(move || bridge_b_steady(losses, main_tid)),
                thread::spawn(move || bridge_c_steady(b, main_tid)),
            ),
        };
        let init = Init { a: ha.thread().id(), b: hb.thread().id(), c: hc.thread().id() };
        // Send every Init before joining anyone.
        traceforge::send_msg(init.a, init.clone());
        traceforge::send_msg(init.b, init.clone());
        traceforge::send_msg(init.c, init.clone());
        let _ = ha.join();
        let _ = hb.join();
        let _ = hc.join();
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    lost: usize,
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
        lost: HELLOS_LOST.load(Ordering::Relaxed),
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
    // A violating execution is aborted by the assertion, so the checker
    // files it under `blocked`, not `execs`. Warning on execs == 0 alone
    // would tell the reader to discard a run that found counterexamples.
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
                     news and provably closed B-C before A-B opened; this hold is vacuous, treat \
                     it as no data.",
                    c.news, c.settled
                );
            }
        }
        Scenario::LostHellos => {
            if c.reverted == 0 {
                println!(
                    "WARNING ({label}): reverted=0: no execution had C enter PRE_FORWARDING and \
                     then get corrected by B's hello. Either MAX_AGE covers every gap between B's \
                     hellos (no age-out can really happen, so the PRE_FORWARDING race was never \
                     exercised) or every such path was discarded; treat this hold as no data for \
                     the forward-delay race."
                );
            }
        }
    }
}

// =====================================================================
// Reporting
// =====================================================================

fn params_line(scenario: Scenario, b: Bounds, rounds: u32, losses: u32) -> String {
    match scenario {
        Scenario::LinkUp => format!(
            "scenario=link-up R={rounds} K={losses}  L={} U={} sd={} H={} P={} MA={} Bd={} Fd={}",
            b.l, b.u, b.sd, b.hello, b.phase, b.max_age, b.backup_delay, b.forward_delay
        ),
        Scenario::LostHellos => format!(
            "scenario=lost-hellos R={rounds} K={losses}  L={} U={} sd={} H={} MA={} Fd={}",
            b.l, b.u, b.sd, b.hello, b.max_age, b.forward_delay
        ),
    }
}

fn counts_line(scenario: Scenario, c: Counts) -> String {
    match scenario {
        Scenario::LinkUp => format!(
            "lost={} news_heard={} settled_backup={} loop_before_news={} loop_in_pre_backup={} \
             violations={}",
            c.lost, c.news, c.settled, c.loop_before, c.loop_pre_backup, c.violations()
        ),
        Scenario::LostHellos => format!(
            "lost={} refreshed={} aged_out={} reverted={} loop_reopened={} violations={}",
            c.lost, c.refreshed, c.aged_out, c.reverted, c.reopened, c.violations()
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn print_one(
    label: &str,
    scenario: Scenario,
    b: Bounds,
    rounds: u32,
    losses: u32,
    stats: &Stats,
    dur: Duration,
    c: Counts,
) {
    println!(
        "{label:<9} {}  execs={:<5} blocked={:<5} {} time={dur:?}",
        params_line(scenario, b, rounds, losses),
        stats.execs,
        stats.block,
        counts_line(scenario, c),
    );
}

#[allow(clippy::too_many_arguments)]
fn print_compare(
    scenario: Scenario,
    b: Bounds,
    rounds: u32,
    losses: u32,
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
    println!("{}", params_line(scenario, b, rounds, losses));
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
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
    losses: u32,
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
    // Default mode is `timed`: baseline always FIREs for this benchmark,
    // so compare's first leg aborts the process (see the doc header).
    let mut a = Args {
        scenario: String::from("link-up"),
        mode: String::from("timed"),
        rounds: None,
        losses: DEFAULT_LOSSES,
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
            "--losses" => a.losses = parse_num(next_val(&mut args, "--losses"), "--losses"),
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
                     [--mode baseline|timed|compare] [--rounds R] [--losses K] [--u U] \
                     [--l-ratio LR] [--sd-ratio SR] [--hello-ratio HR] [--phase-ratio PR] \
                     [--max-age-ratio MR] [--backup-delay-ratio BR] [--forward-delay-ratio FR] \
                     [--keep-going] [--parallel none|shared|partitioned]\n\
                     Ratios are over U. Defaults: link-up, timed, K=0, R=K+1 (link-up) or K+2 \
                     (lost-hellos), U=1, L/U=0, sd/U=0, H/U=2, P=H, MA/U=6 (link-up) or 3 \
                     (lost-hellos), Bd=2*MA, Fd=3*MA.\n\
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
    // Enough root hellos that K losses cannot fake a loop at the horizon:
    // link-up needs one delivered hello, lost-hellos a delivered hello on
    // each side of K consecutive losses.
    let min_rounds = match scenario {
        Scenario::LinkUp => a.losses.saturating_add(1),
        Scenario::LostHellos => a.losses.saturating_add(2),
    };
    let rounds = a.rounds.unwrap_or(min_rounds);
    if rounds < min_rounds {
        cli_bail(&format!(
            "--rounds must be >= {min_rounds} for --scenario {} with --losses {}",
            scenario.name(),
            a.losses
        ));
    }
    if rounds > MAX_ROUNDS {
        cli_bail(&format!("--rounds must be <= {MAX_ROUNDS} (and --losses below it)"));
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
    match a.mode.as_str() {
        "baseline" | "timed" => {
            let (mode, label) = if a.mode == "baseline" {
                (Mode::Baseline, "baseline")
            } else {
                (Mode::Timed, "timed")
            };
            let (s, d) = run(mode, scenario, b, rounds, a.losses, a.keep_going);
            let c = read_counts();
            print_one(label, scenario, b, rounds, a.losses, &s, d, c);
            warn_if_vacuous(label, scenario, s.execs, s.block, c);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, scenario, b, rounds, a.losses, a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, scenario, b, rounds, a.losses, a.keep_going);
            let tc = read_counts();
            let (b_vac, t_vac) =
                ((baseline.0.execs, baseline.0.block), (timed.0.execs, timed.0.block));
            print_compare(scenario, b, rounds, a.losses, baseline, timed, bc, tc);
            warn_if_vacuous("baseline", scenario, b_vac.0, b_vac.1, bc);
            warn_if_vacuous("timed", scenario, t_vac.0, t_vac.1, tc);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
}
