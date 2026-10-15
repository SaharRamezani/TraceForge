//! Redundant controllers with a network reference point: can both be in charge?
//!
//! ## The idea in plain words
//!
//! Factories, power plants and ships are run by small industrial computers
//! called controllers. To survive a hardware fault a controller comes as a
//! pair. One copy, the primary, is in charge and drives the machines. The
//! other copy, the backup, watches quietly and takes over only if the
//! primary has died. The worst mistake such a pair can make is to have
//! BOTH copies in charge at the same moment: they would give the machines
//! conflicting orders. This example checks whether that can happen.
//!
//! Who takes part. Two controllers, DCN1 (in charge at the start) and DCN2
//! (the backup); DCN is the papers' short name for such a controller
//! ("distributed controller node"). They are joined by two separate
//! networks, A and B, so that one broken cable does not cut them apart.
//! Each network is a chain of three switches (boxes that pass messages
//! along): A1, A2, A3 and B1, B2, B3. DCN1 is plugged into A1 and B1, DCN2
//! into A3 and B3.
//!
//! The landmark. The primary picks one switch, A1 at the start, as a
//! landmark (the paper calls it the "network reference point"). The
//! reasoning: if the network is split in two, only one half still contains
//! the landmark, and only a controller that can still reach the landmark
//! may be in charge.
//!
//! What they tell each other.
//!   * "Are you there?": every round the primary asks the landmark, and a
//!     working landmark answers "yes, I am here".
//!   * "I am alive": if the landmark answered, the primary sends one such
//!     note to the backup on each network.
//!   * "New landmark": if the landmark did not answer, the primary picks its
//!     switch on the other network (B1) as the new landmark and tells the
//!     backup.
//!
//! The timers.
//!   * The round length (the heartbeat period): both controllers wake up at
//!     the start of every round.
//!   * The answer wait (the ping timeout): after asking the landmark, a
//!     controller waits this long for the answer. The primary sends its "I
//!     am alive" notes only at the end of this wait, and only if the answer
//!     came in time.
//!   * The patience limit (2 rounds): the backup keeps one "rounds without
//!     news" count per network. Every round it adds one to both counts, and
//!     an "I am alive" note puts that network's count back to zero. The
//!     backup starts to worry only when a count goes above the limit, that
//!     is, on the third silent round.
//!
//! What each controller does.
//!   * The primary asks the landmark every round. No answer: it moves to the
//!     landmark on the other network and, in the code as first written,
//!     sends no notes that round. No answer from the second landmark either:
//!     it steps down and waits for a human.
//!   * The backup, once both networks have gone silent, either takes over at
//!     once when both counts went over the limit in the same round (a
//!     shortcut: "two networks failing together is unlikely, so the primary
//!     itself must be dead"), or asks the landmark itself and takes over
//!     only if the landmark answers.
//!   * The leasing fix treats being in charge like renting a flat: the
//!     right is only on loan (a "lease"), the primary keeps renewing it
//!     simply by asking the landmark every round, and the backup may move
//!     in only after the renewals have stopped for a while. Concretely it
//!     drops the shortcut and gives the landmark a short memory: with every
//!     answer the landmark also says whether the question it handled just
//!     before came from the primary. The backup takes over only after the
//!     landmark has answered three of the backup's questions in a row with
//!     no question from the primary in between, a sign that the primary no
//!     longer reaches the landmark.
//!
//! What can go wrong, as the papers found it.
//!   * DCN1's two switches break at nearly the same moment. DCN1 is cut off
//!     but still running, and it needs two more rounds to notice that
//!     neither landmark answers and to step down. The backup's shortcut
//!     fires a little earlier, so for a short while both are in charge.
//!   * Something wipes out only the "I am alive" notes (noise, an attacker)
//!     while the landmark stays reachable. Without the lease the backup asks
//!     the landmark, gets an answer, and takes over from a perfectly healthy
//!     primary.
//!
//! What this example checks. It plays out no chosen failure: no switch is
//! broken at a set moment and no attacker is switched on. The checker's own
//! rules are that every message may be lost on its way, at any moment and
//! without limit, that a message which has arrived can be read only for a
//! short storage time (0 here), and that a waiting receive reads the
//! message that arrived first, never passing it over for a later one
//! (messages that arrive at the same instant are read in either order). A
//! controller's own timer is a message too, so a clock can stop. A broken
//! switch, a cut cable or an attacker is one particular pattern of lost
//! messages, so the papers' scenarios are among the runs the checker tries,
//! together with every other pattern. The question is: is there ANY pattern
//! of lost messages after which both controllers are in charge at the same
//! moment?
//!
//! What the checker finds, in short. Yes, in every variant, as soon as the
//! run is long enough for the backup to decide. With the shortcut (the code
//! as first written) or without it, three rounds are enough: the backup
//! hears no "I am alive" note for two rounds, because the notes are lost on
//! the way or were never sent (the primary missed an answer of the landmark
//! and, in the code as first written, sends no note in that round); it takes
//! over, and the primary, which has not stepped down, is still in charge.
//! With the lease the backup needs three answers of the landmark in a row
//! with no question of the primary in between; with five rounds that
//! happens too. The findings this file reported until 2026-10-13 (which
//! spacings of two switch failures are safe, the lease's zero-margin tie)
//! came from failures played out by script on links that lost nothing else.
//! They are withdrawn (see Results).
//!
//! Sources. B. Johansson, M. Ragberger, A. V. Papadopoulos, T. Nolte,
//! "Consistency Before Availability: Network Reference Point based Failure
//! Detection for Controller Redundancy", IEEE ETFA 2023 (the algorithm).
//! B. Johansson, B. Pourvatan, Z. Moezkarimi, A. Papadopoulos, M. Sirjani,
//! "Formal Verification of Consistency for Systems with Redundant
//! Controllers", MARS 2024, EPTCS 399, pp. 169-191, DOI 10.4204/EPTCS.399.8
//! (the Timed Rebeca models NRPFD.rebeca and LeasingNRPFD.rebeca, the
//! two-in-charge finding of Table 1 and the leasing fix). M. Sirjani,
//! E. A. Lee, Z. Moezkarimi, B. Pourvatan, B. Johansson, S. Marksteiner,
//! A. V. Papadopoulos, "Actors for Timing Analysis of Distributed Redundant
//! Controllers", Gul Agha Festschrift, LNCS 16120, Springer 2025 (how far
//! apart two switch failures must be).
//!
//! ## How the protocol maps onto TraceForge
//!
//! Three threads, none a daemon: reactive actors, each a loop over ONE
//! blocking timed receive that dispatches on the message, like a Rebeca
//! message server, and each leaving its loop on a bound of its own (see
//! Termination). DCN1 and DCN2 run the node code of
//! NRPFD.rebeca / LeasingNRPFD.rebeca. SW is the DCN1-side switch pair
//! A1 + B1: the initial NRP (A1), the second NRP candidate (B1), forwarding
//! and, for the leasing variant, the per-switch lease memory (`primary`,
//! `which`, `prevWhich`). The relay switches A2, A3, B2, B3 hold no state,
//! so they fold into transit: a DCN1<->SW leg is one hop [L, U] and a
//! SW<->DCN2 leg is three hops [3L, 3U] (A1 -> A2 -> A3 -> DCN2). A
//! heartbeat therefore travels 4 hops, the Rebeca chain exactly at
//! L = U = 1 (Rebeca's networkDelay = 1).
//!
//! Wiring without messages. `main` spawns SW and DCN1 and returns: it joins
//! nobody, sends nothing and reads nothing. DCN1's thread first spawns
//! DCN2, because each of the two needs the other's id, and then runs the
//! DCN1 code. SW knows no thread id: a Ping carries the id to answer, a
//! Heartbeat and a NewNrp carry the id of their destination. Every
//! thread's clock starts at 0, so all three start in the same phase, as in
//! the Rebeca models.
//!
//! Timers are self-messages, exactly like Rebeca's `self.runMe()
//! after(heartbeat_period)` and `ping_timed_out() after(ping_timeout)`:
//! `send_msg_timed(me, Tick, H, H)` and `send_msg_timed(me, PingTimeout, P,
//! P)`. Self-sends are made in firing order (the ping timeout before the
//! next tick) because arrivals on one channel keep send order. With sd = 0
//! a message is handled at its arrival instant (unless it was lost on the
//! way), and same-instant arrivals from different senders are explored in
//! both orders (Rebeca's arbitrary same-time handler order, the "+1" of the
//! GulFest model comment).
//!
//! Loss is the only fault, and nothing is scripted on top of it. Every send
//! may be dropped, with no budget (`with_timed`; `with_all_sends_lossy` in
//! baseline mode). A receive reads the message that arrived first: it never
//! passes over an arrived message to read a later one, and two messages that
//! arrive at the same instant are read in either order. (Until 2026-10-13 a
//! waiting receive could read a later arrival of another sender and let an
//! earlier one expire unread; that is not a behaviour of the semantics any
//! more.) Handlers take no time and an actor does nothing but wait and
//! handle, so a message that is not dropped is read at its arrival instant
//! (with sd > 0, within sd of it) unless its receiver has left its loop (see
//! Termination): the only way a message that a thread waits for is never
//! read is that its send is dropped. This holds for pings, answers,
//! heartbeats and new_NRP, and for the two timers as well, which
//! are lossy self-sends: a DCN whose Tick is dropped stops for good and
//! keeps its role (each tick arms the next), one whose PingTimeout is
//! dropped skips that round's decision. Those stopped clocks are among the
//! explored executions.
//!
//! The run is bounded by a count, not by silence: each DCN handles R
//! ticks (rounds 0..R-1) and arms nothing after the last one, and leaves its
//! loop once it has read everything it armed or sent (see Termination). The
//! bound cannot cause a FIRE. DCN2 decides for the last time at its last
//! ping timer, at most at t2 + (R - 1)H + P (t2 its first tick). A longer
//! run could show DCN1 in another role at t1 + R*H + P at the earliest:
//! only a ping timer makes DCN1 step down, and that is the ping timer of
//! the first round this run does not have. Since t2 - t1 = phase < H, the
//! first instant is before the second, and whatever DCN1 would send in the
//! missing rounds may be lost in the longer run too: every violating
//! execution here is a prefix of a violating execution of every longer
//! run. With sd > 0 a timer may be read up to sd late and the condition
//! becomes phase + (R + 1)sd < H, which the CLI enforces. A hold, on the
//! other hand, speaks for R rounds and nothing beyond.
//!
//! ## Deviations from the paper (deliberate)
//!
//!   * Six switch actors become one SW thread (A1 + B1) plus transit, and no
//!     switch fails by script (the papers' `switchFail() after(t)`): a
//!     message lost on a leg stands for a loss at any hop of that leg.
//!     Because A1 and B1 are one thread, everything one DCN sends towards
//!     either switch shares one channel, and so does everything SW sends
//!     to one DCN, and a channel delivers in send order. At L = U (the
//!     default, Rebeca's fixed delay) send order is arrival order anyway;
//!     at L < U this removes the timelines in which a message on one
//!     network overtakes an earlier message on the other.
//!   * Heartbeats travel as in the source, one message per network:
//!     `Heartbeat { net }` for network A and then for network B, DCN1 -> SW,
//!     each forwarded SW -> DCN2, and DCN2 resets the count of that network
//!     only. Either copy can be lost on either leg by itself, so silence on
//!     one network alone is reachable. (Until 2026-10-13 the two travelled
//!     as ONE message with a per-network mask, which made sense only while
//!     SW dropped per network by script; kept, it would have turned every
//!     lost heartbeat into a loss on both networks.) By the channel rule
//!     above, network A's copy is never read after network B's copy of the
//!     same instant; their two handlers commute.
//!   * Per-hop fixed delays become intervals, and Rebeca's fixed ping send
//!     offsets (`after(5)` for the primary, `after(15)` for the leasing
//!     backup) and the zero-delay `new_NRP` are replaced by ordinary hops.
//!     The primary's ping still reaches the NRP before the backup's ping of
//!     the same tick whenever U < 3L (1 hop against 3 hops).
//!   * Startup (WAITING -> PRIMARY/BACKUP, the first new_NRP) is skipped:
//!     DCN1 starts PRIMARY with NRP A1 agreed, DCN2 BACKUP, both ticking at
//!     t = 0 unless `--phase-ratio` offsets one of them. Rebeca's first
//!     PRIMARY round starts at H, so a Rebeca instant is one H earlier here
//!     (the dual window [4000, 4500) of MARS Figure 8, with H = 1000 and
//!     P = 500, is [3H, 3H + P) here). As in the leasing file's `init`
//!     guard, a stepped-down (WAITING) DCN1 never returns; in NRPFD.rebeca
//!     it would re-promote at its next runMe.
//!   * `--promote-rule` selects which node the backup runs.
//!     `takeover` (default, ETFA Algorithm 3 and the GulFest
//!     `become_primary_on_ping_response` flag) promotes on an answered ping
//!     only if the ping was sent because BOTH counters were over the limit.
//!     `any` is the MARS node verbatim: NRPFD.rebeca's `ping_timed_out`
//!     BACKUP branch promotes whenever the ping was answered
//!     (`if (ping_pending) ping_pending = false; else { mode = PRIMARY; }`)
//!     and LeasingNRPFD.rebeca does the same behind the lease check
//!     (`if (which > 1) { mode = PRIMARY; }`), in both cases including the
//!     one-network ping. `any` is therefore strictly more eager to promote.
//!     No cell of this header separates the two rules: the 2026-09-18
//!     comparison of the two needed a scripted link cut and is withdrawn
//!     (see Results).
//!     NRPFD.rebeca's `NRP_network = -1` artifact is dropped under both.
//!   * The node is the MARS 2024 node (NRPFD.rebeca / LeasingNRPFD.rebeca),
//!     NOT the node of the GulFest timing-analysis file, which differs in
//!     ways that matter for failure spacing: its `new_NRP` handler also
//!     resets the missed count of the new NRP's network (GulFest file lines
//!     248-249); its shortcut fires whenever both counts are over the limit
//!     and equal (no `== max + 1`), with counts clamped to max + 1 only in
//!     the one-network branch; its backup promotes on the ping answer
//!     itself, goes FAILED when a takeover ping is not answered, and asks
//!     the primary for a new NRP (`request_new_NRP`, `nrp_timeout`) when a
//!     one-network ping is not answered; a primary with no candidate left
//!     goes FAILED. None of these differences is measured here: the
//!     comparison reported until 2026-10-13 (the reset alone turning five
//!     scripted two-failure cells from FIRE into holds) needed the scripted
//!     failures and is withdrawn with them.
//!   * A promoted DCN2 is passive: it sends the probe and (leasing)
//!     `new_NRPBack` so the NRP records the new primary, but it sends no
//!     pings or heartbeats afterwards. DCN1 never re-promotes and a primary
//!     ignores heartbeats and lease bits, so this cannot change whether
//!     both are primary at some instant.
//!   * `nrp_timeout`, `request_new_NRP`, FAILED mode, node crashes, the
//!     Backups-Known presence check, operator acknowledgments and the
//!     heartbeat payload are not modelled (dead code in the MARS files, or
//!     present only in the ETFA specification / GulFest node).
//!   * No failure is an actor. The papers' switch failures at chosen
//!     instants and the attacker of Table 1 case 8 are not modelled as
//!     such; the semantics' loss is the only fault. It contains each of
//!     them as one loss pattern (a dead switch: everything through it is
//!     lost from some instant on; the attacker: the heartbeats are lost and
//!     the pings are not) and cannot single one out. It also contains what
//!     the papers do not consider: a timer that is lost, that is, a
//!     controller whose clock has stopped while it still holds its role
//!     (not a crash: the papers' failed node gives the role up).
//!   * Afra honours `@Priority` (switches before nodes at the same instant);
//!     TraceForge explores every same-instant order, so a tie that Afra's
//!     priorities would hide is explored here: for instance DCN1's ping
//!     timer against the probe, or a heartbeat against a tick at DCN2.
//!   * With `--sd-ratio` > 0 a handler may run up to sd after its message
//!     arrives (handler latency). Timers then drift from round to round,
//!     and DCN1 may read the probe up to sd after the promotion, which can
//!     hide a violation but cannot create one (see Property checked). The
//!     CLI then requires phase + (R + 1)sd < H (see the bound above).
//!     Rebeca has no such latency; the default sd = 0 reproduces its
//!     timing.
//!
//! ## No harness (since 2026-10-13)
//!
//! Until 2026-10-13 this file wrapped the protocol in a verification
//! harness and played its failures out by script:
//!   * every message carried a per-channel sequence number and every
//!     receive did `assume!(seq == expected)`; every channel ended with a
//!     Done message that every actor drained to. Together they discarded
//!     each execution in which a message was lost or expired unread;
//!   * `main` was a monitor: the DCNs reported every role change to it with
//!     zero transit, its storage lifetime was forced to 0, and it asserted
//!     NoDualPrimary only at the end of the run, after the Done of both
//!     DCNs, that is, behind messages that can be lost;
//!   * an Init message told SW the thread ids, and an EndOfRun self-message
//!     ended each DCN;
//!   * the failures were scripted on top of the semantics: SW sent itself
//!     one notice per failure instant and attack edge (`--fault
//!     switch|link`, `--fail-a-ratio`, `--sep-ratio`, `--attack-from-ratio`,
//!     `--attack-until-ratio`) and dropped by its own rule what reached it
//!     afterwards.
//! The guard dates from the time when a message could vanish only by
//! sd = 0 eviction, which Rebeca's message bags do not have. Once every
//! send became lossy (2026-09-30) it also threw away every run with a lost
//! message: reliable links re-created by filtering, with the scripted
//! failures as the only faults left. Under the model's semantics a lost
//! message is an execution like any other, and a dual primary it leads to
//! is a dual primary. So all of the above is gone: no `assume!`,
//! no sequence number, no drain, no Done, no Init, no EndOfRun, no monitor,
//! no report and no scripted failure. Nothing is filtered and nothing is
//! judged at the end of the run. What is left besides the protocol is one
//! zero-transit probe (next section), two messages that only end the loops
//! (Termination) and the outcome counters, which are observation only and
//! are raised where their event happens.
//!
//! ## Property checked
//!
//! NoDualPrimary, verbatim from NRPFD.property:
//! `NoDualPrimary:!(DCN1Primary && DCN2Primary);`, a state assertion over
//! every reachable state. Here: no instant (same-instant order included) at
//! which both DCN1 and DCN2 are PRIMARY. Availability (someone eventually
//! in charge) is not asserted, as in the papers.
//!
//! How it is observed: one probe, asserted where it fails. The property
//! relates two processes at one instant and no thread can read a clock, so
//! at the instant DCN2 becomes PRIMARY (`promote`) it sends DCN1 one
//! message, `Promoted`, with transit exactly 0, and DCN1 asserts on its own
//! state when it reads it: `traceforge::assert(role != Role::Primary)`.
//! This is exact for this model. DCN1 starts PRIMARY, can only go PRIMARY
//! -> WAITING and never comes back; DCN2 can only go BACKUP -> PRIMARY and
//! stays there. So both are PRIMARY at some instant iff DCN1 is still
//! PRIMARY at the instant DCN2 promotes, and that is what DCN1 sees when it
//! reads the probe at that instant (sd = 0). If DCN1 steps down at that
//! very instant, both orders of its ping timer and the probe are explored,
//! like Rebeca's arbitrary order of same-time handlers. The probe is a
//! message: it can be lost (the execution in which it arrives is explored
//! too), and with sd > 0 it is read up to sd after the promotion and only
//! after every message that arrived before it, so a step-down that arrived
//! before the promotion but is handled after it goes unseen. A violation
//! can therefore be missed, never invented: a DCN1 that is PRIMARY when it
//! reads the probe has been PRIMARY all along. DCN1 is there to read every
//! probe: it leaves its loop only after DCN2's Finished, which follows the
//! probe on the same channel (Termination). The probe is the only message
//! that is judged, and nothing else is: no collector, no report, no
//! end-of-run check.
//!
//! Reading the output. `execs` counts complete executions, in which every
//! thread terminated; `blocked` those in which a thread waits for ever for
//! a message that was lost (see Termination), the ones that ended at the
//! failed assertion (with --keep-going) among them; `impossible` endings
//! that admit no timeline. The other fields are event counts over all
//! explored endings, raised by program code where the event happens (`dual`
//! just before the assertion), so with `impossible` > 0 a non-zero `dual`
//! is an upper bound and the binary says so. The verdict is the engine's: a
//! run WITHOUT --keep-going that exits 101, in timed mode after printing
//! the execution graph and a certified witness timeline. A hold with no
//! promotion, no takeover ping and no shortcut is a run too short for the
//! backup to decide, and a leasing hold with lease_chances = 0 is one in
//! which the lease could never have granted the role; the binary warns in
//! both cases.
//!
//! ## Termination (since 2026-10-14)
//!
//! No thread is a daemon: each leaves its loop on a count of what it is
//! owed, never on time and never on silence. A run in which no message is
//! lost ends with all three threads terminated, and a thread ends blocked
//! only because a message it still waits for was lost.
//!   * A DCN leaves once its R-th Tick has been read and every PingTimeout
//!     it armed and every PingReply to a ping it sent has been read. A lost
//!     ping, answer or timer is therefore a DCN that waits for ever.
//!   * DCN2 may promote at any of its ticks and ping timers, up to its last,
//!     and DCN1 cannot see when that is. So DCN2, when it leaves, sends DCN1
//!     one `Finished` (after any Promoted: one channel keeps send order),
//!     and DCN1 leaves only when it has read it as well as everything it is
//!     owed itself.
//!   * SW cannot be bounded by a count: DCN1 sends it `Stop` when it leaves,
//!     and SW leaves on reading it. By then no ping is outstanding (both
//!     DCNs read every answer before leaving), everything DCN1 sent SW came
//!     before `Stop` on its channel, and DCN2 has left. What SW may leave
//!     unread, a heartbeat or new_NRP bound for DCN2 and, in the leasing
//!     variant, DCN2's new_NRPBack, could only change an answer to a later
//!     ping, and there is none.
//! `Finished` and `Stop` are plain sends, lossy like every send, marked
//! HARNESS in the code; nothing is judged on them. Their cost is executions:
//! one that reaches `Finished` is explored in two ways (lost, delivered),
//! one that also reaches `Stop` in three (`Finished` lost; delivered and
//! `Stop` lost; both delivered), and a DCN parked at the failed assertion
//! sends nothing. Where nothing fixes the arrival of `Finished` (baseline
//! mode, or timed with L < U or sd > 0) DCN1 can in addition read it at
//! several places among its other reads, which multiplies the delivered
//! cases once more.
//!
//! ## CLI parameters (ratios are over U, like swim_timed)
//!
//!   --mode baseline|timed        default timed; the program is the same in
//!                                both, baseline has no clock (every transit
//!                                window is ignored)
//!   --variant nrp|noopt|leasing  nrp = NRPFD.rebeca (simultaneous-timeout
//!                                shortcut); noopt = shortcut removed, no
//!                                lease (MARS 4.1 first step); leasing =
//!                                LeasingNRPFD.rebeca (default nrp)
//!   --hb-on-nrp-change off|on    off = no heartbeat in the NRP-change
//!                                round (NRPFD.rebeca, and GulFest's first
//!                                implementation); on = heartbeats in that
//!                                round too (the change GulFest describes
//!                                for its modified implementation, here on
//!                                the MARS node, see Deviations)
//!   --phase-ratio F              DCN2's ticks start at F (F < 0: DCN1's
//!                                start at -F instead); |F| < H (default 0)
//!   --rounds R                   ticks per DCN (default 4)
//!   --max-missed M               patience limit (default 2)
//!   --u U                        transit upper bound per hop (default 1)
//!   --l-ratio LR                 L = round(LR * U) (default 1.0: L = U,
//!                                Rebeca's fixed networkDelay)
//!   --sd-ratio SR                sd = round(SR * U) (default 0.0); needs
//!                                phase + (R + 1)sd < H
//!   --h-ratio HR                 H = round(HR * U) (default 20, GulFest)
//!   --pt-ratio PR                P = round(PR * U) (default 10, GulFest);
//!                                1 <= P < H required
//!   --promote-rule any|takeover  which backup node to run (default
//!                                takeover = ETFA Algorithm 3; any = the
//!                                MARS node verbatim, see Deviations)
//!   --keep-going                 explore everything after a violation
//!
//! The failure flags (`--fault`, `--fail-a-ratio`, `--sep-ratio`,
//! `--attack-from-ratio`, `--attack-until-ratio`) went with the scripted
//! failures; passing one is now CLI misuse. CLI misuse exits 2, distinct
//! from a property violation's 101, so exit-code-based sweep harnesses
//! cannot mistake a typo for a FIRE.
//!
//! ## Results (re-measured 2026-10-13 after the first-arrival ruling; nothing is predicted)
//!
//! Defaults: H = 20, P = 10, max = 2, sd = 0, L = U = 1, phase 0,
//! promote-rule takeover. "old" is the harness version (commit 0cbe2c7)
//! run with `--fault none`, its own no-failure self test; "new" is this
//! file, on the engine in which a receive reads the message that arrived
//! first. One worker, 300 s limit unless stated (900 s for leasing at R = 5
//! and for baseline leasing at R = 3). A "new" verdict is from a run
//! WITHOUT --keep-going (FIRE = exit 101, in timed mode with a certified
//! witness; HOLD = exit 0); the counts are execs / blocked / dual of the
//! same cell WITH --keep-going (impossible was 0 in every run). The
//! command of a row is
//!
//!     nrp_failure_detector_timed --mode M --variant V --rounds R [--keep-going]
//!
//!   mode      variant  R  old (execs / blocked / dual)   new
//!   timed     nrp      2  hold  1 / 2,754 / 0            hold  957 / 0 / 0, run too short
//!   timed     nrp      3  hold  1 / 15,678 / 0           FIRE  24,642 / 1,078 / 1,078
//!   timed     nrp      4  hold  1 / 51,806 / 0           FIRE  (counts: no result in 300 s)
//!   timed     noopt    3  hold  1 / 37,215 / 0           FIRE  35,943 / 2,436 / 2,436
//!   timed     leasing  3  hold  1 / 35,367 / 0           hold  35,751 / 0 / 0, lease never armed
//!   timed     leasing  5  not run                        FIRE  (witness after 425 s)
//!   baseline  nrp      2  hold  630 / 13,136 / 0         hold  3,353 / 0 / 0, run too short
//!   baseline  nrp      3  FIRE  4,325 / 282,101 / 4,233  FIRE  560,331 / 55,431 / 55,431
//!   baseline  noopt    3  no result in 300 s             FIRE  (counts: no result in 300 s)
//!   baseline  leasing  3  hold  45,205 / 1,591,556 / 0   hold  1,862,846 / 0 / 0, lease never armed
//!   baseline  leasing  5  not run                        FIRE  (witness after 675 s)
//!
//! (The old verdicts are read off its --keep-going counts. "run too short":
//! the backup's counts first pass the limit at its third tick, so at R = 2
//! it never decides, and the binary warns. "lease never armed": in no
//! execution did the backup read the three NRP answers the lease needs, so
//! the hold says nothing about the lease, and the binary warns. In the new
//! version noopt and leasing at R = 2 give the same counts as nrp at R = 2,
//! 957 timed and 3,353 baseline. The first-arrival rule is a rule of the
//! clock, so the baseline counts are not affected by it.)
//!
//! Bounded threads (2026-10-14, see Termination). Every execs / blocked / dual
//! count above, and the Cost paragraph below, was measured with the daemon
//! version of this file. The bounded version explores the same protocol
//! behaviours (in each cell listed here the two controllers read the same
//! messages in the same order, the violating ones included) and counts each
//! execution once more for every way its termination messages can be lost
//! or delivered; `blocked` is now every execution in which a thread waits
//! for a lost message. With --keep-going, execs / blocked / violating:
//!
//!   mode      variant  R  daemons                       bounded
//!   timed     nrp      1  38 / 0 / 0                     9 / 57 / 0
//!   timed     nrp      2  957 / 0 / 0                    81 / 1,276 / 0
//!   timed     nrp      3  24,642 / 1,078 / 1,078         729 / 31,773 / 2,156
//!   timed     noopt    3  35,943 / 2,436 / 2,436         729 / 45,946 / 4,872
//!   timed     leasing  3  35,751 / 0 / 0                 729 / 40,690 / 0
//!   baseline  nrp      1  53 / 0 / 0                     56 / 137 / 0
//!   baseline  nrp      2  3,353 / 0 / 0                  2,660 / 9,251 / 0
//!   baseline  nrp      3  560,331 / 55,431 / 55,431      242,855 / 1,327,590 / 110,862
//!
//! Timed nrp at R = 3: of the 25,720 executions of the daemon version, 19,667
//! lose a message that DCN2 waits for (they stay one), 5,324 reach Finished
//! (two, the 1,078 violating ones among them: DCN1 is parked at the failed
//! assertion and sends nothing more) and 729 reach Stop as well (three):
//! 32,502 in all, the violating ones exactly twice. The 729 complete
//! executions are the fates of the six heartbeats (lost on one leg, lost on
//! the other, delivered: 3^6). In baseline mode DCN1 can read Finished at
//! any place among its other reads, so a delivered case is multiplied by
//! the number of places (R = 3: the 142,061 non-violating executions that
//! reach Finished and not Stop give 413,542, the 76,623 that reach Stop give
//! 242,855 with Stop lost and as many with Stop delivered).
//!
//! What changed, and why. In the old version exactly one execution reached
//! the judgement in each timed cell, the one in which nothing is lost and
//! nothing expires; every other execution was discarded by the sequence
//! guard or blocked in a drain. With `--fault none` that execution has no
//! fault at all, so the old self test held by construction. Now every
//! execution is judged, and every message that is not dropped is read,
//! the earliest arrival first:
//!
//!   * nrp, R = 3, certified witness (exit 101): three sends are dropped.
//!     DCN1 pings A1 at 0 and SW's answer is dropped, so at 10 DCN1 moves
//!     to B1 and, as in the code as first written, sends no heartbeat that
//!     round. It pings B1 at 20, reads the answer at 22 and at 30 sends both
//!     heartbeats, which SW drops on the forward. DCN2 reads the new_NRP at
//!     14 and no heartbeat: both its counts are 3 = max + 1 at its third
//!     tick at 40, the shortcut promotes it, and its probe finds DCN1 still
//!     PRIMARY at 40. DCN1 has read every timer due before the probe.
//!   * noopt, R = 3, certified witness: four sends are dropped, the three
//!     above and the answer to DCN1's ping to B1 at 40 (sent at 41). DCN2
//!     sends its takeover ping at 40 (to B1: it read the new_NRP), reads the
//!     answer at 46 and promotes at its ping timer at 50. DCN1's own ping
//!     timer, which finds no answer and would step it down, arrives at 50 as
//!     well; the probe is read first (both orders are explored) and finds
//!     DCN1 still PRIMARY. This witness hangs on that tie; most violating
//!     executions do not (if the answer of 41 is delivered, DCN1 is still
//!     PRIMARY at 50 in either order).
//!   * A stopped clock is not needed. In both witnesses DCN1 reads every
//!     timer due before the probe, and the run in which nothing is lost but
//!     the four heartbeat copies of rounds 0 and 1 is a violating execution
//!     too: DCN1 is answered by A1 in every round and sends its heartbeats
//!     in every round, DCN2 hears nothing by its third tick, and the probe
//!     finds a perfectly healthy primary. A re-implementation of this
//!     program in Python (not in the repository), which reproduces every
//!     timed execs / blocked count reported here exactly, tags every
//!     violating execution at the probe. At R = 3, for nrp, 512 of the
//!     1,078 are executions in which DCN1 had read every timer due before
//!     the promotion, 128 of them with DCN1 answered by A1 in every round
//!     and its heartbeats sent in every round; for noopt, 1,740 and 456 of
//!     2,436 (and only 168 of the 2,436 hang on a same-instant order like
//!     the one above). In those 128 and 456 the primary is perfectly
//!     healthy and the backup takes over only because two rounds of
//!     heartbeats never reached it.
//!   * leasing, R = 5, certified witness, found after 425 s: DCN1 loses
//!     A1's first answer and moves to B1 at 10 (DCN2 reads the new_NRP at
//!     14), is answered by B1 in rounds 1 and 2 and sends its heartbeats at
//!     30 and 50, of which DCN2 reads only network A's at 34 (SW drops the
//!     other three on the forward). DCN1's ping of round 3 (at 60) is
//!     dropped and so is its ping timer of that round, so it does not step
//!     down at 70; its ping of round 4 (at 80) is dropped too. DCN2 pings
//!     B1 at 40, 60 and 80: the first answer resets its count (B1's
//!     previous question was the primary's), the next two pass, and it
//!     promotes at 90 with DCN1 still PRIMARY (DCN1's ping timer of round 4
//!     arrives at 90 as well, and the probe is read first). With
//!     `--max-missed 0` the same variant FIREs at R = 3 (a witness after
//!     about 25 s in timed mode, under 1 s in baseline).
//!   * The lease. At R = 3 it holds in both modes, but the hold is empty:
//!     the backup never reads the three NRP answers the lease needs
//!     (lease_chances = 0, the binary warns). The backup first pings the
//!     landmark at its tick max, so its third answer comes at tick max + 2
//!     and R = max + 3 = 5 is the shortest run in which the lease can grant
//!     the role at all; there it FIREs in both modes (with `--max-missed 0`
//!     that is R = 3, and it FIREs). So with every message lossy the lease
//!     does not exclude a dual primary: the witness above is one.
//!   * The run length matters, the timing does not: at R = 2 every variant
//!     holds in both modes because the backup cannot decide yet (also with
//!     `--promote-rule any`, `--hb-on-nrp-change on`, `--l-ratio 0`,
//!     `--phase-ratio 5`, `--phase-ratio -5`, `--sd-ratio 1`: 957
//!     executions each, 4,845 with hb-on-nrp-change, all warned), and at
//!     R = 3 each of those six settings FIREs (timed, nrp). From the round
//!     at which the backup can take over (R = 3 for nrp and noopt, R = 5
//!     for leasing), both modes FIRE.
//!
//! Cost. The exploration is far larger than the old one, which stopped
//! every lossy execution at its first gap: nrp at R = 3 takes 48 s timed
//! and 141 s baseline, noopt 68 s timed, leasing 49 s timed and 350 s
//! baseline (hence its 900 s limit); at the default R = 4 the first FIRE
//! comes after 0.1 s (nrp, timed) but the whole space does not finish in
//! 300 s. (Wall clock with two explorations side by side on a shared
//! machine; the same cell varied by a factor of up to 1.7 between runs.)
//!
//! WITHDRAWN. Everything this header reported before 2026-10-13 described
//! scripted failures on links that lost nothing else, and is no longer
//! claimed: the self-test holds at R = 4, the reproduction of MARS Table 1
//! cases 7 and 8 and of the leasing fix, the comparison of the two promote
//! rules, the separation windows of two switch failures (and their
//! comparison with the GulFest equations), the link-cut cells and the
//! lease's zero-margin tie. Under the model's semantics a switch failure, a
//! cut cable or an attacker is one loss pattern among all those explored,
//! and the property is violated without any of them. Also withdrawn are the
//! witnesses of the first re-measurement of 2026-10-13, in which a message
//! arrived while its receiver waited and was never read: a waiting receive
//! now reads the message that arrived first, and every number of the new
//! column was measured again.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats};

// Outcome counters (observation only; the verdict is the assertion in DCN1's
// Promoted handler). Each is raised by program code at the site where its
// event happens, on every ending the search enumerates, so they are event
// counts over all explored executions (revisited prefixes may re-count).
// Reset before each verify, read after.
static DUAL_PRIMARY: AtomicUsize = AtomicUsize::new(0);
static STEPDOWNS: AtomicUsize = AtomicUsize::new(0);
static PROMOTIONS: AtomicUsize = AtomicUsize::new(0);
static SHORTCUT_PROMOTIONS: AtomicUsize = AtomicUsize::new(0);
static TAKEOVER_PINGS: AtomicUsize = AtomicUsize::new(0);
static BACKUP_REPLIES: AtomicUsize = AtomicUsize::new(0);
static LEASE_RESETS: AtomicUsize = AtomicUsize::new(0);
// Raised once in an execution, when the backup reads its LEASE_MIN_ANSWERS-th
// NRP answer: from then on the lease could have granted it the role at all.
static LEASE_CHANCES: AtomicUsize = AtomicUsize::new(0);
static NRP_CHANGES: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_ROUNDS: u32 = 4;
const DEFAULT_MAX_MISSED: u32 = 2;
const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 1.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const DEFAULT_H_RATIO: f64 = 20.0;
const DEFAULT_PT_RATIO: f64 = 10.0;

/// Fewest NRP answers after which the lease can grant the backup the role.
/// The first answer always resets the backup's `which` (the NRP's previous
/// ping was the primary's, or `which` is still its initial `true`), and a
/// promotion needs `which > 1`: one reset plus two passing answers.
const LEASE_MIN_ANSWERS: u32 = 3;

/// Hops between SW (A1/B1) and DCN2 (A1 -> A2 -> A3 -> DCN2).
const FAR_HOPS: u64 = 3;

// Actor indices (the `from` field of every message).
const DCN1: usize = 1;
const DCN2: usize = 2;
const SW: usize = 3;

// Networks.
const NET_A: usize = 0;
const NET_B: usize = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    /// NRPFD.rebeca: simultaneous-timeout shortcut, no lease.
    Nrp,
    /// Shortcut removed, no lease.
    NoOpt,
    /// LeasingNRPFD.rebeca: no shortcut, lease at the NRP.
    Leasing,
}

impl Variant {
    fn shortcut(self) -> bool {
        self == Variant::Nrp
    }
    fn lease(self) -> bool {
        self == Variant::Leasing
    }
    fn name(self) -> &'static str {
        match self {
            Variant::Nrp => "nrp",
            Variant::NoOpt => "noopt",
            Variant::Leasing => "leasing",
        }
    }
}

/// When a backup that got its ping answered takes over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PromoteRule {
    /// NRPFD.rebeca / LeasingNRPFD.rebeca `ping_timed_out`: the BACKUP branch
    /// promotes whenever the ping was answered (`!ping_pending`), whether the
    /// ping was sent because both counters were over the limit or only the
    /// NRP's network was.
    Any,
    /// ETFA Algorithm 3 / the GulFest node's `become_primary_on_ping_response`:
    /// only a ping sent with BOTH counters over the limit may promote.
    Takeover,
}

impl PromoteRule {
    fn name(self) -> &'static str {
        match self {
            PromoteRule::Any => "any",
            PromoteRule::Takeover => "takeover",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Primary,
    Backup,
    /// Stepped down (Rebeca WAITING); never re-promotes in this model.
    Waiting,
}

#[derive(Clone, Debug, PartialEq)]
enum Body {
    /// Self timer: Rebeca runMe().
    Tick { k: u32 },
    /// Self timer: Rebeca ping_timed_out().
    PingTimeout,
    /// DCN -> SW: pingNRP to the NRP on network `net`; the answer goes to
    /// `reply_to`.
    Ping { net: usize, reply_to: ThreadId },
    /// SW -> DCN: pingNRP_response(which, prevWhich).
    PingReply { w: bool, pw: bool },
    /// DCN1 -> SW -> DCN2 (`to`): heartBeat on network `net`, one message
    /// per network as in the Rebeca models.
    Heartbeat { net: usize, to: ThreadId },
    /// DCN1 -> SW -> DCN2 (`to`): new_NRP over network `net`.
    NewNrp { net: usize, to: ThreadId },
    /// DCN2 -> SW (leasing): new_NRPBack, the NRP records the new primary.
    NewNrpBack { net: usize },
    /// DCN2 -> DCN1, zero transit, sent at the instant DCN2 becomes PRIMARY:
    /// the NoDualPrimary probe (the only message that is judged, see Property
    /// checked).
    Promoted,
    /// HARNESS: DCN2 -> DCN1, sent when DCN2 leaves its loop, after any
    /// Promoted on the same channel. DCN1 leaves its loop only after reading
    /// it, so that DCN1 is still there for every probe DCN2 can send (see
    /// Termination).
    Finished,
    /// HARNESS: DCN1 -> SW, sent when DCN1 leaves its loop; SW leaves its loop
    /// on reading it (see Termination).
    Stop,
}

/// Every message: sender (DCN1, DCN2 or SW; the NRP's lease memory compares
/// senders) and body.
#[derive(Clone, Debug, PartialEq)]
struct Msg {
    from: usize,
    body: Body,
}

#[derive(Clone, Copy, Debug)]
struct Params {
    variant: Variant,
    promote_rule: PromoteRule,
    hb_on_nrp_change: bool,
    rounds: u32,
    max_missed: u32,
    u: u64,
    l: u64,
    sd: u64,
    h: u64,
    p: u64,
    /// DCN2's tick offset; negative offsets DCN1 instead.
    phase: i64,
}

impl Params {
    fn first_tick(&self, node: usize) -> u64 {
        match node {
            DCN1 => (-self.phase).max(0) as u64,
            _ => self.phase.max(0) as u64,
        }
    }
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

#[derive(Clone, Copy)]
enum Leg {
    /// One hop: DCN1 <-> SW.
    Near,
    /// Three hops: SW <-> DCN2.
    Far,
    /// Exact delay: timers and the probe.
    Exact(u64),
}

/// Send `body` from actor `from` to `to` with the transit of `leg`. The
/// transit override has no effect in baseline mode (same program both modes).
/// Like every send, it may be lost.
fn send(p: &Params, from: usize, to: ThreadId, leg: Leg, body: Body) {
    let (lo, hi) = match leg {
        Leg::Near => (p.l, p.u),
        Leg::Far => (FAR_HOPS * p.l, FAR_HOPS * p.u),
        Leg::Exact(d) => (d, d),
    };
    traceforge::send_msg_timed(to, Msg { from, body }, lo, hi);
}

/// The single reactive receive of every actor (a Rebeca message server):
/// the next message, whoever sent it, the one that arrived first. A lost
/// message is never read, exactly like a message that was never sent.
fn recv() -> (usize, Body) {
    let m: Msg = traceforge::recv_msg_block_timed();
    (m.from, m.body)
}

// =====================================================================
// Controller node (DCN1 starts PRIMARY, DCN2 starts BACKUP)
// =====================================================================

/// One controller: one blocking receive loop, left once everything this
/// controller armed or sent has been read (see Termination). `sw` is the
/// switch pair, `peer` the other controller (DCN1 sends its heartbeats to
/// DCN2, DCN2 sends its Promoted probe to DCN1).
fn dcn(node: usize, p: Params, sw: ThreadId, peer: ThreadId) {
    let me = thread::current().id();
    // DCN2's traffic to the switches crosses A3/A2 (or B3/B2) first.
    let net_leg = if node == DCN1 { Leg::Near } else { Leg::Far };

    let mut role = if node == DCN1 { Role::Primary } else { Role::Backup };
    let mut missed = [0u32; 2];
    let mut nrp_net: usize = NET_A;
    let mut ping_pending = false;
    // GulFest's become_primary_on_ping_response: this ping was a takeover
    // ping (both counters over the limit).
    let mut takeover_ping = false;
    // Leasing: consecutive NRP answers with no primary ping in between.
    let mut which: u32 = 0;
    // Observation only (LEASE_CHANCES): NRP answers this backup has read.
    let mut nrp_answers: u32 = 0;
    let max = p.max_missed;
    // pingNRP: the packet names the thread to answer.
    let ping = |net: usize| Body::Ping { net, reply_to: me };
    // What ends the loop (Termination): counts of what this controller is
    // owed, not time. The Ticks still to be read, the PingTimeouts it armed
    // and the PingReplies to the pings it sent (two per ping), and, for
    // DCN1, DCN2's Finished.
    let mut ticks_left = p.rounds;
    let mut owed: u32 = 0;
    let mut peer_finished = node != DCN1;

    send(&p, node, me, Leg::Exact(p.first_tick(node)), Body::Tick { k: 0 });

    while ticks_left > 0 || owed > 0 || !peer_finished {
        let (from, body) = recv();
        // Settle what this message was owed.
        match body {
            Body::Tick { .. } => ticks_left -= 1,
            Body::PingTimeout | Body::PingReply { .. } => owed -= 1,
            Body::Finished => peer_finished = true,
            _ => {}
        }
        match body {
            Body::Tick { k } => {
                match role {
                    Role::Primary if node == DCN1 => {
                        // runMe, PRIMARY: ping the NRP, arm ping_timed_out.
                        ping_pending = true;
                        owed += 2;
                        send(&p, node, sw, net_leg, ping(nrp_net));
                        send(&p, node, me, Leg::Exact(p.p), Body::PingTimeout);
                    }
                    // A promoted DCN2 is passive (see Deviations).
                    Role::Primary => {}
                    Role::Backup => {
                        // runMe, BACKUP: count, then decide.
                        missed[NET_A] += 1;
                        missed[NET_B] += 1;
                        let over_a = missed[NET_A] > max;
                        let over_b = missed[NET_B] > max;
                        if over_a && over_b {
                            if p.variant.shortcut()
                                && missed[NET_A] == missed[NET_B]
                                && missed[NET_A] == max + 1
                            {
                                // Simultaneous timeout on both networks:
                                // take over without asking the NRP.
                                SHORTCUT_PROMOTIONS.fetch_add(1, Ordering::Relaxed);
                                promote(&p, node, sw, peer, &mut role, &mut missed, nrp_net);
                            } else {
                                clamp(&mut missed, max);
                                ping_pending = true;
                                takeover_ping = true;
                                owed += 2;
                                TAKEOVER_PINGS.fetch_add(1, Ordering::Relaxed);
                                send(&p, node, sw, net_leg, ping(nrp_net));
                                send(&p, node, me, Leg::Exact(p.p), Body::PingTimeout);
                            }
                        } else if over_a || over_b {
                            // One network silent: check the NRP only if it
                            // sits on the silent network.
                            if (nrp_net == NET_A && over_a) || (nrp_net == NET_B && over_b) {
                                ping_pending = true;
                                takeover_ping = false;
                                owed += 2;
                                send(&p, node, sw, net_leg, ping(nrp_net));
                                send(&p, node, me, Leg::Exact(p.p), Body::PingTimeout);
                            }
                            clamp(&mut missed, max);
                        }
                    }
                    Role::Waiting => {}
                }
                // Next runMe (sent after the ping timer: firing order). The
                // run is bounded by this count: after its last round a DCN
                // arms nothing and only waits for what it is still owed.
                if k + 1 < p.rounds {
                    send(&p, node, me, Leg::Exact(p.h), Body::Tick { k: k + 1 });
                }
            }
            Body::PingTimeout => match role {
                Role::Primary if node == DCN1 => {
                    if ping_pending {
                        // No answer: move to the next NRP candidate, or
                        // give up when none is left.
                        nrp_net += 1;
                        if nrp_net < 2 {
                            NRP_CHANGES.fetch_add(1, Ordering::Relaxed);
                            send(&p, node, sw, Leg::Near, Body::NewNrp { net: nrp_net, to: peer });
                            if p.hb_on_nrp_change {
                                heartbeats(&p, sw, peer);
                            }
                        } else {
                            // Step down (Rebeca WAITING), silently: DCN1
                            // never becomes PRIMARY again in this model.
                            role = Role::Waiting;
                            STEPDOWNS.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        heartbeats(&p, sw, peer);
                    }
                }
                Role::Backup => {
                    if ping_pending {
                        ping_pending = false;
                    } else if (p.promote_rule == PromoteRule::Any || takeover_ping)
                        && (!p.variant.lease() || which > 1)
                    {
                        promote(&p, node, sw, peer, &mut role, &mut missed, nrp_net);
                    }
                    takeover_ping = false;
                }
                _ => {}
            },
            Body::PingReply { w, pw } => match role {
                Role::Primary => ping_pending = false,
                Role::Backup => {
                    BACKUP_REPLIES.fetch_add(1, Ordering::Relaxed);
                    nrp_answers += 1;
                    if nrp_answers == LEASE_MIN_ANSWERS {
                        LEASE_CHANCES.fetch_add(1, Ordering::Relaxed);
                    }
                    if p.variant.lease() {
                        if !w && !pw {
                            which += 1;
                        } else {
                            which = 0;
                            LEASE_RESETS.fetch_add(1, Ordering::Relaxed);
                        }
                        if which > 1 {
                            ping_pending = false;
                        }
                    } else {
                        ping_pending = false;
                    }
                }
                Role::Waiting => {}
            },
            Body::Heartbeat { net, .. } => {
                if role == Role::Backup {
                    missed[net] = 0;
                }
            }
            Body::NewNrp { net, .. } => {
                // NRPFD.rebeca: adopt the new NRP only. GulFest's node also
                // resets missed[net] here (see Deviations).
                nrp_net = net;
            }
            Body::Promoted => {
                // NoDualPrimary (see Property checked). DCN2 sent this at
                // the instant it became PRIMARY; DCN1 judges its own role
                // now. DCN1 never becomes PRIMARY again once it has stepped
                // down, so PRIMARY here means PRIMARY at that instant too.
                if role == Role::Primary {
                    DUAL_PRIMARY.fetch_add(1, Ordering::Relaxed);
                }
                traceforge::assert(role != Role::Primary);
            }
            // HARNESS: DCN2's termination message, settled above.
            Body::Finished if node == DCN1 => {}
            m => panic!("DCN{node}: unexpected {m:?} from actor {from}"),
        }
    }
    // HARNESS: the termination messages, plain sends (lossy like every
    // send) and the only messages that are neither protocol nor the probe.
    // DCN2 tells DCN1, after any Promoted on their channel, that it decides
    // nothing more; DCN1, having read that and everything it is owed itself,
    // tells SW to leave (see Termination).
    if node == DCN2 {
        traceforge::send_msg(peer, Msg { from: node, body: Body::Finished });
    } else {
        traceforge::send_msg(sw, Msg { from: node, body: Body::Stop });
    }
}

/// DCN1's heartBeat(0) and heartBeat(1) of one instant: one message per
/// network, network A first (NRPFD.rebeca's order), each forwarded by SW.
fn heartbeats(p: &Params, sw: ThreadId, dcn2: ThreadId) {
    for net in [NET_A, NET_B] {
        send(p, DCN1, sw, Leg::Near, Body::Heartbeat { net, to: dcn2 });
    }
}

fn clamp(missed: &mut [u32; 2], max: u32) {
    for m in missed.iter_mut() {
        *m = (*m).min(max + 2);
    }
}

fn promote(
    p: &Params,
    node: usize,
    sw: ThreadId,
    peer: ThreadId,
    role: &mut Role,
    missed: &mut [u32; 2],
    nrp_net: usize,
) {
    *role = Role::Primary;
    *missed = [0, 0];
    PROMOTIONS.fetch_add(1, Ordering::Relaxed);
    // The NoDualPrimary probe, zero transit: DCN1 asserts on its own role
    // when it reads it (see Property checked).
    send(p, node, peer, Leg::Exact(0), Body::Promoted);
    if p.variant.lease() {
        send(p, node, sw, Leg::Far, Body::NewNrpBack { net: nrp_net });
    }
}

// =====================================================================
// SW: switches A1 (initial NRP) and B1 (second candidate)
// =====================================================================

/// The switch pair: one blocking receive loop, left on DCN1's Stop (see
/// Termination). It knows no thread id: every packet carries the address it
/// needs.
fn switch(p: Params) {
    // Lease memory per NRP switch (LeasingNRPFD.rebeca Switch statevars).
    let mut primary: [Option<usize>; 2] = [Some(DCN1), None];
    let mut which = [true; 2];

    loop {
        let (from, body) = recv();
        let leg_to = |node: usize| if node == DCN1 { Leg::Near } else { Leg::Far };
        match body {
            Body::Ping { net, reply_to } => {
                // pingNRP at the NRP: prevWhich = which;
                // which = (sender == primary); reply both.
                let pw = which[net];
                let w = primary[net] == Some(from);
                which[net] = w;
                send(&p, SW, reply_to, leg_to(from), Body::PingReply { w, pw });
            }
            Body::Heartbeat { net, to } => {
                // heartBeat: pass it on along the chain to DCN2.
                send(&p, SW, to, Leg::Far, Body::Heartbeat { net, to });
            }
            Body::NewNrp { net, to } => {
                primary[net] = Some(from);
                send(&p, SW, to, Leg::Far, Body::NewNrp { net, to });
            }
            Body::NewNrpBack { net } => primary[net] = Some(from),
            // HARNESS: DCN1's termination message.
            Body::Stop => break,
            m => panic!("SW: unexpected {m:?} from actor {from}"),
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, p: &Params, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX);
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(p.l, p.u, p.sd).build(),
    }
}

fn reset_counts() {
    for c in [
        &DUAL_PRIMARY,
        &STEPDOWNS,
        &PROMOTIONS,
        &SHORTCUT_PROMOTIONS,
        &TAKEOVER_PINGS,
        &BACKUP_REPLIES,
        &LEASE_RESETS,
        &LEASE_CHANCES,
        &NRP_CHANGES,
    ] {
        c.store(0, Ordering::Relaxed);
    }
}

fn run(mode: Mode, p: Params, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, &p, keep_going);
    reset_counts();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        // Three threads and nothing else, none a daemon: `main` spawns and
        // returns, joins nobody and sends nothing. Each thread leaves its
        // loop on its own bound (see Termination), so a run in which no
        // message is lost ends with all three terminated, and a thread waits
        // for ever only for a message that was lost.
        let sw = thread::spawn(move || switch(p)).thread().id();
        thread::spawn(move || {
            // DCN1 spawns DCN2: each needs the other's id (heartbeats go to
            // DCN2, the probe to DCN1) and no message may carry them.
            let dcn1 = thread::current().id();
            let dcn2 = thread::spawn(move || dcn(DCN2, p, sw, dcn1)).thread().id();
            dcn(DCN1, p, sw, dcn2)
        });
    });
    (stats, start.elapsed())
}

// =====================================================================
// Reporting
// =====================================================================

#[derive(Clone, Copy, Debug)]
struct Counts {
    dual: usize,
    stepdowns: usize,
    promotions: usize,
    shortcut: usize,
    takeover_pings: usize,
    backup_replies: usize,
    lease_resets: usize,
    lease_chances: usize,
    nrp_changes: usize,
}

fn read_counts() -> Counts {
    Counts {
        dual: DUAL_PRIMARY.load(Ordering::Relaxed),
        stepdowns: STEPDOWNS.load(Ordering::Relaxed),
        promotions: PROMOTIONS.load(Ordering::Relaxed),
        shortcut: SHORTCUT_PROMOTIONS.load(Ordering::Relaxed),
        takeover_pings: TAKEOVER_PINGS.load(Ordering::Relaxed),
        backup_replies: BACKUP_REPLIES.load(Ordering::Relaxed),
        lease_resets: LEASE_RESETS.load(Ordering::Relaxed),
        lease_chances: LEASE_CHANCES.load(Ordering::Relaxed),
        nrp_changes: NRP_CHANGES.load(Ordering::Relaxed),
    }
}

fn print_one(label: &str, p: &Params, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<8} variant={v} promote={pmr} hb_on_change={hb} \
         R={r} max={mx} L={l} U={u} sd={sd} H={h} P={pt} phase={ph}  execs={execs} \
         blocked={block} impossible={imp} dual={dual} stepdowns={sdn} promotions={pr} \
         shortcut={sc} takeover_pings={tp} backup_replies={br} lease_resets={lr} \
         lease_chances={lc} nrp_changes={nc} violations={dual} time={dur:?}",
        v = p.variant.name(),
        pmr = p.promote_rule.name(),
        hb = if p.hb_on_nrp_change { "on" } else { "off" },
        r = p.rounds,
        mx = p.max_missed,
        l = p.l,
        u = p.u,
        sd = p.sd,
        h = p.h,
        pt = p.p,
        ph = p.phase,
        execs = stats.execs,
        block = stats.block,
        imp = stats.timeline_impossible,
        dual = c.dual,
        sdn = c.stepdowns,
        pr = c.promotions,
        sc = c.shortcut,
        tp = c.takeover_pings,
        br = c.backup_replies,
        lr = c.lease_resets,
        lc = c.lease_chances,
        nc = c.nrp_changes,
        dur = dur,
    );
    // The counters are raised by PROGRAM code, so they also see endings that
    // the engine later judges to admit no timeline (those run to completion
    // first). A count of zero is sound; a count above zero is an upper
    // bound. The verdict is the engine's: exit 101 with a certified witness,
    // without --keep-going.
    if stats.timeline_impossible > 0 && c.dual > 0 {
        println!(
            "NOTE: {} of the {} endings explored admit no timeline and were counted above too; \
             violations={} is an upper bound. Re-run without --keep-going (exit 101 = the engine \
             certified a counterexample) before quoting it.",
            stats.timeline_impossible,
            stats.execs + stats.block + stats.timeline_impossible,
            c.dual
        );
    }
}

/// Vacuity guards: an exit-0 run only counts as a hold if the backup
/// actually reached a takeover decision and, for the leasing variant, if
/// the lease could have granted the role at all.
fn warn_if_vacuous(label: &str, p: &Params, stats: &Stats, c: Counts) {
    if c.dual > 0 {
        return;
    }
    // A violating execution ends at its assertion and is filed under
    // `block`, like every execution in which a thread waits for a lost
    // message. No complete execution and no violation is an empty run, not
    // a hold.
    if stats.execs == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={}) and no violation: this run \
             verified nothing; treat it as no data, not as a hold.",
            stats.block
        );
        return;
    }
    if c.promotions == 0 && c.takeover_pings == 0 && c.shortcut == 0 {
        println!(
            "WARNING ({label}): the backup never reached a takeover decision (no promotion, no \
             takeover ping, no shortcut): the run is too short for the backup to decide (its \
             counts first pass the limit at its tick number max + 1 = {}), so this hold says \
             nothing about dual primaries; increase --rounds.",
            p.max_missed + 1
        );
    }
    if p.variant.lease() {
        if c.backup_replies == 0 {
            println!(
                "WARNING ({label}): the NRP never answered the backup, so the lease was never \
                 consulted: this hold says nothing about the lease (only that the shortcut is \
                 gone)."
            );
        } else if c.lease_chances == 0 {
            println!(
                "WARNING ({label}): in no execution did the backup read the {LEASE_MIN_ANSWERS} \
                 NRP answers the lease needs before it can grant the role, so the lease could \
                 never have promoted it: this hold is no evidence for the lease; increase \
                 --rounds."
            );
        }
    }
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    variant: Variant,
    hb_on_nrp_change: bool,
    phase_ratio: f64,
    rounds: u32,
    max_missed: u32,
    u: u64,
    l_ratio: f64,
    sd_ratio: f64,
    h_ratio: f64,
    pt_ratio: f64,
    promote_rule: PromoteRule,
    keep_going: bool,
}

fn next_val(args: &mut std::env::Args, flag: &str) -> String {
    args.next().unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
}

fn parse_num<T: std::str::FromStr>(v: String, flag: &str) -> T {
    v.parse().unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
}

fn on_off(v: String, flag: &str) -> bool {
    match v.as_str() {
        "on" => true,
        "off" => false,
        other => cli_bail(&format!("invalid {flag}: {other} (expected on|off)")),
    }
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::from("timed"),
        variant: Variant::Nrp,
        hb_on_nrp_change: false,
        phase_ratio: 0.0,
        rounds: DEFAULT_ROUNDS,
        max_missed: DEFAULT_MAX_MISSED,
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        h_ratio: DEFAULT_H_RATIO,
        pt_ratio: DEFAULT_PT_RATIO,
        promote_rule: PromoteRule::Takeover,
        keep_going: false,
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--variant" => {
                a.variant = match next_val(&mut args, "--variant").as_str() {
                    "nrp" => Variant::Nrp,
                    "noopt" => Variant::NoOpt,
                    "leasing" => Variant::Leasing,
                    other => cli_bail(&format!(
                        "invalid --variant: {other} (expected nrp|noopt|leasing)"
                    )),
                }
            }
            "--hb-on-nrp-change" => {
                a.hb_on_nrp_change =
                    on_off(next_val(&mut args, "--hb-on-nrp-change"), "--hb-on-nrp-change")
            }
            "--phase-ratio" => {
                a.phase_ratio = parse_num(next_val(&mut args, "--phase-ratio"), "--phase-ratio")
            }
            "--rounds" => a.rounds = parse_num(next_val(&mut args, "--rounds"), "--rounds"),
            "--max-missed" => {
                a.max_missed = parse_num(next_val(&mut args, "--max-missed"), "--max-missed")
            }
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--h-ratio" => a.h_ratio = parse_num(next_val(&mut args, "--h-ratio"), "--h-ratio"),
            "--pt-ratio" => a.pt_ratio = parse_num(next_val(&mut args, "--pt-ratio"), "--pt-ratio"),
            "--promote-rule" => {
                a.promote_rule = match next_val(&mut args, "--promote-rule").as_str() {
                    "any" => PromoteRule::Any,
                    "takeover" => PromoteRule::Takeover,
                    other => cli_bail(&format!(
                        "invalid --promote-rule: {other} (expected any|takeover)"
                    )),
                }
            }
            "--keep-going" => a.keep_going = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: nrp_failure_detector_timed [--mode baseline|timed] \
                     [--variant nrp|noopt|leasing] [--hb-on-nrp-change off|on] \
                     [--phase-ratio F] [--rounds R] [--max-missed M] \
                     [--u U] [--l-ratio LR] [--sd-ratio SR] [--h-ratio HR] [--pt-ratio PR] \
                     [--promote-rule any|takeover] [--keep-going]\n\
                     Defaults: timed, nrp, hb-on-nrp-change off, phase 0, R={DEFAULT_ROUNDS}, \
                     max=2, U=1, L/U=1, sd/U=0, H/U=20, P/U=10, promote-rule takeover.\n\
                     No failure is scripted: every send may be lost, and that is the only fault.\n\
                     Exit 0 = no dual primary over the explored executions; exit 101 = dual \
                     primary (NoDualPrimary violated); exit 2 = CLI misuse."
                );
                std::process::exit(0);
            }
            other => cli_bail(&format!("unknown argument: {other}")),
        }
    }
    a
}

fn params_from(a: &Args) -> Params {
    if a.u < 1 {
        cli_bail("U must be >= 1");
    }
    if a.rounds < 1 {
        cli_bail("need at least 1 round");
    }
    let u = a.u;
    let scale = |r: f64, what: &str| -> f64 {
        let v = (r * u as f64).round();
        if !v.is_finite() {
            cli_bail(&format!("{what} must be a finite number"));
        }
        v
    };
    let nonneg = |r: f64, what: &str| -> u64 {
        let v = scale(r, what);
        if v < 0.0 {
            cli_bail(&format!("{what} must be >= 0"));
        }
        v as u64
    };
    let l = nonneg(a.l_ratio, "L");
    if l > u {
        cli_bail("transit lower bound L must be <= U (check --l-ratio)");
    }
    let sd = nonneg(a.sd_ratio, "sd");
    let h = nonneg(a.h_ratio, "H");
    let pt = nonneg(a.pt_ratio, "P");
    if pt < 1 || pt >= h {
        cli_bail("need 1 <= P < H (check --pt-ratio and --h-ratio)");
    }
    let phase = scale(a.phase_ratio, "phase") as i64;
    if phase.unsigned_abs() >= h {
        cli_bail("need |phase| < H (check --phase-ratio)");
    }
    // The bound of the run (R ticks per DCN) must not be able to cause a
    // FIRE. DCN2 decides for the last time at its last ping timer, at most
    // at t2 + (R - 1)H + P + (R + 1)sd (t2 its first tick, each timer read
    // up to sd late); a longer run could show DCN1 stepping down at
    // t1 + R*H + P at the earliest (the ping timer of the first round this
    // run does not have). The first must precede the second; t2 - t1 = phase.
    // Always true at sd = 0, where |phase| < H is enough.
    let drift = (a.rounds as u64 + 1).saturating_mul(sd);
    if phase as i128 + drift as i128 >= h as i128 {
        cli_bail(
            "need phase + (R + 1) * sd < H, or DCN2 could decide after the instant at which a \
             longer run lets DCN1 step down (check --sd-ratio, --rounds, --phase-ratio)",
        );
    }
    let p = Params {
        variant: a.variant,
        promote_rule: a.promote_rule,
        hb_on_nrp_change: a.hb_on_nrp_change,
        rounds: a.rounds,
        max_missed: a.max_missed,
        u,
        l,
        sd,
        h,
        p: pt,
        phase,
    };
    if 2 * FAR_HOPS * u >= pt {
        println!(
            "note: the backup's ping round trip can take {} > P - 1 = {}; late NRP answers are \
             ignored by the ping timer",
            2 * FAR_HOPS * u,
            pt - 1
        );
    }
    p
}

fn main() {
    let a = parse_args();
    let p = params_from(&a);
    let (mode, label) = match a.mode.as_str() {
        "baseline" => (Mode::Baseline, "baseline"),
        "timed" => (Mode::Timed, "timed"),
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed)")),
    };
    let (s, d) = run(mode, p, a.keep_going);
    let c = read_counts();
    print_one(label, &p, &s, d, c);
    warn_if_vacuous(label, &p, &s, c);
}
