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
//! What can go wrong.
//!   * DCN1's two switches break at nearly the same moment. DCN1 is cut off
//!     but still running, and it needs two more rounds to notice that
//!     neither landmark answers and to step down. The backup's shortcut
//!     fires a little earlier, so for a short while both are in charge.
//!   * Something wipes out only the "I am alive" notes (noise, an attacker)
//!     while the landmark stays reachable. Without the lease the backup asks
//!     the landmark, gets an answer, and takes over from a perfectly healthy
//!     primary.
//!   * Two switch failures some time apart can still look like one failure
//!     to the backup. The authors worked out how far apart they must be
//!     (about one round, or two rounds for the code as first written) for
//!     their own version of the controller code. This example lets you
//!     slide the second failure along the clock and find the exact gap for
//!     the version it checks, which differs in one detail that matters
//!     here: when the backup hears about a new landmark, it does not
//!     restart that network's "rounds without news" count. The gaps found
//!     here are therefore not the authors' numbers.
//!   * The lease lasts exactly as long as the primary needs to give up. If
//!     both of DCN1's cables are cut while the "I am alive" notes are also
//!     being wiped out, the backup can earn its three answers at the very
//!     moment DCN1 steps down, and for that moment both are in charge (a
//!     scenario the papers did not check; see the verdicts below).
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
//! Four threads. `main` is a monitor (harness only, see below). DCN1 and
//! DCN2 run the node code of NRPFD.rebeca / LeasingNRPFD.rebeca. SW is the
//! DCN1-side switch pair A1 + B1: the initial NRP (A1), the second NRP
//! candidate (B1), forwarding, fault state and, for the leasing variant,
//! the per-switch lease memory (`primary`, `which`, `prevWhich`). The relay
//! switches A2, A3, B2, B3 hold no state except "failed" and never fail
//! here, so they fold into transit: a DCN1<->SW leg is one hop [L, U] and a
//! SW<->DCN2 leg is three hops [3L, 3U] (A1 -> A2 -> A3 -> DCN2). A
//! heartbeat therefore travels 4 hops, the Rebeca chain exactly at
//! L = U = 1 (Rebeca's networkDelay = 1).
//!
//! Timers are self-messages, exactly like Rebeca's `self.runMe()
//! after(heartbeat_period)` and `ping_timed_out() after(ping_timeout)`:
//! `send_msg_timed(me, Tick, H, H)` and `send_msg_timed(me, PingTimeout, P,
//! P)`. Every actor (rebec) is a reactive loop over ONE blocking timed
//! receive and dispatches on the message, like a Rebeca message server.
//! Self-sends are made in firing order (the ping timeout before the next
//! tick) because arrivals on one channel keep send order. With sd = 0 every
//! message is handled exactly at its arrival instant and same-instant
//! arrivals from different senders are explored in both orders (Rebeca's
//! arbitrary same-time handler order, the "+1" of the GulFest model
//! comment).
//!
//! Failures. SW self-sends one environment notice per failure (and per
//! attack edge) at t = 0 with exact transit to its instant, and judges
//! every message at the instant it reads it. `--fault switch` is Table 1
//! case 7: a failed switch drops everything that reaches it, including the
//! backup's pings to it. `--fault link` cuts DCN1's uplink on that network
//! instead: DCN1's traffic on it is dropped, but the switch (and the NRP)
//! stays alive for DCN2. `--attack-from-ratio` / `--attack-until-ratio` is
//! case 8: SW drops heartbeats on both networks in that window.
//!
//! Dual-primary detection without a clock read. Each DCN reports every
//! role change to `main` with zero transit, at the instant it happens.
//! `main`'s storage lifetime is forced to 0 (`with_node_sd(main_thread_id(),
//! 0)`, a Config setting, timed mode only), so it reads every report at
//! exactly its instant, same-instant reports in both orders. A dual flag is
//! set when both DCNs are PRIMARY, and the assertion is checked only at the
//! END of the run, after both DCNs have finished (see the eviction guard).
//!
//! The run is bounded: each DCN handles R ticks (rounds 0..R-1), then an
//! EndOfRun self-message at R*H; messages read after that are ignored.
//! Because a stepped-down DCN1 never returns, a hold in which DCN1 stepped
//! down in EVERY completed execution (stepdowns = completed) covers every
//! instant at which both could be primary; the binary warns otherwise.
//!
//! ## Deviations from the paper (deliberate)
//!
//!   * Six switch actors become one SW thread (A1 + B1) plus transit. Only
//!     A1 and B1 can fail, so the switch-position term iN of GulFest Eq. 1
//!     (a failure at the second or third switch) is not modelled; the
//!     separation study below is for failures at DCN1's own switches
//!     (i = j = 1).
//!   * The heartbeats of one instant (one per network) travel as ONE
//!     message; SW judges each network separately (A1 for A, B1 for B) and
//!     forwards what survived. For failures at the same instant SW explores
//!     both notice orders (`nondet`), so either network alone can lose a
//!     heartbeat that arrives at that instant. Lost: at L < U, a pair whose
//!     two copies would arrive on opposite sides of a failure instant (only
//!     when 0 < |sep| <= U - L) or of a DCN2 tick (only when a tick instant
//!     falls inside the pair's arrival window, e.g. with phase offsets).
//!     This halves the exploration cost.
//!   * Per-hop fixed delays become intervals, and Rebeca's fixed ping send
//!     offsets (`after(5)` for the primary, `after(15)` for the leasing
//!     backup) and the zero-delay `new_NRP` are replaced by ordinary hops.
//!     The primary's ping still reaches the NRP before the backup's ping of
//!     the same tick whenever U < 3L (1 hop against 3 hops).
//!   * Startup (WAITING -> PRIMARY/BACKUP, the first new_NRP) is skipped:
//!     DCN1 starts PRIMARY with NRP A1 agreed, DCN2 BACKUP, both ticking at
//!     t = 0 unless `--phase-ratio` offsets one of them. Rebeca's first
//!     PRIMARY round starts at H, so its failure instant 2H + P (2500) is
//!     H + P here (the default), and its dual window [4000, 4500) is
//!     [3H, 3H + P) here. As in the leasing file's `init` guard, a
//!     stepped-down (WAITING) DCN1 never returns; in NRPFD.rebeca it would
//!     re-promote at its next runMe.
//!   * `--promote-rule` selects which node the backup runs.
//!     `takeover` (default, ETFA Algorithm 3 and the GulFest
//!     `become_primary_on_ping_response` flag) promotes on an answered ping
//!     only if the ping was sent because BOTH counters were over the limit.
//!     `any` is the MARS node verbatim: NRPFD.rebeca's `ping_timed_out`
//!     BACKUP branch promotes whenever the ping was answered
//!     (`if (ping_pending) ping_pending = false; else { mode = PRIMARY; }`)
//!     and LeasingNRPFD.rebeca does the same behind the lease check
//!     (`if (which > 1) { mode = PRIMARY; }`), in both cases including the
//!     one-network ping. `any` is therefore strictly more eager to promote;
//!     see the `--promote-rule any` verdicts below.
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
//!     goes FAILED. The reset alone changes the separation verdicts: with a
//!     temporary patch of this file that adds only the reset (not a flag of
//!     this binary; run 2026-09-16, R = 5), (tA, sep) = (11, 40), (12, 39),
//!     (16, 35), (21, 30) and (21, 20) turned from FIRE into holds with
//!     stepdowns = completed, while (21, 10) and (31, -20) still FIRE. In
//!     the (11, 40) witness the new_NRP of the NRP-change round reaches
//!     DCN2 at 34; with the reset, network B's count at the tick at 60 is
//!     2, not 3, so the counts differ and the shortcut does not fire. The
//!     separation windows below are therefore for the MARS node and are
//!     not comparable with GulFest's Eq. 5 / Eq. 6.
//!   * A promoted DCN2 is passive: it reports the promotion and (leasing)
//!     sends `new_NRPBack` so the NRP records the new primary, but it sends
//!     no pings or heartbeats afterwards. DCN1 never re-promotes and a
//!     primary ignores heartbeats and lease bits, so this cannot change
//!     whether both are primary at some instant.
//!   * `nrp_timeout`, `request_new_NRP`, FAILED mode, node crashes, the
//!     Backups-Known presence check, operator acknowledgments and the
//!     heartbeat payload are not modelled (dead code in the MARS files, or
//!     present only in the ETFA specification / GulFest node).
//!   * The attacker of case 8 sits in the network (SW drops heartbeats in a
//!     time window) rather than inside the primary: same observable effect,
//!     primary code identical across scenarios.
//!   * `--fault link` is new. Under switch failure the NRP itself dies, no
//!     backup ping is ever answered and the lease is never consulted; a
//!     link cut (the partition of MARS Figure 3) is what exercises it.
//!   * Afra honours `@Priority` (switches before nodes at the same instant);
//!     TraceForge explores every same-instant order, so a tie that Afra's
//!     priorities would hide is reported here. The ties reported below are
//!     between handlers of the two nodes (equal priority in Afra), between
//!     a failure and a message at a switch (the ordering GulFest's "+1"
//!     accounts for), or between a heartbeat and a tick at DCN2.
//!   * With `--sd-ratio` > 0 a handler may run up to sd after its message
//!     arrives (handler latency). Two effects follow. Timers drift from
//!     round to round. And SW's failure and attack notices, being
//!     self-messages too, can take effect up to sd after their instant, so
//!     a message that reaches a switch just after its failure instant may
//!     still be forwarded (see the sd = U verdicts): `--fail-a-ratio`,
//!     `--sep-ratio` and the attack instants are then earliest instants,
//!     not exact ones. Rebeca has no such latency; the default sd = 0
//!     reproduces its timing.
//!
//! ## Timeout-validation assumptions (verification harness, NOT the protocol)
//!
//! This model has no finite-wait receive at all: every timer is a
//! self-message, so there is no timeout branch that could fire trivially,
//! and swim_timed.rs's assume_* validation reads are not needed. The
//! corresponding harness hazard here is EVICTION: a blocking timed receive
//! may take a later message from one sender while an earlier, readable
//! message from another sender expires unread. A lost heartbeat or reply is
//! a fake silence and would produce a false dual primary; Rebeca's message
//! bags never lose messages. The guard, which is scaffolding and not
//! protocol:
//!
//!   * every message carries a per-(sender, receiver) sequence number and
//!     every receive does `assume!(seq == expected)`, so an execution that
//!     skipped a message blocks at the next message on that channel;
//!   * every channel ends with a Done message and every actor drains to it
//!     (DCN -> SW Done, SW -> DCN Done, DCN -> main Done), and SW also
//!     drains its own failure notices, so a skipped LAST message is caught
//!     too;
//!   * the dual-primary assertion runs only after both DCN Dones, so no
//!     report escapes before a gap is detected.
//!
//! Reading the counters. `execs` and `blocked` come from the checker,
//! `completed` and the rest from this harness. An execution that violates
//! NoDualPrimary is counted in `blocked`, NOT in `execs`, because the
//! assertion aborts it; the harness counters are bumped just before the
//! assertion, so `completed = execs + violations` and
//! `blocked = evicted + violations`. Quote `execs + blocked` as the number
//! of executions explored, and beware that on a cell where every complete
//! execution violates, `execs` is 0 and the "0 complete executions" warning
//! below fires even though the run did verify something.
//!
//! Costs: runs report blocked > 0 by design (hundreds to thousands of
//! discarded evicting executions per cell). Measured growth with R: roughly
//! quadratic in the no-failure self test (nrp, blocked 38, 90, 164, 263,
//! 390, 740 at R = 2, 3, 4, 5, 6, 8), faster in the attacked leasing cell
//! of case 8 (blocked 220, 569, 1271, 2253 at R = 3, 4, 5, 6). The outcome
//! counters are summed at `main` from the Done summaries, so they count
//! only completed, eviction-free executions. Self test: with no failure and
//! no attack every variant holds (below); a leaking guard would show up
//! there as a false dual primary.
//!
//! ## Property checked
//!
//! NoDualPrimary, verbatim from NRPFD.property:
//! `NoDualPrimary:!(DCN1Primary && DCN2Primary);`, a state assertion over
//! every reachable state. Here: no instant (same-instant order included) at
//! which both DCN1 and DCN2 are PRIMARY. Availability (someone eventually
//! in charge) is not asserted, as in the papers.
//!
//! ## CLI parameters (ratios are over U, like swim_timed)
//!
//!   --mode baseline|timed        default timed (baseline FIREs even with
//!                                no failure: nrp/noopt at R = 3, leasing
//!                                at R = 5; see Expected verdicts)
//!   --variant nrp|noopt|leasing  nrp = NRPFD.rebeca (simultaneous-timeout
//!                                shortcut); noopt = shortcut removed, no
//!                                lease (MARS 4.1 first step); leasing =
//!                                LeasingNRPFD.rebeca (default nrp)
//!   --fault switch|link|none     what fails at the failure instants
//!                                (default switch)
//!   --fail-a-ratio F             network-A failure instant (default H + P,
//!                                Table 1 case 7, see Deviations)
//!   --sep-ratio D                network-B failure at tA + D, D may be
//!                                negative (default 0); an instant past the
//!                                run horizon R*H means "never fails"
//!   --attack-from-ratio T        heartbeats dropped on both networks from T
//!   --attack-until-ratio T       ... until T (default: forever)
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
//!   --sd-ratio SR                sd = round(SR * U) (default 0.0); forced
//!                                to 0 at the monitor
//!   --h-ratio HR                 H = round(HR * U) (default 20, GulFest)
//!   --pt-ratio PR                P = round(PR * U) (default 10, GulFest);
//!                                1 <= P < H required
//!   --promote-rule any|takeover  which backup node to run (default
//!                                takeover = ETFA Algorithm 3; any = the
//!                                MARS node verbatim, see Deviations)
//!   --keep-going                 explore everything after a violation
//!
//! CLI misuse exits 2, distinct from a property violation's 101, so
//! exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
//!
//! ## Expected verdicts (all from runs performed 2026-09-16)
//!
//! Defaults unless stated: H = 20U, P = 10U, max = 2, sd = 0, L = U = 1,
//! phase 0; times below are absolute, (tA, sep) pairs are ratios.
//!
//! Self test, `--fault none`, R = 4: nrp, noopt and leasing all hold
//! (1 execution each; blocked 164, 302, 300). Nothing happens, and the
//! binary warns that the backup never had to decide.
//!
//! Baseline (untimed) FIREs with no failure at all: nrp and noopt at R = 3,
//! leasing at R = 5 (at R = 3 leasing holds over 45205 executions only
//! because too few NRP answers fit in the run: lease_chances 0, the binary
//! warns), and noopt with the default switch failure at R = 4. Without
//! time, three backup ticks can always pass before a heartbeat is
//! delivered. Baseline leasing at the default R = 4 (`--mode baseline
//! --fault none --variant leasing`) did not finish within 120 s: no data.
//!
//! Table 1, case 7 (default cell, `--fault switch`, tA = tB = 30, R = 4):
//!   nrp FIRE. Witness: round 1's heartbeat dropped at 31, pings to A1 (41)
//!     and B1 (61) dropped, DCN2's shortcut promotion at 60, DCN1 steps
//!     down at 70 (MARS Figure 8: 4000 and 4500).
//!   noopt hold (2 executions, takeover_pings 2, stepdowns 2).
//!   leasing hold, lease-vacuous: backup_replies 0 because the NRP is dead
//!     (the binary warns). The same holds with the attacker added
//!     (`--attack-from-ratio 0 --rounds 5`: hold, backup_replies 0, warned),
//!     the closest cell to the LOCAL LeasingNRPFD.rebeca configuration
//!     (attacker built in, switches A1, A2, B1 and B2 fail at 2500; the
//!     A2/B2 failures cannot be expressed here), whose "satisfied" would
//!     therefore not exercise the lease. That caveat is about the local
//!     file only: the model published in MARS Appendix C has every failtime
//!     0 (attacker only, "Afra created 15891 states ... the assertion is
//!     satisfied"), which is the case 8 leasing cell below.
//! Table 1, case 8 (`--fault none --attack-from-ratio 0`, R = 4):
//!   nrp FIRE (shortcut at 40, DCN1 healthy), noopt FIRE (takeover ping
//!   answered at 46, promotion at 50, DCN1 healthy). Leasing holds, but
//!   at R = 4 the lease could never have granted the role: it needs at
//!   least 3 NRP answers to the backup (the first one always resets, the
//!   ping before it at the NRP being the primary's), and only 2 takeover
//!   pings fit (takeover_pings 2, backup_replies 2, lease_resets 2,
//!   lease_chances 0; the binary warns; R = 3 likewise). Non-vacuous from
//!   R = 5: R = 5 hold (takeover_pings 3, backup_replies 3, lease_resets 3,
//!   lease_chances 1 = completed), R = 6 hold (4, 4, 4, lease_chances 1).
//!   Every answer is a lease reset: a primary ping reaches A1 before each
//!   backup ping. This is the published Leasing NRP FD scenario.
//!
//! Link cut (`--fault link --fail-a-ratio 10 --rounds 5`): nrp FIRE; noopt
//! FIRE at a same-instant tie (answered takeover ping, promotion at 50 at
//! the instant DCN1 steps down); leasing hold, non-vacuous: promotions 2
//! (at 90, after DCN1 stepped down at 50), backup_replies 6, lease_resets
//! 2, lease_chances 2 = completed. Leasing also holds with the cut at 1
//! (R = 5) and 21 (R = 6) (6 executions each, promotions 6, lease_chances
//! 6) and at phase -2 (8 executions, promotions 8, lease_chances 8), all
//! promotions after the step-down.
//!
//! THE MARS NODE IS MORE EXPOSED THAN THE ETFA NODE (`--promote-rule`,
//! runs of 2026-09-18). A 252-cell grid over variant x fault x tA x sep x
//! attack x R was run under both rules: 236 cells agree, and all 16 that
//! differ are a LINK CUT one round apart (`--fault link --sep-ratio 20`) in
//! the nrp and noopt variants, where `takeover` holds and `any` FIREs.
//! Mechanism (witness, `--fault link --fail-a-ratio 10 --sep-ratio 20`):
//! network A's cut at 10 costs DCN1 its round-1 ping, so at 30 it moves the
//! NRP to B1 -- and that `new_NRP` is dropped at the same instant by B's own
//! cut, so DCN2 still believes the NRP is A1. A1 is alive (a link cut stops
//! only DCN1's traffic), so DCN2's ONE-NETWORK ping to it is answered, and
//! the MARS node promotes on any answered ping. The dual primary is not a
//! same-instant tie: at `--phase-ratio -4 --rounds 5` DCN2 is PRIMARY at 50
//! and DCN1 does not step down until 54, a strict window [50, 54).
//! Boundary at the Rebeca-faithful L = U = 1, H = 20, P = 10, R = 6,
//! tA in {10, 30}: `any` FIREs iff 0 <= sep <= H + 1 (21), `takeover` only
//! for sep in {0, 1}. The H term was confirmed at H = 30 (edge 31). The
//! ETFA guard therefore shrinks the vulnerable separation by a whole round.
//! The window needs the backup's ping answer to beat its ping timer: at
//! U = 2 the round trip is 2 * FAR_HOPS * U = 12, so P = 10 kills the
//! scenario outright and P = 14 restores it (FIRE at sep 20..23 and beyond,
//! no closed form measured in U and P).
//! Leasing holds on EVERY cell of that region under both rules, so the
//! paper's leasing fix already covers this class.
//!
//! THE LEASE IS NOT ENOUGH (new scenario): leasing, `--fault link
//! --attack-from-ratio 0 --fail-a-ratio 50 --rounds 5` FIREs. Witness:
//! DCN1's last answered ping at 41; the backup's takeover pings reach A1
//! at 43 (lease reset: the ping before was the primary's), 63 and 83 (no
//! primary ping in between); the backup promotes at 90, the very instant
//! DCN1 (pings at 61 and 81 lost) steps down. The lease grants the role
//! after two rounds without a primary ping, exactly the primary's own
//! give-up time, with no margin. With DCN1's timers lagging DCN2's by
//! delta = -phase: FIRE iff 0 <= delta <= 3U - L. L = U = 1: phase -3 hold
//! (stepdowns 2 = completed, lease_chances 2), -2 FIRE, -1 FIRE with a
//! strict dual window [90, 91), 0 FIRE (tie), +1 hold (promotions 2, after
//! the step-down). L = 0, U = 1: phase -3 FIRE, -4 hold (stepdowns 4 =
//! completed). Cut instant (phase 0): 40 hold (DCN1's last answered ping
//! then precedes the backup's first; promotions 2, after the step-down),
//! 41, 42 and 50 FIRE, 70 FIRE (R = 6).
//!
//! Separation between the two failures (`--variant nrp --fault switch`,
//! tB = tA + sep, J = U - L), for the MARS node only (see Deviations: not
//! comparable with GulFest's Eq. 5 / Eq. 6). Over all failure phases:
//!   hb-on-nrp-change off: dual primary reachable iff
//!     -(H + J) <= sep <= 2H + J;
//!   hb-on-nrp-change on: iff -(H + J) <= sep <= P + J.
//! Evidence at L = U = 1 (sweeps run every tA in one period):
//!   off: sep 40 FIREs at tA = 11 only (tA = 0..20, R = 5), sep 41 holds
//!     at all 21 phases; sep -20 FIREs at tA = 31 only, sep -21 holds at
//!     all tA = 21..40 (R = 4) and at tA = 41 (R = 5; at R = 4 that cell
//!     warns that DCN1 did not step down in every execution). Witness at
//!     (11, 40): SW forwards round 0's heartbeat at 11 just before A's
//!     failure notice of the same instant, the NRP-change round 1 sends no
//!     heartbeat (the new_NRP reaches DCN2 at 34 and resets nothing), B's
//!     round-2 heartbeat is lost at 51, shortcut at 60 with both counts at
//!     3, DCN1 down at 70. The FIRE set is not monotone in sep (R = 5): at
//!     tA = 21, sep 10 FIRE, 11 and 19 hold, 20 and 30 FIRE, 31 hold; at
//!     tA = 16, 0 and 24 hold, 25 and 35 FIRE, 36 hold; sep 30 FIREs
//!     exactly at tA in {0, 1, 11..20}.
//!   on: sep 10 FIREs at tA = 1 only and sep 11 holds at all phases (R = 4);
//!     sep 20, 30 and 40 hold at all 21 phases (R = 5); sep -20 FIREs at
//!     tA = 31 only, -21 holds at all phases (R = 4, tA = 41 at R = 5).
//! Jitter. L = 0, U = 1: off (10, 41) FIRE, sep 42 holds at all tA 0..20
//!   (R = 5), (9, 42) hold, (31, -21) FIRE, sep -22 holds at all tA 22..41
//!   (R = 4, tA = 40 and 41 at R = 5); on (20, 11) FIRE, sep 12 holds at
//!   all tA 0..20, (31, -21) FIRE, sep -22 holds at all tA 22..41 (R = 4,
//!   tA = 40 and 41 at R = 5). U = 2, L = 1: off (10.5, 40.5) FIRE,
//!   (10.5, 41) hold, (31, -20.5) FIRE, (31, -21) hold; on (20.5, 10.5)
//!   FIRE, (20.5, 11) hold. U = 2, L = 2 reproduces the U = 1 verdicts and execution counts
//!   at the same ratios ((11, 40) FIRE, (11, 40.5) hold, (31, -20) FIRE,
//!   (31, -20.5) hold; on (21, 10) FIRE, (21, 10.5) hold).
//! GulFest. These windows say nothing about min_interval = 23 in the local
//!   GulFest file: its node resets the missed count on new_NRP, and with
//!   that reset added the A-first FIREs tried at sep 20 to 40 become holds
//!   (see Deviations). By the authors' own statement, their first
//!   implementation (the one the local GulFest file still contains: no
//!   heartbeat in the NRP-change round) satisfies Eq. 6 (|tB - tA| >=
//!   2H + 2N, 44 at H = 20, N = 1) and a slightly modified implementation
//!   satisfies Eq. 5 (|tB - tA| >= H + 2N), from which 23 = H + 2N + 1 was
//!   derived.
//! With sd = U, off FIREs at (11, 41) and (11, 43) (R = 5), beyond the
//!   sd = 0 bound 2H + J = 40. The (11, 43) witness shows both sd effects
//!   of Deviations: SW reads DCN1's round-0 heartbeat at 12 before the
//!   network-A failure notice that arrived at 11 and forwards it on both
//!   networks (the failure took effect 1 late), and DCN1's timers lag (its
//!   round-2 heartbeat, sent at 53, meets B's failure at 54 and is lost);
//!   shortcut at 60, DCN1 down at 73. No closed form measured.
//!
//! Case 7 with DCN2's ticks offset (0 <= phase < H): FIRE iff phase <= P
//! (dual window [3H + phase, 3H + P]) or phase >= P + 4L (round 0's
//! heartbeat can then reach DCN2 at its first tick, so DCN2 counts one
//! round early). L = U = 1: 10 FIRE, 11 and 13 hold, 14 FIRE. U = 2, L = 1:
//! ratio 10 FIRE, 10.5, 11 and 11.5 hold, 12 FIRE. L = 0: 10 and 11 FIRE.
//!
//! Every timed FIRE prints a certified witness timeline; the witnesses
//! quoted above were read and show the mechanism described, with every
//! message read at its arrival instant (sd = 0) and no sequence gap. Cells
//! take 0.1 s to 35 s.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats};

// Outcome counters, summed at `main` from the Done summaries of completed
// (eviction-free) executions only. Reset before each verify, read after.
static COMPLETED: AtomicUsize = AtomicUsize::new(0);
static DUAL_PRIMARY: AtomicUsize = AtomicUsize::new(0);
static STEPDOWNS: AtomicUsize = AtomicUsize::new(0);
static PROMOTIONS: AtomicUsize = AtomicUsize::new(0);
static SHORTCUT_PROMOTIONS: AtomicUsize = AtomicUsize::new(0);
static TAKEOVER_PINGS: AtomicUsize = AtomicUsize::new(0);
static BACKUP_REPLIES: AtomicUsize = AtomicUsize::new(0);
static LEASE_RESETS: AtomicUsize = AtomicUsize::new(0);
// Executions in which the backup got at least LEASE_MIN_ANSWERS NRP answers,
// so the lease could have granted it the role at all.
static LEASE_CHANCES: AtomicUsize = AtomicUsize::new(0);
static NRP_CHANGES: AtomicUsize = AtomicUsize::new(0);
static SW_DROPS: AtomicUsize = AtomicUsize::new(0);

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

// Actor indices (also the `from` field of every message).
const MAIN: usize = 0;
const DCN1: usize = 1;
const DCN2: usize = 2;
const SW: usize = 3;
const ACTORS: usize = 4;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    /// Table 1 case 7: the switch dies (drops everything, NRP included).
    Switch,
    /// DCN1's uplink on that network is cut; the switch stays alive.
    Link,
}

impl Fault {
    fn name(self) -> &'static str {
        match self {
            Fault::None => "none",
            Fault::Switch => "switch",
            Fault::Link => "link",
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

/// Environment events SW sends to itself at t = 0, each timed to its
/// instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Env {
    Down(usize),
    AttackOn,
    AttackOff,
}

/// Outcome tallies of one actor, shipped in Done messages.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Summary {
    stepdowns: u32,
    promotions: u32,
    shortcut: u32,
    takeover_pings: u32,
    backup_replies: u32,
    lease_resets: u32,
    nrp_changes: u32,
    sw_drops: u32,
}

impl Summary {
    fn add(&mut self, o: Summary) {
        self.stepdowns += o.stepdowns;
        self.promotions += o.promotions;
        self.shortcut += o.shortcut;
        self.takeover_pings += o.takeover_pings;
        self.backup_replies += o.backup_replies;
        self.lease_resets += o.lease_resets;
        self.nrp_changes += o.nrp_changes;
        self.sw_drops += o.sw_drops;
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Body {
    /// main -> SW: the DCN thread ids.
    Init { dcn1: ThreadId, dcn2: ThreadId },
    /// Self timer: Rebeca runMe().
    Tick { k: u32 },
    /// Self timer: Rebeca ping_timed_out().
    PingTimeout,
    /// Self timer: end of the bounded run (harness).
    EndOfRun,
    /// Self timer at SW: failure / attack instant.
    Env(Env),
    /// DCN -> SW: pingNRP to the NRP on network `net`.
    Ping { net: usize },
    /// SW -> DCN: pingNRP_response(which, prevWhich).
    PingReply { w: bool, pw: bool },
    /// DCN1 -> SW -> DCN2: the heartBeats of one instant; `mask[n]` says
    /// whether network n's heartbeat is (still) on its way.
    Heartbeat { mask: [bool; 2] },
    /// DCN1 -> SW -> DCN2: new_NRP over network `net`.
    NewNrp { net: usize },
    /// DCN2 -> SW (leasing): new_NRPBack, the NRP records the new primary.
    NewNrpBack { net: usize },
    /// DCN -> main: role change at this instant (harness).
    RoleChange { role: Role },
    /// End-of-run drain marker carrying the sender's tallies (harness).
    Done { summary: Summary },
}

/// Every message: sender, per-(sender, receiver) sequence number, body.
#[derive(Clone, Debug, PartialEq)]
struct Msg {
    from: usize,
    seq: u32,
    body: Body,
}

#[derive(Clone, Copy, Debug)]
struct Params {
    variant: Variant,
    fault: Fault,
    promote_rule: PromoteRule,
    hb_on_nrp_change: bool,
    rounds: u32,
    max_missed: u32,
    u: u64,
    l: u64,
    sd: u64,
    h: u64,
    p: u64,
    fail_a: Option<u64>,
    fail_b: Option<u64>,
    attack_from: Option<u64>,
    attack_until: Option<u64>,
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
    /// Last instant any DCN handles a message that matters (its EndOfRun).
    fn horizon(&self) -> u64 {
        self.rounds as u64 * self.h + self.phase.unsigned_abs()
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
    /// Exact delay: timers and reports.
    Exact(u64),
}

// =====================================================================
// Channels with the eviction guard (verification harness, see header)
// =====================================================================

struct Chan {
    me: usize,
    ids: [Option<ThreadId>; ACTORS],
    out: [u32; ACTORS],
    inn: [u32; ACTORS],
}

impl Chan {
    fn new(me: usize, ids: [Option<ThreadId>; ACTORS]) -> Self {
        Self { me, ids, out: [0; ACTORS], inn: [0; ACTORS] }
    }

    /// Send `body` to actor `to` with the transit of `leg`. The transit
    /// override has no effect in baseline mode (same program both modes).
    fn send(&mut self, p: &Params, to: usize, leg: Leg, body: Body) {
        let (lo, hi) = match leg {
            Leg::Near => (p.l, p.u),
            Leg::Far => (FAR_HOPS * p.l, FAR_HOPS * p.u),
            Leg::Exact(d) => (d, d),
        };
        let m = Msg { from: self.me, seq: self.out[to], body };
        self.out[to] += 1;
        let tid = self.ids[to].expect("send to an unknown actor");
        traceforge::send_msg_timed(tid, m, lo, hi);
    }

    /// The single reactive receive of every actor. Harness: an execution
    /// in which an earlier message on this channel was evicted unread
    /// blocks here (the seq gap), instead of acting on a fake silence.
    fn recv(&mut self) -> (usize, Body) {
        let m: Msg = traceforge::recv_msg_block_timed();
        let from = m.from;
        traceforge::assume!(m.seq == self.inn[from]);
        self.inn[from] += 1;
        (from, m.body)
    }
}

// =====================================================================
// Controller node (DCN1 starts PRIMARY, DCN2 starts BACKUP)
// =====================================================================

fn dcn(node: usize, p: Params, main_tid: ThreadId, sw_tid: ThreadId) {
    let me = thread::current().id();
    let mut ids = [None; ACTORS];
    ids[MAIN] = Some(main_tid);
    ids[SW] = Some(sw_tid);
    ids[node] = Some(me);
    let mut ch = Chan::new(node, ids);
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
    let mut ended = false;
    let mut sum = Summary::default();
    let max = p.max_missed;

    ch.send(&p, node, Leg::Exact(p.first_tick(node)), Body::Tick { k: 0 });

    loop {
        let (from, body) = ch.recv();
        if ended {
            // Past the horizon: drain until SW's Done, act on nothing.
            if let Body::Done { summary } = body {
                debug_assert_eq!(from, SW);
                sum.add(summary);
                ch.send(&p, MAIN, Leg::Exact(0), Body::Done { summary: sum });
                return;
            }
            continue;
        }
        match body {
            Body::Tick { k } => {
                match role {
                    Role::Primary if node == DCN1 => {
                        // runMe, PRIMARY: ping the NRP, arm ping_timed_out.
                        ping_pending = true;
                        ch.send(&p, SW, net_leg, Body::Ping { net: nrp_net });
                        ch.send(&p, node, Leg::Exact(p.p), Body::PingTimeout);
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
                                sum.shortcut += 1;
                                promote(&mut ch, &p, &mut role, &mut missed, &mut sum, nrp_net);
                            } else {
                                clamp(&mut missed, max);
                                ping_pending = true;
                                takeover_ping = true;
                                sum.takeover_pings += 1;
                                ch.send(&p, SW, net_leg, Body::Ping { net: nrp_net });
                                ch.send(&p, node, Leg::Exact(p.p), Body::PingTimeout);
                            }
                        } else if over_a || over_b {
                            // One network silent: check the NRP only if it
                            // sits on the silent network.
                            if (nrp_net == NET_A && over_a) || (nrp_net == NET_B && over_b) {
                                ping_pending = true;
                                takeover_ping = false;
                                ch.send(&p, SW, net_leg, Body::Ping { net: nrp_net });
                                ch.send(&p, node, Leg::Exact(p.p), Body::PingTimeout);
                            }
                            clamp(&mut missed, max);
                        }
                    }
                    Role::Waiting => {}
                }
                // Next runMe (sent after the ping timer: firing order).
                if k + 1 < p.rounds {
                    ch.send(&p, node, Leg::Exact(p.h), Body::Tick { k: k + 1 });
                } else {
                    ch.send(&p, node, Leg::Exact(p.h), Body::EndOfRun);
                }
            }
            Body::PingTimeout => match role {
                Role::Primary if node == DCN1 => {
                    if ping_pending {
                        // No answer: move to the next NRP candidate, or
                        // give up when none is left.
                        nrp_net += 1;
                        if nrp_net < 2 {
                            sum.nrp_changes += 1;
                            ch.send(&p, SW, Leg::Near, Body::NewNrp { net: nrp_net });
                            if p.hb_on_nrp_change {
                                ch.send(&p, SW, Leg::Near, Body::Heartbeat { mask: [true, true] });
                            }
                        } else {
                            role = Role::Waiting;
                            sum.stepdowns += 1;
                            ch.send(&p, MAIN, Leg::Exact(0), Body::RoleChange { role });
                        }
                    } else {
                        ch.send(&p, SW, Leg::Near, Body::Heartbeat { mask: [true, true] });
                    }
                }
                Role::Backup => {
                    if ping_pending {
                        ping_pending = false;
                    } else if (p.promote_rule == PromoteRule::Any || takeover_ping)
                        && (!p.variant.lease() || which > 1)
                    {
                        promote(&mut ch, &p, &mut role, &mut missed, &mut sum, nrp_net);
                    }
                    takeover_ping = false;
                }
                _ => {}
            },
            Body::PingReply { w, pw } => match role {
                Role::Primary => ping_pending = false,
                Role::Backup => {
                    sum.backup_replies += 1;
                    if p.variant.lease() {
                        if !w && !pw {
                            which += 1;
                        } else {
                            which = 0;
                            sum.lease_resets += 1;
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
            Body::Heartbeat { mask } => {
                if role == Role::Backup {
                    for net in [NET_A, NET_B] {
                        if mask[net] {
                            missed[net] = 0;
                        }
                    }
                }
            }
            Body::NewNrp { net } => {
                // NRPFD.rebeca: adopt the new NRP only. GulFest's node also
                // resets missed[net] here (see Deviations).
                nrp_net = net;
            }
            Body::EndOfRun => {
                ended = true;
                ch.send(&p, SW, net_leg, Body::Done { summary: Summary::default() });
            }
            m => panic!("DCN{node}: unexpected {m:?} from actor {from}"),
        }
    }
}

fn clamp(missed: &mut [u32; 2], max: u32) {
    for m in missed.iter_mut() {
        *m = (*m).min(max + 2);
    }
}

fn promote(
    ch: &mut Chan,
    p: &Params,
    role: &mut Role,
    missed: &mut [u32; 2],
    sum: &mut Summary,
    nrp_net: usize,
) {
    *role = Role::Primary;
    *missed = [0, 0];
    sum.promotions += 1;
    ch.send(p, MAIN, Leg::Exact(0), Body::RoleChange { role: Role::Primary });
    if p.variant.lease() {
        ch.send(p, SW, Leg::Far, Body::NewNrpBack { net: nrp_net });
    }
}

// =====================================================================
// SW: switches A1 (initial NRP) and B1 (second candidate)
// =====================================================================

fn switch(p: Params, main_tid: ThreadId) {
    // Setup handshake (blocking read at t = 0, sender-filtered).
    let init: Msg = traceforge::recv_tagged_msg_block_timed(move |s, _tag| s == main_tid);
    let (dcn1, dcn2) = match init.body {
        Body::Init { dcn1, dcn2 } => (dcn1, dcn2),
        m => panic!("SW: expected Init, got {m:?}"),
    };
    let me = thread::current().id();
    let mut ch = Chan::new(SW, [Some(main_tid), Some(dcn1), Some(dcn2), Some(me)]);
    ch.inn[MAIN] = 1;

    // Environment notices, sent in firing order; instants past the
    // horizon never happen within the run and are not scheduled.
    let mut notices: Vec<(u64, Env)> = Vec::new();
    if let (Some(ta), Some(tb)) = (p.fail_a, p.fail_b) {
        // Same-instant failures: explore both notice orders, so a
        // heartbeat read between them can lose either network (the two
        // per-network heartbeats travel as one message, see Deviations).
        let b_first = ta == tb && traceforge::nondet();
        if b_first {
            notices.push((tb, Env::Down(NET_B)));
            notices.push((ta, Env::Down(NET_A)));
        } else {
            notices.push((ta, Env::Down(NET_A)));
            notices.push((tb, Env::Down(NET_B)));
        }
    }
    if let Some(t) = p.attack_from {
        notices.push((t, Env::AttackOn));
        if let Some(t2) = p.attack_until {
            notices.push((t2, Env::AttackOff));
        }
    }
    notices.retain(|(t, _)| *t <= p.horizon());
    notices.sort_by_key(|(t, _)| *t);
    let mut notices_left = notices.len();
    for (t, e) in notices {
        ch.send(&p, SW, Leg::Exact(t), Body::Env(e));
    }

    let mut down = [false; 2];
    let mut attack = false;
    // Lease memory per NRP switch (LeasingNRPFD.rebeca Switch statevars).
    let mut primary: [Option<usize>; 2] = [Some(DCN1), None];
    let mut which = [true; 2];
    let mut dones = [false; ACTORS];
    let mut sum = Summary::default();

    loop {
        let (from, body) = ch.recv();
        // Is a message of `from` on network `net` dropped at this instant?
        let blocked = |from: usize, net: usize, down: &[bool; 2]| match (p.fault, from) {
            (Fault::None, _) => false,
            (Fault::Switch, _) => down[net],
            (Fault::Link, DCN1) => down[net],
            (Fault::Link, _) => false,
        };
        let leg_to = |node: usize| if node == DCN1 { Leg::Near } else { Leg::Far };
        match body {
            Body::Env(e) => {
                notices_left -= 1;
                match e {
                    Env::Down(net) => down[net] = true,
                    Env::AttackOn => attack = true,
                    Env::AttackOff => attack = false,
                }
            }
            Body::Ping { net } => {
                if blocked(from, net, &down) {
                    sum.sw_drops += 1;
                } else {
                    // pingNRP at the NRP: prevWhich = which;
                    // which = (sender == primary); reply both.
                    let pw = which[net];
                    let w = primary[net] == Some(from);
                    which[net] = w;
                    ch.send(&p, from, leg_to(from), Body::PingReply { w, pw });
                }
            }
            Body::Heartbeat { mask: sent } => {
                // Judge each network's heartbeat separately (A1 for A,
                // B1 for B), then forward what survived as one message.
                let mut mask = [false; 2];
                for net in [NET_A, NET_B] {
                    if !sent[net] {
                        continue;
                    }
                    if blocked(from, net, &down) || attack {
                        sum.sw_drops += 1;
                    } else {
                        mask[net] = true;
                    }
                }
                if mask[NET_A] || mask[NET_B] {
                    ch.send(&p, DCN2, Leg::Far, Body::Heartbeat { mask });
                }
            }
            Body::NewNrp { net } => {
                if blocked(from, net, &down) {
                    sum.sw_drops += 1;
                } else {
                    primary[net] = Some(from);
                    ch.send(&p, DCN2, Leg::Far, Body::NewNrp { net });
                }
            }
            Body::NewNrpBack { net } => {
                if blocked(from, net, &down) {
                    sum.sw_drops += 1;
                } else {
                    primary[net] = Some(from);
                }
            }
            Body::Done { .. } => dones[from] = true,
            m => panic!("SW: unexpected {m:?} from actor {from}"),
        }
        if dones[DCN1] && dones[DCN2] && notices_left == 0 {
            ch.send(&p, DCN1, Leg::Near, Body::Done { summary: Summary::default() });
            ch.send(&p, DCN2, Leg::Far, Body::Done { summary: sum });
            return;
        }
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, p: &Params, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_progress_report(usize::MAX);
    if keep_going {
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        // The monitor reads every role report at exactly its instant.
        Mode::Timed => builder
            .with_timed(p.l, p.u, p.sd)
            .with_node_sd(thread::main_thread_id(), 0)
            .build(),
    }
}

fn reset_counts() {
    for c in [
        &COMPLETED,
        &DUAL_PRIMARY,
        &STEPDOWNS,
        &PROMOTIONS,
        &SHORTCUT_PROMOTIONS,
        &TAKEOVER_PINGS,
        &BACKUP_REPLIES,
        &LEASE_RESETS,
        &LEASE_CHANCES,
        &NRP_CHANGES,
        &SW_DROPS,
    ] {
        c.store(0, Ordering::Relaxed);
    }
}

fn run(mode: Mode, p: Params, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, &p, keep_going);
    reset_counts();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let main_tid = thread::current().id();
        let sw_h = thread::spawn(move || switch(p, main_tid));
        let sw_tid = sw_h.thread().id();
        let d1 = thread::spawn(move || dcn(DCN1, p, main_tid, sw_tid));
        let d2 = thread::spawn(move || dcn(DCN2, p, main_tid, sw_tid));
        let (d1_tid, d2_tid) = (d1.thread().id(), d2.thread().id());
        let mut ch = Chan::new(MAIN, [Some(main_tid), Some(d1_tid), Some(d2_tid), Some(sw_tid)]);
        ch.send(&p, SW, Leg::Exact(0), Body::Init { dcn1: d1_tid, dcn2: d2_tid });

        // Monitor (harness): replay role changes in time order.
        let mut roles = [Role::Backup; ACTORS];
        roles[DCN1] = Role::Primary;
        let mut dual = false;
        let mut dones = [false; ACTORS];
        let mut total = Summary::default();
        while !(dones[DCN1] && dones[DCN2]) {
            let (from, body) = ch.recv();
            match body {
                Body::RoleChange { role } => {
                    roles[from] = role;
                    if roles[DCN1] == Role::Primary && roles[DCN2] == Role::Primary {
                        dual = true;
                    }
                }
                Body::Done { summary } => {
                    dones[from] = true;
                    total.add(summary);
                }
                m => panic!("main: unexpected {m:?} from actor {from}"),
            }
        }
        let _ = sw_h.join();
        let _ = d1.join();
        let _ = d2.join();

        COMPLETED.fetch_add(1, Ordering::Relaxed);
        STEPDOWNS.fetch_add(total.stepdowns as usize, Ordering::Relaxed);
        PROMOTIONS.fetch_add(total.promotions as usize, Ordering::Relaxed);
        SHORTCUT_PROMOTIONS.fetch_add(total.shortcut as usize, Ordering::Relaxed);
        TAKEOVER_PINGS.fetch_add(total.takeover_pings as usize, Ordering::Relaxed);
        BACKUP_REPLIES.fetch_add(total.backup_replies as usize, Ordering::Relaxed);
        LEASE_RESETS.fetch_add(total.lease_resets as usize, Ordering::Relaxed);
        if total.backup_replies >= LEASE_MIN_ANSWERS {
            LEASE_CHANCES.fetch_add(1, Ordering::Relaxed);
        }
        NRP_CHANGES.fetch_add(total.nrp_changes as usize, Ordering::Relaxed);
        SW_DROPS.fetch_add(total.sw_drops as usize, Ordering::Relaxed);
        if dual {
            DUAL_PRIMARY.fetch_add(1, Ordering::Relaxed);
        }
        // NoDualPrimary, judged only now that every channel is drained.
        traceforge::assert(!dual);
    });
    (stats, start.elapsed())
}

// =====================================================================
// Reporting
// =====================================================================

#[derive(Clone, Copy, Debug)]
struct Counts {
    completed: usize,
    dual: usize,
    stepdowns: usize,
    promotions: usize,
    shortcut: usize,
    takeover_pings: usize,
    backup_replies: usize,
    lease_resets: usize,
    lease_chances: usize,
    nrp_changes: usize,
    sw_drops: usize,
}

fn read_counts() -> Counts {
    Counts {
        completed: COMPLETED.load(Ordering::Relaxed),
        dual: DUAL_PRIMARY.load(Ordering::Relaxed),
        stepdowns: STEPDOWNS.load(Ordering::Relaxed),
        promotions: PROMOTIONS.load(Ordering::Relaxed),
        shortcut: SHORTCUT_PROMOTIONS.load(Ordering::Relaxed),
        takeover_pings: TAKEOVER_PINGS.load(Ordering::Relaxed),
        backup_replies: BACKUP_REPLIES.load(Ordering::Relaxed),
        lease_resets: LEASE_RESETS.load(Ordering::Relaxed),
        lease_chances: LEASE_CHANCES.load(Ordering::Relaxed),
        nrp_changes: NRP_CHANGES.load(Ordering::Relaxed),
        sw_drops: SW_DROPS.load(Ordering::Relaxed),
    }
}

fn opt(t: Option<u64>) -> String {
    t.map_or_else(|| String::from("-"), |v| v.to_string())
}

fn print_one(label: &str, p: &Params, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<8} variant={v} fault={f} promote={pmr} tA={ta} tB={tb} attack=[{af},{au}) \
         hb_on_change={hb} \
         R={r} max={mx} L={l} U={u} sd={sd} H={h} P={pt} phase={ph}  execs={execs} \
         blocked={block} completed={comp} dual={dual} stepdowns={sdn} promotions={pr} \
         shortcut={sc} takeover_pings={tp} backup_replies={br} lease_resets={lr} \
         lease_chances={lc} nrp_changes={nc} sw_drops={sw} violations={dual} time={dur:?}",
        v = p.variant.name(),
        f = p.fault.name(),
        pmr = p.promote_rule.name(),
        ta = opt(p.fail_a),
        tb = opt(p.fail_b),
        af = opt(p.attack_from),
        au = opt(p.attack_until),
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
        comp = c.completed,
        dual = c.dual,
        sdn = c.stepdowns,
        pr = c.promotions,
        sc = c.shortcut,
        tp = c.takeover_pings,
        br = c.backup_replies,
        lr = c.lease_resets,
        lc = c.lease_chances,
        nc = c.nrp_changes,
        sw = c.sw_drops,
        dur = dur,
    );
}

/// Vacuity guards: an exit-0 run only counts as a hold if the scenario
/// actually bit and the backup actually reached a takeover decision.
fn warn_if_vacuous(label: &str, p: &Params, stats: &Stats, c: Counts) {
    // A violating execution is aborted by the assertion, so the checker
    // files it under `block`, not `execs`. On a cell where EVERY complete
    // execution violates, `execs` is therefore 0 while the run found real
    // counterexamples: that is a FIRE, not "no data".
    if stats.execs == 0 && c.dual == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={}) and no violation: every \
             execution blocked, this run verified nothing; treat it as no data, not as a hold.",
            stats.block
        );
        return;
    }
    if c.dual > 0 {
        return;
    }
    // Once DCN1 has stepped down it never returns, so a run in which it
    // stepped down in EVERY completed execution has covered every instant
    // at which both could be primary.
    let isolating = p.fault != Fault::None
        && [p.fail_a, p.fail_b].iter().all(|t| t.is_some_and(|t| t <= p.horizon()));
    if isolating && c.stepdowns < c.completed {
        println!(
            "WARNING ({label}): DCN1 stepped down in only {} of {} completed executions: \
             the run ended before the failures fully isolated it; increase --rounds \
             before reading this as a hold.",
            c.stepdowns, c.completed
        );
    }
    let dcn1_gone_everywhere = isolating && c.stepdowns == c.completed;
    if !dcn1_gone_everywhere && c.promotions == 0 && c.takeover_pings == 0 {
        println!(
            "WARNING ({label}): the backup never reached a takeover decision (no takeover \
             ping, no promotion): this hold says nothing about dual primaries; increase \
             --rounds or change the scenario."
        );
    }
    if p.variant.lease() {
        if c.backup_replies == 0 {
            println!(
                "WARNING ({label}): the NRP never answered the backup, so the lease was never \
                 consulted: this hold says nothing about the lease (only that the shortcut is \
                 gone)."
            );
        } else if c.lease_chances < c.completed {
            println!(
                "WARNING ({label}): the backup got the {LEASE_MIN_ANSWERS} NRP answers the lease \
                 needs before it can grant the role in only {} of {} completed executions; in \
                 the others the lease could never have promoted it, so this hold is no evidence \
                 for the lease there; increase --rounds.",
                c.lease_chances, c.completed
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
    fault: Fault,
    fail_a_ratio: Option<f64>,
    sep_ratio: f64,
    attack_from_ratio: Option<f64>,
    attack_until_ratio: Option<f64>,
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
        fault: Fault::Switch,
        fail_a_ratio: None,
        sep_ratio: 0.0,
        attack_from_ratio: None,
        attack_until_ratio: None,
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
            "--fault" => {
                a.fault = match next_val(&mut args, "--fault").as_str() {
                    "switch" => Fault::Switch,
                    "link" => Fault::Link,
                    "none" => Fault::None,
                    other => {
                        cli_bail(&format!("invalid --fault: {other} (expected switch|link|none)"))
                    }
                }
            }
            "--fail-a-ratio" => {
                a.fail_a_ratio = Some(parse_num(next_val(&mut args, "--fail-a-ratio"), "--fail-a-ratio"))
            }
            "--sep-ratio" => a.sep_ratio = parse_num(next_val(&mut args, "--sep-ratio"), "--sep-ratio"),
            "--attack-from-ratio" => {
                a.attack_from_ratio =
                    Some(parse_num(next_val(&mut args, "--attack-from-ratio"), "--attack-from-ratio"))
            }
            "--attack-until-ratio" => {
                a.attack_until_ratio = Some(parse_num(
                    next_val(&mut args, "--attack-until-ratio"),
                    "--attack-until-ratio",
                ))
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
                     [--variant nrp|noopt|leasing] [--fault switch|link|none] [--fail-a-ratio F] \
                     [--sep-ratio D] [--attack-from-ratio T] [--attack-until-ratio T] \
                     [--hb-on-nrp-change off|on] [--phase-ratio F] [--rounds R] [--max-missed M] \
                     [--u U] [--l-ratio LR] [--sd-ratio SR] [--h-ratio HR] [--pt-ratio PR] \
                     [--promote-rule any|takeover] [--keep-going]\n\
                     Defaults: timed, nrp, switch fault at tA = H + P, sep 0, no attack, \
                     hb-on-nrp-change off, phase 0, R=4, max=2, U=1, L/U=1, sd/U=0, H/U=20, P/U=10, \
                     promote-rule takeover.\n\
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
    let (fail_a, fail_b) = if a.fault == Fault::None {
        (None, None)
    } else {
        let ta = match a.fail_a_ratio {
            Some(r) => nonneg(r, "the network-A failure instant"),
            None => h + pt,
        };
        if ta > i64::MAX as u64 {
            cli_bail("the network-A failure instant is too large (check --fail-a-ratio)");
        }
        let sep = scale(a.sep_ratio, "the failure separation") as i64;
        let tb = (ta as i64)
            .checked_add(sep)
            .unwrap_or_else(|| cli_bail("tA + sep overflows (check --sep-ratio)"));
        if tb < 0 {
            cli_bail("the network-B failure instant tA + sep must be >= 0 (check --sep-ratio)");
        }
        (Some(ta), Some(tb as u64))
    };
    let attack_from = a.attack_from_ratio.map(|r| nonneg(r, "attack start"));
    let attack_until = a.attack_until_ratio.map(|r| nonneg(r, "attack end"));
    if attack_until.is_some() && attack_from.is_none() {
        cli_bail("--attack-until-ratio needs --attack-from-ratio");
    }
    if let (Some(f), Some(t)) = (attack_from, attack_until) {
        if t <= f {
            cli_bail("the attack must end after it starts");
        }
    }
    let p = Params {
        variant: a.variant,
        fault: a.fault,
        promote_rule: a.promote_rule,
        hb_on_nrp_change: a.hb_on_nrp_change,
        rounds: a.rounds,
        max_missed: a.max_missed,
        u,
        l,
        sd,
        h,
        p: pt,
        fail_a,
        fail_b,
        attack_from,
        attack_until,
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
