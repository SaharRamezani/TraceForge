//! Gas sensors, a scientist and a rescue team: false alarms caused by timing.
//!
//! ## The story in plain words
//!
//! A scientist is working in an area where toxic gas can leak. Two gas
//! sensors watch the air. Every 10 time units each sensor measures the
//! gas and sends the reading over a wireless network to an admin
//! station. A message is supposed to take 1 time unit to travel, but a
//! real network is not that regular: some messages are quicker, some
//! are slower.
//!
//! The admin station has an alarm clock that rings every 15 time units.
//! When it rings, the admin looks at what came in since the last ring.
//! A sensor that sent nothing in that time is written down as broken.
//! If the latest reading from either sensor shows dangerous gas, the
//! admin radios the scientist to stop work and leave, and sets a second
//! alarm 5 time units away.
//!
//! The scientist answers "got it" as soon as the warning arrives. If the
//! second alarm rings and no answer has come back yet, the admin assumes
//! the scientist is in trouble, sends out the rescue team, and sets a
//! third alarm another 5 time units away. The rescue team may be held up
//! by an obstacle for a moment, then radios back "we reached the
//! scientist". If the third alarm rings and that call has not come back
//! yet, the admin writes the scientist down as dead.
//!
//! In this model nothing ever really goes wrong: the sensors never
//! break, the scientist always answers, and the rescue team always gets
//! there. So every "sensor broken" note and every "scientist dead" note
//! is a false alarm. False alarms still matter: a false "broken" sends a
//! technician out for nothing, and a false "dead" means the admin gave up
//! on someone who was fine. Each false alarm here comes purely from
//! timing: when the network is slower or less even than the admin's
//! alarm settings assume, a reading or an answer lands a moment after the
//! admin has already looked.
//!
//! The admin's alarm settings are fixed numbers chosen for a network
//! where every message takes exactly 1 time unit. The 5 units given to
//! the scientist must cover the warning travelling out AND the answer
//! travelling back; the 5 units given to the rescue team must cover the
//! call going out, the obstacle, and the radio call coming back. (In the
//! published model file the rescue team's own program even believes it
//! has 10 time units, while the admin waits only 5.) This example asks:
//! for which network delays, alarm settings and obstacle lengths is a
//! false alarm possible, and for which is it impossible? The checker
//! tries every possible travel time of every message within the given
//! limits, and either prints one concrete timeline that ends in a false
//! alarm, or confirms that none exists.
//!
//! One piece of bookkeeping shows up in the output. The checker may let
//! the admin pick up one message while another one that already arrived
//! is left lying around and later thrown away. Such a run would "see" a
//! missing message that was actually delivered, which is not a real
//! false alarm. So the admin in this model keeps working until it has
//! picked up every message that was sent to it, and a run in which some
//! message could no longer be picked up is thrown away and counted as
//! "blocked". A blocked count above zero is therefore normal.
//!
//! What the checker finds, in short. When every message takes exactly 1
//! time unit, as the model file assumes, no false alarm happens in any
//! of the runs checked. With the published alarm settings, a false
//! "dead" becomes possible as soon as a message may take 3 time units,
//! even when every message takes exactly 3: the answer comes back at 6
//! units after the warning, 1 unit after the admin gave up waiting, and
//! the rescue call comes back 1 unit too late as well. A false "broken"
//! needs slower readings, and whether it happens depends on how the
//! reading times line up with the alarm times and on how many times the
//! alarm rings. If the admin looks only once (the default run), a
//! reading must be able to take 15 time units. Once the alarm has rung
//! at least three times, readings that may take anywhere from 0 to 5
//! time units are enough at the published periods, while a network that
//! is never faster than 1 unit still needs readings as slow as 10 units.
//!
//! The case study comes from Luca Aceto, Matteo Cimini, Anna
//! Ingolfsdottir, Arni Hermann Reynisson, Steinar Hugi Sigurdarson and
//! Marjan Sirjani, "Modelling and Simulation of Asynchronous Real-Time
//! Systems using Timed Rebeca", FOCLASA 2011 (EPTCS volume 58, pages
//! 1-19), Section 5 and Appendix B, Listing 5. Its journal version is
//! Reynisson, Sirjani, Aceto, Cimini, Jafari, Ingolfsdottir and
//! Sigurdarson, "Modelling and simulation of asynchronous real-time
//! systems using Timed Rebeca", Science of Computer Programming (2014).
//! It was later used as a benchmark by Khamespanah, Sirjani, Sabahi
//! Kaviani, Khosravi and Izadi, "Timed Rebeca schedulability and deadlock
//! freedom analysis using bounded floating time transition system",
//! Science of Computer Programming 98 (2015). The numbers above follow
//! the model file published on rebeca-lang.org (TARO case studies,
//! sensornetwork.rebeca).
//!
//! ## Sources consulted
//!
//! * The rebeca-lang.org model file (a local copy under
//!   docs/research/timed rebeca/sensor-network/), read in full: fixed
//!   netDelay = 1, sensor period 10, adminCheckDelay 15,
//!   scientistDeadline 5, Admin rescueDeadline 5, Rescue rescueDeadline
//!   10, obstacle ?(0, 1).
//! * FOCLASA 2011 (arXiv 1108.0228): Section 5 "Sensor Network",
//!   Table 3 (McErlang simulation results) and Listing 5 (the original
//!   model, all timing constants as environment parameters, one shared
//!   rescueDeadline). This is the paper that introduces the case study.
//! * SCP 2015 (Khamespanah et al.), Section 6: "The main property to be
//!   checked is saving Scientist before the rescue deadline is missed."
//! * The SCP 2014 journal version was not consulted (paywalled); the
//!   open FOCLASA version was used instead.
//!
//! No closed-form timing constraint is published. FOCLASA's property:
//! "the scientist must acknowledge ... before scientistDeadline time
//! units have passed; the rescue team must have reached the scientist
//! within rescueDeadline time units. Otherwise we consider the mission
//! failed." In the Admin that failure is exactly `scientistDead`.
//!
//! ## How the protocol maps onto TraceForge
//!
//! Five long-lived threads plus the harness main thread:
//!
//!   sensor i (i < --sensors) --- Report{i, seq, value} ---> Admin
//!   Admin --- AbortPlan{round, admin} ---> Scientist
//!   Scientist --- Ack{round} ---> Admin
//!   Admin --- Go{round, admin} ---> Rescue
//!   Rescue: sleep(obstacle in {0, OBST}), then --- RescueReach ---> Admin
//!   Admin --- Admin: CheckScientistAck{round} (exact transit Ds)
//!   Admin --- Admin: CheckRescue{round}       (exact transit Dr)
//!   main  --- Admin: CheckSensors{j}, j = 1..R (exact transit j*C)
//!
//! Rebeca actors have no explicit receive: each takes the earliest
//! message from its queue and runs that handler to completion in zero
//! time. The Admin is ported the same way, as ONE blocking timed
//! receive loop over every message kind, including its own timers. At
//! sd = 0 a message is readable only at the instant it arrives, so the
//! loop handles messages in arrival order exactly as Rebeca does (ties
//! in either order); at sd > 0 it does not (see Deviations).
//! Rebeca's `self.m() after(d)` becomes a self-send with exact per-send
//! transit `send_msg_timed(me, m, d, d)`; every Rebeca `after(netDelay)`
//! on an inter-actor message becomes a plain send under the global
//! transit window [L, U] (L = U = 1, sd = 0 is the published model).
//! A sensor's `self.doReport() after(period)` becomes `sleep(P)`
//! between two sends. The handler bodies and the Admin's flags
//! (reported, sensorValue, sensorFailure, scientistAck,
//! scientistReached, scientistDead) are Listing 5's, verbatim.
//!
//! No Init message: Scientist and Rescue learn the Admin's id from the
//! payload of AbortPlan/Go, the Admin and the sensors get ids through
//! their spawn closures, so every thread starts at absolute time 0 in
//! the same phase as the Rebeca model.
//!
//! The periodic CheckSensors alarms are pre-armed by main at time 0
//! (transit exactly j*C), instead of each check re-arming the next on
//! the Admin's own channel. TraceForge channels preserve order per
//! sender and receiver pair, so messages one thread sends to one
//! receiver must also ARRIVE in send order. A self-re-arming alarm
//! would share the Admin-to-Admin channel with the mission timers and
//! force, e.g., CheckRescue{j} (sent after CheckSensors{j+1} was armed,
//! due Ds + Dr after check j) to arrive no earlier than CheckSensors{j+1}
//! (due C after check j), which is false whenever Ds + Dr < C, silently
//! deleting real timelines. Rebeca's alarm has no drift, so at sd = 0
//! the pre-armed metronome is the same schedule (at sd > 0 each alarm
//! may be handled up to sd after it rings, see Deviations, but that
//! lateness never carries over to the next alarm). Only mission timers
//! use the Admin's own channel, and the CLI guard below keeps them in
//! send order.
//!
//! SciMsg::Done and RescueMsg::Done are verification harness, not the
//! protocol: they release the two reactive threads once the Admin has
//! finished, so a bounded model terminates.
//!
//! ## Deviations from the paper (deliberate)
//!
//! * netDelay (fixed 1) becomes the global transit window [L, U] plus
//!   storage lifetime sd (how long a message stays readable after it
//!   arrives). The defaults L = U = 1, sd = 0 are the published model;
//!   the question is which jitter breaks the fixed deadlines.
//! * sd > 0 relaxes Rebeca's scheduler rather than reproducing it.
//!   Timed Rebeca may pick a message only if its time tag is the
//!   smallest in the whole message bag (FOCLASA Section 3, side
//!   condition TT <= min(B)), so an idle actor handles each message the
//!   moment it arrives, in arrival order. Here, at sd > 0, every thread
//!   may pick up a message up to sd after it arrived, even after a
//!   message that arrived later. For the Admin this includes its own
//!   alarms and timers: CheckSensors{j}, due at j*C, may be handled as
//!   late as j*C + sd, CheckScientistAck and CheckRescue up to sd after
//!   they ring, and a reading, Ack or RescueReach may be handled after a
//!   timer that rang later than it arrived. Example, the (Ds, Dr, OBST)
//!   = (4, 5, 1) FIRE at U = 1, L = 0, sd = 1, P 20, C 30:
//!   CheckSensors{1}, due 30, is handled at 31; the Scientist picks up
//!   the warning at 33, so its Ack arrives by 34, yet the Admin handles
//!   CheckScientistAck (rang at 35) at 35 and only then the Ack. Only
//!   sd = 0 keeps the Admin's earliest-first order, so the sd = 0
//!   boundaries below are the pure network-jitter results; the sd terms
//!   of both laws are explained where the laws are stated.
//! * The first sensor check is at time C, not at time 0. The model
//!   file's Admin constructor runs checkSensors() at time 0, before any
//!   report can arrive, so sensorFailure is set in EVERY run of the
//!   published model; the paper never checks sensorFailure, so this is
//!   an artifact of the file, not a finding.
//! * Periodic alarms are pre-armed by main (see above).
//! * Missions must not overlap: the CLI requires Ds + Dr + 3sd < C
//!   (every mission, including a late pick-up of its last timer, ends
//!   before the next alarm can be read), exit 2 otherwise. With
//!   --allow-overlap only the channel-order condition Ds + 2sd < C and
//!   Dr + 2sd <= C is required; the FOCLASA "new rescue mission while
//!   another is still ongoing" flag cross-talk then becomes reachable.
//!   The channel-order condition is never relaxed: it is exactly what
//!   keeps every mission timer arriving in send order on the Admin's
//!   own channel (derived from the timer read windows, not measured);
//!   outside it, same-channel arrival order would delete genuine
//!   timelines. FOCLASA Table 3 rows 3-4 (scientistDeadline 4 >= admin
//!   period 1 or 4) are therefore refused.
//! * FOCLASA's headline result is out of reach of this port. Its
//!   discussion centres on Table 3 rows 3-4: "The admin node initiates
//!   a new rescue mission while another is still ongoing", and
//!   "increasing the value of admin sensor-read period above half the
//!   rescue deadline eliminates the flaw". Both rows are refused (see
//!   the previous bullet) because every mission timer is a self-message
//!   on the one Admin-to-Admin channel, whose messages must arrive in
//!   send order. Once missions of consecutive checks interleave, a timer
//!   sent later can be due earlier (e.g. a CheckRescue followed by the
//!   next check's CheckScientistAck), and exact timers would then make
//!   genuine timelines impossible. So neither the flaw nor the proposed
//!   fix is reproduced here. Rows 1-2 are only approximated (one sensor
//!   with one period of 2, not two sensors with periods 2 and 3).
//! * The Rescue's `deadline(rescueDeadline - netDelay)` message expiry
//!   (9 in the model file) is not modeled, and the Rescue's own
//!   rescueDeadline = 10 is not a parameter. The Admin is always ready
//!   to receive, so the RescueReach is taken at most U + sd after it is
//!   sent and the file's expiry cannot bind while U + sd <= 9;
//!   TraceForge has no per-message expiry (sd is global). The CLI does
//!   not check this. Under the ratio scaling used for every other timer
//!   the file's 9 (ten netDelays minus one) reads as 9U, which never
//!   binds while sd <= 8U; read as an absolute 9, it can bind at e.g.
//!   U = 5, sd = 5, where this port keeps a reach the file would drop.
//!   In FOCLASA there is a single shared rescueDeadline; the 5 versus 10
//!   mismatch exists only in the file. FOCLASA's expiry is a point in
//!   time, now() + rescueDeadline - netDelay taken when go() starts
//!   (before the obstacle), which with a fixed netDelay is exactly the
//!   Admin's checkRescue time: there a RescueReach arriving after that
//!   time is purged unread, while here it is read late and sets
//!   scientistReached, which can only suppress a later round's death
//!   verdict, never create one.
//! * Sensors never fail and send a bounded number of readings,
//!   N = floor((R*C + sd) / P) + 2, so the last one is sent strictly
//!   after the last check could be read; Scientist and Rescue always
//!   answer. False alarms by construction (SENSOR_BROKEN and
//!   SCIENTIST_LOST are never set, like TARGET_CRASHED in swim_timed).
//!   The SCP 2015 "sensors stop working" deadlock variant is out of
//!   scope.
//! * Readings default to always dangerous (--readings danger) so every
//!   check that saw a report starts a mission; --readings random is
//!   Rebeca's ?(2, 4) choice per reading, --readings safe disables
//!   missions (fast sensor-only sweeps).
//! * The obstacle is a two-valued choice {0, OBST}, generalizing
//!   Rebeca's ?(0, 1) (a choice between listed values, not a range).
//!   With OBST = 0 the choice is skipped.
//! * At most 2 sensors (five-thread budget; SCP 2015 scaled 1..4), and
//!   one common sensor period (FOCLASA Table 3 uses separate periods).
//! * Round numbers travel in payloads for debugging only; the Admin
//!   keeps Rebeca's round-agnostic flags, so the model's behaviour
//!   (including a stale late Ack suppressing the next mission's rescue,
//!   a missed rescue that is not asserted) is preserved.
//! * Messages arriving at the same instant may be handled in either
//!   order (ties count), and Rebeca's queue capacities are ignored.
//!
//! ## Timeout-validation assumptions (verification harness, NOT the protocol)
//!
//! This model has no finite-wait receive, so there is no timeout
//! branch to validate with swim_timed's inline blocking read. The
//! analogous hazard is the reactor's: its receive may take one message
//! while another already-arrived message is ignored, and with sd = 0
//! the ignored message expires unread. A check could then see "nothing
//! came in" although the report did arrive. The DRAIN closes this: the
//! Admin keeps receiving until it has read EVERY message it knows was
//! sent to it (all N readings per sensor, all R alarms, one Ack per
//! AbortPlan, one timer per self-send, one RescueReach per Go). An
//! ignored message can never be read later, so that execution BLOCKS
//! (counted in blocked) instead of reaching the assertions, which sit
//! after the drain. A surviving sensor verdict therefore implies the
//! sensor's next reading was read after the check, i.e. it could not be
//! read before it; a surviving death verdict implies the Ack was read
//! after CheckScientistAck and the RescueReach after CheckRescue. This
//! is exactly the "blocking read of the awaited message after the
//! deadline" of swim_timed.rs, done for all messages at the end. An
//! inline blocking detour would take the Admin away from its mailbox
//! and, at sd = 0, destroy every message arriving meanwhile, including
//! its own timers, losing genuine false alarms.
//!
//! Early drain cut: every message carries its position in its channel
//! (reading seq, check round, mission round), and the Admin blocks
//! (`assume!(false)`) as soon as it reads one out of its channel's send
//! order. Reading past an unread same-channel message is only possible
//! when that message was already gone before the read began, so such an
//! execution could never finish the drain; cutting it early only saves
//! work. Measured: exec counts and every outcome counter were identical
//! with and without the cut on 7 hold cells, with about 3x fewer
//! blocked executions and 4x less time.
//!
//! Known costs: hold cells report blocked > 0 by design, and blocked
//! counts grow quickly with R and S (R = 3 death cells at S = 1: 10446
//! and 14486 blocked, 16-23 s; the published model at S = 2, R = 2:
//! 12896 blocked, 21 s; S = 2, R = 3 exceeded 120 s). The verdicts are
//! asserted at the end of the Admin, so a FIRE's certified witness
//! timeline covers the whole run (read it to find the late message).
//! The Admin's `LATE_REPORTS` counter uses the send index times P plus
//! L, never a clock, and prunes nothing.
//!
//! ## Properties checked (at the Admin, after the drain)
//!
//!   sensorFailure  => SENSOR_BROKEN    (never set: a false sensor failure)
//!   scientistDead  => SCIENTIST_LOST   (never set: a false death, which is
//!       FOCLASA's "mission failed" with the knowledge that the scientist
//!       was actually fine)
//!
//! --property both|sensor|rescue selects which recorded verdicts are
//! asserted; both are always counted. Same program in both modes; only
//! the Config differs.
//!
//! ## CLI parameters (ratios are over U, like swim_timed)
//!
//!   --mode baseline|timed|compare   default timed (baseline FIREs whenever
//!                                   a property can be violated, so compare
//!                                   usually aborts in its baseline leg;
//!                                   use --keep-going)
//!   --sensors S                     1 or 2 (default 2)
//!   --rounds R                      Admin check periods (default 1)
//!   --u U                           transit upper bound (default 1)
//!   --l-ratio LR                    L = round(LR * U) (default 1.0, the
//!                                   published fixed netDelay)
//!   --sd-ratio SR                   sd = round(SR * U) (default 0.0)
//!   --period-ratio PR               sensor period P (default 10.0)
//!   --check-ratio CR                adminCheckDelay C (default 15.0)
//!   --ack-deadline-ratio AR         scientistDeadline Ds (default 5.0)
//!   --rescue-deadline-ratio RR      Admin rescueDeadline Dr (default 5.0)
//!   --obstacle-ratio OR             obstacle maximum OBST (default 1.0)
//!   --readings danger|random|safe   sensor values (default danger)
//!   --property both|sensor|rescue   asserted verdicts (default both)
//!   --allow-overlap                 relax the mission guard (see above)
//!   --keep-going                    explore past violations and count them
//!                                   (exit 0; see sensor_failures= and dead=)
//!
//! CLI misuse exits 2, distinct from a property violation's 101, so
//! exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
//!
//! ## Expected verdicts (every line below was run, 2026-09-16)
//!
//! Values are ABSOLUTE; the CLI takes ratios over U (e.g. P = 10 at
//! U = 5 is --period-ratio 2). S = --sensors. "Ties count": a message
//! arriving at the very instant a timer rings may be handled after it.
//!
//! baseline: FIRE (exit 101) at the defaults (S = 2 and S = 1), with
//!     --readings safe --property sensor, and with --property rescue
//!     even at timer values where timed holds (Ds = Dr = 3, L = 0): the
//!     untimed checker reads an alarm before the reading, or a timer
//!     before the reply. Only --readings safe --property rescue holds
//!     (no mission, trivially; NOTE printed). --mode compare
//!     --keep-going at the defaults: baseline 61620 execs (38400 false
//!     sensor failures, 30240 false deaths, 7.4 s) vs timed 8 execs,
//!     none (0.23 s); at S = 1: 263 vs 1.
//!
//! timed, published model (L = U = 1, sd = 0, P 10, C 15, Ds = Dr = 5,
//!     OBST 1, danger readings): hold at S = 2 for R in {1, 2} and at
//!     S = 1 for R in {1, 2, 3}. S = 2, R = 3 hit the 120 s cap: no data.
//!
//! timed, FALSE DEATH (S = 1, --readings danger, --property rescue):
//!
//!     FIRE  iff  Ds <= 2U + 2sd  AND  Dr <= 2U + 2sd + OBST
//!
//!   First gate: the warning out and the answer back each take up to U
//!   and may each be picked up sd late, so the Ack can land at or after
//!   the ack timer. Second gate: the rescue call out, the obstacle, and
//!   the radio call back can land at or after the rescue timer. L plays
//!   no role. Both sd terms of each gate are pick-up lateness that Timed
//!   Rebeca does not have (see Deviations): the first is the Scientist
//!   (or the Rescue) taking the Admin's message sd late, the second is
//!   the Admin taking the Ack (or the RescueReach) sd late, after a timer
//!   that rang later than that message arrived. Verified by an automated
//!   grid at R = 1 over U in {1, 2}, L in {0, U}, sd in {0, 1},
//!   OBST in {0, 1, 2}, with Ds and Dr each at
//!   gate - 1, gate, gate + 1 (P 20, C 30): 216 cells, 96 FIRE, 120 hold,
//!   0 disagreements; the same grid at R = 2 for U = 1, L in {0, 1},
//!   OBST in {0, 1}: 72 cells, 0 disagreements. R = 3 (U = 1, L = 0,
//!   sd = 0, P 10, C 15): (Ds, Dr, OBST) = (2, 3, 1) FIRE, (3, 3, 1) hold,
//!   (2, 4, 1) hold; S = 2, R = 1: the same three verdicts; --readings
//!   random, R = 1: the same three verdicts. Individually inspected cells
//!   include U = 1 sd = 1: (4, 5, 1) FIRE, (5, 5, 1) hold, (4, 6, 1) hold,
//!   and U = 2 at L in {0, 1, 2}: (4, 6, 2) FIRE, (5, 6, 2) hold,
//!   (4, 7, 2) hold.
//!   With the published timers (Ds = Dr = 5, OBST 1, P 10, C 15): hold at
//!   U = 2 for L in {0, 1, 2}; FIRE at U = 3 for L in {0, 1, 3}. The
//!   L = U = 3 witness has no tie: CheckSensors at 15, Ack read at 21
//!   after CheckScientistAck at 20, RescueReach read at 26 after
//!   CheckRescue at 25 (the witness printed depends on the random
//!   schedule seed; some runs show the obstacle taken and the
//!   RescueReach read at 27 instead).
//!   Non-vacuity: holds at the second gate explore the rescue path, e.g.
//!   U = 1, L = 0, (2, 4, 1): execs=8 rescues=7 reached=7 dead=0; at
//!   R = 3: execs=232 rescues=376 reached=376. Holds at the first gate
//!   show missions > 0 and acked > 0 with rescues = 0 (the binary prints
//!   a NOTE: the answer was never late). --keep-going at U = 1, L = 0,
//!   (2, 3, 1): execs=11, rescues=10, reached=7, dead=3.
//!
//! timed, FALSE SENSOR FAILURE (S = 1, --readings safe, --property sensor):
//!
//!     FIRE  iff  C <= U + sd,  or some check j in 2..R has
//!                (m + 1) * P + U + sd >= j * C,
//!                where m = floor(((j - 1) * C + sd - L) / P)
//!
//!   m is the last reading that can be read by check j-1; the reading
//!   after it must be able to arrive and be read no earlier than check
//!   j. This is not one inequality: it depends on how reading times line
//!   up with check times, so L shifts the verdict and R matters
//!   (R >= 1 + P / gcd(P, C) covers every alignment). Both sd terms are
//!   the Admin's pick-up lateness, which Timed Rebeca does not have (see
//!   Deviations): the sd inside m is check j-1 handled up to sd after its
//!   alarm rang, so it can still take a reading that arrived after the
//!   alarm; the sd in (m + 1) * P + U + sd is reading m+1 handled up to
//!   sd after it arrived, after check j. At sd = 0 the rule is pure
//!   network jitter. Verified by an automated grid over P in {3, 4, 6},
//!   C in 3..8, U in {1, 2, 3},
//!   L in {0, 1, U}, sd in {0, 1}, R in {2, 3}: 576 cells, 297 FIRE,
//!   279 hold, 0 disagreements. At the paper's periods P 10, C 15, R = 3:
//!     L = 0, sd = 0: U = 4 hold, U = 5 FIRE (also at R = 4, and at S = 2)
//!     L = 0, sd = 1: U = 3 hold, U = 4 FIRE
//!     L = 1, sd = 0: U = 9 hold, U = 10 FIRE
//!     L = 1, sd = 1: U = 3 hold, U = 4 FIRE (U = 8 still FIREs)
//!   The drop at L = 1 from U = 9/10 (sd = 0) to U = 3/4 (sd = 1) comes
//!   from the sd inside m, i.e. from the late alarm: in the U = 4
//!   witness CheckSensors{2}, due 30, is handled at 31 just after reading
//!   3 (sent 30, arrived 31), so check 3 at 45 misses reading 4 (read at
//!   45). R matters: at R = 2 (L = 0, sd = 0) U = 5 holds and U = 9
//!   holds, U = 10 FIREs; at R = 1 (the default) only C <= U + sd
//!   applies: U = 14 hold, U = 15 FIRE, at L = 0 and at L = 1. Same
//!   ratios at a second U: (P/U, C/U) = (2, 3) FIREs at U = 2 and U = 5
//!   (R = 3); (2.5, 3.75) holds at U = 4 and U = 8.
//!   The U = 5 witness: reading 3 (sent 30) read at 30 just before check
//!   2, reading 4 (sent 40) read at 45 just after check 3.
//!   Non-vacuity: late_reports > 0 on a hold shows a reading was read
//!   after a check it could have been read before, while a sibling
//!   reading kept the window filled. At P 10, C 15, R = 3: L 0, U 4:
//!   late_reports=1; L 0, sd 1, U 3: late_reports=1; L 1, U 9:
//!   late_reports=4; L 1, sd 1, U 3: late_reports=1. At the L = 0 holds
//!   the only reading that can straddle a check is reading 3, sent at 30,
//!   exactly when check 2 is due. Also P 6, C 9, L 2, U 5, R 3:
//!   late_reports=4. No reading can straddle a check at L = U = 2, P 7,
//!   C 15, R = 1
//!   (the reading sent at 14 arrives at 16): late_reports=0, NOTE
//!   printed. --keep-going at U = 5, R = 3: execs=8, sensor_failures=2,
//!   late_reports=12.
//!
//! timed, overlapping missions (--allow-overlap), an APPROXIMATION of
//!   FOCLASA Table 3 rows 1-2, not a reproduction (L = U = 1, P 2, C 4,
//!   Ds 2, OBST 1, S = 1: one sensor with one period of 2 instead of the
//!   paper's two sensors with periods 2 and 3): Dr = 3 FIRE ("Mission
//!   failed" in the paper), Dr = 4 hold ("Mission success"), each at R in
//!   {1, 2, 3}; the Dr = 4 hold at R = 3 has execs=47, rescues=62,
//!   reached=62. Without --allow-overlap both cells exit 2. Rows 3-4 are
//!   out of reach (see Deviations): a row-4-shaped cell, --u 2 --l-ratio
//!   1 --check-ratio 2 --ack-deadline-ratio 2 --rescue-deadline-ratio 3.5
//!   --allow-overlap, exits 2.
//!
//! Witnesses: every timed FIRE above, including all 425 grid FIREs, was
//! parsed from its graph and certified timeline by a script, and the
//! individually listed ones were also read by hand. Each false death
//! shows the same round's Ack read after its CheckScientistAck and the
//! round's RescueReach read after its CheckRescue; each false sensor
//! failure shows a sensor silent between two checks, with its next
//! reading read at or after the second check, at most U + sd after it
//! was sent. At sd > 0 a witness may also show the Admin handling a
//! message out of arrival order, which Timed Rebeca forbids (see
//! Deviations): an alarm handled after a reading that arrived later (the
//! L = 1, sd = 1, U = 4 sensor witness above), or an Ack handled after a
//! timer that rang
//! later than the Ack arrived (the U = 1, sd = 1, (4, 5, 1) death
//! witness, whose RescueReach, arrived by 39, is also handled after
//! CheckRescue at 40).
//!
//! CLI misuse (e.g. --sensors 3, --mode foo, --l-ratio 2, a mission
//! guard violation) exits 2.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats};

// Outcome counters: tallies of the Admin's verdicts, added only after
// its drain (so executions that block on an ignored message never
// count). Accumulated across all explored executions of one verify run
// (revisited prefixes may re-count, so treat them as zero/nonzero
// evidence, not exact per-execution tallies). Reset before each verify.
static CHECKS_OK: AtomicUsize = AtomicUsize::new(0);
static SENSOR_FAILURES: AtomicUsize = AtomicUsize::new(0);
/// Readings read after check k although they could have been read
/// before it: earliest arrival seq*P + L no later than the check's latest
/// pick-up k*C + sd, ties included (computed from the send index, never
/// from a clock; a counter only, it prunes nothing). Nonzero on a sensor
/// hold shows that a reading really missed a check and the property
/// still held.
static LATE_REPORTS: AtomicUsize = AtomicUsize::new(0);
static MISSIONS: AtomicUsize = AtomicUsize::new(0);
static ACKED_IN_TIME: AtomicUsize = AtomicUsize::new(0);
static RESCUES_SENT: AtomicUsize = AtomicUsize::new(0);
static REACHED_IN_TIME: AtomicUsize = AtomicUsize::new(0);
static SCIENTIST_DEAD: AtomicUsize = AtomicUsize::new(0);

// Never set true: sensors never break and the scientist is never lost
// in this model, so every sensorFailure and every scientistDead is a
// false alarm by construction.
static SENSOR_BROKEN: AtomicBool = AtomicBool::new(false);
static SCIENTIST_LOST: AtomicBool = AtomicBool::new(false);

const DEFAULT_SENSORS: u32 = 2;
const DEFAULT_ROUNDS: u32 = 1;
const DEFAULT_U: u64 = 1;
const DEFAULT_L_RATIO: f64 = 1.0;
const DEFAULT_SD_RATIO: f64 = 0.0;
const DEFAULT_PERIOD_RATIO: f64 = 10.0;
const DEFAULT_CHECK_RATIO: f64 = 15.0;
const DEFAULT_ACK_DEADLINE_RATIO: f64 = 5.0;
const DEFAULT_RESCUE_DEADLINE_RATIO: f64 = 5.0;
const DEFAULT_OBSTACLE_RATIO: f64 = 1.0;

/// Rebeca's gas levels: "2=safe gas levels, 4=danger gas levels";
/// the Admin's danger test is value > 3.
const GAS_SAFE: u8 = 2;
const GAS_DANGER: u8 = 4;

/// Everything the Admin receives (Rebeca msgsrv's of Admin).
#[derive(Clone, Debug, PartialEq)]
enum AdminMsg {
    /// report(value); `sensor` stands for Rebeca's `sender`, `seq` is
    /// debugging only.
    Report { sensor: u32, seq: u32, value: u8 },
    /// checkSensors(), pre-armed by main (see the doc header).
    CheckSensors { round: u32 },
    /// ack() from the Scientist.
    Ack { round: u32 },
    /// checkScientistAck(), Admin self-timer.
    CheckScientistAck { round: u32 },
    /// rescueReach() from the Rescue team.
    RescueReach { round: u32 },
    /// checkRescue(), Admin self-timer.
    CheckRescue { round: u32 },
}

#[derive(Clone, Debug, PartialEq)]
enum SciMsg {
    AbortPlan { round: u32, admin: ThreadId },
    /// Verification harness, not the protocol.
    Done,
}

#[derive(Clone, Debug, PartialEq)]
enum RescueMsg {
    Go { round: u32, admin: ThreadId },
    /// Verification harness, not the protocol.
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    Timed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Readings {
    Danger,
    Random,
    Safe,
}

impl Readings {
    fn name(self) -> &'static str {
        match self {
            Readings::Danger => "danger",
            Readings::Random => "random",
            Readings::Safe => "safe",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Property {
    Both,
    Sensor,
    Rescue,
}

impl Property {
    fn name(self) -> &'static str {
        match self {
            Property::Both => "both",
            Property::Sensor => "sensor",
            Property::Rescue => "rescue",
        }
    }
    fn sensor(self) -> bool {
        matches!(self, Property::Both | Property::Sensor)
    }
    fn rescue(self) -> bool {
        matches!(self, Property::Both | Property::Rescue)
    }
}

/// Resolved absolute parameters of one run.
#[derive(Clone, Copy, Debug)]
struct Params {
    sensors: u32,
    rounds: u32,
    u: u64,
    l: u64,
    sd: u64,
    /// Sensor period.
    period: u64,
    /// adminCheckDelay.
    check: u64,
    /// scientistDeadline.
    ack_deadline: u64,
    /// Admin rescueDeadline.
    rescue_deadline: u64,
    /// Obstacle maximum.
    obstacle: u64,
    readings: Readings,
    property: Property,
}

impl Params {
    /// Readings per sensor: the last one is sent strictly after the
    /// last check can be read (R*C + sd), so a "no reading since the
    /// last check" verdict always has a later reading to validate it.
    fn readings_per_sensor(&self) -> u32 {
        ((self.rounds as u64 * self.check + self.sd) / self.period) as u32 + 2
    }
}

/// CLI misuse exits 2, distinct from a property violation's 101, so
/// exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
fn cli_bail(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

// =====================================================================
// Sensor: periodic reporter, never fails (Rebeca doReport)
// =====================================================================

fn sensor(p: Params, index: u32, admin: ThreadId) {
    let n = p.readings_per_sensor();
    for seq in 0..n {
        let value = match p.readings {
            Readings::Danger => GAS_DANGER,
            Readings::Safe => GAS_SAFE,
            // Rebeca ?(2, 4): a choice between the two listed values.
            Readings::Random => {
                if traceforge::nondet() {
                    GAS_DANGER
                } else {
                    GAS_SAFE
                }
            }
        };
        // admin.report(value) after(netDelay): global transit [L, U].
        traceforge::send_msg(admin, AdminMsg::Report { sensor: index, seq, value });
        // self.doReport() after(period). Unconditional, both modes.
        if seq + 1 < n {
            traceforge::sleep(p.period);
        }
    }
}

// =====================================================================
// Scientist: purely reactive, always answers (Rebeca abortPlan)
// =====================================================================

fn scientist() {
    loop {
        match traceforge::recv_msg_block_timed::<SciMsg>() {
            // admin.ack() after(netDelay)
            SciMsg::AbortPlan { round, admin } => {
                traceforge::send_msg(admin, AdminMsg::Ack { round })
            }
            SciMsg::Done => return,
        }
    }
}

// =====================================================================
// Rescue: purely reactive, always gets there (Rebeca go)
// =====================================================================

fn rescue(obstacle: u64) {
    loop {
        match traceforge::recv_msg_block_timed::<RescueMsg>() {
            RescueMsg::Go { round, admin } => {
                // delay(?(0, 1)): an obstacle of 0 or OBST time units.
                if obstacle > 0 && traceforge::nondet() {
                    traceforge::sleep(obstacle);
                }
                // admin.rescueReach() after(netDelay); the Rebeca
                // message deadline is not modeled (doc header).
                traceforge::send_msg(admin, AdminMsg::RescueReach { round });
            }
            RescueMsg::Done => return,
        }
    }
}

// =====================================================================
// Admin: one receive loop, Listing 5's handlers and flags
// =====================================================================

#[derive(Default)]
struct Tally {
    checks_ok: usize,
    sensor_failures: usize,
    late_reports: usize,
    missions: usize,
    acked_in_time: usize,
    rescues_sent: usize,
    reached_in_time: usize,
    dead: usize,
}

impl Tally {
    fn publish(&self) {
        CHECKS_OK.fetch_add(self.checks_ok, Ordering::Relaxed);
        SENSOR_FAILURES.fetch_add(self.sensor_failures, Ordering::Relaxed);
        LATE_REPORTS.fetch_add(self.late_reports, Ordering::Relaxed);
        MISSIONS.fetch_add(self.missions, Ordering::Relaxed);
        ACKED_IN_TIME.fetch_add(self.acked_in_time, Ordering::Relaxed);
        RESCUES_SENT.fetch_add(self.rescues_sent, Ordering::Relaxed);
        REACHED_IN_TIME.fetch_add(self.reached_in_time, Ordering::Relaxed);
        SCIENTIST_DEAD.fetch_add(self.dead, Ordering::Relaxed);
    }
}

/// Drain bookkeeping (verification harness, NOT the protocol; see the
/// doc header): every message known to be on its way to the Admin, in
/// each channel's send order.
struct Drain {
    sensors: usize,
    readings_per_sensor: u32,
    next_seq: [u32; 2],
    rounds: u32,
    next_check: u32,
    /// Rounds that started a mission, in order (one Ack and one
    /// CheckScientistAck each).
    missions: Vec<u32>,
    acks_read: usize,
    ack_checks_read: usize,
    /// Rounds that sent the rescue team, in order (one RescueReach and
    /// one CheckRescue each).
    rescues: Vec<u32>,
    reaches_read: usize,
    rescue_checks_read: usize,
}

impl Drain {
    fn new(p: &Params) -> Self {
        Drain {
            sensors: p.sensors as usize,
            readings_per_sensor: p.readings_per_sensor(),
            next_seq: [0; 2],
            rounds: p.rounds,
            next_check: 1,
            missions: Vec::new(),
            acks_read: 0,
            ack_checks_read: 0,
            rescues: Vec::new(),
            reaches_read: 0,
            rescue_checks_read: 0,
        }
    }

    fn complete(&self) -> bool {
        self.next_seq[..self.sensors].iter().all(|&s| s == self.readings_per_sensor)
            && self.next_check > self.rounds
            && self.acks_read == self.missions.len()
            && self.ack_checks_read == self.missions.len()
            && self.reaches_read == self.rescues.len()
            && self.rescue_checks_read == self.rescues.len()
    }
}

/// Early drain cut (verification harness): a message read out of its
/// channel's send order means an earlier one on that channel was passed
/// over, and a passed-over message can never be read afterwards (it was
/// already gone when the read began). Such an execution can never
/// finish the drain, so block it here instead of at the end. This only
/// saves work: the set of completed executions is unchanged.
fn expect_in_order(in_order: bool) {
    if !in_order {
        traceforge::assume!(false);
    }
}

fn admin(p: Params, scientist: ThreadId, rescue: ThreadId) {
    let me = thread::current().id();
    let n_sensors = p.sensors as usize;

    // Listing 5 state variables.
    let mut reported = [false; 2];
    let mut sensor_value = [0u8; 2];
    let mut sensor_failure = false;
    let mut scientist_ack = false;
    let mut scientist_reached = false;
    let mut scientist_dead = false;

    let mut d = Drain::new(&p);
    let mut t = Tally::default();

    while !d.complete() {
        match traceforge::recv_msg_block_timed::<AdminMsg>() {
            AdminMsg::Report { sensor, seq, value } => {
                let s = sensor as usize;
                expect_in_order(s < n_sensors && seq == d.next_seq[s]);
                d.next_seq[s] += 1;
                // Counter only: read after the last processed check k,
                // but could it have been read before that check?
                let k = d.next_check as u64 - 1;
                if k >= 1 && (seq as u64) * p.period + p.l <= k * p.check + p.sd {
                    t.late_reports += 1;
                }
                reported[s] = true;
                sensor_value[s] = value;
            }
            AdminMsg::CheckSensors { round } => {
                expect_in_order(round == d.next_check);
                d.next_check += 1;
                for s in 0..n_sensors {
                    if reported[s] {
                        reported[s] = false;
                        t.checks_ok += 1;
                    } else {
                        // Recorded now, asserted after the drain.
                        sensor_failure = true;
                        t.sensor_failures += 1;
                    }
                }
                let danger = sensor_value[..n_sensors].iter().any(|&v| v > 3);
                if danger {
                    // scientist.abortPlan() after(netDelay);
                    traceforge::send_msg(scientist, SciMsg::AbortPlan { round, admin: me });
                    // self.checkScientistAck() after(scientistDeadline);
                    traceforge::send_msg_timed(
                        me,
                        AdminMsg::CheckScientistAck { round },
                        p.ack_deadline,
                        p.ack_deadline,
                    );
                    t.missions += 1;
                    d.missions.push(round);
                }
                // self.checkSensors() after(adminCheckDelay): pre-armed
                // by main (doc header).
            }
            AdminMsg::Ack { round } => {
                expect_in_order(d.missions.get(d.acks_read) == Some(&round));
                d.acks_read += 1;
                scientist_ack = true;
            }
            AdminMsg::CheckScientistAck { round } => {
                expect_in_order(d.missions.get(d.ack_checks_read) == Some(&round));
                d.ack_checks_read += 1;
                if !scientist_ack {
                    // rescue.go() after(netDelay);
                    traceforge::send_msg(rescue, RescueMsg::Go { round, admin: me });
                    // self.checkRescue() after(rescueDeadline);
                    traceforge::send_msg_timed(
                        me,
                        AdminMsg::CheckRescue { round },
                        p.rescue_deadline,
                        p.rescue_deadline,
                    );
                    t.rescues_sent += 1;
                    d.rescues.push(round);
                } else {
                    t.acked_in_time += 1;
                }
                scientist_ack = false;
            }
            AdminMsg::RescueReach { round } => {
                expect_in_order(d.rescues.get(d.reaches_read) == Some(&round));
                d.reaches_read += 1;
                scientist_reached = true;
            }
            AdminMsg::CheckRescue { round } => {
                expect_in_order(d.rescues.get(d.rescue_checks_read) == Some(&round));
                d.rescue_checks_read += 1;
                if !scientist_reached {
                    // Recorded now, asserted after the drain.
                    scientist_dead = true;
                    t.dead += 1;
                } else {
                    scientist_reached = false;
                    t.reached_in_time += 1;
                }
            }
        }
    }

    // Drain complete: every message sent to the Admin was read, so the
    // recorded verdicts are backed by genuinely late messages. Release
    // the reactive threads (harness), publish the tallies, then assert.
    traceforge::send_msg(scientist, SciMsg::Done);
    traceforge::send_msg(rescue, RescueMsg::Done);
    t.publish();
    if p.property.sensor() {
        // No false sensor failure: sensors never break here.
        traceforge::assert(!sensor_failure || SENSOR_BROKEN.load(Ordering::Relaxed));
    }
    if p.property.rescue() {
        // No false death: the scientist is always fine here.
        traceforge::assert(!scientist_dead || SCIENTIST_LOST.load(Ordering::Relaxed));
    }
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, p: Params, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_progress_report(usize::MAX);
    if keep_going {
        // Explore the whole state space after a violation; the number
        // of false alarms is then in sensor_failures= and dead=.
        builder = builder.with_keep_going_after_error(true);
    }
    match mode {
        Mode::Baseline => builder.build(),
        Mode::Timed => builder.with_timed(p.l, p.u, p.sd).build(),
    }
}

fn reset_counters() {
    for c in [
        &CHECKS_OK,
        &SENSOR_FAILURES,
        &LATE_REPORTS,
        &MISSIONS,
        &ACKED_IN_TIME,
        &RESCUES_SENT,
        &REACHED_IN_TIME,
        &SCIENTIST_DEAD,
    ] {
        c.store(0, Ordering::Relaxed);
    }
    SENSOR_BROKEN.store(false, Ordering::Relaxed);
    SCIENTIST_LOST.store(false, Ordering::Relaxed);
}

fn run(mode: Mode, p: Params, keep_going: bool) -> (Stats, Duration) {
    let cfg = build_config(mode, p, keep_going);
    reset_counters();
    let start = Instant::now();
    let stats = traceforge::verify(cfg, move || {
        let sci = thread::spawn(scientist);
        let res = thread::spawn(move || rescue(p.obstacle));
        let sci_id = sci.thread().id();
        let res_id = res.thread().id();
        let adm = thread::spawn(move || admin(p, sci_id, res_id));
        let adm_id = adm.thread().id();
        let mut sensors = Vec::new();
        for i in 0..p.sensors {
            sensors.push(thread::spawn(move || sensor(p, i, adm_id)));
        }
        // The Admin's periodic alarm, pre-armed at time 0: check j
        // arrives at exactly j*C (Rebeca checkSensors() after(C) chain).
        for j in 1..=p.rounds {
            let at = j as u64 * p.check;
            traceforge::send_msg_timed(adm_id, AdminMsg::CheckSensors { round: j }, at, at);
        }
        for h in sensors {
            let _ = h.join();
        }
        let _ = adm.join();
        let _ = sci.join();
        let _ = res.join();
    });
    (stats, start.elapsed())
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    checks_ok: usize,
    sensor_failures: usize,
    late_reports: usize,
    missions: usize,
    acked: usize,
    rescues: usize,
    reached: usize,
    dead: usize,
}

fn read_counts() -> Counts {
    Counts {
        checks_ok: CHECKS_OK.load(Ordering::Relaxed),
        sensor_failures: SENSOR_FAILURES.load(Ordering::Relaxed),
        late_reports: LATE_REPORTS.load(Ordering::Relaxed),
        missions: MISSIONS.load(Ordering::Relaxed),
        acked: ACKED_IN_TIME.load(Ordering::Relaxed),
        rescues: RESCUES_SENT.load(Ordering::Relaxed),
        reached: REACHED_IN_TIME.load(Ordering::Relaxed),
        dead: SCIENTIST_DEAD.load(Ordering::Relaxed),
    }
}

/// Vacuity guards: exit 0 is a hold only if the relevant path was
/// actually explored in completed executions.
fn warn_if_vacuous(label: &str, p: Params, execs: usize, blocked: usize, c: Counts) {
    // A violating execution is aborted by the assertion, so the checker
    // files it under `blocked`, not `execs`. Warning on execs == 0 alone
    // would tell the reader to discard a run that found counterexamples.
    if execs == 0 && c.sensor_failures + c.dead == 0 {
        println!(
            "WARNING ({label}): 0 complete executions (blocked={blocked}) and no violation: \
             every execution blocked, this run verified nothing; treat it as no data, not a hold."
        );
        return;
    }
    if p.property.sensor() && c.checks_ok + c.sensor_failures == 0 {
        println!(
            "WARNING ({label}): no sensor check completed in any execution; the sensor \
             property was not exercised, treat it as no data."
        );
    }
    if p.property.sensor() && c.checks_ok > 0 && c.late_reports == 0 && c.sensor_failures == 0 {
        println!(
            "NOTE ({label}): no reading was ever read after a check it could have been read \
             before; a sensor hold here says only that no reading could straddle a check."
        );
    }
    if p.property.rescue() && p.readings == Readings::Safe {
        println!(
            "NOTE ({label}): readings=safe never starts a mission, so the rescue property \
             holds trivially here."
        );
    }
    if p.property.rescue() && p.readings != Readings::Safe && c.missions == 0 {
        println!(
            "WARNING ({label}): no mission was ever started; the rescue property was not \
             exercised, treat it as no data."
        );
    }
    if p.property.rescue() && c.rescues > 0 && c.reached == 0 && c.dead == 0 {
        println!(
            "WARNING ({label}): rescues were sent but no rescue check completed; every \
             rescue outcome was discarded, treat it as no data."
        );
    }
    if p.property.rescue() && p.readings != Readings::Safe && c.missions > 0 && c.rescues == 0 {
        println!(
            "NOTE ({label}): the scientist always answered in time, no rescue was ever \
             sent; a rescue hold here says only that the answer was never late."
        );
    }
}

// =====================================================================
// Reporting
// =====================================================================

fn print_one(label: &str, p: Params, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<9} S={s} R={r} readings={rd} property={pr}  L={l} U={u} sd={sd} P={per} C={chk} \
         Ds={ds} Dr={dr} OBST={ob}  execs={execs:<6} blocked={block:<6} checks_ok={cok} \
         late_reports={lr} sensor_failures={sf} missions={mi} acked={ak} rescues={rs} reached={re} dead={de} \
         time={dur:?}",
        s = p.sensors, r = p.rounds, rd = p.readings.name(), pr = p.property.name(),
        l = p.l, u = p.u, sd = p.sd, per = p.period, chk = p.check,
        ds = p.ack_deadline, dr = p.rescue_deadline, ob = p.obstacle,
        execs = stats.execs, block = stats.block,
        cok = c.checks_ok, lr = c.late_reports, sf = c.sensor_failures, mi = c.missions, ak = c.acked,
        rs = c.rescues, re = c.reached, de = c.dead, dur = dur,
    );
}

fn print_compare(p: Params, baseline: (Stats, Duration), timed: (Stats, Duration), bc: Counts, tc: Counts) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("Sensor network with rescue escalation: MUST vs MUST-timed");
    println!("=========================================================");
    println!(
        "S = {}  R = {}  L = {}  U = {}  sd = {}  P = {}  C = {}  Ds = {}  Dr = {}  OBST = {}",
        p.sensors, p.rounds, p.l, p.u, p.sd, p.period, p.check, p.ack_deadline,
        p.rescue_deadline, p.obstacle
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
        "sensor_failures/missions/rescues/dead: baseline {}/{}/{}/{}   timed {}/{}/{}/{}",
        bc.sensor_failures, bc.missions, bc.rescues, bc.dead,
        tc.sensor_failures, tc.missions, tc.rescues, tc.dead
    );
}

// =====================================================================
// CLI
// =====================================================================

struct Args {
    mode: String,
    sensors: u32,
    rounds: u32,
    u: u64,
    l_ratio: f64,
    sd_ratio: f64,
    period_ratio: f64,
    check_ratio: f64,
    ack_deadline_ratio: f64,
    rescue_deadline_ratio: f64,
    obstacle_ratio: f64,
    readings: Readings,
    property: Property,
    allow_overlap: bool,
    keep_going: bool,
}

fn next_val(args: &mut std::env::Args, flag: &str) -> String {
    args.next().unwrap_or_else(|| cli_bail(&format!("{flag} needs a value")))
}

fn parse_num<T: std::str::FromStr>(v: String, flag: &str) -> T {
    v.parse().unwrap_or_else(|_| cli_bail(&format!("{flag} got a malformed value")))
}

fn parse_args() -> Args {
    let mut a = Args {
        mode: String::from("timed"),
        sensors: DEFAULT_SENSORS,
        rounds: DEFAULT_ROUNDS,
        u: DEFAULT_U,
        l_ratio: DEFAULT_L_RATIO,
        sd_ratio: DEFAULT_SD_RATIO,
        period_ratio: DEFAULT_PERIOD_RATIO,
        check_ratio: DEFAULT_CHECK_RATIO,
        ack_deadline_ratio: DEFAULT_ACK_DEADLINE_RATIO,
        rescue_deadline_ratio: DEFAULT_RESCUE_DEADLINE_RATIO,
        obstacle_ratio: DEFAULT_OBSTACLE_RATIO,
        readings: Readings::Danger,
        property: Property::Both,
        allow_overlap: false,
        keep_going: false,
    };
    let mut args = std::env::args();
    args.next();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => a.mode = next_val(&mut args, "--mode"),
            "--sensors" => a.sensors = parse_num(next_val(&mut args, "--sensors"), "--sensors"),
            "--rounds" => a.rounds = parse_num(next_val(&mut args, "--rounds"), "--rounds"),
            "--u" => a.u = parse_num(next_val(&mut args, "--u"), "--u"),
            "--l-ratio" => a.l_ratio = parse_num(next_val(&mut args, "--l-ratio"), "--l-ratio"),
            "--sd-ratio" => a.sd_ratio = parse_num(next_val(&mut args, "--sd-ratio"), "--sd-ratio"),
            "--period-ratio" => {
                a.period_ratio = parse_num(next_val(&mut args, "--period-ratio"), "--period-ratio")
            }
            "--check-ratio" => {
                a.check_ratio = parse_num(next_val(&mut args, "--check-ratio"), "--check-ratio")
            }
            "--ack-deadline-ratio" => {
                a.ack_deadline_ratio =
                    parse_num(next_val(&mut args, "--ack-deadline-ratio"), "--ack-deadline-ratio")
            }
            "--rescue-deadline-ratio" => {
                a.rescue_deadline_ratio = parse_num(
                    next_val(&mut args, "--rescue-deadline-ratio"),
                    "--rescue-deadline-ratio",
                )
            }
            "--obstacle-ratio" => {
                a.obstacle_ratio =
                    parse_num(next_val(&mut args, "--obstacle-ratio"), "--obstacle-ratio")
            }
            "--readings" => {
                let v = next_val(&mut args, "--readings");
                a.readings = match v.as_str() {
                    "danger" => Readings::Danger,
                    "random" => Readings::Random,
                    "safe" => Readings::Safe,
                    other => cli_bail(&format!(
                        "invalid --readings: {other} (expected danger|random|safe)"
                    )),
                };
            }
            "--property" => {
                let v = next_val(&mut args, "--property");
                a.property = match v.as_str() {
                    "both" => Property::Both,
                    "sensor" => Property::Sensor,
                    "rescue" => Property::Rescue,
                    other => cli_bail(&format!(
                        "invalid --property: {other} (expected both|sensor|rescue)"
                    )),
                };
            }
            "--allow-overlap" => a.allow_overlap = true,
            "--keep-going" => a.keep_going = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: sensor_network_timed [--mode baseline|timed|compare] [--sensors 1|2] \
                     [--rounds R] [--u U] [--l-ratio LR] [--sd-ratio SR] [--period-ratio PR] \
                     [--check-ratio CR] [--ack-deadline-ratio AR] [--rescue-deadline-ratio RR] \
                     [--obstacle-ratio OR] [--readings danger|random|safe] \
                     [--property both|sensor|rescue] [--allow-overlap] [--keep-going]\n\
                     All ratios are over U and rounded. Defaults: S=2, R=1, U=1, L/U=1, sd/U=0, \
                     P/U=10, C/U=15, Ds/U=5, Dr/U=5, OBST/U=1, readings=danger, property=both \
                     (the published model with fixed netDelay 1).\n\
                     Missions must not overlap: Ds + Dr + 3sd < C, unless --allow-overlap, which \
                     still requires Ds + 2sd < C and Dr + 2sd <= C.\n\
                     --keep-going explores past violations and counts them (sensor_failures=, dead=).\n\
                     Exit 0 = no false alarm over the explored state space; exit 101 = a false \
                     sensor failure or a false death; exit 2 = CLI misuse."
                );
                std::process::exit(0);
            }
            other => cli_bail(&format!("unknown argument: {other}")),
        }
    }
    a
}

fn resolve(a: &Args) -> Params {
    if a.u < 1 {
        cli_bail("U must be >= 1");
    }
    if a.sensors < 1 || a.sensors > 2 {
        cli_bail("--sensors must be 1 or 2");
    }
    if a.rounds < 1 {
        cli_bail("need at least 1 round");
    }
    let scale = |r: f64, flag: &str| -> u64 {
        if !(r >= 0.0) || !r.is_finite() {
            cli_bail(&format!("{flag} must be a finite ratio >= 0"));
        }
        (r * a.u as f64).round() as u64
    };
    let p = Params {
        sensors: a.sensors,
        rounds: a.rounds,
        u: a.u,
        l: scale(a.l_ratio, "--l-ratio"),
        sd: scale(a.sd_ratio, "--sd-ratio"),
        period: scale(a.period_ratio, "--period-ratio"),
        check: scale(a.check_ratio, "--check-ratio"),
        ack_deadline: scale(a.ack_deadline_ratio, "--ack-deadline-ratio"),
        rescue_deadline: scale(a.rescue_deadline_ratio, "--rescue-deadline-ratio"),
        obstacle: scale(a.obstacle_ratio, "--obstacle-ratio"),
        readings: a.readings,
        property: a.property,
    };
    if p.l > p.u {
        cli_bail("transit lower bound L must be <= U (check --l-ratio)");
    }
    if p.period < 1 {
        cli_bail("sensor period P must round to >= 1 (check --period-ratio)");
    }
    if p.check < 1 {
        cli_bail("check period C must round to >= 1 (check --check-ratio)");
    }
    if p.ack_deadline < 1 {
        cli_bail("scientist deadline Ds must round to >= 1 (check --ack-deadline-ratio)");
    }
    if p.rescue_deadline < 1 {
        cli_bail("rescue deadline Dr must round to >= 1 (check --rescue-deadline-ratio)");
    }
    if p.readings != Readings::Safe {
        // Channel-order faithfulness (never relaxed): mission timers
        // on the Admin's own channel must always arrive in send order,
        // otherwise genuine timelines would be deleted.
        if !(p.ack_deadline + 2 * p.sd < p.check && p.rescue_deadline + 2 * p.sd <= p.check) {
            cli_bail(
                "mission timers would break channel order: need Ds + 2sd < C and \
                 Dr + 2sd <= C (even with --allow-overlap)",
            );
        }
        if !a.allow_overlap && p.ack_deadline + p.rescue_deadline + 3 * p.sd >= p.check {
            cli_bail(
                "missions may overlap: need Ds + Dr + 3sd < C (pass --allow-overlap to \
                 explore the overlapping-mission flag cross-talk)",
            );
        }
    }
    p
}

fn main() {
    let a = parse_args();
    let p = resolve(&a);
    match a.mode.as_str() {
        "baseline" => {
            let (s, d) = run(Mode::Baseline, p, a.keep_going);
            let c = read_counts();
            print_one("baseline", p, &s, d, c);
            warn_if_vacuous("baseline", p, s.execs, s.block, c);
        }
        "timed" => {
            let (s, d) = run(Mode::Timed, p, a.keep_going);
            let c = read_counts();
            print_one("timed", p, &s, d, c);
            warn_if_vacuous("timed", p, s.execs, s.block, c);
        }
        "compare" => {
            let baseline = run(Mode::Baseline, p, a.keep_going);
            let bc = read_counts();
            let timed = run(Mode::Timed, p, a.keep_going);
            let tc = read_counts();
            let (bv, tv) = ((baseline.0.execs, baseline.0.block), (timed.0.execs, timed.0.block));
            print_compare(p, baseline, timed, bc, tc);
            warn_if_vacuous("baseline", p, bv.0, bv.1, bc);
            warn_if_vacuous("timed", p, tv.0, tv.1, tc);
        }
        other => cli_bail(&format!("invalid --mode: {other} (expected baseline|timed|compare)")),
    }
}
