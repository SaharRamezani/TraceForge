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
//! on someone who was fine. A false alarm here has two possible causes:
//! timing, when the network is slower or less even than the admin's
//! alarm settings assume and a reading or an answer lands a moment after
//! the admin has already looked; and loss, when a reading or an answer
//! never arrives at all.
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
//! Every message may be lost on the way, and a message that has arrived
//! is readable only for a short storage time (0 here, so only at the
//! instant it arrives). An actor that is waiting reads the message that
//! arrived first and does not wait to see another one, so it never passes
//! over a message that is there; a message can expire unread only while
//! its receiver is busy (here only the rescue team, held up by the
//! obstacle). The checker explores these runs like any other, and a false
//! alarm they cause is reported like any other. A run in which some actor
//! waits for a message that was lost ends with that actor waiting and is
//! counted as "blocked"; a blocked count above zero is therefore normal.
//!
//! What the checker finds, in short. Because any message may be lost, a
//! false "broken" is reachable at every check (all of a sensor's
//! readings since the previous check are lost), and a false "dead"
//! wherever a mission can start (the answer is lost, so a rescue goes
//! out, and the rescue team's call is lost too). The asserted properties
//! are violated, in both modes, at each of 22 variations of the published
//! settings tried (jitter, storage time, deadlines, check and sensor
//! periods, obstacle, R = 2, one sensor, random or safe readings, either
//! timer kind), each with a counterexample (a timeline in the timed
//! mode); the Table 3 presets are in Results (rows 3-5 to 3-7 hold at
//! R = 1 only because no mission can start). At the published settings
//! (every transit exactly 1, no storage time) nothing but loss causes a
//! false alarm, and at R = 1 it takes two lost messages: no two messages
//! that matter arrive at the same instant, and a waiting actor never
//! passes over a message that is there. The timing-only findings
//! reported here before 2026-10-12 (a false "dead" from 3-unit messages
//! at the published settings, a false "broken" from readings as slow as
//! 15 units at one check, and so on) were obtained with a harness that
//! threw away every run with a lost message; they described that
//! filtered model and are withdrawn (see Results).
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
//! * SCP 2014 journal version (Reynisson et al., "Modelling and simulation
//!   of asynchronous real-time systems using Timed Rebeca", Science of
//!   Computer Programming 89 (2014) 41-68), consulted 2026-09-18 from a
//!   local copy: Table 3 (p. 56) and Listing 6 (Appendix A). Listing 6 is
//!   the model the `--table3-row` presets reproduce. It differs from the
//!   rebeca-lang.org file in exactly the ways the presets follow: every
//!   constant is an `env` parameter, there are two sensor periods
//!   (sensor0period, sensor1period), and ONE rescueDeadline is shared by
//!   the Rescue's message deadline and the Admin's checkRescue timer (the
//!   file has 10 and 5). Table 3's verdicts are McErlang SIMULATIONS ("we
//!   run a simulation for ten times", p. 52), so its "Mission success"
//!   means ten runs saw no failure, not that none exists.
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
//! receive loop over every message kind, including its own timers. A
//! timed receive reads the message that arrived first among those
//! pending at it (ties in either order), so the loop handles messages in
//! arrival order exactly as Rebeca does. At sd = 0 a message is readable
//! only at the instant it arrives and is handled then; at sd > 0 it may
//! be handled up to sd late (see Deviations).
//! Rebeca's `self.m() after(d)` becomes a self-send with exact per-send
//! transit `send_msg_timed(me, m, d, d)`; every Rebeca `after(netDelay)`
//! on an inter-actor message becomes a plain send under the global
//! transit window [L, U] (L = U = 1, sd = 0 is the published model).
//! A sensor's `self.doReport() after(period)` becomes `sleep(P)`
//! between two sends. The handler bodies and the Admin's flags
//! (reported, sensorValue, scientistAck, scientistReached) are Listing
//! 5's, verbatim; its two sticky verdict flags, sensorFailure and
//! scientistDead, are the assertions at the points that would set them.
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
//! There is no release message: the scientist, the rescue team and the
//! timer threads are daemon reactors (until 2026-10-14 the Admin sent each
//! a Done once it had finished, four lossy sends outside the protocol).
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
//!   still reads the message that arrived first (it never passes over a
//!   pending earlier arrival, whoever sent it), but it may pick that
//!   message up to sd after it arrived. For the Admin this includes its
//!   own alarms and timers: CheckSensors{j}, due at j*C, may be handled
//!   as late as j*C + sd, CheckScientistAck and CheckRescue up to sd
//!   after they ring, and what a handler starts (the warning, the timer
//!   it arms) then starts late too. Only sd = 0 handles every message at
//!   the instant it arrives.
//! * The first sensor check is at time C, not at time 0. The model
//!   file's Admin constructor runs checkSensors() at time 0, before any
//!   report can arrive, so sensorFailure is set in EVERY run of the
//!   published model; the paper never checks sensorFailure, so this is
//!   an artifact of the file, not a finding. For the rescue property the
//!   two are exactly equivalent: at time 0 every sensorValue is still 0,
//!   so Listing 6's check at 0 starts no mission.
//! * Periodic alarms are pre-armed by main (see above).
//! * With `--timers self` (the default), missions must not overlap: the CLI requires Ds + Dr + 3sd < C
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
//!   period 1 or 4) are therefore refused under `--timers self`.
//!   Even inside that guard the self channel is not quite Timed Rebeca at
//!   its boundary Dr + 2sd = C, where CheckRescue{j} and
//!   CheckScientistAck{j+1} fall due at the SAME instant; FIFO forces
//!   their send order, Timed Rebeca allows either. Within the guard that
//!   is the only possible same-instant timer pair, and its two handlers
//!   touch disjoint flags, so no verdict can change, but the execution
//!   count at that boundary is too low by the executions of the other
//!   order.
//! * `--timers separate` removes the channel-order obstacle, and with it
//!   the refusal: each timer kind has its own sender, so timers of one
//!   kind (one shared delay, send order = due order) share a FIFO and
//!   kinds are unordered, exactly Timed Rebeca's earliest-time-tag choice.
//!   It needs sd = 0 (a timer thread must read its Arm the instant it
//!   arrives, which is also Timed Rebeca's own no-slack scheduler). A Go
//!   that reaches the Rescue during its obstacle delay expires unread at
//!   sd = 0, where Rebeca would hold it in the bag; that run is explored
//!   and judged like any other. This is what lets `--table3-row` run SCP
//!   2014 Table 3's parameters, including rows 3-4 and their "new rescue
//!   mission while another is still ongoing".
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
//!   For SCP 2014 Table 3 (Listing 6, one shared rescueDeadline R) the
//!   expiry never binds. The reach is sent after the obstacle delta <= 1
//!   and arrives 2 * netDelay + delta after the Go was sent, while the
//!   message deadline is R - netDelay after go() starts, i.e. R after the
//!   Go was sent. So it is late only if 2 * netDelay + delta > R, and every
//!   row has 2 * netDelay + 1 <= R (row 1: 3 <= 3; row 2: 3 <= 4; row 3:
//!   5 <= 5, 6, 7; row 4: 5 <= 7). This holds whether deadline() is read
//!   as an absolute instant (as Listing 6's now() + ... suggests) or as a
//!   duration from the send, which is later still. At equality (rows 1 and
//!   3-5) the reach arrives exactly at the deadline, still in time, as a
//!   same-instant tie with checkRescue, which this port explores both ways.
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
//!   missions (fast sensor-only sweeps). An all-danger execution is one
//!   legitimate outcome of ?(2, 4), so a danger FIRE is a genuine
//!   counterexample of the random model; a danger HOLD is not a random HOLD.
//! * The obstacle is a two-valued choice {0, OBST}, generalizing
//!   Rebeca's ?(0, 1) (a choice between listed values, not a range).
//!   With OBST = 0 the choice is skipped.
//! * At most 2 sensors (SCP 2015 scaled 1..4; SCP 2014 Table 3 uses 2).
//!   Each sensor may have its own period (--sensor0-period-ratio,
//!   --sensor1-period-ratio), as Table 3's rows 1-2 need (2 and 3).
//! * Round numbers travel in payloads for debugging only; the Admin
//!   keeps Rebeca's round-agnostic flags, so the model's behaviour
//!   (including a stale late Ack suppressing the next mission's rescue,
//!   a missed rescue that is not asserted) is preserved. The stale Ack
//!   matters for Table 3: when the Ack lands exactly on its own
//!   checkScientistAck (Ds = 2 * netDelay, true in every row), a rescued
//!   mission's Ack is read just after that check and leaves scientistAck
//!   set, so the NEXT mission's check reads it as acknowledged and sends no
//!   rescue. Only a checkScientistAck clears the flag, so safe (no-mission)
//!   checks in between do not reset it. Rescues are therefore at least two
//!   missions (2C) apart.
//! * Messages arriving at the same instant may be handled in either
//!   order (ties count), and Rebeca's queue capacities are ignored.
//!
//! ## No harness filter (since 2026-10-12)
//!
//! Until 2026-10-12 the Admin DRAINED: it kept receiving until it had
//! read every message it knew was sent to it, blocked the execution
//! (`assume!(false)`) as soon as a message was read out of its channel's
//! send order, and asserted the two properties only after that. The
//! intent was to judge a false alarm only when the message it blames had
//! really been read late. What the drain excluded were executions the
//! model's semantics allow: a message lost on the way (every send may be
//! lost) and, until the first-arrival rule of 2026-10-13, a message that
//! was in while the Admin took a later one and expired unread (sd = 0; a
//! waiting receiver now reads what arrived first, so that kind is no
//! longer an execution). A lost message leaves the Admin with a silence,
//! and the Admin's alarm on that silence is a false alarm by
//! construction, since sensors never break, the scientist always answers
//! and the rescue always arrives. It is now reported like any other.
//! There is no drain, no early cut and no condition: the Admin handles
//! messages until it has seen the R-th sensor check and the two mission
//! timers of every mission it started, then releases the other actors
//! and ends. A reading, Ack or RescueReach that is lost is simply never
//! read. The `--readings random-relevant` reduction went with the drain:
//! its argument assumed that every reading is read at its arrival
//! instant, which loss breaks.
//!
//! ## Properties checked (at the Admin, at the step that would set the flag)
//!
//!   sensorFailure  => SENSOR_BROKEN    (never set: a false sensor failure)
//!   scientistDead  => SCIENTIST_LOST   (never set: a false death, which is
//!       FOCLASA's "mission failed" with the knowledge that the scientist
//!       was actually fine)
//!
//! --property both|sensor|rescue selects which flags are asserted; both
//! are always counted. The `LATE_REPORTS` counter uses the send index
//! times P plus L, never a clock, and prunes nothing. Same program in both
//! modes; only the Config differs.
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
//!   --sensor0-period-ratio PR       sensor 0's own period (default: P)
//!   --sensor1-period-ratio PR       sensor 1's own period (default: P)
//!   --timers self|separate          how the two mission timers reach the
//!                                   Admin (default self; separate needs
//!                                   sd = 0 and needs no overlap guard;
//!                                   see Deviations)
//!   --table3-row 1|2|3|3-5|3-6|3-7|4
//!                                   SCP 2014 Table 3 row, Listing 6
//!                                   verbatim (sets U, L, sd, S, C, both
//!                                   periods, Ds, Dr, OBST, readings
//!                                   random, property rescue, timers
//!                                   separate; R stays at its default 1);
//!                                   flags AFTER it override it
//!   --parallel none|shared|partitioned
//!                                   checker exploration strategy (Config
//!                                   only; the verified program is the same)
//!   --keep-going                    explore past violations and count them
//!                                   (exit 0; see sensor_failures= and dead=)
//!
//! CLI misuse exits 2, distinct from a property violation's 101, so
//! exit-code-based sweep harnesses cannot mistake a typo for a FIRE.
//!
//! ## Results (re-measured 2026-10-13 after the first-arrival ruling; "old" is
//! the drained version at HEAD 0cbe2c7 on the same cell; one worker, counts
//! with --keep-going, verdicts without)
//!
//! Under the model's semantics every message may be lost, so a false alarm is
//! reachable wherever the Admin reaches a check (sensor property) or a
//! mission's two timers (rescue property), in both modes. Published model
//! (S 2, R 1, L = U = 1, sd 0, P 10, C 15, Ds = Dr = 5, OBST 1, danger):
//!
//!   mode      version  execs    blocked   sensor_failures  dead     verdict
//!   timed     old      8        13,509    0                0        HOLD
//!   timed     new      432      3,159     32               540      VIOLATED (both)
//!   baseline  old      61,620   410,122   153,600          120,960  VIOLATED
//!   baseline  new      45,158   225,423   320              48,970   VIOLATED
//!
//! (The counters are events summed over all endings; a run now ends at its
//! first failed assertion, which is why the new untimed counts are smaller than
//! the old ones. The untimed rows have no clock, so the first-arrival rule does
//! not touch them. Before the rule, on 2026-10-12, the timed cell gave 2,176 /
//! 17,005 / 256 / 5,100: the executions the rule removed are those in which a
//! receive read a later arrival of another sender while an earlier one, already
//! in, expired unread.) The timed leg explores 3,591 executions (execs +
//! blocked) against 270,581 untimed, 75 times fewer.
//!
//! Certified witness of the timed cell without --keep-going (exit 101; the same
//! on seven runs, which differ in the schedule seed): the Admin's first receive
//! takes CheckSensors{1} at 15; the four readings that arrive before it (each
//! sensor's, sent at 0 and 10) are all marked [dropped], so both sensors are
//! written down as broken at 15 (the two readings sent at 20 are never read).
//! Before the rule this witness had no lost message: the Admin took the check
//! while the four readings, already in, expired unread. The drained version
//! discarded such runs (its drain could never complete). A blocked run is
//! either an actor waiting for a lost message or an Admin stopped at its failed
//! assertion.
//!
//! Two more cells (one sensor, two checks, otherwise the published model; the
//! second with safe readings and only the sensor property asserted):
//!
//!   cell        mode      version  execs    blocked   sensor_failures  dead     verdict
//!   S 1, R 2    timed     old      1        109,334   0                0        HOLD
//!   S 1, R 2    timed     new      300      7,228     236              1,392    VIOLATED (both)
//!   S 1, R 2    baseline  old, new no result in 300 s (counts)                  VIOLATED
//!   safe        timed     old      1        347       0                0        HOLD
//!   safe        timed     new      12       152       32               0        VIOLATED
//!   safe        baseline  old      21       440       48               0        VIOLATED
//!   safe        baseline  new      80       608       176              0        VIOLATED
//!
//! The Table 3 presets (--table3-row: S 2, R 1, random readings, property
//! rescue, timers separate), one worker. Verdicts are from runs without
//! --keep-going (they stop at the first counterexample), counts from runs with
//! it; "no result in N s" is a run stopped at N s, not a failure:
//!
//!   row   timed verdict       untimed verdict   timed counts (--keep-going)
//!   1     VIOLATED            VIOLATED          no result in 900 s
//!   2     VIOLATED            VIOLATED          no result in 900 s
//!   3-5   HOLD (vacuous)      VIOLATED          execs 4,096  blocked 69,440
//!   3-6   HOLD (vacuous)      VIOLATED          execs 4,096  blocked 69,440
//!   3-7   HOLD (vacuous)      VIOLATED          execs 4,096  blocked 69,440
//!   4     no result in 900 s, in all three columns
//!
//! Rows 3-5 to 3-7 hold at R = 1 for want of a mission: the only check is at
//! C = 1, before the earliest arrival of any reading (L = U = 2), so nothing
//! starts a mission and the program says so; the untimed leg has no clock,
//! lets a reading arrive before the check, and FIREs. The old version (512
//! executions, 45,696 blocked on each of these rows) held for the same
//! reason. With --rounds 2 the timed cell of row 3-5 gave no result in 300 s,
//! before the first-arrival rule as well. `--mode compare` on row 1 (untimed
//! leg first) gave no result in 900 s, the old version none in 30 min.
//!
//! Rows 1 and 2, first counterexample found (four runs each, which differ in
//! the schedule seed): a reading arrives at the instant of CheckSensors{1}
//! (4) and is read first, so a mission starts; the Ack lands at the instant of
//! its own CheckScientistAck (6) and the check is handled first, so a rescue
//! goes out although the Ack is in; and CheckRescue (9 in row 1, 10 in row 2)
//! finds no RescueReach. In row 2 the RescueReach is lost in every run (it
//! would arrive by 9, before the check). In row 1 it is lost in three runs;
//! in the fourth it is delivered with obstacle 1 and lands at the instant of
//! CheckRescue (9), which is handled first: a false death from ties alone on
//! the rescue path.
//!
//! WITHDRAWN. Everything this header reported before 2026-10-12 was obtained
//! with the drain, i.e. over the executions in which no message is lost or
//! expires unread, and is no longer claimed: the FALSE DEATH law (FIRE iff
//! Ds <= 2U + 2sd and Dr <= 2U + 2sd + OBST), the FALSE SENSOR FAILURE law
//! (C <= U + sd, or the alignment condition (m + 1)P + U + sd >= jC), the
//! reproduction of SCP 2014 Table 3 (6/6) with the observation that its
//! C > Dr/2 fix fails at Dr = 5, and the overlapping-mission approximation of
//! FOCLASA rows 1-2. The timelines those results described (the same-instant
//! ties of row 1, the stale Ack of a rescued mission, 2C <= Dr - 2 netDelay
//! for two rescues in flight) are still executions of this model; they just
//! no longer separate FIRE from HOLD, because a lost message fires wherever
//! a check or a mission is reached.
//!
//! CLI misuse (e.g. --sensors 3, --mode foo, --l-ratio 2, a mission
//! guard violation) exits 2.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use traceforge::thread::{self, ThreadId};
use traceforge::{Config, Stats};

// Outcome counters: tallies of the Admin's observations, published when
// the Admin ends. Accumulated across all explored executions of one verify
// run (revisited prefixes may re-count, so treat them as zero/nonzero
// evidence, not exact per-execution tallies). Reset before each verify.
static CHECKS_OK: AtomicUsize = AtomicUsize::new(0);
static SENSOR_FAILURES: AtomicUsize = AtomicUsize::new(0);
/// Readings read after check k although they could have been read
/// before it: earliest arrival seq*P + L no later than the check's latest
/// pick-up k*C + sd, ties included (computed from the send index, never
/// from a clock; a counter only, it prunes nothing). Exact at sd = 0; at
/// sd > 0 a reading that arrives after the check is read after it (first
/// arrival), so this overcounts there. Nonzero on a sensor hold at sd = 0
/// shows that a reading really missed a check and the property still
/// held.
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
}

#[derive(Clone, Debug, PartialEq)]
enum RescueMsg {
    Go { round: u32, admin: ThreadId },
}

/// Admin -> timer thread (`--timers separate` only). A timer thread is the
/// Admin's own `self.m() after(d)` for one message kind, run on its own
/// sender so that only same-kind timers share a FIFO channel.
#[derive(Clone, Debug, PartialEq)]
enum TimerMsg {
    Arm { round: u32, admin: ThreadId },
}

/// How the Admin's two mission timers reach it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Timers {
    /// Self-messages on the Admin-to-Admin channel. TraceForge channels are
    /// FIFO, so a timer sent later cannot overtake one sent earlier even when
    /// it is due earlier; the CLI therefore refuses overlapping missions.
    SelfChannel,
    /// One sender thread per timer kind. Timers of one kind all carry the same
    /// delay, so their send order IS their due order, and timers of different
    /// kinds are unordered: exactly Timed Rebeca's earliest-time-tag choice
    /// over the Admin's own messages. Needs sd = 0 (see resolve()).
    Separate,
}

impl Timers {
    fn name(self) -> &'static str {
        match self {
            Timers::SelfChannel => "self",
            Timers::Separate => "separate",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerKind {
    ScientistAck,
    Rescue,
}

/// Checker exploration strategy: a Config option only, the verified
/// program is the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Parallel {
    None,
    Shared,
    Partitioned,
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
    /// Sensor periods (Listing 6: sensor0period, sensor1period).
    period: [u64; 2],
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
    timers: Timers,
    parallel: Parallel,
}

impl Params {
    /// Readings sensor `i` sends: the last one is sent strictly after the
    /// last check can be read (R*C + sd), so a "no reading since the
    /// last check" verdict always has a later reading to validate it.
    fn readings_per_sensor(&self, i: usize) -> u32 {
        ((self.rounds as u64 * self.check + self.sd) / self.period[i]) as u32 + 2
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
    let n = p.readings_per_sensor(index as usize);
    let period = p.period[index as usize];
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
            traceforge::sleep(period);
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
        }
    }
}

// =====================================================================
// Timer thread (--timers separate): the Admin's self.m() after(d) for
// one message kind, forwarded with exact transit d
// =====================================================================

fn timer(kind: TimerKind, delay: u64) {
    loop {
        match traceforge::recv_msg_block_timed::<TimerMsg>() {
            // The Arm arrives with exact transit 0 and, at sd = 0, is read at
            // that very instant, so the timer lands exactly `delay` after the
            // Admin armed it, as Rebeca's time tag now() + delay.
            TimerMsg::Arm { round, admin } => {
                let m = match kind {
                    TimerKind::ScientistAck => AdminMsg::CheckScientistAck { round },
                    TimerKind::Rescue => AdminMsg::CheckRescue { round },
                };
                traceforge::send_msg_timed(admin, m, delay, delay);
            }
        }
    }
}

// =====================================================================
// Admin: one receive loop, Listing 5's handlers and flags
// =====================================================================

/// Observation counters of one Admin run, published when the run ends. The
/// two violation counters (SENSOR_FAILURES, SCIENTIST_DEAD) are not here:
/// a failed assertion ends the Admin's run before it publishes, so they are
/// raised at the assertion site itself.
#[derive(Default)]
struct Tally {
    checks_ok: usize,
    late_reports: usize,
    missions: usize,
    acked_in_time: usize,
    rescues_sent: usize,
    reached_in_time: usize,
}

impl Tally {
    fn publish(&self) {
        CHECKS_OK.fetch_add(self.checks_ok, Ordering::Relaxed);
        LATE_REPORTS.fetch_add(self.late_reports, Ordering::Relaxed);
        MISSIONS.fetch_add(self.missions, Ordering::Relaxed);
        ACKED_IN_TIME.fetch_add(self.acked_in_time, Ordering::Relaxed);
        RESCUES_SENT.fetch_add(self.rescues_sent, Ordering::Relaxed);
        REACHED_IN_TIME.fetch_add(self.reached_in_time, Ordering::Relaxed);
    }
}

/// What the Admin's bounded run still has to see: the R sensor checks, and
/// for every mission it started that mission's CheckScientistAck, and for
/// every rescue it sent that rescue's CheckRescue. Counts only: no order
/// check, and no wait for readings, Acks or RescueReaches, since a message
/// that was lost is simply never read. The timers are
/// self-sends and may be lost too; the Admin then waits for ever (a
/// blocked execution, with the twin where the timer arrives judged).
struct Pending {
    rounds: u32,
    next_check: u32,
    missions: usize,
    ack_checks: usize,
    rescues: usize,
    rescue_checks: usize,
}

impl Pending {
    fn new(p: &Params) -> Self {
        Pending { rounds: p.rounds, next_check: 1, missions: 0, ack_checks: 0, rescues: 0, rescue_checks: 0 }
    }

    fn done(&self) -> bool {
        self.next_check > self.rounds && self.ack_checks == self.missions && self.rescue_checks == self.rescues
    }
}

/// Rebeca `self.m() after(delay)` for one of the Admin's two mission timers.
/// Without timer threads it is a self-message on the Admin's own channel;
/// with them it is an Arm (exact transit 0) that the kind's thread turns
/// into the timer, due exactly `delay` later.
fn arm(me: ThreadId, timers: Option<(ThreadId, ThreadId)>, kind: TimerKind, round: u32, delay: u64) {
    match timers {
        None => {
            let m = match kind {
                TimerKind::ScientistAck => AdminMsg::CheckScientistAck { round },
                TimerKind::Rescue => AdminMsg::CheckRescue { round },
            };
            traceforge::send_msg_timed(me, m, delay, delay);
        }
        Some((ack_timer, rescue_timer)) => {
            let to = match kind {
                TimerKind::ScientistAck => ack_timer,
                TimerKind::Rescue => rescue_timer,
            };
            traceforge::send_msg_timed(to, TimerMsg::Arm { round, admin: me }, 0, 0);
        }
    }
}

fn admin(p: Params, scientist: ThreadId, rescue: ThreadId, timers: Option<(ThreadId, ThreadId)>) {
    let me = thread::current().id();
    let n_sensors = p.sensors as usize;

    // Listing 5 state variables (its sticky sensorFailure and scientistDead
    // are the two assertions below).
    let mut reported = [false; 2];
    let mut sensor_value = [0u8; 2];
    let mut scientist_ack = false;
    let mut scientist_reached = false;

    let mut pend = Pending::new(&p);
    let mut t = Tally::default();

    while !pend.done() {
        match traceforge::recv_msg_block_timed::<AdminMsg>() {
            AdminMsg::Report { sensor, seq, value } => {
                let s = sensor as usize;
                // Counter only: read after the last processed check k,
                // but could it have been read before that check?
                let k = pend.next_check as u64 - 1;
                if k >= 1 && (seq as u64) * p.period[s] + p.l <= k * p.check + p.sd {
                    t.late_reports += 1;
                }
                reported[s] = true;
                sensor_value[s] = value;
            }
            AdminMsg::CheckSensors { round } => {
                pend.next_check += 1;
                for s in 0..n_sensors {
                    if reported[s] {
                        reported[s] = false;
                        t.checks_ok += 1;
                    } else {
                        // sensorFailure = true. Sensors never break here,
                        // so this is a false sensor failure, whether the
                        // reading is late or lost: the property is
                        // violated here and now.
                        SENSOR_FAILURES.fetch_add(1, Ordering::Relaxed);
                        if p.property.sensor() {
                            traceforge::assert(SENSOR_BROKEN.load(Ordering::Relaxed));
                        }
                    }
                }
                let danger = sensor_value[..n_sensors].iter().any(|&v| v > 3);
                if danger {
                    // scientist.abortPlan() after(netDelay);
                    traceforge::send_msg(scientist, SciMsg::AbortPlan { round, admin: me });
                    // self.checkScientistAck() after(scientistDeadline);
                    arm(me, timers, TimerKind::ScientistAck, round, p.ack_deadline);
                    t.missions += 1;
                    pend.missions += 1;
                }
                // self.checkSensors() after(adminCheckDelay): pre-armed
                // by main (doc header).
            }
            AdminMsg::Ack { .. } => scientist_ack = true,
            AdminMsg::CheckScientistAck { round } => {
                pend.ack_checks += 1;
                if !scientist_ack {
                    // rescue.go() after(netDelay);
                    traceforge::send_msg(rescue, RescueMsg::Go { round, admin: me });
                    // self.checkRescue() after(rescueDeadline);
                    arm(me, timers, TimerKind::Rescue, round, p.rescue_deadline);
                    t.rescues_sent += 1;
                    pend.rescues += 1;
                } else {
                    t.acked_in_time += 1;
                }
                scientist_ack = false;
            }
            AdminMsg::RescueReach { .. } => scientist_reached = true,
            AdminMsg::CheckRescue { .. } => {
                pend.rescue_checks += 1;
                if !scientist_reached {
                    // scientistDead = true, FOCLASA's "mission failed". The
                    // scientist is always fine here, so this is a false
                    // death, whether the Ack or the RescueReach was late
                    // or lost: violated here and now.
                    SCIENTIST_DEAD.fetch_add(1, Ordering::Relaxed);
                    if p.property.rescue() {
                        traceforge::assert(SCIENTIST_LOST.load(Ordering::Relaxed));
                    }
                } else {
                    scientist_reached = false;
                    t.reached_in_time += 1;
                }
            }
        }
    }

    // Bounded run over: publish the tallies.
    t.publish();
}

// =====================================================================
// Verifier setup
// =====================================================================

fn build_config(mode: Mode, p: Params, keep_going: bool) -> Config {
    let mut builder = Config::builder().with_all_sends_lossy().with_progress_report(usize::MAX);
    builder = match p.parallel {
        Parallel::None => builder,
        Parallel::Shared => builder.with_parallel(true),
        Parallel::Partitioned => builder.with_partitioned_parallelization(true),
    };
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
        // The scientist, the rescue team and the timer threads are
        // reactive daemons: nothing releases them, the run ends when no
        // actor can move. The Admin and the sensors are joined.
        let sci_id = thread::spawn_daemon(scientist).thread().id();
        let res_id = thread::spawn_daemon(move || rescue(p.obstacle)).thread().id();
        let timer_ids = match p.timers {
            Timers::SelfChannel => None,
            Timers::Separate => Some((
                thread::spawn_daemon(move || timer(TimerKind::ScientistAck, p.ack_deadline))
                    .thread()
                    .id(),
                thread::spawn_daemon(move || timer(TimerKind::Rescue, p.rescue_deadline))
                    .thread()
                    .id(),
            )),
        };
        let adm = thread::spawn(move || admin(p, sci_id, res_id, timer_ids));
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

/// "10" when every sensor shares one period, "2/3" otherwise.
fn periods(p: &Params) -> String {
    if p.sensors == 1 || p.period[0] == p.period[1] {
        p.period[0].to_string()
    } else {
        format!("{}/{}", p.period[0], p.period[1])
    }
}

fn print_one(label: &str, p: Params, stats: &Stats, dur: Duration, c: Counts) {
    println!(
        "{label:<9} S={s} R={r} readings={rd} property={pr} timers={tm}  L={l} U={u} \
         sd={sd} P={per} C={chk} \
         Ds={ds} Dr={dr} OBST={ob}  execs={execs:<6} blocked={block:<6} impossible={imp:<6} checks_ok={cok} \
         late_reports={lr} sensor_failures={sf} missions={mi} acked={ak} rescues={rs} reached={re} dead={de} \
         time={dur:?}",
        s = p.sensors, r = p.rounds, rd = p.readings.name(), pr = p.property.name(),
        tm = p.timers.name(),
        l = p.l, u = p.u, sd = p.sd, per = periods(&p), chk = p.check,
        ds = p.ack_deadline, dr = p.rescue_deadline, ob = p.obstacle,
        execs = stats.execs, block = stats.block, imp = stats.timeline_impossible,
        cok = c.checks_ok, lr = c.late_reports, sf = c.sensor_failures, mi = c.missions, ak = c.acked,
        rs = c.rescues, re = c.reached, de = c.dead, dur = dur,
    );
    // The counters above are raised by PROGRAM code, so they also see endings
    // that the (C6b) timeout-miss condition later judges to admit no timeline
    // (such an ending runs to completion, raises the counters, and is only
    // then discarded). A counter of ZERO is sound: nothing happened in any
    // ending, feasible or not. A counter GREATER THAN ZERO is an upper bound.
    if stats.timeline_impossible > 0 && (c.dead > 0 || c.sensor_failures > 0 || c.late_reports > 0) {
        println!(
            "NOTE: {} of the {} endings explored admit no timeline and were counted above too; \
             dead/sensor_failures/late_reports are upper bounds here. Re-run without --keep-going \
             (exit 101 = the engine certified a counterexample) before quoting a failure count.",
            stats.timeline_impossible,
            stats.execs + stats.block + stats.timeline_impossible
        );
    }
}

fn print_compare(p: Params, baseline: (Stats, Duration), timed: (Stats, Duration), bc: Counts, tc: Counts) {
    let (b_stats, b_dur) = baseline;
    let (t_stats, t_dur) = timed;
    println!();
    println!("Sensor network with rescue escalation: MUST vs MUST-timed");
    println!("=========================================================");
    println!(
        "S = {}  R = {}  L = {}  U = {}  sd = {}  P = {}  C = {}  Ds = {}  Dr = {}  OBST = {}  \
         timers = {}",
        p.sensors, p.rounds, p.l, p.u, p.sd, periods(&p), p.check, p.ack_deadline,
        p.rescue_deadline, p.obstacle, p.timers.name()
    );
    println!();
    println!("{:<10} {:>10} {:>10} {:>14}", "mode", "execs", "blocked", "time");
    println!("{:<10} {:>10} {:>10} {:>14?}", "baseline", b_stats.execs, b_stats.block, b_dur);
    println!("{:<10} {:>10} {:>10} {:>14?}", "timed", t_stats.execs, t_stats.block, t_dur);
    println!();
    // Executions EXPLORED is execs + block: a violating execution ends at
    // its assertion and a run whose actor waits for a lost message blocks,
    // so both are filed under `block`. Dividing complete executions alone
    // overstates the reduction.
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
    /// Per-sensor period overrides (Listing 6 sensor0period, sensor1period).
    sensor_period_ratio: [Option<f64>; 2],
    readings: Readings,
    property: Property,
    allow_overlap: bool,
    timers: Timers,
    parallel: Parallel,
    keep_going: bool,
}

/// SCP 2014 Table 3 (Reynisson et al., Science of Computer Programming 89,
/// p. 56), in absolute units: (netDelay, admin period, sensor0 period,
/// sensor1 period, scientist deadline, rescue deadline). Row 3 lists three
/// rescue deadlines, so it is split into 3-5, 3-6 and 3-7.
fn table3_row(row: &str) -> Option<(u64, u64, u64, u64, u64, u64)> {
    match row {
        "1" => Some((1, 4, 2, 3, 2, 3)),
        "2" => Some((1, 4, 2, 3, 2, 4)),
        "3" | "3-5" => Some((2, 1, 1, 1, 4, 5)),
        "3-6" => Some((2, 1, 1, 1, 4, 6)),
        "3-7" => Some((2, 1, 1, 1, 4, 7)),
        "4" => Some((2, 4, 1, 1, 4, 7)),
        _ => None,
    }
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
        sensor_period_ratio: [None, None],
        readings: Readings::Danger,
        property: Property::Both,
        allow_overlap: false,
        timers: Timers::SelfChannel,
        parallel: Parallel::None,
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
                    other => cli_bail(&format!("invalid --readings: {other} (expected danger|random|safe)")),
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
            "--sensor0-period-ratio" => {
                a.sensor_period_ratio[0] = Some(parse_num(
                    next_val(&mut args, "--sensor0-period-ratio"),
                    "--sensor0-period-ratio",
                ))
            }
            "--sensor1-period-ratio" => {
                a.sensor_period_ratio[1] = Some(parse_num(
                    next_val(&mut args, "--sensor1-period-ratio"),
                    "--sensor1-period-ratio",
                ))
            }
            "--timers" => {
                let v = next_val(&mut args, "--timers");
                a.timers = match v.as_str() {
                    "self" => Timers::SelfChannel,
                    "separate" => Timers::Separate,
                    other => cli_bail(&format!("invalid --timers: {other} (expected self|separate)")),
                };
            }
            "--parallel" => {
                let v = next_val(&mut args, "--parallel");
                a.parallel = match v.as_str() {
                    "none" => Parallel::None,
                    "shared" => Parallel::Shared,
                    "partitioned" => Parallel::Partitioned,
                    other => cli_bail(&format!(
                        "invalid --parallel: {other} (expected none|shared|partitioned)"
                    )),
                };
            }
            "--table3-row" => {
                let v = next_val(&mut args, "--table3-row");
                let (n, c, p0, p1, ds, r) = table3_row(&v).unwrap_or_else(|| {
                    cli_bail(&format!(
                        "invalid --table3-row: {v} (expected 1, 2, 3, 3-5, 3-6, 3-7 or 4)"
                    ))
                });
                // Listing 6 verbatim: fixed netDelay (L = U = n), no scheduler
                // slack, two sensors, ?(2, 4) readings, obstacle ?(0, 1), the
                // mission-failure property, and due-time-ordered timers.
                // Ratios are over U = n; flags given AFTER this one override it.
                let f = n as f64;
                a.u = n;
                a.l_ratio = 1.0;
                a.sd_ratio = 0.0;
                a.sensors = 2;
                a.check_ratio = c as f64 / f;
                a.period_ratio = p0 as f64 / f;
                a.sensor_period_ratio = [Some(p0 as f64 / f), Some(p1 as f64 / f)];
                a.ack_deadline_ratio = ds as f64 / f;
                a.rescue_deadline_ratio = r as f64 / f;
                a.obstacle_ratio = 1.0 / f;
                a.readings = Readings::Random;
                a.property = Property::Rescue;
                a.timers = Timers::Separate;
            }
            "--allow-overlap" => a.allow_overlap = true,
            "--keep-going" => a.keep_going = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: sensor_network_timed [--mode baseline|timed|compare] [--sensors 1|2] \
                     [--rounds R] [--u U] [--l-ratio LR] [--sd-ratio SR] [--period-ratio PR] \
                     [--check-ratio CR] [--ack-deadline-ratio AR] [--rescue-deadline-ratio RR] \
                     [--obstacle-ratio OR] [--readings danger|random|safe] \
                     [--property both|sensor|rescue] [--allow-overlap] \
                     [--sensor0-period-ratio PR] [--sensor1-period-ratio PR] \
                     [--timers self|separate] [--table3-row 1|2|3|3-5|3-6|3-7|4] \
                     [--parallel none|shared|partitioned] [--keep-going]\n\
                     All ratios are over U and rounded. Defaults: S=2, R=1, U=1, L/U=1, sd/U=0, \
                     P/U=10, C/U=15, Ds/U=5, Dr/U=5, OBST/U=1, readings=danger, property=both \
                     (the published model with fixed netDelay 1).\n\
                     With --timers self, missions must not overlap: Ds + Dr + 3sd < C, unless \
                     --allow-overlap, which still requires Ds + 2sd < C and Dr + 2sd <= C. \
                     --timers separate lifts both but needs sd = 0.\n\
                     --table3-row reproduces SCP 2014 Table 3 (Listing 6); later flags override it.\n\
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
    let common = scale(a.period_ratio, "--period-ratio");
    let per = |i: usize, flag: &str| a.sensor_period_ratio[i].map_or(common, |r| scale(r, flag));
    let p = Params {
        sensors: a.sensors,
        rounds: a.rounds,
        u: a.u,
        l: scale(a.l_ratio, "--l-ratio"),
        sd: scale(a.sd_ratio, "--sd-ratio"),
        period: [per(0, "--sensor0-period-ratio"), per(1, "--sensor1-period-ratio")],
        check: scale(a.check_ratio, "--check-ratio"),
        ack_deadline: scale(a.ack_deadline_ratio, "--ack-deadline-ratio"),
        rescue_deadline: scale(a.rescue_deadline_ratio, "--rescue-deadline-ratio"),
        obstacle: scale(a.obstacle_ratio, "--obstacle-ratio"),
        readings: a.readings,
        property: a.property,
        timers: a.timers,
        parallel: a.parallel,
    };
    if p.l > p.u {
        cli_bail("transit lower bound L must be <= U (check --l-ratio)");
    }
    if p.period[..p.sensors as usize].iter().any(|&x| x < 1) {
        cli_bail("every sensor period must round to >= 1 (check the --*period-ratio flags)");
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
    if p.readings != Readings::Safe && p.timers == Timers::Separate {
        // A timer thread reads its Arm and re-sends it at once only if it
        // reads the Arm the instant it arrives, i.e. with no pick-up slack.
        // sd = 0 is also Timed Rebeca's own scheduler (no slack at all).
        if p.sd != 0 {
            cli_bail("--timers separate needs sd = 0 (check --sd-ratio)");
        }
        // A Go that reaches the Rescue during its obstacle delay expires
        // unread at sd = 0 (Rebeca would hold it in the bag): the model's
        // semantics, explored and judged like any other run.
        // No overlap guard: timers of one kind share one delay, so their
        // send order is their due order, and kinds do not share a channel.
    }
    if p.readings != Readings::Safe && p.timers == Timers::SelfChannel {
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
