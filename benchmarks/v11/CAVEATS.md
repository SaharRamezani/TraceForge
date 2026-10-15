# v11 campaign: caveats that must travel with the numbers

Campaign: working tree = HEAD 24643fc + the ten daemon-free rewrites in `rewrites_vs_24643fc.patch`
(unstaged), 2026-10-14 19:50 to 21:50, brain05 and brain10, unchanged `benchmarks/v9/sweep_v9.py`
(md5 52188b86...). One pinned physical core per run, sequential, no `--parallel`, SMT siblings idle, ballast on
idle cores. Both sides of a cell and all its repetitions run on the same machine. nisshi is out (user, 2026-10-14).
The v10 caveats (`benchmarks/v10/CAVEATS.md`) still apply where a program was not changed: section 1 (CT lost
`EndOfRun`), section 3 (1000 events per thread), section 4 (baseline over-approximation, per-send windows,
first-arrival rule, Exec. definition).

## 1. What changed against v10: no thread is a daemon

User ruling 2026-10-14: every actor is a paper thread (`thread::spawn`), bounded on rounds, and a termination
message is accepted as a harness that is not part of the protocol. In v10 a reactor was a daemon and
`check_blocked` skipped a blocked daemon (`exec_graph.rs:441-444`), so a run in which a daemon waited for a lost
message was filed under healthy `execs`. Now it is Blocked. Acceptance of every rewrite: explored (execs +
blocked) and violating equal to the HEAD binary in all compared cells, except where a row below says otherwise.

| program | bounded on | termination harness | against v10 |
|---|---|---|---|
| spanning tree | `for _ in 0..rounds` in bridge B | none | explored and violating equal (78/78 cell pairs) |
| alternating bit | receiver ends on `Done` | none | explored and violating equal (60 cells); Blocked holds the Done-dropped twin and the give-ups |
| PAR | receiver ends after K deliveries (ack still sent after the K-th) | none | violating equal; explored 11 to 59% smaller (the tail after the K-th delivery is not explored) |
| online shop | service quota, inventory `checks_due`, direct service read | `Terminate` only for `--assign any` with N >= 2 (not used) | identical counts |
| YARN | RM runs to the horizon and until all apps ended | `Stop` chain RM -> AM k-1 -> ... -> AM 0 | execs equal "no message lost"; Blocked grows by lost-Stop fates; throughput Viol. adds K * V_end |
| sensor network | `--rounds` check periods | `Stop` chain admin -> scientist -> rescue -> ack timer -> rescue timer | Blocked grows by lost-Stop fates |
| NRP | DCN loop until ticks, owed and peer finished are done | `Finished` (DCN2 -> DCN1), `Stop` (DCN1 -> SW) | explored = A + 2B + 3C; Viol. exactly x2 in the default R=3 cells |
| SWIM | reactors end on `Stop` | prober sends `Stop` to the target, then to each bystander, joining each | Viol. and Blocked inflated by the Stop fates, differently per mode; blocked - violating = (N-1) * execs in the small cells |
| BRP | rounds only | none: no Done, no Terminate, no monitor | timed violating = v10 / 2 (v10 counted the Done fates twice); baseline counts shrink (Must over-approximates the bounded R) |
| lease (both) | storage `for _ in 0..C*R` | none | identical counts in 56 of 56 pairs |
| Chandra-Toueg | unchanged (no daemons) | none | identical |

Reading the columns:
- Exec. = healthy complete executions. After the rewrite it is a small fraction of explored, because every send
  (the harness messages too) is lossy and a lost message that a thread needs now leaves it Blocked.
- Viol. = executions violating the property, once per graph. Where a termination message is lossy and sent after
  the point of violation, each violating protocol execution appears once per fate of the Stop messages. That is
  why NRP Viol. is exactly twice the daemon-era value and why SWIM Viol. differs between Must and Mustard by more
  than the protocol explains. The columns `v10_*` of `table_v11.csv` carry the daemon-era numbers for comparison.
- Blocked = block - violating = did not finish and did not violate.

## 2. Measurement protocol

- Cells = the v10 cells (all programs but nisshi) plus, wave 2 (appended 20:09 to the live plans, ids `v11w2-*`),
  higher rungs on every ladder where the first queue drained after 12 minutes and 160 cores were idle.
- Repetitions: 3 for cells predicted under 15 min, 2 under 1 h, otherwise 1. Wave-2 cells ran once; short ones got
  repetitions afterwards where time allowed. Time = median wall of the finished repetitions, spread reported.
- A side that did not finish by 21:50 is DNF with its last progress line; the notes column gives a geometric
  extrapolation from the two finished rungs below it (same other parameters), always marked "extrapolated, not
  measured".
- Cross-machine controls: 9 short cells ran on both machines, counts identical, wall ratio within a few percent
  (brain05 about 1.5% faster). Counts of repetitions are compared inside each cell (determinism check).
- Logs were grepped for "Large thread size" and "panicked".

## 3. Open points for the author

1. NRP and SWIM Viol. include the fates of lossy termination messages. If that is not acceptable, the options are a
   termination design whose messages cannot be lost (a reliable-send exemption does not exist in the tool; every
   send is lossy by decision 2026-09-29), or reporting the protocol-level count (execs-only comparison plus the
   daemon-era Viol. as a separate column).
2. BRP: alternative D (keeps Done) exists as a patch; delivered file is rounds-only.
3. The `lease_timed.rs` header paragraph "Known limitation" says expiry needs PAUSE far above TTL; the agent found
   8 of 1448 blocked graphs at C=2 R=2 PAUSE=3 < TTL=10 where the Grant-to-Release path (four hops of up to U plus
   the pause) exceeds TTL. The paragraph should be reworded.
4. Chandra-Toueg: the lost-`EndOfRun` horizon artifact is not fixed; CT counts remain upper bounds.
5. Chandra-Toueg rounds differ between the modes (see section 4); decision pending: drop the row from the
   Must-vs-Mustard table (recommended), make it count-based in both modes, or keep it with a footnote.

## 4. Do Must and Mustard run the same number of rounds? (audit, 2026-10-14, after the user's remark)

Criterion: the bound of every thread is a count of protocol-level events that is the same loop in both modes;
no time-stamped message acts as the cut. The paper (06-evaluation.tex) says "workload bounds are kept the same
in the two analyses". Method: read every loop bound; grep for mode-conditional code (none in any program body;
the `Mode::` checks only print notes after the run).

| program | what the round parameter counts | verdict |
|---|---|---|
| lease, lease + fencing | R rounds of clients and service, storage C * R writes | same count in both modes |
| SWIM | R probe periods; reactors end on the prober's Stop | same |
| NRP | `ticks_left = rounds` heartbeat ticks | same |
| YARN | RM ticks 0..H (a count kept by the RM) | same |
| alternating bit | K messages, R retries, `Done` | same |
| PAR | K accepted new frames, R retries | same |
| BRP | F files x n chunks x MAX retransmissions; ends at the protocol's own z = TR timer (no clock: that timer may fire early, a property of the baseline abstraction) | same count |
| online shop | service quota and inventory checks per customer * R | same |
| spanning tree | R root hellos (count); C's waits are cut by a local budget R*H + 2L | same count in both modes, derived from time parameters (Must's counts move with Fd at fixed R) |
| sensor network | R admin checks (count); readings per sensor = (R*C + sd)/period + 2 | same count in both modes, derived from time parameters |
| Chandra-Toueg | H: timed = main's `EndOfRun` at time exactly H (a time cut); untimed = the time stamp is abstracted, `EndOfRun` is an ordinary message, only the caps (sender floor(H/3) sends, receiver H ticks) bound the run | DIFFERENT run length per mode |

Chandra-Toueg cells of this campaign sit at to = 3 Phi + Delta - 1: (Delta 2, Phi 2, to 7) and (Delta 2, Phi 4,
to 13), H from 10 to 19. A count-based version would charge Phi per receiver tick against a local budget (as the
spanning tree does): at Phi 4, H = 19 allows 4 receiver ticks while suspicion needs at least 15, so only Phi <= 2
cells stay meaningful.
