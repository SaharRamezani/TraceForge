# v11 results summary (generated from table_v11.csv)

Exec. / Viol. / Blocked per side; Time = median of the finished repetitions (reps in brackets). Exec. = healthy complete executions, Viol. = executions with a failed assertion (once per graph), Blocked = not violated and not finished. One pinned core per run.

## Chandra-Toueg
- largest both-finished: `CT delta=2 phi=4 to=13 H=18`  Must 19,900,392 / 6,368 / 0 in 16.4 min (x2); Mustard 7,016,455 / 764 / 0 in 94.5 min (x1); host brain10
- flip cells: none

## Sensor network
- largest both-finished: `Sensor --period-ratio 5`  Must 19,589,858 / 14,707,244 / 51,804,061 in 104.6 min (x1); Mustard 13,200 / 16,740 / 65,275 in 89 s (x3); host brain10
- flip cells: none
- largest Mustard-only: `Sensor --table3-row 2`  Mustard 172,800 / 213,120 / 1,569,280 in 21.0 min; Must: >7163 (DNF)
  - Must DNF after 2.0 h (79400000 explored); Mustard finished in 21 min. No two finished smaller rungs to extrapolate from

## NRP
- largest both-finished: `NRP noopt R=3`  Must 830,390 / 722,620 / 4,295,035 in 394 s (x3); Mustard 729 / 4,872 / 41,074 in 30 s (x3); host brain10
- flip cells: none
- largest Mustard-only: `NRP noopt R=4`  Mustard 6,561 / 171,920 / 1,913,197 in 34.7 min; Must: >7163 (DNF)
  - Must DNF after 2.0 h (80000000 explored); Mustard finished in 35 min. Estimated finish: more than a day (extrapolated, not measured: x821.5 per R step from rungs [(2, 0.5), (3, 394.3)])

## Spanning tree
- largest both-finished: `STP lost-hellos R=13 Fd=24 L/U=0 sd/U=0`  Must 59,862 / 0 / 8,426,133 in 549 s (x3); Mustard 25,250 / 0 / 2,946,560 in 69.5 min (x1); host brain05
- flip cells: none

## Lease lock
- largest both-finished: `Lease C=2 R=5 L=1 U=2 sd=0 TTL=10 PAUSE=9`  Must 1,889,568 / 1,111,508 / 8,336,312 in 30.0 min (x2); Mustard 32,768 / 21,648 / 349,880 in 12.5 min (x3); host brain10
- largest flip (Must violates, Mustard holds), 9 flip cells: `Lease C=5 R=1 L=0 U=2 sd=1 TTL=10 PAUSE=3`  Must 29,160 / 943,680 / 904,712 in 224 s; Mustard 29,160 / 0 / 904,712 in 544 s
- largest Mustard-only: `Lease C=3 R=3 L=1 U=2 sd=0 TTL=10 PAUSE=9`  Mustard 110,592 / 139,600 / 2,187,408 in 68.0 min; Must: >6054 (DNF)
  - Must DNF after 1.7 h (39800000 explored); Mustard finished in 68 min. No two finished smaller rungs to extrapolate from

## Lease lock (with fencing)
- largest both-finished: `Lease + fencing C=5 R=1 L=0 U=2 sd=1 TTL=10 PAUSE=4`  Must 1,247,400 / 0 / 11,765,192 in 22.7 min (x2); Mustard 29,160 / 0 / 904,712 in 545 s (x3); host brain10
- flip cells: none

## SWIM
- largest both-finished: `SWIM N=10 R=1 L=0 U=2 sd=1 Wp=2 Ws=6`  Must 19,684 / 7,371,918 / 177,156 in 17.7 min (x2); Mustard 19,684 / 6,723,651 / 229,644 in 61.0 min (x1); host brain05
- flip cells: none

## Alternating bit
- largest both-finished: `ABP-file par K=5 R=2 W=2U`  Must 21,891,101 / 3,109,929 / 44,528,549 in 72.7 min (x1); Mustard 680,302 / 60,702 / 1,214,410 in 14.6 min (x3); host brain05
- largest flip (Must violates, Mustard holds), 3 flip cells: `ABP-file par K=5 R=2 W=3U`  Must 21,891,101 / 3,109,929 / 44,528,549 in 71.8 min; Mustard 16,807 / 0 / 39,215 in 22 s
- largest Mustard-only: `ABP-file abp K=2 R=9 W=3U`  Mustard 1,046,529 / 0 / 2,095,105 in 43.0 min; Must: >6052 (DNF)
  - Must DNF after 1.7 h (61400000 explored); Mustard finished in 43 min. Estimated finish: more than a week (extrapolated, not measured: x24.7 per R step from rungs [(4, 80.0), (5, 1975.6)])

## PAR
- largest both-finished: `PAR dK=3 dL=3 dR=1 To=9 K=7 R=1`  Must 51,701,027 / 1,878,176 / 39,546,805 in 108.9 min (x1); Mustard 171,875 / 0 / 124,996 in 165 s (x3); host brain10
- largest flip (Must violates, Mustard holds), 11 flip cells: `PAR dK=3 dL=3 dR=1 To=8 K=7 R=1`  Must 51,701,027 / 1,878,176 / 39,546,805 in 108.2 min; Mustard 171,875 / 0 / 124,996 in 168 s
- largest Mustard-only: `PAR dK=3 dL=3 dR=1 To=9 K=5 R=2`  Mustard 6,028,911 / 0 / 2,178,184 in 76.7 min; Must: >6054 (DNF)
  - Must DNF after 1.7 h (88000000 explored); Mustard finished in 77 min. Estimated finish: more than a week (extrapolated, not measured: x139.5 per K step from rungs [(3, 32.6), (4, 4549.5)])

## BRP
- largest both-finished: `BRP F=1 n=5 MAX=1 TD=1 T1=3 TR=9 SYNC=9 k=2 (p2)`  Must 14,500,281 / 959,143 / 13,594,265 in 31.1 min (x2); Mustard 24,316 / 0 / 30,985 in 42 s (x3); host brain05
- largest flip (Must violates, Mustard holds), 13 flip cells: `BRP F=1 n=5 MAX=1 TD=1 T1=3 TR=9 SYNC=9 k=2 (p2)`  Must 14,500,281 / 959,143 / 13,594,265 in 31.1 min; Mustard 24,316 / 0 / 30,985 in 42 s
- largest Mustard-only: `BRP F=1 n=7 MAX=1 TD=1 T1=3 TR=9 SYNC=9 k=2 (p2)`  Mustard 2,052,084 / 0 / 2,605,125 in 93.5 min; Must: >6051 (DNF)
  - Must DNF after 1.7 h (71800000 explored); Mustard finished in 1.6 h. Estimated finish: more than a week (extrapolated, not measured: x45.7 per n step from rungs [(4, 40.8), (5, 1865.6)])

## Online shop
- largest both-finished: `Shop v2 C=2 N=2 R=4 S=20 D=7`  Must 17,920 / 1,020,432 / 0 in 142 s (x3); Mustard 17,920 / 691,512 / 0 in 568 s (x3); host brain10
- flip cells: none

## YARN
- largest both-finished: `YARN K=2 QS=3 DD=8 U=0 H=9 miss`  Must 3,859,600 / 45,689 / 28,028,707 in 53.9 min (x1); Mustard 650 / 372 / 13,363 in 11 s (x3); host brain05
- largest flip (Must violates, Mustard holds), 3 flip cells: `YARN K=1 QS=2 DD=7 U=0 H=12 late-report`  Must 3,513 / 524 / 22,272 in 2 s; Mustard 32 / 0 / 523 in 0 s
- largest Mustard-only: `YARN K=3 QS=4 DD=8 U=0 H=9 miss`  Mustard 136,704 / 17,336 / 3,443,001 in 76.9 min; Must: >7162 (DNF)
  - Must DNF after 2.0 h (61800000 explored); Mustard finished in 77 min. No two finished smaller rungs to extrapolate from
