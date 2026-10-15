# v12 report: Must vs Mustard under timing regimes taken from real systems

Campaign 2026-10-15 00:16 to 07:50 on brain05 and brain10. 1,815 cells, 5,044 finished runs, 14 not finished at the stop, 444 rows stopped by the operator (not results), 83 runs with 8 or 16 workers (flagged).

## Is brain10 valid?

Yes. 315 identical commands finished on both machines (Must baselines and Mustard cells); counts are identical in 315. Wall time brain10 / brain05 over 172 one-core pairs of at least 5 s: median 1.02, range 0.86 to 1.12. Counts are also deterministic across repetitions: no command that was run more than once gave two different counts.

## Verdicts

544 cells where Must reports a violation and Mustard proves the property under the stated timing (flips), 870 where both report violations, 249 where both hold, 152 where the Must side did not finish. There is no cell where Mustard violates and Must holds.

## Headline pairs (both tools finished)

| protocol | cell | regime | Must graphs / viol. / time | Mustard graphs / viol. / time | graph ratio | verdict |
|---|---|---|---|---|---|---|
| lease | Lease C=3 R=3 net=A[0,1] sd=0 TTL=12000 PAUSE=11997 | A | 49,206,872 / 11,603,400 / 2.1 h | 5,680,040 / 0 / 2.3 h | 9x | FLIP (Must violates, Mustard holds) |
| lease | Lease C=4 R=2 net=A[0,1] sd=0 TTL=12000 PAUSE=11997 | A | 99,628,848 / 37,607,328 / 4.1 h | 11,122,128 / 0 / 4.0 h | 9x | FLIP (Must violates, Mustard holds) |
| lease | Lease C=2 R=6 net=A[0,1] sd=0 TTL=12000 PAUSE=11997 | A | 204,073,324 / 20,007,188 / 3334 s (16 workers) | 22,802,036 / 0 / 3049 s (16 workers) | 9x | FLIP (Must violates, Mustard holds) |
| par | ABP-file par K=4 R=3 W=200 net=A[0,1] sd=0 | A | 308,419,431 / 6,150,740 / 5.4 h | 159,106 / 0 / 67 s | 1,938x | FLIP (Must violates, Mustard holds) |
| par | ABP-file par K=5 R=2 W=3 net=A[0,1] sd=0 | A | 69,529,579 / 3,109,929 / 4379 s | 56,022 / 0 / 21 s | 1,241x | FLIP (Must violates, Mustard holds) |
| brp | BRP F=1 n=6 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A (p2) | A | 1,260,206,952 / 39,940,668 / 2.3 h (16 workers) | 398,145 / 0 / 24 s (16 workers) | 3,165x | FLIP (Must violates, Mustard holds) |
| brp | BRP F=1 n=2 MAX=5 TD=1 T1=200 TR=2003 SYNC=2003 net=A (p2) | A | 206,967,587 / 1,446,477 / 2099 s (8 workers) | 34,006 / 0 / 3.3 s (8 workers) | 6,086x | FLIP (Must violates, Mustard holds) |
| nisshi | nisshi T=32 R=2 L=0 U=1 sd=0 W=30000 B=100 net=A (both) | A | 60,291,240 / 2,123,872 / 2747 s (16 workers) | 504,104 / 0 / 393 s (16 workers) | 120x | FLIP (Must violates, Mustard holds) |
| yarn | YARN K=2 QS=3 DD=8 U=0 H=11 miss net=A-C | A-C | 793,245,760 / 1,108,529 / 2.3 h (16 workers) | 55,555 / 1,448 / 3.8 s (16 workers) | 14,279x | both violate |
| yarn | YARN K=2 QS=3 DD=8 U=0 H=10 late-report net=A-C | A-C | 160,091,241 / 496,450 / 2508 s (8 workers) | 29,515 / 0 / 3.8 s (8 workers) | 5,424x | FLIP (Must violates, Mustard holds) |
| nrp | NRP noopt R=4 M=3 H=1000 P=2 net=A[0,1] sd=0 | A | 314,788,770 / 31,083,976 / 2827 s (14 workers) | 2,502,926 / 221,668 / 158 s (14 workers) | 126x | both violate |
| swim | SWIM N=3 R=7 timers=lan(Wp=500,Ws=4000) net=A[0,1] sd=0 | lan | 633,567,018 / 603,567,018 / 6129 s (16 workers) | 39,478,033 / 36,732,890 / 1850 s (16 workers) | 16x | both violate |
| lease_f | Lease + fencing C=2 R=5 net=A[0,1] sd=0 TTL=12000 PAUSE=11998 | A | 94,406,884 / 0 / 3.8 h | 6,461,828 / 0 / 2.6 h | 15x | both hold |
| retail | Shop v2 C=2 N=2 R=5 S=20 D=29000 net=A[0,1] sd=0 | A | 15,012,496 / 14,754,448 / 2263 s | 10,253,688 / 9,995,640 / 3.0 h | 1x | both violate |

Times with a worker count are not comparable with one-core times; counts are.

## What the regimes change

- Lease: the verdict follows the closed form PAUSE >= TTL - 2(U - L) - sd. Regimes only move the boundary; at the same distance from it the counts agree (regime A is on a coarse 1 ms grid and differs by 1 to 3%).
- PAR: a premature retransmission is feasible iff L <= W <= 2U + sd. With RTO values of 200 ms, 1 s and 3 s the protocol is safe at A and B, unsafe at C for W = 200 ms, and unsafe at D and E for every W. The alternating-bit variant never violates.
- BRP: property (2) violates only where T1 <= 2 TD; property (4) violates everywhere (lost R timer).
- nisshi (Kafka): the bug needs delays above the 30 s request timeout: A, C, D hold in Mustard (Must reports 168,880 to 2.1M violating graphs), E (90 s) violates in both, and the epoch-bump fix holds in both.
- SWIM, online shop: a lost message alone produces the violation, so the verdict does not depend on the regime and nothing flips.
- NRP: at R = 3 every variant holds in both tools; at R = 4 leasing still holds while nrp and noopt violate in both tools, in every regime.

## Where Mustard does not pay off

- In 80 of 1,663 both-finished cells Mustard reports MORE graphs than Must (SWIM at D and E, lease and shop at E: regimes whose delay bound exceeds the protocol timers). On SWIM N=3, R=2 and lease C=2, R=2 both tools visit the same program-visible graphs; they differ only in how many dropped or arrived-but-unread fate variants they enumerate.
- One graph costs 4 to 9 times more in Mustard (median per protocol, see `RESULTS_v12.md`), so Mustard is slower than Must on lease, SWIM and the shop at most regimes even where it explores fewer graphs, and about 10 to 40 times slower where nothing is pruned (ABP, PAR, BRP at D).

## Not in the regime campaign

Chandra-Toueg, spanning tree and the sensor network have no citable real-system numbers at the sizes Mustard reaches; their v11 rows (model units) are in `tab_evaluation_unmapped_v11rows.tex`. Keeping, dropping or footnoting them is still the author's decision.

## Files

`README.md` lists every file. Start with `tab_evaluation_v12_curated.tex` (story rows), `RESULTS_v12.md` (all numbers), `CAVEATS.md`, `REGIMES.md`.

