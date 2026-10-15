# v12 campaign: caveats that must travel with the numbers

Campaign: working tree = HEAD 24643fc + the ten daemon-free rewrites of v11 (`benchmarks/v11/rewrites_vs_24643fc.patch`)
+ the daemon-free nisshi (`nisshi_rewrite_vs_24643fc.patch`), all unstaged; 2026-10-15 00:16 to 07:50, brain05 and
brain10, scheduler `benchmarks/v9/sweep_v9.py` unchanged (md5 52188b86...). Every timing number comes from a real
system (`REGIMES.md`). The v11 caveats (`benchmarks/v11/CAVEATS.md`) still apply to every program that was not changed.

## 1. What was measured

- One pinned physical core per run, SMT sibling idle, ballast on idle cores, sequential per core. Time = median wall
  of the finished repetitions of the cell. Rows run with more workers are marked: `*_cores` and `*_parallel` in
  `table_v12.csv`, a double dagger with the worker count in the LaTeX rows. Their counts are identical to a one-core run
  (checked on 12 pairs: lease, ABP, SWIM, nisshi, BRP, YARN, NRP, online shop); their wall time is NOT comparable.
- A Must baseline does not depend on the regime (it has no clock), so one run serves every regime of the same
  program size. It was run once per machine and key; the table builder falls back to the other machine's run when the
  same-machine one did not finish. 315 identical commands finished on both machines (baselines and Mustard cells):
  counts identical in 315, wall ratio brain10/brain05 median 1.02 (0.86 to 1.12, 172 one-core pairs of at least 5 s). brain10 is therefore valid.
- Cells stopped by the operator (444 rows, status "removed by the operator") are NOT results and NOT DNF.
  Reasons: a repetition of a cell whose first run was already long, a lookahead rung two steps ahead of an unfinished
  rung, a one-core attempt that the measured growth made hopeless (replaced by a parallel run where the program
  supports it), a baseline running on both machines, an identical command. The ladder climber was stopped at 03:52
  because it kept submitting diagonal rungs with a meaningless growth estimate.
- A side that did not finish by 07:50 is DNF (`>t`, seconds elapsed), never extrapolated.

## 2. Closed-form boundaries (measured on the campaign binaries, 2026-10-15)

- **Lease**: a violation is feasible iff PAUSE >= TTL - 2(U - L) - sd. sd enters once. 27 bisections at C = 2, R = 2
  (U in {1, 50, 65, 200, 2500}, L in {0, 10, 42}, sd between 0 and 2U) hit the threshold exactly; every finished
  lease cell of the table agrees. The earlier form 2(U - L + sd) (v9 pin) had only been checked at sd = 0. The first Phase A
  scan of the sd = U regimes used the wrong boundary (those cells hold at it); Phase C pins them at t - 1 and t.
- **PAR (ABP file, variant par)**: a violation is feasible iff L <= W <= 2U + sd (bisection at K = 2, R = 1;
  sd = 0, 100, 200 at U = 200; L = 42 and L = 100). The alternating-bit variant never violates (retransmission
  carries the bit). Every finished par cell agrees.
- **BRP**: property (2) violates only where T1 <= 2 TD (DKRT's premise); property (4) violates everywhere because
  the R timer is a lossy self-send (v10 note), so p4 rows are "both violate" by loss.
- **nisshi**: Must violates and Mustard holds at A, C and D (U <= 2500 < W = 30000); at E (U = 90000 > W) both
  violate; with `--fix epoch-bump` both hold. SWIM and shop: a lost message alone produces the violation, so no flip
  is possible and none occurs. NRP: every variant holds at R = 3; at R = 4 leasing holds while nrp and noopt violate, in both tools and in every regime.

## 3. What Exec. means (and why Mustard can exceed Must)

Exec. = healthy complete graphs + blocked + violating, each counted once per graph. A graph is a reads-from
structure plus the fate of every send that nobody read (dropped, or arrived and never read). Both tools enumerate
fate variants, with different patterns. On SWIM N = 3, R = 2 (Wp = 200, Ws = 3000) both tools visit the same 25
program-visible graphs at every U (1, 200, 201, 2500); only the number of fate variants differs: Must 100, Mustard 49
at U = 1, 100 at U = 200, 118 at U >= 201 (a message that arrives after its receiver ended is a separate fate in the
timed engine, merged into "dropped" by Must). The same holds on lease C = 2, R = 2 at U = 90,000 (TTL 12,000, pause 200):
the 324 complete graphs are identical, and the blocked graphs are 560 distinct structures in both tools (1,600 variants
in Must, 1,736 in Mustard); the 136 extra Mustard graphs differ from a Must graph only in one send that is `[dropped]`
there and not dropped, never read, here. So:
- In regimes A to C (timers much larger than U) the reduction is the pruning of timing-infeasible fate variants;
- in 80 of 1,663 both-finished cells (SWIM at D and E, lease and shop at E; all with U >= a timer) Mustard's Exec. is
  larger than Must's. No new program behaviour is explored; the healthy count is equal in 54 of them.
- Viol. counts graphs with a failed assertion, once per graph, including fate variants; NRP and SWIM Viol. also
  include the fates of the lossy termination messages (v11 caveat, unchanged).

## 3b. Integer time grid (regime A)

Time is integer. Regime A has U = 1 ms, so a transit takes 0 or 1 ticks and some strict orderings cannot be realised;
regimes B, C and D (U = 65, 200, 2,500) have identical counts to each other. For the lease at the b - 1 pause the
counts of A are 1 to 3% below those of B to D at the same size (C = 2: 1,076 / 1,106 at R = 2; 13,172 / 13,562 at
R = 3; 22,802,036 / 23,480,666 at R = 6 for A / D; C = 3, R = 3: 5,680,040 / 5,737,272); for C = 4, 5, 6 at R = 1 they
are equal. The boundary law and every flip are unaffected. If a grid-converged count is wanted, quote B to D.

## 4. Unmappable protocols and open decisions

- Chandra-Toueg (needs a horizon of at least 33 rounds at etcd's ratios), spanning tree (802.1D timers need at least
  18 hellos) and the sensor network (no citable deployment figures) are NOT in v12; their v11 rows use model units.
  Decision (drop, count-based rounds, or footnote) is still the author's; their v11 rows are in
  `tab_evaluation_unmapped_v11rows.tex`.
- NRP and SWIM: Stop-fate inflation of Viol. (see section 3).
- Three examples (YARN, NRP, online shop) were given a `--parallel none|shared|partitioned` option to run the
  largest cells with 8 or 16 workers; `parallel_flag_yarn_nrp_shop.patch` is NOT applied to the repo (default
  behaviour unchanged).
- Most large cells have one repetition (dagger). Cells under 30 minutes were repeated to three runs where time left.
