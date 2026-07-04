# Verdict-stability summary (anti-cherry-picking defence)

Every number below is a TraceForge run on the `inbox` branch (U=20 ticks). WCRT (axis 3, max tau_hi) is not exposed by `Stats` on this branch and is deferred to a separate verified instrumentation pass; it is marked n/a everywhere.

## 3PC (correct) -- the pruning template
- Safety (atomicity) **HOLDS in all 60/60 grid cells** (robust; no violating corner).
- **Commit-outcome axis (commit/abort, the commit-protocol analog of elected/no-leader):** across the 60 N=3 cells the **timed model reaches a COMMIT in 0 executions total (every cell = 0)** -- every completed timed execution aborts (97-159 per cell) or stalls at the ack phase; meanwhile the untimed baseline commits 288 per cell. **FLAG:** timed 3PC never committing is a strong result that needs interpretation -- it is consistent across all W/U (incl. loose W/U=10) and N, which points to the ack-phase `sleep(delta=W+1)` making Acks time out in the timed model. Treat as either a genuine timed-liveness finding (3PC cannot make progress to commit under these timing assumptions) OR an artifact of the example's ack-phase timing to be reviewed before publishing. (commit+abort <= execs; the remainder are ack-stalls.)
- Pruning ratio ranges **11.45x .. 20.26x** (N=3); it *increases* as transit tightens (L/U 0->0.9) and saturates past W/U=3. The timed model removes orderings the untimed checker must explore.
- **N-scaling:** at N=4 (60-cell grid, tail) safety still HOLDS in all 60/60 cells and pruning jumps to **63.5x .. 116.1x** -- the timed model's advantage grows with N.

## 3PC (buggy) -- the safety-bug witness
- The injected majority-commit bug **FIRES in all 60/60 cells under MUST-tau (timed)** AND in all 60/60 cells under untimed MUST (baseline) -- L and sd are now correctly threaded into the timed run (the L/U axis varies 0..0.9, sd/U 0..0.5, W/U 2..10).
- **Honest finding:** this bug is NOT timing-gated within the section-7 grid -- it is a pure logic error (a No-voter can receive Commit whenever the coordinator sees a majority of Yes), so **both** the timed and the untimed checker find it in every regime. It is a safety bug the timed model reliably catches, but NOT an example of a 'timing-only' bug that an untimed checker would miss. (No fire-vs-hold variation across the grid; verify the corrected L/sd threading in `results.csv`.)

## Comm-closed LE
- Agreement (<=1 leader/ballot) **HOLDS in all 64/64 cells** (robust).
- Pruning is ~1.00x for L/U in {0,0.25,0.5} and only **1.77x at L/U=0.9**: the timed model prunes only in the extreme-tight-transit corner. **W/U has no effect** in the section-7 range (W>=40 >> U=20, so the recv timeout never bites).
- **sd/U has no effect AT BALLOTS=1** (all these runs use --ballots 1). This is EXPECTED, not a general result: sd is message storage-lifetime, which can only bite across a *ballot boundary*, and a single ballot has none. Whether sd becomes load-bearing at ballots>=2 is a separate question; ballots=2 is itself a state-space blowup (does not finish at N=3 in 120s) -- see the `group=ballots2` rows in results.csv for the bounded sd=0-vs-sd=0.5 test.
- Successful-election count vs transit tightness (sd/U=0, W/U=5): L/U=0.0: 5064 elected / 5168 no-leader, L/U=0.25: 5064 elected / 5168 no-leader, L/U=0.5: 5064 elected / 5168 no-leader, L/U=0.9: 2106 elected / 3680 no-leader.
  Tightening transit to L/U=0.9 *reduces* successful elections (the only timing axis that bites here).

## Comm-closed LE (inbox)
- Agreement (<=1 leader/ballot) **HOLDS in all 60/60 cells** (robust).
- Pruning is ~1.00x for L/U in {0,0.25,0.5} and only **3.02x at L/U=0.9**: the timed model prunes only in the extreme-tight-transit corner. **W/U has no effect** in the section-7 range (W>=40 >> U=20, so the recv timeout never bites).
- **sd/U has no effect AT BALLOTS=1** (all these runs use --ballots 1). This is EXPECTED, not a general result: sd is message storage-lifetime, which can only bite across a *ballot boundary*, and a single ballot has none. Whether sd becomes load-bearing at ballots>=2 is a separate question; ballots=2 is itself a state-space blowup (does not finish at N=3 in 120s) -- see the `group=ballots2` rows in results.csv for the bounded sd=0-vs-sd=0.5 test.
- Successful-election count vs transit tightness (sd/U=0, W/U=5): L/U=0.0: 2106 elected / 3165 no-leader, L/U=0.25: 2106 elected / 3165 no-leader, L/U=0.5: 2106 elected / 3165 no-leader, L/U=0.9: 183 elected / 1563 no-leader.
  Tightening transit to L/U=0.9 *reduces* successful elections (the only timing axis that bites here).

## Comm-closed LE (inbox) -- larger-N coverage
The inbox variant completes exhaustive TIMED verification at N=5 (1 ballot) in about 48 min. IMPORTANT: this does not by itself prove the inbox variant scales better than the others. The non-inbox N=5 and raft N=5 runs were only given a 600 s cap and timed out; they were not retried with a longer cap, so they are undertested, not shown intractable. A fair comparison needs equal time budgets. Timed-only, tight corner (L/U=0.9, W/U=2, sd/U=0); the untimed baseline did not finish in 9.3 h, so pruning is n/a.
- N=4, ballots=2: **did-not-finish (31.6h, unknown)** -- waited ~31.6 h and it **did not finish; the result is unknown** (the 2-ballot space is far larger; exhaustive timed verification did not complete).
- **N=5, ballots=1: COMPLETES** in 48 min -- 97,586,928 execs, **6,925,445 elected (7%) / 90,661,483 no-leader (93%)**, safety holds. The no-leader fraction is large -- at N=5 tight transit most schedules split-vote.

## Raft leader election
- Election-Safety (<=1 leader/term) **HOLDS in all 6/6 cells**.
- At N=3 the result is **invariant to (delta in {1,2,3}, rounds in {1,2})**: every cell gives timed execs=44, block=214. The election always resolves in round 1, and delta only changes sleep magnitudes, not the timed exec count.
- Leader-election outcome (structural, from Stats): **leaders_elected_total = execs = 44**, **no_leader_execs = block = 214**. Justification: a complete raft execution provably elects exactly one leader (two-per-term fires the Election-Safety assert, which holds; zero-leader is impossible because a never-winning Candidate blocks on recv and is counted as `block`, not a complete exec). So ~83% of explored schedules are split-vote / no-leader stalls.

## Comm-closed LE at ballots=2 -- does `sd` finally bite? (YES)
The single-ballot grid showed sd inert; that is an artifact of B=1 (no ballot boundary). At **B=2** (N=3, timed-only; the untimed baseline is intractable here), sd becomes load-bearing:
- loose transit (L/U=0): sd/U=0 -> 52,152,832 execs / 40,118,304 elected;  sd/U=0.5 -> 53,116,792 execs / 40,932,792 elected (execs x1.02, elected x1.02).
- tight transit (L/U=0.9): sd/U=0 -> 11,272,450 execs / 6,861,468 elected;  sd/U=0.5 -> 21,854,668 execs / 15,448,092 elected (execs x1.94, elected x2.25).
- So `sd` has a small effect at loose transit but a LARGE one at tight transit (state space and successful elections ~double at sd/U=0.5). This confirms the network-parameters report's inclusion of the sd axis: it matters precisely when storage must outlive a ballot boundary. **Ballots was fixed at 1 for the main grid** (a structural axis, not a network ratio, and B=2 is a ~5000x state-space blowup: the B=1 cell with 10,232 execs becomes ~52,000,000 at B=2).
- **Untimed (baseline) B=2 was NOT run to completion** (skipped per time budget). ROUGH ESTIMATE ONLY (not measured, treat as inaccurate): the untimed run reached 2,000,000 execs at ~4,500 execs/s before being aborted; since untimed >= timed it is >=52M execs, so completion would take **~3-4 hours at full CPU** (a lower-bound projection from the early-run rate, which may decline; the true untimed exec count is unknown). Hence `baseline=not-run` and `pruning_x=n/a` for the B=2 rows.
