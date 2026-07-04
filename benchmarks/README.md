# MUST-$\tau$ benchmark artifact

Steering artifact for the thesis *"Verification of Distributed Protocols with
Wait-Time Thresholds."* Every number in `results.csv` and `tables/` is a real
TraceForge run on the **`inbox`** branch. Nothing is cited or hand-estimated.

## TL;DR (what to look at)
- **`results.csv` — ALL results in one file (324 rows).** One row per (protocol, N, L/U, sd/U, W/U, delta, rounds).
  Column order (curated): `protocol, N, rounds, mode, execs, block, baseline_execs, baseline_block, verdict,
  baseline_verdict, pruning_x, leaders_elected_total, no_leader_execs, baseline_elected, commit_execs, abort_execs,
  baseline_commit_execs, baseline_abort_execs, wall_time, status, cmd, l_over_u, sd_over_u, w_over_u, delta`.
  - Each row has the **timed** run (`execs`/`block`/`verdict`) and its **untimed-MUST baseline** (`baseline_*`);
    `pruning_x = baseline_execs / timed_execs`.
  - **Outcome axes** (each protocol fills its own; the other is `—`):
    leader-election → `leaders_elected_total` / `no_leader_execs` (+ `baseline_elected`);
    commit protocols (3PC) → `commit_execs` / `abort_execs` (+ baseline).
  - Partitions (no `group` column; slice by these): N=3 = the headline grid; N=4/N=5 = the larger-N tail
    (N=5 rows are all `status=timeout`); comm-closed N=3 with `rounds=2` = the ballots=2 sd-test.
- `tables/regime_table.tex` — regimes A–F × the four grid protocols (verdict, WCRT, base #execs, timed #execs, prune×, **commit/abort** for 3PC or **elected/no-leader** for comm-closed LE).
- `tables/heatmap_3pc.tex` — 3PC pruning gradient over L/U × W/U (ratio + #execs).
- `tables/native_summary.tex` — raft (native axes, with elected/no-leader).
- `VERDICT_STABILITY.md` — the anti-cherry-picking summary (read this first; includes the 3PC `commit=0` flag).
- `reproduce.sh` — rebuild + rerun everything into the single `results.csv`.
- `results_v1_timedonly.csv`, `results_tail_v1.csv`, `results_user_edited_backup.csv`, `results_user_curated_v2.csv` — backups (kept; nothing lost).

> **Heads-up on the new commit/abort axis:** **timed 3PC reaches `commit=0` in every cell** (all completed timed
> executions abort or stall at the ack phase), while untimed 3PC commits normally. This is
> consistent across all W/U and N and points to the ack-phase `sleep(delta=W+1)` timing out Acks in the timed model.
> Treat it as either a genuine timed-liveness finding or an artifact of the example's ack timing to review before
> publishing — see `VERDICT_STABILITY.md`.

## The grid (U = 20 integer ticks)
The model has dimensionless scale-invariance (network-parameter-selection §2), so we fix
**U = 20** and realise the §7 ratio grid with integer ticks:

| ratio | values | ticks (U=20) |
|---|---|---|
| transit tightness L/U | 0, 0.25, 0.5, 0.9 | L = 0, 5, 10, 18 |
| timeout headroom W/U  | 2, 3, 5, 7, 10    | W = 40, 60, 100, 140, 200 |
| storage sd/U          | 0, 0.25, 0.5      | sd = 0, 5, 10 |

Full cross product = **60 cells** per grid protocol. `delta` is left exactly as each
example derives it (3PC: `delta = W+1` via `Bounds::from_ratio`; we did not touch delta
semantics, so the new numbers stay comparable to the shipped 3PC sweep).

## Which axes were actually run (honesty)
| axis | status |
|---|---|
| 1. verdict (hold/fire) | **model output** — from process exit code (fired `assert` → panic → exit 101; holds → 0). |
| 2. pruning× (baseline/timed #execs) | **model output** — from `--mode compare`; computed uniformly as `baseline_execs / timed_execs`. |
| 3. **WCRT (max τ_hi)** | **NOT available — deferred.** Marked `n/a` everywhere. See below. |
| 4. leader-election outcome | **model output** — see election counting below. |

### WCRT (axis 3) is deferred on purpose
`Stats` on this branch exposes only `execs`, `block`, `coverage`, `max_graph_events`.
The per-event τ-windows (`TimeInterval{lo,hi}` in `src/timed_cons.rs`) are computed for
feasibility filtering and then **discarded** — never aggregated, never on `Stats`, never
printed. Reporting "max τ_hi over rf-classes" requires a verified verifier change
(add `max_tau_hi` to `Stats` + a max-reduction in `must.rs::complete_execution`, scoped
to commit/elect-terminal executions, post-pruning). That is a thesis headline that must be
*right*, so it is split into its own pass rather than bundled into this sweep. We did **not**
fabricate or hand-compute any WCRT number.

## Which protocols got the full grid vs native axes
| protocol | grid? | how |
|---|---|---|
| `three_pc_timed` (correct) | **full 60-cell** | `--u 20 --w-ratio R --l-ratio LR --sd-ratio SR` (config edit, below) |
| `three_pc_timed_buggy` | **full 60-cell** | same flags |
| `comm_closed_leader_election` | **full 60-cell** | native `--u 20 --w W --l L --sd SD` (no edit) |
| `comm_closed_leader_election_inbox` | **full 60-cell** | native (no edit) |
| `raft_leader_election` | **native axes** | U=1 fixed, `sd=(N-1)·delta`, no recv-timeout W → the W/U grid does not apply. Swept `nodes{3} × delta{1,2,3} × rounds{1,2}` (+ N=5 tail). |

## Code changes made (committed as part of the artifact)
Config-only; **no protocol logic, no sleeps, no asserts, no decision rules, no crash model
changed.** Approved before editing.
- `traceforge/examples/three_pc_timed.rs`: add `--l-ratio`/`--sd-ratio`; compute
  `L=round(l_ratio·U)`, `sd=round(sd_ratio·U)`; `with_timed(0,b.u,0)` → `with_timed(b.l,b.u,b.sd)`;
  headers print real L/sd.
- `traceforge/examples/three_pc_timed_buggy.rs`: same flag threading; **`--mode baseline|timed`**
  added (baseline = untimed MUST, simply skips `with_timed`) so the buggy has a comparison point;
  plus one observability line printing `execs/blocked` on the *hold* path. The buggy decision rule
  is untouched.
  - **Correction (mid-run):** an earlier version of this file threaded L/sd into `Bounds` but left
    the actual call as `with_timed(0, b.u, 0)`, so the buggy's L/sd axes were silently ignored (only
    W/U varied). Fixed to `with_timed(b.l, b.u, b.sd)` and the buggy grid re-run. The verdict was
    unaffected (the bug fires in every regime regardless), but the L/sd grid is now genuinely tested.
- raft and both comm-closed examples: **unchanged.**

## Election counting mechanism
- **comm-closed LE (both variants):** the examples already maintain a process-global
  `AtomicUsize` (`EXECS_WITH_ELECTION`), reset before `verify`, incremented once per execution
  whose collected logs are non-empty, read after `verify`, and printed as the `elected` column
  in compare mode. Revisit-safe: the verify closure runs exactly once per explored execution.
  We report `leaders_elected_total = elected`, `no_leader_execs = execs − elected`.
- **raft:** reported **structurally from `Stats`** (no edit): `leaders_elected_total = execs`,
  `no_leader_execs = block`. Justification: a *complete* raft execution provably elects exactly
  one leader (two-per-term would fire the Election-Safety `assert`, which holds in every cell;
  zero-leader is impossible because a never-winning Candidate blocks on `recv` and is counted as
  `block`, not a complete exec). An independent verifying counter was offered and declined for
  this pass.

## Regime A–F → grid snapping (author choices, not cited)
Each regime's normalised `(L/U, sd/U, W/U)` is snapped to the nearest grid value (range
midpoint where a range was given; ties broken to the lower grid value). These mappings are
**our** choices for presentation, not from the source reports.

| regime | source (L/U, sd/U, W/U) | snapped grid cell |
|---|---|---|
| A. Intra-DC RDMA (tight) | (0.02, 0.1, ~10+) | (0, 0, 10) |
| B. Intra-DC / intra-AZ TCP | (0.3, 0..1, ~3–30) | (0.25, 0.5, 10) |
| C. Inter-AZ (same region) | (0.1, 0..0.4, ~4–20) | (0, 0.25, 10) |
| D. Inter-region (regional pair) | (0.08, 0..0.25, ~5–15) | (0, 0, 10) † |
| E. Inter-region (transcontinental) | (0.16, 0..0.08, ~4–12) | (0.25, 0, 7) |
| F. Adversarial / congested | (L/U→small, sd/U≫1, W/U base) | (0, 0.5, 2) ‡ |

† D snaps to the same cell as A — the grid's L/U resolution cannot separate two
"tight-transit, low-storage, generous-timeout" regimes. Reported honestly rather than nudged apart.
‡ F's true `sd/U ≫ 1` (backpressure) is **outside** the grid (grid max sd/U = 0.5); we use the
grid maximum as the closest available point.

## Reproduce
```bash
bash benchmarks/reproduce.sh        # rebuild + Phase A + Phase B + tail + tables
# or run a single cell directly, e.g.:
target/release/examples/three_pc_timed --mode compare --participants 3 \
    --u 20 --w-ratio 5 --l-ratio 0.9 --sd-ratio 0.5
```

## Machine / scale notes
16 cores, 15 GB RAM (≈1 GB free during the run). N=3 cells use ~6 MB RSS and finish in
<0.8 s, so the core sweep is a few minutes at low parallelism (jobs=2). Any timeout/OOM
cell is logged in `logs/`, not dropped.

**Tail status (time-boxed, N>3; slice `results.csv` by N / protocol):**
- **3PC N=4** — DONE (60 cells, ~5 s/cell). Safety holds everywhere; pruning grows to
  ~74–116× (vs 12–20× at N=3): the timed model's advantage scales with N.
- **raft N=5 + comm-closed (non-inbox) N=5** — **timed out at a 600 s cap** (10 `timed`-only cells).
  Recorded as `timeout` rows. IMPORTANT: 600 s is a short cap; these were **not retried with a longer
  budget**, so they are **undertested, not shown intractable**. Do not read them as "cannot scale".
- **comm-closed LE (inbox), larger N**:
  - **N=5, ballots=1 — COMPLETES** timed verification (tight corner L/U=0.9/W/U=2/sd=0, ~48 min):
    97,586,928 execs, 6,925,445 elected (7%) / 90,661,483 no-leader (93%), safety holds. This is a real
    completion, but it does **not** by itself prove the inbox variant scales better: the non-inbox N=5
    above only had a 600 s cap versus this run's 85 min, so the comparison is not equal-budget.
  - **N=4, ballots=2 — did not finish** (killed at ~31.6 h; the 2-ballot space is far larger).
  - **untimed baseline for inbox N=5** — did not finish in 9.3 h, so there is no baseline count or pruning× for that row.

## Ballots axis (`--ballots`) — fixed at 1 for the grid; B=2 tested separately, and sd DOES bite
The comm-closed examples take `--ballots B` (rounds per node). The main grid uses **B=1** (a
*structural* axis, not a §7 network ratio, and each extra ballot multiplies the state space). That
choice **scoped one claim**: "sd/U has no effect" holds **only at B=1**, because `sd` is message
*storage lifetime* and can bite **only across a ballot boundary**, which one ballot never has.

The `group=ballots2` rows in `results.csv` are the B=2 test (N=3, `timed`-only). **Result: sd becomes load-bearing at B=2**,
strongly so at tight transit — at L/U=0.9, sd/U=0→0.5 takes execs 11.27M→21.85M (×1.94) and elected
6.86M→15.45M (×2.25). This confirms the report's inclusion of the sd axis. B=2 is a ~5,000× blowup
(the B=1 cell of 10,232 execs is **52,152,832** at B=2; each timed cell took 3–14 min).

**Untimed (baseline) B=2: NOT run (skipped per time budget).** ROUGH ESTIMATE ONLY — treat as
inaccurate, not a measured value: the untimed run reached 2,000,000 execs at ~4,500 execs/s before
being aborted; since untimed ≥ timed it is ≥52M execs, so completing it would take **~3–4 hours at
full CPU** (a lower-bound projection from the early-run rate, which may decline; the true untimed
exec count is unknown). So `baseline_* = not-run` and `pruning_x = n/a` for the four B=2 rows. (RSS
stayed low throughout — no hang risk; the limit is purely CPU time. An earlier attempt with a 12 GB
`ulimit -v` aborted ~500k execs because a 16-worker rayon run reserves >12 GB of *virtual* address
space despite tiny RSS; rerun without that cap to retry.)
