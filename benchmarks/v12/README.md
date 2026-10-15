# benchmarks/v12: regime campaign (2026-10-15 00:16 to 07:50, brain05 + brain10)

Goal: Must vs Mustard with L, U, sd and timers taken from real systems, rounds and nodes pushed as far as the time allows.

| file | what it is |
|---|---|
| `REGIMES.md` | every timing number with its source; unmappable protocols; what was verified |
| `CAVEATS.md` | what the counts mean, measurement protocol, operator interventions, open decisions |
| `RESULTS_v12.md` | generated summary: verdicts, closed-form boundaries, frontiers, reductions, parallel runs |
| `table_v12.csv` | one row per Mustard cell joined with its Must baseline (counts, times, reps, cores, verdict) |
| `frontier_v12.csv` / `frontier_v12.txt` | largest both-finished and Mustard-only cell per protocol and regime |
| `tab_evaluation_v12_curated.tex` / `.pdf` | paper table (longtable): story rows per protocol (boundary pairs, frontiers, regimes); the PDF is a test render |
| `REPORT_v12.md` | the short written report (facts generated from the data) |
| `tab_evaluation_v12_main.tex` / `_full.tex` | automatic selection (regimes A, C, D, E) / every regime (longtable) |
| `tab_evaluation_unmapped_v11rows.tex` | v11 rows (model units) for Chandra-Toueg, spanning tree, sensor network |
| `controls_v12.txt` / `.json` | cross-machine controls: identical commands finished on brain05 and brain10 |
| `data/` | results, plans, tfstats, scheduler status of both machines, log flags |
| `logs_brain05.tgz`, `logs_brain10.tgz` | raw run logs and the operator control file |
| `scripts/` | planners, ladder climber, builders, monitors, analysis and finalize scripts |
| `nisshi_rewrite_vs_24643fc.patch` | daemon-free nisshi (applied to the campaign binaries) |
| `parallel_flag_yarn_nrp_shop.patch` | `--parallel` option for three examples (used for flagged parallel runs; NOT applied to the repo) |

Reading a row: `Exec.` = healthy + blocked + violating complete graphs, `Viol.` = graphs with a failed assertion, once per
graph. A double dagger with n means the run used n workers (counts identical to one core, wall time not comparable).
`>t` = not finished after t seconds. Cells stopped by the operator are not results.
