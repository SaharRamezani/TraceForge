# v9 results workbook: brief for the per-protocol metadata writers

## Update of 2026-10-11 (read this first; it overrides the text below where they differ)

- The campaign is two days in. `benchmarks/v9/results_v9.csv` now holds the v9 result of almost
  every cell (same columns as described below plus `viol_execs`, `counters` and `summary`). Take
  every measured fact (counts, verdicts, resolved parameter values in `summary`) from
  results_v9.csv first, and from the v8 files only for a cell that has no v9 row yet.
- `plan_v9.csv` now also contains rows with `purpose` = `edge` (ids like `par-e01-r1`): one rung
  above the top of a ladder, added on 2026-10-11, most of them not finished yet. They are ordinary
  cells of your example and need an entry in `cells` like every other cell (same regime as the
  rung below; parameters from their `args`; `expect_*` = "not measured yet" where there is no
  result). Rows with `purpose` = `keeper` are not results: leave that cell out.
- Rows with rep 2 or 3 repeat a cell: one `cells` entry per distinct `cell` string.
- File name: write `benchmarks/v9/meta/<example>.json` (the name of the example binary, for
  instance `meta/lease_timed.json`). The JSON field `family` is the family id of plan_v9.csv.
- The local binaries in target/release/examples are from 30 September, older than the campaign
  commit. You may run one (timeout 60, one at a time, small cells only) to read the resolved
  parameter values a flag combination gives. Never take a count from a local run.
- Still: no ssh, no scp, nothing on brain05. Do not edit any file except your own JSON file(s).
- The example sources are in `traceforge/examples/<example>.rs` at the repository root
  `/home/sahar/Downloads/TraceForge`.

A multi-day benchmark campaign ("v9") is RUNNING right now on the server brain05. It re-measures every
cell of the previous campaign (v8) on the committed code (commit b67ccf7) under strict timing rules.
The job list is benchmarks/v9/plan_v9.csv: one row per job; the job id is the v8 job id plus
`-r<repetition>`; `cell` is the v8 cell name; `args` the exact command line.

**Do not connect to brain05 at all (no ssh, no scp).** Any extra process there spoils the timing of a
running measurement. Everything you need is local.

Your job: write the metadata that turns the raw results of ONE benchmark example into a sheet of a
paper-quality workbook. Sahar (the user) asked for, literally:

> the violations of both baseline and the timed version, the execution graphs explored, blocked
> ones, for the "lease" a column for showing if it has fenced on or off. [...] I want to see the
> params important for each protocol in the columns. I want to know which settings you ran the
> protocol with. Create a glossary for things one should know. Make the regimes visible (this row is
> for which regime and in the glossary, what that regime is). [...] paper-ready and reliable and
> reproducible.

So for every example the workbook needs: (1) parameter columns with the real values of each cell,
(2) a regime for every cell and a definition of every regime, (3) glossary entries, (4) the checker
settings of both modes, (5) what the violation numbers mean.

## What to read

1. The example source, completely: traceforge/examples/<example>.rs (CLI flags and defaults, how
   ratio flags become integer ticks, the `build_config` of each mode, the summary line it prints,
   every assertion and which thread makes it, thread names, the header comment with the timing rule).
2. Your example's rows in benchmarks/v9/plan_v9.csv (the cells that are running) and, for measured
   facts, benchmarks/v8/results_v8.csv (same cell names; column `summary` holds the summary line the
   binary printed, with the RESOLVED parameter values, e.g. `C=5 R=1 fencing=off L=0 U=2 TTL=10
   PAUSE=3 sd=1`) and benchmarks/v8/plan_v8.csv (column `expected`).
3. The memory notes listed in your task (directory
   /home/sahar/.claude/projects/-home-sahar-Downloads-TraceForge/memory/): boundary formulas, known
   pitfalls, citability verdicts. Some are older than the code: when a note and the source disagree,
   the source wins, and say so in `notes`.

You may run the LOCAL release binaries (target/release/examples/<example>, already built; do not
rebuild) to check a resolved parameter value or a verdict, but only small cells: wrap every run in
`timeout 60`, one run at a time, and never start a run that needs more than a minute. Redirect the
output to a file under the scratchpad directory given in your task and look at its last lines
(keep-going runs print one line per violating execution).

## Vocabulary you must use

- "timed" = the run with the clock (`--mode timed`); "baseline" = the same program without the
  clock (`--mode baseline`, plain Must). Never write "untimed". Never write the word "oracle".
- No em-dashes or en-dashes anywhere: use commas, colons, parentheses.
- execs = complete executions; blocked = executions that ended early (a receive that can never be
  served, a failed assume, or, under keep-going, a failed assertion); explored = execs + blocked.
- violating executions = executions in which an assertion failed. The campaign counts them from the
  tool's own output: under keep-going every violating execution prints exactly one line with the
  name of the failing task (`task-N`, or `main-thread-ThreadId(N)` when the main thread asserts).
  The example's own counters (in its summary line) are shown next to it.

## Regimes

A regime = a named setting of the timing parameters together with the side of the protocol's timing
boundary it lies on. EVERY cell gets exactly one regime id. Form of the id: `<Letter>-safe` (real
time rules the violation out: the timed side must report 0 violations; the baseline may report
spurious ones), `<Letter>-viol` (a real timing violation exists: the timed side must report
violations), `<Letter>-edge` (exactly on the boundary, say which side the tool puts it on),
`<Letter>-ctrl` (control: time adds no information, the two sides must agree). The letter names the
network timing setting (U, L, sd and the protocol's own timers). If v8 already used letters for your
example (lease A to E, leader election A to F, SWIM L0 / L1, BRP A to E), keep them. If the example
has no timing boundary in the cells that were run, use one regime per timing setting with the suffix
that fits and say so.

## Output: one JSON file per example, benchmarks/v9/meta/<family id>.json

Write the file EARLY (as soon as you have the parameter columns and the cell table) and improve it
in place: the session may be cut off at any moment and only what is on disk survives. Validate with
`python3 -c "import json; json.load(open(PATH))"` after every write.

```json
{
  "family": "lease",
  "example": "lease_timed",
  "title": "Lease with TTL and fencing tokens",
  "source": "where the protocol and its timing rule come from (paper, year, table or listing), or 'own model'",
  "property": "one sentence: what the assertions check",
  "citable": "yes | with caveats | no",
  "citable_note": "one or two sentences; check the 2026-09-18 audit against the current source",
  "config_timed": "the checker configuration of the timed mode exactly as build_config sets it (consistency model, with_timed(L,U,sd), lossy sends, keep-going, scheduling policy, parallel option), with source line numbers",
  "config_baseline": "the same for the baseline mode",
  "program_differs_between_modes": "no | yes: where (a mode-dependent branch or sleep in protocol code is a citability problem)",
  "failing_tasks": "which threads can fail an assertion and how their line looks (task-N / main-thread-ThreadId(N) / a custom name)",
  "counters": [
    {"key": "violations", "counts": "executions | events", "meaning": "what the example's summary counter counts and how it relates to violating executions"}
  ],
  "violation_counter": "key of the summary counter that should equal the number of violating executions, or null (say why)",
  "param_columns": [
    {"key": "C", "header": "Clients C", "meaning": "number of clients competing for the lease", "size": true},
    {"key": "fencing", "header": "Fencing", "meaning": "on: the storage rejects a write carrying a stale token; off: it accepts it", "size": false}
  ],
  "boundary_law": "the timing rule that separates safe from violating settings, where it comes from, and how well it is checked; or null",
  "regimes": [
    {"id": "A-safe", "name": "plain-language name", "params": "U=2, L=0, sd=1, TTL=10, PAUSE=3",
     "rule": "PAUSE = bound - 1, with bound = TTL - 2(U - L + sd) = 4",
     "expect_timed": "0 violations", "expect_baseline": "spurious violations: without a clock the late write cannot be ruled out"}
  ],
  "glossary": [
    {"term": "PAUSE", "meaning": "how long the lease holder stalls before it writes, in ticks"}
  ],
  "cells": [
    {"cell": "<the v8 cell name, copied exactly from plan_v9.csv>",
     "regime": "A-safe",
     "label": "short readable label for the row, without the parameter values",
     "params": {"C": 5, "R": 1, "U": 2, "L": 0, "sd": 1, "TTL": 10, "PAUSE": 3, "fencing": "off"},
     "kind": "exhaustive | first-violation",
     "expect_timed": "what the timed side should report (from v8 or the rule)",
     "expect_baseline": "what the baseline should report",
     "note": "anything the reader of this row must know, else empty"}
  ],
  "notes": ["anything the workbook author must know: disagreements between notes and source, cells whose two sides do not run the same arguments, counters that double count, and so on"]
}
```

Rules:
- `cells` must contain EVERY distinct `cell` value of your example in benchmarks/v9/plan_v9.csv,
  spelled exactly. Check it with a script before you finish (print the missing and the extra ones).
- `params` has a value for every key of `param_columns`, as the integers or words the binary really
  uses (resolve ratio flags to ticks; cross-check against the v8 summary line of the same cell).
  Use "-" for a parameter that does not apply to a cell.
- `size: true` marks the columns a reader would call the size of the instance (nodes, clients,
  rounds, ballots, messages, retries, horizon, transactions, ...).
- `kind` is "first-violation" for cells run WITHOUT the keep-going flag (the run stops at the first
  certified violation, exit code 101), else "exhaustive".
- Every statement about what a regime or a cell should report must come from the source, from a
  v8 result, or from a run you made; do not guess. If you cannot determine something, write
  "unknown" and explain in `notes`.

Return only the structured summary asked for in your task; do not paste the JSON.
