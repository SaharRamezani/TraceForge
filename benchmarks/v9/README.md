# v9 benchmark campaign (brain05, 9 to 13 October 2026)

Every cell of the v8 campaign measured again on the committed code, under timing rules that make
the wall times repeatable, plus one rung above the top of each ladder. Timed checker
(`--mode timed`) against the baseline (`--mode baseline`, plain Must: the same program without
the clock).

## What ran

| | |
|---|---|
| Source | commit `b67ccf7` (branch `nissih`; tool code last changed in `e96fe7a`) |
| Lockfile | `Cargo.lock.v9` (md5 `a31eb836e4baff3bdb194c2eca9f2942`; git ignores `*.lock`, so it is kept here) |
| Build | `cargo build --release --examples --locked`, cargo 1.97.1, rustc 1.97.1 |
| Machine | brain05: 2 x AMD EPYC 7702 (128 physical cores, 256 logical), 2 TB RAM, Debian 13, kernel 6.18.38 |
| CPU settings | governor `schedutil`, frequency boost on, SMT on (the second hyper-thread of every measurement core is left idle), 8 NUMA nodes |
| Window | launched 2026-10-09 15:14, hard deadline 2026-10-13 08:00 (89 hours) |
| Jobs | `plan_v9.csv`, 2,301 runs: 1,749 at launch (`plan_v9_launch.csv`) + 552 added on 11 October |
| Launch record | `env_v9.txt` (versions, machine, md5 of every binary, of the scheduler and of the launch plan) |

Checker settings, the same in every example: FIFO channels (the default consistency model), every
send lossy in both modes, and in timed mode `with_timed(L, U, sd)` with the values of the cell.
Exhaustive cells pass `--keep-going` where the example has the flag. The search order of `nondet()`
choices is seeded at random per process: exhaustive counts do not depend on it, the time to a first
violation does.

## The plan

- Launch plan (`make_plan_from_v8.py`): the 683 v8 jobs with their v8 argument strings; job id =
  v8 id + `-r<repetition>`. Timing-grade = v8 wall up to one hour: three repetitions, in three
  passes over all cells. Reach-grade = longer, or not finished in v8: one run, started first,
  longest first; the 16 v8 runs that did not finish get the whole window.
- Added on 11 October at campaign hour 44, without stopping anything (the scheduler reads new plan
  rows while it runs):
  - `make_edge_plan.py`: 23 edge rungs (`purpose` = `edge`, ids `<family>-eNN-r1`), one size step
    above the top of a ladder with the top rung's arguments; a second run (`-r2`) of the 96
    reach-grade cells of at most 8 h, queued last; and the keeper (`zz-keeper-r1`), a run that
    cannot finish and only keeps the spin loops on the free cores until the deadline. The keeper
    is not a result.
  - `make_reps_plan.py`: repetitions 4 to 7 of the 108 multi-core timing-grade sides of 5 s or
    more (see "What the repetitions showed").
- Queue order after the additions: cells of the launch plan, edge rungs (longest first, the
  leader-election probe last), extra repetitions, second runs of long cells.

## Timing rules (`sweep_v9.py`)

- One job thread per physical core. A job is pinned with `taskset` to its own cores (logical CPUs
  2-63 and 66-127). The second hyper-thread of every core (CPU `c + 128`) is never used.
- The scheduler and the output filters run on cores 0, 1, 64, 65.
- While a timing-grade job is queued or running, every free measurement core runs a spin loop, so
  the clock frequency does not depend on how full the queue is. From hour 44 on the keeper holds
  this condition until the deadline.
- Each run is audited every 10 s (clock frequency of its cores, load on its sibling hyper-threads,
  foreign load, cpu/wall). A timing-grade run that fails the audit is repeated (ids `-x1`, `-x2`).
- Wall, CPU and peak memory come from `/usr/bin/time` around the binary alone.
- A run still going at the deadline is stopped and recorded as DNF with its progress.

Measured before the launch with one 14.3 s single-thread job: sibling hyper-thread busy 22.2 s
(+55%); one socket busy 17.2 s (+21%, 3.34 to 2.77 GHz); all 124 measurement cores busy 17.45 s
median, 16.8 to 17.6 across cores.

## What the repetitions showed (state of 11 October, 1,666 runs)

- Counts: complete and blocked executions are identical in every repetition of every cell, and
  identical to v8 on all 596 sides that finished in both campaigns. No run reports an impossible
  timeline. No run ended with an error or an unexpected exit code.
- Sequential cells (one core): the wall time repeats. Over the 267 exhaustive sides of 10 s or
  more, (max - min) / median has a median of 1.6% and a maximum of 5.9%.
- Worker-pool cells (several cores): the wall time does not repeat. Over 104 sides the spread has
  a median of 5.4%, exceeds 10% on 34 and reaches 148%, with the same CPU time in every
  repetition: the pool shares the work differently each time. For such a cell quote the range or
  the CPU time, not one wall time. The workbook marks these rows.
- Violating executions are counted from output lines (one per violating execution). In worker-pool
  runs with hundreds of thousands of violations the count differs between repetitions by at most
  9 per million (two workers writing at once can merge two lines); sequential runs are exact. The
  workbook shows the largest count.
- Runs that stop at the first violation (no `--keep-going`) take a time that depends on the random
  search order; it is not a cost measure.

## One run by hand

```bash
TF_PRINT_STATS=1 TF_PROGRESS=100000 RAYON_NUM_THREADS=<cores> \
  taskset -c <one logical CPU per physical core> \
  /usr/bin/time target/release/examples/<example> <args column of plan_v9.csv>
```

`TFSTATS execs=.. blocked=..` on stderr gives complete and blocked executions. In an exhaustive
(keep-going) run every violating execution prints one line naming the failing task (`task-N` or
`main-thread-ThreadId(N)`); their number is the "violating executions" column. The scheduler also
limits the address space of a run to 128 GB.

## The whole campaign again

```bash
git checkout b67ccf7 && cp benchmarks/v9/Cargo.lock.v9 Cargo.lock
cargo build --release --examples --locked            # cargo and rustc 1.97.1
bash benchmarks/v9/launch_v9.sh                      # tmux session v9sweep, reads plan_v9.csv
```

`launch_v9.sh` expects the layout used on brain05: `/local/sramezani/v9/{src,target,bench,logs}`
and a machine with at least 128 physical cores with hyper-thread siblings at `c + 128`; the core
lists are options of `sweep_v9.py` (`--cores`, `--service`, `--smt-offset`).

## Files

| file | role |
|---|---|
| `plan_v9.csv` | every job: id, cell, side, repetition, arguments, cores, pool mode |
| `plan_v9_launch.csv`, `plan_v9_added.csv`, `plan_v9_added2.csv` | the plan as launched, and the rows added on 11 October |
| `make_plan_from_v8.py`, `make_edge_plan.py`, `make_reps_plan.py` | the generators of those rows |
| `sweep_v9.py` | the scheduler |
| `launch_v9.sh`, `run_v9.sh`, `reboot_v9.sh`, `stop_v9.sh`, `check_v9.py` | launch in tmux, restart loop, resume after a reboot, stop early, status |
| `fetch_v9.sh` | copy the results from brain05 (needs the VPN) |
| `results_v9.csv` | one row per run: counts, violations, wall, CPU, memory, audit, cores, binary md5, arguments |
| `build_v9_xlsx.py` | results + `meta/*.json` -> the workbook |
| `meta/<example>.json`, `meta/FORMAT.md` | per-protocol metadata of the workbook: parameter columns, the regime of every cell, glossary, checker settings |
| `make_plan_v9.py` | plan generator for a design-file based campaign (only its field list and simulator are used) |

On brain05 everything is under `/local/sramezani/v9/` (`src`, `target`, `bench`, `logs`).

## While it runs

```bash
ssh sramezani@brain05.mpi-sws.org 'python3 /local/sramezani/v9/bench/check_v9.py'   # status
bash benchmarks/v9/fetch_v9.sh && python3 benchmarks/v9/build_v9_xlsx.py            # workbook from what is there
ssh sramezani@brain05.mpi-sws.org 'bash /local/sramezani/v9/bench/stop_v9.sh'       # stop early
```

Rows appended to `bench/plan_v9.csv` on brain05 (new ids) are queued within 30 s; replace the file
with `mv`, never write into it in place. A line `skip <job id>` in `bench/control_v9.txt` removes a
queued job or stops a running one. A file `bench/deadline_override.txt` (`YYYY-MM-DD HH:MM`) moves
the deadline.
