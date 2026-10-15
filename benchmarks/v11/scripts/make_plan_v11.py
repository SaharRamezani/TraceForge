#!/usr/bin/env python3
"""Plan of the 2-hour bounded-program campaign (v11).

Universe = the cells of the v10 campaign (all programs but nisshi), rerun on the daemon-free binaries.
Predicted wall of a side = v10 wall x a per-program factor for the termination messages (upper bounds from the
agents' acceptance runs). A cell is `core` when both sides finished in v10 and the slower predicted side fits
in 110 min; everything else is an `attempt` (one repetition, runs until the stop). Both sides of a cell and all
its repetitions run on the same machine.
"""
import csv, re, statistics as st, collections, math, sys

V10 = "/home/sahar/Downloads/TraceForge/benchmarks/v10/data/"
OUT = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/"
V9 = "/home/sahar/Downloads/TraceForge/benchmarks/v9/plan_v9.csv"
WINDOW = 7200.0
FIT = 6600.0

plan = list(csv.DictReader(open(V10 + "plan_final_b05.csv")))
res = list(csv.DictReader(open(V10 + "results_final_b05.csv")))
tmpl = {}
for r in csv.DictReader(open(V9)):
    tmpl.setdefault(r["example"], r)
FIELDS = list(next(iter(tmpl.values())).keys())


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# (family, cell, side) -> list of finished walls; DNF flag
walls = collections.defaultdict(list)
dnf = collections.defaultdict(bool)
for r in res:
    k = (r["family"], r["cell"], r["side"])
    if r["status"].startswith("finished") or r["status"].startswith("error (exit 3)"):
        w = f(r["wall_s"])
        if w is not None:
            walls[k].append(w)
    else:
        dnf[k] = True

args = {}
example = {}
for r in plan:
    if r["family"] in ("nisshi", "nisshiB"):
        continue
    if r["purpose"] == "replication":
        continue
    a = re.sub(r"^--mode \w+ ", "", r["args"])
    args[(r["family"], r["cell"])] = a
    example[(r["family"], r["cell"])] = r["example"]


def factor(fam, side, cell):
    n = re.search(r"\bN=(\d+)", cell)
    nn = int(n[1]) if n else 3
    if fam == "swim":
        return (0.25 * nn + 0.6) if side == "baseline" else (0.35 * nn + 0.8)
    if fam == "nrp":
        return 3.6 if side == "baseline" else 1.8
    if fam == "sensor":
        return 3.0 if side == "baseline" else 1.8
    if fam == "yarn":
        return 1.5
    return 1.0


cells = []
for (fam, cell), a in args.items():
    pred = {}
    for side in ("baseline", "timed"):
        k = (fam, cell, side)
        if walls.get(k):
            pred[side] = st.median(walls[k]) * factor(fam, side, cell)
        else:
            pred[side] = None
    both = all(pred[s] is not None for s in pred)
    worst = max([p for p in pred.values() if p is not None] or [WINDOW * 2])
    if both and worst <= FIT:
        cls = "core"
    else:
        cls = "attempt"
    cells.append(dict(fam=fam, cell=cell, args=a, ex=example[(fam, cell)], pred=pred, worst=worst, cls=cls))

# repetitions
for c in cells:
    if c["cls"] == "attempt":
        c["reps"] = 1
    elif c["worst"] <= 900:
        c["reps"] = 3
    elif c["worst"] <= 3600:
        c["reps"] = 2
    else:
        c["reps"] = 1

# machine assignment: greedy on predicted core-seconds, pairs and reps together
load = {"brain05": 0.0, "brain10": 0.0}
for c in sorted(cells, key=lambda c: -(2 * min(c["worst"], WINDOW) * c["reps"])):
    m = min(load, key=load.get)
    c["machine"] = m
    load[m] += 2 * min(c["worst"], WINDOW) * c["reps"]

# control cells: short core cells of distinct families, duplicated on the other machine (1 rep, ids -c)
controls = []
seen = set()
for c in sorted([c for c in cells if c["cls"] == "core" and 5 <= c["worst"] <= 120], key=lambda c: c["worst"]):
    if c["fam"] in seen:
        continue
    seen.add(c["fam"])
    controls.append(c)

rows = {"brain05": [], "brain10": []}
n = 0
for c in cells:
    n += 1
    for side in ("baseline", "timed"):
        for rep in range(1, c["reps"] + 1):
            t = tmpl[c["ex"]]
            row = {k: t[k] for k in FIELDS}
            pred = c["pred"][side] or c["worst"]
            row.update(id=f"v11-{c['fam']}-{n:03d}-{side[0]}-r{rep}", family=c["fam"], example=c["ex"], cell=c["cell"],
                       regime="", side=side, rep=str(rep), reps=str(c["reps"]), tier="timing",
                       purpose=c["cls"], args=f"--mode {side} {c['args']}", cores="1", parallel="none",
                       pred_s=str(int(pred)), mem_gb="128", order=str(-int(min(pred, 99999))), expect="", expect_exit="",
                       params="")
            rows[c["machine"]].append(row)
    if c in controls:
        other = "brain10" if c["machine"] == "brain05" else "brain05"
        for side in ("baseline", "timed"):
            t = tmpl[c["ex"]]
            row = {k: t[k] for k in FIELDS}
            pred = c["pred"][side] or c["worst"]
            row.update(id=f"v11-{c['fam']}-{n:03d}-{side[0]}-c1", family=c["fam"], example=c["ex"], cell=c["cell"],
                       regime="", side=side, rep="21", reps="1", tier="timing", purpose="control",
                       args=f"--mode {side} {c['args']}", cores="1", parallel="none", pred_s=str(int(pred)),
                       mem_gb="128", order=str(-int(min(pred, 99999))), expect="", expect_exit="", params="")
            rows[other].append(row)

import os
os.makedirs(OUT, exist_ok=True)
for m, rs in rows.items():
    with open(OUT + f"plan_v11_{m}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rs)
    longj = sum(1 for r in rs if r["purpose"] == "attempt" or float(r["pred_s"]) > 600)
    hours = sum(min(float(r["pred_s"]), WINDOW) for r in rs) / 3600
    print(f"{m}: {len(rs)} jobs, {longj} long/attempt jobs, <= {hours:.0f} core-hours (upper bound)")
cnt = collections.Counter((c["fam"], c["cls"]) for c in cells)
print(len(cells), "cells;", sum(1 for c in cells if c["cls"] == "core"), "core,", sum(1 for c in cells if c["cls"] == "attempt"), "attempt;", len(controls), "controls")
for fam in sorted({c["fam"] for c in cells}):
    print(f"  {fam:8s} core={cnt[(fam,'core')]:3d} attempt={cnt[(fam,'attempt')]:3d}")
