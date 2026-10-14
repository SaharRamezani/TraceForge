#!/usr/bin/env python3
"""Build the final-campaign plan (plan_final_b05.csv, plan_final_b10.csv) for the paper's evaluation table.

One job = one process, one pinned physical core, sequential (no --parallel, no --workers), both sides of a cell
with identical arguments except --mode. Rows of the paper table are the 13 protocol rows of 06-evaluation.tex.
Cells come from HEAD measurements (head_cells.json) plus a few edge attempts.

Repetition policy (single-core predicted wall of the slower side, seconds):
    pred <= 600  : 3 repetitions on brain05
    pred  > 600  : 1 repetition  on brain05
brain10 replication (1 repetition, ids rep 11): cells with 60 <= pred <= 3000, plus every edge attempt.
"""
import csv, json, sys, collections, re

S = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/"
V9 = "/home/sahar/Downloads/TraceForge/benchmarks/v9/plan_v9.csv"

v9rows = list(csv.DictReader(open(V9)))
TEMPLATE = {}
for r in v9rows:
    TEMPLATE.setdefault(r["example"], r)
FIELDS = list(v9rows[0].keys())

CELLS = []   # dict(fam, example, label, args, pred, edge)
CONTROL = set()


def add(fam, example, label, args, pred=None, edge=False):
    CELLS.append(dict(fam=fam, example=example, label=label, args=args, pred=pred, edge=edge))


# ---------------------------------------------------------------- SWIM (row 7)
def swim(n, r, lr, pred=None, edge=False):
    args = (f"--nodes {n} --rounds {r} --u 2 --sd-ratio 0.5 --w-probe-ratio 1 --l-ratio {lr} "
            f"--w-suspect-ratio 3.0 --keep-going")
    L = 0 if lr == 0 else 1
    add("swim", "swim_timed", f"SWIM N={n} R={r} L={L} U=2 sd=1 Wp=2 Ws=6", args, pred, edge)


# ---------------------------------------------------------------- lease (rows 5 and 6)
def lease(c, r, fen, lr, sr, p, pred=None, edge=False):
    args = (f"--clients {c} --rounds {r} --u 2 --l-ratio {lr} --sd-ratio {sr} --ttl-ratio 5 "
            f"--fencing {'on' if fen else 'off'} --pause-ratio {p} --keep-going")
    L = 0 if lr == 0 else 1
    sd = int(round(sr * 2))
    pause = int(round(p * 2))
    add("lease_f" if fen else "lease", "lease_timed",
        f"Lease{' + fencing' if fen else ''} C={c} R={r} L={L} U=2 sd={sd} TTL=10 PAUSE={pause}", args, pred, edge)


def load_plan_cells():
    exec(open(S + "final_cells.py").read(), globals())


DROP = {"--mode", "--parallel", "--workers", "--keep-going", "--property"}
DEFAULTS = {"--order-services": "2", "--customers": "2", "--assign": "fixed", "--products": "1", "--max-qty": "2",
            "--sd": "0", "--fix": "none", "--variant": None}


def canon(args):
    toks = args.split()
    out = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.startswith("--"):
            if i + 1 < len(toks) and not toks[i + 1].startswith("--"):
                v = toks[i + 1]
                i += 2
            else:
                v = ""
                i += 1
            if t in DROP:
                continue
            if DEFAULTS.get(t) is not None and DEFAULTS[t] == v:
                continue
            out.append((t, v))
        else:
            i += 1
    return tuple(sorted(out))


def pred_from_head(example, args, headcells):
    """predicted single-core wall (s) of the slower side from HEAD measurements (flag order and the
    --property selector ignored), None unless both sides finished on HEAD"""
    key = canon(args)
    best = None
    for c in headcells:
        if c["example"] != example or canon(c["args"]) != key:
            continue
        w = []
        for side in ("baseline", "timed"):
            ok = [r["wall"] for r in c[side] if r["fin"] and r["wall"] is not None]
            if ok:
                w.append(min(ok))
        if len(w) == 2:
            v = max(w)
            best = v if best is None else min(best, v)
    return best


def main():
    headcells = json.load(open(S + "head_cells.json"))
    load_plan_cells()
    b05, b10 = [], []
    seen = set()
    n = 0
    # control cells: per family the measured cell whose predicted wall is closest to 45 s
    preds = {}
    for c in CELLS:
        p0 = c["pred"] if c["pred"] is not None else pred_from_head(c["example"], c["args"], headcells)
        if p0 is not None and not c["edge"] and 10 <= p0 <= 200:
            preds.setdefault(c["fam"], []).append((abs(p0 - 45), c["label"]))
    for fam, lst in preds.items():
        CONTROL.add(sorted(lst)[0][1])
    for c in CELLS:
        key = (c["example"], c["args"])
        if key in seen:
            continue
        seen.add(key)
        n += 1
        pred = c["pred"] if c["pred"] is not None else pred_from_head(c["example"], c["args"], headcells)
        unknown = pred is None
        if unknown and not c["edge"]:
            pred = 300.0                       # not measured at HEAD as such: assumed moderate, one repetition
        if c["edge"]:
            pred = 3600.0
        if c["edge"] or unknown:
            reps = 1
        elif pred <= 60:
            reps = 3
        elif pred <= 300:
            reps = 2
        else:
            reps = 1
        # queue order: edge attempts first (they need the whole window), then known long, unknown, short
        if c["edge"]:
            order = -100000
        elif unknown:
            order = -5000
        else:
            order = -int(pred)
        t = TEMPLATE[c["example"]]
        for side in ("baseline", "timed"):
            for rep in range(1, reps + 1):
                row = {k: t[k] for k in FIELDS}
                row.update(id=f"fin-{c['fam']}-{n:03d}-{side[0]}-r{rep}", family=c["fam"], example=c["example"],
                           cell=c["label"], regime="", side=side, rep=str(rep), reps=str(reps), tier="timing",
                           purpose="edge" if c["edge"] else ("unmeasured" if unknown else "final"),
                           args=f"--mode {side} {c['args']}", cores="1", parallel="none",
                           pred_s=str(int(pred)), mem_gb="128", order=str(order), expect="", expect_exit="",
                           params="")
                b05.append(row)
            # brain10: replication of edge attempts and of known long cells, plus short control cells
            repl = c["edge"] or (not unknown and 240 <= pred <= 3000) or (not unknown and c["label"] in CONTROL)
            if repl:
                row = {k: t[k] for k in FIELDS}
                row.update(id=f"fin-{c['fam']}-{n:03d}-{side[0]}-r11", family=c["fam"], example=c["example"],
                           cell=c["label"], regime="", side=side, rep="11", reps="1", tier="timing",
                           purpose="replication", args=f"--mode {side} {c['args']}", cores="1", parallel="none",
                           pred_s=str(int(pred)), mem_gb="128", order=str(order), expect="", expect_exit="",
                           params="")
                b10.append(row)
    for name, rows in (("plan_final_b05.csv", b05), ("plan_final_b10.csv", b10)):
        with open(S + name, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
    cnt = collections.Counter(r["family"] for r in b05)
    longj = sum(1 for r in b05 if float(r["pred_s"]) > 300)
    print("cells:", n, " brain05 jobs:", len(b05), " brain10 jobs:", len(b10), " jobs with pred>300s on brain05:", longj)
    print("core-hours brain05 (pred, upper bound at 60 min cap):",
          round(sum(min(float(r["pred_s"]), 3600) for r in b05) / 3600, 1))
    print(dict(cnt))


if __name__ == "__main__":
    main()
