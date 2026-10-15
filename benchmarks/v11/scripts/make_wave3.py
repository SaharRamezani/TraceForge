#!/usr/bin/env python3
"""Wave 2 of the v11 campaign: higher rungs on the cores that were idle after the first queue drained.

Rows are appended to the plan files the running scheduler re-reads every 30 s (new ids only). Cell labels and
argument strings are built exactly like the v10/v11 plans so the table builder groups them with the ladder.
Both sides of a cell go to the same machine; 1 repetition each (reps for short cells may be added later).
"""
import csv, re, sys, json, os, collections

S = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/"
V9 = "/home/sahar/Downloads/TraceForge/benchmarks/v9/plan_v9.csv"

tmpl = {}
for r in csv.DictReader(open(V9)):
    tmpl.setdefault(r["example"], r)
FIELDS = list(next(iter(tmpl.values())).keys())

CELLS = []   # dict(fam, example, label, args, pred)


def add(fam, example, label, args, pred):
    CELLS.append(dict(fam=fam, example=example, label=label, args=args, pred=pred))



def abp(variant, k, r, w, pred):
    a = f"--variant {variant} --messages {k} --retries {r} --w-ratio {w} --keep-going"
    add("abp", "alternating_bit_timed", f"ABP-file {variant} K={k} R={r} W={w}U", a, pred)


def brp(n, mx, tr, sy, files, prop="p2", pred=600):
    a = (f"--n {n} --max {mx} --td 1 --t1 3 --tr {tr} --sync {sy} --k 2 --files {files} "
         f"--property {prop} --keep-going")
    add("brp", "brp_timed", f"BRP F={files} n={n} MAX={mx} TD=1 T1=3 TR={tr} SYNC={sy} k=2 ({prop})", a, pred)


def ct(d, p, to, h, pred=900):
    add("ct", "chandra_toueg_timed", f"CT delta={d} phi={p} to={to} H={h}",
        f"--delta {d} --phi {p} --to {to} --horizon {h} --keep-going", pred)


def lease(c, r, fen, lr, sr, p, pred=600):
    a = (f"--clients {c} --rounds {r} --u 2 --l-ratio {lr} --sd-ratio {sr} --ttl-ratio 5 "
         f"--fencing {'on' if fen else 'off'} --pause-ratio {p} --keep-going")
    L = 0 if lr == 0 else 1
    sd = int(round(sr * 2))
    pause = int(round(p * 2))
    add("lease_f" if fen else "lease", "lease_timed",
        f"Lease{' + fencing' if fen else ''} C={c} R={r} L={L} U=2 sd={sd} TTL=10 PAUSE={pause}", a, pred)


def yarn(k, dd, u, h, prop=None, qs=None, pred=600):
    a = f"--ams {k}"
    if qs:
        a += f" --queue-size {qs}"
    a += f" --deadline {dd} --u {u} --horizon {h}"
    if prop:
        a += f" --property {prop}"
    a += " --keep-going"
    add("yarn", "yarn_scheduler_timed", f"YARN K={k} QS={qs or k + 1} DD={dd} U={u} H={h} {prop or 'miss'}", a, pred)


abp("abp", 3, 5, 3, 700)
abp("abp", 4, 4, 3, 1800)
abp("abp", 5, 3, 3, 1500)
brp(3, 4, 27, 27, 1, pred=900)
ct(2, 2, 7, 19, 2600)
lease(2, 6, False, 0.5, 0, 4.5, pred=2000)
yarn(2, 8, 0, 15, qs=3, pred=1300)

# ======================================================================================= emit
DROP = {"--mode", "--parallel", "--workers", "--keep-going"}


def canon(args):
    toks = re.sub(r"^--mode \w+ ", "", args).split()
    out, i = [], 0
    while i < len(toks):
        t = toks[i]
        if t.startswith("--"):
            if i + 1 < len(toks) and not toks[i + 1].startswith("--"):
                v = toks[i + 1]
                i += 2
            else:
                v, i = "", i + 1
            if t not in DROP:
                out.append((t, v))
        else:
            i += 1
    return tuple(sorted(out))


have = set()
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(S + f"data/plan_v11_{m}.csv")):
        have.add((r["example"], canon(r["args"])))

todo, seen = [], set()
for c in CELLS:
    key = (c["example"], canon(c["args"]))
    if key in have or key in seen:
        print("skip (already planned):", c["fam"], c["label"])
        continue
    seen.add(key)
    todo.append(c)

# machine assignment: greedy on predicted core-seconds (2 sides)
load = {"brain05": 0.0, "brain10": 0.0}
rows = {"brain05": [], "brain10": []}
n = 0
for c in sorted(todo, key=lambda c: -c["pred"]):
    m = min(load, key=load.get)
    load[m] += 2 * min(c["pred"], 6000)
    n += 1
    t = tmpl[c["example"]]
    for side in ("baseline", "timed"):
        row = {k: t[k] for k in FIELDS}
        row.update(id=f"v11w3-{c['fam']}-{n:03d}-{side[0]}-r1", family=c["fam"], example=c["example"], cell=c["label"],
                   regime="", side=side, rep="1", reps="1", tier="timing", purpose="wave3",
                   args=f"--mode {side} {c['args']}", cores="1", parallel="none", pred_s=str(int(c["pred"])),
                   mem_gb="128", order=str(-int(c["pred"])), expect="", expect_exit="", params="")
        rows[m].append(row)

for m, rs in rows.items():
    with open(S + f"wave3_rows_{m}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rs)
    print(f"{m}: {len(rs)} jobs ({len(rs)//2} cells)")
cnt = collections.Counter(c["fam"] for c in todo)
print(dict(cnt), len(todo), "cells")
json.dump([dict(fam=c["fam"], label=c["label"], example=c["example"], args=c["args"], pred=c["pred"]) for c in todo],
          open(S + "wave3_cells.json", "w"), indent=1)
