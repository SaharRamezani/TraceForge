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


# ------------------------------------------------------------------ ABP
def abp(variant, k, r, w, pred):
    a = f"--variant {variant} --messages {k} --retries {r} --w-ratio {w} --keep-going"
    add("abp", "alternating_bit_timed", f"ABP-file {variant} K={k} R={r} W={w}U", a, pred)


for r, p in [(5, 300), (7, 900), (9, 3000)]:
    abp("abp", 2, r, 3, p)
for k, r, p in [(5, 2, 900), (7, 2, 2500), (3, 3, 600), (4, 3, 1500), (3, 4, 3000)]:
    abp("abp", k, r, 3, p)
for k, p in [(6, 300), (7, 2500)]:
    abp("par", k, 2, 3, p)


# ------------------------------------------------------------------ PAR
def par(dk, dl, dr, to, k, r=1, noerr=False, pred=600):
    a = f"--dk {dk} --dl {dl} --dr {dr} --to {to} --messages {k} --retries {r} --keep-going"
    if noerr:
        a += " --no-errs"
    add("par", "par_timed", f"PAR dK={dk} dL={dl} dR={dr} To={to} K={k} R={r}{' no-errs' if noerr else ''}", a, pred)


for k, p in [(8, 1200), (9, 6000)]:
    par(3, 3, 1, 9, k, pred=p)
for k, p in [(11, 500), (13, 3000), (14, 9000)]:
    par(3, 3, 1, 9, k, noerr=True, pred=p)
for k, r, p in [(3, 2, 30), (5, 2, 1200), (3, 3, 300), (4, 3, 2000)]:
    par(3, 3, 1, 9, k, r=r, pred=p)


# ------------------------------------------------------------------ BRP
def brp(n, mx, tr, sy, files, prop="p2", pred=600):
    a = (f"--n {n} --max {mx} --td 1 --t1 3 --tr {tr} --sync {sy} --k 2 --files {files} "
         f"--property {prop} --keep-going")
    add("brp", "brp_timed", f"BRP F={files} n={n} MAX={mx} TD=1 T1=3 TR={tr} SYNC={sy} k=2 ({prop})", a, pred)


for n, mx, p in [(6, 1, 600), (7, 1, 8000), (4, 2, 300), (3, 3, 300), (4, 3, 2000), (5, 2, 1500)]:
    brp(n, mx, 6 * mx + 3, 6 * mx + 3, 1, pred=p)
for mx, p in [(5, 500), (7, 800), (8, 3000)]:
    brp(2, mx, 6 * mx + 3, 6 * mx + 3, 1, pred=p)
brp(2, 2, 15, 15, 2, pred=300)
brp(2, 1, 9, 9, 3, pred=600)


# ------------------------------------------------------------------ spanning tree
def stp(rounds, fd, lr=None, sr=None, scenario="lost-hellos", pred=900):
    a = f"--scenario {scenario} --rounds {rounds} --forward-delay-ratio {fd}"
    if lr is not None:
        a += f" --l-ratio {lr}"
    if sr is not None:
        a += f" --sd-ratio {sr}"
    a += " --keep-going"
    add("stp", "spanning_tree_timed", f"STP {scenario} R={rounds} Fd={fd} L/U={lr or 0} sd/U={sr or 0}", a, pred)


stp(14, 26, pred=2500)
stp(15, 28, pred=6000)
stp(13, 23, pred=1200)
stp(13, 20, scenario="link-up", pred=1500)
stp(15, 20, scenario="link-up", pred=5000)


# ------------------------------------------------------------------ Chandra-Toueg
def ct(d, p, to, h, pred=900):
    add("ct", "chandra_toueg_timed", f"CT delta={d} phi={p} to={to} H={h}",
        f"--delta {d} --phi {p} --to {to} --horizon {h} --keep-going", pred)


ct(2, 2, 7, 17, 1500)
ct(2, 2, 7, 18, 3500)
ct(2, 4, 13, 17, 1500)
ct(2, 4, 13, 18, 3500)


# ------------------------------------------------------------------ sensor network
def sensor(a, pred=900):
    a = a.strip() + " --keep-going"
    add("sensor", "sensor_network_timed", "Sensor " + a.replace("--keep-going", "").strip()[:70], a, pred)


sensor("--table3-row 1 --readings danger --rounds 2", 600)
sensor("--table3-row 2 --readings danger --rounds 2", 600)
sensor("--period-ratio 6 --rounds 2", 600)
sensor("--sensors 1 --rounds 3", 1500)
sensor("--sensors 1 --rounds 4", 6000)
sensor("--readings safe --property sensor --rounds 4", 3000)


# ------------------------------------------------------------------ YARN
def yarn(k, dd, u, h, prop=None, qs=None, pred=600):
    a = f"--ams {k}"
    if qs:
        a += f" --queue-size {qs}"
    a += f" --deadline {dd} --u {u} --horizon {h}"
    if prop:
        a += f" --property {prop}"
    a += " --keep-going"
    add("yarn", "yarn_scheduler_timed", f"YARN K={k} QS={qs or k + 1} DD={dd} U={u} H={h} {prop or 'miss'}", a, pred)


for h, p in [(9, 300), (11, 300), (16, 3000)]:
    yarn(2, 8, 0, h, qs=3, pred=p)
for h, p in [(6, 100), (8, 1500)]:
    yarn(3, 8, 0, h, qs=4, pred=p)
for h, p in [(5, 100), (6, 600), (7, 3000)]:
    yarn(4, 8, 0, h, qs=5, pred=p)
for prop in ("preempt", "late-report"):
    yarn(2, 8, 0, 10, prop=prop, qs=3, pred=200)


# ------------------------------------------------------------------ online shop
def retail(var, c, n, r, d=7, s=20, pred=600):
    a = f"--variant {var} --order-services {n} --customers {c} --rounds {r} --stock {s} --u 1"
    if d is not None:
        a += f" --deadline-ratio {d}"
    a += " --keep-going"
    add("retail", "retail_saga_timed", f"Shop {var} C={c} N={n} R={r} S={s} D={d}", a, pred)


for c, n, r, p in [(3, 2, 2, 100), (2, 3, 2, 100), (4, 2, 1, 60), (3, 4, 1, 300), (4, 3, 1, 300), (4, 4, 1, 900),
                   (3, 2, 3, 3000), (2, 3, 3, 3000)]:
    retail("v2", c, n, r, 7, pred=p)


# ------------------------------------------------------------------ lease (fencing off / on)
def lease(c, r, fen, lr, sr, p, pred=600):
    a = (f"--clients {c} --rounds {r} --u 2 --l-ratio {lr} --sd-ratio {sr} --ttl-ratio 5 "
         f"--fencing {'on' if fen else 'off'} --pause-ratio {p} --keep-going")
    L = 0 if lr == 0 else 1
    sd = int(round(sr * 2))
    pause = int(round(p * 2))
    add("lease_f" if fen else "lease", "lease_timed",
        f"Lease{' + fencing' if fen else ''} C={c} R={r} L={L} U=2 sd={sd} TTL=10 PAUSE={pause}", a, pred)


for c, r, p in [(2, 4, 100), (2, 5, 800), (3, 3, 2500), (4, 2, 2500)]:
    lease(c, r, False, 0.5, 0, 4.5, pred=p)
lease(2, 5, True, 0, 0.5, 2, pred=2500)
lease(2, 6, False, 0, 0.5, 1.5, pred=9000)


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
    for r in csv.DictReader(open(S + f"plan_v11_{m}.csv")):
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
load = {"brain05": 72.0 * 0, "brain10": 0.0}
rows = {"brain05": [], "brain10": []}
n = 0
for c in sorted(todo, key=lambda c: -c["pred"]):
    m = min(load, key=load.get)
    load[m] += 2 * min(c["pred"], 6000)
    n += 1
    t = tmpl[c["example"]]
    for side in ("baseline", "timed"):
        row = {k: t[k] for k in FIELDS}
        row.update(id=f"v11w2-{c['fam']}-{n:03d}-{side[0]}-r1", family=c["fam"], example=c["example"], cell=c["label"],
                   regime="", side=side, rep="1", reps="1", tier="timing", purpose="wave2",
                   args=f"--mode {side} {c['args']}", cores="1", parallel="none", pred_s=str(int(c["pred"])),
                   mem_gb="128", order=str(-int(c["pred"])), expect="", expect_exit="", params="")
        rows[m].append(row)

for m, rs in rows.items():
    with open(S + f"wave2_rows_{m}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rs)
    print(f"{m}: {len(rs)} jobs ({len(rs)//2} cells)")
cnt = collections.Counter(c["fam"] for c in todo)
print(dict(cnt), len(todo), "cells")
json.dump([dict(fam=c["fam"], label=c["label"], example=c["example"], args=c["args"], pred=c["pred"]) for c in todo],
          open(S + "wave2_cells.json", "w"), indent=1)
