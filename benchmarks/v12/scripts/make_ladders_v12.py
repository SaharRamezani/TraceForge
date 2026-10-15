#!/usr/bin/env python3
"""Phase B: one-dimensional size ladders per (family, regime), climbed adaptively on each machine by climb_v12.py.

Writes ladders_<machine>.json (rungs carry fully formed scheduler rows) and cells_v12_B.json (metadata for the table).
A rung = the Mustard cell (+ its Must baseline class, if not yet submitted). Rank continues from 100.
"""
import csv, json, os, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from regimes_v12 import *

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
V9 = "/home/sahar/Downloads/TraceForge/benchmarks/v9/plan_v9.csv"
tmpl = {}
for r in csv.DictReader(open(V9)):
    tmpl.setdefault(r["example"], r)
FIELDS = list(next(iter(tmpl.values())).keys())


def g(x):
    return f"{x:.9g}"


def ratio(v, u):
    return g(v / u) if u else "0"


LADDERS = []     # dict(name, fam, ex, kind, rungs=[dict(size, label, args_m, args_b, bkey, regime)])


def ladder(name, fam, ex, rungs):
    LADDERS.append(dict(name=name, fam=fam, ex=ex, rungs=rungs))


# ----------------------------------------------------------------- lease / fencing
def lease_rung(fen, c, r, net, ttl, pausekind, sd=0, Lv=0):
    nm, L, U, Lp = NETS[net]
    b = ttl - 2 * (U - Lv + sd)
    p = {"below": b - 1, "at": b, "far": ttl + 1000}[pausekind]
    fam = "lease_f" if fen else "lease"
    label = f"Lease{' + fencing' if fen else ''} C={c} R={r} net={net}[{Lv},{U}] sd={sd} TTL={ttl} PAUSE={p}"
    am = (f"--clients {c} --rounds {r} --u {U} --l-ratio {ratio(Lv, U)} --sd-ratio {ratio(sd, U)} --ttl-ratio {ratio(ttl, U)} "
          f"--fencing {'on' if fen else 'off'} --pause-ratio {ratio(p, U)} --keep-going")
    ab = (f"--clients {c} --rounds {r} --u 1 --l-ratio 0 --sd-ratio 0 --ttl-ratio 12000 --fencing {'on' if fen else 'off'} "
          f"--pause-ratio 200 --keep-going")
    return dict(size=(c, r), label=label, args_m=am, args_b=ab, bkey=(fam, c, r), regime=net)


for net in ("A", "C", "D"):
    for pk in ("below", "at"):
        ladder(f"lease|{net}|{pk}|R", "lease", "lease_timed", [lease_rung(False, 2, r, net, 12000, pk) for r in range(1, 13)])
        ladder(f"lease|{net}|{pk}|C", "lease", "lease_timed", [lease_rung(False, c, 1, net, 12000, pk) for c in range(2, 14)])
        ladder(f"lease|{net}|{pk}|D", "lease", "lease_timed", [lease_rung(False, k, k, net, 12000, pk) for k in range(2, 8)])
ladder("lease|A|far|R", "lease", "lease_timed", [lease_rung(False, 2, r, "A", 12000, "far") for r in range(1, 13)])
for net in ("A", "D"):
    ladder(f"lease_f|{net}|at|R", "lease_f", "lease_timed", [lease_rung(True, 2, r, net, 12000, "at") for r in range(1, 13)])
    ladder(f"lease_f|{net}|at|C", "lease_f", "lease_timed", [lease_rung(True, c, 1, net, 12000, "at") for c in range(2, 14)])
ladder("lease|A|at|R|gfs60", "lease", "lease_timed", [lease_rung(False, 2, r, "A", 60000, "at") for r in range(1, 13)])


# ----------------------------------------------------------------- SWIM
def swim_rung(n, r, prof, net, sd=0):
    nm, L, U, Lp = NETS[net]
    wp, ws = SWIM_PROFILES[prof]
    label = f"SWIM N={n} R={r} timers={prof}(Wp={wp},Ws={ws}) net={net}[{L},{U}] sd={sd}"
    am = (f"--nodes {n} --rounds {r} --u {U} --l-ratio {ratio(L, U)} --sd-ratio {ratio(sd, U)} --w-probe-ratio {ratio(wp, U)} "
          f"--w-suspect-ratio {ratio(ws, U)} --keep-going")
    ab = f"--nodes {n} --rounds {r} --u 1 --l-ratio 0 --sd-ratio 0 --w-probe-ratio 500 --w-suspect-ratio 4000 --keep-going"
    return dict(size=(n, r), label=label, args_m=am, args_b=ab, bkey=("swim", n, r), regime=f"{prof}/{net}")


for prof, net in (("lan", "A"), ("wan", "C"), ("local", "D"), ("wan", "D"), ("local", "C")):
    ladder(f"swim|{prof}|{net}|N", "swim", "swim_timed", [swim_rung(n, 1, prof, net) for n in range(3, 22)])
    ladder(f"swim|{prof}|{net}|R", "swim", "swim_timed", [swim_rung(3, r, prof, net) for r in range(1, 14)])
    ladder(f"swim|{prof}|{net}|D", "swim", "swim_timed", [swim_rung(k + 2, k, prof, net) for k in range(1, 8)])


# ----------------------------------------------------------------- ABP / par variant
def abp_rung(variant, k, r, net, w):
    nm, L, U, Lp = NETS[net]
    label = f"ABP-file {variant} K={k} R={r} W={w} net={net}[{L},{U}] sd=0"
    am = (f"--variant {variant} --messages {k} --retries {r} --u {U} --l-ratio {ratio(L, U)} --sd-ratio 0 --w-ratio {ratio(w, U)} "
          f"--keep-going")
    ab = f"--variant {variant} --messages {k} --retries {r} --u 1 --l-ratio 0 --sd-ratio 0 --w-ratio 200 --keep-going"
    return dict(size=(k, r), label=label, args_m=am, args_b=ab, bkey=("abp", variant, k, r), regime=f"{net}/W{w}")


for variant, net, w in (("par", "A", 200), ("par", "C", 200), ("par", "C", 1000), ("par", "D", 1000), ("abp", "A", 200), ("abp", "D", 1000)):
    ladder(f"abp|{variant}|{net}|{w}|K", "abp", "alternating_bit_timed", [abp_rung(variant, k, 1, net, w) for k in range(2, 31)])
    ladder(f"abp|{variant}|{net}|{w}|R", "abp", "alternating_bit_timed", [abp_rung(variant, 2, r, net, w) for r in range(1, 15)])
    ladder(f"abp|{variant}|{net}|{w}|D", "abp", "alternating_bit_timed", [abp_rung(variant, k + 1, k, net, w) for k in range(1, 9)])


# ----------------------------------------------------------------- BRP
def brp_rung(n, mx, f, net, t1, prop="p2"):
    nm, L, U, Lp = NETS[net]
    td = U
    tr = 2 * mx * t1 + 3 * td
    label = f"BRP F={f} n={n} MAX={mx} TD={td} T1={t1} TR={tr} SYNC={tr} net={net} ({prop})"
    am = f"--n {n} --max {mx} --td {td} --t1 {t1} --tr {tr} --sync {tr} --k 2 --files {f} --property {prop} --keep-going"
    ab = f"--n {n} --max {mx} --td 1 --t1 200 --tr {2 * mx * 200 + 3} --sync {2 * mx * 200 + 3} --k 2 --files {f} --property {prop} --keep-going"
    return dict(size=(n, mx, f), label=label, args_m=am, args_b=ab, bkey=("brp", n, mx, f, prop), regime=f"{net}/T1={t1}")


for net, t1 in (("A", 200), ("C", 1000), ("D", 1000)):
    ladder(f"brp|{net}|{t1}|n", "brp", "brp_timed", [brp_rung(n, 1, 1, net, t1) for n in range(2, 16)])
    ladder(f"brp|{net}|{t1}|MAX", "brp", "brp_timed", [brp_rung(2, mx, 1, net, t1) for mx in range(1, 12)])
    ladder(f"brp|{net}|{t1}|F", "brp", "brp_timed", [brp_rung(2, 1, f, net, t1) for f in range(1, 6)])


# ----------------------------------------------------------------- NRP
def nrp_rung(variant, r, net):
    nm, L, U, Lp = NETS[net]
    H = VRRP_ADV if 2 * U < VRRP_ADV else 10 * VRRP_ADV
    label = f"NRP {variant} R={r} M=3 H={H} P={2 * U} net={net}[{L},{U}] sd=0"
    am = (f"--variant {variant} --rounds {r} --max-missed 3 --u {U} --l-ratio {ratio(L, U)} --sd-ratio 0 --h-ratio {ratio(H, U)} "
          f"--pt-ratio 2 --keep-going")
    ab = f"--variant {variant} --rounds {r} --max-missed 3 --u 1 --l-ratio 0 --sd-ratio 0 --h-ratio 1000 --pt-ratio 2 --keep-going"
    return dict(size=(r,), label=label, args_m=am, args_b=ab, bkey=("nrp", variant, r), regime=net)


for variant in ("nrp", "noopt", "leasing"):
    for net in ("A", "C", "D"):
        ladder(f"nrp|{variant}|{net}", "nrp", "nrp_failure_detector_timed", [nrp_rung(variant, r, net) for r in range(3, 10)])


# ----------------------------------------------------------------- YARN
def yarn_rung(k, h, u, prop="miss", dd=None):
    dd = dd if dd is not None else (7 if k == 1 else 8)
    netname = "A-C" if u == 0 else "D"
    propflag = f" --property {prop}" if prop != "miss" else ""
    label = f"YARN K={k} QS={k + 1} DD={dd} U={u} H={h} {prop} net={netname}"
    am = f"--ams {k} --queue-size {k + 1} --deadline {dd} --u {u} --l 0 --sd 0 --horizon {h}{propflag} --keep-going"
    ab = f"--ams {k} --queue-size {k + 1} --deadline {dd} --u 0 --l 0 --sd 0 --horizon {h}{propflag} --keep-going"
    return dict(size=(k, h), label=label, args_m=am, args_b=ab, bkey=("yarn", k, h, dd, prop), regime=netname)


for u in (0, 3):
    ladder(f"yarn|{u}|H", "yarn", "yarn_scheduler_timed", [yarn_rung(2, h, u) for h in range(8, 26)])
    ladder(f"yarn|{u}|K", "yarn", "yarn_scheduler_timed", [yarn_rung(k, 6, u) for k in range(1, 5)])
    ladder(f"yarn|{u}|H1p", "yarn", "yarn_scheduler_timed", [yarn_rung(1, h, u, "preempt") for h in range(12, 60, 4)])


# ----------------------------------------------------------------- shop
def shop_rung(c, n, r, net):
    nm, L, U, Lp = NETS[net]
    label = f"Shop v2 C={c} N={n} R={r} S=20 D={HTTP_TIMEOUT} net={net}[{L},{U}] sd=0"
    am = (f"--variant v2 --order-services {n} --customers {c} --rounds {r} --stock 20 --u {U} --l-ratio {ratio(L, U)} "
          f"--sd-ratio 0 --deadline-ratio {ratio(HTTP_TIMEOUT, U)} --keep-going")
    ab = f"--variant v2 --order-services {n} --customers {c} --rounds {r} --stock 20 --u 1 --l-ratio 0 --sd-ratio 0 --deadline-ratio 29000 --keep-going"
    return dict(size=(c, n, r), label=label, args_m=am, args_b=ab, bkey=("retail", c, n, r), regime=net)


for net in ("A", "D"):
    ladder(f"shop|{net}|R", "retail", "retail_saga_timed", [shop_rung(2, 2, r, net) for r in range(1, 9)])
    ladder(f"shop|{net}|C", "retail", "retail_saga_timed", [shop_rung(c, 2, 2, net) for c in range(2, 9)])

# ================================================================== emit
PRED = {"lease": 2.2, "lease_f": 2.2, "swim": 2.4, "abp": 2.0, "brp": 2.0, "nrp": 8.0, "yarn": 2.3, "retail": 3.5}
load = {"brain05": 0.0, "brain10": 0.0}
out = {"brain05": [], "brain10": []}
meta = []
cn = 0
uid = collections.Counter()


def row(fam, ex, label, side, args, pred, rep, reps, rank, tag, params):
    t = tmpl[ex]
    r = {k: t[k] for k in FIELDS}
    uid[fam] += 1
    r.update(id=f"v12L-{fam}-{uid[fam]:05d}-{side[0]}-r{rep}", family=fam, example=ex, cell=label, regime=tag, side=side,
             rep=str(rep), reps=str(reps), tier="timing", purpose="ladder", args=f"--mode {side} {args}", cores="1",
             parallel="none", pred_s=str(int(pred)), mem_gb="128", order=str(int(pred)), expect="", expect_exit="", params=params)
    return r


for lad in sorted(LADDERS, key=lambda l: -len(l["rungs"])):
    m = min(load, key=load.get)
    load[m] += len(lad["rungs"]) * PRED[lad["fam"]]
    rungs = []
    for i, rg in enumerate(lad["rungs"]):
        cn += 1
        cid = f"L{cn:05d}"
        rank = 100 + i
        reps = 2 if i < 3 else 1
        pm = 3 * (PRED[lad["fam"]] ** i)
        mrows = [row(lad["fam"], lad["ex"], rg["label"], "timed", rg["args_m"], pm, k, reps, rank, rg["regime"], cid) for k in range(1, reps + 1)]
        bk = (m,) + tuple(rg["bkey"])
        brows = [row(lad["fam"], lad["ex"], f"{lad['fam']} baseline " + "-".join(map(str, rg["bkey"][1:])), "baseline", rg["args_b"], pm * 3,
                     k, reps, rank, "baseline", "b|" + "|".join(map(str, bk))) for k in range(1, reps + 1)]
        rungs.append(dict(cid=cid, size=list(rg["size"]), mrows=mrows, brows=brows, bparams="b|" + "|".join(map(str, bk))))
        meta.append(dict(cid=cid, machine=m, fam=lad["fam"], label=rg["label"], size=list(rg["size"]), rank=rank,
                         regime=rg["regime"], bkey=list(bk), args=rg["args_m"], phase="B", ladder=lad["name"]))
    out[m].append(dict(name=lad["name"], fam=lad["fam"], rungs=rungs))

for m in out:
    json.dump(out[m], open(HERE + f"ladders_{m}.json", "w"))
    print(m, len(out[m]), "ladders,", sum(len(l["rungs"]) for l in out[m]), "rungs")
json.dump(meta, open(HERE + "cells_v12_B.json", "w"))
