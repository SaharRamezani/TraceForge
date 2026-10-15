#!/usr/bin/env python3
"""v12 plan generator: regime-based cells (citable L, U, sd, W) over size ladders, both machines.

Output: plan_v12_brain05.csv, plan_v12_brain10.csv (scheduler rows), cells_v12.json (cell metadata for the table builder).
Order = ascending predicted cost, so cheap cells finish first and the long ones start when cores free up.
"""
import csv, json, math, sys, collections, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from regimes_v12 import *

V9 = "/home/sahar/Downloads/TraceForge/benchmarks/v9/plan_v9.csv"
OUT = os.path.dirname(os.path.abspath(__file__)) + "/"
tmpl = {}
for r in csv.DictReader(open(V9)):
    tmpl.setdefault(r["example"], r)
FIELDS = list(next(iter(tmpl.values())).keys())

CELLS = []      # dict(fam, ex, label, size, rank, regime, group, args_m (mustard), args_b (baseline), bkey)


def g(x):       # compact float for ratios
    s = f"{x:.9g}"
    return s


def ratio(v, u):
    return g(v / u) if u else "0"


def add(fam, ex, size, rank, regime, group, args_m, args_b, bkey, label):
    CELLS.append(dict(fam=fam, ex=ex, size=size, rank=rank, regime=regime, group=group,
                      args_m=args_m, args_b=args_b, bkey=bkey, label=label))


def tier(rank, full, mid):
    return 0 if rank < full else (1 if rank < mid else 2)


# ------------------------------------------------------------------ lease (rows lease, lease_f)
LEASE_SIZES = [(2, 1), (2, 2), (3, 1), (2, 3), (3, 2), (4, 1), (2, 4), (5, 1), (4, 2), (3, 3)]
for rank, (c, r) in enumerate(LEASE_SIZES):
    for fen in (False, True):
        fam = "lease_f" if fen else "lease"
        t = tier(rank, 4, 7)
        if fen:
            nets = ["A", "C", "D"] if t == 0 else (["A", "D"] if t == 1 else [])
        else:
            nets = ["A", "B", "C", "D", "E"] if t == 0 else (["A", "C", "D"] if t == 1 else ["A", "D"])
        ttls = list(LEASE_TTL.items()) if (t == 0 and not fen) else [("chubby12", 12000)]
        sds = ("0", "U") if (t == 0 and not fen) else ("0",)
        for net in nets:
            name, L, U, Lp = NETS[net]
            lvars = [L, Lp] if (t == 0 and not fen and net in ("B", "C")) else [L]
            for variant_L in lvars:
                for ttlname, ttl in ttls:
                    for sdk in sds:
                        sd = U if sdk == "U" else 0
                        b = ttl - 2 * (U - variant_L + sd)
                        if b <= 0:
                            pauses = [200]
                        elif fen:
                            pauses = sorted({b - 1, b, ttl + 1000}) if t == 0 else sorted({b - 1, b})
                        elif t == 0:
                            pauses = sorted({200, b - 1, b, ttl + 1000, 180000})
                        elif t == 1:
                            pauses = sorted({200, b - 1, b, ttl + 1000})
                        else:
                            pauses = sorted({b - 1, b})
                        for p in pauses:
                            tag = f"{net}{'p' if variant_L != L else ''}"
                            label = (f"Lease{' + fencing' if fen else ''} C={c} R={r} net={tag}[{variant_L},{U}] sd={sd} "
                                     f"TTL={ttl} PAUSE={p}")
                            am = (f"--clients {c} --rounds {r} --u {U} --l-ratio {ratio(variant_L, U)} --sd-ratio {ratio(sd, U)} "
                                  f"--ttl-ratio {ratio(ttl, U)} --fencing {'on' if fen else 'off'} --pause-ratio {ratio(p, U)} --keep-going")
                            ab = (f"--clients {c} --rounds {r} --u 1 --l-ratio 0 --sd-ratio 0 --ttl-ratio 12000 "
                                  f"--fencing {'on' if fen else 'off'} --pause-ratio 200 --keep-going")
                            add(fam, "lease_timed", (c, r), rank, tag, f"{fam}|{net}|{ttlname}", am, ab, (fam, c, r), label)

# ------------------------------------------------------------------ SWIM
SWIM_SIZES = [(3, 1), (4, 1), (3, 2), (5, 1), (4, 2), (6, 1), (3, 3), (5, 2), (7, 1), (4, 3), (8, 1), (6, 2), (3, 4), (9, 1)]
for rank, (n, r) in enumerate(SWIM_SIZES):
    t = tier(rank, 6, 14)
    if t == 0:
        combos = [(p, nt) for p in SWIM_PROFILES for nt in "ABCD"] + [("wan", "E"), ("lan", "E")]
    elif t == 1:
        combos = [("lan", "A"), ("wan", "C"), ("local", "C"), ("local", "D"), ("wan", "D")]
    else:
        combos = [("lan", "A"), ("local", "D")]
    for prof, net in combos:
        name, L, U, Lp = NETS[net]
        wp, ws = SWIM_PROFILES[prof]
        for sdk in (("0", "U") if rank < 6 else ("0",)):
            sd = U if sdk == "U" else 0
            label = f"SWIM N={n} R={r} timers={prof}(Wp={wp},Ws={ws}) net={net}[{L},{U}] sd={sd}"
            am = (f"--nodes {n} --rounds {r} --u {U} --l-ratio {ratio(L, U)} --sd-ratio {ratio(sd, U)} "
                  f"--w-probe-ratio {ratio(wp, U)} --w-suspect-ratio {ratio(ws, U)} --keep-going")
            ab = f"--nodes {n} --rounds {r} --u 1 --l-ratio 0 --sd-ratio 0 --w-probe-ratio 500 --w-suspect-ratio 4000 --keep-going"
            add("swim", "swim_timed", (n, r), rank, f"{prof}/{net}", f"swim|{prof}|{net}", am, ab, ("swim", n, r), label)

# ------------------------------------------------------------------ ABP / PAR variant (TCP-like retransmission timers)
ABP_SIZES = [(2, 1), (3, 1), (4, 1), (2, 2), (5, 1), (3, 2), (6, 1), (4, 2), (7, 1), (2, 3), (5, 2), (8, 1), (3, 3)]
for rank, (k, r) in enumerate(ABP_SIZES):
    t = tier(rank, 5, 9)
    if t == 0:
        combos = [(nt, w) for nt in "ABCDE" for w in RTO]
    elif t == 1:
        combos = [(nt, w) for nt in "ACD" for w in (200, 1000)]
    else:
        combos = [("A", 200), ("C", 200), ("D", 1000)]
    for variant in ("abp", "par"):
        for net, w in combos:
            name, L, U, Lp = NETS[net]
            label = f"ABP-file {variant} K={k} R={r} W={w} net={net}[{L},{U}] sd=0"
            am = (f"--variant {variant} --messages {k} --retries {r} --u {U} --l-ratio {ratio(L, U)} --sd-ratio 0 "
                  f"--w-ratio {ratio(w, U)} --keep-going")
            ab = f"--variant {variant} --messages {k} --retries {r} --u 1 --l-ratio 0 --sd-ratio 0 --w-ratio 200 --keep-going"
            add("abp", "alternating_bit_timed", (k, r), rank, f"{net}/W{w}", f"abp|{variant}|{net}|{w}", am, ab, ("abp", variant, k, r), label)

# ------------------------------------------------------------------ BRP (T1 = RTO, TD = U, TR = 2 MAX T1 + 3 TD, SYNC = TR)
BRP_SIZES = [(2, 1, 1), (3, 1, 1), (2, 2, 1), (4, 1, 1), (3, 2, 1), (2, 3, 1), (5, 1, 1), (4, 2, 1), (3, 3, 1), (2, 1, 2)]
for rank, (n, mx, f) in enumerate(BRP_SIZES):
    t = tier(rank, 5, 8)
    if t == 0:
        combos = [(nt, t1) for nt in "ABCD" for t1 in (200, 1000)]
    elif t == 1:
        combos = [("A", 200), ("B", 200), ("C", 1000), ("D", 1000)]
    else:
        combos = [("A", 200), ("C", 1000)]
    for net, t1 in combos:
        name, L, U, Lp = NETS[net]
        td = U
        tr = 2 * mx * t1 + 3 * td
        for prop in (("p2", "p4") if rank < 6 else ("p2",)):
            label = f"BRP F={f} n={n} MAX={mx} TD={td} T1={t1} TR={tr} SYNC={tr} net={net} ({prop})"
            am = f"--n {n} --max {mx} --td {td} --t1 {t1} --tr {tr} --sync {tr} --k 2 --files {f} --property {prop} --keep-going"
            ab = f"--n {n} --max {mx} --td 1 --t1 200 --tr {2 * mx * 200 + 3} --sync {2 * mx * 200 + 3} --k 2 --files {f} --property {prop} --keep-going"
            add("brp", "brp_timed", (n, mx, f), rank, f"{net}/T1={t1}", f"brp|{net}|{t1}|{prop}", am, ab, ("brp", n, mx, f, prop), label)

# ------------------------------------------------------------------ NRP (VRRP: Adver 1 s, Master_Down = 3 x Adver; ping timeout = 2U)
for rank, r in enumerate([3, 4]):
    nets = "ABCD" if r <= 4 else "ACD"
    for variant in ("nrp", "noopt", "leasing"):
        for net in nets:
            name, L, U, Lp = NETS[net]
            H = VRRP_ADV if 2 * U < VRRP_ADV else 10 * VRRP_ADV   # the model needs P < H; 10 s is within RFC 5798's range
            label = f"NRP {variant} R={r} M=3 H={H} P={2 * U} net={net}[{L},{U}] sd=0"
            am = (f"--variant {variant} --rounds {r} --max-missed 3 --u {U} --l-ratio {ratio(L, U)} --sd-ratio 0 "
                  f"--h-ratio {ratio(H, U)} --pt-ratio 2 --keep-going")
            ab = f"--variant {variant} --rounds {r} --max-missed 3 --u 1 --l-ratio 0 --sd-ratio 0 --h-ratio 1000 --pt-ratio 2 --keep-going"
            add("nrp", "nrp_failure_detector_timed", (r,), rank, net, f"nrp|{variant}|{net}", am, ab, ("nrp", variant, r), label)

# ------------------------------------------------------------------ YARN (tick = NM heartbeat 1 s; U in ticks: 0 for A-C, 3 for D)
YARN_SIZES = [(1, 12), (2, 8), (2, 10), (2, 12), (3, 6), (3, 7), (2, 14), (3, 8), (4, 5)]
for rank, (k, h) in enumerate(YARN_SIZES):
    for u, netname in ((0, "A-C"), (3, "D")):
        for prop in (("miss", "preempt", "late-report") if rank < 6 else ("miss",)):
            dd = 7 if k == 1 else 8
            label = f"YARN K={k} QS={k + 1} DD={dd} U={u} H={h} {prop} net={netname}"
            propflag = f" --property {prop}" if prop != "miss" else ""
            am = f"--ams {k} --queue-size {k + 1} --deadline {dd} --u {u} --l 0 --sd 0 --horizon {h}{propflag} --keep-going"
            ab = f"--ams {k} --queue-size {k + 1} --deadline {dd} --u 0 --l 0 --sd 0 --horizon {h}{propflag} --keep-going"
            add("yarn", "yarn_scheduler_timed", (k, h), rank, netname, f"yarn|{netname}|{prop}", am, ab, ("yarn", k, h, dd, prop), label)

# ------------------------------------------------------------------ online shop (checkout deadline = API gateway 29 s)
SHOP_SIZES = [(2, 2, 1), (2, 2, 2), (3, 3, 1), (2, 2, 3), (3, 2, 2), (3, 3, 2), (2, 2, 4)]
for rank, (c, n, r) in enumerate(SHOP_SIZES):
    for net in ("ABCDE" if rank < 6 else "ACD"):
        name, L, U, Lp = NETS[net]
        label = f"Shop v2 C={c} N={n} R={r} S=20 D={HTTP_TIMEOUT} net={net}[{L},{U}] sd=0"
        am = (f"--variant v2 --order-services {n} --customers {c} --rounds {r} --stock 20 --u {U} --l-ratio {ratio(L, U)} "
              f"--sd-ratio 0 --deadline-ratio {ratio(HTTP_TIMEOUT, U)} --keep-going")
        ab = f"--variant v2 --order-services {n} --customers {c} --rounds {r} --stock 20 --u 1 --l-ratio 0 --sd-ratio 0 --deadline-ratio 29000 --keep-going"
        add("retail", "retail_saga_timed", (c, n, r), rank, net, f"retail|{net}", am, ab, ("retail", c, n, r), label)


# ================================================================== emit
def pred_m(rank, fam):
    base, gr = {"lease": (3, 1.9), "lease_f": (4, 1.9), "swim": (3, 1.7), "abp": (2, 1.9), "brp": (2, 1.9),
                "nrp": (30, 70), "yarn": (3, 2.0), "retail": (4, 2.2)}[fam]
    return base * (gr ** rank)


def pred_b(rank, fam):
    return pred_m(rank, fam) * 2.5


def reps_for(rank):
    return 3 if rank < 6 else (2 if rank < 12 else 1)


# machine assignment by group, balancing predicted cost
groups = collections.defaultdict(float)
for c in CELLS:
    groups[c["group"]] += pred_m(c["rank"], c["fam"])
load = {"brain05": 0.0, "brain10": 0.0}
gmach = {}
for gname, w in sorted(groups.items(), key=lambda kv: -kv[1]):
    m = min(load, key=load.get)
    gmach[gname] = m
    load[m] += w

rows = {"brain05": [], "brain10": []}
meta = []
n = 0
seen_b = set()
uid = collections.Counter()


def mkrow(m, fam, ex, label, side, args, pred, reps, rank, tag, cellid):
    t = tmpl[ex]
    out = []
    for rep in range(1, reps + 1):
        row = {k: t[k] for k in FIELDS}
        uid[fam] += 1
        row.update(id=f"v12-{fam}-{uid[fam]:05d}-{side[0]}-r{rep}", family=fam, example=ex, cell=label, regime=tag,
                   side=side, rep=str(rep), reps=str(reps), tier="timing", purpose="regime",
                   args=f"--mode {side} {args}", cores="1", parallel="none", pred_s=str(int(pred)), mem_gb="128",
                   order=str(int(pred)), expect="", expect_exit="", params=cellid)
        out.append(row)
    return out


for c in CELLS:
    m = gmach[c["group"]]
    n += 1
    reps = reps_for(c["rank"])
    cid = f"c{n:05d}"
    rows[m] += mkrow(m, c["fam"], c["ex"], c["label"], "timed", c["args_m"], pred_m(c["rank"], c["fam"]), reps, c["rank"], c["regime"], cid)
    bk = (m,) + tuple(c["bkey"])
    if bk not in seen_b:
        seen_b.add(bk)
        blabel = f"{c['fam']} baseline {'-'.join(map(str, c['bkey'][1:]))}"
        rows[m] += mkrow(m, c["fam"], c["ex"], blabel, "baseline", c["args_b"], pred_b(c["rank"], c["fam"]), reps, c["rank"], "baseline", "b|" + "|".join(map(str, bk)))
    meta.append(dict(cid=cid, machine=m, fam=c["fam"], label=c["label"], size=list(c["size"]), rank=c["rank"],
                     regime=c["regime"], bkey=list(bk), args=c["args_m"]))

for m, rs in rows.items():
    rs.sort(key=lambda r: (float(r["order"]), r["id"]))
    with open(OUT + f"plan_v12_{m}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rs)
    print(f"{m}: {len(rs)} jobs; predicted core-hours {sum(float(r['pred_s']) for r in rs) / 3600:.0f}")
json.dump(meta, open(OUT + "cells_v12.json", "w"))
cnt = collections.Counter(c["fam"] for c in CELLS)
print("cells per family:", dict(cnt), "total", len(CELLS), "baseline classes", len(seen_b))
