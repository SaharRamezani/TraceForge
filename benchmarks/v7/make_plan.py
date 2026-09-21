#!/usr/bin/env python3
"""Build plan_v7.csv (which lease/SWIM cells to run, on how many cores) from measured probe data.

Cost models (fitted to benchmarks/v7/probes/*, all measured on brain05 2026-09-20):

  lease, unfenced pause=bnd-1 (untimed FIRES, timed holds):
      execs = (C! * 2^C)^R, exact for both sides (checked C=2..6)
      blocked/execs b_timed(C)   = 0.9, 1.5, 2.56, 4.1, 6.6, ...  (x1.6 per client)
      blocked/execs b_untimed(C) = 0.57, 1.8, 4.3, 8.9, 17.1, ... (x2 per client)
      CPU per explored (execs+blocked): timed ~1.4 ms, untimed ~0.16 ms (single core)
  lease, fenced pause=bnd (both hold):
      untimed execs = cnt(C)^R with cnt = 12, 144, 2880, ... (no blocked), 0.155 ms/exec
      timed   per round total ~ 17, 128?, ~2000 ... (measured C2..C4 below), ~0.8-1.4 ms
  swim, Ws = boundary+1 (untimed FIRES, timed holds): log-linear fits below.
"""
import csv, math, os, re, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PROBES = os.path.join(HERE, "probes")
WINDOW_H = float(os.environ.get("WINDOW_H", "44"))
BEYOND = float(os.environ.get("BEYOND", "1.5"))       # include cells up to BEYOND x window (DNF evidence)

# ---------------------------------------------------------------------------- SWIM fits
def load_swim():
    d = {"timed0": {}, "timed1": {}, "base": {}}
    def add(fn, timed_key):
        for r in csv.DictReader(open(os.path.join(PROBES, fn))):
            m = re.match(r"swim (timed|baseline) N(\d+) R(\d+)", r["tag"])
            if not m or r["status"] != "ok":
                continue
            side, N, R = m.group(1), int(m.group(2)), int(m.group(3))
            k = "base" if side == "baseline" else timed_key
            d[k][(N, R)] = float(r["cpu"])
    add("growth.csv", "timed0"); add("growth_mid.csv", "timed1")
    return d

def fit(points, min_t=0.3):
    """log2(t) = a + b N + c R + d N R (least squares) on cells with t >= min_t (monotone, no R^2 term)."""
    pts = [(N, R, t) for (N, R), t in points.items() if t >= min_t]
    A = np.array([[1, N, R, N * R] for N, R, t in pts], float)
    y = np.log2([t for _, _, t in pts])
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    res = A @ coef - y
    return coef, float(np.max(np.abs(res)))

SW = load_swim()
FIT = {k: fit(v) for k, v in SW.items()}
for k, (c, r) in FIT.items():
    print(f"[fit] swim {k}: max |log2 residual| = {r:.2f} over {len(SW[k])} points", file=sys.stderr)

def swim_seq_s(kind, N, R):
    c, _ = FIT[kind]
    return 2 ** (c[0] + c[1] * N + c[2] * R + c[3] * N * R)

def swim_cpus_speedup(kind, R):
    """(cpus, speedup): R=1/2 barely parallelises (2..4 complete executions); R>=3 gets ~7x on 16 cores."""
    return (8, 4.0) if R >= 3 else (1, 1.0)

# ---------------------------------------------------------------------------- lease model
FACT = [1, 1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800, 39916800]
B_T = {2: 0.91, 3: 1.5, 4: 2.56, 5: 4.1, 6: 6.6}
B_U = {2: 0.57, 3: 1.8, 4: 4.3, 5: 8.9, 6: 17.1}
def b_t(C):
    return B_T.get(C, B_T[6] * 1.6 ** (C - 6))
def b_u(C):
    return B_U.get(C, B_U[6] * 2.0 ** (C - 6))
FENCED_U_CNT = {2: 12, 3: 144, 4: 2880}            # measured; C>=5 extrapolated x20/client
def fenced_cnt(C):
    return FENCED_U_CNT.get(C, 2880 * 20.0 ** (C - 4))
FENCED_T_TOT = {2: 17.2, 3: 130, 4: 990}            # per-round explored (execs+blocked), measured; x8 per client beyond
def fenced_t_tot(C):
    return FENCED_T_TOT.get(C, 990 * 8.0 ** (C - 4))

LEASE_SPEEDUP = {1: 1.0, 16: 10.0, 32: 16.0, 64: 24.0}

def lease_total(C, R, side, fence):
    base = (FACT[C] * 2 ** C) ** R
    if fence == "off":
        return base * (1 + (b_t(C) if side == "timed" else b_u(C)))
    return fenced_t_tot(C) ** R if side == "timed" else fenced_cnt(C) ** R

def lease_seq_s(C, R, side, fence):
    unit = {("timed", "off"): 1.4e-3, ("baseline", "off"): 0.16e-3,
            ("timed", "on"): 1.0e-3, ("baseline", "on"): 0.155e-3}[(side, fence)]
    return lease_total(C, R, side, fence) * unit

def lease_cpus(seq_s):
    """smallest core count whose predicted wall fits the window (else 64)."""
    for c in (1, 16, 32, 64):
        if seq_s / LEASE_SPEEDUP[c] <= WINDOW_H * 3600 * 0.5:
            return c
    return 64

# ---------------------------------------------------------------------------- plan
rows = []
def add(proto, family, N, R, side, u, l, sd, ttl, fence, cpus, par, pred_s):
    rows.append(dict(proto=proto, family=family, N=N, R=R, side=side, u=u, l=l, sd=sd, ttl=ttl, fence=fence,
                     cpus=cpus, par=par, pred_s=round(pred_s), prio=round(pred_s * cpus)))

LIM = WINDOW_H * 3600 * BEYOND

# SWIM: timed at L=0 (family swim_L0) and L=1 (swim_L1), untimed once (regime independent)
SWIM_N = [3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26]
SWIM_R = {"timed0": [1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32],
          "base":   [1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32],
          "timed1": [1, 2, 4, 8, 16, 32]}
for kind, fam, l in (("timed0", "swim_L0", 0), ("timed1", "swim_L1", 1), ("base", "swim_untimed", 0)):
    for N in SWIM_N:
        for R in SWIM_R[kind]:
            seq = swim_seq_s(kind, N, R)
            cpus, sp = swim_cpus_speedup(kind, R)
            wall = seq / sp
            # single-core cells are cheap: allow them to run beyond the window (they show up as DNF);
            # multi-core cells only if they are predicted to fit
            if wall > (LIM * 4 / 3 if cpus == 1 else WINDOW_H * 3600 * 0.9):
                continue
            side = "baseline" if kind == "base" else "timed"
            add("swim", fam, N, R, side, 2, l, 1, 0, "", cpus, "none" if cpus == 1 else "partitioned", wall)

# LEASE: P0 (u=2, l=0, sd=1, ttl=10), unfenced (pause = bnd-1) and fenced (pause = bnd)
for fence, fam in (("off", "lease_unfenced"), ("on", "lease_fenced")):
    for C in range(2, 11):
        for R in range(1, 13):
            for side in ("timed", "baseline"):
                seq = lease_seq_s(C, R, side, fence)
                cpus = lease_cpus(seq)
                wall = seq / LEASE_SPEEDUP[cpus]
                if wall > WINDOW_H * 3600 * 0.85:
                    continue
                if fence == "on" and wall > 8 * 3600:   # fenced is the control: keep it to what surely fits
                    continue
                add("lease", fam, C, R, side, 2, 0, 1, 10, fence, cpus, "none" if cpus == 1 else "partitioned", wall)

# make sure both sides of every (family, N, R) exist so a pair can be compared (drop orphans that would only show one side)
from collections import defaultdict
pairs = defaultdict(set)
for r in rows:
    fam = "swim_untimed" if r["family"] == "swim_untimed" else r["family"]
    pairs[(r["proto"], r["N"], r["R"], "swimU" if fam == "swim_untimed" else fam)].add(r["side"])

out = os.path.join(HERE, "plan_v7.csv")
FIELDS = ["proto", "family", "N", "R", "side", "u", "l", "sd", "ttl", "fence", "cpus", "par", "pred_s", "prio"]
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader()
    for r in sorted(rows, key=lambda r: -r["prio"]):
        w.writerow(r)

tot_core_h = sum(r["pred_s"] * r["cpus"] for r in rows) / 3600
print(f"plan: {len(rows)} jobs, predicted {tot_core_h:,.0f} core-hours (window {WINDOW_H} h x 252 cores = {WINDOW_H*252:,.0f})")
long_ = [r for r in rows if r["pred_s"] > 3600]
print(f"jobs predicted > 1 h: {len(long_)}; cores they need concurrently: {sum(r['cpus'] for r in long_)}")
for proto in ("lease", "swim"):
    print(f"--- {proto}: jobs predicted > 2 h")
    for r in sorted([r for r in rows if r["proto"] == proto and r["pred_s"] > 7200], key=lambda r: (r["family"], r["side"], r["N"], r["R"])):
        print(f"  {r['family']:15s} {r['side']:8s} N={r['N']:>2} R={r['R']:>2} cpus={r['cpus']:>2} pred={r['pred_s']/3600:7.1f} h")
