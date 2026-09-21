#!/usr/bin/env python3
"""Quick scaling probe for lease/swim (v7 binaries). Prints one line per run."""
import os, re, subprocess, sys, time, itertools
from concurrent.futures import ThreadPoolExecutor
BIN = "/local/sramezani/target-v7/release/examples"
W = os.environ.get("PROBE_WORKERS", "8")
TMO = int(os.environ.get("PROBE_TIMEOUT", "240"))

def lease(C, R, u, l, sd, ttl, pause, fence, mode):
    return [f"{BIN}/lease_timed", "--clients", str(C), "--rounds", str(R), "--u", str(u),
            "--l-ratio", str(l/u), "--sd-ratio", str(sd/u), "--ttl-ratio", str(ttl/u),
            "--pause-ratio", str(pause/u), "--fencing", fence, "--parallel", "shared",
            "--keep-going", "--mode", mode]

def swim(N, R, u, l, sd, wp, ws, mode):
    return [f"{BIN}/swim_timed", "--nodes", str(N), "--rounds", str(R), "--u", str(u),
            "--l-ratio", str(l/u), "--sd-ratio", str(sd/u), "--w-probe-ratio", str(wp/u),
            "--w-suspect-ratio", str(ws/u), "--parallel", "shared", "--keep-going", "--mode", mode]

def run(tag, cmd):
    env = dict(os.environ, MUST_PARALLEL_WORKERS=W)
    t0 = time.time()
    try:
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=TMO, env=env)
        out = cp.stdout.decode("utf-8", "replace"); st = "ok"
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode("utf-8", "replace"); st = "TIMEOUT"
    wall = time.time() - t0
    ex = re.search(r"execs=(\d+)", out); bl = re.search(r"blocked=(\d+)", out); vi = re.search(r"violations=(\d+)", out)
    return f"{tag:70s} {st:7s} execs={ex.group(1) if ex else '-':>10} blocked={bl.group(1) if bl else '-':>10} viol={vi.group(1) if vi else '-':>6} wall={wall:8.1f}s"

jobs = []
which = sys.argv[1]
if which == "params":       # which parameter set is hardest? (lease C=2 R=3, swim N=4 R=3)
    for (u, l, sd, ttl) in itertools.product((1, 2), (0, 'u'), (0, 1), (6, 10)):
        l = u if l == 'u' else l
        bnd = ttl - 2*(u-l+sd)
        if bnd - 1 < 0: continue
        for mode in ("timed", "baseline"):
            jobs.append((f"lease C2 R3 u{u} l{l} sd{sd} ttl{ttl} unfenced p{bnd-1} {mode}", lease(2, 3, u, l, sd, ttl, bnd-1, "off", mode)))
    for (u, l, sd) in itertools.product((1, 2), (0, 'u'), (0, 1)):
        l = u if l == 'u' else l
        wp = u; ws = 2*u + 2*sd - min(l, max(0, 2*l - wp)) + 1
        for mode in ("timed", "baseline"):
            jobs.append((f"swim N4 R3 u{u} l{l} sd{sd} wp{wp} ws{ws} {mode}", swim(4, 3, u, l, sd, wp, ws, mode)))
elif which == "swim":       # swim ladder at fixed params
    u, l, sd = 2, 0, 1
    wp = u; ws = 2*u + 2*sd - min(l, max(0, 2*l - wp)) + 1
    for N in (3, 5, 8, 12):
        for R in (1, 2, 3, 5, 8):
            for mode in ("timed", "baseline"):
                jobs.append((f"swim N{N} R{R} u{u} l{l} sd{sd} wp{wp} ws{ws} {mode}", swim(N, R, u, l, sd, wp, ws, mode)))
elif which == "lease":
    u, l, sd, ttl = 2, 0, 1, 10
    bnd = ttl - 2*(u-l+sd)
    for C in (2, 3, 4, 5):
        for R in (1, 2, 3):
            for mode in ("timed", "baseline"):
                jobs.append((f"lease C{C} R{R} u{u} l{l} sd{sd} ttl{ttl} unfenced p{bnd-1} {mode}", lease(C, R, u, l, sd, ttl, bnd-1, "off", mode)))
with ThreadPoolExecutor(max_workers=int(os.environ.get("PROBE_JOBS", "8"))) as ex:
    for line in ex.map(lambda j: run(*j), jobs):
        print(line, flush=True)
