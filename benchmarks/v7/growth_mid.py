#!/usr/bin/env python3
"""Growth-law probe on brain05 (v7 binaries): many single-core SWIM runs at once.
usage: growth.py OUT.csv   (runs the fixed job list below; per-job timeout GROWTH_TIMEOUT s)"""
import csv, os, re, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
BIN = "/local/sramezani/target-v7/release/examples"
TMO = int(os.environ.get("GROWTH_TIMEOUT", "900"))
JOBS = int(os.environ.get("GROWTH_JOBS", "64"))
u = int(os.environ.get('G_U','2')); l = int(os.environ.get('G_L','0')); sd = int(os.environ.get('G_SD','1'))

def swim(N, R, mode, par):
    wp = u; ws = 2*u + 2*sd - min(l, max(0, 2*l - wp)) + 1
    return [f"{BIN}/swim_timed", "--nodes", str(N), "--rounds", str(R), "--u", str(u), "--l-ratio", str(l/u),
            "--sd-ratio", str(sd/u), "--w-probe-ratio", str(wp/u), "--w-suspect-ratio", str(ws/u),
            "--keep-going", "--mode", mode] + (["--parallel", par] if par != "none" else [])

def lease(C, R, mode, par):
    bnd = 10 - 2*(u-l+sd)  # TTL=10 ticks
    return [f"{BIN}/lease_timed", "--clients", str(C), "--rounds", str(R), "--u", str(u), "--l-ratio", str(l/u),
            "--sd-ratio", str(sd/u), "--ttl-ratio", str(10/u), "--pause-ratio", str((bnd-1)/u), "--fencing", "off",
            "--keep-going", "--mode", mode] + (["--parallel", par] if par != "none" else [])

def run(spec):
    tag, cmd = spec
    t0 = time.time()
    try:
        cp = subprocess.run(["/usr/bin/time", "-f", "TFRUSAGE user=%U sys=%S maxrss_kb=%M"] + cmd,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=TMO)
        out = cp.stdout.decode("utf-8", "replace"); st = "ok"
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode("utf-8", "replace"); st = "TIMEOUT"
    g = lambda p: (re.search(p, out).group(1) if re.search(p, out) else "")
    ru = re.search(r"user=([\d.]+) sys=([\d.]+) maxrss_kb=(\d+)", out)
    return dict(tag=tag, status=st, execs=g(r"execs=(\d+)"), blocked=g(r"blocked=(\d+)"), viol=g(r"violations=(\d+)"),
                wall=f"{time.time()-t0:.1f}", cpu=f"{float(ru.group(1))+float(ru.group(2)):.1f}" if ru else "",
                rss_mb=f"{int(ru.group(3))/1024:.0f}" if ru else "")

specs = []
for N in (6, 8, 10, 12, 14, 16, 18):
    specs.append((f"swim timed N{N} R1", swim(N, 1, "timed", "none")))
for N in (4, 6, 8):
    for R in (2, 3, 4, 6):
        specs.append((f"swim timed N{N} R{R}", swim(N, R, "timed", "none")))
for N in (3, 5):
    for R in (8, 10, 12):
        specs.append((f"swim timed N{N} R{R}", swim(N, R, "timed", "none")))
for N in (4, 6):
    for R in (1, 2, 3, 4):
        specs.append((f"swim baseline N{N} R{R}", swim(N, R, "baseline", "none")))
for C in (2, 3, 4, 5):
    for R in (1, 2, 3):
        specs.append((f"lease timed C{C} R{R}", lease(C, R, "timed", "none")))
        specs.append((f"lease fenced-skip C{C} R{R}", lease(C, R, "baseline", "none")))
for R in (4, 5, 6, 8):
    specs.append((f"lease timed C2 R{R}", lease(2, R, "timed", "none")))
    specs.append((f"lease timed C3 R{R}", lease(3, R, "timed", "none")))
with open(sys.argv[1], "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["tag", "status", "execs", "blocked", "viol", "wall", "cpu", "rss_mb"])
    w.writeheader()
    with ThreadPoolExecutor(max_workers=JOBS) as ex:
        for row in ex.map(run, specs):
            w.writerow(row); f.flush()
            print(f"{row['tag']:32s} {row['status']:7s} execs={row['execs']:>10} blocked={row['blocked']:>11} "
                  f"viol={row['viol']:>10} wall={row['wall']:>7}s cpu={row['cpu']}s", flush=True)
print("GROWTH-DONE", flush=True)
