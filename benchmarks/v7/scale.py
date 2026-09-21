#!/usr/bin/env python3
"""Worker-scaling / growth probe: python3 scale.py 'proto:N:R:mode:workers:timeout' ... (sequential)."""
import os, re, subprocess, sys, time
BIN = "/local/sramezani/target-v7/release/examples"
u, l, sd, ttl = 2, 0, 1, 10
def cmd(proto, N, R, mode):
    if proto == "lease":
        bnd = ttl - 2*(u-l+sd)
        return [f"{BIN}/lease_timed", "--clients", str(N), "--rounds", str(R), "--u", str(u), "--l-ratio", str(l/u),
                "--sd-ratio", str(sd/u), "--ttl-ratio", str(ttl/u), "--pause-ratio", str((bnd-1)/u), "--fencing", "off",
                "--parallel", "shared", "--keep-going", "--mode", mode]
    wp = u; ws = 2*u + 2*sd - min(l, max(0, 2*l - wp)) + 1
    return [f"{BIN}/swim_timed", "--nodes", str(N), "--rounds", str(R), "--u", str(u), "--l-ratio", str(l/u),
            "--sd-ratio", str(sd/u), "--w-probe-ratio", str(wp/u), "--w-suspect-ratio", str(ws/u),
            "--parallel", "shared", "--keep-going", "--mode", mode]
for spec in sys.argv[1:]:
    proto, N, R, mode, workers, tmo = spec.split(":")
    env = dict(os.environ, MUST_PARALLEL_WORKERS=workers)
    t0 = time.time()
    try:
        cp = subprocess.run(["/usr/bin/time", "-f", "TFRUSAGE user=%U sys=%S maxrss_kb=%M"] + cmd(proto, int(N), int(R), mode),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=int(tmo), env=env)
        out = cp.stdout.decode("utf-8", "replace"); st = "ok"
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode("utf-8", "replace"); st = "TIMEOUT"
    wall = time.time() - t0
    g = lambda p: (re.search(p, out).group(1) if re.search(p, out) else "-")
    ru = re.search(r"user=([\d.]+) sys=([\d.]+) maxrss_kb=(\d+)", out)
    cpu = f"{float(ru.group(1))+float(ru.group(2)):.0f}" if ru else "-"
    rss = f"{int(ru.group(3))/1024:.0f}MB" if ru else "-"
    print(f"{spec:34s} {st:7s} execs={g(r'execs=(\d+)'):>11} blocked={g(r'blocked=(\d+)'):>11} viol={g(r'violations=(\d+)'):>10} wall={wall:8.1f}s cpu={cpu}s rss={rss}", flush=True)
