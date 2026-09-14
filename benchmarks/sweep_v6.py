#!/usr/bin/env python3
"""
v6 sweep (2026-09-06): re-run every benchmark under the exactly-k timed inbox
(2026-09-03) and the shared-pool fixes (2026-09-05), with NO time caps, and with
an untimed-versus-timed execution-count comparison for the lease and SWIM
protocols in addition to their boundary-formula checks.

Differences from sweep_v5.py:
  * no timeout by default (--timeout is optional and off);
  * timed and baseline are SEPARATE work items, each written as its own CSV
    row (column `side`) the moment it finishes, so a never-finishing side
    never delays the other side's row;
  * --workers W sets MUST_PARALLEL_WORKERS for every child, so J lanes x W
    workers can be sized to the machine (sweep_v5 let every child spawn
    num_cpus workers, which oversubscribed brain05 6x);
  * resumable: --append skips (protocol, N, rounds, cell, side) rows that are
    already in the output CSV;
  * per-run CPU time and peak memory via /usr/bin/time when available;
  * lease/swim tiers: the boundary grid (formula check, abort on first
    violation, as in v5) PLUS `cmp` cells run with --keep-going, where the
    untimed run explores its whole state space and reports how many executions
    violate the property (violations=), giving a like-for-like count
    comparison with the timed run;
  * larger configurations: 3PC up to 8 participants and 4 rounds, plain LE up
    to 5 nodes, inbox LE up to 7 nodes and 3 ballots, lease up to 5 rounds and
    4 clients, SWIM up to 5 members and 5 rounds. Cells are ordered cheap to
    expensive so results stream in; the largest cells may run for days.

Usage (on brain05, 256 cores):
  python3 sweep_v6.py --tier grid       --jobs 3 --workers 64 --out results_v6_grid.csv --append
  python3 sweep_v6.py --tier lease_swim --jobs 8 --workers 8  --out results_v6_lease_swim.csv --append
"""
import argparse, csv, os, re, resource, shutil, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TGT = os.environ.get("CARGO_TARGET_DIR", os.path.join(ROOT, "target"))
BIN  = os.path.join(_TGT, "release", "examples")
LOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs_v6")
os.makedirs(LOGS, exist_ok=True)
TIME_BIN = "/usr/bin/time" if os.path.exists("/usr/bin/time") else None

U = 20  # ticks for the grid protocols (ratio scale-invariance, README)

FIELDS = ["protocol", "N", "rounds", "cell", "side", "variant",
          "execs", "block", "extra", "violations", "verdict", "expected",
          "wall_s", "cpu_s", "rss_mb", "workers", "started", "finished", "cmd"]

# --------------------------------------------------------------------------- #
def run(cmd, timeout, mem_gb, workers):
    env = dict(os.environ)
    if workers:
        env["MUST_PARALLEL_WORKERS"] = str(workers)
    full = ([TIME_BIN, "-f", "TFRUSAGE user=%U sys=%S maxrss_kb=%M"] if TIME_BIN else []) + cmd
    def preexec():
        if mem_gb:
            b = int(mem_gb * 1024**3)
            try: resource.setrlimit(resource.RLIMIT_AS, (b, b))
            except (ValueError, OSError): pass
    t0 = time.time()
    try:
        cp = subprocess.run(full, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=timeout, preexec_fn=preexec, env=env)
        wall = time.time() - t0
        out = cp.stdout.decode("utf-8", "replace")
        if cp.returncode in (137, 134) or "memory allocation of" in out:
            return cp.returncode, out, wall, "oom"
        return cp.returncode, out, wall, "ok"
    except subprocess.TimeoutExpired as e:
        return None, (e.stdout or b"").decode("utf-8", "replace"), time.time() - t0, "timeout"

def parse_counts(out):
    ex = re.search(r"execs=(\d+)", out)
    bl = re.search(r"blocked=(\d+)", out)
    xt = re.search(r"(?:elected|commits?)=(\d+)", out)
    vi = re.search(r"violations=(\d+)", out)
    return (int(ex.group(1)) if ex else None,
            int(bl.group(1)) if bl else None,
            int(xt.group(1)) if xt else None,
            int(vi.group(1)) if vi else None)

def parse_rusage(out):
    m = re.search(r"TFRUSAGE user=([\d.]+) sys=([\d.]+) maxrss_kb=(\d+)", out)
    if not m:
        return "", ""
    return f"{float(m.group(1)) + float(m.group(2)):.1f}", f"{int(m.group(3)) / 1024:.0f}"

def verdict_of(ec, status, keep_going, violations):
    if status == "timeout": return "DNF"
    if status == "oom":     return "oom"
    if keep_going:
        if ec != 0: return f"err(ec={ec})"
        if violations is None: return "err(no-violations-line)"
        return "FIRE" if violations > 0 else "hold"
    if ec == 0:   return "hold"
    if ec == 101: return "FIRE"
    return f"err(ec={ec})"

# --------------------------------------------------------------------------- #
# cell builders                                                               #
# --------------------------------------------------------------------------- #
def mk(protocol, N, rounds, cell, cmd, expected_timed="hold", expected_base="hold",
       variant="", keep_going=False, cost=0):
    """One cell = two work items (timed, baseline). `cost` orders the queue."""
    return dict(protocol=protocol, N=N, rounds=rounds, cell=cell, variant=variant,
                keep_going=keep_going, cost=cost, cmd=cmd,
                expected={"timed": expected_timed, "baseline": expected_base})

def grid_cell(protocol, binname, N, ballots, l, sd, w, tag, cost):
    cmd = [os.path.join(BIN, binname), "--nodes", str(N), "--ballots", str(ballots),
           "--u", str(U), "--w", str(w), "--l", str(l), "--sd", str(sd),
           "--parallel", "shared"]
    return mk(protocol, N, ballots, tag, cmd, cost=cost)

def pc3_cell(N, rounds, wr, lr, sr, tag, cost):
    cmd = [os.path.join(BIN, "three_pc_timed"), "--participants", str(N),
           "--rounds", str(rounds), "--u", str(U), "--w-ratio", str(wr),
           "--l-ratio", str(lr), "--sd-ratio", str(sr), "--parallel", "shared"]
    return mk("three_pc_timed", N, rounds, tag, cmd, cost=cost)

# regimes used since results_v4 (L, sd, W at U=20)
REGIME = {"A": (18, 2, 200), "B": (6, 0, 60), "C": (2, 5, 100), "F": (0, 10, 40)}

def grid_cells():
    """3PC, plain LE, inbox LE. cost = rough log10 of the expected untimed
    execution count (product law for the inbox, measured rows otherwise), so
    that the queue runs cheap cells first and the multi-day attempts last."""
    cells = []
    # ---- three_pc: every v4/v5 cell plus N=7, N=8, and more rounds
    for N, base in ((3, 3), (4, 5), (5, 6), (6, 8)):
        for wr in (2, 3, 5, 7, 10):
            cells.append(pc3_cell(N, 1, wr, 0.0, 0.25, f"W/U={wr}", base))
    cells.append(pc3_cell(5, 1, 7, 0.25, 0.25, "L/U=0.25", 6))
    cells.append(pc3_cell(5, 1, 7, 0.0, 0.5, "sd/U=0.5", 6))
    for wr in (2, 7, 10):
        cells.append(pc3_cell(7, 1, wr, 0.0, 0.25, f"W/U={wr}", 10))
    cells.append(pc3_cell(8, 1, 7, 0.0, 0.25, "W/U=7", 12))
    cells.append(pc3_cell(3, 2, 7, 0.0, 0.25, "R=2", 7))
    cells.append(pc3_cell(4, 2, 7, 0.0, 0.25, "R=2", 9))
    cells.append(pc3_cell(5, 2, 7, 0.0, 0.25, "R=2", 11))
    cells.append(pc3_cell(3, 3, 7, 0.0, 0.25, "R=3", 9))
    cells.append(pc3_cell(4, 3, 7, 0.0, 0.25, "R=3", 12))
    cells.append(pc3_cell(3, 4, 7, 0.0, 0.25, "R=4", 11))
    # ---- plain-receive leader election
    P = ("comm_closed_leader_election", "comm_closed_leader_election")
    for reg, (l, sd, w) in REGIME.items():
        cells.append(grid_cell(*P, 3, 1, l, sd, w, f"regime {reg}", 4))
    for reg in ("A", "C"):
        cells.append(grid_cell(*P, 3, 2, *REGIME[reg], f"B=2 regime {reg}", 8))
    for reg in ("A", "F"):
        cells.append(grid_cell(*P, 4, 1, *REGIME[reg], f"regime {reg}", 8))
    cells.append(grid_cell(*P, 4, 2, *REGIME["A"], "B=2 regime A", 13))
    cells.append(grid_cell(*P, 5, 1, *REGIME["A"], "regime A", 12))
    cells.append(grid_cell(*P, 5, 1, *REGIME["F"], "regime F", 12))
    # ---- inbox leader election (exactly-k timed inbox)
    I = ("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox")
    for reg, (l, sd, w) in REGIME.items():
        cells.append(grid_cell(*I, 3, 1, l, sd, w, f"regime {reg}", 3))
    for reg in ("A", "C"):
        cells.append(grid_cell(*I, 3, 2, *REGIME[reg], f"B=2 regime {reg}", 6))
    cells.append(grid_cell(*I, 3, 3, *REGIME["A"], "B=3 regime A", 9))
    for reg, (l, sd, w) in REGIME.items():
        cells.append(grid_cell(*I, 4, 1, l, sd, w, f"regime {reg}", 5))
    for reg in ("A", "C"):
        cells.append(grid_cell(*I, 4, 2, *REGIME[reg], f"B=2 regime {reg}", 9))
    for reg, (l, sd, w) in REGIME.items():
        cells.append(grid_cell(*I, 5, 1, l, sd, w, f"regime {reg}", 8))
    cells.append(grid_cell(*I, 5, 2, *REGIME["A"], "B=2 regime A", 15))
    for reg in ("A", "F"):
        cells.append(grid_cell(*I, 6, 1, *REGIME[reg], f"regime {reg}", 10))
    cells.append(grid_cell(*I, 7, 1, *REGIME["A"], "regime A", 13))
    return cells

# --------------------------------------------------------------------------- #
# lease / swim: boundary grids (formula check) and cmp cells (count comparison)
# --------------------------------------------------------------------------- #
def lease_cmd(R, u, l, sd, ttl, pause, fencing, clients=2, parallel=None):
    """Boundary cells run single-threaded (exact abort semantics); cmp cells
    (--keep-going, never abort) use the shared pool."""
    return [os.path.join(BIN, "lease_timed"), "--clients", str(clients),
            "--rounds", str(R), "--u", str(u), "--l-ratio", str(l / u),
            "--sd-ratio", str(sd / u), "--ttl-ratio", str(ttl / u),
            "--pause-ratio", str(pause / u), "--fencing", fencing] \
           + (["--parallel", parallel] if parallel else [])

def swim_cmd(R, u, l, sd, wp, ws, nodes=3, parallel=None):
    return [os.path.join(BIN, "swim_timed"), "--nodes", str(nodes), "--rounds", str(R),
            "--u", str(u), "--l-ratio", str(l / u), "--sd-ratio", str(sd / u),
            "--w-probe-ratio", str(wp / u), "--w-suspect-ratio", str(ws / u)] \
           + (["--parallel", parallel] if parallel else [])

def lease_boundary_cells():
    """FIRE iff pause >= TTL - 2*(U - L + sd); boundary (FIRE) and boundary-1
    (hold) unfenced, boundary fenced (hold). Abort on first violation, as v5,
    now at R in {1,2,3}. Baseline always FIREs (documented)."""
    cells = []
    for R in (1, 2, 3):
        for u in (1, 2):
            for l in (0, u):
                for sd in (0, 1):
                    for ttl in (6, 8, 10, 12):
                        bnd = ttl - 2 * (u - l + sd)
                        for pause, fencing, expect in ((bnd, "off", "FIRE"),
                                                       (bnd - 1, "off", "hold"),
                                                       (bnd, "on", "hold")):
                            if pause < 0:
                                continue
                            tag = f"U={u} L={l} sd={sd} TTL={ttl} pause={pause} fence={fencing}"
                            base_exp = "hold" if fencing == "on" else "FIRE"
                            cells.append(mk("lease_timed", 2, R, tag,
                                            lease_cmd(R, u, l, sd, ttl, pause, fencing),
                                            expect, base_exp, variant="boundary", cost=R))
    return cells

def swim_boundary_cells():
    """FIRE iff Ws <= 2U + 2sd - min(L, max(0, 2L - Wp)); boundary (FIRE) and
    boundary+1 (hold). Baseline always FIREs."""
    cells = []
    combos = [(R, u, l, sd, u) for R in (1, 2, 3) for u in (1, 2) for l in (0, u) for sd in (0, 1)]
    combos += [(1, 4, 3, 0, 4), (1, 3, 2, 0, 3)]
    for (R, u, l, sd, wp) in combos:
        bnd = 2 * u + 2 * sd - min(l, max(0, 2 * l - wp))
        for ws, expect in ((bnd, "FIRE"), (bnd + 1, "hold")):
            if ws <= 0:
                continue
            tag = f"U={u} L={l} sd={sd} Wp={wp} Ws={ws}"
            cells.append(mk("swim_timed", 3, R, tag, swim_cmd(R, u, l, sd, wp, ws),
                            expect, "FIRE", variant="boundary", cost=R))
    return cells

def lease_cmp_cells():
    """Untimed vs timed COUNT comparison with --keep-going: the untimed run
    explores everything and reports violations=; the timed run holds. Cells:
    unfenced at pause = boundary-1 (timed proves safety, untimed cannot) and
    fenced at pause = boundary (both hold), over R up to 5 and up to 4 clients."""
    cells = []
    grid = [(u, l, sd, ttl) for u in (1, 2) for l in (0, u) for sd in (0, 1) for ttl in (6, 10)]
    for R in (1, 2, 3, 4, 5):
        for clients in ((2, 3, 4) if R <= 2 else (2,)):
            for (u, l, sd, ttl) in grid:
                bnd = ttl - 2 * (u - l + sd)
                if bnd - 1 < 0:
                    continue
                for pause, fencing, t_exp, b_exp in ((bnd - 1, "off", "hold", "FIRE"),
                                                     (bnd, "on", "hold", "hold")):
                    tag = f"C={clients} U={u} L={l} sd={sd} TTL={ttl} pause={pause} fence={fencing}"
                    cells.append(mk("lease_timed", clients, R, tag,
                                    lease_cmd(R, u, l, sd, ttl, pause, fencing, clients, "shared") + ["--keep-going"],
                                    t_exp, b_exp, variant="cmp", keep_going=True,
                                    cost=R * 2 + clients))
    return cells

def swim_cmp_cells():
    """Untimed vs timed COUNT comparison with --keep-going at Ws = boundary+1
    (timed holds, untimed reports every false positive), N up to 5, R up to 5."""
    cells = []
    grid = [(u, l, sd) for u in (1, 2) for l in (0, u) for sd in (0, 1)]
    for R in (1, 2, 3, 4, 5):
        for nodes in ((3, 4, 5) if R <= 3 else (3,)):
            for (u, l, sd) in grid:
                wp = u
                ws = 2 * u + 2 * sd - min(l, max(0, 2 * l - wp)) + 1
                tag = f"N={nodes} U={u} L={l} sd={sd} Wp={wp} Ws={ws}"
                cells.append(mk("swim_timed", nodes, R, tag,
                                swim_cmd(R, u, l, sd, wp, ws, nodes, "shared") + ["--keep-going"],
                                "hold", "FIRE", variant="cmp", keep_going=True,
                                cost=R * 2 + nodes))
    return cells

# --------------------------------------------------------------------------- #
def work_items(cells):
    items = []
    for c in cells:
        for side in ("timed", "baseline"):
            items.append((c, side))
    items.sort(key=lambda cs: (cs[0]["cost"], cs[0]["protocol"], cs[0]["N"], cs[0]["rounds"], cs[1]))
    return items

def key_of(protocol, N, rounds, cell, side):
    return (str(protocol), str(N), str(rounds), str(cell), str(side))

def do_item(cell, side, timeout, mem_gb, workers):
    cmd = cell["cmd"] + ["--mode", side]
    row = {"protocol": cell["protocol"], "N": cell["N"], "rounds": cell["rounds"],
           "cell": cell["cell"], "side": side, "variant": cell["variant"],
           "expected": cell["expected"][side], "workers": workers or "",
           "cmd": " ".join(cmd).replace(BIN + os.sep, "")}
    row["started"] = time.strftime("%Y-%m-%d %H:%M:%S")
    ec, out, wall, status = run(cmd, timeout, mem_gb, workers)
    row["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
    ex, bl, xt, vi = parse_counts(out)
    row["execs"] = ex if ex is not None else ""
    row["block"] = bl if bl is not None else ""
    row["extra"] = xt if xt is not None else ""
    row["violations"] = vi if vi is not None else ""
    row["verdict"] = verdict_of(ec, status, cell["keep_going"], vi)
    row["wall_s"] = f"{wall:.1f}"
    row["cpu_s"], row["rss_mb"] = parse_rusage(out)
    tag = re.sub(r"[^A-Za-z0-9._=-]+", "_",
                 f'{row["protocol"]}_N{row["N"]}_R{row["rounds"]}_{row["cell"]}_{side}')
    if row["verdict"] != row["expected"] or ex is None:
        with open(os.path.join(LOGS, tag + ".log"), "w") as f:
            f.write(out[-300_000:])
    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", required=True, choices=["grid", "lease_swim", "smoke"])
    ap.add_argument("--out", default=None)
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--workers", type=int, default=None,
                    help="MUST_PARALLEL_WORKERS per child (shared pool); default = all cores")
    ap.add_argument("--timeout", type=int, default=None, help="seconds per invocation; default none")
    ap.add_argument("--mem-gb", type=float, default=None)
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--only", default=None, help="regex on 'protocol N rounds cell' to select cells")
    args = ap.parse_args()

    if args.tier == "grid":
        cells = grid_cells()
    elif args.tier == "lease_swim":
        cells = lease_boundary_cells() + swim_boundary_cells() + lease_cmp_cells() + swim_cmp_cells()
    else:  # smoke: a handful of tiny cells from every family
        cells = [c for c in grid_cells() if c["cost"] <= 3] \
              + [c for c in lease_boundary_cells() if c["rounds"] == 1][:2] \
              + [c for c in swim_boundary_cells() if c["rounds"] == 1][:2] \
              + [c for c in lease_cmp_cells() if c["rounds"] == 1][:2] \
              + [c for c in swim_cmp_cells() if c["rounds"] == 1][:2]
    if args.only:
        rx = re.compile(args.only)
        cells = [c for c in cells if rx.search(f'{c["protocol"]} {c["N"]} {c["rounds"]} {c["cell"]}')]
    out = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), f"results_v6_{args.tier}.csv")

    done_keys = set()
    if args.append and os.path.exists(out):
        with open(out, newline="") as f:
            for r in csv.DictReader(f):
                done_keys.add(key_of(r["protocol"], r["N"], r["rounds"], r["cell"], r["side"]))
    items = [(c, s) for (c, s) in work_items(cells)
             if key_of(c["protocol"], c["N"], c["rounds"], c["cell"], s) not in done_keys]
    print(f"[v6:{args.tier}] {len(cells)} cells, {len(items)} work items to run "
          f"({len(done_keys)} rows already present), jobs={args.jobs}, workers={args.workers}, "
          f"timeout={args.timeout}", flush=True)

    write_header = not (args.append and os.path.exists(out) and os.path.getsize(out) > 0)
    f = open(out, "a" if args.append else "w", newline="")
    w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
    if write_header:
        w.writeheader()
    f.flush()

    done = 0
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = [ex.submit(do_item, c, s, args.timeout, args.mem_gb, args.workers) for (c, s) in items]
        for fut in as_completed(futs):
            row = fut.result()
            w.writerow(row); f.flush()
            done += 1
            mark = "" if row["verdict"] == row["expected"] else "  <-- UNEXPECTED"
            print(f"[{done}/{len(items)}] {row['protocol']:>34} N{row['N']} R{row['rounds']} "
                  f"{row['cell']:<40} {row['side']:>8}={row['verdict']:>5}/{row['execs']} "
                  f"viol={row['violations']} wall={row['wall_s']}s cpu={row['cpu_s']}s{mark}", flush=True)
    f.close()
    print(f"[v6:{args.tier}] wrote {done} rows -> {out}", flush=True)

if __name__ == "__main__":
    main()
