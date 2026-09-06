#!/usr/bin/env python3
"""
v5 sweep: redo every results_v4.csv cell on one machine, plus the lease and
SWIM boundary grids from P1_FEEDBACK_LEASE_SWIM.md.

Differences from sweep.py:
  * timed and baseline are SEPARATE invocations, each with its own timeout,
    so a DNF on one side never eats the other side's budget;
  * lease/swim cells carry an `expected` verdict computed from the published
    boundary inequalities, so any deviation is visible in the CSV;
  * tiers (--tier small|big|lease_swim) so heavy cells can run in their own
    tmux session with more threads and fewer parallel jobs.

Usage (on brain05):
  python3 sweep_v5.py --tier small      --timeout 172800 --jobs 24 --out results_v5.csv --append
  python3 sweep_v5.py --tier big        --timeout 172800 --jobs 4  --out results_v5.csv --append
  python3 sweep_v5.py --tier lease_swim --timeout 172800 --jobs 24 --out results_v5.csv --append
"""
import argparse, csv, os, re, resource, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TGT = os.environ.get("CARGO_TARGET_DIR", os.path.join(ROOT, "target"))
BIN  = os.path.join(_TGT, "release", "examples")
LOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs_v5")
os.makedirs(LOGS, exist_ok=True)

U = 20  # ticks for the grid protocols (ratio scale-invariance, README)

FIELDS = ["protocol", "N", "rounds", "cell",
          "timed_execs", "timed_block", "timed_extra", "timed_verdict", "timed_wall",
          "base_execs", "base_block", "base_extra", "base_verdict", "base_wall",
          "pruning_pct", "expected", "timed_cmd"]

# --------------------------------------------------------------------------- #
def run(cmd, timeout, mem_gb=None):
    def preexec():
        if mem_gb:
            b = int(mem_gb * 1024**3)
            try: resource.setrlimit(resource.RLIMIT_AS, (b, b))
            except (ValueError, OSError): pass
    t0 = time.time()
    try:
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=timeout, preexec_fn=preexec)
        wall = time.time() - t0
        out = cp.stdout.decode("utf-8", "replace")
        if cp.returncode in (137, 134) or "memory allocation of" in out:
            return cp.returncode, out, wall, "oom"
        return cp.returncode, out, wall, "ok"
    except subprocess.TimeoutExpired as e:
        return None, (e.stdout or b"").decode("utf-8", "replace"), time.time() - t0, "timeout"

def parse_counts(out):
    """execs=N blocked=M [elected=K | commits=K]; falls back to the compare-table
    row format 'timed  N  M ...' if single-mode printing ever changes."""
    ex = re.search(r"execs=(\d+)", out)
    bl = re.search(r"blocked=(\d+)", out)
    xt = re.search(r"(?:elected|commits?)=(\d+)", out)
    return (int(ex.group(1)) if ex else None,
            int(bl.group(1)) if bl else None,
            int(xt.group(1)) if xt else None)

def verdict_of(ec, status):
    if status == "timeout": return "DNF"
    if status == "oom":     return "oom"
    if ec == 0:   return "hold"
    if ec == 101: return "FIRE"
    return f"err(ec={ec})"

# --------------------------------------------------------------------------- #
# cell builders                                                               #
# --------------------------------------------------------------------------- #
def grid_cell(protocol, binname, N, ballots, l, sd, w, tag):
    """One comm-closed LE cell (timed + baseline as separate invocations)."""
    base = [os.path.join(BIN, binname), "--nodes", str(N), "--ballots", str(ballots),
            "--u", str(U), "--w", str(w), "--l", str(l), "--sd", str(sd),
            "--parallel", "shared"]
    return dict(protocol=protocol, N=N, rounds=ballots, cell=tag, expected="hold",
                timed_cmd=base + ["--mode", "timed"],
                base_cmd=base + ["--mode", "baseline"])

def pc3_cell(N, rounds, wr, lr, sr, tag):
    base = [os.path.join(BIN, "three_pc_timed"), "--participants", str(N),
            "--rounds", str(rounds), "--u", str(U), "--w-ratio", str(wr),
            "--l-ratio", str(lr), "--sd-ratio", str(sr)]
    return dict(protocol="three_pc_timed", N=N, rounds=rounds, cell=tag, expected="hold",
                timed_cmd=base + ["--mode", "timed"],
                base_cmd=base + ["--mode", "baseline"])

# regimes used by results_v4 (L, sd, W at U=20)
REGIME = {"A": (18, 2, 200), "B": (6, 0, 60), "C": (2, 5, 100), "F": (0, 10, 40)}

def v4_small_cells():
    cells = []
    # --- three_pc rows (v4: L/U=0, sd/U=0.25 defaults unless stated) ---
    for N in (3, 4):
        for wr in (2, 3, 5, 7, 10):
            cells.append(pc3_cell(N, 1, wr, 0.0, 0.25, f"W/U={wr}"))
    cells.append(pc3_cell(3, 2, 7, 0.0, 0.25, "R=2"))
    # --- comm-closed LE N=3, plain and inbox, regimes A/B/C/F ---
    for proto, binname in (("comm_closed_leader_election", "comm_closed_leader_election"),
                           ("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox")):
        for reg, (l, sd, w) in REGIME.items():
            cells.append(grid_cell(proto, binname, 3, 1, l, sd, w, f"regime {reg}"))
        cells.append(grid_cell(proto, binname, 3, 2, *REGIME["A"], "B=2 regime A"))
        cells.append(grid_cell(proto, binname, 3, 2, *REGIME["C"], "B=2 regime C"))
    return cells

def v4_big_cells():
    cells = []
    # 3PC large
    for wr in (2, 3, 5, 7, 10):
        cells.append(pc3_cell(5, 1, wr, 0.0, 0.25, f"W/U={wr}"))
    cells.append(pc3_cell(5, 1, 7, 0.25, 0.25, "L/U=0.25"))
    cells.append(pc3_cell(5, 1, 7, 0.0, 0.5, "sd/U=0.5"))
    for wr in (2, 5, 7, 10):
        cells.append(pc3_cell(6, 1, wr, 0.0, 0.25, f"W/U={wr}"))
    cells.append(pc3_cell(7, 1, 10, 0.0, 0.25, "W/U=10"))
    cells.append(pc3_cell(4, 2, 7, 0.0, 0.25, "R=2"))
    cells.append(pc3_cell(3, 3, 7, 0.0, 0.25, "R=3"))
    # LE large
    for reg in ("A", "F"):
        cells.append(grid_cell("comm_closed_leader_election", "comm_closed_leader_election",
                               4, 1, *REGIME[reg], f"regime {reg}"))
        cells.append(grid_cell("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox",
                               4, 1, *REGIME[reg], f"regime {reg}"))
        cells.append(grid_cell("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox",
                               5, 1, *REGIME[reg], f"regime {reg}"))
    cells.append(grid_cell("comm_closed_leader_election", "comm_closed_leader_election",
                           4, 2, *REGIME["A"], "B=2 regime A"))
    cells.append(grid_cell("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox",
                           4, 2, *REGIME["A"], "B=2 regime A"))
    cells.append(grid_cell("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox",
                           5, 2, *REGIME["A"], "B=2 regime A"))
    cells.append(grid_cell("comm_closed_leader_election_inbox", "comm_closed_leader_election_inbox",
                           6, 1, *REGIME["A"], "regime A"))
    return cells

# --------------------------------------------------------------------------- #
# lease / swim boundary grids (P1_FEEDBACK_LEASE_SWIM.md)                     #
# --------------------------------------------------------------------------- #
def lease_cells():
    """FIRE iff pause >= TTL - 2*(U - L + sd). Each cell: boundary pause
    (expect FIRE) and boundary-1 (expect hold), unfenced; boundary pause
    fenced (expect hold)."""
    cells = []
    for R in (1, 2):
        for u in (1, 2):
            for l in (0, u):
                for sd in (0, 1):
                    for ttl in (6, 8, 10, 12):
                        bnd = ttl - 2 * (u - l + sd)
                        for pause, fencing, expect in (
                                (bnd,     "off", "FIRE"),
                                (bnd - 1, "off", "hold"),
                                (bnd,     "on",  "hold")):
                            if pause < 0:
                                continue
                            cmd = [os.path.join(BIN, "lease_timed"),
                                   "--rounds", str(R), "--u", str(u),
                                   "--l-ratio", str(l / u), "--sd-ratio", str(sd / u),
                                   "--ttl-ratio", str(ttl / u),
                                   "--pause-ratio", str(pause / u),
                                   "--fencing", fencing]
                            tag = f"U={u} L={l} sd={sd} TTL={ttl} pause={pause} fence={fencing}"
                            cells.append(dict(protocol="lease_timed", N=2, rounds=R,
                                              cell=tag, expected=expect,
                                              timed_cmd=cmd + ["--mode", "timed"],
                                              base_cmd=cmd + ["--mode", "baseline"]))
    return cells

def swim_cells():
    """FIRE iff Ws <= 2U + 2sd - min(L, max(0, 2L - Wp)). Each cell: boundary
    Ws (expect FIRE) and boundary+1 (expect hold). Baseline always FIREs."""
    cells = []
    combos = [(R, u, l, sd, u)          # Wp = U (the default ratio 1.0)
              for R in (1, 2, 3) for u in (1, 2) for l in (0, u) for sd in (0, 1)]
    combos += [(1, 4, 3, 0, 4), (1, 3, 2, 0, 3)]   # the max()-term pin cells
    for (R, u, l, sd, wp) in combos:
        bnd = 2 * u + 2 * sd - min(l, max(0, 2 * l - wp))
        for ws, expect in ((bnd, "FIRE"), (bnd + 1, "hold")):
            if ws <= 0:
                continue
            cmd = [os.path.join(BIN, "swim_timed"),
                   "--nodes", "3", "--rounds", str(R), "--u", str(u),
                   "--l-ratio", str(l / u), "--sd-ratio", str(sd / u),
                   "--w-probe-ratio", str(wp / u), "--w-suspect-ratio", str(ws / u)]
            tag = f"U={u} L={l} sd={sd} Wp={wp} Ws={ws}"
            cells.append(dict(protocol="swim_timed", N=3, rounds=R, cell=tag,
                              expected=expect,
                              timed_cmd=cmd + ["--mode", "timed"],
                              base_cmd=cmd + ["--mode", "baseline"]))
    return cells

# --------------------------------------------------------------------------- #
def do_cell(cell, timeout, mem_gb):
    row = {k: cell.get(k, "") for k in ("protocol", "N", "rounds", "cell", "expected")}
    row["timed_cmd"] = " ".join(cell["timed_cmd"]).replace(BIN + os.sep, "")
    tag = re.sub(r"[^A-Za-z0-9._=-]+", "_", f'{row["protocol"]}_N{row["N"]}_R{row["rounds"]}_{row["cell"]}')

    ec, out, wall, status = run(cell["timed_cmd"], timeout, mem_gb)
    row["timed_verdict"] = verdict_of(ec, status)
    row["timed_wall"] = f"{wall:.1f}"
    ex, bl, xt = parse_counts(out)
    row["timed_execs"], row["timed_block"], row["timed_extra"] = \
        (ex if ex is not None else "", bl if bl is not None else "", xt if xt is not None else "")
    if row["timed_verdict"] not in ("hold", row["expected"]):
        with open(os.path.join(LOGS, tag + ".timed.log"), "w") as f: f.write(out[-200_000:])

    b_ec, b_out, b_wall, b_status = run(cell["base_cmd"], timeout, mem_gb)
    row["base_verdict"] = verdict_of(b_ec, b_status)
    row["base_wall"] = f"{b_wall:.1f}"
    bex, bbl, bxt = parse_counts(b_out)
    row["base_execs"], row["base_block"], row["base_extra"] = \
        (bex if bex is not None else "", bbl if bbl is not None else "", bxt if bxt is not None else "")

    if ex and bex:
        row["pruning_pct"] = f"{100.0 * (bex - ex) / bex:.1f}"
    else:
        row["pruning_pct"] = ""
    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", required=True, choices=["small", "big", "lease_swim"])
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results_v5.csv"))
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--timeout", type=int, default=172800)   # 2 days per invocation
    ap.add_argument("--mem-gb", type=float, default=None)
    ap.add_argument("--append", action="store_true")
    args = ap.parse_args()

    cells = {"small": v4_small_cells, "big": v4_big_cells,
             "lease_swim": lambda: lease_cells() + swim_cells()}[args.tier]()
    print(f"[v5:{args.tier}] {len(cells)} cells, jobs={args.jobs}, timeout={args.timeout}s")

    write_header = not (args.append and os.path.exists(args.out))
    f = open(args.out, "a" if args.append else "w", newline="")
    w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
    if write_header: w.writeheader()
    f.flush()

    done = 0
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = {ex.submit(do_cell, c, args.timeout, args.mem_gb): c for c in cells}
        for fut in as_completed(futs):
            row = fut.result()
            w.writerow(row); f.flush()
            done += 1
            mark = "" if row["expected"] in ("", row["timed_verdict"]) else "  <-- UNEXPECTED"
            print(f"[{done}/{len(cells)}] {row['protocol']:>34} N{row['N']} R{row['rounds']} "
                  f"{row['cell']:<28} timed={row['timed_verdict']:>5}/{row['timed_execs']} "
                  f"base={row['base_verdict']:>5}/{row['base_execs']} "
                  f"prune={row['pruning_pct']}%{mark}", flush=True)
    f.close()
    print(f"[v5:{args.tier}] wrote {done} rows -> {args.out}")

if __name__ == "__main__":
    main()
