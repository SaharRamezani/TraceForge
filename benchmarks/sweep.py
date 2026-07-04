#!/usr/bin/env python3
"""
MUST-tau benchmark sweep driver for the thesis
"Verification of Distributed Protocols with Wait-Time Thresholds".

Runs the already-present TraceForge timed examples across the network-parameter
section-7 ratio grid (U fixed at 20 integer ticks), one process invocation per
(protocol, cell), and appends one CSV row per cell to results.csv.

Axes captured per cell (see README.md):
  1. verdict       -- hold / fire / err  (from process exit code)
  2. pruning_x     -- baseline_execs / timed_execs (computed, uniform)
  3. wcrt          -- NOT model output today -> always "needs-instr" (deferred)
  4. leaders_elected_total / no_leader_execs  -- leader-election protocols only

Nothing here changes protocol logic. Only the binaries' own CLI flags are used.
3PC and 3PC-buggy require the (config-only) --l-ratio/--sd-ratio flags; the driver
detects whether those flags are accepted and marks the L/sd axis "not-run" if not.
"""
import argparse, csv, os, re, resource, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN  = os.path.join(ROOT, "target", "release", "examples")
LOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

# ---- the section-7 ratio grid, realised at U = 20 integer ticks -------------
U = 20
L_RATIOS  = [0.0, 0.25, 0.5, 0.9]     # L/U  -> L  in {0,5,10,18}
W_RATIOS  = [2, 3, 5, 7, 10]          # W/U  -> W  in {40,60,100,140,200}
SD_RATIOS = [0.0, 0.25, 0.5]          # sd/U -> sd in {0,5,10}

def L_of(r):  return round(r * U)
def W_of(r):  return r * U
def SD_of(r): return round(r * U)

# Column order matches the user's curated layout (group/regime/wcrt dropped).
# Timed run: execs/block/verdict/leaders_elected_total/no_leader_execs/commit_execs/abort_execs.
# Untimed MUST baseline: baseline_*. pruning_x = baseline/timed.
# leader-election protocols fill leaders_elected_total/no_leader_execs (commit/abort = "—");
# commit protocols (3PC) fill commit_execs/abort_execs (elected/no-leader = "—").
CSV_FIELDS = ["protocol","N","rounds","mode","execs","block","baseline_execs","baseline_block",
              "verdict","baseline_verdict","pruning_x",
              "leaders_elected_total","no_leader_execs","baseline_elected",
              "commit_execs","abort_execs","baseline_commit_execs","baseline_abort_execs",
              "wall_time","status","cmd","l_over_u","sd_over_u","w_over_u","delta"]

# --------------------------------------------------------------------------- #
# output parsing                                                              #
# --------------------------------------------------------------------------- #
def parse_compare(out):
    """Parse the 'mode/execs/blocked[/elected]/time' table printed by compare mode.
    Returns {'baseline':(execs,block,elected_or_None), 'timed':(...)}."""
    res = {}
    for line in out.splitlines():
        p = line.split()
        if len(p) >= 3 and p[0] in ("baseline", "timed") and p[1].lstrip("-").isdigit():
            execs = int(p[1]); block = int(p[2])
            elected = None
            # comm-closed LE prints a 5-col table (mode,execs,blocked,elected,time);
            # the time field always carries a unit (ms/us/ns/s) so it is non-numeric.
            if len(p) >= 5 and p[3].lstrip("-").isdigit():
                elected = int(p[3])
            res[p[0]] = (execs, block, elected)
    return res

def parse_single_execs(out):
    """Parse 'execs=N blocked=M' (single-mode / instrumented buggy hold path)."""
    m = re.search(r"execs=(\d+).*?blocked=(\d+)", out)
    if m: return int(m.group(1)), int(m.group(2))
    m = re.search(r"execs=(\d+)", out)
    if m: return int(m.group(1)), None
    return None, None

def parse_commit_abort(out):
    """Parse 'commit/abort (complete execs): baseline C/A   timed C/A' (3PC compare).
    Returns (b_commit, b_abort, t_commit, t_abort) or None."""
    m = re.search(r"commit/abort.*?baseline\s+(\d+)/(\d+)\s+timed\s+(\d+)/(\d+)", out)
    return tuple(int(g) for g in m.groups()) if m else None

def parse_single_full(out):
    """Parse 'execs=N blocked=M [elected=K]' from a single-mode print_one line.
    Returns (execs, block, elected_or_None)."""
    ex = re.search(r"execs=(\d+)", out)
    bl = re.search(r"blocked=(\d+)", out)
    el = re.search(r"elected=(\d+)", out)
    return (int(ex.group(1)) if ex else None,
            int(bl.group(1)) if bl else None,
            int(el.group(1)) if el else None)

# --------------------------------------------------------------------------- #
# process runner                                                              #
# --------------------------------------------------------------------------- #
def run(cmd, timeout, mem_gb=None):
    """Run cmd (list). Returns (exit, out, wall, status). status in
    ok/timeout/oom/error. exit may be None on timeout."""
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
        ec = cp.returncode
        if ec in (137, 134) or "memory allocation of" in out or "Out of memory" in out:
            return ec, out, wall, "oom"
        return ec, out, wall, "ok"
    except subprocess.TimeoutExpired as e:
        wall = time.time() - t0
        out = (e.stdout or b"").decode("utf-8", "replace")
        return None, out, wall, "timeout"

# --------------------------------------------------------------------------- #
# per-protocol cell builders -> list of dicts with keys:                       #
#   cmd(list), and the CSV identity fields                                     #
# --------------------------------------------------------------------------- #
def cells_comm_closed(binname, protocol, nodes_list, ballots=1):
    cells = []
    for N in nodes_list:
        for lr in L_RATIOS:
            for sr in SD_RATIOS:
                for wr in W_RATIOS:
                    cmd = [os.path.join(BIN, binname), "--mode", "compare",
                           "--nodes", str(N), "--ballots", str(ballots),
                           "--u", str(U), "--w", str(W_of(wr)),
                           "--l", str(L_of(lr)), "--sd", str(SD_of(sr)),
                           "--parallel", "shared"]
                    cells.append(dict(protocol=protocol, N=N, l_over_u=lr,
                                      sd_over_u=sr, w_over_u=wr, rounds=ballots,
                                      kind="le_compare", cmd=cmd))
    return cells

def cells_3pc(nodes_list, l_sd_supported):
    cells = []
    for N in nodes_list:
        lrs = L_RATIOS if l_sd_supported else [0.0]
        srs = SD_RATIOS if l_sd_supported else [0.0]
        for lr in lrs:
            for sr in srs:
                for wr in W_RATIOS:
                    cmd = [os.path.join(BIN, "three_pc_timed"), "--mode", "compare",
                           "--participants", str(N), "--u", str(U), "--w-ratio", str(wr)]
                    if l_sd_supported:
                        cmd += ["--l-ratio", str(lr), "--sd-ratio", str(sr)]
                    cells.append(dict(protocol="three_pc_timed", N=N, l_over_u=lr,
                                      sd_over_u=sr, w_over_u=wr, kind="3pc_compare",
                                      cmd=cmd))
    return cells

def cells_3pc_buggy(nodes_list, l_sd_supported):
    cells = []
    for N in nodes_list:
        lrs = L_RATIOS if l_sd_supported else [0.0]
        srs = SD_RATIOS if l_sd_supported else [0.0]
        for lr in lrs:
            for sr in srs:
                for wr in W_RATIOS:
                    base = [os.path.join(BIN, "three_pc_timed_buggy"),
                            "--participants", str(N), "--u", str(U), "--w-ratio", str(wr)]
                    if l_sd_supported:
                        base += ["--l-ratio", str(lr), "--sd-ratio", str(sr)]
                    cmd = base + ["--mode", "timed"]
                    baseline_cmd = base + ["--mode", "baseline"]
                    cells.append(dict(protocol="three_pc_timed_buggy", N=N, l_over_u=lr,
                                      sd_over_u=sr, w_over_u=wr, kind="3pc_buggy",
                                      cmd=cmd, baseline_cmd=baseline_cmd))
    return cells

def cells_raft_timed(nodes_list, deltas, rounds_list):
    """raft TIMED-only cells (no baseline -> no pruning ratio). For N>=5 where the
    baseline subset-enumeration is intractable; still gives verdict + LE outcome."""
    cells = []
    for N in nodes_list:
        for d in deltas:
            for r in rounds_list:
                cmd = [os.path.join(BIN, "raft_leader_election"), "--mode", "timed",
                       "--nodes", str(N), "--delta", str(d), "--rounds", str(r)]
                cells.append(dict(protocol="raft_leader_election", N=N, l_over_u="",
                                  sd_over_u="", w_over_u="", delta=d, rounds=r,
                                  kind="raft_timed", cmd=cmd))
    return cells

def cells_ccle_timed(binname, protocol, nodes_list, grid, ballots=1):
    """comm-closed TIMED-only cells over an explicit (lr,sr,wr) list `grid`
    (no baseline -> pruning n/a). For N>=5 or ballots>1 where compare is intractable.
    The ballots count is recorded in the `rounds` CSV column (the cmd column also
    carries the literal --ballots)."""
    cells = []
    for N in nodes_list:
        for (lr, sr, wr) in grid:
            cmd = [os.path.join(BIN, binname), "--mode", "timed",
                   "--nodes", str(N), "--ballots", str(ballots),
                   "--u", str(U), "--w", str(W_of(wr)),
                   "--l", str(L_of(lr)), "--sd", str(SD_of(sr)), "--parallel", "shared"]
            cells.append(dict(protocol=protocol, N=N, l_over_u=lr, sd_over_u=sr,
                              w_over_u=wr, delta="", rounds=ballots, kind="ccle_timed", cmd=cmd))
    return cells

def cells_raft(nodes_list, deltas, rounds_list):
    cells = []
    for N in nodes_list:
        for d in deltas:
            for r in rounds_list:
                cmd = [os.path.join(BIN, "raft_leader_election"), "--mode", "compare",
                       "--nodes", str(N), "--delta", str(d), "--rounds", str(r)]
                cells.append(dict(protocol="raft_leader_election", N=N,
                                  l_over_u="", sd_over_u="", w_over_u="",
                                  delta=d, rounds=r, kind="raft_compare",
                                  cmd=cmd))
    return cells

# --------------------------------------------------------------------------- #
# run one cell, produce CSV row                                               #
# --------------------------------------------------------------------------- #
def do_cell(cell, timeout, mem_gb):
    ec, out, wall, status = run(cell["cmd"], timeout, mem_gb)
    kind = cell["kind"]
    row = {k: cell.get(k, "") for k in
           ["protocol","N","l_over_u","sd_over_u","w_over_u","delta","rounds"]}
    # store a relative cmd (strip the target/release/ prefix to keep the CSV readable)
    _pfx = os.path.join(ROOT, "target", "release") + os.sep
    row["cmd"] = " ".join(cell["cmd"]).replace(_pfx, "")
    row["wall_time"] = f"{wall:.3f}"
    row["status"] = status
    row["baseline_execs"] = ""; row["baseline_block"] = ""
    row["baseline_elected"] = ""; row["baseline_verdict"] = ""
    # commit/abort default to "—" (only commit protocols 3PC fill these)
    row["commit_execs"] = "—"; row["abort_execs"] = "—"
    row["baseline_commit_execs"] = "—"; row["baseline_abort_execs"] = "—"
    # log raw output for anything that fired / failed / timed out
    tag = f'{cell["protocol"]}_N{cell["N"]}_l{cell.get("l_over_u","")}_sd{cell.get("sd_over_u","")}_w{cell.get("w_over_u","")}'
    tag = tag.replace(".", "p").replace(" ", "")

    if kind in ("le_compare", "3pc_compare", "raft_compare"):
        row["mode"] = "compare"
        c = parse_compare(out)
        b = c.get("baseline"); t = c.get("timed")
        if status != "ok" or t is None or b is None:
            row.update(execs="", block="", verdict="err", pruning_x="",
                       leaders_elected_total="", no_leader_execs="")
            _log(tag, out)
            return row
        b_ex, b_bl, b_el = b; t_ex, t_bl, t_el = t
        row["execs"] = t_ex; row["block"] = t_bl
        row["baseline_execs"] = b_ex; row["baseline_block"] = b_bl
        row["baseline_elected"] = b_el if b_el is not None else ""
        row["pruning_x"] = f"{(b_ex / max(t_ex,1)):.3f}"
        # these are correct protocols -> exit 0 means property holds (both runs)
        row["verdict"] = "hold" if ec == 0 else "FIRE"
        row["baseline_verdict"] = row["verdict"]  # compare runs both in one process
        if ec != 0: _log(tag, out)
        if kind == "le_compare":
            row["leaders_elected_total"] = t_el if t_el is not None else ""
            row["no_leader_execs"] = (t_ex - t_el) if t_el is not None else ""
        elif kind == "raft_compare":
            # structural: complete raft exec => exactly one leader; blocked => no leader
            row["leaders_elected_total"] = t_ex
            row["no_leader_execs"] = t_bl
        else:
            # commit protocols (3PC): no election; fill commit/abort instead
            row["leaders_elected_total"] = "—"; row["no_leader_execs"] = "—"
            ca = parse_commit_abort(out)
            if ca is not None:
                b_c, b_a, t_c, t_a = ca
                row["commit_execs"] = t_c; row["abort_execs"] = t_a
                row["baseline_commit_execs"] = b_c; row["baseline_abort_execs"] = b_a
        return row

    if kind in ("raft_timed", "ccle_timed"):
        row["mode"] = "timed"
        row["pruning_x"] = "n/a"  # baseline intractable at this N -> no ratio
        row["baseline_execs"] = "intractable"; row["baseline_block"] = "intractable"
        row["baseline_verdict"] = "intractable"
        if status != "ok":
            row.update(execs="", block="", verdict=status,
                       leaders_elected_total="", no_leader_execs="")
            _log(tag, out); return row
        ex, bl, el = parse_single_full(out)
        if ex is None:
            row.update(execs="", block="", verdict="err",
                       leaders_elected_total="", no_leader_execs="")
            _log(tag, out); return row
        row["execs"] = ex; row["block"] = bl if bl is not None else ""
        row["verdict"] = "hold" if ec == 0 else "FIRE"
        if ec != 0: _log(tag, out)
        if kind == "ccle_timed":
            row["leaders_elected_total"] = el if el is not None else ""
            row["no_leader_execs"] = (ex - el) if el is not None else ""
        else:  # raft structural
            row["leaders_elected_total"] = ex
            row["no_leader_execs"] = bl if bl is not None else ""
        return row

    if kind == "3pc_buggy":
        row["mode"] = "compare"  # we run BOTH baseline (untimed) and timed invocations
        row["leaders_elected_total"] = "—"; row["no_leader_execs"] = "—"
        row["pruning_x"] = "n/a"  # both runs fire -> no exec counts to ratio

        # timed run already executed at top of do_cell (cell["cmd"] has --mode timed)
        if ec == 0:
            tex, tbl = parse_single_execs(out)
        else:
            tex, tbl = "", ""
        row["verdict"] = ("timeout" if status == "timeout" else
                          "oom" if status == "oom" else
                          "hold" if ec == 0 else "FIRE" if ec == 101 else f"err(ec={ec})")
        row["execs"] = tex if (ec == 0 and tex is not None) else ""
        row["block"] = tbl if (ec == 0 and tbl is not None) else ""
        if row["verdict"] not in ("hold",): _log(tag, out)

        # baseline (untimed MUST) run -- the comparison point
        b_ec, b_out, b_wall, b_status = run(cell["baseline_cmd"], timeout, mem_gb)
        if b_status == "timeout": row["baseline_verdict"] = "timeout"
        elif b_status == "oom":   row["baseline_verdict"] = "oom"
        elif b_ec == 0:
            bex, bbl = parse_single_execs(b_out)
            row["baseline_verdict"] = "hold"
            row["baseline_execs"] = bex if bex is not None else ""
            row["baseline_block"] = bbl if bbl is not None else ""
        elif b_ec == 101:
            row["baseline_verdict"] = "FIRE"
        else:
            row["baseline_verdict"] = f"err(ec={b_ec})"
        return row

    raise ValueError(kind)

def _log(tag, out):
    try:
        with open(os.path.join(LOGS, tag + ".log"), "w") as f:
            f.write(out)
    except OSError:
        pass

# --------------------------------------------------------------------------- #
def build_cells(which):
    cells = []
    if "comm_closed" in which:
        cells += cells_comm_closed("comm_closed_leader_election",
                                   "comm_closed_leader_election", [3])
    if "comm_closed_inbox" in which:
        cells += cells_comm_closed("comm_closed_leader_election_inbox",
                                   "comm_closed_leader_election_inbox", [3])
    if "raft" in which:
        cells += cells_raft([3], [1, 2, 3], [1, 2])
    if "raft5" in which:
        cells += cells_raft([5], [1, 2, 3], [1])
    if "3pc" in which:
        cells += cells_3pc([3], l_sd_supported=detect_lsd("three_pc_timed"))
    if "3pc_buggy" in which:
        cells += cells_3pc_buggy([3], l_sd_supported=detect_lsd("three_pc_timed_buggy"))
    if "3pc4" in which:
        cells += cells_3pc([4], l_sd_supported=detect_lsd("three_pc_timed"))
    if "comm_closed5" in which:
        cells += cells_comm_closed("comm_closed_leader_election",
                                   "comm_closed_leader_election", [5])
    # --- N=5 timed-only (baseline intractable): bounded subsets ---
    if "raft5_timed" in which:
        cells += cells_raft_timed([5], [1, 2, 3], [1])
    if "ccle5_timed" in which:
        # representative subset at N=5: the L/U axis (W/U=5, sd=0) -- the only axis
        # that bit at N=3 -- plus tight/loose W extremes and the max-sd corner.
        grid = [(l, 0.0, 5) for l in L_RATIOS] \
             + [(0.0, 0.0, 2), (0.0, 0.0, 10), (0.0, 0.5, 5)]
        cells += cells_ccle_timed("comm_closed_leader_election",
                                  "comm_closed_leader_election", [5], grid)
    if "inbox_n4b2" in which:
        # N=4 ballots=2 alone, tight corner; run with an effectively-unbounded cap.
        cells += cells_ccle_timed("comm_closed_leader_election_inbox",
                                  "comm_closed_leader_election_inbox", [4], [(0.9, 0.0, 2)], ballots=2)
    if "inbox_n45" in which:
        # inbox LE scales further than non-inbox: probe N=4 b=2 and N=5 b=1 at the
        # tight corner (L/U=0.9, sd=0, W/U=2 -- most pruning, best chance to finish).
        tight = [(0.9, 0.0, 2)]
        cells += cells_ccle_timed("comm_closed_leader_election_inbox",
                                  "comm_closed_leader_election_inbox", [4], tight, ballots=2)
        cells += cells_ccle_timed("comm_closed_leader_election_inbox",
                                  "comm_closed_leader_election_inbox", [5], tight, ballots=1)
    if "ccle_b2" in which:
        # ballots=2 at N=3: the decisive sd test. sd can only bite across a ballot
        # boundary, so compare sd/U=0 vs 0.5 at loose (L/U=0) and tight (L/U=0.9) transit.
        grid = [(0.0, 0.0, 5), (0.0, 0.5, 5), (0.9, 0.0, 5), (0.9, 0.5, 5)]
        cells += cells_ccle_timed("comm_closed_leader_election",
                                  "comm_closed_leader_election", [3], grid, ballots=2)
    return cells

def detect_lsd(binname):
    """Return True iff the compiled binary contains the --l-ratio flag string
    (i.e. the config-only edit landed). Instant, no model-check run."""
    path = os.path.join(BIN, binname)
    try:
        with open(path, "rb") as f:
            return b"--l-ratio" in f.read()
    except OSError:
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--which", required=True,
                    help="comma list: comm_closed,comm_closed_inbox,raft,3pc,3pc_buggy,raft5,comm_closed5,inbox_n45,ccle_b2")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv"))
    ap.add_argument("--jobs", type=int, default=2)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--mem-gb", type=float, default=None)
    ap.add_argument("--append", action="store_true")
    args = ap.parse_args()

    which = [w.strip() for w in args.which.split(",") if w.strip()]
    cells = build_cells(which)
    print(f"[sweep] {len(cells)} cells across {which}  jobs={args.jobs} timeout={args.timeout}s")

    write_header = not (args.append and os.path.exists(args.out))
    mode = "a" if args.append else "w"
    f = open(args.out, mode, newline="")
    w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
    if write_header: w.writeheader()
    f.flush()

    done = 0
    with ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = {ex.submit(do_cell, c, args.timeout, args.mem_gb): c for c in cells}
        for fut in as_completed(futs):
            row = fut.result()
            w.writerow(row); f.flush()
            done += 1
            print(f"[{done}/{len(cells)}] {row['protocol']:>34} N{row['N']} "
                  f"l{row['l_over_u']} sd{row['sd_over_u']} w{row['w_over_u']} "
                  f"-> {row['verdict']:>7} prune={row['pruning_x']} "
                  f"elect={row['leaders_elected_total']} {row['status']} {row['wall_time']}s")
    f.close()
    print(f"[sweep] wrote {done} rows -> {args.out}")

if __name__ == "__main__":
    main()
