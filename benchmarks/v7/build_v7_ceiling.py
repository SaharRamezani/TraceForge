#!/usr/bin/env python3
"""Adds the lease / SWIM ceiling sheets to the v7 workbook. Works on partial data:
every planned cell is finished (hold/FIRE), DNF (killed unfinished), running, queued, or not attempted.

Inputs (fetched from brain05 by fetch_ceiling.sh into benchmarks/v7_snapshot/ceiling/):
  results_v7_ceiling.csv   rows written by sweep_v7.py as jobs end
  plan_v7.csv              every job the sweep was asked to run
  ps_running.txt           `ps -o etimes,args` lines of the jobs alive at fetch time
"""
import csv, os, re
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
CEIL = os.path.join(os.path.dirname(HERE), "v7_snapshot", "ceiling")

HDR_FILL = PatternFill("solid", fgColor="FF2C5F8A")
HDR_FONT = Font(bold=True, color="FFFFFFFF")
GREEN = PatternFill("solid", fgColor="FFC6EFCE")
YELLOW = PatternFill("solid", fgColor="FFFFF2CC")
ORANGE = PatternFill("solid", fgColor="FFF8CBAD")
BLUE = PatternFill("solid", fgColor="FFDDEBF7")
GREY = PatternFill("solid", fgColor="FFEDEDED")
THIN = Side(style="thin", color="FFBFBFBF")
BOX = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

FAMILIES = [  # (key, sheet label, timed family, untimed family, N label, cell description)
    ("swim_L0", "SWIM, L=0 (widest transit window)", "swim_L0", "swim_untimed", "members N"),
    ("swim_L1", "SWIM, L=1=U/2", "swim_L1", "swim_untimed", "members N"),
    ("lease_unfenced", "Lease, unfenced, pause = boundary-1", "lease_unfenced", "lease_unfenced", "clients C"),
    ("lease_fenced", "Lease, fenced, pause = boundary", "lease_fenced", "lease_fenced", "clients C"),
]

def wall_txt(s):
    s = float(s)
    if s < 1: return "<1 s"
    if s < 90: return f"{s:.0f} s"
    if s < 5400: return f"{s / 60:.0f} min"
    if s < 172800: return f"{s / 3600:.1f} h"
    return f"{s / 86400:.1f} d"

def dur_txt(sec):
    return wall_txt(sec)

def load_csv(fn):
    p = os.path.join(CEIL, fn)
    return list(csv.DictReader(open(p, newline=""))) if os.path.exists(p) else []

def parse_running():
    p = os.path.join(CEIL, "ps_running.txt")
    out = {}
    if not os.path.exists(p):
        return out
    for line in open(p):
        m = re.match(r"\s*(\d+)\s+(.*)", line)
        if not m or "target-v7/release/examples/" not in line or "/usr/bin/time" in line or "taskset" in line:
            continue
        et, args = int(m.group(1)), m.group(2).split()
        exe = os.path.basename(args[0])
        if exe not in ("lease_timed", "swim_timed"):
            continue
        g = lambda flag: args[args.index(flag) + 1] if flag in args else None
        side = g("--mode")
        rounds = g("--rounds")
        if exe == "lease_timed":
            N = g("--clients"); fam = "lease_fenced" if g("--fencing") == "on" else "lease_unfenced"
        else:
            N = g("--nodes")
            fam = "swim_untimed" if side == "baseline" else ("swim_L0" if float(g("--l-ratio")) == 0 else "swim_L1")
        out[(fam, int(N), int(rounds), side)] = et
    return out

class Data:
    def __init__(self):
        self.results = load_csv("results_v7_ceiling.csv")
        self.plan = load_csv("plan_v7.csv")
        self.running = parse_running()
        self.done = {}
        for r in self.results:
            self.done[(r["family"], int(r["N"]), int(r["rounds"]), r["side"])] = r
        self.planned = {(p["family"], int(p["N"]), int(p["R"]), p["side"]): p for p in self.plan}

    def state(self, fam, N, R, side):
        k = (fam, N, R, side)
        if k in self.done:
            r = self.done[k]
            v = r["verdict"]
            if v.startswith("DNF"): return "dnf", r
            if v.startswith("not run"): return "notrun", r
            return "done", r
        if k in self.running: return "running", self.running[k]
        if k in self.planned: return "queued", self.planned[k]
        return "none", None

def tot(r):
    return int(r["execs"]) + int(r["block"])

def side_txt(st, obj):
    if st == "done": return f"{wall_txt(obj['wall_s'])}"
    if st == "dnf": return "DNF"
    if st == "running": return f"running {dur_txt(obj)}"
    if st == "queued": return "queued"
    if st == "notrun": return "not run"
    return "n/a"

def style_header(ws, row, ncols):
    for c in range(1, ncols + 1):
        cell = ws.cell(row, c)
        cell.fill, cell.font = HDR_FILL, HDR_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BOX

def fill_of(ts, us):
    st = {ts, us}
    if ts == "done" and us == "done": return GREEN
    if "none" in st and st <= {"none", "queued"}: return GREY
    if "dnf" in st and ts == "dnf" and us == "dnf": return ORANGE
    if "dnf" in st or "notrun" in st: return YELLOW
    if st & {"running", "queued"}: return BLUE
    return GREY

def add_sheets(wb):
    D = Data()
    add_summary(wb, D)
    add_maps(wb, D)
    add_data(wb, D)
    add_method(wb, D)

# ------------------------------------------------------------------ ceiling maps
GRID_N = {"swim": [3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26],
          "lease": list(range(2, 11))}
GRID_R = {"swim": [1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32],
          "lease": list(range(1, 12))}
SWIM_R_L1 = [1, 2, 4, 8, 16, 32]

def add_maps(wb, D):
    for proto, title in (("swim", "SWIM ceiling map"), ("lease", "Lease ceiling map")):
        ws = wb.create_sheet(title)
        ws["A1"] = (f"{'SWIM' if proto == 'swim' else 'Lease'}: wall time per cell, timed (T) / untimed (U). "
                    "Green = both finished, yellow = one side did not finish, orange = neither finished, "
                    "blue = still running or queued when the file was built, grey = not attempted (projected beyond two days).")
        ws["A1"].font = Font(bold=True)
        row = 3
        for key, label, tf, uf, nlabel in FAMILIES:
            if not key.startswith(proto):
                continue
            ws.cell(row, 1, label).font = Font(bold=True, size=12)
            row += 1
            Rs = GRID_R[proto] if key != "swim_L1" else SWIM_R_L1
            ws.cell(row, 1, f"{nlabel} \\ rounds R")
            for j, R in enumerate(Rs):
                ws.cell(row, 2 + j, R)
            style_header(ws, row, 1 + len(Rs))
            row += 1
            for N in GRID_N[proto]:
                ws.cell(row, 1, N).font = Font(bold=True)
                ws.cell(row, 1).alignment = Alignment(horizontal="center")
                for j, R in enumerate(Rs):
                    ts, to = D.state(tf, N, R, "timed")
                    us, uo = D.state(uf, N, R, "baseline")
                    cell = ws.cell(row, 2 + j)
                    if ts == "none" and us == "none":
                        cell.value = ""
                    else:
                        cell.value = f"T {side_txt(ts, to)}\nU {side_txt(us, uo)}"
                    cell.fill = fill_of(ts, us)
                    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                    cell.border = BOX
                ws.row_dimensions[row].height = 30
                row += 1
            row += 2
        ws.column_dimensions["A"].width = 22
        for j in range(2, 20):
            ws.column_dimensions[get_column_letter(j)].width = 15
        ws.freeze_panes = "B3"

# ------------------------------------------------------------------ long data sheet
def add_data(wb, D):
    for proto, title in (("swim", "SWIM ceiling data"), ("lease", "Lease ceiling data")):
        ws = wb.create_sheet(title)
        ws["A1"] = f"{'SWIM' if proto == 'swim' else 'Lease'}: every planned cell, both sides (--keep-going: each side explores its whole space)"
        ws["A1"].font = Font(bold=True)
        hdr = ["Regime / variant", "N (members / clients)", "Rounds", "Cell (parameters)",
               "Timed execs", "Timed blocked", "Timed total", "Timed violations", "Timed verdict", "Timed wall (s)", "Timed cores",
               "Untimed execs", "Untimed blocked", "Untimed total", "Untimed violations", "Untimed verdict", "Untimed wall (s)", "Untimed cores",
               "Total ratio (untimed / timed)", "Wall ratio (untimed / timed)"]
        for c, h in enumerate(hdr, 1):
            ws.cell(3, c, h)
        style_header(ws, 3, len(hdr))
        r = 4
        allk = list(D.planned) + list(D.done) + list(D.running)
        for (fam, tf, uf) in [(f[0], f[2], f[3]) for f in FAMILIES if f[0].startswith(proto)]:
            cells = {(N, R) for (f, N, R, s) in allk if f == tf and s == "timed"}
            if fam == "swim_L0":     # untimed-only cells (untimed reaches further than timed) are listed under L=0
                cells |= {(N, R) for (f, N, R, s) in allk if f == "swim_untimed"}
            for (N, R) in sorted(cells):
                ts, to = D.state(tf, N, R, "timed")
                us, uo = D.state(uf, N, R, "baseline")
                def sidecols(st, o):
                    if st == "done":
                        ex, bl = int(o["execs"]), int(o["block"])
                        return [ex, bl, ex + bl, int(o["violations"]), o["verdict"], float(o["wall_s"]), int(o["cpus"])]
                    if st == "dnf":
                        return [None, None, None, None, o["verdict"], None, int(o["cpus"])]
                    if st == "running":
                        return [None, None, None, None, f"running ({dur_txt(o)} so far)", None, None]
                    if st == "queued":
                        return [None, None, None, None, "queued", None, int(o["cpus"])]
                    return [None, None, None, None, "not attempted", None, None]
                tc, uc = sidecols(ts, to), sidecols(us, uo)
                cell = to["cell"] if ts in ("done", "dnf") else (uo["cell"] if us in ("done", "dnf") else "")
                vals = [fam, N, R, cell] + tc + uc
                vals += [round(uc[2] / tc[2], 2) if tc[2] and uc[2] else None,
                         round(uc[5] / tc[5], 2) if tc[5] and uc[5] else None]
                for c, v in enumerate(vals, 1):
                    ws.cell(r, c, v)
                for c in (5, 6, 7, 8, 12, 13, 14, 15):
                    ws.cell(r, c).number_format = "#,##0"
                for c in (10, 17):
                    ws.cell(r, c).number_format = "#,##0.0"
                f = fill_of(ts, us)
                for c in range(1, len(hdr) + 1):
                    ws.cell(r, c).fill = f
                r += 1
        widths = [16, 10, 8, 48] + [13, 13, 13, 12, 34, 12, 8] * 2 + [14, 14]
        for c, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(c)].width = w
        ws.row_dimensions[3].height = 45
        ws.freeze_panes = "E4"
        ws.auto_filter.ref = f"A3:{get_column_letter(len(hdr))}{max(r - 1, 4)}"

# ------------------------------------------------------------------ summary
def frontier(D, fam, side, proto):
    """max N that finished at each R, and max R that finished at each N"""
    byR, byN = {}, {}
    for (f, N, R, s), r in D.done.items():
        if f == fam and s == side and not r["verdict"].startswith(("DNF", "not run", "err")):
            byR[R] = max(byR.get(R, 0), N)
            byN[N] = max(byN.get(N, 0), R)
    return byR, byN

def add_summary(wb, D):
    ws = wb.create_sheet("Ceiling summary")
    n_done = sum(1 for r in D.results if not r["verdict"].startswith(("DNF", "not run")))
    n_dnf = sum(1 for r in D.results if r["verdict"].startswith("DNF"))
    n_run = len(D.running)
    n_q = sum(1 for k in D.planned if k not in D.done and k not in D.running)
    ws["A1"] = "How far can lease and SWIM be pushed in two days? (brain05, 240 cores, launched 2026-09-20 19:23, deadline 2026-09-22 17:00)"
    ws["A1"].font = Font(bold=True, size=13)
    ws["A2"] = (f"State when this file was built: {n_done} sides finished, {n_dnf} did not finish (killed), "
                f"{n_run} running, {n_q} queued, of {len(D.plan)} planned. Sheets 'SWIM ceiling map' and 'Lease ceiling map' "
                "show every cell; the two '... data' sheets carry the counts.")
    ws["A2"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A2:M2"); ws.row_dimensions[2].height = 34
    r = 4
    hdr = ["Family", "Side", "Largest N finished, by rounds R"]
    ws.cell(r, 1, "Largest number of nodes (SWIM members / lease clients) that finished, for each round count R")
    ws.cell(r, 1).font = Font(bold=True)
    r += 1
    Rs_show = {"swim": [1, 2, 3, 4, 6, 8, 12, 16, 24], "lease": [1, 2, 3, 4, 5, 6, 8, 10]}
    for key, label, tf, uf, nlabel in FAMILIES:
        proto = "swim" if key.startswith("swim") else "lease"
        ws.cell(r, 1, "Family"); ws.cell(r, 2, "Side")
        for j, R in enumerate(Rs_show[proto]):
            ws.cell(r, 3 + j, f"R={R}")
        style_header(ws, r, 2 + len(Rs_show[proto]))
        r += 1
        for side, fam, sname in (("timed", tf, "timed"), ("baseline", uf, "untimed")):
            byR, _ = frontier(D, fam, side, proto)
            ws.cell(r, 1, label); ws.cell(r, 2, sname)
            for j, R in enumerate(Rs_show[proto]):
                ws.cell(r, 3 + j, byR.get(R, "-"))
            for c in range(1, 3 + len(Rs_show[proto])):
                ws.cell(r, c).border = BOX
            r += 1
        r += 1
    r += 1
    ws.cell(r, 1, "Largest number of rounds R that finished, for each node count N").font = Font(bold=True)
    r += 1
    Ns_show = {"swim": [3, 4, 5, 6, 8, 10, 12, 14], "lease": [2, 3, 4, 5, 6, 8]}
    for key, label, tf, uf, nlabel in FAMILIES:
        proto = "swim" if key.startswith("swim") else "lease"
        ws.cell(r, 1, "Family"); ws.cell(r, 2, "Side")
        for j, N in enumerate(Ns_show[proto]):
            ws.cell(r, 3 + j, f"N={N}")
        style_header(ws, r, 2 + len(Ns_show[proto]))
        r += 1
        for side, fam, sname in (("timed", tf, "timed"), ("baseline", uf, "untimed")):
            _, byN = frontier(D, fam, side, proto)
            ws.cell(r, 1, label); ws.cell(r, 2, sname)
            for j, N in enumerate(Ns_show[proto]):
                ws.cell(r, 3 + j, byN.get(N, "-"))
            for c in range(1, 3 + len(Ns_show[proto])):
                ws.cell(r, c).border = BOX
            r += 1
        r += 1
    # largest finished cells by wall time
    r += 1
    ws.cell(r, 1, "The ten longest runs that finished").font = Font(bold=True)
    r += 1
    for c, h in enumerate(["Family", "Side", "N", "Rounds", "Explored (execs + blocked)", "Wall", "Cores", "Verdict"], 1):
        ws.cell(r, c, h)
    style_header(ws, r, 8); r += 1
    fin = [x for x in D.results if not x["verdict"].startswith(("DNF", "not run", "err"))]
    for x in sorted(fin, key=lambda x: -float(x["wall_s"]))[:10]:
        for c, v in enumerate([x["family"], "timed" if x["side"] == "timed" else "untimed", int(x["N"]), int(x["rounds"]),
                               tot(x), wall_txt(x["wall_s"]), int(x["cpus"]), x["verdict"]], 1):
            ws.cell(r, c, v)
        ws.cell(r, 5).number_format = "#,##0"
        r += 1
    ws.column_dimensions["A"].width = 40
    ws.column_dimensions["B"].width = 10
    for c in range(3, 14):
        ws.column_dimensions[get_column_letter(c)].width = 12

# ------------------------------------------------------------------ method
def add_method(wb, D):
    ws = wb.create_sheet("Ceiling method")
    lines = [
        ("Method of the ceiling experiment (v7, September 2026)", True),
        ("Question: how many nodes and rounds can the timed and the untimed checker handle for lease and SWIM when each run may take up to two days? The v6 comparison stopped at 4 clients / 5 rounds (lease) and 5 members / 5 rounds (SWIM), where every run took under a few minutes.", False),
        ("Machine and window: brain05 (256 cores); the sweep used 240 pinned cores, started 2026-09-20 19:23 and had a hard deadline of 2026-09-22 17:00 (about 45 hours). A side that was still running at the deadline was killed and is recorded as 'DNF (did not finish; killed after ...)': a DNF is neither hold nor FIRE.", False),
        ("Every cell runs the identical program twice, once under the untimed checker and once timed, as separate processes (a DNF on one side never delays the other). Both explore their whole state space: the checker keeps going after a violation, so the untimed side reports how many of its executions violate the property.", False),
        ("Parameters (integer ticks): U=2, sd=1. Lease: TTL=10, L=0; unfenced runs use pause = boundary-1 = TTL-2(U-L+sd)-1 (timed proves the safety property, untimed cannot), fenced runs use pause = boundary (both hold). SWIM: Wp=U, Ws = boundary+1 = 2U+2sd-min(L,max(0,2L-Wp))+1; L=0 (widest transit window) and L=1=U/2. The untimed counts do not depend on L, U, sd or the windows (the untimed checker ignores time), so one untimed run serves both SWIM regimes.", False),
        ("Why L=0 as the main regime: with L=0 the message window is widest and timing prunes no complete execution at all; the timed run still rejects every violating one. It is the honest worst case for the timed side. At L=U the timed side collapses to a handful of executions and would push far further, which would flatter it.", False),
        ("Counts: 'execs' = complete executions; 'blocked' = executions that ended blocked (an unmatched receive, or a kept-going assertion violation); 'total' = execs + blocked = everything the checker explored. 'violations' = stale writes applied (lease) or dead declarations of a live member (SWIM); it is an event counter and can exceed the number of executions, so quote it as 'violation events'.", False),
        ("Exact laws seen in the data: lease unfenced complete executions = (C! * 2^C)^R on both sides (8, 48, 384, 3,840, 46,080 per round for C=2..6); the blocked/complete ratio grows about 1.6x per client (timed) and 2x per client (untimed). SWIM timed has exactly 2^R complete executions at any N, while its BLOCKED explorations grow about 3x per member at L=0 (2x at L=1) and about 4x to 6x per round; that blocked work is what limits the timed side, whereas the untimed side is limited by its complete executions.", False),
        ("Parallelism: lease and SWIM runs with R>=3 used the rayon 'partitioned' exploration on pinned cores (8 to 64 per run); SWIM runs with R<=2 have too few complete executions to parallelise and ran on one core. Counts are identical to single-threaded exploration (lease C=3 R=2: 2,304 / 3,593 timed and 2,304 / 4,164 untimed in both modes; SWIM N=8 R=4 and N=5 R=8 match across none, shared and partitioned). Timed lease is fastest at 16 to 64 cores; untimed lease at 8 to 16.", False),
        ("Cell selection: the cost of each cell was predicted from small measured runs (log-linear fits for SWIM, the exact count laws plus measured per-execution costs for lease). Cells predicted to fit in the window were run; single-core SWIM cells were also run up to about 1.3x beyond the window so that some DNFs are real observations. Grey cells were not attempted: they are projected to need more than two days on the cores available and are NOT measured DNFs.", False),
        ("Wall times were measured with about 240 jobs sharing the machine (memory bandwidth, hyper-threads), so treat them as indicative; execution counts are machine independent and are the primary metric.", False),
        ("Binaries: built 2026-09-20 from the TraceForge working tree at commit 697fc36 (branch inbox_new_fix) plus the uncommitted shared-pool hang fix in exec_pool.rs; the lease/SWIM timed exact engine and FIFO arrival semantics are as in the v6 fixes. These counts supersede the v6 lease numbers wherever the two overlap: the v6 lease timed rows were produced before the duplicate-execution fix (83 cells differed, all strictly decreased, no verdict changed).", False),
    ]
    for i, (t, bold) in enumerate(lines, 1):
        c = ws.cell(i, 1, t)
        c.alignment = Alignment(wrap_text=True, vertical="top")
        if bold:
            c.font = Font(bold=True, size=13)
        else:
            ws.row_dimensions[i].height = max(30, 15 * (len(t) // 130 + 1))
    ws.column_dimensions["A"].width = 150
