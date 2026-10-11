#!/usr/bin/env python3
"""Build results_v9.xlsx from the v9 campaign (brain05, 9 to 13 Oct 2026).

Inputs (same directory):
  plan_v9.csv      every job of the campaign (cell, side, repetition, arguments, cores)
  results_v9.csv   one row per finished job, written by sweep_v9.py (may be partial)
  status_v9.json   what was running when the files were fetched (optional)
  env_v9.txt       build and machine record written at launch (optional)
  meta/<family>.json  per-protocol metadata: parameter columns, the regime of every cell,
                      glossary, checker settings (optional; a protocol without it gets a
                      plain sheet)
  ../v8/results_v8.csv, ../v8/results_v8_w0A.csv   previous campaign, for the cross-check

Rebuild at any time: unfinished sides show as running / queued. Rebuilding overwrites the
workbook, so do not rebuild after editing it by hand.
"""
import argparse
import collections
import csv
import datetime as dt
import glob
import json
import os
import re
import statistics

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))

ORDER = ["lease_timed", "swim_timed", "sensor_network_timed", "brp_timed", "par_timed", "alternating_bit_timed",
         "chandra_toueg_timed", "nrp_failure_detector_timed", "nisshi_txn_timed", "spanning_tree_timed",
         "yarn_scheduler_timed", "retail_saga_timed", "comm_closed_leader_election", "three_pc_timed",
         "two_pc_timed", "raft_leader_election"]
NAME = {
    "lease_timed": ("Lease", "Lease with TTL and fencing tokens"),
    "swim_timed": ("SWIM", "SWIM failure detector"),
    "sensor_network_timed": ("Sensor network", "Sensor network (SCP 2014)"),
    "brp_timed": ("BRP", "Bounded retransmission protocol"),
    "par_timed": ("PAR", "Positive acknowledgement with retransmission (Bosnacki-Dams)"),
    "alternating_bit_timed": ("ABP", "Alternating bit protocol"),
    "chandra_toueg_timed": ("Chandra-Toueg", "Chandra-Toueg failure detector"),
    "nrp_failure_detector_timed": ("NRP", "NRP failure detector"),
    "nisshi_txn_timed": ("nisshi", "nisshi transactions (KAFKA-17754)"),
    "spanning_tree_timed": ("Spanning tree", "Spanning tree (Perlman)"),
    "yarn_scheduler_timed": ("YARN", "YARN scheduler"),
    "retail_saga_timed": ("Retail saga", "Retail saga"),
    "comm_closed_leader_election": ("Leader election", "Communication-closed leader election"),
    "three_pc_timed": ("3PC", "Three-phase commit"),
    "two_pc_timed": ("2PC", "Two-phase commit"),
    "raft_leader_election": ("Raft", "Raft leader election"),
}

# ---- styles ------------------------------------------------------------------------------------
WHITE_B = Font(bold=True, color="FFFFFF")
BOLD = Font(bold=True)
ITAL = Font(italic=True, color="595959")
F_TITLE = Font(bold=True, size=14)
FILL = {k: PatternFill("solid", fgColor=v) for k, v in dict(
    head="305496", timed="1F4E78", base="7F6000", ratio="375623", setup="404040", param="5B2C6F",
    good="E2EFDA", bad="FCE4D6", grey="EDEDED", safe="E2EFDA", viol="FCE4D6", ctrl="DDEBF7", edge="FFF2CC",
    other="F2F2F2", warn="FFE699", band="F7F9FC").items()}
THIN = Side(style="thin", color="BFBFBF")
BOX = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
WRAP = Alignment(wrap_text=True, vertical="top")
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def inum(x):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def dur(s):
    if s is None:
        return ""
    s = float(s)
    if s < 120:
        return f"{s:.1f} s"
    if s < 7200:
        return f"{s / 60:.1f} min"
    if s < 172800:
        return f"{s / 3600:.1f} h"
    return f"{s / 86400:.2f} d"


def regime_fill(rid):
    rid = (rid or "").lower()
    for suf in ("safe", "viol", "ctrl", "edge"):
        if rid.endswith(suf):
            return FILL[suf]
    return FILL["other"]


# ---- data ----------------------------------------------------------------------------------------
def load(args):
    plan = list(csv.DictReader(open(args.plan)))
    res = list(csv.DictReader(open(args.results))) if os.path.exists(args.results) else []
    # The keeper run only keeps the spin loops alive, and a run the deadline (or the operator)
    # prevented from starting measured nothing: neither is a result.
    never = {r["id"] for r in res if r["status"].startswith(("not run", "skipped")) and not r.get("wall_s")}
    plan = [p for p in plan if p["purpose"] != "keeper" and p["id"] not in never]
    res = [r for r in res if r["purpose"] != "keeper" and r["id"] not in never]
    status = {}
    if os.path.exists(args.status):
        try:
            status = json.load(open(args.status))
        except ValueError:
            status = {}
    meta = {}
    for p in sorted(glob.glob(os.path.join(args.meta, "*.json"))):
        try:
            m = json.load(open(p))
            meta[m["example"]] = m
        except (ValueError, KeyError):
            print("skipping unreadable metadata file", p)
    v8 = {}
    for name in ("results_v8.csv", "results_v8_w0A.csv"):
        p = os.path.join(args.v8, name)
        if os.path.exists(p):
            for r in csv.DictReader(open(p)):
                v8[r["id"][:-2] if name.endswith("w0A.csv") else r["id"]] = r
    return plan, res, status, meta, v8


class SideSum:
    """Everything the workbook shows for one side (timed or baseline) of one cell."""

    def __init__(self, prs, rrs, running, v8):
        self.planned = len(prs)
        self.rows = rrs
        p0 = prs[0]
        self.args, self.cores, self.parallel, self.tier = p0["args"], p0["cores"], p0["parallel"], p0["tier"]
        self.purpose = p0["purpose"]
        self.pool = (inum(p0["cores"]) or 1) > 1
        self.spread = None
        self.viol_range = None
        self.viol_exact = None
        self.twin = None
        self.v8id = re.sub(r"-r\d+$", "", p0["id"])
        fin = [r for r in rrs if r["status"].startswith("finished")]
        self.fin = fin
        self.fire = any("FIRE" in r["status"] for r in fin)
        self.execs = self.blocked = self.explored = self.viol = self.imp = None
        self.counters = {}
        self.wall = self.wall_min = self.wall_max = self.cpu = self.mhz = None
        self.note, self.audit, self.consistent = "", "", True
        run_now = [j for j in running if re.sub(r"-(r\d+|x\d+)$", "", re.sub(r"-x\d+$", "", j["id"])) == self.v8id]
        if fin:
            sig = {(r["execs"], r["blocked"]) for r in fin}
            self.consistent = len(sig) == 1
            r0 = fin[0]
            self.execs, self.blocked, self.explored = inum(r0["execs"]), inum(r0["blocked"]), inum(r0["explored"])
            # Violating executions are counted from output lines (one per violating execution).
            # Workers of a pool write concurrently and two lines can merge, so a pool run can
            # miss a few lines in a million: the largest count of the repetitions is the best one.
            viols = [inum(r["viol_execs"]) for r in fin if inum(r["viol_execs"]) is not None]
            self.viol = max(viols) if viols else None
            self.viol_range = (min(viols), max(viols)) if viols and min(viols) != max(viols) else None
            self.imp = inum(r0["timeline_impossible"])
            try:
                self.counters = json.loads(r0["counters"]) if r0["counters"] else {}
            except ValueError:
                self.counters = {}
            clean = [r for r in fin if r.get("timing_quality") == "clean"]
            use = clean or fin
            walls = [float(r["wall_s"]) for r in use if r["wall_s"]]
            if walls:
                self.wall, self.wall_min, self.wall_max = statistics.median(walls), min(walls), max(walls)
                if len(walls) >= 2 and self.wall > 0:
                    self.spread = (self.wall_max - self.wall_min) / self.wall
            cpus = [float(r["cpu_s"]) for r in use if r["cpu_s"]]
            self.cpu = statistics.median(cpus) if cpus else None
            mhz = [float(r["mhz_avg"]) for r in use if r.get("mhz_avg")]
            self.mhz = statistics.median(mhz) if mhz else None
            self.n_used, self.n_fin, self.n_clean = len(use), len(fin), len(clean)
            flagged = [r for r in fin if r.get("timing_quality") not in ("clean", "", None)]
            if not flagged:
                self.audit = f"clean ({len(clean)})"
            elif clean:
                self.audit = f"{len(clean)} clean, {len(flagged)} flagged and left out"
            else:
                self.audit = "flagged: " + flagged[0]["timing_quality"]
            pending = self.planned - len(rrs)
            if self.fire:
                self.status = "FIRE (stopped at the first violation)"
            elif pending <= 0:
                self.status = "finished"
            else:
                self.status = f"finished ({len(fin)} runs; {pending} more queued or running)"
            late = [r for r in rrs if r["status"].startswith("DNF")]
            if late:
                self.note = f"{len(late)} further repetition(s) stopped at the deadline, not used"
        else:
            self.n_used = self.n_fin = self.n_clean = 0
            dnf = [r for r in rrs if r["status"].startswith("DNF")]
            err = [r for r in rrs if r["status"].startswith("error")]
            other = [r for r in rrs if not r["status"].startswith(("DNF", "error"))]
            if dnf:
                r0 = dnf[0]
                self.status = "DNF"
                self.wall = fnum(r0["wall_s"])
                self.cpu = fnum(r0["cpu_s"])
                self.note = (f"stopped after {dur(self.wall)}"
                             + (f"; {r0['progress']}" if r0["progress"] else "; no progress line")
                             + (f"; {inum(r0['viol_execs']):,} violating executions so far" if inum(r0["viol_execs"]) else ""))
            elif err:
                self.status = err[0]["status"]
                self.note = err[0]["summary"][:200]
            elif other:
                self.status = other[0]["status"]
            elif run_now:
                self.status = f"running ({dur(run_now[0]['elapsed_s'])} so far)"
            else:
                self.status = "queued"
        # cross-check with the previous campaign
        self.v8 = ""
        o = v8.get(self.v8id)
        if self.purpose == "edge":
            self.v8 = "new rung"
        elif o is not None and fin and not self.fire:
            if o["status"] == "finished" and o["execs"] and o["blocked"]:
                same = (inum(o["execs"]), inum(o["blocked"])) == (self.execs, self.blocked)
                self.v8 = "same" if same else f"DIFFERS (v8 {inum(o['execs']) + inum(o['blocked']):,})"
            elif o["status"].startswith("DNF"):
                self.v8 = "new (v8 did not finish)"
        elif o is not None and self.fire:
            self.v8 = "same" if "FIRE" in o["status"] else "DIFFERS (v8 did not fire)"


def merge_twins(cells, meta):
    """Some instances were planned under two cell names, one per side (a timed run and, under a
    name ending in 'ceiling', its baseline run). With the same regime and the same parameter
    values they are one instance: show them as one row."""
    for ex, m in meta.items():
        mc = {c["cell"]: c for c in m.get("cells", [])}
        ident = lambda c: (mc[c].get("regime"), mc[c].get("kind"), json.dumps(mc[c].get("params", {}), sort_keys=True))
        one = collections.defaultdict(lambda: collections.defaultdict(list))
        for (e, cell), sides in cells.items():
            if e == ex and cell in mc and len(sides) == 1:
                one[ident(cell)][next(iter(sides))].append(cell)
        for key, by_side in one.items():
            if len(by_side.get("timed", [])) == 1 and len(by_side.get("baseline", [])) == 1:
                tc, bc = by_side["timed"][0], by_side["baseline"][0]
                b = cells.pop((ex, bc))["baseline"]
                b.twin = bc
                cells[(ex, tc)]["baseline"] = b
    return cells


def vcounter(meta, ex, cell):
    """Key of the protocol counter that counts violating executions: per cell if the metadata
    gives one for the cell, else per protocol, else None."""
    m = meta.get(ex) or {}
    for c in m.get("cells", []):
        if c.get("cell") == cell and c.get("violation_counter"):
            return c["violation_counter"]
    return m.get("violation_counter")


def exact_violations(cells, meta):
    """Where the protocol keeps its own counter of violating executions (metadata field
    violation_counter), a worker-pool side takes its count from that counter: it is the same
    in every repetition, while the line count can fall a few lines short."""
    for (ex, cell), sides in cells.items():
        key = vcounter(meta, ex, cell)
        if not key:
            continue
        for s in sides.values():
            c = s.counters.get(key) if s.fin and not s.fire else None
            if s.pool and c is not None and str(c).isdigit() and s.viol is not None:
                c = int(c)
                if s.viol < c <= s.viol * 1.0001 + 1:
                    s.viol_exact = (s.viol_range or (s.viol, s.viol), c)
                    s.viol = c
                    s.viol_range = None
    return cells


def collect(plan, res, status, v8):
    running = status.get("running", [])
    by_res = collections.defaultdict(list)
    for r in res:
        by_res[(r["example"], r["cell"], r["side"])].append(r)
    cells = collections.OrderedDict()
    for p in plan:
        cells.setdefault((p["example"], p["cell"]), collections.defaultdict(list))[p["side"]].append(p)
    out = collections.OrderedDict()
    for (ex, cell), sides in cells.items():
        out[(ex, cell)] = {s: SideSum(prs, by_res.get((ex, cell, s), []), running, v8) for s, prs in sides.items()}
    return out


# ---- sheet helpers ---------------------------------------------------------------------------------
def put(ws, row, col, value, font=None, fill=None, fmt=None, align=None, border=True):
    c = ws.cell(row, col, value)
    if font:
        c.font = font
    if fill:
        c.fill = fill
    if fmt:
        c.number_format = fmt
    c.alignment = align or WRAP
    if border:
        c.border = BOX
    return c


def text_sheet(wb, title, lines, width=150):
    ws = wb.create_sheet(title)
    ws.column_dimensions["A"].width = width
    for i, l in enumerate(lines, 1):
        if isinstance(l, tuple):
            c = ws.cell(i, 1, l[1])
            c.font = F_TITLE if l[0] == "title" else BOLD
        else:
            c = ws.cell(i, 1, l)
        c.alignment = WRAP
    return ws


SIDE_COLS = ["Status", "Complete executions", "Blocked executions", "Explored (complete + blocked)",
             "Violating executions", "Wall time, s (median)", "Wall min, s", "Wall max, s", "CPU time, s", "Runs used"]
SIDE_W = [20, 14, 14, 16, 13, 12, 10, 10, 12, 7]
SIDE_FMT = [None, "#,##0", "#,##0", "#,##0", "#,##0", "#,##0.0", "#,##0.0", "#,##0.0", "#,##0.0", "0"]


def side_values(s):
    if s is None:
        return ["not planned"] + [None] * 9
    if s.fin and not s.fire:
        return [s.status, s.execs, s.blocked, s.explored, s.viol, s.wall, s.wall_min, s.wall_max, s.cpu, s.n_used]
    if s.fire:
        return [s.status, None, None, None, None, s.wall, s.wall_min, s.wall_max, s.cpu, s.n_used]
    if s.status == "DNF":
        return [f"DNF (stopped after {dur(s.wall)})" if s.wall else "DNF", None, None, None, None, None, None, None, s.cpu, None]
    return [s.status] + [None] * 9


def counters_text(s, keys):
    if s is None or not s.counters:
        return ""
    ks = [k for k in keys if k in s.counters] or sorted(s.counters)
    return ", ".join(f"{k}={int(s.counters[k]):,}" if str(s.counters[k]).isdigit() else f"{k}={s.counters[k]}" for k in ks)


def ratio(a, b):
    return round(a / b, 2) if a and b else None


def protocol_sheet(wb, ex, rows, m, used_titles):
    short, long_ = NAME.get(ex, (ex, ex))
    title = short[:31]
    ws = wb.create_sheet(title)
    used_titles[ex] = title
    pcols = (m or {}).get("param_columns", [])
    ckeys = [c["key"] for c in (m or {}).get("counters", [])]
    mcells = {c["cell"]: c for c in (m or {}).get("cells", [])}
    rorder = {r["id"]: i for i, r in enumerate((m or {}).get("regimes", []))}
    # ---- title block
    ws.cell(1, 1, (m or {}).get("title", long_)).font = F_TITLE
    info = []
    if m:
        info = [("Checks", m.get("property", "")), ("Source", m.get("source", "")),
                ("Timing rule", m.get("boundary_law") or "none stated"),
                ("Use in the paper", f"{m.get('citable', '')}: {m.get('citable_note', '')}"),
                ("Timed settings", m.get("config_timed", "")),
                ("Baseline settings", m.get("config_baseline", ""))]
    else:
        info = [("Note", "No metadata file for this protocol yet: parameter and regime columns are missing, the cell name carries them.")]
    for i, (k, v) in enumerate(info, 2):
        ws.cell(i, 1, k).font = BOLD
        c = ws.cell(i, 2, v)
        c.alignment = Alignment(wrap_text=False, vertical="top")
    top = len(info) + 3
    # ---- header
    heads = [("Regime", 11, "param")] + [(c["header"], max(8, min(16, len(c["header"]) + 2)), "param") for c in pcols]
    heads += [("Row kind", 10, "param"), ("Cell (what this row is)", 34, "param")]
    n_lead = len(heads)
    heads += [(h, w, "timed") for h, w in zip(SIDE_COLS, SIDE_W)]
    heads += [(h, w, "base") for h, w in zip(SIDE_COLS, SIDE_W)]
    heads += [("Explored ratio (baseline / timed)", 11, "ratio"), ("Wall ratio (baseline / timed)", 11, "ratio"),
              ("CPU ratio (baseline / timed)", 11, "ratio"),
              ("Cores per run (timed | baseline when they differ)", 8, "setup"), ("Parallel mode", 11, "setup"), ("Runs planned (timed | baseline)", 9, "setup"),
              ("Timing audit (timed | baseline)", 26, "setup"), ("Mean clock, MHz (timed | baseline)", 12, "setup"),
              ("Protocol counters, timed", 40, "setup"), ("Protocol counters, baseline", 40, "setup"),
              ("Same counts as v8 (timed | baseline)", 20, "setup"), ("Notes", 50, "setup"),
              ("Arguments (of the timed run when the row has one; the baseline run is the same line with --mode baseline unless Notes says otherwise)", 110, "setup")]
    groups = [("Instance", 1, n_lead, "param"), ("Timed (with the clock)", n_lead + 1, n_lead + 10, "timed"),
              ("Baseline (Must, same program without the clock)", n_lead + 11, n_lead + 20, "base"),
              ("Baseline / timed", n_lead + 21, n_lead + 23, "ratio"), ("Run setup, checks, reproduction", n_lead + 24, len(heads), "setup")]
    for name, a, b, key in groups:
        ws.merge_cells(start_row=top, start_column=a, end_row=top, end_column=b)
        put(ws, top, a, name, WHITE_B, FILL[key], align=CENTER)
        for c in range(a, b + 1):
            ws.cell(top, c).fill = FILL[key]
    for i, (h, w, key) in enumerate(heads, 1):
        put(ws, top + 1, i, h, WHITE_B, FILL[key], align=CENTER)
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.row_dimensions[top + 1].height = 62
    ws.freeze_panes = ws.cell(top + 2, n_lead + 1)

    # ---- rows
    def sort_key(item):
        (cell, sides) = item
        mc = mcells.get(cell, {})
        ps = mc.get("params", {})
        size = [ps.get(c["key"]) for c in pcols if c.get("size")]
        size = [x if isinstance(x, (int, float)) else 10 ** 9 for x in size]
        any_side = sides.get("timed") or sides.get("baseline")
        fired = any(x.fire for x in sides.values())      # rows that stopped at a first violation go last in their regime
        return (rorder.get(mc.get("regime"), 999), fired, size, inum(any_side.cores) or 0, cell)

    r = top + 2
    for cell, sides in sorted(rows, key=sort_key):
        t, b = sides.get("timed"), sides.get("baseline")
        mc = mcells.get(cell, {})
        ps = mc.get("params", {})
        any_side = t or b
        put(ws, r, 1, mc.get("regime", ""), BOLD, regime_fill(mc.get("regime")))
        for i, c in enumerate(pcols, 2):
            put(ws, r, i, ps.get(c["key"], ""))
        put(ws, r, n_lead - 1, KIND.get(any_side.purpose, any_side.purpose))
        put(ws, r, n_lead, mc.get("label") or cell)
        col = n_lead + 1
        for s in (t, b):
            for v, fmt in zip(side_values(s), SIDE_FMT):
                c = put(ws, r, col, v, fmt=fmt)
                if s is not None and s.status in ("DNF",) or (s is not None and s.status.startswith(("running", "queued"))):
                    c.fill = FILL["grey"]
                col += 1
        if t is not None and b is not None and t.fin and b.fin and not t.fire and not b.fire:
            if t.viol == 0 and (b.viol or 0) > 0:
                ws.cell(r, n_lead + 5).fill = FILL["good"]
                ws.cell(r, n_lead + 15).fill = FILL["bad"]
            elif (t.viol or 0) > 0:
                ws.cell(r, n_lead + 5).fill = FILL["bad"]
            put(ws, r, col, ratio(b.explored, t.explored), fmt="0.00")
            timeable = (t.wall or 0) >= 0.05 and (b.wall or 0) >= 0.05
            c = put(ws, r, col + 1, ratio(b.wall, t.wall) if timeable else None, fmt="0.00")
            if timeable and max(t.spread or 0, b.spread or 0) > SPREAD_WARN:
                c.fill = FILL["warn"]
            put(ws, r, col + 2, ratio(b.cpu, t.cpu) if timeable else None, fmt="0.00")
        else:
            put(ws, r, col, None)
            put(ws, r, col + 1, None)
            put(ws, r, col + 2, None)
        col += 3
        core_set = [s.cores for s in (t, b) if s is not None]
        put(ws, r, col, inum(any_side.cores) if len(set(core_set)) == 1 else " | ".join(core_set))
        put(ws, r, col + 1, any_side.parallel)
        put(ws, r, col + 2, " | ".join(str(s.planned) for s in (t, b) if s is not None))
        put(ws, r, col + 3, " | ".join(s.audit for s in (t, b) if s is not None and s.audit))
        put(ws, r, col + 4, " | ".join(f"{s.mhz:.0f}" if s.mhz else "-" for s in (t, b) if s is not None))
        col += 1
        put(ws, r, col + 4, counters_text(t, ckeys))
        put(ws, r, col + 5, counters_text(b, ckeys))
        put(ws, r, col + 6, " | ".join((s.v8 or "-") for s in (t, b) if s is not None))
        notes = []
        if mc.get("note"):
            notes.append(mc["note"])
        for nm, s in (("timed", t), ("baseline", b)):
            if s is None:
                continue
            if s.note:
                notes.append(f"{nm}: {s.note}")
            if s.twin:
                notes.append(f"{nm}: planned under the cell name '{s.twin}'")
            if s.fin and not s.fire and (s.spread or 0) > SPREAD_WARN and (s.wall or 0) >= 5:
                notes.append(f"{nm}: wall time varies from {s.wall_min:,.1f} to {s.wall_max:,.1f} s over {s.n_used} runs"
                             + (" (worker pool: the share of work between the workers differs from run to run; "
                                "compare CPU time or quote the range)" if s.pool else ""))
            if s.viol_exact:
                (lo, hi), c = s.viol_exact
                notes.append(f"{nm}: violating executions taken from the program's own counter ({c:,}); the line count of the "
                             f"worker-pool runs gave {lo:,}" + (f" to {hi:,}" if hi != lo else ""))
            if s.viol_range:
                notes.append(f"{nm}: the line count of violating executions ranges from {s.viol_range[0]:,} to {s.viol_range[1]:,} "
                             "over the repetitions (merged output lines of the worker pool); the largest is shown")
            if not s.consistent:
                notes.append(f"{nm}: COUNTS DIFFER BETWEEN REPETITIONS")
            if s.imp:
                notes.append(f"{nm}: timeline_impossible={s.imp}")
        if t is not None and b is not None and re.sub(r"--mode\s+\S+", "", t.args).split() != re.sub(r"--mode\s+\S+", "", b.args).split():
            notes.append("the two sides do NOT run the same arguments; baseline: " + b.args)
        put(ws, r, col + 7, "; ".join(notes))
        put(ws, r, col + 8, (t or b).args, align=Alignment(wrap_text=False, vertical="top"))
        r += 1
    ws.auto_filter.ref = f"A{top + 1}:{get_column_letter(len(heads))}{max(r - 1, top + 1)}"
    return ws


KIND = {"headline": "headline", "ladder": "ladder", "boundary": "boundary", "ceiling": "ceiling", "edge": "edge rung"}
SPREAD_WARN = 0.10      # (max - min) / median of the repeated wall times above which a row is flagged


# ---- the edge -----------------------------------------------------------------------------------------
def edge_rows(ex, rows, m):
    """Ladders along every size parameter: the largest rung each side finished, the first it did not."""
    out = []
    pcols = m.get("param_columns", [])
    size_keys = [c["key"] for c in pcols if c.get("size")]
    mcells = {c["cell"]: c for c in m.get("cells", [])}
    items = []
    for cell, sides in rows:
        mc = mcells.get(cell)
        if not mc:
            continue
        # a run without keep-going that ends normally explored everything; only a run that
        # actually stopped at a violation (status FIRE) is not a rung, and that is checked per side
        items.append((mc, sides))
    def follows(c):
        """Columns that do not define the instance family: values derived from the size, and the pool size."""
        return bool(c.get("moves_with_size")) or bool(re.search(r"worker|pool|cores", c["key"] + " " + c.get("header", ""), re.I))

    for k in size_keys:
        ladders = collections.defaultdict(list)
        for mc, sides in items:
            ps = mc.get("params", {})
            if not isinstance(ps.get(k), (int, float)):
                continue
            fixed = tuple((c["key"], str(ps.get(c["key"]))) for c in pcols if c["key"] != k and not follows(c))
            ladders[(mc["regime"], fixed)].append((ps[k], sides))
        for (regime, fixed), rungs in ladders.items():
            vals = sorted({v for v, _ in rungs})
            if len(vals) < 2:
                continue
            row = dict(regime=regime, fixed=", ".join(f"{a}={b}" for a, b in fixed if b not in ("-", "")), key=k,
                       rungs=", ".join(str(v) for v in vals))
            for side in ("timed", "baseline"):
                done = [(v, s[side]) for v, s in rungs if side in s and s[side].fin and not s[side].fire]
                open_ = [(v, s[side]) for v, s in rungs if side in s and not s[side].fin]
                if done:
                    v, s = max(done, key=lambda z: (z[0], -(z[1].wall or 0)))
                    row[side + "_last"] = f"{k}={v}: {s.explored:,} explored in {dur(s.wall)} on {s.cores} core(s)"
                    row[side + "_last_v"] = v
                else:
                    row[side + "_last"] = "none finished yet"
                    row[side + "_last_v"] = None
                beyond = [(v, s) for v, s in open_ if row[side + "_last_v"] is None or v > row[side + "_last_v"]]
                if beyond:
                    v, s = min(beyond, key=lambda z: z[0])
                    row[side + "_next"] = f"{k}={v}: {s.status}" + (f" ({s.note})" if s.note else "")
                else:
                    row[side + "_next"] = ""
            out.append(row)
    return out


# ---- workbook ---------------------------------------------------------------------------------------------
GLOSSARY = [
    ("Timed", "The run with the clock (--mode timed): the checker explores only executions that admit a timeline under the stated bounds (transit time in [L, U], storage sd, timeouts W)."),
    ("Baseline", "The same program, same arguments, run with --mode baseline: plain Must, no clock. It explores every execution the program allows when time is ignored."),
    ("Cell", "One instance of a protocol: one row of a protocol sheet. Both sides of a cell run the same command line except the mode word."),
    ("Regime", "A named setting of the timing parameters together with the side of the protocol's timing rule it lies on. Suffix -safe: real time rules the violation out, the timed side must report 0 violations (the baseline may report spurious ones). Suffix -viol: a real timing violation exists, the timed side must report violations. Suffix -edge: exactly on the boundary. Suffix -ctrl: control, time adds no information and the two sides must agree. Every regime is defined on the sheet 'Regimes'."),
    ("Complete executions (execs)", "Explored executions in which every thread ran to its end."),
    ("Edge rung / The edge", "The edge of a protocol is the largest instance (nodes, clients, rounds, messages, ...) a side finishes inside the campaign window of 89 hours. The sheet 'The edge' lists, per ladder, the largest rung each side finished and the first it did not."),
    ("Blocked executions", "Explored executions that ended early: a thread waits on a receive that can never be served (for example its message was lost), an assume failed, or, in an exhaustive run, an assertion failed."),
    ("Explored", "Complete + blocked executions: the number of execution graphs the checker visited. This is the machine-independent cost measure."),
    ("Violating executions", "Executions in which an assertion of the model failed. Counted from the checker's own output (one line per violating execution in an exhaustive run), so it means the same thing for every protocol. In most protocols a violating execution ends at the failed assertion and is counted among the blocked ones; where the model records the violation and lets the run go on to its end (Chandra-Toueg, sensor network), it is counted among the complete ones. In a worker-pool run two workers writing at the same instant can merge two lines, so the count can fall short by a few per million; the workbook then shows the protocol's own counter where it counts violating executions, else the largest count of the repetitions, and the Notes column says so. Sequential runs are exact."),
    ("Row kind", "headline: a cell chosen for the paper's main table. ladder: one rung of a series that grows one size parameter. boundary: a cell next to the protocol's timing rule, on one side or the other. ceiling: a cell that was not expected to finish; it documents where a side stops. edge rung: one rung above the top of a ladder, added on 11 Oct to find the largest instance that finishes inside the campaign window."),
    ("Protocol counters", "Counters the example program keeps itself and prints at the end (for example stale writes, dead declarations, dirty reads). Some count events, not executions, so they can exceed the number of executions; the Glossary rows of each protocol say which."),
    ("Exhaustive run (keep-going)", "The checker records a violation and continues, so each side explores its whole space and the violations are counted. All rows are exhaustive unless the status says FIRE."),
    ("FIRE (stopped at the first violation)", "A run without keep-going: the checker stops at the first certified violation (exit code 101) and prints a witness timeline. Used to certify that a violation is real; its wall time depends on the search order and is not a cost measure."),
    ("DNF", "Did not finish: the run was still going at the deadline and was stopped. The Notes column gives how long it ran and how many executions it had explored."),
    ("Wall time", "Elapsed time of the checker process alone (/usr/bin/time), in seconds, measured with every physical core of the machine busy and the second hyper-thread of every core idle. Median over the runs used; min and max next to it. Sequential runs (one core) repeat within a few percent. Worker-pool runs do not: the same cell can take 1.3 to 2.5 times longer in one repetition than in the next, with the same CPU time, because the pool shares the work differently each time. For a pool row quote the range or the CPU time; a yellow wall ratio marks a row whose repetitions spread by more than 10%."),
    ("CPU ratio", "Baseline CPU time divided by timed CPU time. For worker-pool rows it is steadier than the wall ratio."),
    ("Mean clock, MHz", "Mean clock frequency of the run's cores while it ran. Runs of one cell should show similar values; a lower value on a pool run means some of its cores sat idle."),
    ("CPU time", "User + system CPU seconds of the process (median). Equal to the wall time for a sequential run that had its core for itself."),
    ("Runs used", "Number of repetitions behind the median. Timing-grade cells (wall up to one hour) are run three times, in three passes hours apart, and seven times when they use a worker pool; a run that failed the timing audit is repeated and left out. Longer cells (reach-grade) are run once, and a second time if the campaign window allowed it."),
    ("Timing audit", "Each run is watched every 10 s: clock frequency of its cores, load on its sibling hyper-threads, foreign load on the machine, and cpu/wall at the end. 'clean' means nothing disturbed it."),
    ("Cores per run / Parallel mode", "Number of physical cores the run was pinned to, and the exploration mode: none (sequential), shared (shared work queue), partitioned (rayon), workers (the example's own --workers pool). Both sides of a cell use the same."),
    ("Explored ratio, Wall ratio", "Baseline divided by timed. Above 1: the timed side explored less (or was faster). Below 1: the timed side explored more (or was slower), which happens when time adds precision but no pruning."),
    ("Same counts as v8", "Whether complete and blocked executions are identical to the 30 Sep to 2 Oct campaign (same arguments, earlier build). 'new' marks a side that did not finish there."),
    ("L, U", "Lower and upper bound on the transit time of a message, in ticks."),
    ("sd", "Storage duration: how long a delivered message stays available to the receiver after it arrived."),
    ("W (timeout)", "How long a timed receive waits before it gives up, in ticks."),
    ("Lossy sends", "Every send may be lost, in both modes. A lost message is one reason for a blocked execution."),
    ("timeline_impossible", "Endings the checker completed and then found to admit no timeline. Must be 0 with the current algorithm; the Checks sheet verifies it."),
]


def build(args):
    plan, res, status, meta, v8 = load(args)
    cells = exact_violations(merge_twins(collect(plan, res, status, v8), meta), meta)
    by_ex = collections.defaultdict(list)
    for (ex, cell), sides in cells.items():
        by_ex[ex].append((cell, sides))
    exs = [e for e in ORDER if e in by_ex] + [e for e in by_ex if e not in ORDER]
    env = open(args.env).read().splitlines() if os.path.exists(args.env) else []
    wb = Workbook()
    wb.remove(wb.active)
    fin_rows = [r for r in res if r["status"].startswith("finished")]
    sides_all = [s for sides in cells.values() for s in sides.values()]
    n_fin = sum(1 for s in sides_all if s.fin and len(s.rows) >= s.planned)
    n_part = sum(1 for s in sides_all if s.fin and len(s.rows) < s.planned)
    n_dnf = sum(1 for s in sides_all if s.status == "DNF")
    n_open = len(sides_all) - n_fin - n_part - n_dnf
    state = (f"{len(cells)} cells, {len(sides_all)} sides: {n_fin} finished, {n_part} partly measured, {n_dnf} did not finish, "
             f"{n_open} running or queued. {len(res)} of {len(plan)} planned runs recorded.")

    # ---- Read me
    lines = [("title", "TraceForge v9 results: timed checker against the baseline (Must), brain05, 9 to 13 Oct 2026"),
             f"Built {dt.datetime.now().strftime('%Y-%m-%d %H:%M')} from results_v9.csv. State: {state}",
             "",
             ("h", "What is in this workbook"),
             "One sheet per protocol. One row per cell (one instance). Columns from left to right: the regime, the protocol's own parameters, the timed side, the baseline side, the two ratios, how it was run, the checks, and in the LAST column the exact arguments.",
             "Sheet 'Summary': one line per protocol: how many cells finished on which side, the largest instance both sides finished, and the largest pruning ratio.",
             "Sheet 'The edge': for every size parameter (nodes, clients, rounds, ballots, messages, ...) the largest instance each side finished inside the campaign window (9 Oct 15:14 to 13 Oct 08:00, 89 hours), and the first one it did not finish.",
             "Sheet 'Regimes': what every regime id means, with its parameter values, the rule that puts it on its side of the boundary, and what each side is expected to report.",
             "Sheet 'Glossary': every term and every protocol parameter.",
             "Sheet 'Settings': machine, build, checker configuration of every protocol in both modes, timing method, and how to reproduce a row.",
             "Sheet 'Checks': automatic consistency checks on the data (repetitions agree, exit codes, comparison with the previous campaign, timing audit).",
             "Sheet 'All runs': every single run (each repetition) with its raw measurements.",
             "",
             ("h", "How to read a row"),
             "Violating executions: timed 0 with baseline above 0 (green / orange) means the baseline reports violations that real time rules out. Timed above 0 is a real violation in that regime.",
             "Explored = complete + blocked executions. Ratios are baseline / timed.",
             "Wall times are medians of clean repetitions on a fully busy machine (see Settings). Grey cells: not finished (DNF, running or queued).",
             "Which wall times to trust: a sequential row (Cores per run = 1) repeats within a few percent. A worker-pool row can vary a lot between repetitions with the same CPU time; its wall ratio is yellow when the repetitions spread by more than 10%, and the Notes column gives the range. The sheet 'Checks' has the numbers.",
             "",
             ("h", "Status"),
             state]
    text_sheet(wb, "Read me", lines)

    # ---- Summary
    ws = wb.create_sheet("Summary")
    heads = ["Protocol", "Use in the paper", "Cells", "Both sides finished", "Only the timed side finished", "Only the baseline finished",
             "Cells where the baseline reports violations and the timed side none",
             "Largest instance both sides finished (by baseline explored)", "Timed explored", "Baseline explored",
             "Explored ratio (baseline / timed)", "Timed wall, s", "Baseline wall, s", "Wall ratio (baseline / timed)",
             "Timed violating executions", "Baseline violating executions",
             "Largest explored ratio in the protocol", "in cell"]
    widths = [18, 14, 7, 9, 10, 10, 14, 52, 14, 14, 10, 11, 11, 10, 12, 12, 11, 52]
    for i, (h, w) in enumerate(zip(heads, widths), 1):
        put(ws, 1, i, h, WHITE_B, FILL["head"], align=CENTER)
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.row_dimensions[1].height = 75
    ws.freeze_panes = "C2"
    r = 2
    for ex in exs:
        both, t_only, b_only, prec = [], 0, 0, 0
        for cell, sides in by_ex[ex]:
            t, b = sides.get("timed"), sides.get("baseline")
            if t is None or b is None:
                continue
            tf, bf = bool(t.fin) and not t.fire, bool(b.fin) and not b.fire
            if tf and bf:
                both.append((cell, t, b))
                if t.viol == 0 and (b.viol or 0) > 0:
                    prec += 1
            elif tf and b.status == "DNF":
                t_only += 1
            elif bf and t.status == "DNF":
                b_only += 1
        label = lambda cell: ({c["cell"]: c for c in (meta.get(ex) or {}).get("cells", [])}.get(cell, {}).get("label") or cell)
        vals = [NAME.get(ex, (ex,))[0], (meta.get(ex) or {}).get("citable", ""), len(by_ex[ex]), len(both), t_only, b_only, prec]
        if both:
            cell, t, b = max(both, key=lambda z: z[2].explored or 0)
            vals += [label(cell), t.explored, b.explored, ratio(b.explored, t.explored), t.wall, b.wall,
                     ratio(b.wall, t.wall) if (t.wall or 0) >= 0.05 else None, t.viol, b.viol]
            c2, t2, b2 = max(both, key=lambda z: (z[2].explored or 0) / max(z[1].explored or 1, 1))
            vals += [ratio(b2.explored, t2.explored), label(c2)]
        else:
            vals += [None] * 11
        fmts = [None, None, "0", "0", "0", "0", "0", None, "#,##0", "#,##0", "0.00", "#,##0.0", "#,##0.0", "0.00", "#,##0", "#,##0", "0.00", None]
        for i, (v, f) in enumerate(zip(vals, fmts), 1):
            c = put(ws, r, i, v, fmt=f, font=BOLD if i == 1 else None)
            if i == 2:
                c.fill = FILL["good"] if v == "yes" else FILL["bad"] if v == "no" else FILL["edge"]
        r += 1
    ws.cell(r + 1, 1, "Exhaustive cells only. 'Explored' = complete + blocked executions. Ratios above 1: the timed side explored less or was faster. "
                      "'Use in the paper' is the verdict of the protocol's sheet (row 'Use in the paper' at its top gives the reasons).").alignment = Alignment(wrap_text=False)

    # ---- The edge
    ws = wb.create_sheet("The edge")
    heads = ["Protocol", "Regime", "Other parameters (fixed)", "Size parameter", "Values run",
             "Timed: largest finished", "Timed: first not finished", "Baseline: largest finished", "Baseline: first not finished"]
    widths = [18, 11, 44, 9, 22, 46, 60, 46, 60]
    for i, (h, w) in enumerate(zip(heads, widths), 1):
        put(ws, 1, i, h, WHITE_B, FILL["head"], align=CENTER)
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    r = 2
    for ex in exs:
        m = meta.get(ex)
        if not m:
            continue
        for row in edge_rows(ex, by_ex[ex], m):
            vals = [NAME.get(ex, (ex,))[0], row["regime"], row["fixed"], row["key"], row["rungs"],
                    row["timed_last"], row["timed_next"], row["baseline_last"], row["baseline_next"]]
            for i, v in enumerate(vals, 1):
                c = put(ws, r, i, v)
                if i == 2:
                    c.fill = regime_fill(row["regime"])
            r += 1
    if not meta:
        ws.cell(2, 1, "Needs the per-protocol metadata (meta/*.json) to know which parameters are sizes.")
    ws.cell(r + 1, 1, "A ladder = cells of one regime that differ only in one size parameter. Protocols without metadata, and "
                      "size parameters with a single value, do not appear.").alignment = Alignment(wrap_text=False)

    # ---- protocol sheets
    used = {}
    for ex in exs:
        protocol_sheet(wb, ex, by_ex[ex], meta.get(ex), used)

    # ---- Regimes
    ws = wb.create_sheet("Regimes")
    heads = ["Protocol", "Regime", "Name", "Parameter values", "Rule (why it is on this side)", "Timed side must report", "Baseline may report", "Cells"]
    widths = [18, 11, 34, 40, 60, 34, 40, 7]
    for i, (h, w) in enumerate(zip(heads, widths), 1):
        put(ws, 1, i, h, WHITE_B, FILL["head"], align=CENTER)
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    r = 2
    for ex in exs:
        m = meta.get(ex)
        if not m:
            continue
        cnt = collections.Counter(c.get("regime") for c in m.get("cells", []))
        for g in m.get("regimes", []):
            vals = [NAME.get(ex, (ex,))[0], g.get("id"), g.get("name"), g.get("params"), g.get("rule"),
                    g.get("expect_timed"), g.get("expect_baseline"), cnt.get(g.get("id"), 0)]
            for i, v in enumerate(vals, 1):
                c = put(ws, r, i, v)
                if i == 2:
                    c.fill = regime_fill(g.get("id"))
                    c.font = BOLD
            r += 1

    # ---- Glossary
    ws = wb.create_sheet("Glossary")
    for i, (h, w) in enumerate(zip(["Term", "Where", "Meaning"], [34, 18, 130]), 1):
        put(ws, 1, i, h, WHITE_B, FILL["head"], align=CENTER)
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    r = 2
    for term, meaning in GLOSSARY:
        put(ws, r, 1, term, BOLD)
        put(ws, r, 2, "all sheets")
        put(ws, r, 3, meaning)
        r += 1
    for ex in exs:
        m = meta.get(ex)
        if not m:
            continue
        seen = set()
        for c in m.get("param_columns", []):
            put(ws, r, 1, c["header"], BOLD)
            put(ws, r, 2, NAME.get(ex, (ex,))[0])
            put(ws, r, 3, c.get("meaning", "") + ("  (size parameter)" if c.get("size") else ""))
            seen.add(c["header"])
            seen.add(c["key"])
            r += 1
        for g in m.get("glossary", []):
            if g.get("term") in seen:
                continue
            put(ws, r, 1, g.get("term"), BOLD)
            put(ws, r, 2, NAME.get(ex, (ex,))[0])
            put(ws, r, 3, g.get("meaning"))
            r += 1
        for c in m.get("counters", []):
            put(ws, r, 1, f"counter {c.get('key')}", BOLD)
            put(ws, r, 2, NAME.get(ex, (ex,))[0])
            put(ws, r, 3, f"counts {c.get('counts', '?')}: {c.get('meaning', '')}")
            r += 1

    # ---- Settings
    lines = [("title", "Settings and reproduction"),
             ("h", "Machine and build (recorded at launch on brain05)")] + env + ["",
             ("h", "Checker settings common to all protocols"),
             "Consistency model: FIFO per channel (the default of every example; none sets another one).",
             "Every send is lossy in both modes (every example builds its configuration with with_all_sends_lossy()).",
             "Timed mode adds the clock: with_timed(L, U, sd) and, where a protocol has its own channel delays, per-send transit bounds; the values are in the parameter columns and in the per-protocol lines below. Baseline mode is the same program and configuration without it.",
             "Exhaustive rows use with_keep_going_after_error (flag --keep-going where the example has it); FIRE rows do not.",
             "The search order of nondet() choices is seeded at random per process; exhaustive counts do not depend on it (the repetitions agree, see Checks). Only the time to a first violation does.",
             "",
             ("h", "How wall times were measured"),
             "Each run is one process pinned with taskset to its own physical cores (logical CPUs 2-63 and 66-127). The second hyper-thread of every core (CPUs 130-191, 194-255) is never used. The scheduler and the output filters run on cores 0, 1, 64, 65.",
             "While a timing-grade cell is queued or running, every free measurement core runs a spin loop, so the clock frequency does not depend on how full the queue is. Measured beforehand with one 14.3 s job: sibling hyper-thread busy +55%; one socket busy +21% (3.34 to 2.77 GHz); all 124 cores busy 17.45 s with 16.8 to 17.6 across cores.",
             "Wall, CPU and peak memory come from /usr/bin/time around the binary alone. Each run's output goes through a filter (on the service cores) that counts the failing-task lines.",
             "Timing-grade cells (wall up to one hour in v8): three repetitions, hours apart; the workbook shows the median, min and max of the clean ones. A run that fails the audit is repeated up to twice. Reach-grade cells: one run.",
             "Added on 11 Oct (campaign hour 44), when the three repetitions showed that sequential cells repeat within a few percent but worker-pool cells do not: four more repetitions (4 to 7) of every multi-core timing-grade side of 5 s or more; a second run of every reach-grade cell of at most 8 h, as far as the window allowed; and one run that cannot finish (the keeper), whose only purpose is to keep the spin loops on the free cores until the deadline, so that late runs see the same clock frequency as early ones. The keeper is not a result.",
             "Edge rungs (row kind 'edge rung') were added the same day: one size step above the top of a ladder, with the top rung's arguments. Where the ladder is sequential and the example has a worker pool, the edge rung uses the pool (the cores column says so); its counts are comparable with the ladder, its wall time is not.",
             "Cores of a multi-core run that the shared pool leaves idle are not filled, so 'all cores busy' is approximate for the runs next to them; every run records the mean frequency of its cores and the machine occupancy (sheet All runs).",
             "",
             ("h", "Reproduce a row"),
             "1. git checkout b67ccf7 (branch nissih; the tool code was last changed in e96fe7a). Use the Cargo.lock whose md5 is in the build record above (it is not tracked by git; a copy is in benchmarks/v9/Cargo.lock.v9).",
             "2. cargo build --release --examples --locked     (cargo and rustc 1.97.1)",
             "3. TF_PRINT_STATS=1 taskset -c <one physical core per thread> /usr/bin/time target/release/examples/<example> <arguments of the last column>",
             "   For the baseline side replace '--mode timed' by '--mode baseline'. For a multi-core row also set RAYON_NUM_THREADS=<cores>.",
             "4. The line 'TFSTATS execs=.. blocked=..' gives complete and blocked executions; the number of lines naming a failing task (task-N or main-thread-ThreadId(N)) gives the violating executions.",
             "   The scheduler also sets TF_PROGRESS=100000 (a progress line every 100,000 executions) and limits the address space to 128 GB; neither changes a count.",
             "The whole campaign: benchmarks/v9/plan_v9.csv (every job) with benchmarks/v9/sweep_v9.py (scheduler), launch_v9.sh, run_v9.sh. plan_v9_launch.csv is the plan as launched on 9 Oct (make_plan_from_v8.py); make_edge_plan.py and make_reps_plan.py produced the rows added on 11 Oct.",
             "",
             ("h", "Checker configuration per protocol (from each example's build_config)")]
    for ex in exs:
        m = meta.get(ex)
        if not m:
            continue
        lines += [("h", f"{NAME.get(ex, (ex,))[0]}  ({ex})"),
                  "timed: " + str(m.get("config_timed", "")),
                  "baseline: " + str(m.get("config_baseline", "")),
                  "program differs between the modes: " + str(m.get("program_differs_between_modes", "")),
                  "failing tasks: " + str(m.get("failing_tasks", ""))]
    text_sheet(wb, "Settings", lines, width=190)

    # ---- Checks
    chk = [("title", "Automatic checks on the data"), state, ""]
    rep = [(k, s) for k, sides in cells.items() for s in sides.values() if len(s.fin) >= 2]
    bad = [(k, s) for k, s in rep if not s.consistent]
    chk.append(f"{'PASS' if not bad else 'FAIL'}  Repetitions agree: {len(rep) - len(bad)} of {len(rep)} sides with two or more finished runs have identical complete and blocked counts.")
    chk += [f"      differs: {k[0]} | {k[1]}" for k, s in bad[:40]]
    wob = [(k, s) for k, s in rep if s.viol_range or (s.viol_exact and s.viol_exact[0][0] != s.viol_exact[0][1])]
    for k, s in wob:
        if not s.viol_range:
            s.viol_range_chk = s.viol_exact[0]
        else:
            s.viol_range_chk = s.viol_range
    wob_seq = [(k, s) for k, s in wob if not s.pool]
    worst = max(((s.viol_range_chk[1] - s.viol_range_chk[0]) / s.viol_range_chk[1] for k, s in wob), default=0)
    chk.append(f"{'PASS' if not wob_seq else 'FAIL'}  Violating executions (counted from output lines) are identical over the repetitions in "
               f"{len(rep) - len(wob)} of {len(rep)} sides. The other {len(wob)} are all worker-pool runs with at least "
               f"{min((s.viol_range_chk[1] for k, s in wob), default=0):,} violating executions; they differ by at most "
               f"{1e6 * worst:.0f} per million (two workers writing at once can merge two lines). The workbook shows the protocol's "
               f"own counter where it counts violating executions, else the largest line count.")
    chk += [f"      SEQUENTIAL side differs: {k[0]} | {k[1]}" for k, s in wob_seq[:40]]
    imp = [r for r in fin_rows if r["timeline_impossible"] not in ("", "0")]
    chk.append(f"{'PASS' if not imp else 'FAIL'}  timeline_impossible = 0 in {len(fin_rows) - len(imp)} of {len(fin_rows)} finished runs.")
    unexp = [r for r in res if r.get("as_expected") == "NO"]
    chk.append(f"{'PASS' if not unexp else 'FAIL'}  Exit codes as in v8: {sum(1 for r in res if r.get('as_expected') == 'yes')} runs match, {len(unexp)} do not.")
    chk += [f"      {r['id']}: exit {r['exit_code']}, v8 had {r['expect_exit']} | {r['cell']}" for r in unexp[:40]]
    errs = [r for r in res if r["status"].startswith("error")]
    chk.append(f"{'PASS' if not errs else 'FAIL'}  Runs that ended with an error: {len(errs)}.")
    chk += [f"      {r['id']}: {r['status']} | {r['cell']}" for r in errs[:40]]
    same = sum(1 for s in sides_all if s.v8 == "same")
    diff = [(k, n, s) for k, sides in cells.items() for n, s in sides.items() if s.v8.startswith("DIFFERS")]
    new = sum(1 for s in sides_all if s.v8.startswith("new (v8"))
    chk.append(f"{'PASS' if not diff else 'FAIL'}  Same complete and blocked counts as the v8 campaign: {same} sides identical, {len(diff)} differ, {new} finished now that did not finish in v8.")
    chk += [f"      {k[0]} | {k[1]} | {n}: {s.v8}" for k, n, s in diff[:60]]
    vc = []
    for (ex, cell), sides in cells.items():
        key = vcounter(meta, ex, cell)
        if not key:
            continue
        for n, s in sides.items():
            if s.fin and not s.fire and key in s.counters and str(s.counters[key]).isdigit():
                vc.append(((ex, cell, n), int(s.counters[key]) == (s.viol or 0), s.counters[key], s.viol))
    if vc:
        nb = [v for v in vc if not v[1]]
        chk.append(f"{'PASS' if not nb else 'INFO'}  Violating executions (tool) equal the protocol's own violation counter: {len(vc) - len(nb)} of {len(vc)} sides.")
        chk += [f"      {k[0]} | {k[1]} | {k[2]}: counter {c}, tool {v}" for k, _, c, v in nb[:40]]
    reg = []
    for (ex, cell), sides in cells.items():
        m = meta.get(ex) or {}
        mc = {c["cell"]: c for c in m.get("cells", [])}.get(cell)
        t = sides.get("timed")
        if not mc or t is None or not t.fin or t.fire:
            continue
        rid = mc.get("regime", "")
        if rid.endswith("safe") and (t.viol or 0) > 0:
            reg.append(f"      {ex} | {cell}: regime {rid} but the timed side reports {t.viol:,} violating executions")
        if rid.endswith("viol") and (t.viol or 0) == 0:
            reg.append(f"      {ex} | {cell}: regime {rid} but the timed side reports no violation")
    if meta:
        chk.append(f"{'PASS' if not reg else 'FAIL'}  Timed verdict matches the regime (safe: 0 violations, viol: above 0): {len(reg)} mismatches.")
        chk += reg[:60]
    tg = [r for r in fin_rows if r["tier"] == "timing"]
    clean = [r for r in tg if r["timing_quality"] == "clean"]
    chk.append(f"INFO  Timing audit: {len(clean)} of {len(tg)} timing-grade runs clean.")
    spreads = []
    for k, sides in cells.items():
        for s in sides.values():
            ws_ = [float(r["wall_s"]) for r in s.fin if r.get("timing_quality") == "clean" and r["wall_s"]]
            if len(ws_) >= 3 and statistics.median(ws_) >= 10:
                spreads.append((max(ws_) - min(ws_)) / statistics.median(ws_))
    if spreads:
        spreads.sort()
        chk.append(f"INFO  Spread of repeated wall times ((max - min) / median, sides with 3 clean runs of 10 s or more, n = {len(spreads)}): "
                   f"median {100 * statistics.median(spreads):.1f}%, 90th percentile {100 * spreads[int(0.9 * (len(spreads) - 1))]:.1f}%, max {100 * spreads[-1]:.1f}%.")
    for label, want_pool in (("sequential", False), ("worker-pool", True)):
        sp = sorted(s.spread for sides in cells.values() for s in sides.values()
                    if s.fin and not s.fire and s.pool == want_pool and s.spread is not None and s.n_used >= 3 and (s.wall or 0) >= 10)
        if sp:
            chk.append(f"INFO  Wall-time spread of {label} exhaustive sides (3 or more runs, 10 s or more, n = {len(sp)}): median "
                       f"{100 * statistics.median(sp):.1f}%, largest {100 * sp[-1]:.1f}%, above 5% in {sum(1 for x in sp if x > 0.05)}, "
                       f"above 10% in {sum(1 for x in sp if x > 0.10)}.")
    mhz = sorted(float(r["mhz_avg"]) for r in tg if r.get("mhz_avg"))
    if mhz:
        chk.append(f"INFO  Mean clock frequency of timing-grade runs: median {statistics.median(mhz):.0f} MHz, range {mhz[0]:.0f} to {mhz[-1]:.0f}.")
    sib = [float(r["sib_busy_avg"]) for r in fin_rows if r.get("sib_busy_avg")]
    if sib:
        chk.append(f"INFO  Sibling hyper-threads: largest average load next to a run {100 * max(sib):.1f}% (0 means the second hyper-thread stayed idle).")
    missing = [ex for ex in exs if ex not in meta]
    if missing:
        chk.append("INFO  No metadata (parameter columns, regimes) yet for: " + ", ".join(missing))
    text_sheet(wb, "Checks", chk, width=200)

    # ---- All runs
    ws = wb.create_sheet("All runs")
    cols = ["id", "example", "cell", "side", "rep", "tier", "status", "exit_code", "execs", "blocked", "explored",
            "viol_execs", "timeline_impossible", "pruned", "wall_s", "cpu_s", "cpu_over_wall", "rss_mb", "cores",
            "parallel", "mhz_avg", "occupancy_avg", "sib_busy_avg", "foreign_busy_avg", "timing_quality",
            "started", "finished", "core_list", "counters", "progress", "binary_md5", "args"]
    for i, h in enumerate(cols, 1):
        put(ws, 1, i, h, WHITE_B, FILL["head"], align=CENTER)
        ws.column_dimensions[get_column_letter(i)].width = {"cell": 44, "args": 100, "counters": 40, "status": 30,
                                                            "progress": 40, "id": 16, "example": 24}.get(h, 12)
    ws.freeze_panes = "B2"
    numeric = {"execs", "blocked", "explored", "viol_execs", "timeline_impossible", "pruned", "wall_s", "cpu_s",
               "cpu_over_wall", "rss_mb", "cores", "mhz_avg", "occupancy_avg", "sib_busy_avg", "foreign_busy_avg"}
    for r_i, row in enumerate(res, 2):
        for i, h in enumerate(cols, 1):
            v = row.get(h, "")
            if h in numeric and v != "":
                v = fnum(v)
                if v is not None and v == int(v) and h not in ("wall_s", "cpu_s", "cpu_over_wall", "sib_busy_avg", "foreign_busy_avg"):
                    v = int(v)
            ws.cell(r_i, i, v)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(cols))}{max(len(res) + 1, 1)}"

    wb.save(args.out)
    print("wrote", args.out)
    print(state)
    print("metadata for:", ", ".join(sorted(m["family"] for m in meta.values())) or "none")
    for l in chk[3:]:
        if isinstance(l, str) and l[:4] in ("FAIL", "PASS", "INFO"):
            print("  " + l[:230])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default=os.path.join(HERE, "plan_v9.csv"))
    ap.add_argument("--results", default=os.path.join(HERE, "results_v9.csv"))
    ap.add_argument("--status", default=os.path.join(HERE, "status_v9.json"))
    ap.add_argument("--env", default=os.path.join(HERE, "env_v9.txt"))
    ap.add_argument("--meta", default=os.path.join(HERE, "meta"))
    ap.add_argument("--v8", default=os.path.join(HERE, "..", "v8"))
    ap.add_argument("--out", default=os.path.join(HERE, "results_v9.xlsx"))
    build(ap.parse_args())
