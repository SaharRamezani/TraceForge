#!/usr/bin/env python3
"""Build results_v5.xlsx from the three results_v5_*.csv tiers.

Re-runnable: rerun after brain05 finishes the `big` tier to refresh.
Sheets: Results (v4-shaped grid rows), Lease & SWIM boundaries, Legend & method.
"""
import csv, os, re, datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
EXPECTED = {"small": 23, "big": 24, "lease_swim": 242}

def load(tier):
    p = os.path.join(HERE, f"results_v5_{tier}.csv")
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []

small, big, ls = load("small"), load("big"), load("lease_swim")

def params(cmd):
    """Recover (L, U, sd, W) in ticks from a cell's command line.
    Leader election passes absolute ticks (--l/--u/--sd/--w);
    3PC passes ratios of U (--l-ratio/--sd-ratio/--w-ratio)."""
    def flag(name):
        m = re.search(r"--%s\s+([0-9.]+)" % re.escape(name), cmd)
        return float(m.group(1)) if m else None
    u = flag("u")
    if u is None:
        return ("", "", "", "")
    if flag("w") is not None:                       # absolute form
        l, sd, w = flag("l"), flag("sd"), flag("w")
    else:                                           # ratio form (3PC)
        lr, sr, wr = flag("l-ratio"), flag("sd-ratio"), flag("w-ratio")
        l  = round(lr * u) if lr is not None else None
        sd = round(sr * u) if sr is not None else None
        w  = round(wr * u) if wr is not None else None
    fmt = lambda v: "" if v is None else int(v)
    return (fmt(l), fmt(u), fmt(sd), fmt(w))

HDR   = Font(bold=True, color="FFFFFF")
HFILL = PatternFill("solid", fgColor="2C5F8A")
SUB   = PatternFill("solid", fgColor="DCE6F1")
WARN  = PatternFill("solid", fgColor="FCE4D6")
GOOD  = PatternFill("solid", fgColor="E2EFDA")
BOLD  = Font(bold=True)
THIN  = Border(*[Side(style="thin", color="BFBFBF")] * 4)

def style_header(ws, row, ncol):
    for c in range(1, ncol + 1):
        cell = ws.cell(row=row, column=c)
        cell.font, cell.fill, cell.border = HDR, HFILL, THIN
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

def autosize(ws, maxw=42):
    for col in ws.columns:
        w = max((len(str(c.value)) for c in col if c.value is not None), default=8)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(w + 2, maxw)

wb = Workbook()

# ---------------------------------------------------------------- Results ---
ws = wb.active; ws.title = "Results"
ws["A1"] = "TraceForge v5 sweep: timed vs untimed baseline"; ws["A1"].font = Font(bold=True, size=14)
ws["A2"] = ("All runs on brain05 (single machine, one uniform 2-day cap per invocation). "
            "Timed and baseline are separate invocations, so a DNF on one side does not consume the other's budget.")
ws["A3"] = f"Generated {datetime.date.today().isoformat()} from results_v5_small.csv + results_v5_big.csv"

cols = ["Protocol", "N", "Rounds/Ballots", "Cell", "Timed execs", "Timed blocked",
        "Timed verdict", "Timed wall (s)", "Base execs", "Base blocked",
        "Base verdict", "Base wall (s)", "Pruning %",
        "L", "U", "sd", "W", "Command"]
ws.append([]); ws.append(cols); style_header(ws, 5, len(cols))

def num(v):
    try: return int(v)
    except (TypeError, ValueError):
        try: return float(v)
        except (TypeError, ValueError): return v or ""

rows = sorted(small + big, key=lambda r: (r["protocol"], int(r["N"]), int(r["rounds"]), r["cell"]))
for r in rows:
    L, U, SD, W = params(r["timed_cmd"])
    ws.append([r["protocol"], num(r["N"]), num(r["rounds"]), r["cell"],
               num(r["timed_execs"]), num(r["timed_block"]), r["timed_verdict"], num(r["timed_wall"]),
               num(r["base_execs"]), num(r["base_block"]), r["base_verdict"], num(r["base_wall"]),
               num(r["pruning_pct"]), L, U, SD, W, r["timed_cmd"]])
    i = ws.max_row
    for c in range(1, len(cols) + 1):
        ws.cell(row=i, column=c).border = THIN
    for c in (5, 6, 8, 9, 10, 12):
        ws.cell(row=i, column=c).number_format = "#,##0"
    ws.cell(row=i, column=13).number_format = '0.0"%"'
    if "DNF" in (r["timed_verdict"], r["base_verdict"]):
        for c in range(1, len(cols) + 1):
            ws.cell(row=i, column=c).fill = WARN
    elif r["pruning_pct"] and float(r["pruning_pct"]) >= 99:
        ws.cell(row=i, column=13).fill = GOOD
        ws.cell(row=i, column=13).font = BOLD

missing = EXPECTED["big"] - len(big)
if missing > 0:
    ws.append([])
    ws.append([f"NOTE: the 'big' tier is still running on brain05: {len(big)}/{EXPECTED['big']} cells present, "
               f"{missing} pending. Rerun build_v5_xlsx.py after it finishes."])
    ws.cell(row=ws.max_row, column=1).font = Font(bold=True, color="9C5700")
    ws.cell(row=ws.max_row, column=1).fill = WARN
ws.freeze_panes = "E6"; autosize(ws)

# ------------------------------------------------- Lease & SWIM boundaries ---
ws2 = wb.create_sheet("Lease & SWIM boundaries")
ws2["A1"] = "Boundary validation: predicted verdict vs measured"; ws2["A1"].font = Font(bold=True, size=14)
ws2["A2"] = "Lease:  FIRE  iff  pause >= TTL - 2(U - L + sd)"
ws2["A3"] = "SWIM:   FIRE  iff  W_suspect <= 2U + 2sd - min(L, max(0, 2L - W_probe))"
agree = sum(1 for r in ls if r["timed_verdict"] == r["expected"])
ws2["A4"] = f"{agree} / {len(ls)} cells match the predicted verdict"
ws2["A4"].font = Font(bold=True, color=("2E6B3E" if agree == len(ls) and ls else "C00000"))

c2 = ["Protocol", "Rounds", "Cell (parameters)", "Predicted", "Timed verdict",
      "Match?", "Timed execs", "Timed blocked", "Base verdict", "Timed wall (s)"]
ws2.append([]); ws2.append(c2); style_header(ws2, 6, len(c2))
for r in sorted(ls, key=lambda r: (r["protocol"], int(r["rounds"]), r["cell"])):
    ok = r["timed_verdict"] == r["expected"]
    ws2.append([r["protocol"], num(r["rounds"]), r["cell"], r["expected"], r["timed_verdict"],
                "yes" if ok else "NO", num(r["timed_execs"]), num(r["timed_block"]),
                r["base_verdict"], num(r["timed_wall"])])
    i = ws2.max_row
    for c in range(1, len(c2) + 1):
        ws2.cell(row=i, column=c).border = THIN
    ws2.cell(row=i, column=6).fill = GOOD if ok else WARN
    if not ok:
        ws2.cell(row=i, column=6).font = Font(bold=True, color="C00000")
ws2.freeze_panes = "D7"; autosize(ws2)

# ------------------------------------------------------- Legend & method ----
ws3 = wb.create_sheet("Legend & method")
for line in [
    ("Method", True),
    ("Every cell runs the identical program twice: once under the untimed baseline, once timed. Only the timing configuration differs.", False),
    ("Timed and baseline are separate process invocations, each with its own 2-day cap, so one side timing out never shortens the other.", False),
    ("Execution counts are machine-independent and are the primary metric; wall times are indicative and are all from brain05.", False),
    ("", False),
    ("Columns", True),
    ("Timed/Base execs", False), ("  Number of explored executions.", False),
    ("Pruning %", False), ("  100 * (base - timed) / base. Blank when either side did not finish.", False),
    ("Verdict", False), ("  hold = no assertion fired anywhere; FIRE = a certified counterexample; DNF = exceeded the 2-day cap.", False),
    ("", False),
    ("Regimes (U = 20 ticks)", True),
    ("A  tight intra-DC:   L=18, sd=2,  W=200   (L/U=0.9,  sd/U=0.1,  W/U=10)", False),
    ("B  intra-DC TCP:     L=6,  sd=0,  W=60    (L/U=0.3,  sd/U=0,    W/U=3)", False),
    ("C  inter-AZ:         L=2,  sd=5,  W=100   (L/U=0.1,  sd/U=0.25, W/U=5)", False),
    ("F  adversarial:      L=0,  sd=10, W=40    (L/U=0,    sd/U=0.5,  W/U=2)  <- widest message window; the completeness control", False),
    ("", False),
    ("Boundary sheet", True),
    ("Each lease/SWIM cell carries the verdict predicted by the published inequality; 'Match?' compares it to what the tool returned.", False),
    ("The untimed baseline FIREs on every lease/SWIM cell by construction: without real time it cannot separate the regimes.", False),
]:
    ws3.append([line[0]])
    if line[1]:
        ws3.cell(row=ws3.max_row, column=1).font = Font(bold=True, size=12)
ws3.column_dimensions["A"].width = 120


# --------------------------------------------------- Cell column guide -----
ws4 = wb.create_sheet("Cell guide")
ws4["A1"] = "What each value in the 'Cell' column means"
ws4["A1"].font = Font(bold=True, size=14)
ws4["A2"] = "The Cell column names the operating point of a row. L, U, sd and W are also given as explicit columns; all are integer ticks with U = 20 for the grid protocols."
ws4["A2"].alignment = Alignment(wrap_text=True)

g = ["Cell value", "Appears for", "Meaning", "L", "U", "sd", "W"]
ws4.append([]); ws4.append(g); style_header(ws4, 4, len(g))

GUIDE = [
 ("regime A", "leader election (plain + inbox)",
  "Tight intra-datacentre link: narrow transit window, short storage, generous deadline. The regime where timing prunes most.", 18, 20, 2, 200),
 ("regime B", "leader election (plain + inbox)",
  "Intra-datacentre TCP: moderate transit floor, no storage, modest deadline.", 6, 20, 0, 60),
 ("regime C", "leader election (plain + inbox)",
  "Inter-availability-zone: low transit floor with a longer tail, mid storage.", 2, 20, 5, 100),
 ("regime F", "leader election (plain + inbox)",
  "Adversarial / backpressure: widest message window (L = 0, largest sd) and tightest deadline ratio. Serves as the COMPLETENESS CONTROL: timing excludes nothing, so timed counts must equal the baseline exactly.", 0, 20, 10, 40),
 ("B=2 regime A", "leader election (plain + inbox)",
  "Two ballots at regime A. The ballot count is in the Rounds/Ballots column; the regime part is as above.", 18, 20, 2, 200),
 ("B=2 regime C", "leader election (plain + inbox)",
  "Two ballots at regime C.", 2, 20, 5, 100),
 ("W/U=2", "three-phase commit",
  "Skeen's minimum bounded wait (W = 2U): the tightest deadline that still permits non-blocking termination. Least pruning.", 0, 20, 5, 40),
 ("W/U=3", "three-phase commit", "Deadline three times the worst-case transit.", 0, 20, 5, 60),
 ("W/U=5", "three-phase commit", "Deadline five times the worst-case transit.", 0, 20, 5, 100),
 ("W/U=7", "three-phase commit", "Default operating point of the 3PC grid.", 0, 20, 5, 140),
 ("W/U=10", "three-phase commit",
  "Deadline ten times the worst-case transit, matching the order-of-magnitude headroom production Raft deployments use.", 0, 20, 5, 200),
 ("L/U=0.25", "three-phase commit",
  "Same as W/U=7 but with the transit floor raised to a quarter of U, which narrows every arrival window and prunes slightly more.", 5, 20, 5, 140),
 ("sd/U=0.5", "three-phase commit",
  "Same as W/U=7 but with storage duration doubled to half of U.", 0, 20, 10, 140),
 ("R=2", "three-phase commit", "Two commit rounds at the default point (W/U=7).", 0, 20, 5, 140),
 ("R=3", "three-phase commit", "Three commit rounds at the default point.", 0, 20, 5, 140),
]
for row in GUIDE:
    ws4.append(list(row))
    i = ws4.max_row
    for c in range(1, len(g) + 1):
        ws4.cell(row=i, column=c).border = THIN
        ws4.cell(row=i, column=c).alignment = Alignment(wrap_text=True, vertical="top")
    ws4.cell(row=i, column=1).font = BOLD
    if "regime F" in row[0]:
        for c in range(1, len(g) + 1):
            ws4.cell(row=i, column=c).fill = GOOD

ws4.append([])
ws4.append(["Boundary sheet cells (lease and SWIM)"]); ws4.cell(row=ws4.max_row, column=1).font = Font(bold=True, size=12)
for txt in [
 "Those cells spell their parameters out directly, e.g. 'U=1 L=0 sd=0 TTL=6 pause=4 fence=off'.",
 "  TTL    lease time-to-live.",
 "  pause  how long the lease holder is paused. The predicted boundary is pause = TTL - 2(U - L + sd).",
 "  fence  whether the fencing-token fix is enabled. Fenced runs must hold at every cell.",
 "  Wp     SWIM probe window;  Ws  SWIM suspicion window. Predicted boundary: Ws = 2U + 2sd - min(L, max(0, 2L - Wp)).",
 "Each parameter set appears twice, once at the boundary value (predicted FIRE) and once one tick off it (predicted hold).",
]:
    ws4.append([txt])

ws4.column_dimensions["A"].width = 16
ws4.column_dimensions["B"].width = 32
ws4.column_dimensions["C"].width = 82
for col in ("D", "E", "F", "G"):
    ws4.column_dimensions[col].width = 6
ws4.freeze_panes = "A5"

out = os.path.join(HERE, "results_v5.xlsx")
wb.save(out)
print(f"wrote {out}")
print(f"  Results sheet:    {len(rows)} rows (small {len(small)}/{EXPECTED['small']}, big {len(big)}/{EXPECTED['big']})")
print(f"  Boundary sheet:   {len(ls)} rows, {agree} matching")
