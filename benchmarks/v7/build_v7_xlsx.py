#!/usr/bin/env python3
"""Build benchmarks/results_v7.xlsx as a COPY of results_v6.xlsx (never modifies v6).

Rule (Sahar, 2026-09-14): her visual edits of the workbook must survive, so the
existing sheets are only edited cell by cell; nothing is regenerated.

Stage 1 (this file): close out the v6 campaign that was stopped on 2026-09-20.
  * results that finished after the v6 workbook was written are filled in;
  * every side that was still running when the campaign was killed is marked
    "DNF (did not finish; killed ...)" with how long it had run.
Stage 2 (ceiling sheets for lease / SWIM) is added by build_v7_ceiling() below
when its CSVs exist.
"""
import csv, datetime as dt, os, shutil, sys
import openpyxl
from openpyxl.styles import PatternFill

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
SRC = os.path.join(BENCH, "results_v6.xlsx")
DST = os.path.join(BENCH, "results_v7.xlsx")
SNAP = os.path.join(BENCH, "v7_snapshot")

KILL = dt.datetime(2026, 9, 20, 18, 9, 17)                 # brain05 clock, from kill_log
START_REST = dt.datetime(2026, 9, 14, 14, 2, 40)           # v6rest relaunch (process lstart)
START_GRID = dt.datetime(2026, 9, 11, 10, 33, 22)          # v6grid relaunch (process lstart)
# the one side of the old sweep that was still running at kill time
GRID_RUNNING = {("comm_closed_leader_election_inbox", "6", "1", "regime A", "timed"): START_GRID}

YELLOW = PatternFill(fill_type="solid", fgColor="FFFFF2CC", bgColor="FFFFF2CC")
NOFILL = PatternFill(fill_type=None)

def dur(t0, t1=KILL):
    s = int((t1 - t0).total_seconds())
    return f"{s // 86400} d {s % 86400 // 3600} h {s % 3600 // 60} min"

def load(fn):
    with open(fn, newline="") as f:
        return list(csv.DictReader(f))

def key(r, side=None):
    return (r["protocol"], str(r["N"]), str(r["rounds"]), r["cell"], side or r["side"])

def num(x, cast=float):
    return cast(x) if x not in ("", None) else None

def close_out_v6(wb):
    ws = wb["Results"]
    rows = load(os.path.join(SNAP, "results_v6_grid.csv")) + load(os.path.join(SNAP, "results_v6_grid_rest.csv"))
    done = {key(r): r for r in rows}
    changes, dnf = [], []
    # column map (row 3 header): E timed execs, F blocked, G verdict, H wall, I cpu,
    # J untimed execs, K blocked, L verdict, M wall, N cpu, O pruning %, P wall ratio
    T = dict(execs=5, block=6, verdict=7, wall=8, cpu=9)
    U = dict(execs=10, block=11, verdict=12, wall=13, cpu=14)
    for r in range(4, ws.max_row + 1):
        proto, N, R, cell = (ws.cell(r, c).value for c in (1, 2, 3, 4))
        if not proto or proto.startswith("NOTE") or N is None:
            continue
        k = lambda side: (proto, str(N), str(R), cell, side)
        any_dnf, touched = False, False
        for side, cols in (("timed", T), ("baseline", U)):
            rec = done.get(k(side))
            cur = ws.cell(r, cols["verdict"]).value
            unfinished = cur is None or (isinstance(cur, str) and cur.startswith("DNF"))
            if rec and unfinished:
                new = dict(execs=num(rec["execs"], int), block=num(rec["block"], int), verdict=rec["verdict"],
                           wall=num(rec["wall_s"]), cpu=num(rec["cpu_s"]))
                for f, v in new.items():
                    ws.cell(r, cols[f]).value = v
                changes.append((r, proto, N, R, cell, side, cur, new["execs"]))
                touched = True
            elif rec:
                # finished in the workbook already: never overwrite, only cross-check
                if ws.cell(r, cols["execs"]).value != num(rec["execs"], int):
                    print("MISMATCH (kept workbook value):", k(side), ws.cell(r, cols["execs"]).value, rec["execs"])
            elif unfinished:
                t0 = GRID_RUNNING.get(k(side), START_REST)
                ws.cell(r, cols["verdict"]).value = f"DNF (did not finish; killed 2026-09-20 18:09 after {dur(t0)})"
                for f in ("execs", "block", "wall", "cpu"):
                    ws.cell(r, cols[f]).value = None      # a stale lower bound is not a measurement
                dnf.append((r, proto, N, R, cell, side, dur(t0)))
                touched = True
            if isinstance(ws.cell(r, cols["verdict"]).value, str) and ws.cell(r, cols["verdict"]).value.startswith("DNF"):
                any_dnf = True
        if touched:
            te, ue = ws.cell(r, T["execs"]).value, ws.cell(r, U["execs"]).value
            tw, uw = ws.cell(r, T["wall"]).value, ws.cell(r, U["wall"]).value
            ws.cell(r, 15).value = round(100 * (ue - te) / ue, 1) if te is not None and ue else None
            ws.cell(r, 16).value = round(uw / tw, 2) if tw and uw else None
            fill = YELLOW if any_dnf else NOFILL
            for c in range(1, 22):
                ws.cell(r, c).fill = fill
    # header / note texts
    ws["A1"].value = "TraceForge v7 workbook: v6 grid closed out on 2026-09-20, plus lease/SWIM ceiling sheets"
    nrows = len({(x[1], x[2], x[3], x[4]) for x in dnf})
    for r in range(4, ws.max_row + 1):
        v = ws.cell(r, 1).value
        if isinstance(v, str) and v.startswith("NOTE:"):
            ws.cell(r, 1).value = (f"NOTE: {nrows} cells have at least one side that did not finish (DNF, yellow rows). "
                                   f"The v6 campaign on brain05 was stopped on 2026-09-20 18:09; {len(dnf)} sides were killed "
                                   f"after 6 d 4 h (relaunch of 2026-09-14) or 9 d 7 h (inbox N=6 B=1 regime A timed). "
                                   f"A DNF is not a hold and not a FIRE: their counts are unknown.")
    return changes, dnf

def main():
    shutil.copyfile(SRC, DST)
    wb = openpyxl.load_workbook(DST)
    changes, dnf = close_out_v6(wb)
    if "--ceiling" in sys.argv:
        import build_v7_ceiling
        build_v7_ceiling.add_sheets(wb)
    wb.save(DST)
    print(f"filled/changed {len(changes)} sides, marked {len(dnf)} sides DNF -> {DST}")
    for c in changes: print("  FILLED", c)
    for d in dnf: print("  DNF   ", d)

if __name__ == "__main__":
    main()
