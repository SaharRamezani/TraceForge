#!/usr/bin/env python3
"""RESULTS_SUMMARY.md: per protocol the largest both-finished cell, the largest flip cell and the largest Mustard-only cell."""
import csv, re, collections
HERE = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/"
rows = list(csv.DictReader(open(HERE + "table_v11.csv")))
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def t(x):
    v = num(x)
    if v is None: return str(x) or "-"
    return f"{v:.0f} s" if v < 600 else f"{v/60:.1f} min"
def cell(r, s):
    e, v, b = r[s + "_exec"], r[s + "_viol"], r[s + "_blocked"]
    return f"{int(float(e)):,} / {int(float(v)):,} / {int(float(b)):,}"
by = collections.OrderedDict()
for r in rows: by.setdefault(r["paper_row"], []).append(r)
out = ["# v11 results summary (generated from table_v11.csv)", "",
       "Exec. / Viol. / Blocked per side; Time = median of the finished repetitions (reps in brackets). "
       "Exec. = healthy complete executions, Viol. = executions with a failed assertion (once per graph), "
       "Blocked = not violated and not finished. One pinned core per run.", ""]
for prow, rs in by.items():
    both = [r for r in rs if num(r["must_time_s"]) is not None and num(r["mustard_time_s"]) is not None]
    out.append(f"## {prow}")
    if both:
        big = max(both, key=lambda r: max(num(r["must_time_s"]), num(r["mustard_time_s"])))
        flips = [r for r in both if num(r["must_viol"]) > 0 and num(r["mustard_viol"]) == 0]
        out.append(f"- largest both-finished: `{big['cell']}`  Must {cell(big,'must')} in {t(big['must_time_s'])} (x{big['must_reps']}); "
                   f"Mustard {cell(big,'mustard')} in {t(big['mustard_time_s'])} (x{big['mustard_reps']}); host {big['host']}")
        if flips:
            fl = max(flips, key=lambda r: num(r["must_viol"]))
            out.append(f"- largest flip (Must violates, Mustard holds), {len(flips)} flip cells: `{fl['cell']}`  Must {cell(fl,'must')} in {t(fl['must_time_s'])}; "
                       f"Mustard {cell(fl,'mustard')} in {t(fl['mustard_time_s'])}")
        else:
            out.append("- flip cells: none")
    mo = [r for r in rs if num(r["must_time_s"]) is None and num(r["mustard_time_s"]) is not None]
    if mo:
        m = max(mo, key=lambda r: num(r["mustard_time_s"]))
        out.append(f"- largest Mustard-only: `{m['cell']}`  Mustard {cell(m,'mustard')} in {t(m['mustard_time_s'])}; Must: {m['must_time_s']}")
        notes = [n for n in m["notes"].split(" | ") if n.startswith("Must DNF")]
        if notes: out.append(f"  - {notes[0]}")
    out.append("")
open("/home/sahar/Downloads/TraceForge/benchmarks/v11/RESULTS_SUMMARY.md", "w").write("\n".join(out))
print("\n".join(out))
