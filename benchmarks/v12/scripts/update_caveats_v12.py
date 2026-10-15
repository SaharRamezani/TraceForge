#!/usr/bin/env python3
"""Replace the count-dependent phrases of CAVEATS.md with the final numbers."""
import csv, json, re, sys, os

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
CAV = sys.argv[1]
D = HERE + "data/"
s = open(CAV).read()


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


rows = list(csv.DictReader(open(HERE + "table_v12.csv")))
both = [r for r in rows if num(r["must_time_s"]) is not None and num(r["mustard_time_s"]) is not None]
more = [r for r in both if num(r["mustard_exec"]) > num(r["must_exec"])]
healthy_eq = sum(1 for r in more if num(r["mustard_healthy"]) == num(r["must_healthy"]))
n_removed = 0
n_fin = 0
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        if "removed by the operator" in r["status"]:
            n_removed += 1
        if r["status"].startswith("finished") or r["status"] == "error (exit 3)":
            n_fin += 1
ctl = json.load(open(HERE + "controls_v12.json"))
txt = open(HERE + "controls_v12.txt").read()
mm = re.search(r"over (\d+) pairs.*?median ([\d.]+), min ([\d.]+), max ([\d.]+)", txt)
same = sum(1 for c in ctl if c["same"])

s = re.sub(r"232 identical commands finished on both machines \(baselines and Mustard cells\):\n  counts identical in all 232, wall ratio brain10/brain05 median 1\.01 \(0\.86 to 1\.09\)\.",
           f"{len(ctl)} identical commands finished on both machines (baselines and Mustard cells):\n  counts identical in {same}, wall ratio brain10/brain05 median {float(mm.group(2)):.2f} ({float(mm.group(3)):.2f} to {float(mm.group(4)):.2f}, {mm.group(1)} one-core pairs of at least 5 s).", s)
s = s.replace("Cells stopped by the operator (about 400 rows, status", f"Cells stopped by the operator ({n_removed} rows, status")
s = re.sub(r"in 79 of 1,568 both-finished cells \(SWIM at D and E, lease and shop at E; all with U >= a timer\) Mustard's Exec\. is\n  larger than Must's\. No new program behaviour is explored; the healthy count is equal in 53 of them\.",
           f"in {len(more)} of {len(both):,} both-finished cells (SWIM at D and E, lease and shop at E; all with U >= a timer) Mustard's Exec. is\n  larger than Must's. No new program behaviour is explored; the healthy count is equal in {healthy_eq} of them.", s)
open(CAV, "w").write(s)
print("caveats updated:", len(ctl), same, n_removed, len(more), len(both), healthy_eq, n_fin)
