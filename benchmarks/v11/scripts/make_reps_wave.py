#!/usr/bin/env python3
"""Add repetitions for finished single-rep sides so the table's Time is a median of up to 3 runs.

Usage: make_reps_wave.py <minutes_left>
Reads data/ (fetched results + plans), writes reps_rows_<m>.csv (rows to append to the live plan of machine m).
A side qualifies when it finished with fewer repetitions than its target and its wall fits into the time left
(wall * 1.15 + 120 s).  Target: 3 reps for wall <= 900 s, 2 reps for wall <= 3000 s, otherwise 1.
Same machine as the original reps (a cell's two sides and all its reps stay together).
"""
import csv, re, sys, statistics as st, collections

HERE = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/"
D = HERE + "data/"
left = float(sys.argv[1]) * 60 if len(sys.argv) > 1 else 3000.0
tag = sys.argv[2] if len(sys.argv) > 2 else "w3"

plan, rows = {}, []
FIELDS = None
for m in ("brain05", "brain10"):
    rd = csv.DictReader(open(D + f"plan_v11_{m}.csv"))
    FIELDS = rd.fieldnames
    for r in rd:
        r["_m"] = m
        plan[r["id"]] = r
res = {}
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(D + f"results_v11_{m}.csv")):
        res[r["id"]] = r

# group by (family, cell, side) over non-control rows
grp = collections.defaultdict(list)
for i, p in plan.items():
    if p["purpose"] == "control":
        continue
    grp[(p["family"], p["cell"], p["side"])].append(i)

new = {"brain05": [], "brain10": []}
used_ids = set(plan)
n_add = 0
for (fam, cell, side), ids in grp.items():
    fin = [res[i] for i in ids if i in res and (res[i]["status"].startswith("finished") or res[i]["status"] == "error (exit 3)")]
    pending = [i for i in ids if i not in res]
    if not fin or pending:
        continue            # not finished yet, or reps still running/queued
    walls = [float(r["wall_s"]) for r in fin]
    w = st.median(walls)
    target = 3 if w <= 900 else (2 if w <= 3000 else 1)
    have = len(ids)
    if have >= target or w * 1.15 + 120 > left:
        continue
    base = plan[ids[0]]
    for k in range(have + 1, target + 1):
        nid = re.sub(r"-r\d+$", f"-r{k}", ids[0])
        if nid in used_ids:
            nid = re.sub(r"-r\d+$", f"-{tag}r{k}", ids[0])
        used_ids.add(nid)
        row = {k2: base[k2] for k2 in FIELDS}
        row.update(id=nid, rep=str(k), reps=str(target), purpose=base["purpose"], order=str(-int(w)),
                   pred_s=str(int(w)))
        new[base["_m"]].append(row)
        n_add += 1

for m, rs in new.items():
    with open(HERE + f"reps_rows_{m}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rs)
    print(m, len(rs), "rep jobs; sum predicted core-s", sum(float(r["pred_s"]) for r in rs))
print("total", n_add)
