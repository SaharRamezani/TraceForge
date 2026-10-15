#!/usr/bin/env python3
"""Second control set: the largest cells (both sides finished, 120 s <= wall <= max_wall) of each family,
rerun once on the OTHER machine. Rows go to controls2_rows_<m>.csv (m = machine that runs the control)."""
import csv, re, sys, statistics as st, collections

HERE = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/"
D = HERE + "data/"
max_wall = float(sys.argv[1]) if len(sys.argv) > 1 else 1500.0
per_family = int(sys.argv[2]) if len(sys.argv) > 2 else 1

plan, FIELDS = {}, None
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

grp = collections.defaultdict(list)
have_ctrl = set()
for i, p in plan.items():
    if p["purpose"] == "control":
        have_ctrl.add((p["family"], p["cell"], p["side"]))
        continue
    grp[(p["family"], p["cell"], p["side"])].append(i)

cells = collections.defaultdict(dict)
for (fam, cell, side), ids in grp.items():
    fin = [res[i] for i in ids if i in res and (res[i]["status"].startswith("finished") or res[i]["status"] == "error (exit 3)")]
    if fin and len(fin) == len(ids):
        cells[(fam, cell)][side] = (st.median(float(r["wall_s"]) for r in fin), plan[ids[0]])

cand = collections.defaultdict(list)
for (fam, cell), d in cells.items():
    if "baseline" in d and "timed" in d:
        w = max(d["baseline"][0], d["timed"][0])
        if 120 <= w <= max_wall and (fam, cell, "baseline") not in have_ctrl:
            cand[fam].append((w, cell, d))

out = {"brain05": [], "brain10": []}
n = 0
for fam, lst in sorted(cand.items()):
    for w, cell, d in sorted(lst, key=lambda x: -x[0])[:per_family]:
        n += 1
        for side in ("baseline", "timed"):
            base = d[side][1]
            other = "brain10" if base["_m"] == "brain05" else "brain05"
            nid = f"v11c2-{fam}-{n:03d}-{side[0]}-c2"
            row = {k: base[k] for k in FIELDS}
            row.update(id=nid, rep="22", reps="1", purpose="control", order=str(-int(d[side][0])),
                       pred_s=str(int(d[side][0])))
            out[other].append(row)
        print(f"control2: {fam:8s} {cell[:60]:60s} wall~{w:.0f}s main={d['baseline'][1]['_m']}")
for m, rs in out.items():
    with open(HERE + f"controls2_rows_{m}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rs)
    print(m, len(rs), "control jobs")
