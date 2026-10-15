#!/usr/bin/env python3
"""Extra repetitions (to 3 runs) for every selected table cell and its baseline when a run is short.
Writes extra_rows_<machine>.json; usage: extra_reps_v12.py <max_wall_s> <machine> [limit]"""
import csv, json, re, sys, os, collections, statistics as st
sys.argv_saved = sys.argv
maxwall = float(sys.argv[1]) if len(sys.argv) > 1 else 1800
target = sys.argv[2] if len(sys.argv) > 2 else "brain05"
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 10 ** 9
HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
import importlib.util
sys.argv = ["make_tex_v12.py", "/tmp/"]
spec = importlib.util.spec_from_file_location("mk", HERE + "make_tex_v12.py")
mk = importlib.util.module_from_spec(spec); spec.loader.exec_module(mk)
sys.argv = sys.argv_saved
D = HERE + "data/"
plan, res = {}, {}
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(D + f"plan_v12_{m}.csv")):
        plan[r["id"]] = r
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
def fin(i):
    r = res.get(i); return r is not None and (r["status"].startswith("finished") or r["status"] == "error (exit 3)")
byparams = collections.defaultdict(list)
for i, p in plan.items():
    byparams[p["params"]].append(i)
cells = {}
import glob
for p in sorted(glob.glob(HERE + "cells_v12*.json")):
    for c in json.load(open(p)):
        cells[c["cid"]] = c
sel = []
for fam, title, desc in mk.FAMILIES:
    sel += mk.select([d for d in mk.recs if d["fam"] == fam])
want = {}
for d in sel:
    r = d["r"]
    c = cells.get(r["cid"])
    if not c:
        continue
    keys = [("timed", r["cid"]), ("baseline", "b|" + "|".join(map(str, c["bkey"])))]
    for side, params in keys:
        ids = byparams.get(params, [])
        ids = [i for i in ids if not (i in res and "removed" in res[i]["status"])]
        if side == "baseline" and not any(fin(i) for i in ids):
            for m2 in ("brain05", "brain10"):
                alt = "b|" + m2 + "|" + "|".join(map(str, c["bkey"][1:]))
                ids += byparams.get(alt, [])
        done = [i for i in ids if fin(i)]
        if not done:
            continue
        walls = [float(res[i]["wall_s"]) for i in done]
        cores = int(float(res[done[0]]["cores"] or 1))
        if cores > 1 or st.median(walls) > maxwall or len(done) >= 3:
            continue
        if any((i not in res) for i in ids):      # some rep queued/running already
            continue
        want[(side, params)] = (done[0], 3 - len(done), st.median(walls))
rows = []
for (side, params), (src, need, w) in sorted(want.items(), key=lambda kv: kv[1][2]):
    base = dict(plan[src])
    for k in range(need):
        r = dict(base)
        r.update(id=f"v12R-{src.split('-', 1)[1]}-rep{k + 2}", rep=str(2 + k), reps="3", purpose="rep", order=str(int(w)))
        rows.append(r)
rows = rows[:limit]
json.dump(rows, open(HERE + f"extra_rows_{target}.json", "w"))
print(len(want), "cells need reps;", len(rows), "rows; total est. core-s", int(sum(float(r["order"]) for r in rows)))
