#!/usr/bin/env python3
"""Must baselines (any machine) that are unfinished although a Mustard cell with that baseline key finished."""
import csv, json, re, glob, collections, sys
D = "data/"
M = ("brain05", "brain10")
TOP = int(sys.argv[1]) if len(sys.argv) > 1 else 60
cells = {}
for p in sorted(glob.glob("cells_v12*.json")):
    for c in json.load(open(p)):
        cells[c["cid"]] = c
plan, res, run = {}, {}, {}
for m in M:
    for r in csv.DictReader(open(D + f"plan_v12_{m}.csv")):
        r["_m"] = m; plan[r["id"]] = r
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
    for j in json.load(open(D + f"status_{m}.json"))["running"]:
        run[j["id"]] = (j["elapsed_s"], m)
def fin(i):
    r = res.get(i); return r is not None and (r["status"].startswith("finished") or r["status"] == "error (exit 3)")
byparams = collections.defaultdict(list)
for i, p in plan.items():
    byparams[p["params"]].append(i)
def bstate(key):
    ids = []
    for m in M:
        ids += byparams.get(f"b|{m}|{key}", [])
    ids = [i for i in ids if not (i in res and "removed" in res[i]["status"])]
    if any(fin(i) for i in ids):
        return "FIN", min(float(res[i]["wall_s"]) for i in ids if fin(i)), ""
    runs = [(run[i][0], run[i][1]) for i in ids if i in run]
    if runs:
        e, mm = max(runs)
        return "RUN", e, ",".join(sorted({x[1][-2:] for x in runs}))
    if ids:
        return ("DNF" if any(i in res for i in ids) else "QUEUED"), 0, ",".join(sorted({plan[i]["_m"][-2:] for i in ids}))
    return "NONE", 0, ""
tab = list(csv.DictReader(open("table_v12.csv")))
gaps = {}
for r in tab:
    try:
        mt = float(r["mustard_time_s"])
    except ValueError:
        continue
    c = cells[r["cid"]]
    key = "|".join(map(str, c["bkey"][1:]))
    st, w, where = bstate(key)
    if st == "FIN":
        continue
    g = gaps.setdefault((c["fam"], key), dict(st=st, w=w, where=where, n=0, maxmt=0, label=r["label"]))
    g["n"] += 1
    if mt > g["maxmt"]:
        g["maxmt"] = mt; g["label"] = r["label"]
print(len(gaps), "baseline keys unfinished with a finished Mustard cell")
for (fam, key), g in sorted(gaps.items(), key=lambda kv: -kv[1]["maxmt"])[:TOP]:
    print(f"{fam:8s} {key:28s} {g['st']:6s} {g['w'] / 60:5.0f}m [{g['where']:5s}] cells={g['n']:2d} maxMustard={g['maxmt']:6.0f}s  {g['label'][:50]}")
