#!/usr/bin/env python3
"""Running jobs of each ladder: elapsed, last two finished rungs, unclamped growth, prediction."""
import csv, json, re, statistics as st, datetime, sys
D = "data/"
DEADLINE = datetime.datetime.strptime("2026-10-15 07:50", "%Y-%m-%d %H:%M").timestamp()
REM = DEADLINE - datetime.datetime.now().timestamp()
out = []
for m in ("brain05", "brain10"):
    res = {}
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
    plan = {r["id"]: r for r in csv.DictReader(open(D + f"plan_v12_{m}.csv"))}
    run = {j["id"]: (j["elapsed_s"], j["cores"]) for j in json.load(open(D + f"status_{m}.json"))["running"]}
    for lad in json.load(open(f"ladders_{m}.json")):
        fins = []
        for i, rg in enumerate(lad["rungs"]):
            ids = [r["id"] for r in rg["mrows"]]
            ok = [float(res[x]["wall_s"]) for x in ids if x in res and (res[x]["status"].startswith("finished") or res[x]["status"] == "error (exit 3)")]
            if ok:
                fins.append((i, st.median(ok), rg["size"]))
        for i, rg in enumerate(lad["rungs"]):
            for kind, rows in (("T", rg["mrows"]), ("B", rg["brows"])):
                for r in rows:
                    if r["id"] in run and run[r["id"]][1] == 1 and r["id"] not in res:
                        el = run[r["id"]][0]
                        if not fins:
                            continue
                        last = fins[-1]
                        g = None
                        if len(fins) >= 2 and fins[-2][1] >= 0.05:
                            g = last[1] / fins[-2][1]
                        steps = max(1, i - last[0])
                        pred = last[1] * (g ** steps) if g else None
                        out.append(dict(m=m, id=r["id"], kind=kind, lad=lad["name"], el=el, last=last[1], lastsize=last[2], g=g, steps=steps,
                                        pred=pred, size=rg["size"], cell=plan[r["id"]]["cell"][:60]))
print(f"remaining {REM / 3600:.2f} h")
for o in sorted(out, key=lambda o: -o["el"]):
    ok = "?" if o["pred"] is None else ("FEAS" if o["pred"] - o["el"] < REM * 0.9 else "HOPELESS")
    gtxt = "  n/a" if o["g"] is None else f"{o['g']:5.1f}"
    ptxt = "   n/a" if o["pred"] is None else f"{o['pred'] / 3600:6.1f}h"
    print(f"{o['m'][-2:]} {o['kind']} {o['id'][:22]:22s} el={o['el'] // 60:4d}m last={o['last']:7.0f}s {str(o['lastsize']):12s} g={gtxt} steps={o['steps']} pred={ptxt} {ok:8s} {o['lad'][:26]:26s} -> {o['size']}")
