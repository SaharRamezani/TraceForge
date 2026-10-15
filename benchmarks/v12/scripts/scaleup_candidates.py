#!/usr/bin/env python3
"""For every ladder: finished frontier, first unfinished rung, 1-core prediction, parallel prediction (speedup ~ 0.5 x cores)."""
import csv, json, re, os, sys, time, statistics as st, datetime

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
D = HERE + "data/"
DEADLINE = datetime.datetime.strptime("2026-10-15 07:50", "%Y-%m-%d %H:%M").timestamp()
now = time.time() + (datetime.datetime.now().timestamp() - time.time())   # local clock
cores = int(sys.argv[1]) if len(sys.argv) > 1 else 24
out = []
for m in ("brain05", "brain10"):
    res = {}
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
    plan = {r["id"]: r for r in csv.DictReader(open(D + f"plan_v12_{m}.csv"))}
    try:
        status = json.load(open(D + f"status_{m}.json"))
        elapsed = {j["id"]: j["elapsed_s"] for j in status["running"]}
    except Exception:
        elapsed = {}
    for lad in json.load(open(HERE + f"ladders_{m}.json")):
        fin, first_un = [], None
        for i, rg in enumerate(lad["rungs"]):
            ids = [r["id"] for r in rg["mrows"]]
            if not all(i_ in plan for i_ in ids):
                break                       # not submitted
            got = [res.get(i_) for i_ in ids]
            ok = [float(g["wall_s"]) for g in got if g and (g["status"].startswith("finished") or g["status"] == "error (exit 3)")]
            if ok:
                fin.append((i, st.median(ok)))
            elif first_un is None:
                first_un = (i, max([elapsed.get(i_, 0) for i_ in ids] or [0]))
        if not fin:
            continue
        g = 4.0
        if len(fin) >= 2 and fin[-2][1] >= 1 and fin[-1][1] >= 1:
            g = min(25.0, max(1.6, fin[-1][1] / fin[-2][1]))
        nxt = fin[-1][0] + 1
        k = first_un[0] if first_un else nxt
        pred = fin[-1][1] * g ** (k - fin[-1][0])
        out.append(dict(m=m, name=lad["name"], last=lad["rungs"][fin[-1][0]]["size"], tlast=fin[-1][1], g=g,
                        nextsize=lad["rungs"][k]["size"] if k < len(lad["rungs"]) else None,
                        pred1=pred, predP=pred / (0.5 * cores), el=first_un[1] if first_un else 0,
                        nrungs=len(lad["rungs"]), k=k))
rem = DEADLINE - datetime.datetime.now().timestamp()
print(f"remaining {rem / 3600:.2f} h; parallel prediction uses speedup 0.5 x {cores}")
for o in sorted(out, key=lambda o: o["name"]):
    feas1 = "1-core ok" if o["pred1"] * 1.2 < rem else ("PARALLEL" if o["predP"] * 1.2 < rem else "hopeless")
    print(f"{o['m']} {o['name']:28s} last {str(o['last']):12s} {o['tlast']:7.0f}s g={o['g']:5.1f} next {str(o['nextsize']):12s} "
          f"pred1={o['pred1'] / 3600:6.2f}h predP={o['predP'] / 3600:5.2f}h el={o['el'] // 60}m {feas1}")
