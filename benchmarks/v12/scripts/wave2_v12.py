#!/usr/bin/env python3
"""Second-wave candidates: next rung of every ladder (Mustard and Must) that is feasible with 16 workers but not on one core.

Uses the unclamped growth of the last two finished rungs (floor 1.5, cap 60) and a 16-worker speedup of 8.
Usage: wave2_v12.py [max_hours_parallel=2.2]
"""
import csv, json, re, sys, statistics as st, datetime, collections

D = "data/"
MAXH = float(sys.argv[1]) if len(sys.argv) > 1 else 2.2
DEADLINE = datetime.datetime.strptime("2026-10-15 07:50", "%Y-%m-%d %H:%M").timestamp()
REM = DEADLINE - datetime.datetime.now().timestamp()
SPEEDUP = 8.0
PARALLEL_FAMS = {"lease", "lease_f", "swim", "abp", "brp", "nisshi", "yarn", "nrp", "retail"}   # yarn/nrp/retail via the _p binaries

res, plan, run = {}, {}, {}
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(D + f"plan_v12_{m}.csv")):
        plan[r["id"]] = r
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
    for j in json.load(open(D + f"status_{m}.json"))["running"]:
        run[j["id"]] = j["elapsed_s"]


def fin(i):
    r = res.get(i)
    return r is not None and (r["status"].startswith("finished") or r["status"] == "error (exit 3)")


def ids_state(ids):
    if any(fin(i) for i in ids):
        return "fin"
    if any(i in run for i in ids):
        return "running"
    if any(i in plan and i not in res for i in ids):
        return "queued"
    return "open"          # never submitted or removed by the operator


def norm(p):
    return p.split("|", 2)[2] if p.startswith("b|") else p


covered = set()
for i, p in plan.items():
    r = res.get(i)
    if i in run or fin(i) or (r is None):
        covered.add(norm(p["params"]))
    elif r is not None and "removed" not in r["status"]:
        covered.add(norm(p["params"]))      # DNF at the deadline etc.

out = []
for m in ("brain05", "brain10"):
    for lad in json.load(open(f"ladders_{m}.json")):
        for side, key in (("Mustard", "mrows"), ("Must", "brows")):
            fins = []
            for i, rg in enumerate(lad["rungs"]):
                ids = [r["id"] for r in rg[key]]
                ok = [float(res[x]["wall_s"]) for x in ids if fin(x)]
                if ok:
                    fins.append((i, st.median(ok), rg["size"]))
            if not fins:
                continue
            last = fins[-1]
            g = 4.0
            if len(fins) >= 2 and fins[-2][1] >= 5:
                g = min(60.0, max(1.5, last[1] / fins[-2][1]))
            # next rung after the last finished one that is open (not queued, not running)
            for i in range(last[0] + 1, len(lad["rungs"])):
                rg = lad["rungs"][i]
                ids = [r["id"] for r in rg[key]]
                s = ids_state(ids)
                if s in ("running", "queued"):
                    break
                if any(norm(plan[x]["params"]) in covered for x in ids if x in plan) or any(norm(r.get("params", "")) in covered for r in rg[key] if r.get("params")):
                    break
                steps = i - last[0]
                t1 = last[1] * g ** steps
                tp = t1 / SPEEDUP
                fam = rg[key][0]["family"] if rg[key] else ""
                if s == "open" and tp / 3600 <= MAXH and t1 / 3600 > 1.2 and fam in PARALLEL_FAMS and tp < REM - 1800:
                    out.append((tp, side, m, lad["name"], last[2], rg["size"], last[1], g, t1, fam, rg[key][0]["id"]))
                break
print(f"remaining {REM / 3600:.2f} h; candidates with 16-worker estimate <= {MAXH} h")
for o in sorted(out, key=lambda o: o[0]):
    print(f"{o[1]:7s} {o[2][-2:]} {o[3]:30s} last {str(o[4]):12s} {o[6]:7.0f}s g={o[7]:5.1f} -> next {str(o[5]):12s} 1-core {o[8] / 3600:6.1f} h, 16 workers {o[0] / 3600:5.2f} h  [{o[10]}]")
