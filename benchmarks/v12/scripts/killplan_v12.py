#!/usr/bin/env python3
"""Kill plan: lookahead rungs, hopeless attempts, duplicates across machines. Writes skip_<m>.txt (not applied)."""
import csv, json, re, statistics as st, datetime, collections, sys
D = "data/"
DEADLINE = datetime.datetime.strptime("2026-10-15 07:50", "%Y-%m-%d %H:%M").timestamp()
REM = DEADLINE - datetime.datetime.now().timestamp()
M = ("brain05", "brain10")
KEEP_RUNNING = set(sys.argv[1:])        # ids to protect
plan, res, run = {}, {}, {}
for m in M:
    for r in csv.DictReader(open(D + f"plan_v12_{m}.csv")):
        r["_m"] = m; plan[r["id"]] = r
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
    for j in json.load(open(D + f"status_{m}.json"))["running"]:
        run[j["id"]] = (j["elapsed_s"], j["cores"], m)
def fin(i):
    r = res.get(i); return r is not None and (r["status"].startswith("finished") or r["status"] == "error (exit 3)")
kill = collections.OrderedDict()
kill_q = collections.OrderedDict()
MANUAL = ["v12L-nisshi-00016-b-r1", "v12L-brp-00016-b-r1", "v12-lease-01633-t-r1", "v12-lease-01635-t-r1", "v12L-nisshi-00057-t-r1"]
def add(i, why):
    if i in run and i not in KEEP_RUNNING and i not in kill:
        kill[i] = why
# ladder based
for m in M:
    for lad in json.load(open(f"ladders_{m}.json")):
        fins = []
        for i, rg in enumerate(lad["rungs"]):
            ids = [r["id"] for r in rg["mrows"]]
            ok = [float(res[x]["wall_s"]) for x in ids if fin(x)]
            if ok:
                fins.append((i, st.median(ok)))
        if not fins:
            continue
        last = fins[-1]
        g = None
        if len(fins) >= 2 and fins[-2][1] >= 0.05:
            g = last[1] / fins[-2][1]
        for i, rg in enumerate(lad["rungs"]):
            steps = i - last[0]
            if steps < 1:
                continue
            for kind, rows in (("T", rg["mrows"]), ("B", rg["brows"])):
                for r in rows:
                    if r["id"] not in run or run[r["id"]][1] != 1:
                        continue
                    el = run[r["id"]][0]
                    pred = last[1] * (g ** steps) if g else None
                    hopeless = pred is not None and pred - el > 0.9 * REM
                    if kind == "T" and steps >= 2:
                        add(r["id"], f"lookahead rung {steps} steps ahead")
                    elif kind == "T" and hopeless:
                        add(r["id"], f"Mustard attempt hopeless ({pred / 3600:.0f} h pred)")
                    elif kind == "B" and steps >= 2:
                        add(r["id"], f"lookahead baseline {steps} steps ahead")
                    elif kind == "B" and hopeless:
                        # baseline of a rung whose Mustard side did not finish: no pairing value
                        mfin = any(fin(x["id"]) for x in rg["mrows"])
                        if not mfin:
                            add(r["id"], "baseline of an unfinished hopeless rung")
# duplicates of baseline keys across machines
bykey = collections.defaultdict(list)
for i, p in plan.items():
    if p["side"] == "baseline" and p["params"].startswith("b|") and not (i in res and "removed" in res[i]["status"]):
        bykey[(p["family"], p["params"].split("|", 2)[2])].append(i)
for (fam, k), ids in bykey.items():
    ms = {plan[i]["_m"] for i in ids}
    if len(ms) < 2:
        continue
    done = any(fin(i) for i in ids)
    live = [i for i in ids if i in run]
    if done:
        for i in live:
            add(i, "baseline finished on the other machine")
        continue
    # keep one live copy: the one with the largest elapsed, prefer brain05 on ties
    if len(live) > 1:
        live.sort(key=lambda i: (-(run[i][0] // 600), plan[i]["_m"] != "brain05"))
        for i in live[1:]:
            add(i, "duplicate baseline running on both machines")
# R5: identical commands (same example and args): keep one, prefer finished > longest running; skip queued copies
def sig(p):
    return (p["example"], re.sub(r"\s+--(parallel shared|workers \d+)", "", p["args"]))
groups = collections.defaultdict(list)
for i, p in plan.items():
    if i in res and "removed" in res[i]["status"]:
        continue
    groups[sig(p)].append(i)
queued = {i for i in plan if i not in res and i not in run}
for s_, ids in groups.items():
    if len(ids) < 2:
        continue
    done = [i for i in ids if fin(i)]
    live = sorted([i for i in ids if i in run and i not in kill], key=lambda i: -run[i][0])
    que = [i for i in ids if i in queued]
    if done:
        for i in live:
            add(i, "identical cell finished elsewhere")
        for i in que:
            kill_q[i] = "identical cell finished elsewhere"
    else:
        for i in live[1:]:
            add(i, "identical command running elsewhere")
        if live:
            for i in que:
                kill_q[i] = "identical command running elsewhere"
        elif len(que) > 1:
            for i in que[1:]:
                kill_q[i] = "identical queued command"
for i in MANUAL:
    add(i, "manual: replaced by a parallel run or redundant regime")
for i, why in kill.items():
    print(f"{plan[i]['_m'][-2:]} {i:26s} el={run[i][0] // 60:4d}m  {plan[i]['cell'][:52]:52s} {why}")
print(len(kill_q), "queued rows to skip:", collections.Counter(plan[i]["_m"][-2:] + " " + plan[i]["family"] for i in kill_q))
print(len(kill), "to kill;", sum(1 for i in kill if plan[i]["_m"] == "brain10"), "on brain10")
json.dump(dict(running=list(kill), queued=list(kill_q)), open("kill_ids.json", "w"))
for m in M:
    open(f"skip_{m}.txt", "w").write("".join(f"skip {i}\n" for i in list(kill) + list(kill_q) if plan[i]["_m"] == m))
