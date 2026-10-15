#!/usr/bin/env python3
"""Ladder climber for the v12 campaign (runs on each machine next to the scheduler).

Every 45 s, for each ladder:
  * Mustard: keep up to LOOKAHEAD rungs in flight; submit the next rung while the predicted wall (last finished wall x
    growth per rung) fits before the deadline, plus one final 'attempt' rung that runs to the stop.
  * Must: the baseline of a submitted rung is submitted only while the previous rung's baseline finished and
    6 x its wall still fits in 80% of the time left (after a baseline that did not fit, the ladder's Must is dead).
New rows are appended to the plan file the scheduler re-reads (atomic rename in the same directory).
"""
import csv, json, os, sys, time, re, statistics as st, datetime, collections, fcntl

M = sys.argv[1]
DEADLINE = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M").timestamp()
B = "/local/sramezani/v12/bench/"
PLAN, RES = B + f"plan_v12_{M}.csv", B + f"results_v12_{M}.csv"
LAD = json.load(open(B + f"ladders_{M}.json"))
LOOKAHEAD = 2
LOG = open(B + f"climb_{M}.log", "a", buffering=1)
LOCK = open(B + "plan.lock", "w")


def log(*a):
    print(time.strftime("%F %T"), *a, file=LOG)


def read_results():
    out = {}
    try:
        for r in csv.DictReader(open(RES)):
            out[re.sub(r"-x\d+$", "", r["id"])] = r
    except FileNotFoundError:
        pass
    return out


def read_plan():
    byp = collections.defaultdict(list)
    fields = None
    rows = []
    with open(PLAN, newline="") as fh:
        rd = csv.DictReader(fh)
        fields = rd.fieldnames
        for r in rd:
            rows.append(r)
            byp[r["params"]].append(r["id"])
    return fields, rows, byp


def append(fields, rows, new):
    if not new:
        return
    tmp = PLAN + ".climb.tmp"
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
        w.writerows(new)
    os.replace(tmp, PLAN)


def wall(res, ids):
    """None: some rep still running/queued or no rows; 'dnf': all stopped; float: median wall of finished reps"""
    if not ids:
        return None
    got = [res.get(i) for i in ids]
    if any(g is None for g in got):
        return None
    ok = [float(g["wall_s"]) for g in got if g["status"].startswith("finished") or g["status"] == "error (exit 3)"]
    return st.median(ok) if ok else "dnf"


state = {l["name"]: dict(sub=0, attempted=False, bdead=False, bsub=set()) for l in LAD}
log("climber start", M, "ladders", len(LAD), "deadline", sys.argv[2])
while True:
    now = time.time()
    if now > DEADLINE:
        log("deadline reached, exit")
        break
    fcntl.flock(LOCK, fcntl.LOCK_EX)
    res = read_results()
    fields, rows, byp = read_plan()
    new = []
    queued = {r["id"] for r in rows}
    for lad in LAD:
        s = state[lad["name"]]
        rungs = lad["rungs"]
        remaining = DEADLINE - now
        # ---- Mustard pass
        done, inflight = [], 0
        for i in range(s["sub"]):
            w = wall(res, [r["id"] for r in rungs[i]["mrows"]])
            if w is None:
                inflight += 1
            elif w != "dnf":
                done.append((i, w))
        t_last = done[-1][1] if done else None
        g = 4.0
        if len(done) >= 2 and done[-2][1] >= 1.0 and done[-1][1] >= 1.0:
            g = min(25.0, max(1.6, done[-1][1] / done[-2][1]))
        while s["sub"] < len(rungs) and inflight < LOOKAHEAD:
            i = s["sub"]
            feasible = True
            if done:
                feasible = t_last * (g ** (i - done[-1][0])) * 1.25 < remaining - 300
            if not feasible:
                if s["attempted"] or remaining < 2400:
                    break
                s["attempted"] = True
            new += rungs[i]["mrows"]
            s["sub"] += 1
            inflight += 1
            log(lad["name"], "Mustard rung", i, rungs[i]["size"], "inflight", inflight, "g=%.1f" % g,
                "last=%s" % (round(t_last, 1) if t_last else None), "attempt" if not feasible else "")
            if not feasible:
                break
        # ---- Must pass (baselines of submitted rungs)
        if not s["bdead"]:
            for i in range(s["sub"]):
                bp = rungs[i]["bparams"]
                if bp in byp or bp in s["bsub"]:
                    continue
                if i > 0:
                    pw = wall(res, byp.get(rungs[i - 1]["bparams"], []))
                    if pw is None:
                        break           # previous baseline still running (or never submitted): wait
                    if pw == "dnf" or not (pw * 6 < remaining * 0.8):
                        s["bdead"] = True
                        log(lad["name"], "Must dead at rung", i)
                        break
                new += rungs[i]["brows"]
                s["bsub"].add(bp)
                byp[bp] = [r["id"] for r in rungs[i]["brows"]]
                log(lad["name"], "Must rung", i, rungs[i]["size"])
    new = [r for r in new if r["id"] not in queued]
    append(fields, rows, new)
    fcntl.flock(LOCK, fcntl.LOCK_UN)
    time.sleep(45)
