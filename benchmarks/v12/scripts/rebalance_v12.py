#!/usr/bin/env python3
"""Prune redundant regime ladders, truncate ladders at their attempt rung, and run flagship attempt rungs in parallel.

Usage: rebalance_v12.py [--apply]   (without --apply: dry run, prints what would be done)
Works on the fetched data in data/ and the local ladders_<m>.json; the remote side is touched only with --apply.
"""
import csv, json, re, os, sys, time, statistics as st, datetime, subprocess, copy, collections

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
D = HERE + "data/"
APPLY = "--apply" in sys.argv
DEADLINE = datetime.datetime.strptime("2026-10-15 07:50", "%Y-%m-%d %H:%M").timestamp()
NOW = datetime.datetime.now().timestamp()
REM = DEADLINE - NOW
PCORES = 16

STOP_PREFIXES = ("lease|C|", "lease|D|", "lease_f|D|", "nisshi|C|", "nisshi|D|", "swim|local|C|", "swim|wan|D|",
                 "abp|par|C|1000|", "abp|abp|D|1000|", "brp|C|1000|", "nrp|nrp|C", "nrp|nrp|D", "nrp|noopt|C", "nrp|noopt|D",
                 "nrp|leasing|C", "nrp|leasing|D", "shop|D|")
# flagship attempt rungs run with PCORES workers: ladder name -> (extra args builder)
PARALLEL = ["lease|A|at|R", "lease|A|below|R", "swim|lan|A|R", "nisshi|E-fix|T", "abp|par|C|200|K", "brp|D|1000|MAX",
            "abp|par|D|1000|K"]

FIELDS = None
out_sh = []
for m in ("brain05", "brain10"):
    plan_rows = list(csv.DictReader(open(D + f"plan_v12_{m}.csv")))
    FIELDS = list(plan_rows[0].keys())
    plan = {r["id"]: r for r in plan_rows}
    res = {}
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        res[re.sub(r"-x\d+$", "", r["id"])] = r
    status = json.load(open(D + f"status_{m}.json"))
    running = {j["id"] for j in status["running"]}
    lads = json.load(open(HERE + f"ladders_{m}.json"))

    def fin(i):
        r = res.get(i)
        return r is not None and (r["status"].startswith("finished") or r["status"] == "error (exit 3)")

    def done(i):
        return i in res

    keep, stop = [], []
    for l in lads:
        (stop if l["name"].startswith(STOP_PREFIXES) else keep).append(l)
    keep_b = set()
    for l in keep:
        for rg in l["rungs"]:
            keep_b.add(rg["bparams"])
    skip = []
    for l in stop:
        for rg in l["rungs"]:
            for r in rg["mrows"]:
                if r["id"] in plan and not done(r["id"]):
                    skip.append(r["id"])
            if rg["bparams"] not in keep_b:
                for r in rg["brows"]:
                    if r["id"] in plan and not done(r["id"]):
                        skip.append(r["id"])
    # truncate kept ladders at their attempt rung; collect parallel rows
    newrows = []
    trunc = []
    for l in keep:
        rungs = l["rungs"]
        fins = []
        first_un = None
        for i, rg in enumerate(rungs):
            ids = [r["id"] for r in rg["mrows"]]
            if not all(x in plan for x in ids):
                break
            ok = [float(res[x]["wall_s"]) for x in ids if fin(x)]
            if ok:
                fins.append((i, st.median(ok)))
            elif first_un is None:
                first_un = i
        if not fins or first_un is None:
            continue
        g = 4.0
        if len(fins) >= 2 and fins[-2][1] >= 1 and fins[-1][1] >= 1:
            g = min(25.0, max(1.6, fins[-1][1] / fins[-2][1]))
        pred1 = fins[-1][1] * g ** (first_un - fins[-1][0])
        infeasible1 = pred1 * 1.25 > REM - 300
        if infeasible1 and l["name"] not in PARALLEL:
            l["rungs"] = rungs[: first_un + 1]
            trunc.append((l["name"], first_un))
        if l["name"] in PARALLEL:
            rg = rungs[first_un]
            l["rungs"] = rungs[: first_un + 1]
            for r in rg["mrows"]:
                if r["id"] in plan and not done(r["id"]):
                    skip.append(r["id"])
            base = copy.deepcopy(rg["mrows"][0])
            fam = base["family"]
            flag = f" --workers {PCORES}" if fam == "brp" else " --parallel shared"
            base.update(id=base["id"].replace("v12L-", "v12P-").rsplit("-r", 1)[0] + "-r1", rep="1", reps="1", cores=str(PCORES),
                        parallel="shared", order="-1000", args=base["args"] + flag, purpose="parallel")
            newrows.append(base)
            trunc.append((l["name"] + " [parallel]", first_un))
    print(f"{m}: keep {len(keep)} ladders, stop {len(stop)}, skip {len(skip)} queued/running jobs, truncate {len(trunc)}, "
          f"parallel rows {len(newrows)}")
    for t in trunc:
        print("    ", t)
    json.dump(keep, open(HERE + f"ladders_{m}.new.json", "w"))
    json.dump(newrows, open(HERE + f"parallel_rows_{m}.json", "w"))
    open(HERE + f"skip_{m}.txt", "w").write("".join(f"skip {i}\n" for i in sorted(set(skip))))
    out_sh.append(m)

if APPLY:
    for m in out_sh:
        H = f"sramezani@{m}.mpi-sws.org"
        B = "/local/sramezani/v12/bench/"
        rows = json.load(open(HERE + f"parallel_rows_{m}.json"))
        with open(HERE + f"parallel_rows_{m}.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
        subprocess.run(["scp", "-q", "-o", "BatchMode=yes", HERE + f"ladders_{m}.new.json", H + f":{B}ladders_{m}.json"], check=True)
        subprocess.run(["scp", "-q", "-o", "BatchMode=yes", HERE + f"parallel_rows_{m}.csv", HERE + f"skip_{m}.txt", H + f":{B}"], check=True)
        remote = f"""cd {B} && tmux kill-session -t v12climb 2>/dev/null; sleep 2
python3 - <<'EOF'
import csv, fcntl, os
lock = open('plan.lock', 'w'); fcntl.flock(lock, fcntl.LOCK_EX)
rows = list(csv.DictReader(open('plan_v12_{m}.csv'))); fields = list(rows[0].keys())
have = {{r['id'] for r in rows}}
new = [r for r in csv.DictReader(open('parallel_rows_{m}.csv')) if r['id'] not in have]
tmp = 'plan_v12_{m}.csv.rb.tmp'
with open(tmp, 'w', newline='') as fh:
    w = csv.DictWriter(fh, fieldnames=fields); w.writeheader(); w.writerows(rows); w.writerows(new)
os.replace(tmp, 'plan_v12_{m}.csv'); print('appended', len(new), 'parallel rows')
EOF
cat skip_{m}.txt >> control_v9.txt
tmux new-session -d -s v12climb "python3 climb_v12.py {m} '2026-10-15 07:50' > climb_{m}.out 2>&1"
sleep 5; tail -n 2 climb_{m}.log"""
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", H, remote], capture_output=True, text=True)
        print(m, r.stdout[-600:], r.stderr[-300:])
