#!/usr/bin/env python3
"""v9 job plan = every cell of the v8 campaign, re-measured under the v9 timing rules.

Input : ../v8/plan_v8.csv (argument strings, cores, purpose), ../v8/results_v8.csv and
        ../v8/results_v8_w0A.csv (measured wall times, used as predictions)
Output: plan_v9.csv, read by sweep_v9.py

  * job id = v8 id + "-r<repetition>", so every v9 row can be compared with its v8 row;
  * the argument string, the core count and the parallel mode are exactly those of v8
    (all of them ran on the current code in the 8 Oct rerun);
  * timing-grade = v8 wall <= 1 h: three repetitions; reach-grade = longer or not
    finished in v8: one run, started first, longest first;
  * the v8 runs that did not finish get the whole v9 window (85 h instead of 42 h).
"""
import csv
import datetime as dt
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from make_plan_v9 import FIELDS, TIMING_MAX_S, simulate  # noqa: E402

V8 = os.path.join(HERE, "..", "v8")
TIMING_S = 3600          # three repetitions up to one hour (v8 wall); longer runs once
TASK_RX = r"^(task-[0-9]+|main-thread-ThreadId\([0-9]+\))$"
COUNTERS = ["violations", "dirty_reads", "stray_commits", "p1_fails", "p2_fails", "p2_runs", "p4_runs",
            "sensor_failures", "dead", "late_reports", "oversold", "late", "stale", "expired", "elected",
            "impossible", "explored"]
CSPEC = json.dumps([{"key": k, "regex": rf"\b{k}=(\d+)"} for k in COUNTERS])


def main():
    deadline = sys.argv[1] if len(sys.argv) > 1 else "2026-10-13 08:00"
    plan = list(csv.DictReader(open(os.path.join(V8, "plan_v8.csv"))))
    res = {r["id"]: r for r in csv.DictReader(open(os.path.join(V8, "results_v8.csv")))}
    for r in csv.DictReader(open(os.path.join(V8, "results_v8_w0A.csv"))):
        res[r["id"][:-2]] = r                     # "brp-007-A" replaces "brp-007" (rule A rerun)
    # Cores. v8 gave 4 to 8 cores to many runs whose shared pool kept only one or two busy
    # (cpu_s / wall_s). Such a run gets the cores it can use (at least 2), and both sides of
    # a cell get the same number.
    import math
    need = {}
    for p in plan:
        r = res.get(p["id"], {})
        k = int(float(p["cpus"]))
        if k >= 4 and r.get("status", "").startswith("finished") and r.get("cpu_s") and float(r["wall_s"] or 0) > 60:
            k = min(k, max(2, math.ceil(1.25 * float(r["cpu_s"]) / float(r["wall_s"]))))
        key = (p["group"], p["example"], p["cell"])
        need[key] = max(need.get(key, 0), k)
    drop = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else set()
    jobs = []
    for p in plan:
        if p["id"] in drop:
            continue
        r = res.get(p["id"], {})
        st = r.get("status", "")
        finished = st.startswith("finished")
        wall = float(r["wall_s"]) if finished and r.get("wall_s") else None
        pred = max(wall, 1.0) if wall is not None else max(float(p["pred_s"] or 0), 400000.0)
        tier = "timing" if pred <= TIMING_S else "reach"
        reps = 3 if tier == "timing" else 1
        p = dict(p, cpus=need[(p["group"], p["example"], p["cell"])])
        for rep in range(1, reps + 1):
            order = (1.0 - min(pred, 9.9e6) / 1e7) if tier == "reach" else \
                    (10.0 + rep + 0.5 * (1.0 - min(pred, TIMING_S) / TIMING_S))
            jobs.append(dict(
                id=f"{p['id']}-r{rep}", family=p["group"], example=p["example"], cell=p["cell"], regime="",
                side=p["side"], rep=rep, reps=reps, tier=tier, purpose=p["purpose"], args=p["args"],
                cores=int(float(p["cpus"])), parallel=p["parallel"], pred_s=f"{pred:.0f}",
                mem_gb=max(float(p["mem_gb"] or 0), 128), order=f"{order:.6f}", expect=p["expected"],
                expect_exit=r.get("exit_code", "") if finished else "", task_regex=TASK_RX,
                counters_spec=CSPEC, params=""))
    with open(os.path.join(HERE, "plan_v9.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(jobs)
    window = (dt.datetime.strptime(deadline, "%Y-%m-%d %H:%M") - dt.datetime.now()).total_seconds()
    sim = simulate(jobs, 124, 44, window)
    tot = sum((sim[j["id"]][1] - sim[j["id"]][0]) * int(j["cores"]) for j in jobs if sim[j["id"]][0] is not None) / 3600
    print(f"{len(jobs)} jobs from {len(plan)} v8 jobs; timing-grade {sum(1 for j in jobs if j['tier'] == 'timing')}, "
          f"reach-grade {sum(1 for j in jobs if j['tier'] == 'reach')}; window {window / 3600:.1f} h")
    print(f"predicted {tot:.0f} core-h of {124 * window / 3600:.0f}")
    t_end = max((sim[j["id"]][1] for j in jobs if j["tier"] == "timing" and sim[j["id"]][1] is not None), default=0)
    print(f"timing phase ends about {t_end / 3600:.1f} h after launch")
    late = [j for j in jobs if sim[j["id"]][2]]
    print(f"{len(late)} jobs predicted to run into the deadline:")
    for j in late:
        s = sim[j["id"]]
        print(f"   {j['id']:<16} {j['side']:<8} cores={j['cores']:>2} pred={float(j['pred_s']) / 3600:>7.1f} h "
              f"start={'never' if s[0] is None else f'{s[0] / 3600:.1f} h'}  {j['cell'][:60]}")
    delayed = [j for j in jobs if j["tier"] == "reach" and sim[j["id"]][0] not in (None, 0.0)]
    print(f"{len(delayed)} reach-grade jobs start after the launch; latest starts:")
    for j in sorted(delayed, key=lambda j: -sim[j["id"]][0])[:6]:
        s = sim[j["id"]]
        print(f"   {j['id']:<16} start {s[0] / 3600:.1f} h, end {s[1] / 3600:.1f} h, pred {float(j['pred_s']) / 3600:.1f} h")


if __name__ == "__main__":
    main()
