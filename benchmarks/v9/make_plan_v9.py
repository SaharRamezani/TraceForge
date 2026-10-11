#!/usr/bin/env python3
"""Build the v9 job plan from the per-family design files.

Input : design/<family>.json (one per example; see the "cells" list in each)
Output: plan_v9.csv   one row per job (cell x side x repetition), read by sweep_v9.py
        design_v9.json the merged family metadata (columns, regimes, glossary), read by
                       build_v9_xlsx.py

Rules applied here (the same for every family):
  * the two sides of a cell get byte-identical arguments except the mode flag;
  * timing-grade = both predictions <= 4 h: every side is run 3 times;
    reach-grade  = longer: one run per side; certify cells: one run;
  * queue order: reach-grade jobs longest first, then the timing-grade jobs in
    three passes (all first repetitions, all second, all third), longest first
    inside a pass, so the repetitions of a cell are measured hours apart.

It also replays the scheduler's policy on the predicted times and reports what
would not finish before the deadline.
"""
import argparse
import csv
import datetime as dt
import glob
import heapq
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ORDER = ["lease", "swim", "sensor", "brp", "par", "abp", "ct", "nrp", "nisshi", "stp", "yarn", "retail",
         "le", "three_pc", "two_pc", "raft"]
TIMING_MAX_S = 4 * 3600
FIELDS = ["id", "family", "example", "cell", "regime", "side", "rep", "reps", "tier", "purpose", "args",
          "cores", "parallel", "pred_s", "mem_gb", "order", "expect", "expect_exit", "task_regex",
          "counters_spec", "params"]
REQ_FAMILY = ["family", "example", "title", "property", "mode_timed", "mode_baseline", "failing_task_regex",
              "counters", "param_columns", "regimes", "cells"]
REQ_CELL = ["cell", "regime", "params", "args", "sides", "cores", "parallel", "purpose"]


def load_designs(folder):
    fams, errors = {}, []
    for path in sorted(glob.glob(os.path.join(folder, "*.json"))):
        try:
            d = json.load(open(path))
        except ValueError as e:
            errors.append(f"{os.path.basename(path)}: not valid JSON ({e})")
            continue
        for k in REQ_FAMILY:
            if k not in d:
                errors.append(f"{os.path.basename(path)}: missing key {k}")
        if "family" in d:
            fams[d["family"]] = d
    return fams, errors


def lint(fam):
    """Return a list of problems of one family design."""
    out = []
    name = fam.get("family", "?")
    keys = [c["key"] for c in fam.get("param_columns", [])]
    regimes = {r["id"] for r in fam.get("regimes", [])}
    seen = set()
    for i, c in enumerate(fam.get("cells", [])):
        tag = f"{name} cell {i} ({c.get('cell', '?')})"
        for k in REQ_CELL:
            if k not in c:
                out.append(f"{tag}: missing {k}")
        if c.get("cell") in seen:
            out.append(f"{tag}: duplicate cell name")
        seen.add(c.get("cell"))
        if c.get("regime") not in regimes:
            out.append(f"{tag}: regime {c.get('regime')} is not defined")
        for k in keys:
            if k not in c.get("params", {}):
                out.append(f"{tag}: params lacks {k}")
        a = c.get("args", "")
        for word in (fam.get("mode_timed", "--mode timed"), fam.get("mode_baseline", "--mode baseline")):
            flag = word.split()[0]
            if re.search(rf"(^|\s){re.escape(flag)}(\s|=|$)", a):
                out.append(f"{tag}: args already contains {flag}")
        for s in c.get("sides", []):
            if s not in ("timed", "baseline"):
                out.append(f"{tag}: unknown side {s}")
            if not isinstance(c.get(f"pred_{s}_s"), (int, float)):
                out.append(f"{tag}: pred_{s}_s is not a number")
        if not isinstance(c.get("cores"), int) or c.get("cores", 0) < 1:
            out.append(f"{tag}: cores must be a positive integer")
        text = json.dumps(c, ensure_ascii=False)
        if "—" in text or "–" in text:
            out.append(f"{tag}: contains a dash character that is not allowed")
    text = json.dumps({k: v for k, v in fam.items() if k != "cells"}, ensure_ascii=False)
    if "—" in text or "–" in text:
        out.append(f"{name}: metadata contains a dash character that is not allowed")
    return out


def build_jobs(fams, cap_cores):
    jobs = []
    for name in sorted(fams, key=lambda n: (ORDER.index(n) if n in ORDER else 99, n)):
        fam = fams[name]
        rx = fam["failing_task_regex"].replace("\\d", "[0-9]")
        cspec = json.dumps([{"key": c["key"], "regex": c["regex"]} for c in fam["counters"] if c.get("regex")])
        for i, c in enumerate(fam["cells"], 1):
            preds = [float(c[f"pred_{s}_s"]) for s in c["sides"]]
            certify = c["purpose"] == "certify"
            tier = "timing" if max(preds) <= TIMING_MAX_S else "reach"
            reps = 1 if (tier == "reach" or certify) else 3
            for s in c["sides"]:
                mode = fam["mode_timed"] if s == "timed" else fam["mode_baseline"]
                pred = float(c[f"pred_{s}_s"])
                for rep in range(1, reps + 1):
                    if tier == "reach":
                        order = 1.0 - min(pred, 9.9e6) / 1e7
                    else:
                        order = 10.0 + rep + 0.5 * (1.0 - min(pred, TIMING_MAX_S) / TIMING_MAX_S)
                    jobs.append(dict(
                        id=f"{name}-{i:03d}-{s[0]}-r{rep}", family=name, example=fam["example"], cell=c["cell"],
                        regime=c["regime"], side=s, rep=rep, reps=reps, tier=tier, purpose=c["purpose"],
                        args=(c["args"].strip() + " " + mode).strip(), cores=min(int(c["cores"]), cap_cores),
                        parallel=c["parallel"], pred_s=f"{pred:.0f}", mem_gb=c.get("mem_gb", 128) or 128,
                        order=f"{order:.6f}", expect=c.get(f"expect_{s}", ""),
                        expect_exit=c.get("expect_exit", "") if c.get("expect_exit", "") is not None else "",
                        task_regex=rx, counters_spec=cspec, params=json.dumps(c["params"], sort_keys=True)))
    return jobs


def simulate(jobs, ncores, reserve, window_s):
    """Replay the scheduler on predicted times. Returns per-job (start, end, dnf)."""
    queue = sorted(jobs, key=lambda j: (float(j["order"]), j["id"]))
    free, t, running, res = ncores, 0.0, [], {}
    while queue or running:
        t_queued = any(j["tier"] == "timing" for j in queue)
        t_in_use = sum(c for _, _, c, tier in running if tier == "timing")
        rsv = max(0, reserve - t_in_use) if t_queued else 0
        waiting = set()
        for j in list(queue):
            if j["tier"] in waiting:
                continue
            avail = free - (rsv if j["tier"] != "timing" else 0)
            k = int(j["cores"])
            if k <= avail and t < window_s:
                dur = float(j["pred_s"])
                end = min(t + dur, window_s)
                res[j["id"]] = (t, end, t + dur > window_s)
                heapq.heappush(running, (end, j["id"], k, j["tier"]))
                free -= k
                queue.remove(j)
            elif k > 1:
                waiting.add(j["tier"])
        if not running:
            for j in queue:
                res[j["id"]] = (None, None, True)
            break
        end, _, k, _ = heapq.heappop(running)
        t = end
        free += k
        while running and running[0][0] <= t:
            _, _, k2, _ = heapq.heappop(running)
            free += k2
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--design", default=os.path.join(HERE, "design"))
    ap.add_argument("--out", default=os.path.join(HERE, "plan_v9.csv"))
    ap.add_argument("--meta", default=os.path.join(HERE, "design_v9.json"))
    ap.add_argument("--launch", default=None, help="YYYY-MM-DD HH:MM (default: now)")
    ap.add_argument("--deadline", default="2026-10-13 08:00")
    ap.add_argument("--cores", type=int, default=124)
    ap.add_argument("--reserve-timing", type=int, default=44)
    ap.add_argument("--cap-cores", type=int, default=32)
    ap.add_argument("--force", action="store_true", help="write the plan even if the lint reports problems")
    args = ap.parse_args()

    fams, errors = load_designs(args.design)
    for f in fams.values():
        errors += lint(f)
    for e in errors:
        print("LINT:", e)
    if errors and not args.force:
        sys.exit(f"{len(errors)} problems; fix the design files or pass --force")

    jobs = build_jobs(fams, args.cap_cores)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(jobs)
    json.dump({n: fams[n] for n in sorted(fams, key=lambda n: (ORDER.index(n) if n in ORDER else 99, n))},
              open(args.meta, "w"), indent=1, ensure_ascii=False)

    launch = dt.datetime.strptime(args.launch, "%Y-%m-%d %H:%M") if args.launch else dt.datetime.now()
    window = (dt.datetime.strptime(args.deadline, "%Y-%m-%d %H:%M") - launch).total_seconds()
    sim = simulate(jobs, args.cores, args.reserve_timing, window)
    print(f"\n{len(jobs)} jobs in {sum(len(f['cells']) for f in fams.values())} cells, {len(fams)} families; "
          f"window {window / 3600:.1f} h on {args.cores} cores")
    print(f"{'family':<10} {'cells':>5} {'jobs':>5} {'timing':>6} {'reach':>5} {'core-h':>8} {'DNF pred':>8} {'max cores':>9}")
    tot = 0.0
    for name in fams:
        js = [j for j in jobs if j["family"] == name]
        ch = sum((sim[j["id"]][1] - sim[j["id"]][0]) * int(j["cores"]) for j in js if sim[j["id"]][0] is not None) / 3600
        tot += ch
        print(f"{name:<10} {len(fams[name]['cells']):>5} {len(js):>5} {sum(1 for j in js if j['tier'] == 'timing'):>6} "
              f"{sum(1 for j in js if j['tier'] == 'reach'):>5} {ch:>8.0f} {sum(1 for j in js if sim[j['id']][2]):>8} "
              f"{max(int(j['cores']) for j in js):>9}")
    cap = args.cores * window / 3600
    print(f"total predicted {tot:.0f} core-h of {cap:.0f} available ({100 * tot / cap:.0f}%)")
    t_end = max((sim[j["id"]][1] for j in jobs if j["tier"] == "timing" and sim[j["id"]][1] is not None), default=0)
    print(f"timing phase (constant-load ballast) ends about {t_end / 3600:.1f} h after launch")
    late = [j for j in jobs if sim[j["id"]][2]]
    print(f"{len(late)} jobs predicted not to finish (they run to the deadline and are reported as DNF with their progress):")
    for j in late:
        s = sim[j["id"]]
        print(f"   {j['id']:<22} {j['side']:<8} cores={j['cores']:>2} pred={float(j['pred_s']) / 3600:>7.1f} h "
              f"start={'never' if s[0] is None else f'{s[0] / 3600:.1f} h'}  {j['cell']}")
    delayed = [j for j in jobs if j["tier"] == "reach" and sim[j["id"]][0] not in (None, 0.0) and not sim[j["id"]][2]]
    if delayed:
        print(f"{len(delayed)} reach-grade jobs start later than the launch (all still predicted to finish); latest:")
        for j in sorted(delayed, key=lambda j: -sim[j["id"]][0])[:8]:
            s = sim[j["id"]]
            print(f"   {j['id']:<22} start {s[0] / 3600:.1f} h, end {s[1] / 3600:.1f} h  {j['cell']}")


if __name__ == "__main__":
    main()
