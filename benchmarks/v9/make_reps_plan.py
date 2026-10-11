#!/usr/bin/env python3
"""More repetitions of the multi-core timing-grade cells (added to the running plan on 2026-10-11).

Why: after two days the three repetitions of every SEQUENTIAL exhaustive cell agree within a few
percent (1 of about 280 sides spreads more than 5%), but the cells that run on a worker pool
do not: about half of them spread more than 5%, some by 30 to 150%, with the same CPU time in
every repetition. The wall time of a pool run depends on how the pool happens to share the
work, not on the machine. Three runs are too few to describe that, so every multi-core
timing-grade side with a wall time of 5 s or more gets four more (repetitions 4 to 7).

Input : plan_v9.csv (the live plan), results_v9.csv
Output: plan_v9_added2.csv (the new rows), plan_v9_new.csv (live plan + new rows)

The rows are queued as tier "reach" so that they do not reserve cores (a queued timing-grade
job reserves 44), and start after the edge rungs; the keeper run keeps the free cores busy, so
they are measured under the same conditions as repetitions 1 to 3.
"""
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from make_plan_v9 import FIELDS  # noqa: E402

REPS = (4, 5, 6, 7)
MIN_WALL_S = 5.0


def main():
    plan = list(csv.DictReader(open(os.path.join(HERE, "plan_v9.csv"))))
    res = {r["id"]: r for r in csv.DictReader(open(os.path.join(HERE, "results_v9.csv")))}
    ids = {r["id"] for r in plan}
    out = []
    for p in plan:
        if p["tier"] != "timing" or p["rep"] != "1" or int(p["cores"]) < 2 or p["purpose"] == "keeper":
            continue
        r = res.get(p["id"])
        if r is None or not r["status"].startswith("finished") or float(r["wall_s"]) < MIN_WALL_S:
            continue
        need = float(r["wall_s"])
        for rep in REPS:
            out.append(dict(p, id=f"{p['id'][:-1]}{rep}", rep=rep, reps=max(REPS), tier="reach", pred_s=f"{need:.0f}",
                            order=f"{1.7 + 0.01 * rep - need / 1e7:.6f}"))
    assert not [j["id"] for j in out if j["id"] in ids]
    with open(os.path.join(HERE, "plan_v9_added2.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(out)
    with open(os.path.join(HERE, "plan_v9_new.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(plan)
        w.writerows(out)
    print(f"{len(out)} runs on {len(out) // len(REPS)} sides, "
          f"{sum(int(j['cores']) * float(j['pred_s']) for j in out) / 3600:.0f} core-h predicted")


if __name__ == "__main__":
    main()
