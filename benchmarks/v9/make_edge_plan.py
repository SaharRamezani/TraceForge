#!/usr/bin/env python3
"""Rows added to the running v9 plan on 2026-10-11 (campaign hour 44).

Input : plan_v9.csv (the launch plan, fetched from brain05) and results_v9.csv
Output: plan_v9_added.csv (the new rows only) and plan_v9_new.csv (launch plan + new rows,
        the file that replaces bench/plan_v9.csv on brain05; the scheduler queues new ids
        within 30 s)

Three kinds of rows, all with new job ids:

  edge   one rung above the top of a ladder, where the measured growth says it can finish
         before the deadline (purpose "edge", ids <family>-eNN-r1). Arguments = the top rung's
         arguments with one size parameter raised. Where the top rung ran on one core and the
         example has a worker pool, the edge rung uses the pool, on both sides alike.
  rep 2  a second run of every reach-grade cell that needs at most 8 h (ids <v8 id>-r2), so
         that a long cell does not rest on a single measurement. Started after everything else.
  keeper one run that cannot finish (id zz-keeper-r1, purpose "keeper", tier "timing"). While a
         timing-grade job is running the scheduler keeps every free measurement core busy with
         a spin loop, so the clock frequency of the remaining runs does not rise as the queue
         empties. It is not a result and is left out of the workbook.
"""
import csv
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from make_plan_v9 import FIELDS  # noqa: E402

REP2_MAX_S = 8 * 3600

# (new id, template v8 id, {flag: value}, extra flags, cell name, cores, parallel, predicted s, expectation)
EDGE = [
    # PAR (Bosnacki-Dams): messages K. Timed top: K=10 with errors (8.4 h), K=14 without.
    ("par-e01", "par-007", {"--messages": 11}, "", "K=11 R=1 To=9 errs 16c (edge rung, timed only)", 16, "shared", 55000,
     "hold; explored about 4.9e8 (x5 per message)"),
    ("par-e02", "par-020", {"--messages": 15}, "", "no-errs K=15 R=1 To=9 4c (edge rung, timed only)", 4, "shared", 43000,
     "hold; explored about 8.6e7 (x3 per message)"),
    ("par-e03", "par-020", {"--messages": 16}, "", "no-errs K=16 R=1 To=9 8c (edge rung, timed only)", 8, "shared", 65000,
     "hold; explored about 2.6e8 (x3 per message)"),
    # BRP family D: chunks n at MAX=1.
    ("brp-e01", "brp-040", {"--n": 6}, "", "D n=6 MAX=1 TR=9 F=1 all w6 (edge rung, timed only)", 6, "--workers 6", 4500,
     "HOLDS, impossible=0; explored about 2.5e7"),
    ("brp-e02", "brp-040", {"--n": 7, "--workers": 12}, "", "D n=7 MAX=1 TR=9 F=1 all w12 (edge rung, timed only)", 12,
     "--workers 12", 45000, "HOLDS, impossible=0; explored about 3.5e8"),
    # nisshi: transactions T and retries R.
    ("nisshi-e01", "nisshi-028", {"--txns": 4, "--retries": 5}, "", "T=4 R=5 L=1 U=5 W=1 B=0 shared4 (edge rung, timed only)", 4,
     "shared", 10000, "hold: ec=0, violations=0"),
    ("nisshi-e02", "nisshi-028", {"--txns": 8, "--retries": 5}, "", "T=8 R=5 L=1 U=5 W=1 B=0 shared4 (edge rung, timed only)", 4,
     "shared", 25000, "hold: ec=0, violations=0"),
    ("nisshi-e03", "nisshi-028", {"--txns": 16, "--retries": 4}, "", "T=16 R=4 L=1 U=5 W=1 B=0 shared4 (edge rung, timed only)", 4,
     "shared", 2500, "hold: ec=0, violations=0"),
    ("nisshi-e04", "nisshi-028", {"--txns": 16, "--retries": 5}, "", "T=16 R=5 L=1 U=5 W=1 B=0 shared8 (edge rung, timed only)", 8,
     "shared", 40000, "hold: ec=0, violations=0"),
    ("nisshi-e05", "nisshi-028", {"--txns": 32, "--retries": 4}, "", "T=32 R=4 L=1 U=5 W=1 B=0 shared4 (edge rung, timed only)", 4,
     "shared", 10000, "hold: ec=0, violations=0"),
    # Sensor network, published timers: rounds R.
    ("sensor-e01", "sensor-062", {"--rounds": 6}, "", "published timers, sensor property, S=2 R=6 kg, p16 (edge rung, timed only)", 16,
     "partitioned", 50000, "exit 0 HOLD; explored about 1.6e9 (x8.8 per round)"),
    # NRP failure detector: rounds R.
    ("nrp-e01", "nrp-021", {"--rounds": 11}, "", "selftest R=11 no-kg (edge rung, timed only)", 1, "none", 48000,
     "hold, violations=0"),
    ("nrp-e02", "nrp-011", {"--rounds": 7}, "", "case8 published leasing R=7 no-kg (edge rung, timed only)", 1, "none", 90000,
     "hold, violations=0"),
    # YARN scheduler: horizon H at K=3 application masters.
    ("opt-e01", "opt-019", {"--horizon": 9}, "", "K=3 QS=4 DD=8 U=0 H=9 (edge rung, timed only)", 1, "none", 2500,
     "hold, violations=0"),
    ("opt-e02", "opt-019", {"--horizon": 11}, "", "K=3 QS=4 DD=8 U=0 H=11 (edge rung, timed only)", 1, "none", 19000,
     "hold, violations=0"),
    # Alternating bit: messages K and retries R. The ladder is sequential; these two use the pool.
    ("abp-e01", "abp-007", {"--messages": 9}, "--parallel shared", "abp K=9 R=2 W=3 (U=1) shared8 (edge rung, timed only)", 8,
     "shared", 60000, "hold; execs about (10/3)*7^9 = 1.3e8"),
    ("abp-e02", "abp-021", {"--retries": 11}, "--parallel shared", "abp K=2 R=11 W=3 shared8 (edge rung, timed only)", 8,
     "shared", 60000, "hold; execs about 3*4095^2 = 5.0e7"),
    # Spanning tree: rounds R, both sides, tight hold setting of the same series (Fd + 2 per round).
    ("stp-e01", "stp-011", {"--rounds": 16, "--forward-delay-ratio": 28}, "--parallel shared",
     "lost-hellos L=U=1 R=16 MA=3 Fd=28 (tight hold, bound MA+Fd<=30) shared8 (edge rung)", 8, "shared", 40000,
     "hold exit 0, violations=0, execs=3^16=43046721"),
    ("stp-e02", "stp-012", {"--rounds": 16, "--forward-delay-ratio": 28}, "--parallel shared",
     "lost-hellos L=U=1 R=16 MA=3 Fd=28 (tight hold, bound MA+Fd<=30) shared8 (edge rung)", 8, "shared", 20000,
     "exit 0, spurious violations"),
    ("stp-e03", "stp-009", {"--rounds": 16, "--forward-delay-ratio": 47}, "--parallel shared",
     "link-up R=16 Fd=47 (tight hold, bound 46) shared8 (edge rung)", 8, "shared", 60000,
     "hold exit 0, violations=0, execs=(3^16-1)/2=21523360"),
    ("stp-e04", "stp-010", {"--rounds": 16, "--forward-delay-ratio": 47}, "--parallel shared",
     "link-up R=16 Fd=47 (tight hold, bound 46) shared8 (edge rung)", 8, "shared", 15000,
     "exit 0, spurious violations"),
    # Ballot leader election: nodes N. One probe above N=3, both sides.
    ("legacy-e01", "legacy-001", {"--nodes": 4}, "", "N=4 B=1 regime D (l=u=20 sd=0 w=200) shared x8 (edge rung)", 8, "shared",
     130000, "hold if it finishes; size unknown"),
    ("legacy-e02", "legacy-002", {"--nodes": 4}, "", "N=4 B=1 regime D (l=u=20 sd=0 w=200) shared x8 (edge rung)", 8, "shared",
     130000, "hold if it finishes; size unknown"),
]

NOW = {"nrp-e01", "nrp-e02"}
LAST = {"legacy-e01", "legacy-e02"}      # the probe of unknown size starts after the other edge rungs
KEEPER_TEMPLATE = "swim-093"     # timed SWIM N=9: predicted far beyond the window


def set_flag(args, flag, value):
    new, n = re.subn(rf"(?<!\S){re.escape(flag)} \S+", f"{flag} {value}", args)
    if n != 1:
        raise SystemExit(f"flag {flag} found {n} times in: {args}")
    return new


def main():
    plan = list(csv.DictReader(open(os.path.join(HERE, "plan_v9.csv"))))
    res = {r["id"]: r for r in csv.DictReader(open(os.path.join(HERE, "results_v9.csv")))}
    ids = {r["id"] for r in plan}
    first = {}
    for r in plan:
        first.setdefault(r["id"].rsplit("-", 1)[0], r)
    out = []
    # ---- edge rungs: after the cells of the launch plan (order < 1), longest first; the two
    # long single-core ones start at once on the cores that the head of the queue cannot use;
    # the leader-election probe, whose size is unknown, starts after the other edge rungs
    for new, tmpl, flags, extra, cell, cores, par, pred, expect in EDGE:
        t = first[tmpl]
        args = t["args"]
        for f, v in flags.items():
            args = set_flag(args, f, v)
        if extra:
            assert extra.split()[0] not in args, (new, args)
            args = f"{args} {extra}"
        out.append(dict(t, id=f"{new}-r1", cell=cell, rep=1, reps=1, tier="reach", purpose="edge", args=args,
                        cores=cores, parallel=par, pred_s=pred,
                        order="0.000200" if new in NOW else "1.600000" if new in LAST else f"{1.5 - pred / 1e7:.6f}",
                        expect=expect, expect_exit="0", regime="", params=""))
    # ---- second repetition of the reach-grade cells of at most 8 h --------------------------
    for r in plan:
        if r["tier"] != "reach" or r["rep"] != "1" or r["purpose"] == "edge":
            continue
        done = res.get(r["id"])
        if done is not None and not done["status"].startswith("finished"):
            continue
        need = float(done["wall_s"]) if done is not None else float(r["pred_s"])
        if need > REP2_MAX_S:
            continue
        out.append(dict(r, id=r["id"][:-1] + "2", rep=2, reps=2, pred_s=f"{need:.0f}",
                        order=f"{3.0 - need / 1e7:.6f}"))
    # ---- keeper ------------------------------------------------------------------------------
    k = first[KEEPER_TEMPLATE]
    out.append(dict(k, id="zz-keeper-r1", cell="keeper: keeps the spin loops on the free cores until the deadline (not a result)",
                    rep=1, reps=1, tier="timing", purpose="keeper", cores=1, order="0.000001",
                    expect="does not finish", expect_exit=""))
    dup = [j["id"] for j in out if j["id"] in ids]
    assert not dup, dup
    assert len({j["id"] for j in out}) == len(out)
    with open(os.path.join(HERE, "plan_v9_added.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(out)
    with open(os.path.join(HERE, "plan_v9_new.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(plan)
        w.writerows(out)
    edge = [j for j in out if j["purpose"] == "edge"]
    rep2 = [j for j in out if j["rep"] == 2]
    print(f"{len(edge)} edge rungs, {sum(int(j['cores']) for j in edge)} cores, "
          f"{sum(int(j['cores']) * float(j['pred_s']) for j in edge) / 3600:.0f} core-h predicted")
    print(f"{len(rep2)} second repetitions, {sum(int(j['cores']) * float(j['pred_s']) for j in rep2) / 3600:.0f} core-h predicted")
    for j in edge:
        print(f"  {j['id']:<15}{j['side']:<9}c={j['cores']:<3}pred={float(j['pred_s']) / 3600:5.1f} h  {j['example']} {j['args']}")


if __name__ == "__main__":
    main()
