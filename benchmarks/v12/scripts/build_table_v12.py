#!/usr/bin/env python3
"""v12 table: one row per Mustard cell (regime x size) joined with its Must baseline class.

Exec = execs + blocked-incl-violating ("complete graphs"), Viol = violating (once per graph), Blocked = block - violating.
Inputs in data/: results_v12_<m>.csv, plan_v12_<m>.csv, tfstats_<m>.tsv ; cells_v12.json (+ cells_v12_B*.json) from the plan generators.
"""
import csv, re, json, glob, statistics as st, collections, os, sys

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
D = HERE + "data/"
MACH = ("brain05", "brain10")


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_tfstats(path):
    d = {}
    for line in open(path):
        i, _, s = line.rstrip("\n").partition("\t")
        m = re.search(r"execs=(\d+) blocked=(\d+) timeline_impossible=(\d+) pruned=(\d+)(?: violating=(\d+))?", s)
        if m:
            d[i] = dict(execs=int(m[1]), block=int(m[2]), impossible=int(m[3]), pruned=int(m[4]),
                        violating=int(m[5]) if m[5] is not None else 0)
    return d


def main():
    cells = {}
    for p in sorted(glob.glob(HERE + "cells_v12*.json")):
        for c in json.load(open(p)):
            cells[c["cid"]] = c
    plan = {}
    res = []
    for m in MACH:
        for r in csv.DictReader(open(D + f"plan_v12_{m}.csv")):
            r["_m"] = m
            plan[r["id"]] = r
    for m in MACH:
        tf = load_tfstats(D + f"tfstats_{m}.tsv")
        for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
            t = tf.get(r["id"])
            fin = r["status"].startswith("finished") or r["status"].startswith("error (exit 3)")
            r["_t"] = t if (fin and t) else None
            r["_fin"] = bool(r["_t"])
            r["_m"] = m
            res.append(r)
    byp = collections.defaultdict(list)       # params (cid or baseline key) -> result rows
    for r in res:
        if r["status"].startswith("removed by the operator"):
            continue          # duplicate repetitions stopped early to free cores
        byp[(r["side"], plan[re.sub(r"-x\d+$", "", r["id"])]["params"])].append(r)

    def cores_of(r):
        return int(float(r["cores"] or 1))

    def stats(rows, want=None):
        fin = [r for r in rows if r["_fin"]]
        if want is not None:
            fw = [r for r in fin if cores_of(r) == want]
            fin = fw or fin
        if not fin:
            if not rows:
                return dict(ok=False, state="queued/running")
            w = max(f(r["wall_s"]) or 0 for r in rows)
            return dict(ok=False, state="DNF", wall_last=w, progress=next((r["progress"] for r in rows if r["progress"]), ""))
        walls = sorted(f(r["wall_s"]) for r in fin)
        med = st.median(walls)
        r0 = min(fin, key=lambda r: abs(f(r["wall_s"]) - med))
        t = r0["_t"]
        return dict(ok=True, execs=t["execs"], viol=t["violating"], blocked=t["block"] - t["violating"],
                    complete=t["execs"] + t["block"], wall=round(med, 2), reps=len(fin),
                    spread=round(100 * (walls[-1] - walls[0]) / med, 1) if med else 0.0,
                    q=",".join(sorted({r["timing_quality"] or "clean" for r in fin})),
                    cores=int(float(r0["cores"] or 1)), parallel=r0.get("parallel", "none"),
                    same=len({(x["_t"]["execs"], x["_t"]["block"], x["_t"]["violating"]) for x in fin}) == 1)

    out = []
    for cid, c in cells.items():
        mrows = byp.get(("timed", cid), [])
        bk = list(map(str, c["bkey"]))
        brows = byp.get(("baseline", "b|" + "|".join(bk)), [])
        if not any(r["_fin"] for r in brows):
            # Must is net- and machine-independent: use the same baseline run on the other machine (counts identical, controls)
            for m2 in MACH:
                if m2 != bk[0]:
                    brows = brows + byp.get(("baseline", "b|" + m2 + "|" + "|".join(bk[1:])), [])
        if not mrows and not brows:
            continue
        cm = {cores_of(r) for r in mrows if r["_fin"]}
        cb = {cores_of(r) for r in brows if r["_fin"]}
        common = cm & cb
        wm = wb = None
        if common:
            wm = wb = min(common)
        else:
            wm = min(cm) if cm else None
            wb = min(cb) if cb else None
        sm, sb = stats(mrows, wm), stats(brows, wb)
        row = dict(cid=cid, family=c["fam"], label=c["label"], size="x".join(map(str, c["size"])), rank=c["rank"],
                   regime=c["regime"], machine=c["machine"], phase=c.get("phase", "A"))
        for pre, s in (("must", sb), ("mustard", sm)):
            if s["ok"]:
                row.update({f"{pre}_exec": s["complete"], f"{pre}_viol": s["viol"], f"{pre}_blocked": s["blocked"],
                            f"{pre}_healthy": s["execs"], f"{pre}_time_s": s["wall"], f"{pre}_reps": s["reps"],
                            f"{pre}_spread_pct": s["spread"], f"{pre}_quality": s["q"],
                            f"{pre}_cores": s["cores"], f"{pre}_parallel": s["parallel"]})
            else:
                row.update({f"{pre}_exec": "", f"{pre}_viol": "", f"{pre}_blocked": "", f"{pre}_healthy": "",
                            f"{pre}_time_s": (f">{s['wall_last']:.0f} (DNF)" if s["state"] == "DNF" else s["state"]),
                            f"{pre}_reps": 0, f"{pre}_spread_pct": "", f"{pre}_quality": "",
                            f"{pre}_cores": "", f"{pre}_parallel": ""})
        mv, tv = f(row["must_viol"]), f(row["mustard_viol"])
        row["verdict"] = ("" if mv is None or tv is None else
                          "FLIP (Must violates, Mustard holds)" if mv > 0 and tv == 0 else
                          "both violate" if mv > 0 and tv > 0 else "both hold" if mv == 0 and tv == 0 else "Mustard only")
        out.append(row)
    out.sort(key=lambda r: (r["family"], r["rank"], r["label"]))
    if not out:
        print("no rows")
        return
    with open(HERE + "table_v12.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    c = collections.Counter((r["family"], r["verdict"] or "pending") for r in out)
    print(len(out), "cells;", f"{sum(1 for r in res if r['_fin'])} finished runs, {sum(1 for r in res if r['status'].startswith('DNF'))} DNF")
    for k, v in sorted(c.items()):
        print("  ", k, v)


if __name__ == "__main__":
    main()
