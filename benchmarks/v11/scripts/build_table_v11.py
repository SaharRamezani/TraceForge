#!/usr/bin/env python3
"""v11 table: Must (baseline) vs Mustard (timed) per cell, daemon-free bounded programs.

Inputs in data/: results_v11_<m>.csv, plan_v11_<m>.csv, tfstats_<m>.tsv (id <TAB> TFSTATS line), flags_<m>.txt.
Counts come from the TFSTATS line of each run's log:
    Exec    = Stats.execs             (healthy complete executions)
    Viol    = Stats.violating         (once per execution graph)
    Blocked = Stats.block - violating (not violated, did not finish)
    paper Exec = execs + block        (every complete execution, as the paper's column defines it)
Time = median wall of the finished repetitions of a side, in seconds; reps and spread are reported.
A side that did not finish gets a notes entry with its progress and a geometric extrapolation from the two
finished rungs just below it (same other parameters), clearly marked as not measured.
"""
import csv, re, json, statistics as st, collections, math, os, sys

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
D = HERE + "data/"
V10 = "/home/sahar/Downloads/TraceForge/benchmarks/v10/table_final.csv"
MACHINES = ("brain05", "brain10")

ROW = {
    "ct": "Chandra-Toueg", "sensor": "Sensor network", "nrp": "NRP", "stp": "Spanning tree",
    "lease": "Lease lock", "lease_f": "Lease lock (with fencing)", "swim": "SWIM",
    "abp": "Alternating bit", "par": "PAR", "brp": "BRP", "retail": "Online shop", "yarn": "YARN",
}
ORDER = ["ct", "sensor", "nrp", "stp", "lease", "lease_f", "swim", "abp", "par", "brp", "retail", "yarn"]

# ladder keys per family: (name, regex with one group = the integer that grows)
KEYS = {
    "swim": [("N", r"\bN=(\d+)"), ("R", r"\bR=(\d+)")],
    "lease": [("R", r"\bR=(\d+)"), ("C", r"\bC=(\d+)")],
    "lease_f": [("R", r"\bR=(\d+)"), ("C", r"\bC=(\d+)")],
    "par": [("K", r"\bK=(\d+)"), ("R", r"\bR=(\d+)")],
    "abp": [("K", r"\bK=(\d+)"), ("R", r"\bR=(\d+)")],
    "brp": [("MAX", r"\bMAX=(\d+)"), ("n", r"\bn=(\d+)"), ("F", r"\bF=(\d+)")],
    "stp": [("R", r"\bR=(\d+)")],
    "ct": [("H", r"\bH=(\d+)")],
    "sensor": [("rounds", r"--rounds (\d+)"), ("sensors", r"--sensors (\d+)")],
    "yarn": [("H", r"\bH=(\d+)"), ("K", r"\bK=(\d+)")],
    "retail": [("R", r"\bR=(\d+)"), ("C", r"\bC=(\d+)"), ("N", r"\bN=(\d+)")],
    "nrp": [("R", r"\bR=(\d+)")],
}
# parameters that move together with the ladder key and are ignored when matching the other parameters
IGNORE = {
    "brp": [r"\bTR=\d+", r"\bSYNC=\d+"],
    "stp": [r"\bFd=\d+"],
}


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
                        violating=int(m[5]) if m[5] is not None else None)
    return d


def load_all():
    plan, res = {}, []
    for m in MACHINES:
        for r in csv.DictReader(open(D + f"plan_v11_{m}.csv")):
            r["_plan_m"] = m
            plan[r["id"]] = r
    for m in MACHINES:
        tf = load_tfstats(D + f"tfstats_{m}.tsv")
        for r in csv.DictReader(open(D + f"results_v11_{m}.csv")):
            t = tf.get(r["id"])
            fin = r["status"].startswith("finished") or r["status"].startswith("error (exit 3)")
            r["_t"] = t if (fin and t) else None
            r["_fin"] = bool(r["_t"])
            r["_m"] = m
            res.append(r)
    return plan, res


def side_stats(rows):
    fin = [r for r in rows if r["_fin"]]
    if not fin:
        dn = rows
        w = max([f(r["wall_s"]) or 0 for r in dn] or [0])
        pr = next((r["progress"] for r in dn if r["progress"]), "")
        stt = (dn[0]["status"] if dn else "not run")
        return dict(ok=False, wall_last=w, progress=pr, status=stt,
                    notrun=stt.startswith("not run") or not dn)
    walls = sorted(f(r["wall_s"]) for r in fin)
    med = st.median(walls)
    r0 = min(fin, key=lambda r: abs(f(r["wall_s"]) - med))
    t = r0["_t"]
    viol = t["violating"] if t["violating"] is not None else int(r0["viol_execs"] or 0)
    triples = {(r["_t"]["execs"], r["_t"]["block"], r["_t"]["violating"]) for r in fin}
    return dict(ok=True, execs=t["execs"], viol=viol, blocked=t["block"] - viol, block_total=t["block"],
                impossible=t["impossible"], pruned=t["pruned"], wall=round(med, 2), wall_min=walls[0],
                wall_max=walls[-1], reps=len(fin), cpu=f(r0["cpu_s"]),
                q=",".join(sorted({r["timing_quality"] or "clean" for r in fin})),
                exit=r0["exit_code"], host=r0["_m"], same_counts=(len(triples) == 1),
                other_dnf=sum(1 for r in rows if not r["_fin"]))


def signature(label, fam, key_rx):
    s = re.sub(key_rx, lambda m: m.group(0).replace(m.group(1), "#"), label, count=1)
    for rx in IGNORE.get(fam, []):
        s = re.sub(rx, "<ign>", s)
    return s


def extrapolate(fam, cell, side, finished):
    """finished: {(fam, cell, side): wall}. Returns (estimate_s, growth_per_step, key, rungs) or None."""
    best = None
    for name, rx in KEYS.get(fam, []):
        m = re.search(rx, cell)
        if not m:
            continue
        k0 = int(m.group(1))
        sig = signature(cell, fam, rx)
        pts = []
        for (fm, c2, sd), w in finished.items():
            if fm != fam or sd != side or c2 == cell:
                continue
            m2 = re.search(rx, c2)
            if m2 and signature(c2, fam, rx) == sig and int(m2.group(1)) < k0:
                pts.append((int(m2.group(1)), w, c2))
        agg = collections.defaultdict(list)
        for kk, w, c2 in pts:
            agg[kk].append(w)
        pts = sorted((kk, st.median(ws), "") for kk, ws in agg.items())
        if len(pts) >= 2 and pts[-1][1] > 0.5 and pts[-2][1] > 0.05:
            (ka, wa, _), (kb, wb, _) = pts[-2], pts[-1]
            g = (wb / wa) ** (1.0 / (kb - ka))
            if g > 1.05:
                est = wb * g ** (k0 - kb)
                cand = (k0 - kb, est, g, name, [(kk, round(w, 1)) for kk, w, _ in pts[-2:]])
                if best is None or cand[0] < best[0]:
                    best = cand
    if best:
        return best[1], best[2], best[3], best[4]
    return None


def fmt_min(s):
    if s >= 7 * 86400:
        return "more than a week"
    if s >= 86400:
        return "more than a day"
    if s >= 5400:
        return f"{s / 3600:.1f} h"
    return f"{s / 60:.0f} min" if s >= 90 else f"{s:.0f} s"


def main():
    plan, res = load_all()
    v10 = {}
    if os.path.exists(V10):
        for r in csv.DictReader(open(V10)):
            v10[(r["family"], r["cell"])] = r
    cells = collections.OrderedDict()
    ctrl = collections.defaultdict(lambda: {"baseline": [], "timed": []})
    for r in res:
        if r["purpose"] == "control":
            ctrl[(r["family"], r["cell"])][r["side"]].append(r)
            continue
        c = cells.setdefault((r["family"], r["cell"]), {"baseline": [], "timed": [], "purpose": r["purpose"]})
        c[r["side"]].append(r)
    # jobs that never produced a row (still running at fetch time, or not started)
    seen_ids = {r["id"] for r in res}
    pending = collections.Counter()
    for i, p in plan.items():
        if i not in seen_ids and p["purpose"] != "control":
            cells.setdefault((p["family"], p["cell"]), {"baseline": [], "timed": [], "purpose": p["purpose"]})
            pending[(p["family"], p["cell"], p["side"])] += 1

    stats = {}
    for (fam, cell), d in cells.items():
        for side in ("baseline", "timed"):
            stats[(fam, cell, side)] = side_stats(d[side]) if d[side] else dict(ok=False, status="running or not run",
                                                                              wall_last=0, progress="", notrun=True)
    finished = {k: s["wall"] for k, s in stats.items() if s["ok"]}

    rows = []
    for (fam, cell), d in cells.items():
        b, t = stats[(fam, cell, "baseline")], stats[(fam, cell, "timed")]
        m = re.search(r"\bL=(\d+)\b.*?\bU=(\d+)\b.*?\bsd=(\d+)\b", cell)
        if not m:
            for r0 in d["baseline"] + d["timed"]:
                m = re.search(r"\bL=(\d+)\b.*?\bU=(\d+)\b.*?\bsd=(\d+)\b", r0.get("summary") or "")
                if m:
                    break
        L, U, sd = (m[1], m[2], m[3]) if m else ("", "", "")
        notes = []
        for side, s in (("baseline", b), ("timed", t)):
            name = "Must" if side == "baseline" else "Mustard"
            other = t if side == "baseline" else b
            oname = "Mustard" if side == "baseline" else "Must"
            if s["ok"]:
                continue
            if pending.get((fam, cell, side)) and not d[side]:
                notes.append(f"{name} not finished at fetch time (still running or queued)")
                continue
            if s.get("notrun") and s["status"].startswith("not run"):
                notes.append(f"{name} not started before the stop")
                continue
            msg = f"{name} DNF after {fmt_min(s['wall_last'])}"
            if s["progress"]:
                msg += f" ({s['progress'].split(' (')[0]})"
            if other["ok"]:
                msg += f"; {oname} finished in {fmt_min(other['wall'])}"
            ex = extrapolate(fam, cell, side, finished)
            if ex:
                est, g, key, rungs = ex
                when = fmt_min(est)
                when = when if when.startswith("more than") else f"about {when}"
                msg += (f". Estimated finish: {when} (extrapolated, not measured: x{g:.1f} per {key} step "
                        f"from rungs {rungs})")
            else:
                msg += ". No two finished smaller rungs to extrapolate from"
            notes.append(msg)
        for s, nm in ((b, "Must"), (t, "Mustard")):
            if s.get("ok") and not s["same_counts"]:
                notes.append(f"{nm} repetitions DISAGREE on the counts (determinism check failed)")
            if s.get("ok") and s["other_dnf"]:
                notes.append(f"{nm}: {s['other_dnf']} extra repetition(s) did not finish")
        if fam == "ct":
            notes.append("CT caveat: lossy EndOfRun and lossy self-timers; violated in both modes at every non-vacuous cell; "
                         "counts are upper bounds (CAVEATS.md s.1)")
        if fam == "nrp":
            notes.append("Viol. includes the lossy Finished/Stop fates of the termination harness (x2 per dual-primary "
                         "world); daemon-era counts in v10_* columns")
        if fam == "swim":
            notes.append("Viol./Blocked include the fates of the prober's lossy Stop messages (differs per mode); "
                         "daemon-era counts in v10_* columns")
        if fam == "sensor":
            notes.append("termination chain admin->scientist->rescue->timers: Blocked includes lost Stop fates")
        if fam == "yarn":
            notes.append("RM->AM Stop chain after the horizon: Blocked includes lost Stop fates; throughput Viol. adds K*V_end")
        if fam == "brp":
            notes.append("rounds-only: no Done/Terminate; R ends at the last timeout; timed Viol. = daemon-era / 2 "
                         "(Done fates removed); baseline counts shrink (Must over-approximates)")
        if fam == "par":
            notes.append("receiver bounded on K deliveries: tail after the K-th delivery is no longer explored")
        if fam == "abp":
            notes.append("Blocked includes the Done-dropped twin and give-ups")
        if fam == "retail" and "D=None" in cell:
            notes.append("SafeStockLevel control: S=20 covers demand, cannot fail; exit 3 = report-level deadlock, counts valid")
        if fam == "lease_f":
            notes.append("fencing on: assertion unfalsifiable, state space only")
        if fam == "sensor" and "--table3-row" in cell and "readings" not in cell:
            notes.append("random readings; compare sides with care")
        if b.get("q") not in (None, "clean") and b.get("ok"):
            notes.append(f"Must timing flag: {b['q']}")
        if t.get("q") not in (None, "clean") and t.get("ok"):
            notes.append(f"Mustard timing flag: {t['q']}")
        v = v10.get((fam, cell), {})
        rows.append(dict(
            paper_row=ROW.get(fam, fam), family=fam, cell=cell, class_=d["purpose"], L=L, U=U, sd=sd,
            must_exec=b.get("execs", ""), must_viol=b.get("viol", ""), must_blocked=b.get("blocked", ""),
            must_complete_paper=(b["execs"] + b["block_total"]) if b["ok"] else "",
            must_time_s=b["wall"] if b["ok"] else (f">{b['wall_last']:.0f} (DNF)" if b["wall_last"] else ""),
            must_reps=b.get("reps", 0),
            must_spread_pct=(round(100 * (b["wall_max"] - b["wall_min"]) / b["wall"], 1) if b["ok"] and b["wall"] else ""),
            mustard_exec=t.get("execs", ""), mustard_viol=t.get("viol", ""), mustard_blocked=t.get("blocked", ""),
            mustard_complete_paper=(t["execs"] + t["block_total"]) if t["ok"] else "",
            mustard_time_s=t["wall"] if t["ok"] else (f">{t['wall_last']:.0f} (DNF)" if t["wall_last"] else ""),
            mustard_reps=t.get("reps", 0),
            mustard_spread_pct=(round(100 * (t["wall_max"] - t["wall_min"]) / t["wall"], 1) if t["ok"] and t["wall"] else ""),
            time_ratio=(round(t["wall"] / b["wall"], 2) if b["ok"] and t["ok"] and b["wall"] else ""),
            cores=1, host=(b.get("host") or t.get("host") or ""),
            pruned_mustard=t.get("pruned", ""), impossible_mustard=t.get("impossible", ""),
            v10_must_exec=v.get("must_exec", ""), v10_must_viol=v.get("must_viol", ""), v10_must_blocked=v.get("must_blocked", ""),
            v10_mustard_exec=v.get("mustard_exec", ""), v10_mustard_viol=v.get("mustard_viol", ""),
            v10_mustard_blocked=v.get("mustard_blocked", ""),
            notes=" | ".join(notes)))
    order = {k: i for i, k in enumerate(ORDER)}
    rows.sort(key=lambda r: (order.get(r["family"], 99), r["cell"]))
    with open(HERE + "table_v11.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    json.dump(rows, open(HERE + "table_v11.json", "w"))

    # cross-machine controls
    xr = []
    for (fam, cell), d in ctrl.items():
        for side in ("baseline", "timed"):
            if not d[side]:
                continue
            sc = side_stats(d[side])
            sm = stats.get((fam, cell, side))
            if sc["ok"] and sm and sm["ok"]:
                xr.append(dict(paper_row=ROW.get(fam, fam), cell=cell, side=side, main_host=sm["host"], main_s=sm["wall"],
                               control_host=sc["host"], control_s=sc["wall"],
                               ratio=round(sc["wall"] / sm["wall"], 3) if sm["wall"] else "",
                               counts_identical=(sc["execs"], sc["viol"], sc["blocked"]) == (sm["execs"], sm["viol"], sm["blocked"])))
    if xr:
        with open(HERE + "controls_v11.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(xr[0].keys()))
            w.writeheader()
            w.writerows(xr)

    # audit summary
    nfin = sum(1 for r in res if r["_fin"])
    q = collections.Counter(r["timing_quality"] or "-" for r in res if r["_fin"])
    dnf = [r for r in res if r["status"].startswith("DNF")]
    errs = collections.Counter(r["status"] for r in res if not r["_fin"] and not r["status"].startswith("DNF"))
    lines = [f"results rows {len(res)}, finished with TFSTATS {nfin}", f"timing quality: {dict(q)}",
             f"DNF rows {len(dnf)}; other non-finished: {dict(errs)}"]
    for m in MACHINES:
        p = D + f"flags_{m}.txt"
        if os.path.exists(p):
            txt = open(p).read().strip()
            lines.append(f"{m} log flags (Large thread size / panicked): {'none' if not txt else txt.splitlines()}")
    dis = [(r["family"], r["cell"]) for r in rows if "DISAGREE" in r["notes"]]
    lines.append(f"cells whose repetitions disagree on counts: {dis or 'none'}")
    open(HERE + "audit_v11.txt", "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(len(rows), "cells;", len(xr), "control pairs")


if __name__ == "__main__":
    main()
