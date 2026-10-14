#!/usr/bin/env python3
"""Final v10 table: pairs Must (baseline) and Mustard (timed) per cell, brain05 primary, brain10 replication.

Columns per side: Exec (= Stats.execs, no violation), Viol (= Stats.violating, once per graph),
Blocked (= Stats.block - Stats.violating), Time (median wall of the finished repetitions, s).
Counts come from the TFSTATS line of each run's log, not from the example's own counters.
"""
import csv, re, json, statistics as st, collections, math, sys, os

D = os.path.dirname(os.path.abspath(__file__)) + "/"

ROW = {  # paper row (tab:evaluation) per family
    "ct": "Chandra-Toueg", "sensor": "Sensor network", "nrp": "NRP", "stp": "Spanning tree",
    "lease": "Lease lock", "lease_f": "Lease lock (with fencing)", "swim": "SWIM",
    "nisshi": "Kafka/nisshi (P1 only)", "nisshiB": "Kafka/nisshi (P1+P2, supplement)",
    "abp": "Alternating bit", "par": "PAR", "brp": "BRP", "retail": "Online shop", "yarn": "YARN",
}
ORDER = ["ct", "sensor", "nrp", "stp", "lease", "lease_f", "swim", "nisshi", "nisshiB", "abp", "par", "brp",
         "retail", "yarn"]


def load_tfstats(path):
    d = {}
    for line in open(path):
        i, _, s = line.rstrip("\n").partition("\t")
        m = re.search(r"execs=(\d+) blocked=(\d+) timeline_impossible=(\d+) pruned=(\d+)(?: violating=(\d+))?", s)
        if m:
            d[i] = dict(execs=int(m[1]), block=int(m[2]), impossible=int(m[3]), pruned=int(m[4]),
                        violating=int(m[5]) if m[5] is not None else None)
    return d


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load(results, tfs):
    out = []
    for r in csv.DictReader(open(results)):
        t = tfs.get(r["id"])
        fin = r["status"].startswith("finished") or r["status"].startswith("error (exit 3)")
        r["_t"] = t if (fin and t) else None
        r["_fin"] = bool(r["_t"])
        out.append(r)
    return out


def side_stats(rows):
    fin = [r for r in rows if r["_fin"]]
    if not fin:
        dn = [r for r in rows if not r["_fin"]]
        w = max([f(r["wall_s"]) or 0 for r in dn] or [0])
        pr = dn[0]["progress"] if dn else ""
        return dict(ok=False, wall_last=w, progress=pr, status=(dn[0]["status"] if dn else "not run")[:30])
    walls = sorted(f(r["wall_s"]) for r in fin)
    r0 = min(fin, key=lambda r: abs(f(r["wall_s"]) - st.median(walls)))
    t = r0["_t"]
    viol = t["violating"] if t["violating"] is not None else int(r0["viol_execs"] or 0)
    return dict(ok=True, execs=t["execs"], viol=viol, blocked=t["block"] - viol, block_total=t["block"],
                impossible=t["impossible"], pruned=t["pruned"],
                wall=round(st.median(walls), 2), wall_min=walls[0], wall_max=walls[-1], reps=len(fin),
                cpu=f(r0["cpu_s"]), q=",".join(sorted({r["timing_quality"] or "clean" for r in fin})),
                exit=r0["exit_code"])


def tokens(label):
    """integer size tokens of a cell label, e.g. {'R': 3, 'N': 4}"""
    return {k: int(v) for k, v in re.findall(r"\b([A-Za-z]+)=(\d+)\b", label)}


SIZE = ("N", "R", "C", "K", "T", "H", "MAX", "n", "Fd")


def extrapolate(label, fam, side, byfam):
    """geometric extrapolation of the wall time of a DNF side from the two rungs below (same other params)"""
    tk = tokens(label)
    base = re.sub(r"\b([A-Za-z]+)=(\d+)\b", lambda m: m.group(1) + "=#" if m.group(1) in SIZE else m.group(0), label)
    for key in SIZE:
        if key not in tk:
            continue
        w = []
        for step in (1, 2):
            tk2 = dict(tk)
            tk2[key] = tk[key] - step
            lab2 = label
            lab2 = re.sub(r"\b%s=\d+\b" % key, "%s=%d" % (key, tk2[key]), lab2, count=1)
            s = byfam.get((fam, lab2, side))
            if s and s["ok"]:
                w.append(s["wall"])
        if len(w) == 2 and w[1] > 0.5:
            g = w[0] / w[1]
            if g > 1.2:
                return w[0] * g, g, key
    return None


def main():
    tfs5 = load_tfstats(D + "tfstats_b05.tsv")
    tfs10 = load_tfstats(D + "tfstats_b10.tsv")
    r5 = load(D + "results_final_b05.csv", tfs5)
    r10 = load(D + "results_final_b10.csv", tfs10)
    cells = collections.OrderedDict()
    for r in r5:
        cells.setdefault((r["family"], r["cell"]), {"baseline": [], "timed": [], "meta": r})[r["side"]].append(r)
    rep10 = collections.defaultdict(lambda: {"baseline": [], "timed": []})
    for r in r10:
        rep10[(r["family"], r["cell"])][r["side"]].append(r)
    stats = {}
    for (fam, cell), d in cells.items():
        for side in ("baseline", "timed"):
            stats[(fam, cell, side)] = side_stats(d[side]) if d[side] else dict(ok=False, status="not run", wall_last=0, progress="")
    rows = []
    for (fam, cell), d in cells.items():
        b, t = stats[(fam, cell, "baseline")], stats[(fam, cell, "timed")]
        meta = d["meta"]
        m = re.search(r"\bL=(\d+)\b.*?\bU=(\d+)\b.*?\bsd=(\d+)\b", meta["summary"] or "") or \
            re.search(r"\bL=(\d+)\b.*?\bU=(\d+)\b.*?\bsd=(\d+)\b", cell)
        L, U, sd = (m[1], m[2], m[3]) if m else ("", "", "")
        notes = []
        # DNF notes with extrapolation
        for side, s in (("baseline", b), ("timed", t)):
            name = "Must" if side == "baseline" else "Mustard"
            if not s["ok"] and s.get("status") != "not run":
                mins = s["wall_last"] / 60
                other = t if side == "baseline" else b
                msg = f"{name} DNF after {mins:.0f} min ({s['progress'].split(' (')[0]})"
                if other["ok"]:
                    msg += f"; {'Mustard' if side == 'baseline' else 'Must'} finished in {other['wall']/60:.1f} min"
                ex = extrapolate(cell, fam, side, stats_by_label)
                if ex:
                    est, g, key = ex
                    msg += f". Estimated finish about {est/60:.0f} min (extrapolated x{g:.1f} per {key} step, not measured)"
                else:
                    msg += ". No finished smaller rung to extrapolate from"
                notes.append(msg)
        if fam == "ct":
            notes.append("CT caveat: lossy EndOfRun and lossy self-timers; violated in both modes at every non-vacuous cell; "
                         "counts are upper bounds (CAVEATS.md s.1)")
            tk = tokens(cell)
            mm = re.search(r"delta=(\d+) phi=(\d+) to=(\d+) H=(\d+)", cell)
            if mm:
                dl, ph, to, H = map(int, mm.groups())
                if ph >= 2 and math.floor(3 * (H // 3) / ph) + 1 + 1 + 3 * math.ceil(to / 3) <= H:
                    notes.append("EndOfRun-loss artifact possible here by the rough condition (Phi>=2)")
        if fam == "nisshi":
            mm = re.search(r"L=(\d+) U=(\d+) sd=(\d+) W=(\d+) B=(\d+)", cell)
            if mm:
                l, u, sdv, w, bb = map(int, mm.groups())
                if w + bb + 5 * l <= u < w + bb + 7 * l:
                    notes.append("in the dirty-read band [W+B+5L, W+B+7L): P1 holds although P2 is violated; see the P1+P2 supplement")
        if fam == "nrp":
            notes.append("Exec includes dropped-probe twins of dual-primary worlds; blocked = violating by construction (daemon actors)")
        if fam == "retail" and "D=None" in cell:
            notes.append("SafeStockLevel control: S=20 covers demand, cannot fail; exit 3 = report-level deadlock, counts valid")
        if fam in ("lease_f",):
            notes.append("fencing on: assertion unfalsifiable, state space only")
        if fam == "sensor" and "--table3-row" in cell and "readings" not in cell:
            notes.append("random readings; compare sides with care")
        rows.append(dict(
            paper_row=ROW.get(fam, fam), family=fam, cell=cell, L=L, U=U, sd=sd,
            must_exec=b.get("execs", ""), must_viol=b.get("viol", ""), must_blocked=b.get("blocked", ""),
            must_complete_paper=(b["execs"] + b["block_total"]) if b["ok"] else "",
            must_time_s=b.get("wall", "") if b["ok"] else f">{b['wall_last']:.0f} (DNF)" if b.get("status") != "not run" else "",
            must_reps=b.get("reps", 0), must_spread_pct=(round(100 * (b["wall_max"] - b["wall_min"]) / b["wall"], 1) if b["ok"] and b["wall"] else ""),
            mustard_exec=t.get("execs", ""), mustard_viol=t.get("viol", ""), mustard_blocked=t.get("blocked", ""),
            mustard_complete_paper=(t["execs"] + t["block_total"]) if t["ok"] else "",
            mustard_time_s=t.get("wall", "") if t["ok"] else f">{t['wall_last']:.0f} (DNF)" if t.get("status") != "not run" else "",
            mustard_reps=t.get("reps", 0), mustard_spread_pct=(round(100 * (t["wall_max"] - t["wall_min"]) / t["wall"], 1) if t["ok"] and t["wall"] else ""),
            time_ratio=(round(t["wall"] / b["wall"], 2) if b["ok"] and t["ok"] and b["wall"] else ""),
            cores=1, host="brain05", timing_quality=(b.get("q", "") if b["ok"] else "") or (t.get("q", "") if t["ok"] else ""),
            pruned_mustard=t.get("pruned", ""), impossible_mustard=t.get("impossible", ""),
            notes=" | ".join(notes)))
    # cross-machine replication
    xrows = []
    for (fam, cell), d in rep10.items():
        for side in ("baseline", "timed"):
            s10 = side_stats(d[side]) if d[side] else None
            s5 = stats.get((fam, cell, side))
            if s10 and s10["ok"] and s5 and s5["ok"]:
                same = (s10["execs"], s10["viol"], s10["blocked"]) == (s5["execs"], s5["viol"], s5["blocked"])
                xrows.append(dict(paper_row=ROW.get(fam, fam), cell=cell, side=side, brain05_s=s5["wall"], brain10_s=s10["wall"],
                                  ratio=round(s10["wall"] / s5["wall"], 3) if s5["wall"] else "", counts_identical=same))
    order = {k: i for i, k in enumerate(ORDER)}
    rows.sort(key=lambda r: (order.get(r["family"], 99), r["cell"]))
    with open(D + "table_final.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    with open(D + "replication_b10_vs_b05.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(xrows[0].keys()))
        w.writeheader()
        w.writerows(xrows)
    json.dump(rows, open(D + "table_final.json", "w"))
    print(len(rows), "cells;", len(xrows), "cross-machine pairs")


# stats_by_label is needed inside main() for extrapolation: filled lazily
stats_by_label = {}

if __name__ == "__main__":
    # two-pass: first compute finished stats by (family, label, side), then notes with extrapolation
    tfs5 = load_tfstats(D + "tfstats_b05.tsv")
    r5 = load(D + "results_final_b05.csv", tfs5)
    tmp = collections.defaultdict(list)
    for r in r5:
        tmp[(r["family"], r["cell"], r["side"])].append(r)
    for k, v in tmp.items():
        stats_by_label[k] = side_stats(v)
    main()
