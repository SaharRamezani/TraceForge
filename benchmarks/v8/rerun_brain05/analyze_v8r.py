#!/usr/bin/env python3
"""Second look at the v8 rerun: reproducibility of the old counters, the task-line
count in the shared pool, and time (geometric means, baseline cells as the control)."""
import csv, collections, math, os, re, statistics, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import importlib.util
spec = importlib.util.spec_from_file_location("cmp", os.path.join(HERE, "compare_v8r.py"))
plan = collections.OrderedDict((p["id"], p) for p in csv.DictReader(open(os.path.join(HERE, "plan_v8r.csv"))))
res = {r["id"]: r for r in csv.DictReader(open(os.path.join(HERE, "results_v8r.csv")))}
cells = collections.OrderedDict()
for i, p in plan.items():
    cells.setdefault(p["orig_id"], {})[p["build"]] = i

def counters(summary, r=None):
    d = {}
    for k, v in re.findall(r"([A-Za-z_][A-Za-z_0-9]*)=(-?\d+)(?![\d.]*s\b)", summary or ""):
        d[k] = int(v)
    d.pop("time", None)
    if r:
        for k in ("execs", "blocked", "timeline_impossible", "pruned"):
            if r.get(k) not in ("", None):
                d[k] = int(r[k])
    return d
def C(o, b): return counters(res[cells[o][b]]["summary"], res[cells[o][b]])
def P(o): return plan[cells[o]["old"]]
def gm(xs): return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float("nan")
def pct(xs, q):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(q * len(xs)))]

print("== A. Is the old side reproducible? campaign vs old (bc296a7), every printed counter ==")
fam = collections.defaultdict(lambda: [0, 0, collections.Counter(), 0.0])
for o in cells:
    p = P(o)
    cc, co = counters(p["c_summary"]), C(o, "old")
    for k, col in (("execs", "c_execs"), ("blocked", "c_blocked"), ("pruned", "c_pruned")):
        if p[col] != "": cc[k] = int(p[col])
    keys = [k for k in cc if k in co and cc[k] != co[k]]
    f = fam[(p["family"], p["side"], p["parallel"].split()[0])]
    f[0] += 1
    if keys:
        f[1] += 1
        for k in keys:
            f[2][k] += 1
            if cc[k]: f[3] = max(f[3], abs(co[k] - cc[k]) / abs(cc[k]))
for k, (n, d, ks, mx) in sorted(fam.items()):
    if d: print(f"  {k[0]:<7} {k[1]:<8} {k[2]:<12} cells {n:3d}, old != campaign in {d:3d} (largest relative gap {mx:.2e}): " + ", ".join(f"{a}({b})" for a, b in ks.most_common(8)))
print("  (families not listed: old == campaign in every counter)")

print("\n== B. Bisect, restated ==")
four = [o for o, bs in cells.items() if len(bs) == 4]
moved = [o for o in four if {k: v for k, v in C(o, "old").items()} != C(o, "new")]
vs = [o for o in moved if C(o, "vsend") == C(o, "new")]
vr = [o for o in moved if C(o, "vrest") == C(o, "old")]
print(f"cells with four builds {len(four)}; some counter (incl. pruned) differs old vs new in {len(moved)}; vsend == new in {len(vs)}; vrest == old in {len(vr)}")
bad = [o for o in moved if o not in vs]
for o in bad: print("  vsend != new:", o, P(o)["example"], P(o)["parallel"], {k: (C(o, 'vsend').get(k), C(o, 'new').get(k)) for k in C(o, 'new') if C(o, 'vsend').get(k) != C(o, 'new').get(k)})
nr = [o for o in moved if o not in vr]
print("  vrest != old:", collections.Counter((P(o)["example"], P(o)["parallel"].split()[0]) for o in nr))
for o in nr[:40]:
    p = P(o); cc = counters(p["c_summary"]); co, cr, cn = C(o, "old"), C(o, "vrest"), C(o, "new")
    ks = [k for k in co if co[k] != cr.get(k)]
    print(f"    {o:<10} " + "; ".join(f"{k}: campaign {cc.get(k)} old {co[k]} vrest {cr.get(k)} new {cn.get(k)}" for k in ks[:3]))
same = [o for o in four if o not in moved]
odd = [o for o in same if not (C(o, "vsend") == C(o, "new") == C(o, "vrest"))]
print(f"  cells where old == new: {len(same)}; a variant differs there in {len(odd)}", odd[:10])

print("\n== C. Task lines in the shared pool ==")
for o in ("lease-061", "lease-062", "swim-036", "ct-012"):
    if o in cells:
        ex = P(o)["example"]
        print(f"  {o} {P(o)['side']}: task lines " + ", ".join(f"{b} {res[i]['tasklines']}" for b, i in cells[o].items()) + " | violations= " + ", ".join(f"{b} {res[i]['violations']}" for b, i in cells[o].items()))
tl4 = [(o, [res[cells[o][b]]["tasklines"] for b in ("old", "vrest", "vsend", "new")]) for o in four]
d4 = [(o, t) for o, t in tl4 if len(set(t)) > 1]
print(f"  four-build cells with unequal task lines: {len(d4)} of {len(four)}")
for o, t in d4: print("   ", o, P(o)["example"], P(o)["parallel"], "old/vrest/vsend/new", t)
by = collections.Counter()
for o, bs in cells.items():
    a, b = res[bs["old"]]["tasklines"], res[bs["new"]]["tasklines"]
    by[(P(o)["parallel"].split()[0], a == b, a not in ("", "0"))] += 1
print("  (parallel mode, old == new, nonzero) ->", dict(by))

print("\n== D. Time ==")
def T(o, b, col="wall_s"): return float(res[cells[o][b]][col])
ok = [o for o in cells if all(res[i]["exit_code"] == "0" for i in cells[o].values()) and T(o, "old") >= 10]
def line(sel, label, col="wall_s"):
    r = [T(o, "new", col) / T(o, "old", col) for o in sel if T(o, "old", col) > 0]
    if r: print(f"  {label:<34} cells {len(r):3d}  geometric mean {gm(r):.3f}  p10 {pct(r, .1):.2f} median {statistics.median(r):.3f} p90 {pct(r, .9):.2f}  sum ratio {sum(T(o, 'new', col) for o in sel) / sum(T(o, 'old', col) for o in sel):.3f}")
for side in ("baseline", "timed"):
    for par in ("none", "shared", "partitioned"):
        sel = [o for o in ok if P(o)["side"] == side and P(o)["parallel"].split()[0] == par]
        line(sel, f"{side} {par} wall new/old")
        line(sel, f"{side} {par} cpu  new/old", "cpu_s")
print("  four-build timed cells (old wall >= 10 s): effect of each half of the commit, geometric mean over cells")
f4 = [o for o in ok if len(cells[o]) == 4]
for par in ("none", "shared"):
    sel = [o for o in f4 if P(o)["parallel"].split()[0] == par]
    if not sel: continue
    for col in ("wall_s", "cpu_s"):
        send = [math.sqrt(T(o, "vsend", col) * T(o, "new", col) / (T(o, "old", col) * T(o, "vrest", col))) for o in sel]
        rest = [math.sqrt(T(o, "vrest", col) * T(o, "new", col) / (T(o, "old", col) * T(o, "vsend", col))) for o in sel]
        aa = [T(o, "vrest", col) / T(o, "old", col) for o in sel]
        print(f"    {par:<7} {col:<7} cells {len(sel):3d}: send change {gm(send):.3f}, rest of the commit {gm(rest):.3f}; spread of one pair (vrest/old) p10 {pct(aa, .1):.2f} p90 {pct(aa, .9):.2f}")
print("  timed cells, by family (wall, geometric mean new/old; baseline of the same family as control)")
for f in sorted({P(o)["family"] for o in ok}):
    t = [T(o, "new") / T(o, "old") for o in ok if P(o)["family"] == f and P(o)["side"] == "timed"]
    b = [T(o, "new") / T(o, "old") for o in ok if P(o)["family"] == f and P(o)["side"] == "baseline"]
    print(f"    {f:<7} timed {gm(t):.3f} ({len(t)} cells)   baseline {gm(b):.3f} ({len(b)} cells)")
print("  chandra_toueg timed cells:")
for o in cells:
    if P(o)["family"] == "ct" and P(o)["side"] == "timed" and float(P(o)["c_wall_s"]) >= 100:
        print(f"    {o:<8} {P(o)['parallel']:<20} campaign {P(o)['c_wall_s']:>8} s cpu {P(o)['c_cpu_s']:>9} | old {T(o,'old'):8.1f} s cpu {T(o,'old','cpu_s'):9.1f} | new {T(o,'new'):8.1f} s cpu {T(o,'new','cpu_s'):9.1f} | started old {res[cells[o]['old']]['started'][11:]} new {res[cells[o]['new']]['started'][11:]}")
