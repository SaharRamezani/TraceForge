#!/usr/bin/env python3
"""Compare the v8 rerun on brain05 (results_v8r.csv) with the campaign record
(the c_* columns of plan_v8r.csv). Builds: old = bc296a7, new = e96fe7a,
vsend = bc296a7 + send change only, vrest = e96fe7a with the send change reverted.
Usage: compare_v8r.py [-v]     (-v lists every differing cell)"""
import csv, collections, json, os, re, statistics, sys
HERE = os.path.dirname(os.path.abspath(__file__))
VERBOSE = "-v" in sys.argv
sys.path.insert(0, os.path.dirname(HERE))

def num(pat, s):
    m = re.findall(pat, s or "")
    return int(m[-1]) if m else None

def violations(example, r):
    """Same rule as build_v8_xlsx.py: the example's own count of violating endings."""
    s = r.get("summary", "")
    if example == "nisshi_txn_timed":
        return num(r"dirty_reads=(\d+)", s)
    if example in ("par_timed", "alternating_bit_timed"):
        a, b = num(r"p1_fails=(\d+)", s), num(r"p2_fails=(\d+)", s)
        return None if a is None and b is None else (a or 0) + (b or 0)
    if example == "brp_timed":
        vals = [num(rf"{k}=(\d+)", s) for k in ("p2_runs", "p4_runs")]
        return None if all(v is None for v in vals) else sum(v or 0 for v in vals)
    if example == "sensor_network_timed":
        a, b = num(r"sensor_failures=(\d+)", s), num(r"\bdead=(\d+)", s)
        return None if a is None and b is None else (a or 0) + (b or 0)
    if example == "retail_saga_timed":
        a, b = num(r"oversold=(\d+)", s), num(r"\blate=(\d+)", s)
        return None if a is None and b is None else (a or 0) + (b or 0)
    v = r.get("violations", "")
    return int(v) if v not in ("", None) else num(r"violations=(\d+)", s)

def book(example, r):
    """What the workbook shows for one side of a cell."""
    st = r["status"]
    kind = "FIRE" if "assertion failed" in st else st.split(" (")[0]
    ex, bl = r.get("execs"), r.get("blocked")
    explored = int(ex) + int(bl) if ex not in ("", None) and bl not in ("", None) else None
    imp = r.get("timeline_impossible")
    return dict(status=kind, explored=explored, violations=violations(example, r),
                impossible=int(imp) if imp not in ("", None) else None)

def counters(r):
    """Every name=integer of the summary (last occurrence), without the wall clock."""
    d = {}
    for k, v in re.findall(r"([A-Za-z_][A-Za-z_0-9]*)=(-?\d+)(?![\d.]*s\b)", r.get("summary", "")):
        d[k] = int(v)
    d.pop("time", None)
    for k in ("execs", "blocked", "timeline_impossible", "pruned"):
        if r.get(k) not in ("", None):
            d[k] = int(r[k])
    return d

plan = collections.OrderedDict()
for p in csv.DictReader(open(os.path.join(HERE, "plan_v8r.csv"))):
    plan[p["id"]] = p
res = {r["id"]: r for r in csv.DictReader(open(os.path.join(HERE, "results_v8r.csv")))}
cells = collections.OrderedDict()
for i, p in plan.items():
    cells.setdefault(p["orig_id"], {})[p["build"]] = i
camp = {}
for o, bs in cells.items():
    p = plan[next(iter(bs.values()))]
    camp[o] = dict(status=p["c_status"], exit_code=p["c_exit_code"], execs=p["c_execs"], blocked=p["c_blocked"],
                   timeline_impossible=p["c_timeline_impossible"], pruned=p["c_pruned"], violations=p["c_violations"],
                   wall_s=p["c_wall_s"], cpu_s=p["c_cpu_s"], summary=p["c_summary"], example=p["example"],
                   side=p["side"], family=p["family"], cell=p["cell"], cpus=p["cpus"])

done = collections.Counter(res[i]["build"] for i in res if i in plan)
tot = collections.Counter(p["build"] for p in plan.values())
print("jobs finished / planned: " + ", ".join(f"{b} {done[b]}/{tot[b]}" for b in ("old", "new", "vsend", "vrest")))
bad = [r for i, r in res.items() if i in plan and not r["status"].startswith("finished")]
for r in bad:
    print("  NOT FINISHED:", r["id"], r["status"][:60], "| campaign wall", plan[r["id"]]["c_wall_s"])
try:
    st = json.load(open(os.path.join(HERE, "status_v8r.json")))
    print(f"scheduler at {st['time']}: running {len(st['running'])}, queued {st['queued']}")
except Exception:
    pass

def fin(i):
    return i in res and res[i]["status"].startswith("finished")

# 1. the numbers the workbook shows
print("\n== 1. Workbook numbers (status, explored, violations, timeline_impossible) ==")
both = [o for o, bs in cells.items() if fin(bs.get("old")) and fin(bs.get("new"))]
chg_new, chg_old = [], []
for o in both:
    c = camp[o]
    bc, bo, bn = book(c["example"], c), book(c["example"], res[cells[o]["old"]]), book(c["example"], res[cells[o]["new"]])
    if bn != bc:
        chg_new.append((o, bc, bo, bn))
    if bo != bc:
        chg_old.append((o, bc, bo, bn))
print(f"cells with old and new both finished: {len(both)} of {len(cells)}")
print(f"new (e96fe7a) differs from the campaign record in {len(chg_new)} cells; old (bc296a7) differs in {len(chg_old)} cells")
for o, bc, bo, bn in chg_new:
    c = camp[o]
    diff = {k: (bc[k], bo[k], bn[k]) for k in bc if not (bc[k] == bo[k] == bn[k])}
    print(f"  {o:<12} {c['example']:<26} {c['side']:<8} campaign/old/new: {diff}")
for o, bc, bo, bn in chg_old:
    if o not in [x[0] for x in chg_new]:
        diff = {k: (bc[k], bo[k], bn[k]) for k in bc if not (bc[k] == bo[k] == bn[k])}
        print(f"  {o:<12} (old only) campaign/old/new: {diff}")

# 2. every counter the examples print
print("\n== 2. All printed counters, old vs new ==")
per_family = collections.defaultdict(lambda: [0, 0, collections.Counter()])
lower = higher = 0
for o in both:
    c = camp[o]
    co, cn = counters(res[cells[o]["old"]]), counters(res[cells[o]["new"]])
    keys = [k for k in sorted(set(co) | set(cn)) if co.get(k) != cn.get(k)]
    f = per_family[(c["family"], c["side"])]
    f[0] += 1
    if keys:
        f[1] += 1
        for k in keys:
            f[2][k] += 1
            if co.get(k) is not None and cn.get(k) is not None:
                if cn[k] < co[k]: lower += 1
                else: higher += 1
        if VERBOSE:
            print(f"  {o:<12} " + ", ".join(f"{k} {co.get(k)}->{cn.get(k)}" for k in keys))
for (fam, side), (n, d, ks) in sorted(per_family.items()):
    print(f"  {fam:<7} {side:<8} cells {n:3d}, with a differing counter {d:3d}" + (": " + ", ".join(f"{k}({v})" for k, v in ks.most_common(12)) if d else ""))
print(f"  differing counter values: new lower in {lower}, new higher in {higher}")

# 3. certified failures seen by the tool
print("\n== 3. Tool-side certified failures (task lines), old vs new ==")
tl = [(o, res[cells[o]["old"]]["tasklines"], res[cells[o]["new"]]["tasklines"]) for o in both]
nz = [t for t in tl if t[1] not in ("", "0") or t[2] not in ("", "0")]
df = [t for t in tl if t[1] != t[2]]
print(f"cells with a nonzero count: {len(nz)}; old != new in {len(df)}")
for o, a, b in df:
    print(f"  {o:<12} {camp[o]['example']:<26} {camp[o]['side']:<8} parallel={plan[cells[o]['old']]['parallel']:<12} old {a} new {b}")

# 4. bisect: which part of the commit
print("\n== 4. Bisect (vsend = old + send change only, vrest = new minus send change) ==")
four = [o for o, bs in cells.items() if all(fin(bs.get(b)) for b in ("old", "new", "vsend", "vrest"))]
moved = same = ok = 0
fails = []
for o in four:
    c = {b: counters(res[cells[o][b]]) for b in ("old", "new", "vsend", "vrest")}
    for b in c:
        c[b].pop("pruned", None)
    if c["old"] == c["new"]:
        same += 1
        if not (c["vsend"] == c["new"] and c["vrest"] == c["old"]):
            fails.append((o, "old == new but a variant differs"))
        continue
    moved += 1
    if c["vsend"] == c["new"] and c["vrest"] == c["old"]:
        ok += 1
    else:
        ks = [k for k in c["old"] if not (c["vsend"].get(k) == c["new"].get(k) and c["vrest"].get(k) == c["old"].get(k))]
        fails.append((o, "counters not explained by the send change: " + ", ".join(
            f"{k} old {c['old'].get(k)} vrest {c['vrest'].get(k)} vsend {c['vsend'].get(k)} new {c['new'].get(k)}" for k in ks[:6])))
print(f"cells with all four builds finished: {len(four)}; old == new in {same}; old != new in {moved}, of which "
      f"vsend == new and vrest == old in {ok}")
for o, why in fails:
    print(f"  {o:<12} {camp[o]['example']:<26} parallel={plan[cells[o]['old']]['parallel']:<12} {why}")
key = ["lease-041", "lease-045", "lease-049", "lease-053", "lease-055", "lease-061", "opt-033", "nisshi-037"]
print("  key cells, workbook violations campaign / old / vrest / vsend / new, and task lines old / new:")
for o in key:
    if o not in cells: continue
    ex = camp[o]["example"]
    v = [violations(ex, camp[o])] + [violations(ex, res[cells[o][b]]) if fin(cells[o].get(b)) else "..." for b in ("old", "vrest", "vsend", "new")]
    t = [res[cells[o][b]]["tasklines"] if fin(cells[o].get(b)) else "..." for b in ("old", "new")]
    w = [camp[o]["wall_s"]] + [res[cells[o][b]]["wall_s"] if fin(cells[o].get(b)) else "..." for b in ("old", "new")]
    print(f"  {o:<11} violations {v[0]} / {v[1]} / {v[2]} / {v[3]} / {v[4]}   task lines {t[0]} / {t[1]}   wall campaign/old/new {w[0]} / {w[1]} / {w[2]} s")

# 5. time
print("\n== 5. Time, new / old, same night on brain05 (cells with old wall >= 10 s, exit 0) ==")
rows = []
for o in both:
    ro, rn = res[cells[o]["old"]], res[cells[o]["new"]]
    if ro["exit_code"] != "0" or rn["exit_code"] != "0":
        continue
    try:
        wo, wn, co_, cn_ = float(ro["wall_s"]), float(rn["wall_s"]), float(ro["cpu_s"]), float(rn["cpu_s"])
    except ValueError:
        continue
    if wo >= 10:
        rows.append((o, wo, wn, co_, cn_))
def agg(sel, label):
    if not sel: return
    r = [x[2] / x[1] for x in sel]
    print(f"  {label:<18} cells {len(sel):3d}  wall sum new/old {sum(x[2] for x in sel) / sum(x[1] for x in sel):.3f}  "
          f"cpu sum new/old {sum(x[4] for x in sel) / sum(x[3] for x in sel):.3f}  per-cell wall ratio min {min(r):.2f} median {statistics.median(r):.3f} max {max(r):.2f}")
agg(rows, "all")
for side in ("timed", "baseline"):
    agg([x for x in rows if camp[x[0]]["side"] == side], side)
for fam in sorted({camp[x[0]]["family"] for x in rows}):
    agg([x for x in rows if camp[x[0]]["family"] == fam and camp[x[0]]["side"] == "timed"], fam + " timed")
out = [x for x in rows if not 0.9 <= x[2] / x[1] <= 1.1]
print(f"  cells with a wall ratio outside 0.90-1.10: {len(out)}")
for o, wo, wn, co_, cn_ in sorted(out, key=lambda x: x[2] / x[1]):
    print(f"    {o:<12} {camp[o]['example']:<26} {camp[o]['side']:<8} cpus {camp[o]['cpus']:<2} old {wo:9.1f} s new {wn:9.1f} s ratio {wn / wo:.2f} (cpu {cn_ / co_ if co_ else 0:.2f}); campaign {camp[o]['wall_s']} s")
cw = [(float(camp[x[0]]["wall_s"]), x[1]) for x in rows if float(camp[x[0]]["wall_s"]) >= 10]
if cw:
    print(f"  for reference, old tonight / campaign wall (other load on the machine): sum ratio {sum(b for a, b in cw) / sum(a for a, b in cw):.2f}")
