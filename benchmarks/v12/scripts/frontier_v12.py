#!/usr/bin/env python3
"""Per family and (regime, variant): largest both-finished cell and largest Mustard-only cell. Text report + CSV."""
import csv, re, sys, collections, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
rows = list(csv.DictReader(open("table_v12.csv")))
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def g(x):
    v = num(x)
    return "-" if v is None else f"{int(round(v)):,}"
def tf(x):
    v = num(x)
    if v is None: return str(x)[:12]
    return f"{v:.1f}s" if v < 10 else (f"{v:.0f}s" if v < 3600 else f"{v/3600:.2f}h")
only = sys.argv[1] if len(sys.argv) > 1 else None
byf = collections.OrderedDict()
for r in rows:
    byf.setdefault(r["family"], []).append(r)
for fam, rs in byf.items():
    if only and fam != only:
        continue
    print(f"\n=== {fam}")
    grp = collections.OrderedDict()
    for r in rs:
        l = r["label"]
        m = re.search(r"net=([\w-]+)", l)
        key = (r["regime"],)
        grp.setdefault(key, []).append(r)
    for key, lst in sorted(grp.items()):
        both = [r for r in lst if num(r["mustard_time_s"]) is not None and num(r["must_time_s"]) is not None]
        mo = [r for r in lst if num(r["mustard_time_s"]) is not None and num(r["must_time_s"]) is None]
        def line(tag, r):
            return (f"   {tag:6s} {r['label'][:64]:64s} Must {g(r['must_exec']):>12s}/{g(r['must_viol']):>10s}/{tf(r['must_time_s']):>8s} | "
                    f"Mustard {g(r['mustard_exec']):>11s}/{g(r['mustard_viol']):>9s}/{tf(r['mustard_time_s']):>8s} {r['verdict'][:4]} c{r['mustard_cores'] or 1}")
        print(f" {key[0]}: {len(lst)} cells, {len(both)} both, {len(mo)} Mustard-only")
        if both:
            print(line("BOTH", max(both, key=lambda r: max(num(r["must_time_s"]), num(r["mustard_time_s"])))))
            fl = [r for r in both if r["verdict"].startswith("FLIP")]
            if fl:
                print(line("FLIP", max(fl, key=lambda r: num(r["must_time_s"]))))
        if mo:
            print(line("MONLY", max(mo, key=lambda r: num(r["mustard_time_s"]))))
