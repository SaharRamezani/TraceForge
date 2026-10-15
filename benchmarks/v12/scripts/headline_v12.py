#!/usr/bin/env python3
"""Per protocol: the both-finished cell with the largest Must run, and the one with the largest execution reduction."""
import csv, collections
rows = list(csv.DictReader(open("table_v12.csv")))
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def g(x):
    v = num(x); return "-" if v is None else f"{int(round(v)):,}"
def t(x, c):
    v = num(x)
    if v is None: return str(x)[:9]
    s = f"{v:.1f}s" if v < 10 else (f"{v:.0f}s" if v < 7200 else f"{v/3600:.1f}h")
    return s + (f"(c{int(float(c))})" if c and int(float(c)) > 1 else "")
by = collections.defaultdict(list)
for r in rows:
    a, b = num(r["must_exec"]), num(r["mustard_exec"])
    if a is None or b is None or b == 0: continue
    by[r["family"]].append((a, b, a / b, r))
for fam, lst in by.items():
    big = max(lst, key=lambda x: x[0])
    red = max(lst, key=lambda x: x[2])
    for tag, x in (("largest Must", big), ("largest reduction", red)):
        r = x[3]
        print(f"{fam:8s} {tag:18s} {r['label'][:68]:68s} Must {g(r['must_exec']):>14s}/{g(r['must_viol']):>11s}/{t(r['must_time_s'], r.get('must_cores')):>9s} | "
              f"Mustard {g(r['mustard_exec']):>12s}/{g(r['mustard_viol']):>10s}/{t(r['mustard_time_s'], r.get('mustard_cores')):>9s} | {x[2]:>9,.0f}x {r['verdict'][:5]}")
