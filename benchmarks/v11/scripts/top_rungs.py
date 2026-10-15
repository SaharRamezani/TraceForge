#!/usr/bin/env python3
"""Per paper row: the highest rungs, in four groups, from table_v11.csv.

  both   : both sides finished (largest Must wall first)  -> candidates for the paper table
  timed  : only Mustard finished (Must DNF)               -> "Must does not finish, Mustard does"
  must   : only Must finished (Mustard DNF)
  none   : neither finished
"""
import csv, sys, collections

HERE = "/tmp/claude-1000/-home-sahar-Downloads-TraceForge/3b473ba1-d1a5-4cfc-9815-aa2ca73fa0e8/scratchpad/v11/"
rows = list(csv.DictReader(open(HERE + "table_v11.csv")))
K = int(sys.argv[1]) if len(sys.argv) > 1 else 3


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fmt_t(x):
    v = num(x)
    if v is None:
        return str(x) or "-"
    return f"{v:.0f}s" if v < 600 else f"{v / 60:.1f}m"


by = collections.OrderedDict()
for r in rows:
    by.setdefault(r["paper_row"], []).append(r)
for prow, rs in by.items():
    print(f"\n### {prow}")
    both = [r for r in rs if num(r["must_time_s"]) is not None and num(r["mustard_time_s"]) is not None]
    timed = [r for r in rs if num(r["must_time_s"]) is None and num(r["mustard_time_s"]) is not None]
    must = [r for r in rs if num(r["must_time_s"]) is not None and num(r["mustard_time_s"]) is None]
    for name, grp, key in (("both", both, lambda r: -max(num(r["must_time_s"]), num(r["mustard_time_s"]))),
                           ("only Mustard", timed, lambda r: -num(r["mustard_time_s"])),
                           ("only Must", must, lambda r: -num(r["must_time_s"]))):
        for r in sorted(grp, key=key)[:K]:
            print(f"  [{name:12s}] {r['cell'][:58]:58s} Must {r['must_exec']}/{r['must_viol']}/{r['must_blocked']} {fmt_t(r['must_time_s'])}"
                  f" (x{r['must_reps']}) | Mustard {r['mustard_exec']}/{r['mustard_viol']}/{r['mustard_blocked']} {fmt_t(r['mustard_time_s'])} (x{r['mustard_reps']})")
    nn = [r for r in rs if num(r["must_time_s"]) is None and num(r["mustard_time_s"]) is None]
    for r in nn[:K]:
        print(f"  [neither     ] {r['cell'][:58]:58s} Must {r['must_time_s']} | Mustard {r['mustard_time_s']}")
