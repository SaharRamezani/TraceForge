#!/usr/bin/env python3
"""Check meta/*.json against plan_v9.csv and results_v9.csv (run before building the workbook)."""
import collections
import csv
import glob
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
BANNED = re.compile(r"untimed|oracle|—|–", re.I)

plan = [p for p in csv.DictReader(open(os.path.join(HERE, "plan_v9.csv"))) if p["purpose"] != "keeper"]
res = [r for r in csv.DictReader(open(os.path.join(HERE, "results_v9.csv"))) if r["purpose"] != "keeper"]
want = collections.defaultdict(set)
for p in plan:
    want[p["example"]].add(p["cell"])
viol = collections.defaultdict(list)
for r in res:
    if r["status"] == "finished" and r["viol_execs"] != "":
        viol[(r["example"], r["cell"], r["side"])].append(int(r["viol_execs"]))
seen = set()
problems = 0
for path in sorted(glob.glob(os.path.join(HERE, "meta", "*.json"))):
    name = os.path.basename(path)
    try:
        m = json.load(open(path))
    except ValueError as e:
        print(f"{name}: NOT JSON ({e})")
        problems += 1
        continue
    ex = m.get("example")
    seen.add(ex)
    out = []
    cells = {c.get("cell"): c for c in m.get("cells", [])}
    missing, extra = want.get(ex, set()) - set(cells), set(cells) - want.get(ex, set())
    if missing:
        out.append(f"{len(missing)} cells missing, e.g. {sorted(missing)[:2]}")
    if extra:
        out.append(f"{len(extra)} cells not in the plan, e.g. {sorted(extra)[:2]}")
    keys = [c["key"] for c in m.get("param_columns", [])]
    regs = {g.get("id") for g in m.get("regimes", [])}
    nokey = [c["cell"] for c in cells.values() if any(k not in c.get("params", {}) for k in keys)]
    if nokey:
        out.append(f"{len(nokey)} cells lack a parameter value, e.g. {nokey[0]!r}")
    noreg = sorted({str(c.get("regime")) for c in cells.values() if c.get("regime") not in regs})
    if noreg:
        out.append(f"undefined regimes used: {noreg}")
    unused = sorted(regs - {c.get("regime") for c in cells.values()})
    if unused:
        out.append(f"regimes without a cell: {unused}")
    bad_suffix = sorted(r for r in regs if not str(r).endswith(("-safe", "-viol", "-edge", "-ctrl")))
    if bad_suffix:
        out.append(f"regime ids without a standard suffix: {bad_suffix}")
    drafts = [k for k, v in m.items() if isinstance(v, str) and "first draft" in v]
    if drafts:
        out.append(f"placeholder text left in: {drafts}")
    text = json.dumps({k: v for k, v in m.items() if k != "cells"}, ensure_ascii=False) + json.dumps(
        [{k: v for k, v in c.items() if k != "cell"} for c in m.get("cells", [])], ensure_ascii=False)
    hits = sorted(set(x.lower() for x in BANNED.findall(text)))
    if hits:
        out.append(f"banned words or dashes: {hits}")
    if not any(c.get("size") for c in m.get("param_columns", [])):
        out.append("no size parameter marked")
    mism = []
    for cell, c in cells.items():
        rid = str(c.get("regime", ""))
        t = viol.get((ex, cell, "timed"))
        if t:
            if rid.endswith("-safe") and max(t) > 0:
                mism.append(f"{cell!r}: {rid} but timed reports {max(t):,} violating executions")
            if rid.endswith("-viol") and max(t) == 0:
                mism.append(f"{cell!r}: {rid} but timed reports no violation")
    if mism:
        out.append(f"{len(mism)} verdict mismatches: " + " ;; ".join(mism[:4]))
    problems += len(out)
    print(f"{name:<34} {ex:<28} cells {len(cells):>3}/{len(want.get(ex, ())):<3} regimes {len(regs):>2} params {len(keys):>2}  "
          + ("OK" if not out else "PROBLEMS: " + " | ".join(out)))
for ex in sorted(set(want) - seen):
    print(f"{'(no file)':<34} {ex:<28} cells   0/{len(want[ex])}")
    problems += 1
print("problems:", problems)
