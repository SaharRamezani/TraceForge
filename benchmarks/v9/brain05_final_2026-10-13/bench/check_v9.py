#!/usr/bin/env python3
"""Status of the v9 campaign (run on brain05, or locally on fetched files)."""
import collections
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
plan = list(csv.DictReader(open(os.path.join(HERE, "plan_v9.csv"))))
res = list(csv.DictReader(open(os.path.join(HERE, "results_v9.csv")))) if os.path.exists(os.path.join(HERE, "results_v9.csv")) else []
st = collections.Counter(r["status"].split(" (")[0] if not r["status"].startswith("finished") else r["status"] for r in res)
print(f"plan: {len(plan)} jobs; recorded: {len(res)}")
for k, v in st.most_common():
    print(f"  {v:>5}  {k}")
q = collections.Counter(r["timing_quality"] == "clean" for r in res if r["status"].startswith("finished") and r["tier"] == "timing")
print(f"timing-grade runs: {q.get(True, 0)} clean, {q.get(False, 0)} flagged by the audit")
bad = [r for r in res if r.get("as_expected") == "NO" or r["status"].startswith("error")]
print(f"unexpected exit codes or errors: {len(bad)}")
for r in bad[:20]:
    print("  ", r["id"], r["status"], "expected exit", r["expect_exit"], "|", r["cell"])
imp = [r for r in res if r.get("timeline_impossible") not in ("", "0")]
print(f"runs with timeline_impossible > 0: {len(imp)}")
p = os.path.join(HERE, "status_v9.json")
if os.path.exists(p):
    s = json.load(open(p))
    print(f"status at {s['time']}: {len(s['running'])} running on {sum(j['cores'] for j in s['running'])} cores, "
          f"{s['queued']} queued ({s['queued_timing']} timing-grade), {s['ballast']} ballast cores, deadline {s['deadline']}")
    for j in sorted(s["running"], key=lambda j: -j["elapsed_s"])[:12]:
        print(f"   {j['id']:<18} cores={j['cores']:>2} elapsed={j['elapsed_s'] / 3600:6.1f} h  predicted={j['pred_s'] / 3600:6.1f} h")
