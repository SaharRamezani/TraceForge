#!/usr/bin/env python3
"""Plan of the v8 rerun on brain05 (2026-10-08).
Every v8 cell that FINISHED within CAP seconds in the campaign is run again with
the commit before (old = bc296a7) and the commit after (new = e96fe7a) the send /
revisit / choice change. The two intermediate builds (vsend, vrest) run on the
cells whose workbook count moved (KEY) and on every timed cell of at most
VARCAP seconds. A rule-A rerun row (<id>-A) replaces its base row."""
import csv, sys
V8, OUT = sys.argv[1], sys.argv[2]
CAP, VARCAP = 14400.0, 600.0
KEY = {"lease-041", "lease-045", "lease-049", "lease-053", "lease-055", "lease-061", "opt-033", "nisshi-037"}
rows = {r["id"]: r for r in csv.DictReader(open(f"{V8}/results_v8.csv"))}
for r in csv.DictReader(open(f"{V8}/results_v8_w0A.csv")):
    rows.pop(r["id"][:-2], None)
    rows[r["id"]] = r
fields = ["id", "orig_id", "build", "family", "group", "example", "cell", "side", "args", "cpus", "parallel",
          "pred_s", "mem_gb", "prio", "purpose", "expected",
          "c_status", "c_exit_code", "c_execs", "c_blocked", "c_timeline_impossible", "c_pruned",
          "c_violations", "c_wall_s", "c_cpu_s", "c_summary"]
out, n_cells, skipped = [], 0, []
for i, r in rows.items():
    if not r["status"].startswith("finished"):
        skipped.append((i, r["status"][:30])); continue
    wall = float(r["wall_s"])
    if wall > CAP:
        skipped.append((i, f"longer than cap ({wall:.0f} s)")); continue
    n_cells += 1
    builds = ["old", "new"]
    if i in KEY or (r["side"] == "timed" and wall <= VARCAP):
        builds += ["vsend", "vrest"]
    for b in builds:
        out.append(dict(id=f"{i}@{b}", orig_id=i, build=b, family=r["group"], group="all", example=r["example"],
                        cell=r["cell"], side=r["side"], args=r["args"], cpus=r["cpus"], parallel=r["parallel"],
                        pred_s=f"{max(wall, 1.0):.1f}", mem_gb=r["mem_gb"], prio=1 if i in KEY else 2,
                        purpose=r["purpose"], expected=r["expected"],
                        c_status=r["status"], c_exit_code=r["exit_code"], c_execs=r["execs"], c_blocked=r["blocked"],
                        c_timeline_impossible=r["timeline_impossible"], c_pruned=r["pruned"],
                        c_violations=r["violations"], c_wall_s=r["wall_s"], c_cpu_s=r["cpu_s"], c_summary=r["summary"]))
with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(out)
ch = sum(float(o["pred_s"]) * int(float(o["cpus"])) for o in out) / 3600
print(f"cells {n_cells}, jobs {len(out)}, core-hours {ch:.0f}, max cpus {max(int(float(o['cpus'])) for o in out)}")
import collections
print("builds", dict(collections.Counter(o["build"] for o in out)))
print("not in the plan:", len(skipped), dict(collections.Counter(s[1].split(' (')[0] if 'longer' in s[1] else s[1] for s in skipped)))
with open(OUT.replace(".csv", "_skipped.txt"), "w") as f:
    for i, why in sorted(skipped): f.write(f"{i}\t{why}\t{rows[i]['side']}\t{rows[i]['cpus']}\t{rows[i]['example']}\t{rows[i]['cell']}\n")
