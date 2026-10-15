#!/usr/bin/env python3
"""Cross-machine controls: identical commands finished on brain05 and brain10 (counts and wall ratio)."""
import csv, re, statistics as st, collections, sys, json
D = "data/"
M = ("brain05", "brain10")
def load_tf(path):
    d = {}
    for line in open(path):
        i, _, s = line.rstrip("\n").partition("\t")
        m = re.search(r"execs=(\d+) blocked=(\d+) timeline_impossible=(\d+) pruned=(\d+)(?: violating=(\d+))?", s)
        if m:
            d[i] = (int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5] or 0))
    return d
rows = {m: {} for m in M}
tf = {m: load_tf(D + f"tfstats_{m}.tsv") for m in M}
plan = {m: {r["id"]: r for r in csv.DictReader(open(D + f"plan_v12_{m}.csv"))} for m in M}
for m in M:
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        i = re.sub(r"-x\d+$", "", r["id"])
        if (r["status"].startswith("finished") or r["status"] == "error (exit 3)") and i in tf[m] and i in plan[m]:
            p = plan[m][i]
            sig = (p["example"], re.sub(r"\s+--(parallel shared|workers \d+)", "", p["args"]))
            rows[m].setdefault(sig, []).append((float(r["wall_s"]), tf[m][i], int(float(r["cores"] or 1)), r["timing_quality"]))
common = set(rows["brain05"]) & set(rows["brain10"])
same, diff, ratios = 0, [], []
out = []
for sig in sorted(common):
    a = rows["brain05"][sig]; b = rows["brain10"][sig]
    # compare 1-core runs only for wall ratio; counts for all
    ca = {x[1] for x in a}; cb = {x[1] for x in b}
    ok = len(ca | cb) == 1
    if ok: same += 1
    else: diff.append(sig)
    a1 = [x[0] for x in a if x[2] == 1]; b1 = [x[0] for x in b if x[2] == 1]
    if a1 and b1 and min(st.median(a1), st.median(b1)) >= 5:
        ratios.append(st.median(b1) / st.median(a1))
    out.append((sig, ok, st.median([x[0] for x in a]), st.median([x[0] for x in b])))
print(f"{len(common)} identical commands finished on both machines; counts identical in {same}; differing {len(diff)}")
if ratios:
    print(f"wall ratio brain10/brain05 over {len(ratios)} pairs with >= 5 s: median {st.median(ratios):.3f}, min {min(ratios):.3f}, max {max(ratios):.3f}")
    qs = sorted(ratios)
    print("   quartiles:", [round(qs[int(len(qs) * f)], 3) for f in (0.1, 0.25, 0.5, 0.75, 0.9)])
for sig in diff:
    print("DIFF", sig[0], sig[1][:100])
json.dump([dict(example=s[0], args=s[1], same=ok, wall05=a, wall10=b) for s, ok, a, b in out], open("controls_v12.json", "w"))
