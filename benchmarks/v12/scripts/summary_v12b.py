#!/usr/bin/env python3
"""RESULTS_v12.md: campaign facts, verdict statistics, boundary laws, frontiers, parallel runs, controls.

Inputs (scratchpad/v12): table_v12.csv, data/*, controls_v12.json (controls_v12.py), frontier_v12.csv (make_tex_v12.py)
"""
import csv, json, re, os, sys, collections, statistics as st, datetime

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
OUT = sys.argv[1] if len(sys.argv) > 1 else HERE
D = HERE + "data/"
rows = list(csv.DictReader(open(HERE + "table_v12.csv")))


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def g(x):
    v = num(x)
    return "-" if v is None else f"{int(round(v)):,}"


def tf(x):
    v = num(x)
    if v is None:
        return str(x)[:14]
    return f"{v:.1f} s" if v < 10 else (f"{v:.0f} s" if v < 3600 else f"{v / 3600:.2f} h")


md = []
P = md.append
P("# v12 results (generated)\n")

# campaign facts
res_rows, plan_rows = [], {}
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        r["_m"] = m
        res_rows.append(r)
    for r in csv.DictReader(open(D + f"plan_v12_{m}.csv")):
        plan_rows[r["id"]] = r
fin = [r for r in res_rows if r["status"].startswith("finished") or r["status"] == "error (exit 3)"]
dnf = [r for r in res_rows if r["status"].startswith("DNF")]
rem = [r for r in res_rows if "removed by the operator" in r["status"]]
par = [r for r in fin if int(float(r["cores"] or 1)) > 1]
P("## Campaign\n")
P(f"- finished runs: {len(fin):,}; DNF at the deadline: {len(dnf)}; stopped by the operator (not results): {len(rem)}; "
  f"parallel runs (8 or 16 workers): {len(par)}")
P(f"- cells in the table: {len(rows):,}; Mustard finished in {sum(1 for r in rows if num(r['mustard_time_s']) is not None):,}; "
  f"both tools finished in {sum(1 for r in rows if num(r['mustard_time_s']) is not None and num(r['must_time_s']) is not None):,}")
try:
    ctl = json.load(open(HERE + "controls_v12.json"))
    same = sum(1 for c in ctl if c["same"])
    P(f"- cross-machine controls: {len(ctl)} identical commands finished on both machines, counts identical in {same}")
except Exception:
    pass
P("")

# verdicts per family
P("## Verdicts per protocol\n")
P("| protocol | cells | Mustard finished | both finished | flips (Must violates, Mustard holds) | both violate | both hold | pending |")
P("|---|---|---|---|---|---|---|---|")
byf = collections.OrderedDict()
for r in rows:
    byf.setdefault(r["family"], []).append(r)
for fam, rs in byf.items():
    c = collections.Counter(r["verdict"] or "pending" for r in rs)
    mf = sum(1 for r in rs if num(r["mustard_time_s"]) is not None)
    bf = sum(1 for r in rs if num(r["mustard_time_s"]) is not None and num(r["must_time_s"]) is not None)
    P(f"| {fam} | {len(rs)} | {mf} | {bf} | {c.get('FLIP (Must violates, Mustard holds)', 0)} | {c.get('both violate', 0)} | "
      f"{c.get('both hold', 0)} | {c.get('pending', 0)} |")
P("")

# lease law
ok = bad = 0
badl = []
byreg = collections.defaultdict(lambda: [0, 0])
for r in rows:
    if r["family"] != "lease":
        continue
    m = re.search(r"net=([\w-]+)\[(\d+),(\d+)\] sd=(\d+) TTL=(\d+) PAUSE=(\d+)", r["label"])
    mv = num(r["mustard_viol"])
    if not m or mv is None:
        continue
    L, U, sd, ttl, p = (int(m.group(i)) for i in (2, 3, 4, 5, 6))
    if (p >= ttl - 2 * (U - L) - sd) == (mv > 0):
        ok += 1
        byreg[r["regime"]][0] += 1
    else:
        bad += 1
        byreg[r["regime"]][1] += 1
        badl.append(r["label"])
P("## Lease: closed-form boundary\n")
P(f"A violation is feasible iff PAUSE >= TTL - 2(U - L) - sd. Checked on every finished lease cell: {ok} consistent, {bad} not.")
for b in badl[:10]:
    P(f"  - INCONSISTENT: {b}")
P("")
P("| regime | cells consistent | inconsistent |")
P("|---|---|---|")
for k, (a, b) in sorted(byreg.items()):
    P(f"| {k} | {a} | {b} |")
P("")

# PAR law
okp = badp = 0
badpl = []
for r in rows:
    if r["family"] != "abp" or not r["label"].startswith("ABP-file par"):
        continue
    m = re.match(r"ABP-file par K=(\d+) R=(\d+) W=(\d+) net=\w+\[(\d+),(\d+)\] sd=(\d+)", r["label"])
    mv = num(r["mustard_viol"])
    if not m or mv is None:
        continue
    W, L, U, sd = (int(m.group(i)) for i in (3, 4, 5, 6))
    pred = (L <= W <= 2 * U + sd)
    if pred == (mv > 0):
        okp += 1
    else:
        badp += 1
        badpl.append(r["label"])
P("## PAR (ABP file, variant par): closed-form window\n")
P(f"A violation (premature retransmission accepted as a new frame) is feasible iff L <= W <= 2U + sd. "
  f"Checked on every finished par cell: {okp} consistent, {badp} not.")
for b in badpl[:10]:
    P(f"  - INCONSISTENT: {b}")
P("")

# frontiers
P("## Frontiers per protocol and regime\n")
P("Both = largest cell where both tools finished; Mustard only = largest Mustard cell with the Must baseline unfinished. "
  "Times in seconds on one core unless marked (c16 = 16 workers).\n")
for fam, rs in byf.items():
    P(f"### {fam}\n")
    grp = collections.OrderedDict()
    for r in rs:
        grp.setdefault(r["regime"], []).append(r)
    for key, lst in sorted(grp.items()):
        both = [r for r in lst if num(r["mustard_time_s"]) is not None and num(r["must_time_s"]) is not None]
        mo = [r for r in lst if num(r["mustard_time_s"]) is not None and num(r["must_time_s"]) is None]
        if not both and not mo:
            continue

        def line(tag, r):
            c1 = int(num(r.get("must_cores")) or 1)
            c2 = int(num(r.get("mustard_cores")) or 1)
            return (f"- {tag}: `{r['label']}`: Must {g(r['must_exec'])} / {g(r['must_viol'])} / {tf(r['must_time_s'])}"
                    f"{' (c%d)' % c1 if c1 > 1 else ''}; Mustard {g(r['mustard_exec'])} / {g(r['mustard_viol'])} / "
                    f"{tf(r['mustard_time_s'])}{' (c%d)' % c2 if c2 > 1 else ''}; {r['verdict'] or 'Must pending'}")
        P(f"**{key}** ({len(lst)} cells)")
        if both:
            b = max(both, key=lambda r: max(num(r["must_time_s"]), num(r["mustard_time_s"])))
            P(line("both", b))
            fl = [r for r in both if r["verdict"].startswith("FLIP")]
            if fl:
                P(line("largest flip", max(fl, key=lambda r: num(r["must_time_s"]))))
        if mo:
            P(line("Mustard only", max(mo, key=lambda r: num(r["mustard_time_s"]))))
        P("")



# size frontier per protocol
P("## Largest finished sizes per protocol and dimension\n")
P("For each size coordinate: the largest value reached by a finished cell, with the full size of that cell. "
  "Size coordinates: lease (C, R); SWIM (N, R); nisshi (T, R); ABP/PAR (K, R); BRP (F, n, MAX); NRP (R); shop (C, N, R); YARN (K, H).\n")
P("| protocol | coordinate | Mustard max (size) | Must max (size) |")
P("|---|---|---|---|")
NAMES = {"lease": ["C", "R"], "lease_f": ["C", "R"], "swim": ["N", "R"], "nisshi": ["T", "R"], "abp": ["K", "R"], "brp": ["n(F=...)", "MAX", "F"],
         "nrp": ["R"], "retail": ["C", "N", "R"], "yarn": ["K", "H"]}
for fam, rs in byf.items():
    sizes = []
    for r in rs:
        try:
            sz = tuple(int(x) for x in r["size"].split("x"))
        except ValueError:
            continue
        if fam in ("lease", "lease_f"):
            mm = re.search(r"net=[\w-]+\[(\d+),(\d+)\] sd=(\d+) TTL=(\d+) PAUSE=(\d+)", r["label"])
            if mm:
                L_, U_, sd_, ttl_, p_ = (int(mm.group(i)) for i in range(1, 6))
                if p_ not in (ttl_ - 2 * (U_ - L_) - sd_ - 1, ttl_ - 2 * (U_ - L_) - sd_):
                    continue           # only cells at the boundary count as frontier
        sizes.append((sz, num(r["mustard_time_s"]) is not None, num(r["must_time_s"]) is not None, r))
    if not sizes:
        continue
    dims = len(sizes[0][0])
    for d in range(dims):
        def best(idx):
            c = [x for x in sizes if x[idx]]
            if not c:
                return "-"
            m = max(c, key=lambda x: (x[0][d], x[0]))
            return f"{m[0][d]} ({'x'.join(map(str, m[0]))})"
        nm = NAMES.get(fam, [str(i) for i in range(dims)])[d] if d < len(NAMES.get(fam, [])) else str(d)
        P(f"| {fam} | {nm} | {best(1)} | {best(2)} |")
P("")
# reductions
P("## Reductions where both tools finished (cells with Must time >= 10 s)\n")
P("Exec. ratio = Must Exec. / Mustard Exec.; time ratio = Must time / Mustard time (> 1: Mustard faster). Parallel runs excluded.\n")
P("| protocol | regime | cells | Exec. ratio median | Exec. ratio max | time ratio median | time ratio max | Mustard slower in |")
P("|---|---|---|---|---|---|---|---|")
for fam, rs in byf.items():
    grp = collections.defaultdict(list)
    for r in rs:
        a, b = num(r["must_exec"]), num(r["mustard_exec"])
        ta, tb = num(r["must_time_s"]), num(r["mustard_time_s"])
        if None in (a, b, ta, tb) or ta < 10 or b == 0:
            continue
        if int(num(r.get("must_cores")) or 1) > 1 or int(num(r.get("mustard_cores")) or 1) > 1:
            continue
        net = r["regime"].split("/")[0].replace("-sd", "")
        grp[net].append((a / b, ta / tb))
    for net, lst in sorted(grp.items()):
        er = sorted(x[0] for x in lst)
        tr = sorted(x[1] for x in lst)
        slower = sum(1 for x in tr if x < 1)
        P(f"| {fam} | {net} | {len(lst)} | {er[len(er) // 2]:.1f} | {er[-1]:.0f} | {tr[len(tr) // 2]:.1f} | {tr[-1]:.0f} | {slower} |")
P("")

# per-graph cost
P("## Cost per explored graph (both finished on one core, Must time >= 10 s)\n")
P("Ratio = (Mustard time / Mustard Exec.) / (Must time / Must Exec.): how much more one graph costs in Mustard. "
  "A tool wins on time when its execution reduction exceeds this ratio.\n")
P("| protocol | cells | per-graph cost ratio, median | min | max |")
P("|---|---|---|---|---|")
for fam, rs in byf.items():
    vals = []
    for r in rs:
        a, b = num(r["must_exec"]), num(r["mustard_exec"])
        ta, tb = num(r["must_time_s"]), num(r["mustard_time_s"])
        if None in (a, b, ta, tb) or ta < 10 or a == 0 or b == 0:
            continue
        if int(num(r.get("must_cores")) or 1) > 1 or int(num(r.get("mustard_cores")) or 1) > 1:
            continue
        vals.append((tb / b) / (ta / a))
    if len(vals) >= 3:
        vals.sort()
        P(f"| {fam} | {len(vals)} | {vals[len(vals) // 2]:.1f} | {vals[0]:.1f} | {vals[-1]:.1f} |")
P("")
# parallel runs
P("## Parallel runs (8 or 16 workers; wall time not comparable with one core)\n")
for r in sorted(par, key=lambda r: r["id"]):
    p = plan_rows.get(re.sub(r"-x\d+$", "", r["id"]), {})
    P(f"- `{r['id']}` {p.get('cell', '')}: {float(r['wall_s']):.0f} s on {int(float(r['cores']))} workers, {r['_m']}")
P("")
open(OUT + "RESULTS_v12.md", "w").write("\n".join(md) + "\n")
print("wrote", OUT + "RESULTS_v12.md", len(md), "lines")
