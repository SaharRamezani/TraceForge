#!/usr/bin/env python3
"""Curated paper table: explicit story rows per protocol (regex on the cell label), in a fixed order.

Reads table_v12.csv; writes tab_evaluation_v12_curated.tex (rows + wrapped table) and prints which patterns matched nothing.
"""
import csv, re, sys, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
sys.argv_saved = sys.argv
sys.argv = ["make_tex_v12.py", "/tmp/"]
spec = importlib.util.spec_from_file_location("mk", HERE + "make_tex_v12.py")
mk = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mk)
sys.argv = sys.argv_saved

CURATED = [
    ("lease", "Lease lock", [
        r"^Lease C=3 R=3 net=A\[0,1\] sd=0 TTL=12000 PAUSE=1199[78]$",
        r"^Lease C=4 R=2 net=A\[0,1\] sd=0 TTL=12000 PAUSE=1199[78]$",
        r"^Lease C=6 R=1 net=A\[0,1\] sd=0 TTL=12000 PAUSE=1199[78]$",
        r"^Lease C=2 R=6 net=A\[0,1\] sd=0 TTL=12000 PAUSE=1199[78]$",
        r"^Lease C=2 R=5 net=C\[0,200\] sd=0 TTL=12000 PAUSE=(11599|11600)$",
        r"^Lease C=2 R=5 net=D\[0,2500\] sd=0 TTL=12000 PAUSE=(6999|7000)$",
        r"^Lease C=2 R=6 net=D\[0,2500\] sd=0 TTL=12000 PAUSE=(6999|7000)$",
        r"^Lease C=3 R=3 net=A\[0,1\] sd=1 TTL=12000 PAUSE=(11996|11997)$",
        r"^Lease C=2 R=5 net=A\[0,1\] sd=1 TTL=12000 PAUSE=(11996|11997)$",
        r"^Lease C=2 R=5 net=D\[0,2500\] sd=2500 TTL=12000 PAUSE=(4499|4500)$",
        r"^Lease C=2 R=5 net=A\[0,1\] sd=0 TTL=60000 PAUSE=5999[78]$",
        r"^Lease C=2 R=3 net=E\[0,90000\] sd=0 TTL=12000 PAUSE=200$",
    ]),
    ("lease_f", "Lease lock with fencing", [
        r"^Lease \+ fencing C=2 R=5 net=A\[0,1\] sd=0 TTL=12000 PAUSE=11998$",
        r"^Lease \+ fencing C=5 R=1 net=A\[0,1\] sd=0 TTL=12000 PAUSE=11998$",
        r"^Lease \+ fencing C=5 R=1 net=D\[0,2500\] sd=0 TTL=12000 PAUSE=7000$",
    ]),
    ("swim", "SWIM", [
        r"^SWIM N=3 R=7 timers=lan\(Wp=500,Ws=4000\) net=A\[0,1\] sd=0$",
        r"^SWIM N=11 R=1 timers=lan\(Wp=500,Ws=4000\) net=A\[0,1\] sd=0$",
        r"^SWIM N=6 R=2 timers=lan\(Wp=500,Ws=4000\) net=A\[0,1\] sd=0$",
        r"^SWIM N=6 R=2 timers=wan\(Wp=3000,Ws=30000\) net=C\[0,200\] sd=0$",
        r"^SWIM N=6 R=2 timers=local\(Wp=200,Ws=3000\) net=D\[0,2500\] sd=0$",
        r"^SWIM N=3 R=5 timers=local\(Wp=200,Ws=3000\) net=D\[0,2500\] sd=0$",
    ]),
    ("nrp", "NRP", [
        r"^NRP nrp R=4 M=3 H=1000 P=2 net=A\[0,1\] sd=0$",
        r"^NRP noopt R=4 M=3 H=1000 P=2 net=A\[0,1\] sd=0$",
        r"^NRP leasing R=4 M=3 H=1000 P=2 net=A\[0,1\] sd=0$",
        r"^NRP (nrp|noopt) R=4 M=3 H=10000 P=5000 net=D\[0,2500\] sd=0$",
        r"^NRP nrp R=4 M=3 H=1000 P=400 net=C\[0,200\] sd=0$",
        r"^NRP leasing R=4 M=3 H=10000 P=5000 net=D\[0,2500\] sd=0$",
        r"^NRP nrp R=5 M=3 H=1000 P=2 net=A\[0,1\] sd=0$",
    ]),
    ("nisshi", "Kafka/nisshi", [
        r"^nisshi T=16 R=2 L=0 U=1 sd=0 W=30000 B=100 net=A \(both\)$",
        r"^nisshi T=32 R=2 L=0 U=1 sd=0 W=30000 B=100 net=A \(both\)$",
        r"^nisshi T=8 R=4 L=0 U=1 sd=0 W=30000 B=100 net=A \(both\)$",
        r"^nisshi T=2 R=5 L=0 U=1 sd=0 W=30000 B=100 net=A \(both\)$",
        r"^nisshi T=32 R=2 L=0 U=200 sd=0 W=30000 B=100 net=C \(both\)$",
        r"^nisshi T=32 R=2 L=0 U=2500 sd=0 W=30000 B=100 net=D \(both\)$",
        r"^nisshi T=2 R=(8|9|10) L=0 U=1 sd=0 W=30000 B=100 net=A \(both\)$",
        r"^nisshi T=6 R=3 L=0 U=90000 sd=0 W=30000 B=100 net=E \(both\)$",
        r"^nisshi T=8 R=2 L=0 U=90000 sd=0 W=30000 B=100 fix=epoch-bump net=E \(both\)$",
    ]),
    ("abp", "Alternating bit", [
        r"^ABP-file abp K=4 R=2 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file abp K=5 R=2 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file abp K=9 R=1 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file abp K=2 R=9 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file abp K=13 R=1 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file abp K=4 R=2 W=1000 net=C\[0,200\] sd=0$",
        r"^ABP-file abp K=3 R=3 W=1000 net=D\[0,2500\] sd=0$",
    ]),
    ("par", "PAR", [
        r"^ABP-file par K=5 R=2 W=(2|3|200) net=A\[0,1\] sd=0$",
        r"^ABP-file par K=4 R=3 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file par K=8 R=1 W=(200|1000) net=A\[0,1\] sd=0$",
        r"^ABP-file par K=6 R=1 W=(400|401) net=C\[0,200\] sd=0$",
        r"^ABP-file par K=6 R=1 W=(5000|5001) net=D\[0,2500\] sd=0$",
        r"^ABP-file par K=8 R=1 W=(200|1000) net=C\[0,200\] sd=0$",
        r"^ABP-file par K=2 R=(9|10|11) W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file par K=(13|14|15) R=1 W=200 net=A\[0,1\] sd=0$",
        r"^ABP-file par K=8 R=1 W=1000 net=D\[0,2500\] sd=0$",
    ]),
    ("brp", "BRP", [
        r"^BRP F=1 n=3 MAX=2 TD=1 T1=1000 TR=4003 SYNC=4003 net=A \(p[24]\)$",
        r"^BRP F=1 n=5 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A \(p2\)$",
        r"^BRP F=1 n=(6|7|8) MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A \(p2\)$",
        r"^BRP F=1 n=2 MAX=(5|8|9) TD=1 T1=200 TR=\d+ SYNC=\d+ net=A \(p2\)$",
        r"^BRP F=1 n=3 MAX=2 TD=200 T1=200 TR=1400 SYNC=1400 net=C \(p[24]\)$",
        r"^BRP F=1 n=2 MAX=5 TD=2500 T1=1000 TR=17500 SYNC=17500 net=D \(p2\)$",
        r"^BRP F=3 n=2 MAX=1 TD=200 T1=1000 TR=2600 SYNC=2600 net=C \(p2\)$",
    ]),
    ("retail", "Online shop", [
        r"^Shop v2 C=2 N=2 R=4 S=20 D=29000 net=A\[0,1\] sd=0$",
        r"^Shop v2 C=3 N=3 R=2 S=20 D=29000 net=B\[0,65\] sd=0$",
        r"^Shop v2 C=3 N=3 R=2 S=20 D=29000 net=E\[0,90000\] sd=0$",
    ]),
    ("yarn", "YARN", [
        r"^YARN K=2 QS=3 DD=8 U=0 H=9 miss net=A-C$",
        r"^YARN K=2 QS=3 DD=8 U=3 H=9 miss net=D$",
        r"^YARN K=2 QS=3 DD=8 U=0 H=10 (miss|preempt|late-report) net=A-C$",
        r"^YARN K=2 QS=3 DD=8 U=0 H=11 miss net=A-C$",
        r"^YARN K=2 QS=3 DD=8 U=3 H=11 miss net=D$",
        r"^YARN K=2 QS=3 DD=8 U=3 H=15 miss net=D$",
        r"^YARN K=2 QS=3 DD=8 U=0 H=1[789] miss net=A-C$",
        r"^YARN K=4 QS=5 DD=8 U=0 H=[56] miss net=A-C$",
        r"^YARN K=1 QS=2 DD=7 U=0 H=(24|48|52) preempt net=A-C$",
    ]),
]

def natkey(t):
    return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", t)]


rows = list(csv.DictReader(open(HERE + "table_v12.csv")))
by_label = {}
for d in mk.recs:
    by_label.setdefault(d["r"]["label"], []).append(d)


def best(ds):
    """one record per label: prefer the one with both sides finished, then the largest Must time"""
    def key(d):
        r = d["r"]
        mt = mk.num(r["must_time_s"])
        return (mk.num(r["mustard_time_s"]) is not None, mt is not None, mt or 0)
    return max(ds, key=key)


out, missing = [], []
for fam, title, pats in CURATED:
    sub = []
    desc = next((x[2] for x in mk.FAMILIES if x[0] == fam), "")
    for pat in pats:
        hits = [lab for lab in by_label if re.match(pat, lab)]
        if not hits:
            missing.append(pat)
            continue
        for lab in sorted(hits, key=natkey):
            d = best(by_label[lab])
            if mk.num(d["r"]["mustard_time_s"]) is None and mk.num(d["r"]["must_time_s"]) is None:
                missing.append(pat + "  (not finished: " + lab + ")")
                continue
            sub.append(d)
    if not sub:
        continue
    out.append(f"    \\multicolumn{{10}}{{l}}{{\\emph{{{title}}} ({desc})}} \\\\")
    for d in sub:
        out.append(mk.row_tex(d))
    out.append("    \\midrule")
if out and out[-1].endswith("midrule"):
    out.pop()
path = HERE + "tab_evaluation_v12_curated_rows.tex"
open(path, "w").write("\n".join(out) + "\n")
mk.wrap_table(path, HERE + "tab_evaluation_v12_curated.tex", longtable=True)
print(len(out), "lines;", len(missing), "patterns without a finished cell:")
for m in missing:
    print("   ", m)

# compact text view for the report
def g(x):
    v = mk.num(x)
    return "-" if v is None else f"{int(round(v)):,}"
def tfm(x, cores):
    v = mk.num(x)
    if v is None:
        return str(x)[:10]
    return (f"{v:.1f}s" if v < 10 else f"{v:.0f}s") + (f"(c{cores})" if cores and int(float(cores)) > 1 else "")
lines = []
for fam, title, pats in CURATED:
    for pat in pats:
        for lab in sorted((l for l in by_label if re.match(pat, l)), key=natkey):
            d = best(by_label[lab])
            r = d["r"]
            a, b = mk.num(r["must_exec"]), mk.num(r["mustard_exec"])
            ratio = f"{a / b:,.0f}x" if a and b else "-"
            lines.append(f"{fam:8s} {lab[:70]:70s} Must {g(r['must_exec']):>13s}/{g(r['must_viol']):>11s}/{tfm(r['must_time_s'], r.get('must_cores')):>12s} | "
                         f"Mustard {g(r['mustard_exec']):>12s}/{g(r['mustard_viol']):>10s}/{tfm(r['mustard_time_s'], r.get('mustard_cores')):>12s} | {ratio:>8s} {r['verdict'][:5]}")
open(HERE + "curated_text_v12.txt", "w").write("\n".join(lines) + "\n")
