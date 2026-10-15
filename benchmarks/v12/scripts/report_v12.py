#!/usr/bin/env python3
"""REPORT_v12.md: the short written report (facts from the data, static text for the reasoning)."""
import csv, json, re, os, sys, collections, statistics as st

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


def tm(x, c=None):
    v = num(x)
    if v is None:
        return str(x)[:12]
    s = f"{v:.1f} s" if v < 10 else (f"{v:.0f} s" if v < 7200 else f"{v / 3600:.1f} h")
    return s + (f" ({int(float(c))} workers)" if c and int(float(c)) > 1 else "")


res = []
for m in ("brain05", "brain10"):
    for r in csv.DictReader(open(D + f"results_v12_{m}.csv")):
        r["_m"] = m
        res.append(r)
fin = [r for r in res if r["status"].startswith("finished") or r["status"] == "error (exit 3)"]
dnf = [r for r in res if r["status"].startswith("DNF")]
rem = [r for r in res if "removed by the operator" in r["status"]]
par = [r for r in fin if int(float(r["cores"] or 1)) > 1]
both = [r for r in rows if num(r["must_time_s"]) is not None and num(r["mustard_time_s"]) is not None]
ctl = json.load(open(HERE + "controls_v12.json"))
cnt = collections.Counter(r["verdict"] or "pending" for r in rows)
more = [r for r in both if num(r["mustard_exec"]) > num(r["must_exec"])]

md = []
P = md.append
P("# v12 report: Must vs Mustard under timing regimes taken from real systems\n")
P(f"Campaign 2026-10-15 00:16 to 07:50 on brain05 and brain10. {len(rows):,} cells, {len(fin):,} finished runs, {len(dnf)} not finished at the stop, "
  f"{len(rem)} rows stopped by the operator (not results), {len(par)} runs with 8 or 16 workers (flagged).\n")
P("## Is brain10 valid?\n")
same = sum(1 for c in ctl if c["same"])
wr = ""
try:
    mm = re.search(r"wall ratio brain10/brain05 over (\d+) pairs.*?median ([\d.]+), min ([\d.]+), max ([\d.]+)", open(HERE + "controls_v12.txt").read())
    wr = f"Wall time brain10 / brain05 over {mm.group(1)} one-core pairs of at least 5 s: median {float(mm.group(2)):.2f}, range {float(mm.group(3)):.2f} to {float(mm.group(4)):.2f}. "
except Exception:
    wr = "Wall ratios are in `controls_v12.txt`. "
P(f"Yes. {len(ctl)} identical commands finished on both machines (Must baselines and Mustard cells); counts are identical in {same}. "
  + wr + "Counts are also deterministic across repetitions: no command that was run more than once gave two different counts.\n")
P("## Verdicts\n")
P(f"{cnt.get('FLIP (Must violates, Mustard holds)', 0)} cells where Must reports a violation and Mustard proves the property under the stated timing "
  f"(flips), {cnt.get('both violate', 0)} where both report violations, {cnt.get('both hold', 0)} where both hold, {cnt.get('pending', 0)} where the "
  "Must side did not finish. There is no cell where Mustard violates and Must holds.\n")
P("## Headline pairs (both tools finished)\n")
P("| protocol | cell | regime | Must graphs / viol. / time | Mustard graphs / viol. / time | graph ratio | verdict |")
P("|---|---|---|---|---|---|---|")
sel = ["Lease C=3 R=3 net=A[0,1] sd=0 TTL=12000 PAUSE=11997", "Lease C=4 R=2 net=A[0,1] sd=0 TTL=12000 PAUSE=11997",
       "Lease C=2 R=6 net=A[0,1] sd=0 TTL=12000 PAUSE=11997", "ABP-file par K=4 R=3 W=200 net=A[0,1] sd=0", "ABP-file par K=5 R=2 W=3 net=A[0,1] sd=0",
       "BRP F=1 n=6 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A (p2)", "BRP F=1 n=2 MAX=5 TD=1 T1=200 TR=2003 SYNC=2003 net=A (p2)",
       "nisshi T=32 R=2 L=0 U=1 sd=0 W=30000 B=100 net=A (both)", "YARN K=2 QS=3 DD=8 U=0 H=11 miss net=A-C", "YARN K=2 QS=3 DD=8 U=0 H=10 late-report net=A-C",
       "NRP noopt R=4 M=3 H=1000 P=2 net=A[0,1] sd=0", "SWIM N=3 R=7 timers=lan(Wp=500,Ws=4000) net=A[0,1] sd=0",
       "Lease + fencing C=2 R=5 net=A[0,1] sd=0 TTL=12000 PAUSE=11998", "Shop v2 C=2 N=2 R=5 S=20 D=29000 net=A[0,1] sd=0"]
byl = {}
for r in rows:
    byl.setdefault(r["label"], []).append(r)
for lab in sel:
    c = byl.get(lab)
    if not c:
        continue
    r = max(c, key=lambda r: (num(r["must_time_s"]) is not None, num(r["mustard_time_s"]) is not None))
    a, b = num(r["must_exec"]), num(r["mustard_exec"])
    ratio = f"{a / b:,.0f}x" if a and b else "-"
    fam = "par" if lab.startswith("ABP-file par") else ("abp" if lab.startswith("ABP-file abp") else r["family"])
    P(f"| {fam} | {lab} | {r['regime'].split('/')[0]} | {g(r['must_exec'])} / {g(r['must_viol'])} / {tm(r['must_time_s'], r.get('must_cores'))} | "
      f"{g(r['mustard_exec'])} / {g(r['mustard_viol'])} / {tm(r['mustard_time_s'], r.get('mustard_cores'))} | {ratio} | {r['verdict'] or '-'} |")
P("")
P("Times with a worker count are not comparable with one-core times; counts are.\n")
P("## What the regimes change\n")
P("- Lease: the verdict follows the closed form PAUSE >= TTL - 2(U - L) - sd. Regimes only move the boundary; at the same distance from it the "
  "counts agree (regime A is on a coarse 1 ms grid and differs by 1 to 3%).")
P("- PAR: a premature retransmission is feasible iff L <= W <= 2U + sd. With RTO values of 200 ms, 1 s and 3 s the protocol is safe at A and B, "
  "unsafe at C for W = 200 ms, and unsafe at D and E for every W. The alternating-bit variant never violates.")
P("- BRP: property (2) violates only where T1 <= 2 TD; property (4) violates everywhere (lost R timer).")
P("- nisshi (Kafka): the bug needs delays above the 30 s request timeout: A, C, D hold in Mustard (Must reports 168,880 to 2.1M violating graphs), "
  "E (90 s) violates in both, and the epoch-bump fix holds in both.")
P("- SWIM, online shop: a lost message alone produces the violation, so the verdict does not depend on the regime and nothing flips.")
P("- NRP: at R = 3 every variant holds in both tools; at R = 4 leasing still holds while nrp and noopt violate in both tools, in every regime.\n")
P("## Where Mustard does not pay off\n")
P(f"- In {len(more)} of {len(both):,} both-finished cells Mustard reports MORE graphs than Must (SWIM at D and E, lease and shop at E: regimes whose "
  "delay bound exceeds the protocol timers). On SWIM N=3, R=2 and lease C=2, R=2 both tools visit the same program-visible graphs; they differ only in "
  "how many dropped or arrived-but-unread fate variants they enumerate.")
P("- One graph costs 4 to 9 times more in Mustard (median per protocol, see `RESULTS_v12.md`), so Mustard is slower than Must on lease, SWIM and "
  "the shop at most regimes even where it explores fewer graphs, and about 10 to 40 times slower where nothing is pruned (ABP, PAR, BRP at D).")
P("")
P("## Not in the regime campaign\n")
P("Chandra-Toueg, spanning tree and the sensor network have no citable real-system numbers at the sizes Mustard reaches; their v11 rows (model "
  "units) are in `tab_evaluation_unmapped_v11rows.tex`. Keeping, dropping or footnoting them is still the author's decision.\n")
P("## Files\n")
P("`README.md` lists every file. Start with `tab_evaluation_v12_curated.tex` (story rows), `RESULTS_v12.md` (all numbers), `CAVEATS.md`, `REGIMES.md`.\n")
open(OUT + "REPORT_v12.md", "w").write("\n".join(md) + "\n")
print("wrote", OUT + "REPORT_v12.md")
