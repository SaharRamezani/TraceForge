#!/usr/bin/env python3
"""LaTeX rows for tab:evaluation from table_v11.csv (Exec. = execs + blocked, i.e. all complete graphs incl. failed ones)."""
import csv, re, sys
rows = {(r["family"], r["cell"]): r for r in csv.DictReader(open("table_v11.csv"))}

def g(x):                       # thousands grouping with thin spaces
    return f"{int(round(float(x))):,}".replace(",", r"\,")
def tm(x):
    v = float(x)
    if v < 10: return f"{v:.1f}"
    if v < 1000: return f"{v:.0f}"
    return g(v)
def side(r, s):
    t = r[f"{s}_time_s"]
    reps = int(float(r[f"{s}_reps"] or 0))
    if t.startswith(">"):
        n = re.search(r">(\d+)", t).group(1)
        return ("--", "--", f"$>${g(n)}")
    dag = r"$^\dagger$" if reps < 3 else ""
    return (g(r[f"{s}_complete_paper"]), g(r[f"{s}_viol"]), tm(t) + dag)
def row(fam, cell, label, L=None, U=None, sd=None):
    r = rows[(fam, cell)]
    L = L if L is not None else (r["L"] or "--"); U = U if U is not None else (r["U"] or "--"); sd = sd if sd is not None else (r["sd"] or "--")
    m, t = side(r, "must"), side(r, "mustard")
    return f"    \\quad {label} & {L} & {U} & {sd} & {m[0]} & {m[1]} & {m[2]} & {t[0]} & {t[1]} & {t[2]} \\\\"

SPEC = [
 ("Sensor network", r"$S$ sensors, $R$ check rounds, sensor period", [
   ("sensor", "Sensor --sensors 1 --rounds 2", r"$S{=}1,R{=}2$"),
   ("sensor", "Sensor --period-ratio 5", r"$S{=}2,R{=}1$, period $5U$"),
   ("sensor", "Sensor --sensors 1 --rounds 3", r"$S{=}1,R{=}3$"),
 ]),
 ("NRP", r"variant, $R$ heartbeat rounds", [
   ("nrp", "NRP noopt R=3", r"noopt, $R{=}3$"),
   ("nrp", "NRP leasing R=3", r"leasing, $R{=}3$"),
   ("nrp", "NRP noopt R=4", r"noopt, $R{=}4$"),
 ]),
 ("Lease lock", r"$C$ clients, $R$ rounds, TTL 10, client pause", [
   ("lease", "Lease C=2 R=3 L=0 U=2 sd=1 TTL=10 PAUSE=4", r"$C{=}2,R{=}3$, pause 4"),
   ("lease", "Lease C=2 R=3 L=0 U=2 sd=1 TTL=10 PAUSE=5", r"$C{=}2,R{=}3$, pause 5"),
   ("lease", "Lease C=2 R=4 L=0 U=2 sd=1 TTL=10 PAUSE=3", r"$C{=}2,R{=}4$, pause 3"),
   ("lease", "Lease C=5 R=1 L=0 U=2 sd=1 TTL=10 PAUSE=3", r"$C{=}5,R{=}1$, pause 3"),
   ("lease", "Lease C=2 R=5 L=1 U=2 sd=0 TTL=10 PAUSE=9", r"$C{=}2,R{=}5$, pause 9"),
 ]),
 ("Lease lock (with fencing)", r"$C$ clients, $R$ rounds, TTL 10, client pause", [
   ("lease_f", "Lease + fencing C=2 R=4 L=0 U=2 sd=1 TTL=10 PAUSE=4", r"$C{=}2,R{=}4$, pause 4"),
   ("lease_f", "Lease + fencing C=5 R=1 L=0 U=2 sd=1 TTL=10 PAUSE=4", r"$C{=}5,R{=}1$, pause 4"),
 ]),
 ("Alternating bit", r"$K$ messages, $R$ retries, timeout $W$", [
   ("abp", "ABP-file abp K=4 R=2 W=3U", r"$K{=}4,R{=}2,W{=}3U$"),
   ("abp", "ABP-file abp K=3 R=3 W=3U", r"$K{=}3,R{=}3,W{=}3U$"),
   ("abp", "ABP-file abp K=2 R=9 W=3U", r"$K{=}2,R{=}9,W{=}3U$"),
   ("abp", "ABP-file par K=5 R=2 W=3U", r"var.\ par, $K{=}5,R{=}2,W{=}3U$"),
   ("abp", "ABP-file par K=5 R=2 W=2U", r"var.\ par, $K{=}5,R{=}2,W{=}2U$"),
 ]),
 ("PAR", r"$K$ messages, $R$ retries, timeout $T_o$, delays $d_K{=}d_L{=}3$", [
   ("par", "PAR dK=3 dL=3 dR=1 To=7 K=6 R=1", r"$K{=}6,R{=}1,T_o{=}7$"),
   ("par", "PAR dK=3 dL=3 dR=1 To=8 K=6 R=1", r"$K{=}6,R{=}1,T_o{=}8$"),
   ("par", "PAR dK=3 dL=3 dR=1 To=8 K=7 R=1", r"$K{=}7,R{=}1,T_o{=}8$"),
   ("par", "PAR dK=3 dL=3 dR=1 To=9 K=5 R=2", r"$K{=}5,R{=}2,T_o{=}9$"),
 ]),
 ("BRP", r"$F$ files, $n$ chunks, $\mathit{MAX}$ retransmissions, $T_1{=}3$, $TR$, $\mathit{SYNC}$", [
   ("brp", "BRP F=1 n=2 MAX=3 TD=1 T1=3 TR=20 SYNC=20 k=2 (p2)", r"$n{=}2,\mathit{MAX}{=}3,TR{=}\mathit{SYNC}{=}20$"),
   ("brp", "BRP F=1 n=2 MAX=3 TD=1 T1=3 TR=21 SYNC=21 k=2 (p2)", r"$n{=}2,\mathit{MAX}{=}3,TR{=}\mathit{SYNC}{=}21$"),
   ("brp", "BRP F=2 n=2 MAX=1 TD=1 T1=3 TR=9 SYNC=6 k=2 (p2)", r"$F{=}2,n{=}2,\mathit{MAX}{=}1,TR{=}9,\mathit{SYNC}{=}6$"),
   ("brp", "BRP F=2 n=2 MAX=1 TD=1 T1=3 TR=9 SYNC=7 k=2 (p2)", r"$F{=}2,n{=}2,\mathit{MAX}{=}1,TR{=}9,\mathit{SYNC}{=}7$"),
   ("brp", "BRP F=1 n=5 MAX=1 TD=1 T1=3 TR=9 SYNC=9 k=2 (p2)", r"$n{=}5,\mathit{MAX}{=}1,TR{=}\mathit{SYNC}{=}9$"),
   ("brp", "BRP F=1 n=7 MAX=1 TD=1 T1=3 TR=9 SYNC=9 k=2 (p2)", r"$n{=}7,\mathit{MAX}{=}1,TR{=}\mathit{SYNC}{=}9$"),
 ]),
 ("YARN", r"$K$ application masters, deadline $DD$, horizon $H$, property", [
   ("yarn", "YARN K=1 QS=2 DD=7 U=0 H=12 late-report", r"$K{=}1,DD{=}7,H{=}12$, late-report"),
   ("yarn", "YARN K=1 QS=2 DD=8 U=0 H=12 preempt", r"$K{=}1,DD{=}8,H{=}12$, preempt"),
   ("yarn", "YARN K=2 QS=3 DD=8 U=0 H=9 miss", r"$K{=}2,DD{=}8,H{=}9$, miss"),
   ("yarn", "YARN K=2 QS=3 DD=8 U=0 H=16 miss", r"$K{=}2,DD{=}8,H{=}16$, miss"),
 ]),
]
OVERRIDE = {"par": ("3", "3", "0"), "brp": (r"$\tfrac12$", "1", "0")}   # per-send exact windows (PAR), resolution 1/2 (BRP)
out = []
for title, params, items in SPEC:
    out.append(f"    \\multicolumn{{10}}{{l}}{{\\emph{{{title}}} ({params})}} \\\\")
    for fam, cell, label in items:
        o = OVERRIDE.get(fam)
        out.append(row(fam, cell, label, *(o if o else (None, None, None))))
    out.append(r"    \midrule")
out = out[:-1]
open("tab_evaluation_rows_v11.tex", "w").write("\n".join(out) + "\n")
print("\n".join(out))
