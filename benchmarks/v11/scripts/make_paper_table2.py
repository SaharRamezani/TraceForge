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
 ("Chandra--Toueg", r"$\Delta,\Phi$, timeout $to$, horizon $H$", [
   ("ct", "CT delta=2 phi=4 to=13 H=18", r"$\Delta{=}2,\Phi{=}4,to{=}13,H{=}18$"),
   ("ct", "CT delta=2 phi=2 to=7 H=19", r"$\Delta{=}2,\Phi{=}2,to{=}7,H{=}19$"),
   ("ct", "CT delta=2 phi=4 to=13 H=19", r"$\Delta{=}2,\Phi{=}4,to{=}13,H{=}19$"),
 ]),
 ("Spanning tree", r"$R$ root hellos, forward delay $F_d$", [
   ("stp", "STP lost-hellos R=13 Fd=23 L/U=0 sd/U=0", r"lost hellos, $R{=}13,F_d{=}23$"),
   ("stp", "STP lost-hellos R=13 Fd=24 L/U=0 sd/U=0", r"lost hellos, $R{=}13,F_d{=}24$ (vacuous)"),
   ("stp", "STP lost-hellos R=15 Fd=28 L/U=0 sd/U=0", r"lost hellos, $R{=}15,F_d{=}28$"),
 ]),
 ("SWIM", r"$N$ nodes, $R$ probe periods, $W_p{=}2,W_s{=}6$", [
   ("swim", "SWIM N=10 R=1 L=0 U=2 sd=1 Wp=2 Ws=6", r"$N{=}10,R{=}1$"),
   ("swim", "SWIM N=6 R=2 L=1 U=2 sd=1 Wp=2 Ws=6", r"$N{=}6,R{=}2$"),
   ("swim", "SWIM N=11 R=1 L=0 U=2 sd=1 Wp=2 Ws=6", r"$N{=}11,R{=}1$"),
 ]),
 ("Online shop", r"$C$ customers, $N$ order services, $R$ rounds, deadline $D$", [
   ("retail", "Shop v2 C=2 N=2 R=4 S=20 D=7", r"$C{=}2,N{=}2,R{=}4,D{=}7$"),
   ("retail", "Shop v2 C=3 N=3 R=2 S=20 D=7", r"$C{=}3,N{=}3,R{=}2,D{=}7$"),
 ]),
]
OVERRIDE = {"ct": ("0", "1", "0")}   # per-send exact windows (PAR), resolution 1/2 (BRP)
out = []
for title, params, items in SPEC:
    out.append(f"    \\multicolumn{{10}}{{l}}{{\\emph{{{title}}} ({params})}} \\\\")
    for fam, cell, label in items:
        o = OVERRIDE.get(fam)
        out.append(row(fam, cell, label, *(o if o else (None, None, None))))
    out.append(r"    \midrule")
out = out[:-1]
open("tab_evaluation_dropped_v11.tex", "w").write("\n".join(out) + "\n")
print("\n".join(out))
