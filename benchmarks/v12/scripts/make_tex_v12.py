#!/usr/bin/env python3
"""Paper-ready rows for tab:evaluation (v12): L, U, sd, W from cited real systems.

Input : table_v12.csv (build_table_v12.py)
Output: tab_evaluation_v12_main.tex, tab_evaluation_v12_full.tex, frontier_v12.csv

Selection per (family, regime group): the largest cell where both tools finished, together with every cell of the same
size and regime group (the boundary partner: pause b-1/b, property p2/p4); and the largest cell where only Mustard finished.
"""
import csv, re, sys, os, collections

HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
rows = list(csv.DictReader(open(HERE + "table_v12.csv")))
OUT = sys.argv[1] if len(sys.argv) > 1 else HERE


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def th(n):
    """thousands separator for LaTeX"""
    n = int(round(n))
    return f"{n:,}".replace(",", "\\,")


def tnum(v):
    if v < 10:
        return f"{v:.1f}"
    return th(v)


def fmt_val(x):
    v = num(x)
    return "--" if v is None else th(v)


def fmt_time(r, pre):
    v = num(r[f"{pre}_time_s"])
    if v is not None:
        mark = ""
        reps = int(num(r[f"{pre}_reps"]) or 0)
        cores = int(num(r.get(f"{pre}_cores")) or 1)
        if cores > 1:
            mark += f"$^{{\\ddagger{cores}}}$"
        elif 0 < reps < 3:
            mark += "$^\\dagger$"
        return tnum(v) + mark
    s = str(r[f"{pre}_time_s"])
    m = re.match(r">(\d+)", s)
    if m:
        return f"$>$ {th(float(m.group(1)))}"
    return "--"


def pm(n):
    """number for L/U/sd columns"""
    try:
        v = float(n)
    except (TypeError, ValueError):
        return str(n)
    return th(v) if v == int(v) else f"{v:g}"


FAMILIES = [
    ("lease", "Lease lock", "$C$ clients, $R$ rounds, TTL 12\\,s, client pause $p$"),
    ("lease_f", "Lease lock (with fencing)", "$C$ clients, $R$ rounds, TTL 12\\,s, pause $p$"),
    ("swim", "SWIM", "$N$ nodes, $R$ periods, probe, suspicion"),
    ("nisshi", "Kafka/nisshi", "$T$ transactions, $R$ retries, $W$, $B$"),
    ("abp", "Alternating bit", "$K$ messages, $R$ retries, $W$"),
    ("par", "PAR", "$K$ messages, $R$ retries, $W$"),
    ("brp", "BRP", "$n$ chunks, $\\mathit{MAX}$, $T_1$, $\\mathit{TR}$, property"),
    ("nrp", "NRP", "variant, $R$, $M$, $H$, $P$"),
    ("retail", "Online shop", "$C$ clients, $N$, $R$, $S$, $D$"),
    ("yarn", "YARN", "$K$ AMs, horizon $H$, property"),
]


def parse(r):
    l = r["label"]
    d = dict(fam=r["family"], net=r["regime"].split("/")[0].replace("-sd", ""), L="0", U="", sd="0", cfg="", grp="", size=r["size"])
    m = re.search(r"net=([\w-]+)\[(\d+),(\d+)\]", l)
    if m:
        d["L"], d["U"] = m.group(2), m.group(3)
    m = re.search(r"\bsd=(\d+)", l)
    if m:
        d["sd"] = m.group(1)
    fam = r["family"]
    if fam in ("lease", "lease_f"):
        m = re.search(r"C=(\d+) R=(\d+) .*TTL=(\d+) PAUSE=(\d+)", l)
        c, rr, ttl, p = (int(x) for x in m.groups())
        b = ttl - 2 * (int(d["U"]) - int(d["L"])) - int(d["sd"])
        tag = " ($b{-}1$)" if p == b - 1 else " ($b$)" if p == b else ""
        d["boundary"] = p in (b - 1, b)
        ttl_txt = "TTL 60\\,s, " if ttl == 60000 else ""
        d["cfg"] = f"$C{{=}}{c},R{{=}}{rr}$, {ttl_txt}$p{{=}}{th(p)}${tag}"
        d["grp"] = (r["regime"], ttl)
        d["fam"] = fam
    elif fam == "abp":
        m = re.match(r"ABP-file (\w+) K=(\d+) R=(\d+) W=(\d+)", l)
        var, k, rr, w = m.groups()
        d["fam"] = "par" if var == "par" else "abp"
        d["cfg"] = f"$K{{=}}{k},R{{=}}{rr},W{{=}}{th(int(w))}$"
        d["grp"] = (d["net"], int(w))
        d["W"] = w
    elif fam == "brp":
        m = re.search(r"n=(\d+) MAX=(\d+) TD=(\d+) T1=(\d+) TR=(\d+) SYNC=(\d+) net=(\w+) \((p\d)\)", l)
        n, mx, td, t1, tr, sy, net, prop = m.groups()
        d["net"] = net
        d["L"], d["U"], d["sd"] = "0.5", td, "0"
        d["cfg"] = f"$n{{=}}{n},\\mathit{{MAX}}{{=}}{mx}$, $T_1{{=}}{th(int(t1))}$, $\\mathit{{TR}}{{=}}{th(int(tr))}$, ({prop[1]})"
        d["grp"] = (net, int(t1))
    elif fam == "nisshi":
        m = re.search(r"T=(\d+) R=(\d+) L=(\d+) U=(\d+) sd=(\d+) W=(\d+) B=(\d+)", l)
        t, rr, lo, up, sd, w, bk = m.groups()
        fix = " fix" if "fix=" in l else ""
        d["L"], d["U"], d["sd"] = lo, up, sd
        d["cfg"] = f"$T{{=}}{t},R{{=}}{rr}$, $W{{=}}{th(int(w))}$, $B{{=}}{bk}${fix}"
        d["grp"] = (d["net"], "fix" if fix else "bug")
    elif fam == "swim":
        m = re.search(r"N=(\d+) R=(\d+) timers=(\w+)\(Wp=(\d+),Ws=(\d+)\)", l)
        n, rr, prof, wp, ws = m.groups()
        d["cfg"] = f"$N{{=}}{n},R{{=}}{rr}$, {prof} ({th(int(wp))}, {th(int(ws))})"
        d["grp"] = (d["net"], prof)
    elif fam == "nrp":
        m = re.search(r"NRP (\w+) R=(\d+) M=(\d+) H=(\d+) P=(\d+)", l)
        var, rr, mm, h, p = m.groups()
        d["cfg"] = f"{var}, $R{{=}}{rr}$, $M{{=}}{mm}$, $H{{=}}{th(int(h))}$, $P{{=}}{th(int(p))}$"
        d["grp"] = (d["net"], var)
    elif fam == "retail":
        m = re.search(r"C=(\d+) N=(\d+) R=(\d+) S=(\d+) D=(\d+)", l)
        c, n, rr, s, dd = m.groups()
        d["cfg"] = f"$C{{=}}{c},N{{=}}{n},R{{=}}{rr}$, $S{{=}}{s}$, $D{{=}}{th(int(dd))}$"
        d["grp"] = (d["net"],)
    elif fam == "yarn":
        m = re.search(r"K=(\d+) QS=(\d+) DD=(\d+) U=(\d+) H=(\d+) ([\w-]+) net=", l)
        k, qs, dd, u, h, prop = m.groups()
        d["net"] = "D" if int(u) > 0 else "A-C"
        d["L"], d["U"], d["sd"] = "0", u, "0"
        d["cfg"] = f"$K{{=}}{k},H{{=}}{h}$, {prop}"
        d["grp"] = (d["net"], prop)
    return d


recs = []
for r in rows:
    try:
        d = parse(r)
    except AttributeError:
        continue
    d["r"] = r
    recs.append(d)

verdict_mark = {"FLIP (Must violates, Mustard holds)": "flip", "both violate": "viol", "both hold": "hold"}


def row_tex(d):
    r = d["r"]
    cells = [f"{d['net']}: " + d["cfg"], pm(d["L"]), pm(d["U"]), pm(d["sd"]),
             fmt_val(r["must_exec"]), fmt_val(r["must_viol"]), fmt_time(r, "must"),
             fmt_val(r["mustard_exec"]), fmt_val(r["mustard_viol"]), fmt_time(r, "mustard")]
    return "    \\quad " + " & ".join(cells) + " \\\\"


def select(fam_recs):
    """per regime group: headline both-finished cell + same-size partners, and the largest Mustard-only cell"""
    by = collections.defaultdict(list)
    for d in fam_recs:
        if d["fam"] in ("lease", "lease_f") and not d.get("boundary", False):
            continue
        by[d["grp"]].append(d)
    sel = []
    for grp, lst in sorted(by.items(), key=lambda kv: str(kv[0])):
        both = [d for d in lst if num(d["r"]["must_time_s"]) is not None and num(d["r"]["mustard_time_s"]) is not None]
        if both:
            top = max(both, key=lambda d: (max(num(d["r"]["must_time_s"]), num(d["r"]["mustard_time_s"])), num(d["r"]["rank"])))
            same = [d for d in both if d["r"]["size"] == top["r"]["size"] and d["r"]["rank"] == top["r"]["rank"]]
            # pair: pause b-1 and b (lease), property p2/p4 (BRP): keep distinct configurations, one row each
            seen = set()
            for d in sorted(same, key=lambda d: d["cfg"]):
                if d["cfg"] not in seen:
                    seen.add(d["cfg"])
                    sel.append(d)
        mo = [d for d in lst if num(d["r"]["mustard_time_s"]) is not None and num(d["r"]["must_time_s"]) is None]
        if mo:
            top = max(mo, key=lambda d: num(d["r"]["mustard_time_s"]))
            if not both or num(top["r"]["mustard_time_s"]) > 1.5 * max(num(d["r"]["mustard_time_s"]) for d in both):
                sel.append(top)
    return sel


def write(path, regimes, caption_extra=""):
    out = []
    for fam, title, desc in FAMILIES:
        fr = [d for d in recs if d["fam"] == fam and (regimes is None or d["net"] in regimes)]
        sel = select(fr)
        if not sel:
            continue
        out.append(f"    \\multicolumn{{10}}{{l}}{{\\emph{{{title}}} ({desc})}} \\\\")
        last = None
        for d in sorted(sel, key=lambda d: (list(["A", "B", "Bp", "C", "Cp", "D", "E", "A-C"]).index(d["net"]) if d["net"] in ["A", "B", "Bp", "C", "Cp", "D", "E", "A-C"] else 9, str(d["grp"]), num(d["r"]["rank"]), d["cfg"])):
            if d["net"] != last:
                last = d["net"]
            out.append(row_tex(d))
        out.append("    \\midrule")
    if out and out[-1] == "    \\midrule":
        out.pop()
    open(path, "w").write("\n".join(out) + "\n")
    return len(out)


MAIN = {"A", "C", "D", "E", "A-C"}
n1 = write(OUT + "tab_evaluation_v12_rows_main.tex", MAIN)
n2 = write(OUT + "tab_evaluation_v12_rows_full.tex", None)
print("main rows lines", n1, "full rows lines", n2)

# machine-readable frontier
with open(OUT + "frontier_v12.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["family", "net", "L", "U", "sd", "config", "must_exec", "must_viol", "must_time_s", "must_cores", "mustard_exec", "mustard_viol",
                "mustard_time_s", "mustard_cores", "verdict", "label"])
    for fam, title, desc in FAMILIES:
        for d in select([d for d in recs if d["fam"] == fam]):
            r = d["r"]
            w.writerow([fam, d["net"], d["L"], d["U"], d["sd"], re.sub(r"[$\\{}]", "", d["cfg"]), r["must_exec"], r["must_viol"], r["must_time_s"],
                        r.get("must_cores", ""), r["mustard_exec"], r["mustard_viol"], r["mustard_time_s"], r.get("mustard_cores", ""),
                        r["verdict"], r["label"]])


HEADER = r"""\toprule
    & \multicolumn{3}{c}{Timing} & \multicolumn{3}{c}{\must{}} & \multicolumn{3}{c}{\tool{}} \\
    \cmidrule(lr){2-4}\cmidrule(lr){5-7}\cmidrule(l){8-10}
    Configuration & $L$ & $U$ & $\sd$ & Exec. & Viol. & Time & Exec. & Viol. & Time \\
    \midrule"""

CAPTION = (r"Comparison of \must{} and \tool{} under timing regimes taken from real systems (all times in ms, one tick = 1\,ms; "
           r"YARN in 1\,s ticks). Networks: [A] same-region cross-AZ $U{=}1$ (Azure AZ round trip below about 2\,ms), "
           r"[B] continental $U{=}65$ (etcd tuning guide, 130\,ms round trip), [C] intercontinental $U{=}200$ (etcd, 350 to 400\,ms), "
           r"[D] degraded $U{=}2\,500$ (etcd election cap), [E] partition-delayed $U{=}90\,000$ (GitHub 2012); suffix p: $L$ is half the median. "
           r"Times are seconds on one core, medians of three runs unless marked $^\dagger$ (fewer runs) or $^{\ddagger n}$ "
           r"(run with $n$ workers; wall time not comparable); $>t$: not finished after $t$ seconds. "
           r"\emph{Exec.} counts all complete execution graphs including blocked ones and those with a failed assertion; "
           r"\emph{Viol.} counts the graphs with at least one failed assertion, once per graph. "
           r"Configuration columns: lease: clients, rounds, TTL, client pause ($b$ is the boundary $\mathit{TTL}-2(U-L)-\sd$); SWIM: probe period and suspicion timeout; "
           r"BRP: $L{=}0.5$ is the half-unit grid of the transit interval $(0,T_D]$; YARN: ticks of the 1\,s NM heartbeat.")


def wrap_table(rows_path, out_path, longtable=False):
    body = open(rows_path).read()
    ncols = 10
    if longtable:
        txt = ("% generated by make_tex_v12.py from benchmarks/v12/table_v12.csv\n\\begingroup\\footnotesize\\setlength{\\tabcolsep}{2pt}\n"
               "\\begin{longtable}{@{}l*{3}{>{\\centering\\arraybackslash}p{3.2em}}rrrrrr@{}}\n"
               "\\caption{" + CAPTION + "}\\label{tab:evaluation-full}\\\\\n    " + HEADER + "\n\\endfirsthead\n    " + HEADER + "\n\\endhead\n"
               + body + "    \\bottomrule\n\\end{longtable}\n\\endgroup\n")
    else:
        txt = ("% generated by make_tex_v12.py from benchmarks/v12/table_v12.csv\n\\begin{table}[H]\n  \\centering\n  \\caption{" + CAPTION + "}\n"
               "  \\label{tab:evaluation}\n  \\smallskip\n  \\begingroup\n  \\footnotesize\n  \\setlength{\\tabcolsep}{2pt}\n"
               "  \\begin{tabular}{@{}l*{3}{>{\\centering\\arraybackslash}p{3.2em}}rrrrrr@{}}\n    " + HEADER + "\n" + body
               + "    \\bottomrule\n  \\end{tabular}\n  \\endgroup\n\\end{table}\n")
    open(out_path, "w").write(txt)


wrap_table(OUT + "tab_evaluation_v12_rows_main.tex", OUT + "tab_evaluation_v12_main.tex")
wrap_table(OUT + "tab_evaluation_v12_rows_full.tex", OUT + "tab_evaluation_v12_full.tex", longtable=True)
