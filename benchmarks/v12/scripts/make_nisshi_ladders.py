#!/usr/bin/env python3
"""nisshi (Kafka transactions, KAFKA-17754) ladders: W = request.timeout.ms 30000, B = retry.backoff.ms 100, L = 0, U per network."""
import csv, json, os, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from regimes_v12 import *
HERE = os.path.dirname(os.path.abspath(__file__)) + "/"
V9 = "/home/sahar/Downloads/TraceForge/benchmarks/v9/plan_v9.csv"
tmpl = {}
for r in csv.DictReader(open(V9)):
    tmpl.setdefault(r["example"], r)
FIELDS = list(next(iter(tmpl.values())).keys())
EX = "nisshi_txn_timed"


def rung(t, r, net, fix=None, prop="both"):
    nm, L, U, Lp = NETS[net]
    fx = f" --fix {fix}" if fix else ""
    label = f"nisshi T={t} R={r} L={L} U={U} sd=0 W={KAFKA_REQ} B={KAFKA_BACKOFF}{' fix=' + fix if fix else ''} net={net} ({prop})"
    am = f"--txns {t} --retries {r} --l {L} --u {U} --w {KAFKA_REQ} --backoff {KAFKA_BACKOFF}{fx} --property {prop} --keep-going"
    ab = f"--txns {t} --retries {r} --l 0 --u 1 --w {KAFKA_REQ} --backoff {KAFKA_BACKOFF}{fx} --property {prop} --keep-going"
    return dict(size=(t, r), label=label, args_m=am, args_b=ab, bkey=("nisshi", t, r, prop, fix or "none"), regime=f"{net}{'/fix' if fix else ''}")


LAD = []
for net, fix in (("A", None), ("D", None), ("E", None), ("E", "epoch-bump"), ("C", None)):
    tag = f"{net}{'-fix' if fix else ''}"
    LAD.append((f"nisshi|{tag}|T", [rung(t, 2, net, fix) for t in (2, 4, 8, 16, 32, 64, 128)]))
    LAD.append((f"nisshi|{tag}|R", [rung(2, r, net, fix) for r in range(1, 9)]))
    LAD.append((f"nisshi|{tag}|D", [rung(2 * k, k, net, fix) for k in (1, 2, 3, 4, 5, 6)]))

uid = collections.Counter()
cn = 0


def row(label, side, args, pred, rep, reps, rank, tag, params):
    t = tmpl[EX]
    r = {k: t[k] for k in FIELDS}
    uid["nisshi"] += 1
    r.update(id=f"v12L-nisshi-{uid['nisshi']:05d}-{side[0]}-r{rep}", family="nisshi", example=EX, cell=label, regime=tag,
             side=side, rep=str(rep), reps=str(reps), tier="timing", purpose="ladder", args=f"--mode {side} {args}",
             cores="1", parallel="none", pred_s=str(int(pred)), mem_gb="128", order=str(int(pred)), expect="",
             expect_exit="", params=params)
    return r


meta = []
extra = {"brain05": [], "brain10": []}
cnt = {"brain05": 0, "brain10": 0}
for name, rungs in LAD:
    m = min(cnt, key=cnt.get)
    cnt[m] += len(rungs)
    out = []
    for i, rg in enumerate(rungs):
        cn += 1
        cid = f"N{cn:05d}"
        reps = 2 if i < 3 else 1
        pm = 3 * (2.6 ** i)
        mrows = [row(rg["label"], "timed", rg["args_m"], pm, k, reps, 100 + i, rg["regime"], cid) for k in range(1, reps + 1)]
        bk = (m,) + tuple(rg["bkey"])
        bp = "b|" + "|".join(map(str, bk))
        brows = [row("nisshi baseline " + "-".join(map(str, rg["bkey"][1:])), "baseline", rg["args_b"], pm * 3, k, reps, 100 + i,
                     "baseline", bp) for k in range(1, reps + 1)]
        out.append(dict(cid=cid, size=list(rg["size"]), mrows=mrows, brows=brows, bparams=bp))
        meta.append(dict(cid=cid, machine=m, fam="nisshi", label=rg["label"], size=list(rg["size"]), rank=100 + i,
                         regime=rg["regime"], bkey=list(bk), args=rg["args_m"], phase="B", ladder=name))
    extra[m].append(dict(name=name, fam="nisshi", rungs=out))
for m in extra:
    cur = json.load(open(HERE + f"ladders_{m}.json"))
    cur = [l for l in cur if l["fam"] != "nisshi"] + extra[m]
    json.dump(cur, open(HERE + f"ladders_{m}.json", "w"))
    print(m, "nisshi ladders", len(extra[m]), "total ladders", len(cur))
json.dump(meta, open(HERE + "cells_v12_N.json", "w"))
# commands for the CLI check
cmds, seen = [], set()
for m in extra:
    for l in extra[m]:
        for rg in l["rungs"]:
            for r in (rg["mrows"][0], rg["brows"][0]):
                k = r["args"]
                if k not in seen:
                    seen.add(k); cmds.append(dict(id=r["id"], example=EX, args=r["args"], fam="nisshi"))
json.dump(cmds, open(HERE + "nisshi_cmds.json", "w"))
print(len(cmds), "commands")
