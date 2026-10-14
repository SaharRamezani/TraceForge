# Explicit cell list of the final campaign (exec'd by make_final_plan.py; uses add/swim/lease and CELLS from there).
import json, re

_HEAD = json.load(open(S + "head_cells.json"))


def H(example, pattern):
    """arg strings of HEAD-measured cells of `example` whose args match the regex (sorted, unique)."""
    out = sorted({c["args"] for c in _HEAD if c["example"] == example and re.search(pattern, c["args"])})
    return out


# ================================================================ Row 7: SWIM (N nodes, R rounds)
# L=0 (jitter window [0,2]) and L=1 (window [1,2]); sd=1, Wp=2, Ws=6, as in the v9 ladder.
for n, r in [(3, 1), (3, 2), (3, 3), (3, 4), (3, 5), (4, 1), (4, 2), (4, 3), (5, 1), (5, 2),
             (6, 1), (7, 1), (8, 1), (9, 1), (10, 1)]:
    swim(n, r, 0)
for n, r in [(3, 3), (3, 5), (4, 3), (5, 2), (6, 2), (7, 1), (8, 1), (9, 1), (10, 1)]:
    swim(n, r, 0.5)
# edge attempts (60 min cap, one side or both may DNF)
for n, r in [(3, 6), (4, 4), (5, 3), (11, 1), (12, 1)]:
    swim(n, r, 0, edge=True)
for n, r in [(3, 6), (4, 4), (5, 3)]:
    swim(n, r, 0.5, edge=True)

# ================================================================ Row 5: Lease lock (fencing off)
# L=0 sd=1 (A family) pause ratio P: PAUSE=2P; L=1 sd=0 (B family)
for c, r in [(2, 1), (2, 3), (3, 1), (3, 2), (4, 1), (5, 1)]:
    lease(c, r, False, 0, 0.5, 1.5)        # below the bound: Must violates, Mustard holds
for c, r in [(2, 3), (3, 2)]:
    for p in (2, 2.5, 3):                 # pause scan across the bound (L=0, sd=1)
        lease(c, r, False, 0, 0.5, p)
lease(2, 4, False, 0, 0.5, 1.5)
for c, r in [(4, 1), (5, 1)]:
    lease(c, r, False, 0.5, 0, 4.5)        # L=1 sd=0: PAUSE=9 (below) and 10 (at the boundary)
    lease(c, r, False, 0.5, 0, 5)
lease(2, 3, False, 0.5, 0, 4.5)
lease(2, 3, False, 0.5, 0, 5)
lease(3, 2, False, 0.5, 0, 4.5)
lease(3, 2, False, 0.5, 0, 5)
for c, r in [(2, 5), (3, 3), (4, 2)]:
    lease(c, r, False, 0, 0.5, 1.5, edge=True)

# ================================================================ Row 6: Lease lock with fencing
for c, r in [(2, 1), (2, 3), (3, 1), (3, 2), (4, 1), (2, 4)]:
    lease(c, r, True, 0, 0.5, 2)
for c, r in [(2, 3), (3, 2)]:
    lease(c, r, True, 0, 0.5, 3)           # PAUSE past the bound: the fence is exercised
for c, r in [(5, 1), (4, 2), (3, 3)]:
    lease(c, r, True, 0, 0.5, 2, edge=True)

# ================================================================ Row 8: Kafka / nisshi (P1, KAFKA-17754)
def nisshi(t, r, l, u, w, b, fix=None, sd=None, edge=False):
    a = f"--txns {t} --retries {r} --l {l} --u {u} --w {w} --backoff {b}"
    if sd is not None:
        a += f" --sd {sd}"
    if fix:
        a += f" --fix {fix}"
    a += " --property stray --keep-going"
    add("nisshi", "nisshi_txn_timed",
        f"nisshi T={t} R={r} L={l} U={u} sd={sd or 0} W={w} B={b}{' fix=epoch-bump' if fix else ''}", a, None, edge)


for r in (1, 2, 3, 4):
    nisshi(2, r, 1, 5, 1, 0)
for u in (6, 7, 8):
    nisshi(2, 3, 1, u, 1, 0)
    nisshi(2, 4, 1, u, 1, 0)
for u in (9, 10, 12):
    nisshi(2, 3, 1, u, 5, 0)
nisshi(2, 4, 1, 9, 5, 0)
nisshi(2, 4, 1, 10, 5, 0)
nisshi(2, 3, 1, 8, 1, 2)
nisshi(2, 3, 1, 5, 1, 0, sd=1)
nisshi(2, 3, 1, 8, 1, 0, sd=1)
nisshi(2, 3, 1, 8, 1, 0, fix="epoch-bump")
nisshi(2, 4, 1, 8, 1, 0, fix="epoch-bump")
for t, r in [(4, 3), (8, 2), (16, 1), (16, 2), (32, 2), (64, 1), (8, 3), (4, 4)]:
    nisshi(t, r, 1, 5, 1, 0)
for t, r in [(16, 3), (8, 4)]:
    nisshi(t, r, 1, 5, 1, 0, edge=True)

# ================================================================ Row 10: PAR (Bosnacki-Dams listing)
def par(dk, dl, dr, to, k, r=1, noerr=False, edge=False):
    a = f"--dk {dk} --dl {dl} --dr {dr} --to {to} --messages {k} --retries {r} --keep-going"
    if noerr:
        a += " --no-errs"
    add("par", "par_timed", f"PAR dK={dk} dL={dl} dR={dr} To={to} K={k} R={r}{' no-errs' if noerr else ''}", a, None, edge)


for k in (3, 4, 5):
    par(3, 3, 1, 8, k)
    par(3, 3, 1, 7, k)
par(2, 5, 1, 8, 5)
par(2, 5, 1, 9, 5)
par(3, 3, 3, 9, 5)
par(3, 3, 3, 10, 5)
par(30, 30, 10, 70, 5)
par(30, 30, 10, 71, 5)
par(3, 3, 1, 9, 6)
par(3, 3, 1, 8, 6)
par(3, 3, 1, 7, 6)
par(3, 3, 1, 9, 7)
par(3, 3, 1, 9, 4, r=2)
for k in (8, 9, 10):
    par(3, 3, 1, 9, k, noerr=True)
for k, to in [(7, 8), (12, 9)]:
    par(3, 3, 1, to, k, edge=True, noerr=(k == 12))

# ================================================================ Row 9: Alternating bit (abp = control) + par variant
def abp(variant, k, r, w, edge=False, extra=""):
    a = f"--variant {variant} --messages {k} --retries {r} --w-ratio {w}{extra} --keep-going"
    add("abp", "alternating_bit_timed", f"ABP-file {variant} K={k} R={r} W={w}U", a, None, edge)


for k, r in [(2, 2), (3, 2), (2, 4), (4, 2), (2, 6)]:
    abp("abp", k, r, 3)
abp("abp", 4, 2, 2)
abp("abp", 3, 2, 2)
abp("abp", 4, 2, 1)
abp("abp", 3, 2, 1)
for k, r in [(6, 2), (2, 8)]:
    abp("abp", k, r, 3, edge=True)
for k, r, w in [(3, 2, 3), (4, 2, 3), (5, 2, 3), (4, 2, 2), (5, 2, 2)]:
    abp("par", k, r, w)

# ================================================================ Row 11: BRP (property (2) rows; (4) as footnote rows)
def brp(n, mx, tr, sy, files, prop="p2", edge=False):
    a = (f"--n {n} --max {mx} --td 1 --t1 3 --tr {tr} --sync {sy} --k 2 --files {files} "
         f"--property {prop} --keep-going")
    add("brp", "brp_timed", f"BRP F={files} n={n} MAX={mx} TD=1 T1=3 TR={tr} SYNC={sy} k=2 ({prop})", a, None, edge)


for n, mx, tr in [(2, 1, 9), (2, 2, 15), (3, 1, 9), (3, 2, 15), (2, 3, 21), (2, 4, 27), (4, 1, 9)]:
    brp(n, mx, tr, tr, 1)
brp(2, 3, 20, 20, 1)
brp(2, 3, 21, 20, 1)
brp(2, 3, 21, 19, 1)
for sy in (6, 7, 8, 9):
    brp(2, 1, 9, sy, 2)
brp(2, 2, 15, 15, 1, prop="p4")
brp(2, 3, 21, 21, 1, prop="p4")
for n, mx, tr in [(2, 6, 39), (5, 1, 9)]:
    brp(n, mx, tr, tr, 1, edge=True)
brp(3, 1, 9, 6, 2, edge=True)

# ================================================================ Row 3: NRP (NoDualPrimary)
def nrp(variant, r, extra="", edge=False):
    a = f"--variant {variant} --rounds {r}{extra} --keep-going"
    add("nrp", "nrp_failure_detector_timed", f"NRP {variant} R={r}{extra}", a, None, edge)


for v in ("nrp", "noopt", "leasing"):
    for r in (1, 2, 3):
        nrp(v, r)
for v in ("nrp", "noopt", "leasing"):
    nrp(v, 4, edge=True)
nrp("nrp", 3, " --u 2 --l-ratio 0.5", edge=True)

# ================================================================ Row 4: spanning tree (loop)
def stp(rounds, fd, lr=None, sr=None, edge=False, scenario="lost-hellos"):
    a = f"--scenario {scenario} --rounds {rounds} --forward-delay-ratio {fd}"
    if lr is not None:
        a += f" --l-ratio {lr}"
    if sr is not None:
        a += f" --sd-ratio {sr}"
    a += " --keep-going"
    add("stp", "spanning_tree_timed", f"STP {scenario} R={rounds} Fd={fd} L/U={lr or 0} sd/U={sr or 0}", a, None, edge)


for fd in (17, 18):
    stp(10, fd)
for fd in (15, 16):
    stp(10, fd, lr=1)
stp(10, 18, sr=1)
stp(10, 19, sr=1)
stp(11, 20)
for rounds, fd, lr in [(12, 21, None), (12, 20, 1), (13, 24, None)]:
    stp(rounds, fd, lr=lr, edge=True)
stp(17, 20, scenario="link-up", edge=True)

# ================================================================ Row 1: Chandra-Toueg (strong accuracy)
def ct(d, p, to, h, edge=False):
    add("ct", "chandra_toueg_timed", f"CT delta={d} phi={p} to={to} H={h}",
        f"--delta {d} --phi {p} --to {to} --horizon {h} --keep-going", None, edge)


for h in (10, 12, 14):
    ct(2, 2, 7, h)
for to in (5, 6, 9):
    ct(2, 2, to, 14)
ct(2, 4, 13, 13)
ct(2, 4, 13, 16)
ct(1, 1, 3, 8)
ct(1, 1, 5, 8)
ct(2, 2, 7, 16, edge=True)
ct(2, 4, 13, 19, edge=True)

# ================================================================ Row 2: Sensor network (both properties)
SENSOR = [
    "--sensors 1 --rounds 2 --keep-going",
    "--table3-row 1 --readings danger --keep-going",
    "--table3-row 2 --readings danger --keep-going",
    "--period-ratio 6 --keep-going",
    "--period-ratio 5 --keep-going",
    "--readings safe --property sensor --rounds 3 --keep-going",
]
SENSOR += H("sensor_network_timed", r"^--keep-going --u 3 --period-ratio 2 --check-ratio 5")
SENSOR += H("sensor_network_timed", r"^--keep-going --u 2 --period-ratio 3 --check-ratio 7\.5")
for a in SENSOR:
    add("sensor", "sensor_network_timed", "Sensor " + a.replace("--keep-going", "").strip()[:70], a, None, False)
for a in ("--table3-row 1 --keep-going", "--table3-row 2 --keep-going"):
    add("sensor", "sensor_network_timed", "Sensor " + a.replace("--keep-going", "").strip(), a, None, True)

# ================================================================ Row 13: YARN
def yarn(k, dd, u, h, prop=None, qs=None, edge=False):
    a = f"--ams {k}"
    if qs:
        a += f" --queue-size {qs}"
    a += f" --deadline {dd} --u {u} --horizon {h}"
    if prop:
        a += f" --property {prop}"
    a += " --keep-going"
    add("yarn", "yarn_scheduler_timed", f"YARN K={k} QS={qs or k + 1} DD={dd} U={u} H={h} {prop or 'miss'}", a, None, edge)


for dd, u in [(7, 0), (8, 0), (11, 1), (12, 1)]:
    yarn(1, dd, u, 12)
for prop in ("preempt", "late-report"):
    for dd, u in [(7, 0), (8, 0), (11, 1)]:
        yarn(1, dd, u, 12, prop=prop)
yarn(2, 7, 0, 8, qs=3)
yarn(2, 8, 0, 8, qs=3)
yarn(2, 8, 0, 10, qs=3)
yarn(2, 8, 0, 12, qs=3)
yarn(3, 8, 0, 7, qs=4)
yarn(2, 8, 0, 14, qs=3, edge=True)
yarn(3, 8, 0, 9, qs=4, edge=True)

# ================================================================ Row 12: Online shop
def retail(var, c, n, r, d=None, s=20, edge=False):
    a = f"--variant {var} --order-services {n} --customers {c} --rounds {r} --stock {s} --u 1"
    if d is not None:
        a += f" --deadline-ratio {d}"
    a += " --keep-going"
    add("retail", "retail_saga_timed", f"Shop {var} C={c} N={n} R={r} S={s} D={d}", a, None, edge)


for r in (1, 2, 3):
    retail("v2", 2, 2, r, 7)
retail("v2", 2, 2, 3, 6)
retail("v1", 2, 2, 2, 9)
retail("v2", 2, 2, 4, 7)
retail("v2", 3, 3, 1, 7)
retail("v2", 3, 3, 1, 9)
retail("v2", 3, 3, 2, 7, edge=True)
for r in (2, 3):
    retail("v2", 2, 2, r, None)                # SafeStockLevel control (no deadline)
