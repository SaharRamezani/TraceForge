#!/usr/bin/env python3
"""v7 ceiling sweep for lease and SWIM (2026-09-20).

Executes a PLAN (plan_v7.csv, produced by make_plan.py) on one machine:
  * each job is pinned to its own cores with taskset (num_cpus honours affinity, so
    `--parallel partitioned` uses exactly the granted cores);
  * jobs are started greedily in plan order whenever enough cores are free;
  * every job may run until the shared --deadline (or --cap seconds, whichever is
    sooner); a job still running then is killed and recorded as
    "DNF (did not finish ...)" with the time it ran;
  * one CSV row per (cell, side) is appended the moment the job ends;
  * --append resumes (rows already in the output are skipped);
  * status_v7.json is rewritten every 30 s (running jobs, free cores, queue length).

Untimed and timed sides are separate jobs (as in v6): a DNF on one side never
delays the other.
"""
import argparse, csv, json, os, re, signal, subprocess, sys, tempfile, time, datetime as dt

# NOT read from $CARGO_TARGET_DIR: brain05 exports it pointing at the old v5 build (no --keep-going)
BIN = "/local/sramezani/target-v7/release/examples"
HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs_v7")
os.makedirs(LOGS, exist_ok=True)

FIELDS = ["protocol", "N", "rounds", "cell", "family", "side", "variant", "execs", "block", "violations",
          "verdict", "expected", "wall_s", "cpu_s", "rss_mb", "cpus", "parallel", "pred_s", "started",
          "finished", "cmd"]

def lease_cmd(r, mode):
    u, l, sd, ttl = int(r["u"]), int(r["l"]), int(r["sd"]), int(r["ttl"])
    bnd = ttl - 2 * (u - l + sd)
    fence = r["fence"]
    pause = bnd if fence == "on" else bnd - 1
    tag = f"C={r['N']} U={u} L={l} sd={sd} TTL={ttl} pause={pause} fence={fence}"
    cmd = [f"{BIN}/lease_timed", "--clients", r["N"], "--rounds", r["R"], "--u", str(u),
           "--l-ratio", str(l / u), "--sd-ratio", str(sd / u), "--ttl-ratio", str(ttl / u),
           "--pause-ratio", str(pause / u), "--fencing", fence, "--keep-going", "--mode", mode]
    exp = {"timed": "hold", "baseline": "hold" if fence == "on" else "FIRE"}[mode]
    return tag, cmd, exp

def swim_cmd(r, mode):
    u, l, sd = int(r["u"]), int(r["l"]), int(r["sd"])
    wp = u
    ws = 2 * u + 2 * sd - min(l, max(0, 2 * l - wp)) + 1
    tag = f"N={r['N']} U={u} L={l} sd={sd} Wp={wp} Ws={ws}"
    cmd = [f"{BIN}/swim_timed", "--nodes", r["N"], "--rounds", r["R"], "--u", str(u), "--l-ratio", str(l / u),
           "--sd-ratio", str(sd / u), "--w-probe-ratio", str(wp / u), "--w-suspect-ratio", str(ws / u),
           "--keep-going", "--mode", mode]
    return tag, cmd, {"timed": "hold", "baseline": "FIRE"}[mode]

def build(r):
    tag, cmd, exp = (lease_cmd if r["proto"] == "lease" else swim_cmd)(r, r["side"])
    if r["par"] != "none":
        cmd += ["--parallel", r["par"]]
    return tag, cmd, exp

def key_of(protocol, N, rounds, cell, side):
    return (str(protocol), str(N), str(rounds), str(cell), str(side))

def fmt_dur(s):
    s = int(s)
    return f"{s // 86400} d {s % 86400 // 3600} h {s % 3600 // 60} min"

def parse(out):
    g = lambda p: (int(re.search(p, out).group(1)) if re.search(p, out) else None)
    ru = re.search(r"TFRUSAGE user=([\d.]+) sys=([\d.]+) maxrss_kb=(\d+)", out)
    return (g(r"execs=(\d+)"), g(r"blocked=(\d+)"), g(r"violations=(\d+)"),
            f"{float(ru.group(1)) + float(ru.group(2)):.1f}" if ru else "",
            f"{int(ru.group(3)) / 1024:.0f}" if ru else "")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default=os.path.join(HERE, "plan_v7.csv"))
    ap.add_argument("--out", default=os.path.join(HERE, "results_v7_ceiling.csv"))
    ap.add_argument("--deadline", required=True, help="YYYY-MM-DD HH:MM (local clock of this machine)")
    ap.add_argument("--cap", type=int, default=172800, help="per-job cap in seconds (default 2 days)")
    ap.add_argument("--cores", default="28-243", help="big pool: long / many-core jobs, strict longest-first order")
    ap.add_argument("--small-cores", default="4-27", help="small pool: jobs predicted <= --small-pred s on <= --small-cpus cores")
    ap.add_argument("--small-pred", type=float, default=7200)
    ap.add_argument("--small-cpus", type=int, default=8)
    ap.add_argument("--min-window", type=int, default=600, help="do not start a job with less window left")
    ap.add_argument("--mem-gb", type=float, default=150)
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--only", default=None, help="regex on 'proto N R side fence'")
    args = ap.parse_args()

    deadline = dt.datetime.strptime(args.deadline, "%Y-%m-%d %H:%M").timestamp()
    lo, hi = (int(x) for x in args.cores.split("-"))
    free = list(range(lo, hi + 1))                     # big pool
    slo, shi = (int(x) for x in args.small_cores.split("-"))
    sfree = list(range(slo, shi + 1))                  # small pool
    plan = list(csv.DictReader(open(args.plan)))
    if args.only:
        rx = re.compile(args.only)
        plan = [r for r in plan if rx.search(f"{r['proto']} {r['N']} {r['R']} {r['side']} {r.get('fence', '')}")]
    done = set()
    if args.append and os.path.exists(args.out):
        for r in csv.DictReader(open(args.out)):
            done.add(key_of(r["protocol"], r["N"], r["rounds"], r["cell"], r["side"]))
    queue = []
    for r in plan:
        tag, cmd, exp = build(r)
        k = key_of(f"{r['proto']}_timed", r["N"], r["R"], tag, r["side"])
        if k in done:
            continue
        queue.append(dict(r=r, tag=tag, cmd=cmd, exp=exp, cpus=int(r["cpus"]), pred=float(r["pred_s"]), key=k))
    for j in queue:
        j["small"] = j["pred"] <= args.small_pred and j["cpus"] <= args.small_cpus
    # longest predicted WALL first (they need the whole window); tier 1 = the headline families
    tier = lambda j: 0 if j["r"]["family"] in ("lease_unfenced", "swim_L0", "swim_untimed") else 1
    queue.sort(key=lambda j: (tier(j), -j["pred"]))
    print(f"[v7] small pool {len(sfree)} cores: {sum(j['small'] for j in queue)} jobs; big pool {len(free)} cores: "
          f"{sum(not j['small'] for j in queue)} jobs", flush=True)

    new = not (args.append and os.path.exists(args.out) and os.path.getsize(args.out) > 0)
    fout = open(args.out, "a" if args.append else "w", newline="")
    w = csv.DictWriter(fout, fieldnames=FIELDS, extrasaction="ignore")
    if new:
        w.writeheader()
    fout.flush()
    print(f"[v7] {len(queue)} jobs queued, {len(free)} cores, deadline {args.deadline}, cap {args.cap}s", flush=True)

    running = []   # dicts: job, proc, cores, t0, t_end, logf
    def record(job, status, wall, out, ec):
        r = job["r"]
        ex, bl, vi, cpu, rss = parse(out)
        row = dict(protocol=f"{r['proto']}_timed", N=r["N"], rounds=r["R"], cell=job["tag"], family=r["family"],
                   side=job["r"]["side"], variant="ceiling", execs=ex if ex is not None else "",
                   block=bl if bl is not None else "", violations=vi if vi is not None else "", expected=job["exp"],
                   wall_s=f"{wall:.1f}", cpu_s=cpu, rss_mb=rss, cpus=job["cpus"], parallel=r["par"],
                   pred_s=f"{job['pred']:.0f}", started=job["started"],
                   finished=time.strftime("%Y-%m-%d %H:%M:%S"),
                   cmd=" ".join(job["cmd"]).replace(BIN + os.sep, ""))
        if status == "dnf":
            row["verdict"] = f"DNF (did not finish; killed after {fmt_dur(wall)})"
            row["execs"] = row["block"] = row["violations"] = ""
        elif status == "notrun":
            row["verdict"] = "not run (deadline too close)"
        elif ec != 0 or ex is None or vi is None:
            row["verdict"] = f"err(ec={ec})"
        else:
            row["verdict"] = "FIRE" if vi > 0 else "hold"
        w.writerow(row); fout.flush()
        print(f"{time.strftime('%m-%d %H:%M:%S')} {row['protocol']} N{row['N']} R{row['rounds']} {row['side']:>8} "
              f"{row['verdict']:<40} exec={row['execs']} blk={row['block']} viol={row['violations']} "
              f"wall={row['wall_s']}s cpus={row['cpus']}", flush=True)
        if row["verdict"] != row["expected"] and status == "ok":
            print("   ^^ UNEXPECTED verdict", flush=True)
        if status == "ok" and (row["verdict"] != row["expected"] or ex is None):
            open(os.path.join(LOGS, re.sub(r"[^A-Za-z0-9._=-]+", "_", f"{row['protocol']}_N{row['N']}_R{row['rounds']}_{row['side']}") + ".log"), "w").write(out[-200000:])

    def kill_all(status):
        for j in list(running):
            try: os.killpg(j["proc"].pid, signal.SIGKILL)
            except ProcessLookupError: pass
            j["proc"].wait()
            j["logf"].seek(0); out = j["logf"].read().decode("utf-8", "replace")
            record(j["job"], status, time.time() - j["t0"], out, None)
            (sfree if j["job"]["small"] else free).extend(j["cores"]); running.remove(j)
    def on_term(sig, frm):
        print("[v7] SIGTERM: killing running jobs and recording them as DNF", flush=True)
        kill_all("dnf"); fout.close(); sys.exit(0)
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    last_status = 0
    while queue or running:
        now = time.time()
        # reap finished / timed-out
        for j in list(running):
            ec = j["proc"].poll()
            if ec is not None:
                j["logf"].seek(0); out = j["logf"].read().decode("utf-8", "replace")
                record(j["job"], "ok", now - j["t0"], out, ec)
                (sfree if j["job"]["small"] else free).extend(j["cores"]); running.remove(j)
            elif now >= j["t_end"]:
                try: os.killpg(j["proc"].pid, signal.SIGKILL)
                except ProcessLookupError: pass
                j["proc"].wait()
                j["logf"].seek(0); out = j["logf"].read().decode("utf-8", "replace")
                record(j["job"], "dnf", now - j["t0"], out, None)
                (sfree if j["job"]["small"] else free).extend(j["cores"]); running.remove(j)
        free.sort(); sfree.sort()
        # start what fits.  Small pool: first-fit with backfill.  Big pool: strict order, no backfill,
        # so a 32/64-core job is never starved by later small ones.
        def launch(job, pool):
            cores = pool[:job["cpus"]]; del pool[:job["cpus"]]
            cl = ",".join(map(str, cores))
            logf = tempfile.TemporaryFile()
            mem = int(args.mem_gb * 1024 ** 3)
            pre = lambda: __import__("resource").setrlimit(__import__("resource").RLIMIT_AS, (mem, mem))
            full = ["taskset", "-c", cl, "/usr/bin/time", "-f", "TFRUSAGE user=%U sys=%S maxrss_kb=%M"] + job["cmd"]
            p = subprocess.Popen(full, stdout=logf, stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=pre)
            job["started"] = time.strftime("%Y-%m-%d %H:%M:%S")
            running.append(dict(job=job, proc=p, cores=cores, t0=time.time(), t_end=min(deadline, time.time() + args.cap), logf=logf))
            queue.remove(job)
        for job in list(queue):
            if deadline - now < args.min_window or job["pred"] > 3 * (deadline - now):
                queue.remove(job); record(job, "notrun", 0.0, "", None); continue
        for job in [q for q in queue if q["small"]]:
            if len(sfree) >= job["cpus"]:
                launch(job, sfree)
        for job in [q for q in queue if not q["small"]]:
            if len(free) >= job["cpus"]:
                launch(job, free)
            else:
                break
        if now - last_status > 30:
            last_status = now
            st = dict(time=time.strftime("%F %T"), free_cores=len(free), free_small=len(sfree), queued=len(queue),
                      running=[dict(family=j["job"]["r"]["family"], proto=j["job"]["r"]["proto"], N=j["job"]["r"]["N"], R=j["job"]["r"]["R"],
                                    side=j["job"]["r"]["side"], cpus=j["job"]["cpus"], elapsed_s=int(now - j["t0"]),
                                    pred_s=int(j["job"]["pred"])) for j in running])
            json.dump(st, open(os.path.join(HERE, "status_v7.json"), "w"), indent=1)
        time.sleep(2)
    print("[v7] SWEEP-DONE", flush=True)

if __name__ == "__main__":
    main()
