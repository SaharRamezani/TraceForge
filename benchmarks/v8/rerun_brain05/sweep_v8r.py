#!/usr/bin/env python3
"""v8 RERUN scheduler (brain05, 2026-10-08): sweep_v8.py with a build per plan row.

Each plan row names its build (old = bc296a7, new = e96fe7a, vsend = bc296a7 plus the
send change only, vrest = e96fe7a with the send change reverted); the binary is
/local/sramezani/target-v8r/<build>/release/examples/<example>. The task-N lines
(one per counted execution with a certified assertion failure) are counted, not
only dropped: the log ends with "TFTASKLINES n". Everything else is the v8 scheduler:


Runs a PLAN (plan_v8.csv) of benchmark jobs until a hard deadline.
  * Every plan row is one job: one example binary, one side (timed or baseline),
    the exact argument string, the cores it needs, a predicted wall time.
  * Cores are split into per-group POOLS (group = example family, see --pools).
    A job starts on its own group's free cores; when a group has nothing left
    queued, its free cores are lent to the other groups (returned on job end).
  * Order inside a group: prio ascending, then longest predicted first.
    A prio-1 (headline) job that does not fit blocks later jobs of its group
    (no starvation); prio >= 2 jobs backfill.
  * Each job is pinned with taskset, wrapped in /usr/bin/time, limited by
    RLIMIT_AS (mem_gb), killed at the deadline (or --cap) and then recorded as
    "DNF (did not finish; killed after ...)".
  * One CSV row per job, written the moment it ends; --append resumes.
  * status_v8.json rewritten every 30 s.
The binaries are NOT taken from $CARGO_TARGET_DIR (brain05 exports an old one).
"""
import argparse, csv, json, os, re, resource, shlex, signal, subprocess, sys, tempfile, time
import datetime as dt

BINROOT = "/local/sramezani/target-v8r"
HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs_v8r")
os.makedirs(LOGS, exist_ok=True)
# Full (filtered) job logs go to local disk, never to /tmp (tmpfs = RAM on brain05).
RAWLOGS = "/local/sramezani/v8rlogs"
os.makedirs(RAWLOGS, exist_ok=True)
# Harmless high-volume lines: one "task-N" per violating ending under
# --keep-going, generator-cancellation unwinds of the parallel pools, and
# partitioned-mode chatter. Dropped before they reach the log.
SPAM = (r"^task-[0-9]+$|^main-thread-ThreadId\([0-9]+\)$|panicked at .*generator|^Box<dyn Any>|"
        r"^note: run with|Stopping exploration because|^  rt[0-9]+")
TAIL = 400000

FIELDS = ["id", "orig_id", "build", "family", "tasklines", "group", "example", "cell", "side", "purpose", "prio", "expected", "status", "exit_code",
          "execs", "blocked", "timeline_impossible", "pruned", "violations", "progress", "summary",
          "wall_s", "cpu_s", "rss_mb", "cpus", "parallel", "pred_s", "mem_gb",
          "started", "finished", "cores", "log", "args"]


def parse_pools(spec):
    pools = {}
    for part in spec.split(";"):
        part = part.strip()
        if not part:
            continue
        g, rng = part.split("=")
        cores = []
        for piece in rng.split(","):
            if "-" in piece:
                a, b = piece.split("-")
                cores += list(range(int(a), int(b) + 1))
            else:
                cores.append(int(piece))
        pools[g.strip()] = cores
    return pools


def fmt_dur(s):
    s = int(s)
    return f"{s // 86400} d {s % 86400 // 3600} h {s % 3600 // 60} min"


def last_int(pat, out):
    m = re.findall(pat, out)
    return m[-1] if m else ""


def parse(out):
    ru = re.search(r"TFRUSAGE user=([\d.]+) sys=([\d.]+) maxrss_kb=(\d+) elapsed=([\d.:]+)", out)
    st = re.findall(r"TFSTATS execs=(\d+) blocked=(\d+) timeline_impossible=(\d+) pruned=(\d+)", out)
    prog = re.findall(r"Executions attempted so far: (\d+) total (\d+) finished normally (\d+) blocked", out)
    lines = [l for l in out.splitlines()
             if not re.fullmatch(r"task-\d+", l.strip()) and not l.startswith("TFRUSAGE")
             and not l.startswith("TFTASKLINES")
             and re.search(r"execs=|blocked=|violations=|impossible|pruned|explored|reduction|FIRE|HOLD|panicked|verified", l)]
    return dict(
        execs=st[-1][0] if st else last_int(r"\bexecs=(\d+)", out),
        blocked=st[-1][1] if st else last_int(r"\bblocked=(\d+)", out),
        timeline_impossible=st[-1][2] if st else last_int(r"(?:timeline_impossible|impossible)=(\d+)", out),
        pruned=st[-1][3] if st else last_int(r"\bpruned=(\d+)", out),
        progress=(f"{prog[-1][0]} explored ({prog[-1][1]} execs, {prog[-1][2]} blocked)" if prog else ""),
        violations=last_int(r"\bviolations=(\d+)", out),
        tasklines=last_int(r"TFTASKLINES (\d+)", out),
        summary=" | ".join(lines[-4:])[:1500],
        cpu_s=f"{float(ru.group(1)) + float(ru.group(2)):.1f}" if ru else "",
        rss_mb=f"{int(ru.group(3)) / 1024:.0f}" if ru else "",
        elapsed=float(ru.group(4)) if ru else None,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default=os.path.join(HERE, "plan_v8r.csv"))
    ap.add_argument("--out", default=os.path.join(HERE, "results_v8r.csv"))
    ap.add_argument("--deadline", required=True, help="YYYY-MM-DD HH:MM, local clock")
    ap.add_argument("--pools", required=True, help="'lease=4-91;swim=92-179;...'")
    ap.add_argument("--cap", type=int, default=10 ** 9)
    ap.add_argument("--min-window", type=int, default=300)
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--only", default=None, help="regex on 'group example cell side'")
    args = ap.parse_args()

    deadline = dt.datetime.strptime(args.deadline, "%Y-%m-%d %H:%M").timestamp()
    pools = parse_pools(args.pools)
    owner = {c: g for g, cs in pools.items() for c in cs}
    free = {g: sorted(cs) for g, cs in pools.items()}

    plan = list(csv.DictReader(open(args.plan)))
    if args.only:
        rx = re.compile(args.only)
        plan = [r for r in plan if rx.search(f"{r['group']} {r['example']} {r['cell']} {r['side']}")]
    done = set()
    if args.append and os.path.exists(args.out):
        for r in csv.DictReader(open(args.out)):
            done.add(r["id"])
    queue = []
    for r in plan:
        if r["id"] in done:
            continue
        if r["group"] not in pools:
            sys.exit(f"plan row {r['id']}: group {r['group']} has no pool")
        j = dict(r=r, id=r["id"], group=r["group"], cpus=int(float(r["cpus"])), pred=float(r["pred_s"]),
                 prio=int(float(r["prio"])), mem=float(r["mem_gb"] or 150))
        j["cmd"] = [f"{BINROOT}/{r['build']}/release/examples/{r['example']}"] + shlex.split(r["args"])
        queue.append(j)
    queue.sort(key=lambda j: (j["prio"], -j["pred"]))
    print(f"[v8r] {len(queue)} jobs queued, pools " + ", ".join(f"{g}:{len(c)}" for g, c in pools.items())
          + f", deadline {args.deadline}", flush=True)

    new = not (args.append and os.path.exists(args.out) and os.path.getsize(args.out) > 0)
    fout = open(args.out, "a" if not new else "w", newline="")
    w = csv.DictWriter(fout, fieldnames=FIELDS, extrasaction="ignore")
    if new:
        w.writeheader()
        fout.flush()

    running = []

    def give_back(cores):
        for c in cores:
            free[owner[c]].append(c)
        for g in free:
            free[g].sort()

    def record(job, status, wall, out, ec, cores):
        r = job["r"]
        p = parse(out)
        el = p.pop("elapsed")
        if el is not None:
            wall = el
        row = dict(id=job["id"], orig_id=r["orig_id"], build=r["build"], family=r["family"], group=r["group"], example=r["example"], cell=r["cell"], side=r["side"],
                   purpose=r["purpose"], prio=r["prio"], expected=r["expected"], exit_code="" if ec is None else ec,
                   wall_s=f"{wall:.1f}", cpus=job["cpus"], parallel=r.get("parallel", ""), pred_s=r["pred_s"],
                   mem_gb=job["mem"], started=job.get("started", ""), finished=time.strftime("%Y-%m-%d %H:%M:%S"),
                   cores=",".join(map(str, cores)) if cores else "", log=job.get("logpath", ""),
                   args=r["args"], **p)
        if status == "dnf":
            row["status"] = f"DNF (did not finish; killed after {fmt_dur(wall)})"
        elif status == "notrun":
            row["status"] = "not run (deadline too close)"
        elif ec == 0:
            row["status"] = "finished"
        elif ec == 101:
            row["status"] = "finished (assertion failed: FIRE)"
        else:
            row["status"] = f"error (exit {ec})"
        w.writerow(row)
        fout.flush()
        print(f"{time.strftime('%m-%d %H:%M:%S')} {row['group']:>7} {row['example']:<28} {row['cell']:<34} "
              f"{row['side']:>8} {row['status']:<40} execs={row['execs']} blocked={row['blocked']} "
              f"viol={row['violations']} impossible={row['timeline_impossible']} wall={row['wall_s']}s",
              flush=True)
        if out:
            safe = re.sub(r"[^A-Za-z0-9._=-]+", "_", job["id"])
            with open(os.path.join(LOGS, safe + ".log"), "w") as f:
                f.write(" ".join(job["cmd"]) + "\n")
                f.write(out)

    def read_out(j):
        # only the tail: the summary, TFSTATS and TFRUSAGE lines are last
        try:
            with open(j["job"]["logpath"], "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - TAIL))
                return f.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def kill_all(status):
        for j in list(running):
            try:
                os.killpg(j["proc"].pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            j["proc"].wait()
            record(j["job"], status, time.time() - j["t0"], read_out(j), None, j["cores"])
            give_back(j["cores"])
            running.remove(j)

    def on_term(sig, frm):
        print("[v8r] SIGTERM: killing running jobs, recording them as DNF", flush=True)
        kill_all("dnf")
        fout.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    def launch(job, cores):
        cl = ",".join(map(str, cores))
        safe = re.sub(r"[^A-Za-z0-9._=-]+", "_", job["id"])
        logpath = os.path.join(RAWLOGS, safe + ".log")
        job["logpath"] = logpath
        mem = int(max(job["mem"], 32) * 1024 ** 3)

        def pre():
            resource.setrlimit(resource.RLIMIT_AS, (mem, mem))

        full = ["taskset", "-c", cl, "/usr/bin/time", "-f", "TFRUSAGE user=%U sys=%S maxrss_kb=%M elapsed=%e"] + job["cmd"]
        # grep drops the spam; `; true` keeps grep's own status out of the
        # pipeline so that pipefail reports the job's exit code.
        wrapper = ["bash", "-o", "pipefail", "-c",
                   'log="$1"; shift; exec "$@" 2>&1 | awk \'BEGIN{s=ENVIRON["SPAM"]} /^task-[0-9]+$/{n++; next} $0 ~ s {next} {print; fflush()} END{printf "TFTASKLINES %d\\n", n}\' > "$log"',
                   "_", logpath] + full
        p = subprocess.Popen(wrapper, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
                             preexec_fn=pre,
                             env=dict(os.environ, RAYON_NUM_THREADS=str(len(cores)), SPAM=SPAM,
                                      TF_PRINT_STATS="1", TF_PROGRESS=str(job.get("progress_every", 100000))))
        job["started"] = time.strftime("%Y-%m-%d %H:%M:%S")
        running.append(dict(job=job, proc=p, cores=cores, t0=time.time(),
                            t_end=min(deadline, time.time() + args.cap)))
        queue.remove(job)

    last_status = 0
    while queue or running:
        now = time.time()
        for j in list(running):
            ec = j["proc"].poll()
            if ec is not None:
                record(j["job"], "ok", now - j["t0"], read_out(j), ec, j["cores"])
                give_back(j["cores"])
                running.remove(j)
            elif now >= j["t_end"]:
                try:
                    os.killpg(j["proc"].pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                j["proc"].wait()
                record(j["job"], "dnf", now - j["t0"], read_out(j), None, j["cores"])
                give_back(j["cores"])
                running.remove(j)
        remaining = deadline - now
        for job in list(queue):
            too_close = remaining < args.min_window
            hopeless = job["pred"] > 3 * remaining and job["r"]["purpose"] != "ceiling"
            if too_close or hopeless:
                queue.remove(job)
                record(job, "notrun", 0.0, "", None, None)
        queued_groups = {j["group"] for j in queue}
        blocked_groups = set()
        for job in list(queue):
            g = job["group"]
            if g in blocked_groups:
                continue
            own = free[g]
            lenders = [h for h in free if h != g and h not in queued_groups]
            avail = len(own) + sum(len(free[h]) for h in lenders)
            if avail >= job["cpus"]:
                take = own[:job["cpus"]]
                del own[:len(take)]
                need = job["cpus"] - len(take)
                for h in lenders:
                    if need == 0:
                        break
                    got = free[h][:need]
                    del free[h][:len(got)]
                    take += got
                    need -= len(got)
                launch(job, take)
            elif job["prio"] <= 1:
                blocked_groups.add(g)
        if now - last_status > 30:
            last_status = now
            st = dict(time=time.strftime("%F %T"), deadline=args.deadline,
                      free={g: len(c) for g, c in free.items()}, queued=len(queue),
                      queued_by_group={g: sum(1 for j in queue if j["group"] == g) for g in pools},
                      running=[dict(id=j["job"]["id"], cpus=len(j["cores"]), elapsed_s=int(now - j["t0"]),
                                    pred_s=int(j["job"]["pred"])) for j in running])
            json.dump(st, open(os.path.join(HERE, "status_v8r.json"), "w"), indent=1)
        time.sleep(1)
    print("[v8r] SWEEP-DONE", flush=True)


if __name__ == "__main__":
    main()
