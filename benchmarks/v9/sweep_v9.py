#!/usr/bin/env python3
"""v9 benchmark scheduler (brain05, October 2026).

Runs the job plan (plan_v9.csv, one row per job = one process) until a hard
deadline and writes one CSV row per finished job. Built for wall times that can
be trusted and reproduced:

  * One job thread per PHYSICAL core. A job is pinned (taskset) to its own
    physical cores; the second hyper-thread of every core (logical CPU
    c + smt_offset) is never used by the campaign.
  * Timing phase with constant load. While a timing-grade job (3 repetitions,
    predicted <= 4 h) is queued or running, every free measurement core runs a
    spin loop ("ballast"), so the clock frequency seen by a measured job does
    not depend on how full the queue is. Measured on brain05: 14.3 s alone on
    an idle machine, 17.2-17.6 s with all cores busy, 22.2 s when the sibling
    hyper-thread is busy.
  * The scheduler, the per-job output filters and everything else of the
    campaign run on separate service cores.
  * Every job is audited while it runs (every --sample seconds): mean clock
    frequency of its cores, how busy its sibling hyper-threads were, how many
    foreign CPUs were busy on the machine, and cpu/wall at the end. A
    timing-grade run that fails the audit is flagged and repeated (at most
    --extra-reps times).
  * Wall, CPU and peak memory come from /usr/bin/time around the binary alone.

Order: reach-grade jobs (one run, predicted > 4 h) longest first, so every job
starts as early as its length requires; timing-grade jobs longest first on the
cores reserved for them (--reserve-timing), which are lent out once the
timing-grade queue is empty.

Output of a job goes through a filter that counts the lines naming a failing
task (one per violating execution under keep-going) and drops them and other
high-volume noise before the log is written; the count is appended as
"TFTASKLINES n".

A job still running at the deadline is stopped and recorded as DNF with its
last progress line. The scheduler can be restarted with --append: finished jobs
are skipped and jobs that are still running from the previous scheduler process
are adopted, not restarted.
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import resource
import shlex
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

FIELDS = ["id", "family", "example", "cell", "regime", "side", "rep", "tier", "purpose",
          "status", "exit_code", "expect_exit", "as_expected",
          "execs", "blocked", "explored", "timeline_impossible", "pruned", "viol_execs", "counters",
          "progress", "summary", "n_tfstats",
          "wall_s", "cpu_s", "cpu_over_wall", "rss_mb",
          "mhz_avg", "sib_busy_avg", "sib_busy_max", "foreign_busy_avg", "foreign_busy_max",
          "occupancy_avg", "ballast_frac", "samples", "timing_quality",
          "cores", "core_list", "parallel", "pred_s", "mem_gb",
          "started", "finished", "host", "binary_md5", "log", "args", "params"]

# Harmless high-volume lines dropped from the logs: generator-cancellation
# unwinds of the parallel pools and partitioned-mode chatter.
SPAM = (r"panicked at .*generator|^Box<dyn Any>|^note: run with|Stopping exploration because|^  rt[0-9]+")

AWK = r'''BEGIN { rx = ENVIRON["TASKRX"]; sp = ENVIRON["SPAM"]; n = 0 }
{ if (rx != "" && $0 ~ rx) { n++; next }
  if (sp != "" && $0 ~ sp) next
  print; fflush() }
END { printf "TFTASKLINES %d\n", n }'''

BALLAST = r'''
import os, sys, time
ppid, end = int(sys.argv[1]), float(sys.argv[2])
while True:
    t = time.time() + 0.5
    while time.time() < t:
        pass
    if os.getppid() != ppid or time.time() > end:
        break
'''

TAIL = 400000


def parse_cpulist(spec):
    out = []
    for piece in spec.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "-" in piece:
            a, b = piece.split("-")
            out += list(range(int(a), int(b) + 1))
        else:
            out.append(int(piece))
    return out


def fmt_dur(s):
    s = int(s)
    return f"{s // 86400} d {s % 86400 // 3600} h {s % 3600 // 60} min"


def safe_name(s):
    return re.sub(r"[^A-Za-z0-9._=-]+", "_", s)


def md5_of(path):
    try:
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


class Sampler:
    """Per-CPU busy fraction between two calls (from /proc/stat) and clock frequency."""

    def __init__(self):
        self.prev = self._read()

    @staticmethod
    def _read():
        d = {}
        try:
            with open("/proc/stat") as f:
                for line in f:
                    if line.startswith("cpu") and line[3:4].isdigit():
                        p = line.split()
                        v = [int(x) for x in p[1:9]]
                        d[int(p[0][3:])] = (sum(v), v[3] + v[4])
        except OSError:
            pass
        return d

    def sample(self):
        cur = self._read()
        busy = {}
        for c, (tot, idle) in cur.items():
            pt, pi = self.prev.get(c, (tot, idle))
            d = tot - pt
            busy[c] = 0.0 if d <= 0 else max(0.0, 1.0 - (idle - pi) / d)
        self.prev = cur
        return busy

    @staticmethod
    def mhz(cpus):
        vals = []
        for c in cpus:
            try:
                with open(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_cur_freq") as f:
                    vals.append(int(f.read()) / 1000.0)
            except (OSError, ValueError):
                pass
        return sum(vals) / len(vals) if vals else 0.0


def parse_log(text, counters_spec):
    ru = re.findall(r"TFRUSAGE user=([\d.]+) sys=([\d.]+) maxrss_kb=(\d+) elapsed=([\d.]+) exit=(\d+)", text)
    st = re.findall(r"TFSTATS execs=(\d+) blocked=(\d+) timeline_impossible=(\d+) pruned=(\d+)", text)
    prog = re.findall(r"Executions attempted so far: (\d+) total (\d+) finished normally (\d+) blocked", text)
    tl = re.findall(r"TFTASKLINES (\d+)", text)
    sig = re.findall(r"Command terminated by signal (\d+)", text)
    counters = {}
    for spec in counters_spec:
        try:
            m = re.findall(spec["regex"], text)
            if m:
                last = m[-1]
                counters[spec["key"]] = last if isinstance(last, str) else last[0]
        except re.error:
            counters[spec.get("key", "?")] = "bad regex"
    lines = [l for l in text.splitlines()
             if l.strip() and not l.startswith(("TFRUSAGE", "TFTASKLINES", "TFSTATS", "Executions attempted so far",
                                                "Command terminated", "Command exited"))
             and "=" in l]
    out = dict(
        execs=st[-1][0] if st else "",
        blocked=st[-1][1] if st else "",
        timeline_impossible=st[-1][2] if st else "",
        pruned=st[-1][3] if st else "",
        n_tfstats=len(st),
        viol_execs=tl[-1] if tl else "",
        counters=json.dumps(counters, sort_keys=True) if counters else "",
        progress=(f"{prog[-1][0]} explored ({prog[-1][1]} execs, {prog[-1][2]} blocked)" if prog else ""),
        summary=" | ".join(lines[-4:])[:1500],
        cpu=None, rss_mb="", elapsed=None, exit_in_log=None,
        signal=sig[-1] if sig else "",
    )
    if st:
        out["explored"] = str(int(st[-1][0]) + int(st[-1][1]))
    else:
        out["explored"] = ""
    if ru:
        u, s, rss, el, ex = ru[-1]
        out["cpu"] = float(u) + float(s)
        out["rss_mb"] = f"{int(rss) / 1024:.0f}"
        out["elapsed"] = float(el)
        out["exit_in_log"] = int(ex)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default=os.path.join(HERE, "plan_v9.csv"))
    ap.add_argument("--out", default=os.path.join(HERE, "results_v9.csv"))
    ap.add_argument("--binroot", default="/local/sramezani/v9/target/release/examples")
    ap.add_argument("--logs", default="/local/sramezani/v9/logs")
    ap.add_argument("--rundir", default="/local/sramezani/v9/run")
    ap.add_argument("--deadline", required=True, help="YYYY-MM-DD HH:MM, local clock")
    ap.add_argument("--cores", default="2-63,66-127", help="measurement cores (one logical CPU per physical core)")
    ap.add_argument("--service", default="0,1,64,65", help="cores for the scheduler and the output filters")
    ap.add_argument("--smt-offset", type=int, default=128, help="logical CPU c + offset is the sibling of c (0 = no SMT)")
    ap.add_argument("--numa-block", type=int, default=16, help="physical cores per NUMA node")
    ap.add_argument("--reserve-timing", type=int, default=44, help="cores kept for timing-grade jobs while any is queued")
    ap.add_argument("--no-ballast", action="store_true")
    ap.add_argument("--sample", type=float, default=10.0, help="seconds between audit samples")
    ap.add_argument("--extra-reps", type=int, default=2, help="extra repetitions of a timing-grade run that failed the audit")
    ap.add_argument("--min-window", type=int, default=120, help="do not start a job when less than this is left (s)")
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--only", default=None, help="regex on 'family example cell side'")
    ap.add_argument("--status", default=None)
    ap.add_argument("--progress-every", type=int, default=100000)
    ap.add_argument("--awk", default="mawk", help="awk used for the output filter")
    args = ap.parse_args()

    status_path = args.status or os.path.join(os.path.dirname(os.path.abspath(args.out)), "status_v9.json")
    running_path = os.path.join(os.path.dirname(os.path.abspath(args.out)), "running_v9.json")
    override_path = os.path.join(os.path.dirname(os.path.abspath(args.out)), "deadline_override.txt")
    os.makedirs(args.logs, exist_ok=True)
    os.makedirs(args.rundir, exist_ok=True)

    def read_deadline():
        d = args.deadline
        try:
            with open(override_path) as f:
                t = f.read().strip()
                if t:
                    d = t
        except OSError:
            pass
        try:
            return dt.datetime.strptime(d, "%Y-%m-%d %H:%M").timestamp(), d
        except ValueError:
            return dt.datetime.strptime(args.deadline, "%Y-%m-%d %H:%M").timestamp(), args.deadline

    deadline, deadline_txt = read_deadline()
    measure = parse_cpulist(args.cores)
    service = parse_cpulist(args.service)
    off = args.smt_offset
    sib = (lambda c: c + off) if off else (lambda c: None)
    ncpu = os.cpu_count() or 1
    service_all = sorted(set(service + [sib(c) for c in service if off and sib(c) < ncpu]))
    try:
        os.sched_setaffinity(0, set(service_all))
    except OSError as e:
        print(f"[v9] could not pin the scheduler to the service cores: {e}", flush=True)
    host = os.uname().nodename

    plan = list(csv.DictReader(open(args.plan)))
    if args.only:
        rx = re.compile(args.only)
        plan = [r for r in plan if rx.search(f"{r['family']} {r['example']} {r['cell']} {r['side']}")]
    done = set()
    if args.append and os.path.exists(args.out):
        for r in csv.DictReader(open(args.out)):
            done.add(r["id"])
    md5s = {}
    queue = []
    seen = set(done)

    def make_job(r):
        binp = os.path.join(args.binroot, r["example"])
        if r["example"] not in md5s:
            md5s[r["example"]] = md5_of(binp)
        try:
            cspec = json.loads(r.get("counters_spec") or "[]")
        except ValueError:
            cspec = []
        j = dict(r=r, id=r["id"], cores=int(float(r["cores"])), pred=float(r["pred_s"] or 0),
                 tier=r["tier"], mem=float(r["mem_gb"] or 128), cmd=[binp] + shlex.split(r["args"]),
                 cspec=cspec, taskrx=r.get("task_regex", ""), extra=0,
                 order=float(r.get("order") or 0))
        if j["cores"] > len(measure) or j["cores"] < 1:
            print(f"[v9] plan row {r['id']} needs {j['cores']} cores, only {len(measure)} measurement cores: skipped",
                  flush=True)
            return None
        return j

    for r in plan:
        seen.add(r["id"])
        if r["id"] in done:
            continue
        j = make_job(r)
        if j is not None:
            queue.append(j)
    queue.sort(key=lambda j: (j["order"], j["id"]))

    def reload_plan():
        """Rows appended to the plan file while the campaign runs are queued (new ids only)."""
        try:
            rows = list(csv.DictReader(open(args.plan)))
        except Exception:
            return
        added = 0
        for r in rows:
            try:
                if not r.get("id") or r["id"] in seen:
                    continue
                seen.add(r["id"])
                if args.only and not re.search(args.only, f"{r['family']} {r['example']} {r['cell']} {r['side']}"):
                    continue
                j = make_job(r)
                if j is not None:
                    queue.append(j)
                    added += 1
            except Exception as e:
                print(f"[v9] bad plan row {r.get('id')}: {e}", flush=True)
        if added:
            queue.sort(key=lambda j: (j["order"], j["id"]))
            print(f"[v9] {time.strftime('%F %T')} {added} new jobs read from the plan file", flush=True)
    print(f"[v9] {time.strftime('%F %T')} {len(queue)} jobs queued ({sum(1 for j in queue if j['tier'] == 'timing')} timing-grade), "
          f"{len(measure)} measurement cores, service {service_all}, deadline {deadline_txt}, "
          f"ballast {'off' if args.no_ballast else 'on during the timing phase'}", flush=True)

    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(args.out)), "sweep_v9.pid"), "w") as f:
            f.write(str(os.getpid()))
    except OSError:
        pass
    new = not (args.append and os.path.exists(args.out) and os.path.getsize(args.out) > 0)
    fout = open(args.out, "w" if new else "a", newline="")
    w = csv.DictWriter(fout, fieldnames=FIELDS, extrasaction="ignore")
    if new:
        w.writeheader()
        fout.flush()

    free = set(measure)
    running = []
    ballast = {}
    sampler = Sampler()
    state = dict(stop=False)

    # ---- adopt jobs left running by a previous scheduler process ---------------------------
    if args.append and os.path.exists(running_path):
        try:
            prev = json.load(open(running_path))
        except ValueError:
            prev = []
        by_id = {j["id"]: j for j in queue}
        for e in prev:
            job = by_id.get(e.get("id"))
            if job is None:
                continue
            alive = False
            try:
                with open(f"/proc/{e['pid']}/cmdline", "rb") as f:
                    alive = os.path.basename(job["cmd"][0]).encode() in f.read()
            except OSError:
                alive = False
            if not alive:
                # not running any more: usable only if it ended by itself (its log is complete);
                # otherwise it stays in the queue and is run again from the start
                try:
                    with open(e.get("log", ""), "rb") as f:
                        f.seek(0, 2)
                        f.seek(max(0, f.tell() - 20000))
                        if b"TFRUSAGE" not in f.read():
                            continue
                except OSError:
                    continue
            cores = [c for c in e["cores"] if c in free]
            if len(cores) != len(e["cores"]):
                continue
            for c in cores:
                free.discard(c)
            queue.remove(job)
            job["started"] = e.get("started", "")
            job["logpath"] = e["log"]
            running.append(dict(job=job, proc=None, filt=None, pid=e["pid"], cores=cores, t0=e["t0"],
                                acc=dict(n=0, sib=0.0, sibmax=0.0, fo=0.0, fomax=0, mhz=0.0, occ=0.0, bal=0),
                                adopted=True))
            print(f"[v9] adopted {job['id']} (pid {e['pid']}, {'alive' if alive else 'already finished'})", flush=True)

    def save_running():
        tmp = running_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump([dict(id=j["job"]["id"], pid=j["pid"], cores=j["cores"], t0=j["t0"],
                            started=j["job"].get("started", ""), log=j["job"].get("logpath", "")) for j in running], f)
        os.replace(tmp, running_path)

    def timing_active():
        return any(j["tier"] == "timing" for j in queue) or any(j["job"]["tier"] == "timing" for j in running)

    def kill_ballast(cores=None):
        for c in list(ballast if cores is None else cores):
            p = ballast.pop(c, None)
            if p is not None:
                try:
                    p.kill()
                    p.wait(timeout=10)
                except Exception:
                    pass

    def fill_ballast():
        if args.no_ballast:
            return
        if not timing_active() or state["stop"]:
            kill_ballast()
            return
        for c in list(ballast):
            if ballast[c].poll() is not None:
                ballast.pop(c)
        for c in sorted(free):
            if c not in ballast:
                try:
                    ballast[c] = subprocess.Popen(
                        [sys.executable, "-c", BALLAST, str(os.getpid()), str(deadline + 600)],
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        preexec_fn=(lambda cc=c: os.sched_setaffinity(0, {cc})))
                except Exception as e:
                    print(f"[v9] ballast on core {c} failed: {e}", flush=True)

    def pick_cores(k):
        """k free cores, inside one NUMA node when possible (best fit)."""
        blocks = {}
        for c in sorted(free):
            blocks.setdefault(c // args.numa_block, []).append(c)
        fits = [b for b in blocks.values() if len(b) >= k]
        if fits:
            return min(fits, key=len)[:k]
        take = []
        for b in sorted(blocks.values(), key=len, reverse=True):
            take += b[:k - len(take)]
            if len(take) == k:
                return take
        return None

    def children_of(pid):
        try:
            with open(f"/proc/{pid}/task/{pid}/children") as f:
                return [int(x) for x in f.read().split()]
        except (OSError, ValueError):
            return []

    def read_tail(path):
        try:
            with open(path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - TAIL))
                return f.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def launch(job, cores):
        kill_ballast(cores)
        for c in cores:
            free.discard(c)
        cl = ",".join(map(str, cores))
        logpath = os.path.join(args.logs, safe_name(job["id"]) + ".log")
        job["logpath"] = logpath
        rund = os.path.join(args.rundir, safe_name(job["id"]))
        os.makedirs(rund, exist_ok=True)
        mem = int(max(job["mem"], 64) * 1024 ** 3)

        def pre():
            resource.setrlimit(resource.RLIMIT_AS, (mem, mem))

        logf = open(logpath, "wb")
        logf.write((" ".join(shlex.quote(x) for x in job["cmd"]) + "\n").encode())
        logf.flush()
        filt = subprocess.Popen([args.awk, AWK], stdin=subprocess.PIPE, stdout=logf, stderr=subprocess.DEVNULL,
                                env=dict(os.environ, TASKRX=job["taskrx"], SPAM=SPAM), start_new_session=True)
        logf.close()
        full = ["taskset", "-c", cl, "/usr/bin/time", "-f",
                "TFRUSAGE user=%U sys=%S maxrss_kb=%M elapsed=%e exit=%x"] + job["cmd"]
        env = dict(os.environ, RAYON_NUM_THREADS=str(len(cores)), TF_PRINT_STATS="1",
                   TF_PROGRESS=str(args.progress_every))
        env.pop("CARGO_TARGET_DIR", None)
        p = subprocess.Popen(full, stdin=subprocess.DEVNULL, stdout=filt.stdin, stderr=subprocess.STDOUT,
                             start_new_session=True, preexec_fn=pre, env=env, cwd=rund)
        filt.stdin.close()
        job["started"] = time.strftime("%Y-%m-%d %H:%M:%S")
        running.append(dict(job=job, proc=p, filt=filt, pid=p.pid, cores=list(cores), t0=time.time(),
                            acc=dict(n=0, sib=0.0, sibmax=0.0, fo=0.0, fomax=0, mhz=0.0, occ=0.0, bal=0),
                            adopted=False))
        queue.remove(job)
        save_running()

    def quality(job, a, wall, cpu, ncores):
        """Audit verdict of one run: 'clean' or the reasons it is not."""
        flags = []
        n = a["n"]
        if n:
            if a["sib"] / n > 0.02:
                flags.append(f"sibling hyper-thread busy {100 * a['sib'] / n:.0f}% on average")
            if a["fo"] / n > 16:
                flags.append(f"{a['fo'] / n:.0f} foreign CPUs busy on average")
        if cpu is not None and wall and wall >= 5 and ncores == 1 and cpu / wall < 0.985:
            flags.append(f"cpu/wall {cpu / wall:.3f}")
        return "clean" if not flags else "; ".join(flags)

    def record(j, kind, ec):
        job, r, a = j["job"], j["job"]["r"], j["acc"]
        text = read_tail(job.get("logpath", "")) if job.get("logpath") else ""
        if j.get("adopted"):
            # the filter belongs to the previous scheduler process: give it time to write its last line
            for _ in range(15):
                if "TFTASKLINES" in text:
                    break
                time.sleep(1)
                text = read_tail(job.get("logpath", ""))
        p = parse_log(text, job["cspec"])
        wall = p["elapsed"] if p["elapsed"] is not None else time.time() - j["t0"]
        if ec is None and p["exit_in_log"] is not None and kind == "ok":
            ec = p["exit_in_log"]
        if kind == "ok" and p["signal"]:
            ec = -int(p["signal"])
        n = a["n"]
        q = quality(job, a, wall, p["cpu"], len(j["cores"])) if kind == "ok" else ""
        row = dict(id=job["id"], family=r["family"], example=r["example"], cell=r["cell"], regime=r["regime"],
                   side=r["side"], rep=r["rep"], tier=r["tier"], purpose=r["purpose"],
                   exit_code="" if ec is None else ec, expect_exit=r.get("expect_exit", ""),
                   execs=p["execs"], blocked=p["blocked"], explored=p["explored"],
                   timeline_impossible=p["timeline_impossible"], pruned=p["pruned"], viol_execs=p["viol_execs"],
                   counters=p["counters"], progress=p["progress"], summary=p["summary"], n_tfstats=p["n_tfstats"],
                   wall_s=f"{wall:.2f}", cpu_s="" if p["cpu"] is None else f"{p['cpu']:.2f}",
                   cpu_over_wall="" if p["cpu"] is None or not wall else f"{p['cpu'] / wall:.3f}",
                   rss_mb=p["rss_mb"],
                   mhz_avg=f"{a['mhz'] / n:.0f}" if n else "", sib_busy_avg=f"{a['sib'] / n:.4f}" if n else "",
                   sib_busy_max=f"{a['sibmax']:.3f}" if n else "", foreign_busy_avg=f"{a['fo'] / n:.2f}" if n else "",
                   foreign_busy_max=a["fomax"] if n else "", occupancy_avg=f"{a['occ'] / n:.1f}" if n else "",
                   ballast_frac=f"{a['bal'] / n:.2f}" if n else "", samples=n, timing_quality=q,
                   cores=len(j["cores"]), core_list=" ".join(map(str, j["cores"])), parallel=r.get("parallel", ""),
                   pred_s=r["pred_s"], mem_gb=job["mem"], started=job.get("started", ""),
                   finished=time.strftime("%Y-%m-%d %H:%M:%S"), host=host, binary_md5=md5s.get(r["example"], ""),
                   log=job.get("logpath", ""), args=r["args"], params=r.get("params", ""))
        if kind == "dnf":
            row["status"] = f"DNF (did not finish; stopped after {fmt_dur(wall)})"
        elif kind == "notrun":
            row["status"] = "not run (deadline reached before it could start)"
            row["wall_s"] = ""
        elif kind == "skipped":
            row["status"] = (f"removed by the operator after {fmt_dur(wall)} (replaced by another row)"
                             if j.get("cores") else "removed by the operator before it started")
            if not j.get("cores"):
                row["wall_s"] = ""
        elif ec == 0:
            row["status"] = "finished"
        elif ec == 101:
            row["status"] = "finished (assertion failed: FIRE)"
        else:
            row["status"] = f"error (exit {ec})"
        if kind == "ok" and r.get("expect_exit", "") != "":
            try:
                row["as_expected"] = "yes" if int(float(r["expect_exit"])) == ec else "NO"
            except ValueError:
                row["as_expected"] = ""
        w.writerow(row)
        fout.flush()
        print(f"{time.strftime('%m-%d %H:%M:%S')} {row['id']:<22} {row['side']:>8} {row['status']:<34} "
              f"execs={row['execs']} blocked={row['blocked']} viol={row['viol_execs']} wall={row['wall_s']}s "
              f"mhz={row['mhz_avg']} {q}", flush=True)
        # A timing-grade run that failed the audit is measured again.
        if (kind == "ok" and job["tier"] == "timing" and q != "clean" and job["extra"] < args.extra_reps
                and not state["stop"]):
            clone = dict(job)
            clone["extra"] = job["extra"] + 1
            base = re.sub(r"-x\d+$", "", job["id"])
            clone["id"] = f"{base}-x{clone['extra']}"
            clone["r"] = dict(r, id=clone["id"], rep=f"{r['rep']}x{clone['extra']}")
            queue.append(clone)
            queue.sort(key=lambda z: (z["order"], z["id"]))

    def release(j):
        for c in j["cores"]:
            free.add(c)
        running.remove(j)
        save_running()

    def finished_pid(j):
        """exit code if the job has ended, else None; adopted jobs are polled by pid."""
        if j["proc"] is not None:
            ec = j["proc"].poll()
            if ec is not None and j["filt"] is not None:
                try:
                    j["filt"].wait(timeout=120)
                except subprocess.TimeoutExpired:
                    j["filt"].kill()
            return ec
        try:
            os.kill(j["pid"], 0)
            with open(f"/proc/{j['pid']}/stat") as f:
                if f.read().rsplit(")", 1)[1].split()[0] == "Z":
                    return -999
            return None
        except (ProcessLookupError, OSError):
            return -999

    def stop_job(j):
        """Stop the binary but let /usr/bin/time and the filter finish, so the log keeps
        the CPU time and the violation count of an unfinished run."""
        kids = children_of(j["pid"])
        for k in kids:
            try:
                os.kill(k, signal.SIGKILL)
            except ProcessLookupError:
                pass
        t_end = time.time() + 15
        while time.time() < t_end and finished_pid(j) is None:
            time.sleep(0.2)
        if finished_pid(j) is None:
            try:
                os.killpg(j["pid"], signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            if j["proc"] is not None:
                try:
                    j["proc"].wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
        if j["filt"] is not None:
            try:
                j["filt"].wait(timeout=60)
            except subprocess.TimeoutExpired:
                j["filt"].kill()

    def stop_all(reason):
        state["stop"] = True
        kill_ballast()
        for j in list(running):
            stop_job(j)
            try:
                record(j, "dnf", None)
            except Exception as e:
                print(f"[v9] record failed for {j['job']['id']}: {e}", flush=True)
            release(j)
        print(f"[v9] {reason}", flush=True)

    control_path = os.path.join(os.path.dirname(os.path.abspath(args.out)), "control_v9.txt")
    handled = set()

    def read_control():
        """Operator commands, one per line in control_v9.txt: 'skip <job id>' removes a queued
        job, or stops it if it is running; either way it is recorded and not run again."""
        try:
            with open(control_path) as f:
                lines = [l.split() for l in f if l.strip() and not l.startswith("#")]
        except OSError:
            return
        for parts in lines:
            if len(parts) != 2 or parts[0] != "skip" or parts[1] in handled:
                continue
            jid = parts[1]
            handled.add(jid)
            try:
                for job in list(queue):
                    if job["id"] == jid:
                        queue.remove(job)
                        record(dict(job=job, cores=[], t0=time.time(), acc=dict(n=0)), "skipped", None)
                for j in list(running):
                    if j["job"]["id"] == jid:
                        stop_job(j)
                        record(j, "skipped", None)
                        release(j)
            except Exception as e:
                print(f"[v9] control 'skip {jid}' failed: {e}", flush=True)

    def on_term(sig, frm):
        print("[v9] SIGTERM: stopping running jobs, recording them as DNF", flush=True)
        stop_all("SWEEP-STOPPED")
        fout.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    measure_set = set(measure)
    sib_of = {c: sib(c) for c in measure} if off else {}
    known = set(service_all)
    last_sample = last_status = last_deadline_read = time.time()
    fill_ballast()
    while queue or running:
        now = time.time()
        if now - last_deadline_read > 30:
            last_deadline_read = now
            deadline, deadline_txt = read_deadline()
            reload_plan()
            read_control()
        # ---- finished jobs ---------------------------------------------------------------
        for j in list(running):
            ec = finished_pid(j)
            if ec is not None:
                try:
                    record(j, "ok", None if ec == -999 else ec)
                except Exception as e:
                    print(f"[v9] record failed for {j['job']['id']}: {e}", flush=True)
                release(j)
        # ---- deadline ----------------------------------------------------------------------
        if now >= deadline:
            for job in list(queue):
                queue.remove(job)
                try:
                    record(dict(job=job, cores=[], t0=now, acc=dict(n=0)), "notrun", None)
                except Exception as e:
                    print(f"[v9] record failed for {job['id']}: {e}", flush=True)
            stop_all("deadline reached")
            break
        remaining = deadline - now
        # ---- start what fits -----------------------------------------------------------------
        if remaining >= args.min_window:
            t_queued = any(j["tier"] == "timing" for j in queue)
            t_in_use = sum(len(j["cores"]) for j in running if j["job"]["tier"] == "timing")
            reserve = max(0, args.reserve_timing - t_in_use) if t_queued else 0
            waiting = set()     # tiers in which a multi-core job is waiting for cores
            for job in list(queue):
                if job["tier"] in waiting:
                    continue
                avail = len(free) - (reserve if job["tier"] != "timing" else 0)
                if job["cores"] <= avail:
                    cores = pick_cores(job["cores"])
                    if cores:
                        try:
                            launch(job, cores)
                        except Exception as e:
                            print(f"[v9] launch failed for {job['id']}: {e}", flush=True)
                            for c in cores:
                                free.add(c)
                            if job in queue:
                                queue.remove(job)
                elif job["cores"] > 1:
                    # later jobs of this tier must not take the cores it is collecting
                    waiting.add(job["tier"])
        fill_ballast()
        # ---- audit sample ----------------------------------------------------------------------
        if now - last_sample >= args.sample:
            last_sample = now
            try:
                busy = sampler.sample()
                ours = set(known) | set(ballast)
                for j in running:
                    ours.update(j["cores"])
                foreign = sum(1 for c, b in busy.items() if b > 0.5 and c not in ours)
                occ = sum(1 for c in measure if busy.get(c, 0.0) > 0.5)
                for j in running:
                    a = j["acc"]
                    sb = [busy.get(sib_of[c], 0.0) for c in j["cores"] if c in sib_of]
                    s = max(sb) if sb else 0.0
                    a["n"] += 1
                    a["sib"] += s
                    a["sibmax"] = max(a["sibmax"], s)
                    a["fo"] += foreign
                    a["fomax"] = max(a["fomax"], foreign)
                    a["mhz"] += sampler.mhz(j["cores"])
                    a["occ"] += occ
                    a["bal"] += 1 if ballast else 0
            except Exception as e:
                print(f"[v9] audit sample failed: {e}", flush=True)
        # ---- status ------------------------------------------------------------------------------
        if now - last_status > 30:
            last_status = now
            st = dict(time=time.strftime("%F %T"), deadline=deadline_txt, free_cores=len(free), ballast=len(ballast),
                      queued=len(queue), queued_timing=sum(1 for j in queue if j["tier"] == "timing"),
                      running=[dict(id=j["job"]["id"], cores=len(j["cores"]), elapsed_s=int(now - j["t0"]),
                                    pred_s=int(j["job"]["pred"]), tier=j["job"]["tier"]) for j in running])
            try:
                tmp = status_path + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(st, f, indent=1)
                os.replace(tmp, status_path)
            except OSError:
                pass
        time.sleep(0.5)
    kill_ballast()
    fout.close()
    try:
        os.remove(running_path)
    except OSError:
        pass
    print(f"[v9] {time.strftime('%F %T')} SWEEP-DONE", flush=True)


if __name__ == "__main__":
    main()
