import csv, collections, json, os
os.chdir(os.path.expanduser("~/traceforge-v8r/run"))
rows = list(csv.DictReader(open("results_v8r.csv")))
print("rows", len(rows), dict(collections.Counter((r["build"], r["status"].split(" (")[0]) for r in rows)))
for r in rows:
    if r["status"].startswith(("error", "DNF")):
        print("!!", r["id"], r["example"], r["cell"][:40], "ec", r["exit_code"], r["status"][:40], "|", r["summary"][-160:])
st = json.load(open("status_v8r.json"))
print(st["time"], "running", len(st["running"]), "queued", st["queued"], "free", st["free"])
for j in sorted(st["running"], key=lambda j: -j["pred_s"])[:8]:
    print("  ", j)
