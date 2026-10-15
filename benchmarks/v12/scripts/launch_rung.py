#!/usr/bin/env python3
"""launch_rung.py <ladder-name-prefix> <size-json> <mustard|must> <target-machine> <cores> [order]
Writes launch_<n>.json with the rung's rows (as plan rows) ready for add_rows.py on the target machine."""
import json, sys, copy
name, size, side, target, cores = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5])
order = sys.argv[6] if len(sys.argv) > 6 else "-500"
out = []
for m in ("brain05", "brain10"):
    for lad in json.load(open(f"ladders_{m}.json")):
        if lad["name"] != name:
            continue
        for rg in lad["rungs"]:
            if rg["size"] != size:
                continue
            rows = rg["mrows" if side == "mustard" else "brows"]
            r = copy.deepcopy(rows[0])
            fam = r["family"]
            r["rep"] = "1"; r["reps"] = "1"
            if cores > 1:
                flag = f" --workers {cores}" if fam == "brp" else " --parallel shared"
                r.update(id=r["id"].replace("v12L-", f"v12P{cores}-W2-", 1), cores=str(cores), parallel="shared", order=order,
                         args=r["args"] + flag, purpose="parallel")
                if fam in ("yarn", "nrp", "retail"):
                    r["example"] = r["example"] + "_p"
            else:
                r.update(id=r["id"].replace("v12L-", "v12W2-", 1), order=order, purpose="wave2")
            out.append((m, r))
if not out:
    raise SystemExit(f"no rung {name} {size}")
json.dump([r for _, r in out[:1]], open(f"launch_{target}_{abs(hash((name, str(size), side))) % 10**6}.json", "w"))
m, r = out[0]
print(r["id"], r["cell"], r["cores"], r["params"], "->", target)
