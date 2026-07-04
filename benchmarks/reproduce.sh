#!/usr/bin/env bash
# Rebuild and re-run the entire MUST-tau benchmark sweep, then regenerate tables.
# Usage:  bash benchmarks/reproduce.sh
# Branch: inbox (the timed inbox model). Do NOT switch branches.
set -euo pipefail
cd "$(dirname "$0")/.."          # repo root

echo "==> building examples (release)"
cargo build --release --examples

# Single consolidated results file. Every phase appends to it; the `group` column
# (core | n4_tail | n5_tail | ballots2) partitions the rows.
OUT=benchmarks/results.csv
rm -f "$OUT"   # fresh consolidated file

echo "==> Phase A: zero-code-change protocols (comm-closed LE x2, raft N=3)"
python3 benchmarks/sweep.py --which comm_closed,comm_closed_inbox,raft \
        --out "$OUT" --jobs 2 --timeout 600

echo "==> Phase B (group=core): 3PC + 3PC-buggy full 60-cell grids (needs the --l-ratio/--sd-ratio edit)"
python3 benchmarks/sweep.py --which 3pc,3pc_buggy \
        --out "$OUT" --append --jobs 2 --timeout 600

echo "==> Tail (group=n4_tail/n5_tail; time-boxed, may time out): 3PC N=4, raft N=5, comm-closed N=5"
# serial + memory cap so a blow-up cannot hang the machine; failures are logged not fatal.
python3 benchmarks/sweep.py --which 3pc4,raft5,ccle5_timed \
        --out "$OUT" --append --jobs 1 --timeout 600 --mem-gb 8 || true

echo "==> ballots=2 sd test (group=ballots2; comm-closed N=3, B=2; ~3-14 min/cell; untimed baseline intractable)"
python3 benchmarks/sweep.py --which ccle_b2 \
        --out "$OUT" --append --jobs 1 --timeout 900 --mem-gb 10 || true

echo "==> regenerating LaTeX tables + verdict-stability summary"
python3 benchmarks/make_tables.py

echo "==> done.  ALL results in: $OUT   tables: benchmarks/tables/   summary: benchmarks/VERDICT_STABILITY.md"
