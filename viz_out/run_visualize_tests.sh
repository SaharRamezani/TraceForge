#!/usr/bin/env bash
# Run every TraceForge visualization and rebuild the HTML
# viewer.
#
# Usage:
#   viz_out/run_visualize_tests.sh           # run from repo root
#   ./run_visualize_tests.sh                 # run from inside viz_out/

set -euo pipefail

# Resolve repo root from this script's location, so it works regardless of cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

EXAMPLES=(
  timed_visualize
  two_pc_visualize
  three_pc_visualize
  comm_closed_leader_election_visualize
)

echo "==> Running ${#EXAMPLES[@]} visualization harnesses from $REPO_ROOT"

for t in "${EXAMPLES[@]}"; do
  echo
  echo "---- cargo run -p traceforge --example $t ----"
  cargo run -p traceforge --example "$t" "$@"
done

echo
echo "==> Building viz_out/index.html"
python3 "$SCRIPT_DIR/build_html.py"

echo
echo "==> Done. Open viz_out/index.html in a browser."
