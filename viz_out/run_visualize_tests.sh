#!/usr/bin/env bash
# Run every TraceForge visualization integration test and rebuild the HTML
# viewer. Each test writes its own viz_out/<slug>/ directory; build_html.py
# stitches them into viz_out/index.html.
#
# Usage:
#   viz_out/run_visualize_tests.sh           # run from repo root
#   ./run_visualize_tests.sh                 # run from inside viz_out/
#
# Forward extra args to cargo (e.g. --release, --features=foo):
#   viz_out/run_visualize_tests.sh --release

set -euo pipefail

# Resolve repo root from this script's location, so it works regardless of cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TESTS=(
  temporal_visualize
  two_pc_correct_visualize
  two_pc_buggy_visualize
  three_pc_correct_visualize
  three_pc_buggy_visualize
)

echo "==> Running ${#TESTS[@]} visualization test binaries from $REPO_ROOT"

for t in "${TESTS[@]}"; do
  echo
  echo "---- cargo test -p traceforge --test $t ----"
  cargo test -p traceforge --test "$t" "$@" -- --nocapture
done

echo
echo "==> Building viz_out/index.html"
python3 "$SCRIPT_DIR/build_html.py"

echo
echo "==> Done. Open viz_out/index.html in a browser."
