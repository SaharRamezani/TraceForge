#!/bin/bash
# Clone nisshi at the pinned commit into .nisshi-src/ (gitignored), so the
# nisshi-tf harness can run their real code. Re-run to restore after a clean.
set -eu
REPO=https://github.com/nisshi-io/nisshi.git
PIN=b3d0cea
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/.nisshi-src/nisshi"
mkdir -p "$ROOT/.nisshi-src"
[ -d "$DEST/.git" ] || git clone --quiet "$REPO" "$DEST"
git -C "$DEST" fetch --quiet origin
git -C "$DEST" checkout --quiet "$PIN"
# test-only visibility change so the harness can inject an object store
git -C "$DEST" apply "$ROOT/tools/nisshi-visibility.patch" 2>/dev/null || true
git -C "$DEST" log --oneline -1
