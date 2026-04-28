#!/usr/bin/env python3
"""Build a single self-contained HTML viewer for the captured TraceForge
execution graphs.

Reads every <viz_out>/<slug>/ subdirectory that contains a `meta.json`
and emits <viz_out>/index.html. The HTML embeds all dot strings inline
and uses viz-standalone.js (loaded from a CDN) to render each one to
SVG in the browser. No system graphviz required.

This script is intentionally protocol-agnostic. It does NOT carry any
hardcoded knowledge of the test scenarios it visualizes. Each visualize
test writes its own `meta.json` describing how its dots should be
rendered, and this script just reads and respects that. To add a new
protocol (3PC, Raft, Paxos, ...): write a Rust visualize test that
dumps its dots to `viz_out/<your_slug>/exec_NNN.dot` and a `meta.json`
of the shape documented in `load_meta`. No changes here.
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Schema documented inline so test authors know what to write. All
# fields are optional with sensible fallbacks; older meta.jsons keep
# rendering.
META_SCHEMA_DOC = """
meta.json fields (all optional):

  kind         "pipeline" | "vote"
                  Selects how the sidebar summarizes each exec.
                  pipeline = S/T pattern from RECV outcomes
                  vote     = Y/N pattern from per-thread NONDET, plus
                             Commit/Abort decision and FAIL pill on
                             assertion violations.

  title        str   Human-readable name shown next to the slug.
  description  str   One-line context line below the title.
  labels       {tid: name}  Friendly thread names ("t1" -> "coordinator").
  order        [tid, ...]   Sidebar / summary thread display order.

  Temporal-config display strip:
  l, u, sd     int   Transit-time bounds + storage delay.
  wait         str   "inf" or recv W_r in ticks.
"""


def load_meta(run_dir):
    """Read run_dir/meta.json. Returns a dict with sensible defaults
    for every renderer-required field. Documented in META_SCHEMA_DOC."""
    defaults = {
        "kind": "pipeline",
        "title": run_dir.name,
        "description": "",
        "labels": {},
        "order": [],
        "l": "?",
        "u": "?",
        "sd": "?",
        "wait": "?",
    }
    p = run_dir / "meta.json"
    if not p.is_file():
        defaults["description"] = "(no meta.json skipped)"
        return defaults
    try:
        loaded = json.loads(p.read_text())
    except json.JSONDecodeError:
        defaults["description"] = "(meta.json malformed)"
        return defaults
    out = dict(defaults)
    out.update(loaded)
    # If the test pre-dates this schema and didn't supply labels/order,
    # discover the threads we see in the dots and fall back to t0/t1/...
    return out


def discover_runs():
    """Find every <viz_out>/<slug>/ directory that has a meta.json.

    Returned in stable lexicographic-by-slug order so the sidebar tabs
    don't shuffle on re-run.
    """
    runs = []
    for sub in sorted(ROOT.iterdir()):
        if not sub.is_dir():
            continue
        if not (sub / "meta.json").is_file():
            continue
        runs.append(sub)
    return runs

# --- Extract a compact per-execution summary -------------------------------

# Match a label line like:  "(t1, 2)" [label=<(t1, 2): RECV() [TIMEOUT]>]
# or, when the recv is timed:   "(t1, 2)" [label=<(t1, 2): RECV W_r=∞() [(t2, 3)]>]
# The optional ` W_r=...` tag is captured but discarded — the summary only
# cares about S/T outcomes.
RECV_RE = re.compile(
    r'"\((t\d+),\s*(\d+)\)"\s*\[label=<\(t\d+,\s*\d+\):\s*RECV(?:\s+W_r=[^()]*)?\(\)\s*\[(TIMEOUT|\(t\d+,\s*\d+\))\]'
)

# For "vote" runs (e.g. 2PC): one NONDET per participant thread.
# Don't anchor on the trailing `>` temporal mode appends `<br/>τ ∈ [..]`.
NONDET_RE = re.compile(
    r'"\((t\d+),\s*\d+\)"\s*\[label=<\(t\d+,\s*\d+\):\s*NONDET\s+(true|false)\b'
)
# Final decision: pick out the broadcast Commit/Abort sent by the coordinator.
DECISION_RE = re.compile(r'val:\s*(Commit|Abort),\s*type_name:\s*"[^"]*ParticipantMsg"')
# A failing execution leaves a `BLK Assert` block node on some thread.
ASSERT_RE = re.compile(r'BLK\s+Assert')
# Other block kinds (BLK Value, BLK Block, ...) mark an execution that
# got stuck while still being partially explored, meaningful only when
# `with_dot_out_blocked(true)` was set on the run that produced these
# files. Without that flag, blocked execs leave empty .dot files
# instead and we still fall through to the legacy "no graph captured"
# placeholder.
BLOCK_RE  = re.compile(r'BLK\s+(?!Assert\b)(?:Value|Join|Block)\b')


def summarize_votes(dot_text: str, labels: dict, order: list) -> dict:
    """Per-participant Y/N pattern + final decision for a 2PC-style run.

    The label and counts emulate the schema produced by `summarize` so
    the JS sidebar code can stay generic: `s_count` becomes the number
    of Yes votes (rendered green), `t_count` the number of No votes
    (rendered orange/red).
    """
    votes = {tid: ("Y" if val == "true" else "N") for tid, val in NONDET_RE.findall(dot_text)}
    parts = []
    yes = 0
    no = 0
    for tid in order:
        if tid not in votes:
            continue
        v = votes[tid]
        label = labels.get(tid, tid)
        parts.append(f"{label}:{v}")
        if v == "Y":
            yes += 1
        else:
            no += 1
    decisions = DECISION_RE.findall(dot_text)
    # The coordinator broadcasts the same decision to every participant,
    # so any one occurrence is the run's decision.
    decision = decisions[0] if decisions else None
    return {
        "label": "  ".join(parts),
        "s_count": yes,
        "t_count": no,
        "decision": decision,
    }


def summarize(dot_text: str, labels: dict, order: list) -> dict:
    """Scan every RECV label and group the outcomes per thread, in
    program-order (event-index order). Each receive is S if it read from
    a real send and T if it took ⊥. Threads are emitted in `order`
    using `labels` for human-friendly names."""
    per_thread = {}  # tid -> [(event_idx, "S"/"T")]
    for tid, idx, body in RECV_RE.findall(dot_text):
        outcome = "T" if body == "TIMEOUT" else "S"
        per_thread.setdefault(tid, []).append((int(idx), outcome))
    rounds = {tid: [o for _, o in sorted(evs)] for tid, evs in per_thread.items()}

    summary_parts = []
    s_count = 0
    t_count = 0
    # Iterate threads in declared order; include any threads observed but
    # not pre-declared at the end so we don't silently drop them.
    seen = set()
    sequence = list(order) + [t for t in sorted(rounds) if t not in order]
    for tid in sequence:
        if tid in seen:
            continue
        seen.add(tid)
        outs = rounds.get(tid, [])
        for o in outs:
            if o == "S":
                s_count += 1
            else:
                t_count += 1
        if outs:
            label = labels.get(tid, tid)
            summary_parts.append(f"{label}:{''.join(outs)}")
    return {
        "label": "  ".join(summary_parts),
        "s_count": s_count,
        "t_count": t_count,
    }


# =====================================================================
# Presentation-layer prettifier
# =====================================================================
#
# TraceForge's dot output uses Rust's `Debug` formatter, which makes node
# labels both verbose ("Val { val: Yes, type_name: \"...::CoordinatorMsg\" }",
# "TSome(Loc(Thread(ThreadId { opaque_id: 2 })))") and hard to follow at a
# glance. We don't want to change the source-of-truth dot output (it's
# stable, replayable, and other tools may depend on its shape), so the
# rewriting happens here, in the viewer, and never touches the .dot files
# on disk.
#
# Rewrites are conservative. They collapse known patterns into shorter
# labels with light HTML coloring/bolding (graphviz HTML labels accept
# <font>, <b>, <i>, <br/>) and recolor rf-edges by the message variant
# they carry. If a pattern doesn't match (e.g. a third-party run with
# a different message shape) the original label survives unchanged.

# Color palette tuned for the dark viewer background but readable on the
# white SVG canvas.
def msg_color(val: str) -> str:
    val = val.strip()
    if val in ("Yes", "Commit"):
        return "#1f8b3a"   # green
    if val == "No":
        return "#c0392b"   # red
    if val == "Abort":
        return "#d68a18"   # orange
    if val.startswith("Prepare"):
        return "#2c7be5"   # blue
    if val.startswith("Init"):
        return "#888888"   # muted: bookkeeping
    return "#444444"


# Used during rf-edge recoloring (BEFORE label prettifying, because we
# need the raw Rust-debug `Val { val: V, ... }` to read off the variant).
SEND_LABEL_RAW_RE = re.compile(
    r'"(\(t\d+,\s*\d+\))"\s*\[label=<\(t\d+,\s*\d+\):\s*SEND\([^)]*Val\s*\{\s*val:\s*'
    r'([^,]+(?:\([^)]*\))?)'
)
# rf-edges in TraceForge's dot output are tagged [color=green]. Po-edges
# (within a thread) carry no color attr.
RF_EDGE_RE = re.compile(
    r'"(\(t\d+,\s*\d+\))"\s*->\s*"(\(t\d+,\s*\d+\))"\[color=green\]'
)


def prettify_dot(dot: str, labels: dict) -> str:
    """Rewrite verbose Rust-debug labels into compact, color-coded text.

    Transformations applied in order:
      ThreadId { opaque_id: N }    -> friendly thread name (from `labels`)
      TSome(Loc(Thread(NAME)))     -> NAME
      TNone                        -> "⊥"
      Val { val: V, type_name: .. }-> V
      SEND(NAME, V)                -> SEND <V> → <NAME>     (color-coded)
      RECV() [(tA, X)]             -> RECV ← <NAME> @ X
      NONDET true / false          -> "vote Yes" / "vote No" (green / red)
      SLEEP(N)                     -> "sleep N"   (dimmed grey)
      TCREATE(tN)                  -> "spawn NAME" (dimmed)
      BLK Assert                   -> bold red, with a ✗ marker
    Recolors rf edges by the message variant they carry.
    """
    oid_name = {}
    for tid, name in labels.items():
        if tid.startswith("t"):
            try:
                oid_name[int(tid[1:])] = name
            except ValueError:
                pass

    def name_for_oid(n: int) -> str:
        return oid_name.get(n, f"t{n}")

    # --- 1. Recolor rf edges by message variant. -----------------------
    event_color = {}
    for m in SEND_LABEL_RAW_RE.finditer(dot):
        event_color[m.group(1)] = msg_color(m.group(2))

    def recolor_rf(m):
        src, dst = m.group(1), m.group(2)
        c = event_color.get(src, "#56d364")  # default fallback
        return f'"{src}" -> "{dst}"[color="{c}",penwidth=2]'
    dot = RF_EDGE_RE.sub(recolor_rf, dot)

    # --- 2. ThreadId { opaque_id: N } -> friendly name. ----------------
    dot = re.sub(
        r"ThreadId\s*\{\s*opaque_id:\s*(\d+)\s*\}",
        lambda m: name_for_oid(int(m.group(1))),
        dot,
    )

    # --- 3. TSome(Loc(Thread(NAME))) -> NAME, TNone -> ⊥. -------------
    dot = re.sub(r"TSome\(Loc\(Thread\(([^()]+)\)\)\)", r"\1", dot)
    dot = dot.replace("TNone", "⊥")

    # --- 4. Val { val: V, type_name: ".." } -> V. ----------------------
    # Non-greedy on V so it stops at the first `, type_name:` that closes
    # the Val struct (works even when V itself contains commas/parens).
    dot = re.sub(
        r'Val\s*\{\s*val:\s*(.*?),\s*type_name:\s*"[^"]*"\s*\}',
        r"\1",
        dot,
        flags=re.DOTALL,
    )

    # --- 5. SEND(NAME, V) -> SEND <V> → <NAME>. ------------------------
    # Greedy on V so we eat any nested parens (e.g. Init([...])); the
    # ")<br/>" lookahead anchors the end at the SEND's own closing paren.
    def repl_send(m):
        target = m.group(1).strip()
        val = m.group(2).strip()
        c = msg_color(val)
        return f'SEND <font color="{c}"><b>{val}</b></font> → <i>{target}</i>'
    dot = re.sub(
        r'SEND\(([^,()]+),\s*(.*?)\)(?=<br/>)',
        repl_send, dot,
    )

    # --- 6. RECV() [(tA, X)] -> RECV ← <NAME> @ X (with optional W_r tag).
    # The dot may carry a ` W_r=N` or ` W_r=∞` annotation right after the
    # word RECV (added by RecvMsg's Display impl when the recv is timed).
    # Color-code it so blocking recvs (W_r=∞) stand out in red and finite
    # waits stay neutral.
    def repl_recv(m):
        wait_raw = (m.group(1) or "").strip()
        try:
            n = int(m.group(2)[1:])
            name = name_for_oid(n)
        except ValueError:
            name = m.group(2)
        wait_html = ""
        if wait_raw:
            # wait_raw looks like "W_r=∞" or "W_r=10".
            color = "#c0392b" if wait_raw.endswith("∞") else "#3366aa"
            wait_html = f' <font color="{color}" point-size="10"><b>{wait_raw}</b></font>'
        return f'RECV{wait_html} ← <i>{name}</i> @ {m.group(3)}'
    dot = re.sub(
        r'RECV(\s+W_r=[^()\s]+)?\(\)\s*\[\((t\d+),\s*(\d+)\)\]',
        repl_recv, dot,
    )

    # Same shape but for the TIMEOUT outcome — keep the W_r tag visible.
    def repl_recv_timeout(m):
        wait_raw = (m.group(1) or "").strip()
        wait_html = ""
        if wait_raw:
            color = "#c0392b" if wait_raw.endswith("∞") else "#3366aa"
            wait_html = f' <font color="{color}" point-size="10"><b>{wait_raw}</b></font>'
        return f'RECV{wait_html} [<font color="#d68a18">TIMEOUT</font>]'
    dot = re.sub(
        r'RECV(\s+W_r=[^()\s]+)?\(\)\s*\[TIMEOUT\]',
        repl_recv_timeout, dot,
    )

    # --- 7. NONDET coloring. -------------------------------------------
    dot = re.sub(
        r'NONDET\s+true\b',
        '<font color="#1f8b3a"><b>vote Yes</b></font>',
        dot,
    )
    dot = re.sub(
        r'NONDET\s+false\b',
        '<font color="#c0392b"><b>vote No</b></font>',
        dot,
    )

    # --- 8. SLEEP dimmed. ----------------------------------------------
    dot = re.sub(
        r'SLEEP\((\d+)\)',
        r'<font color="#888888">sleep \1</font>',
        dot,
    )

    # --- 9. TCREATE(tN) -> spawn NAME (dimmed). ------------------------
    def repl_tcreate(m):
        try:
            n = int(m.group(1)[1:])
            name = name_for_oid(n)
        except ValueError:
            name = m.group(1)
        return f'<font color="#888888">spawn <i>{name}</i></font>'
    dot = re.sub(r'TCREATE\((t\d+)\)', repl_tcreate, dot)

    # --- 10. BLK Assert prominent. -------------------------------------
    dot = re.sub(
        r'BLK\s+Assert',
        '<b><font color="#ff3333">BLK Assert ✗</font></b>',
        dot,
    )

    # --- 11. BLK Value with optional W_r tag. --------------------------
    # The Display impl now emits e.g. `BLK Value W_r=∞(...)` for blocked
    # timed receives. Surface the wait inline so the user can still tell
    # which kind of recv this thread was stuck on after the recv label
    # was overwritten by the Block.
    def repl_blk_value(m):
        wait_raw = (m.group(1) or "").strip()
        if not wait_raw:
            return 'BLK Value'
        color = "#c0392b" if wait_raw.endswith("∞") else "#3366aa"
        return f'BLK Value <font color="{color}" point-size="10"><b>{wait_raw}</b></font>'
    dot = re.sub(r'BLK\s+Value(\s+W_r=[^()\s]+)?', repl_blk_value, dot)

    return dot


def load_run(run_dir: Path, kind: str, labels: dict, order: list):
    items = []
    for f in sorted(run_dir.glob("exec_*.dot")):
        eid = int(f.stem.split("_")[1])
        text_raw = f.read_text()
        # Empty dot files come from blocked executions: TraceForge only
        # writes the dot for blocked execs when `with_verbose(2)` is set,
        # but our visualizer test runs at verbose=1 to keep stdout calm.
        # We still want to surface "this attempt blocked" in the UI.
        blocked = text_raw.strip() == ""
        decision = None
        if blocked:
            summ = {"label": "(blocked execution no graph captured)", "s_count": 0, "t_count": 0}
        elif kind == "vote":
            summ = summarize_votes(text_raw, labels, order)
            decision = summ.get("decision")
        else:
            summ = summarize(text_raw, labels, order)
        # A failing execution leaves a BLK Assert block node on some
        # thread (only meaningful for runs that use traceforge::assert).
        # An execution with any other BLK node was blocked but has its
        # partial graph captured (only when with_dot_out_blocked was set).
        # Detect from the RAW text, `prettify_dot` rewrites BLK Assert
        # into colored HTML, but the substring still appears so the
        # regex matches either way; using raw is just defensive.
        failed       = (not blocked) and bool(ASSERT_RE.search(text_raw))
        blocked_partial = (not blocked) and (not failed) and bool(BLOCK_RE.search(text_raw))
        # Prettify the dot for the renderer; summaries above were
        # computed against the raw debug text, where their regexes are
        # tuned for the Rust `Debug` shape.
        text = prettify_dot(text_raw, labels) if not blocked else text_raw
        items.append(
            {
                "eid": eid,
                "dot": text,
                "label": summ["label"],
                "s": summ["s_count"],
                "t": summ["t_count"],
                # `blocked` (= empty .dot) keeps the legacy
                # "no graph captured" placeholder semantics intact
                # for runs that didn't opt into with_dot_out_blocked.
                "blocked": blocked,
                # New flag: blocked but graph was captured anyway.
                "blocked_partial": blocked_partial,
                "failed": failed,
                "decision": decision,
            }
        )
    return items


def load_prunes(run_dir: Path):
    """Read the JSONL pruning log (one rejection per line). Each record is:
        {eid, kind, recv, send, reason, iv_lo, iv_hi}
    Returns list sorted by (eid, original line order)."""
    p = run_dir / "prunes.jsonl"
    if not p.is_file():
        return []
    out = []
    for n, line in enumerate(p.read_text().splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        rec["seq"] = n
        out.append(rec)
    return out


def main():
    runs = []
    for run_dir in discover_runs():
        meta = load_meta(run_dir)
        kind = meta.get("kind", "pipeline")
        labels = meta.get("labels", {}) or {}
        order = meta.get("order", []) or []
        execs = load_run(run_dir, kind, labels, order)
        prunes = load_prunes(run_dir)
        # Cross-reference: count how many prune rejections fired during
        # each exec, so the sidebar can flag "prune-caused block" when an
        # exec is blocked AND has at least one prune attached. Uses the
        # back-reference `eid` already stored on every prune record.
        prunes_per_eid = {}
        for p in prunes:
            eid = p.get("eid")
            if eid is None:
                continue
            prunes_per_eid[eid] = prunes_per_eid.get(eid, 0) + 1
        for ex in execs:
            ex["prunes_in_exec"] = prunes_per_eid.get(ex["eid"], 0)
        runs.append(
            {
                "slug": run_dir.name,
                "kind": kind,
                "title": meta.get("title", run_dir.name),
                "meta": meta,
                "execs": execs,
                "prunes": prunes,
            }
        )

    payload = json.dumps(runs)

    html = HTML_TEMPLATE.replace("__PAYLOAD__", payload)
    out = ROOT / "index.html"
    out.write_text(html)
    print(f"Wrote {out}")
    for r in runs:
        print(f"  {r['slug']}: {len(r['execs'])} executions, {len(r['prunes'])} pruned rfs")
    if not runs:
        print(f"  (no visualize runs found. see schema:\n{META_SCHEMA_DOC})")


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>TraceForge: temporal pipeline executions</title>
<style>
  :root {
    --bg: #0e1116;
    --panel: #161b22;
    --panel-2: #1f242c;
    --line: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --accent: #79c0ff;
    --green: #56d364;
    --orange: #f0a050;
    --highlight: #1f6feb33;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; height: 100%; }
  body {
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    display: grid;
    grid-template-rows: auto 1fr;
    height: 100vh;
  }
  header {
    background: var(--panel);
    border-bottom: 1px solid var(--line);
    padding: 14px 22px;
    display: flex;
    align-items: center;
    gap: 18px;
    flex-wrap: wrap;
  }
  header h1 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
  }
  header .legend {
    margin-left: auto;
    color: var(--muted);
    font-size: 13px;
    display: flex;
    gap: 14px;
    align-items: center;
  }
  header .legend .pill {
    background: var(--panel-2);
    border: 1px solid var(--line);
    border-radius: 999px;
    padding: 3px 10px;
    font-size: 12px;
  }
  header .legend .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 2px;
    margin-right: 6px;
    vertical-align: middle;
  }
  header .legend .swatch.s { background: var(--green); }
  header .legend .swatch.t { background: var(--orange); }
  /* Run picker. Was a row of buttons; switched to a <select> so the
     header stays compact as scenarios pile up. */
  .tabs {
    background: var(--panel-2);
    color: var(--text);
    border: 1px solid var(--line);
    padding: 6px 30px 6px 12px;
    border-radius: 6px;
    font-size: 13px;
    font-family: inherit;
    cursor: pointer;
    min-width: 280px;
    max-width: 520px;
    appearance: none;
    -webkit-appearance: none;
    /* Inline SVG chevron on the right edge. */
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='none' stroke='%238b949e' stroke-width='1.5' d='M1 1l4 4 4-4'/></svg>");
    background-repeat: no-repeat;
    background-position: right 10px center;
  }
  .tabs:focus { outline: 1px solid var(--accent); outline-offset: 1px; }
  /* Make the open list legible against the dark theme on browsers that
     respect option styling (Chromium does; Firefox is OS-themed). */
  .tabs option { background: var(--panel); color: var(--text); }
  main {
    display: grid;
    grid-template-columns: 320px 1fr;
    min-height: 0;
  }
  aside {
    background: var(--panel);
    border-right: 1px solid var(--line);
    overflow-y: auto;
  }
  aside .item {
    padding: 9px 16px;
    border-bottom: 1px solid #20262e;
    cursor: pointer;
    font-size: 13px;
    line-height: 1.35;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  aside .item:hover { background: #20262e; }
  aside .item.active {
    background: var(--highlight);
    border-left: 3px solid var(--accent);
    padding-left: 13px;
  }
  aside .item .head {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
  }
  aside .item .eid {
    font-weight: 600;
    color: var(--accent);
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  aside .item .counts {
    color: var(--muted);
    font-size: 11px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  aside .item .pattern {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11.5px;
    color: var(--text);
  }
  aside .item .pattern .S { color: var(--green); }
  aside .item .pattern .T { color: var(--orange); }
  aside .item .pattern .Y { color: var(--green); }
  aside .item .pattern .N { color: #ff6b6b; }
  aside .item.failed-exec {
    border-left: 3px solid #cc3333;
    padding-left: 13px;
  }
  aside .item.failed-exec .eid { color: #ff6b6b; }
  aside .item .fail-pill {
    display: inline-block;
    background: #c0392b;
    color: #fff;
    border-radius: 3px;
    padding: 1px 6px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: .4px;
    margin-left: 6px;
  }
  aside .item .pass-pill {
    display: inline-block;
    background: #1f3b1f;
    color: var(--green);
    border-radius: 3px;
    padding: 1px 6px;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: .4px;
    margin-left: 6px;
  }
  aside .item .block-pill {
    display: inline-block;
    background: #3a2a08;
    color: var(--orange);
    border-radius: 3px;
    padding: 1px 6px;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: .4px;
    margin-left: 6px;
  }
  /* "prune-caused block": block whose stuck recv had at least one
     temporal prune fire during the same exec. Uses the same orange
     family as block-pill but with a sharper border so the user can
     tell the variant at a glance. */
  aside .item .prune-block-pill {
    display: inline-block;
    background: #2a1a08;
    color: #f0a050;
    border: 1px solid #f0a050;
    border-radius: 3px;
    padding: 0 6px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: .3px;
    margin-left: 6px;
  }
  aside .item.blocked-partial .eid { color: var(--orange); }
  aside .item.blocked-partial {
    border-left: 3px solid var(--orange);
    padding-left: 13px;
  }
  aside .item .decision {
    color: var(--muted);
    font-size: 11px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    margin-top: 1px;
  }
  aside .item .decision.commit { color: var(--green); }
  aside .item .decision.abort  { color: var(--orange); }
  section.viewer {
    display: grid;
    grid-template-rows: auto 1fr auto;
    min-height: 0;
  }
  .viewer-header {
    padding: 12px 22px;
    border-bottom: 1px solid var(--line);
    display: flex;
    gap: 16px;
    align-items: center;
    background: var(--panel);
  }
  .viewer-header h2 {
    margin: 0;
    font-size: 15px;
    font-weight: 600;
  }
  .viewer-header .meta {
    color: var(--muted);
    font-size: 12.5px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  .viewer-header .meta .S { color: var(--green); }
  .viewer-header .meta .T { color: var(--orange); }
  .nav-buttons { margin-left: auto; display: flex; gap: 6px; }
  .nav-buttons button {
    background: var(--panel-2);
    color: var(--text);
    border: 1px solid var(--line);
    padding: 4px 12px;
    cursor: pointer;
    border-radius: 4px;
    font-size: 13px;
  }
  .nav-buttons button:hover { background: #2a313a; }
  .nav-buttons button:disabled { opacity: .35; cursor: default; }
  .graph-area {
    /* svg-pan-zoom handles scrolling internally via its own viewport
       transform, so no overflow:auto here. The SVG fills the area and
       the user pans/zooms inside it. */
    position: relative;
    background: #0a0d12;
    padding: 12px;
  }
  .graph-area svg {
    width: 100%;
    height: 100%;
    background: #fff;
    border-radius: 6px;
    cursor: grab;
  }
  .graph-area svg:active { cursor: grabbing; }
  .graph-area .zoom-hint {
    position: absolute;
    right: 18px;
    bottom: 14px;
    background: rgba(14, 17, 22, 0.85);
    color: var(--muted);
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 11px;
    pointer-events: none;
    user-select: none;
  }
  .graph-area .zoom-hint kbd {
    background: var(--panel-2);
    border: 1px solid var(--line);
    border-bottom-width: 2px;
    padding: 0 4px;
    border-radius: 3px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 10px;
    color: var(--text);
  }
  .viewer-footer {
    padding: 8px 22px;
    background: var(--panel);
    border-top: 1px solid var(--line);
    display: flex;
    justify-content: space-between;
    align-items: center;
    color: var(--muted);
    font-size: 12px;
  }
  .viewer-footer .hint kbd {
    background: var(--panel-2);
    border: 1px solid var(--line);
    border-bottom-width: 2px;
    padding: 1px 6px;
    border-radius: 3px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
  }
  .progress {
    flex: 0 0 220px;
    height: 4px;
    background: var(--panel-2);
    border-radius: 2px;
    overflow: hidden;
    margin: 0 14px;
  }
  .progress > div {
    height: 100%;
    background: var(--accent);
    width: 0%;
    transition: width 120ms ease;
  }
  .loading { color: var(--muted); font-size: 14px; padding: 24px; }
  aside .item.blocked-exec .eid { color: var(--orange); }
  aside .item.blocked-exec .counts { color: var(--orange); }
  .cfg-pill {
    display: inline-block;
    background: var(--panel-2);
    border: 1px solid var(--line);
    border-radius: 999px;
    padding: 1px 10px;
    margin-left: 8px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11.5px;
    color: var(--accent);
    font-weight: 500;
  }
  .mode-tabs { display: flex; gap: 4px; margin-right: 8px; }
  .mode-tabs button {
    background: var(--panel-2);
    color: var(--text);
    border: 1px solid var(--line);
    padding: 6px 12px;
    cursor: pointer;
    border-radius: 6px;
    font-size: 13px;
  }
  .mode-tabs button.active {
    background: #f0a050;
    color: #1a0e02;
    border-color: #f0a050;
    font-weight: 600;
  }
  .mode-tabs button.exec.active { background: var(--accent); color: #0a1018; border-color: var(--accent); }
  aside .item.prune {
    padding: 9px 16px;
    border-left: 3px solid transparent;
  }
  aside .item.prune.active { border-left-color: #f0a050; background: #2a1a08; padding-left: 13px; }
  aside .item.prune .head .eid { color: #f0a050; }
  aside .item.prune .reason {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
    color: var(--muted);
    margin-top: 2px;
  }
  aside .item.prune .reason .ev { color: var(--text); }
  aside .item.prune .reason .empty { color: #cc3333; }
  .legend.pruning .pill { background: #2a1a08; border-color: #f0a050; }
</style>
</head>
<body>
<header>
  <h1>TraceForge execution viewer</h1>
  <div class="mode-tabs" id="mode-tabs">
    <button id="mode-execs" class="exec active">Executions</button>
    <button id="mode-prunes">Pruned</button>
  </div>
  <select class="tabs" id="tabs" aria-label="Run picker"></select>
  <div class="legend" id="legend">
    <span class="pill"><span class="swatch s"></span>S = Some(send)</span>
    <span class="pill"><span class="swatch t"></span>T = Timeout (⊥)</span>
    <span class="pill">order: a · b · c · main</span>
  </div>
</header>
<main>
  <aside id="sidebar"></aside>
  <section class="viewer">
    <div class="viewer-header">
      <h2 id="viewer-title">-</h2>
      <div class="meta" id="viewer-meta"></div>
      <div class="progress"><div id="progress-bar"></div></div>
      <div class="nav-buttons">
        <button id="prev-btn">← Prev</button>
        <button id="next-btn">Next →</button>
      </div>
    </div>
    <div class="graph-area" id="graph-area">
      <div class="loading">Loading viz.js…</div>
    </div>
    <div class="viewer-footer">
      <div class="hint">
        <kbd>←</kbd> <kbd>→</kbd> step ·
        <kbd>1</kbd>–<kbd>5</kbd> switch run ·
        <kbd>e</kbd> execs ·
        <kbd>p</kbd> prunes ·
        <kbd>+</kbd> <kbd>-</kbd> zoom ·
        <kbd>0</kbd> reset ·
        <kbd>Home</kbd>/<kbd>End</kbd> first/last
      </div>
      <div id="viewer-pos"></div>
    </div>
  </section>
</main>

<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.7.0/lib/viz-standalone.js"></script>
<script src="https://cdn.jsdelivr.net/npm/svg-pan-zoom@3.6.1/dist/svg-pan-zoom.min.js"></script>
<script>
const RUNS = __PAYLOAD__;

let viz = null;
let mode = "execs";          // "execs" | "prunes"
let activeRun = 0;
let activeIdx = 0;
let panZoomInst = null;      // current svg-pan-zoom instance, destroyed on swap

const tabsEl = document.getElementById("tabs");
const sidebarEl = document.getElementById("sidebar");
const titleEl = document.getElementById("viewer-title");
const metaEl = document.getElementById("viewer-meta");
const graphEl = document.getElementById("graph-area");
const prevBtn = document.getElementById("prev-btn");
const nextBtn = document.getElementById("next-btn");
const posEl = document.getElementById("viewer-pos");
const progressEl = document.getElementById("progress-bar");
const modeExecBtn = document.getElementById("mode-execs");
const modePrunesBtn = document.getElementById("mode-prunes");
const legendEl = document.getElementById("legend");

function currentList() {
  const run = RUNS[activeRun];
  return mode === "execs" ? run.execs : run.prunes;
}

function colorPattern(label) {
  // S/T for pipeline runs, Y/N for vote runs. Both are single-char
  // tokens, so a single regex covers both kinds.
  return label.replace(/[STYN]/g, c => `<span class="${c}">${c}</span>`);
}

function updateLegend() {
  legendEl.classList.toggle("pruning", mode === "prunes");
  if (mode === "prunes") {
    legendEl.innerHTML = `
      <span class="pill"><span class="swatch" style="background:#f0a050"></span>rejected rf</span>
      <span class="pill"><span class="swatch" style="background:#56d364"></span>kept equivalent (symmetric)</span>
      <span class="pill">reason: <code>temporal</code> (empty τ) or <code>symmetric</code> (POR dup)</span>
      <span class="pill">eid = exec being explored when rejection fired</span>
    `;
  } else if (RUNS[activeRun] && RUNS[activeRun].kind === "vote") {
    legendEl.innerHTML = `
      <span class="pill"><span class="swatch" style="background:#56d364"></span>Y = vote Yes</span>
      <span class="pill"><span class="swatch" style="background:#ff6b6b"></span>N = vote No</span>
      <span class="pill"><span style="color:#fff;background:#c0392b;border-radius:3px;padding:0 6px;font-size:11px;font-weight:700;">FAIL</span> assertion violation in graph</span>
      <span class="pill"><span style="color:#1a0e02;background:#f0a050;border-radius:3px;padding:0 6px;font-size:11px;font-weight:700;">BLOCKED</span> partial graph (BLK node)</span>
    `;
  } else {
    legendEl.innerHTML = `
      <span class="pill"><span class="swatch s"></span>S = Some(send)</span>
      <span class="pill"><span class="swatch t"></span>T = Timeout (⊥)</span>
      <span class="pill">order: a · b · c · main</span>
    `;
  }
}

function setMode(m) {
  mode = m;
  activeIdx = 0;
  modeExecBtn.classList.toggle("active", m === "execs");
  modePrunesBtn.classList.toggle("active", m === "prunes");
  renderAll();
}

function renderTabs() {
  // Refresh the option list. Each option's text shows the run title plus
  // the count for the current mode (execs vs. prunes), so the dropdown
  // stays informative when the user toggles between modes.
  tabsEl.innerHTML = "";
  RUNS.forEach((r, i) => {
    const opt = document.createElement("option");
    const count = mode === "execs" ? r.execs.length : r.prunes.length;
    const title = r.title || r.slug;
    opt.textContent = `${title}  (${count})`;
    opt.value = String(i);
    if (i === activeRun) opt.selected = true;
    tabsEl.appendChild(opt);
  });
}

tabsEl.addEventListener("change", () => {
  activeRun = Number(tabsEl.value);
  activeIdx = 0;
  renderAll();
});

function renderSidebar() {
  sidebarEl.innerHTML = "";
  const run = RUNS[activeRun];

  if (mode === "execs") {
    run.execs.forEach((ex, i) => {
      const div = document.createElement("div");
      const klass = "item"
        + (i === activeIdx ? " active" : "")
        + (ex.blocked ? " blocked-exec" : "")
        + (ex.blocked_partial ? " blocked-partial" : "")
        + (ex.failed  ? " failed-exec"  : "");
      div.className = klass;
      // For vote runs (decision != null) the s/t counts are Yes/No;
      // for pipeline runs they are Some(send)/Timeout. Same shape, but
      // the legend in the header explains which is which.
      const isVote = ex.decision !== null && ex.decision !== undefined;
      let counts;
      if (ex.blocked) {
        counts = "blocked";
      } else if (isVote) {
        counts = `Y=${ex.s} N=${ex.t}`;
      } else {
        counts = `S=${ex.s} T=${ex.t}`;
      }
      const pattern = ex.blocked
        ? `<i style="color:var(--orange)">no graph captured</i>`
        : (ex.label.length ? colorPattern(ex.label) : `<i style="color:var(--muted)">partial graph</i>`);
      let pill;
      if (ex.blocked) {
        pill = "";
      } else if (ex.failed) {
        pill = `<span class="fail-pill">FAIL</span>`;
      } else if (ex.blocked_partial) {
        // Differentiate "block whose recv was emptied by the temporal
        // filter during this same exec" from a generic block (e.g. a
        // deadlock that has nothing to do with pruning). Uses the
        // back-reference count attached during build.
        const pCount = ex.prunes_in_exec || 0;
        if (pCount > 0) {
          pill = `<span class="prune-block-pill" title="${pCount} temporal prune(s) fired during this exec">prune-caused block</span>`;
        } else {
          pill = `<span class="block-pill">blocked</span>`;
        }
      } else if (isVote) {
        pill = `<span class="pass-pill">pass</span>`;
      } else {
        pill = "";
      }
      const decisionHtml = (isVote && ex.decision)
        ? `<div class="decision ${ex.decision.toLowerCase()}">decision: ${ex.decision}</div>`
        : "";
      div.innerHTML = `
        <div class="head">
          <span class="eid">#${String(ex.eid).padStart(3, "0")}${pill}</span>
          <span class="counts">${counts}</span>
        </div>
        <div class="pattern">${pattern}</div>
        ${decisionHtml}
      `;
      div.onclick = () => { activeIdx = i; renderAll(); };
      sidebarEl.appendChild(div);
    });
  } else {
    if (run.prunes.length === 0) {
      const empty = document.createElement("div");
      empty.style.padding = "16px";
      empty.style.color = "var(--muted)";
      empty.textContent = "No prunings recorded for this run.";
      sidebarEl.appendChild(empty);
    }
    run.prunes.forEach((p, i) => {
      const div = document.createElement("div");
      div.className = "item prune" + (i === activeIdx ? " active" : "");
      const detail = (p.reason === "symmetric")
        ? `equiv to <span class="ev">${p.equivalent}</span>`
        : `τ ∈ <span class="empty">[${p.iv_lo}, ${p.iv_hi}]</span>`;
      div.innerHTML = `
        <div class="head">
          <span class="eid">#${i + 1}  →  exec #${String(p.eid).padStart(3, "0")}</span>
          <span class="counts">${p.reason}/${p.kind}</span>
        </div>
        <div class="reason">
          <span class="ev">${p.recv}</span> ↤̸ <span class="ev">${p.send}</span>
          &nbsp;·&nbsp;
          ${detail}
        </div>
      `;
      div.onclick = () => { activeIdx = i; renderAll(); };
      sidebarEl.appendChild(div);
    });
  }
  const active = sidebarEl.querySelector(".item.active");
  if (active) active.scrollIntoView({ block: "nearest" });
}

// Build a hypothetical graph dot showing the rejected rf as a red
// dashed edge, so the user sees what would-have-been. Symmetric prunes
// also add a green-dotted arrow to the equivalent surviving rf.
function dotWithPrunedRf(baseDot, p) {
  const closeIdx = baseDot.lastIndexOf("}");
  if (closeIdx < 0) return baseDot;
  let label;
  if (p.reason === "symmetric") {
    label = `symmetric (equiv. to ${p.equivalent})`;
  } else {
    label = `τ ∈ [${p.iv_lo}, ${p.iv_hi}] (empty)`;
  }
  let extra = `\n  "${p.send}" -> "${p.recv}" [color="#f0a050", style=dashed, penwidth=2, label=<<font point-size="10" color="#cc3333">${label}</font>>]\n`;
  if (p.reason === "symmetric" && p.equivalent) {
    extra += `  "${p.equivalent}" -> "${p.recv}" [color="#56d364", style=dotted, penwidth=2, label=<<font point-size="10" color="#56d364">surviving symmetric rf</font>>]\n`;
  }
  return baseDot.slice(0, closeIdx) + extra + baseDot.slice(closeIdx);
}

async function renderViewer() {
  const run = RUNS[activeRun];
  const list = currentList();
  if (list.length === 0) {
    titleEl.textContent = mode === "prunes"
      ? `No prunings: ${run.slug}`
      : `No executions: ${run.slug}`;
    metaEl.innerHTML = "";
    graphEl.innerHTML = `<div class="loading">Nothing to show.</div>`;
    posEl.textContent = "0 / 0";
    progressEl.style.width = "0%";
    prevBtn.disabled = true;
    nextBtn.disabled = true;
    return;
  }

  const m = run.meta || {};
  const cfgPill = `<span class="cfg-pill">l=${m.l}, u=${m.u}, sd=${m.sd}, wait=${m.wait}</span>`;

  const item = list[activeIdx];
  if (mode === "execs") {
    const ex = item;
    let statusTag = "";
    if (ex.failed) {
      statusTag = ` <span style="color:#fff;background:#c0392b;border-radius:3px;padding:1px 8px;font-size:12px;font-weight:700;letter-spacing:.4px;margin-left:6px;">FAIL</span>`;
    } else if (ex.blocked_partial) {
      const pCount = ex.prunes_in_exec || 0;
      const label = pCount > 0 ? "PRUNE-CAUSED BLOCK" : "BLOCKED";
      statusTag = ` <span style="color:#1a0e02;background:#f0a050;border-radius:3px;padding:1px 8px;font-size:12px;font-weight:700;letter-spacing:.4px;margin-left:6px;">${label}</span>`;
    }
    titleEl.innerHTML = `Execution #${String(ex.eid).padStart(3, "0")} of ${run.execs.length} &nbsp; &nbsp; ${run.slug} ${cfgPill}${statusTag}`;
    if (ex.blocked) {
      metaEl.innerHTML = `<span style="color:var(--orange)">blocked execution: no graph captured (TraceForge only writes dot files for blocked execs at verbose ≥ 2, or when with_dot_out_blocked(true) is set)</span>`;
    } else if (ex.blocked_partial) {
      const pCount = ex.prunes_in_exec || 0;
      const extra = pCount > 0
        ? ` &nbsp;·&nbsp; <b>${pCount}</b> temporal prune(s) fired during this exec — switch to the <b>Pruned</b> tab to see the rejected rf(s).`
        : "";
      metaEl.innerHTML = `<span style="color:var(--orange)">partial graph: this execution attempt got stuck before all threads finished. Look for the BLK node in the graph below to see exactly where.${extra}</span>`;
    } else if (run.kind === "vote") {
      const decisionPill = ex.decision
        ? ` &nbsp;·&nbsp; decision: <b style="color:${ex.decision === 'Commit' ? 'var(--green)' : 'var(--orange)'}">${ex.decision}</b>`
        : "";
      const reason = ex.failed
        ? ` &nbsp;·&nbsp; <span style="color:#ff6b6b">a participant voted N but received Commit</span>`
        : "";
      metaEl.innerHTML = colorPattern(ex.label) + `  ·  Y=${ex.s} N=${ex.t}` + decisionPill + reason;
    } else {
      metaEl.innerHTML = colorPattern(ex.label) + `  ·  S=${ex.s} T=${ex.t}`;
    }
  } else {
    const p = item;
    titleEl.innerHTML = `Pruning #${activeIdx + 1} of ${run.prunes.length} &nbsp; &nbsp; ${run.slug} ${cfgPill}`;
    const tail = (p.reason === "symmetric")
      ? `kept equivalent rf <span class="ev">${p.equivalent}</span>`
      : `empty interval τ ∈ <span style="color:#cc3333">[${p.iv_lo}, ${p.iv_hi}]</span>`;
    metaEl.innerHTML = `
      <span class="ev">${p.recv}</span> would have read from
      <span class="ev">${p.send}</span> &nbsp;·&nbsp;
      reason = <b>${p.reason}</b>/<b>${p.kind}</b> &nbsp;·&nbsp;
      ${tail} &nbsp;·&nbsp;
      detected while exploring exec #${String(p.eid).padStart(3, "0")}
    `;
  }

  posEl.textContent = `${activeIdx + 1} / ${list.length}`;
  progressEl.style.width = ((activeIdx + 1) / list.length * 100) + "%";
  prevBtn.disabled = activeIdx === 0;
  nextBtn.disabled = activeIdx === list.length - 1;

  if (!viz) {
    graphEl.innerHTML = `<div class="loading">Loading viz.js…</div>`;
    return;
  }
  try {
    let dotText;
    if (mode === "execs") {
      if (item.blocked || !item.dot.trim()) {
        mountSvg(null);
        graphEl.innerHTML = `
          <div class="loading" style="text-align:center; color:var(--orange);">
            <div style="font-size:48px; line-height:1;">⊘</div>
            <div style="margin-top:12px;">This execution attempt was blocked.</div>
            <div style="margin-top:6px; color:var(--muted); font-size:12px;">
              The runtime emitted a Block event for an unmatched receive,<br/>
              and TraceForge only writes a dot file for blocked execs at verbose ≥ 2.<br/>
              Switch to the <b>Pruned</b> tab to see why this attempt was rejected.
            </div>
          </div>`;
        return;
      }
      dotText = item.dot;
    } else {
      const baseExec = run.execs.find(e => e.eid === item.eid && !e.blocked);
      if (!baseExec) {
        // The corresponding execution didn't capture a graph (it was
        // blocked). Render the prune as a tiny standalone diagram.
        const fallback = `digraph {\n  rankdir=LR; node [shape=plaintext];\n  "${item.send}" -> "${item.recv}" [color="#f0a050", style=dashed, penwidth=2, label=<<font point-size="10" color="#cc3333">τ ∈ [${item.iv_lo ?? "?"}, ${item.iv_hi ?? "?"}] (empty)</font>>];\n}`;
        const svg = await viz.renderSVGElement(fallback);
        mountSvg(svg);
        return;
      }
      dotText = dotWithPrunedRf(baseExec.dot, item);
    }
    const svg = await viz.renderSVGElement(dotText);
    mountSvg(svg);
  } catch (e) {
    mountSvg(null);
    graphEl.innerHTML = `<div class="loading">Render error: ${String(e)}</div>`;
  }
}

// Replace the SVG inside the graph area, wiring up pan/zoom on the new
// element and tearing down the previous instance so we don't leak its
// global event listeners (svg-pan-zoom installs window resize hooks).
function mountSvg(svg) {
  if (panZoomInst) {
    try { panZoomInst.destroy(); } catch (_) {}
    panZoomInst = null;
  }
  graphEl.innerHTML = "";
  if (!svg) return;
  // Make sure the root SVG fills the container; svg-pan-zoom uses its
  // intrinsic size to compute the initial fit.
  svg.setAttribute("width",  "100%");
  svg.setAttribute("height", "100%");
  graphEl.appendChild(svg);
  const hint = document.createElement("div");
  hint.className = "zoom-hint";
  hint.innerHTML = `scroll to zoom · drag to pan · <kbd>+</kbd> <kbd>-</kbd> zoom · <kbd>0</kbd> reset`;
  graphEl.appendChild(hint);
  // Defer a tick so layout is done; svg-pan-zoom needs real bbox sizes.
  requestAnimationFrame(() => {
    panZoomInst = svgPanZoom(svg, {
      panEnabled: true,
      zoomEnabled: true,
      controlIconsEnabled: true,
      fit: true,
      center: true,
      contain: false,
      minZoom: 0.1,
      maxZoom: 50,
      zoomScaleSensitivity: 0.35,
      dblClickZoomEnabled: false,
    });
  });
}

function renderAll() {
  renderTabs();
  updateLegend();
  renderSidebar();
  renderViewer();
}

modeExecBtn.onclick = () => setMode("execs");
modePrunesBtn.onclick = () => setMode("prunes");

prevBtn.onclick = () => { if (activeIdx > 0) { activeIdx--; renderAll(); } };
nextBtn.onclick = () => {
  if (activeIdx < currentList().length - 1) { activeIdx++; renderAll(); }
};

document.addEventListener("keydown", e => {
  // Don't hijack typing in inputs / textareas (defensive: none today,
  // but cheap).
  if (e.target.matches("input, textarea")) return;
  const list = currentList();
  if (e.key === "ArrowLeft") { if (activeIdx > 0) { activeIdx--; renderAll(); } }
  else if (e.key === "ArrowRight") { if (activeIdx < list.length - 1) { activeIdx++; renderAll(); } }
  else if (e.key === "Home") { activeIdx = 0; renderAll(); }
  else if (e.key === "End") { activeIdx = list.length - 1; renderAll(); }
  else if (/^[1-9]$/.test(e.key) && !(e.key === "0")) {
    const idx = Number(e.key) - 1;
    if (RUNS[idx]) { activeRun = idx; activeIdx = 0; renderAll(); }
  }
  else if (e.key === "e") setMode("execs");
  else if (e.key === "p") setMode("prunes");
  // --- zoom shortcuts ---
  else if ((e.key === "+" || e.key === "=") && panZoomInst) { panZoomInst.zoomIn(); e.preventDefault(); }
  else if (e.key === "-" && panZoomInst)                    { panZoomInst.zoomOut(); e.preventDefault(); }
  else if (e.key === "0" && panZoomInst)                    { panZoomInst.resetZoom(); panZoomInst.center(); panZoomInst.fit(); e.preventDefault(); }
});

(async function init() {
  renderAll();
  viz = await Viz.instance();
  renderViewer();
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
