#!/usr/bin/env python3
"""Build a visually-pleasing results.xlsx: title 'Results' + one curated table of the
non-trivial runs (inbox first). Baseline figures are blue, timed figures are red."""
import sys
import openpyxl
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter

OUT = sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results.xlsx"

# ---- palette ----
NAVY   = "1F3864"; TEAL = "2E6E6E"; HEADER = "305496"
BLUEF  = "0000CC"; BLUEFILL = "DEE7FB"      # baseline
REDF   = "C00000"; REDFILL  = "FBE0E0"      # timed
ALT    = "F4F6F9"; WHITE = "FFFFFF"; GREY = "808080"
thin = Side(style="thin", color="BFBFBF")
BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)

# ---- columns ----
COLS = [
    ("File", 30), ("Nodes", 6), ("L", 4), ("U", 4), ("sd", 5), ("W", 6),
    ("Ballots / Rounds", 9), ("Reasoning for the chosen parameters", 46),
    ("Fit for a regime?", 30),
    ("#execs\nbaseline", 13), ("#execs\ntimed", 13), ("Pruning x\n(timed)", 10),
    ("Elections / Commits\nbaseline", 13), ("Elections / Commits\ntimed", 13),
    ("Wall time", 12), ("Bug / failure", 40),
]
# indices (0-based) of baseline vs timed columns
BASE_COLS = {9, 12}; TIMED_COLS = {10, 13}

# ---- curated rows (measured; inbox first) ----
# each: file,N,L,U,sd,W,BR, reason, regime, base_ex, tim_ex, prune, base_ec, tim_ec, wall, bug
R = [
# ---------- inbox leader election ----------
["comm_closed_leader_election_inbox",5,18,20,0,40,"1 ballot",
 "Tightest transit and timeout in the grid (L/U=0.9, W/U=2). Pruning is strongest here, so this gave the best chance of completing exhaustive verification at five nodes.",
 "No. Tight stress corner, not a regime. Kept as the scalability result.",
 "did not finish (9.3 h)",97586928,"n/a","n/a",6925445,"48 min",
 "None. Safety holds. Only 7% of executions elect a leader; 93% end with no leader."],

["comm_closed_leader_election_inbox",4,18,20,0,40,"2 ballots",
 "Same tight corner but with two ballots. Two ballots enlarge the state space by several thousand times, so completion was not expected within any practical time.",
 "No. Kept only to document the limit of exhaustive verification.",
 "intractable","-","n/a","n/a","-","31.6 h",
 "Did not finish after about 31.6 hours. Result unknown."],

["comm_closed_leader_election_inbox",3,18,20,0,40,"1 ballot",
 "Tight transit (L/U=0.9) is the only point where the inbox model prunes for this protocol. Looser cells give about 1.0x.",
 "No. Tight edge, not a regime. Kept as the pruning and election finding.",
 5271,1746,3.02,2106,183,"0.7 s",
 "None. Safety holds. Tight transit cuts successful elections."],

# ---------- plain comm-closed leader election ----------
["comm_closed_leader_election",3,18,20,0,40,"1 ballot",
 "Same tight corner as the inbox variant, run to compare the two encodings on identical parameters.",
 "No. Tight edge. Kept to contrast with the inbox encoding.",
 10232,5786,1.77,5064,2106,"0.8 s",
 "None. Safety holds. The plain encoding prunes less than the inbox one (1.77x vs 3.02x)."],

["comm_closed_leader_election",5,18,20,0,100,"1 ballot",
 "Attempted at five nodes but only with a 600 s cap, unlike the inbox N=5 run which had 85 min. It timed out, so its tractability with more time is untested.",
 "No. Undertested (short cap), not a limit result.",
 "not tested","-","n/a","n/a","-","600 s cap",
 "Did not finish under a 600 s cap. This is not a fair comparison with inbox N=5, which had 85 min and finished in 48. A longer cap is needed to judge whether it completes."],

["comm_closed_leader_election",3,18,20,0,100,"2 ballots",
 "Two ballots at tight transit with no storage delay. Baseline for the storage-delay test below.",
 "No. Kept as the baseline of the storage-delay finding.",
 "not run",11272450,"n/a","n/a",6861468,"3.1 min",
 "None. Safety holds."],

["comm_closed_leader_election",3,18,20,10,100,"2 ballots",
 "Same as the row above but with storage delay sd/U=0.5. With two ballots a message can survive across a ballot boundary, so storage delay becomes active.",
 "No. Kept as the storage-delay finding (sd matters only from two ballots).",
 "not run",21854668,"n/a","n/a",15448092,"5.7 min",
 "None. Storage delay nearly doubles the state space (x1.94) and the elections (x2.25) versus sd=0."],

# ---------- 3PC (atomic commit) ----------
["three_pc_timed",3,0,20,0,200,"1 round",
 "Loose transit with a generous timeout, which matches a fast intra-datacentre network. U is fixed at 20 ticks to realise the dimensionless grid.",
 "Yes. Regimes A, B, C, D and E all map here: they give identical numbers, so one row covers them.",
 2026,113,17.93,288,0,"0.2 s",
 "No safety bug. But no timed execution reaches a commit (commit=0); this needs review, most likely an ack-phase timing artifact."],

["three_pc_timed",3,0,20,10,40,"1 round",
 "Tight timeout (W/U=2) with high storage delay, the adversarial or congested profile. This is the only regime cell that differs from A to E.",
 "Yes. Regime F (adversarial / congested).",
 2026,177,11.45,288,0,"0.2 s",
 "No safety bug. Same commit=0 anomaly as the other 3PC cells; less pruning because the timeout is tight."],

["three_pc_timed",4,18,20,0,40,"1 round",
 "Tight corner at four participants, chosen to show that the pruning ratio grows with the participant count.",
 "No. Tight edge. Kept as the scaling result.",
 49816,646,77.12,9216,0,"5.0 s",
 "No safety bug. Pruning rises from about 15-20x at N=3 to 77x at N=4. The commit=0 anomaly persists."],

# ---------- 3PC buggy (safety-bug witness) ----------
["three_pc_timed_buggy",3,18,20,0,40,"1 round",
 "Standard tight corner. This variant commits on a majority of yes votes instead of unanimity, so the run is meant to expose the safety bug.",
 "No. Safety-bug witness.",
 "-","-","n/a","n/a","n/a","0.02 s",
 "Bug found. A participant that voted No can receive Commit; the safety assertion fires in both the timed and the untimed run."],

# ---------- raft leader election (native axes) ----------
["raft_leader_election",3,0,1,4,"n/a","2 rounds",
 "Native parameters (U=1, storage delay = (N-1) times the stagger). Raft has no receive timeout, so the W/U grid does not apply.",
 "No. Native timing model; the W/U grid does not apply.",
 90,44,2.05,"n/a",44,"0.09 s",
 "None. Election safety holds. 44 executions elect a leader; 214 reach no leader (split vote)."],

["raft_leader_election",5,0,1,8,"n/a","1 round",
 "Same native setup at five nodes. The baseline enumeration is too large to complete.",
 "No. Limit result.",
 "intractable","-","n/a","n/a","-","600 s",
 "Did not finish (timeout at 600 s)."],
]

wb = openpyxl.Workbook(); ws = wb.active; ws.title = "Results"
ncols = len(COLS)

# title
ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=ncols)
t = ws.cell(1, 1, "Results")
t.font = Font(bold=True, size=20, color=WHITE); t.alignment = Alignment("center", "center")
t.fill = PatternFill("solid", fgColor=NAVY); ws.row_dimensions[1].height = 34

# header
for j,(name,w) in enumerate(COLS, start=1):
    c = ws.cell(2, j, name)
    c.font = Font(bold=True, color=WHITE, size=10); c.fill = PatternFill("solid", fgColor=HEADER)
    c.alignment = Alignment("center","center", wrap_text=True); c.border = BORDER
    ws.column_dimensions[get_column_letter(j)].width = w
ws.row_dimensions[2].height = 42

# data
for i,row in enumerate(R):
    r = i + 3
    alt = PatternFill("solid", fgColor=ALT) if i % 2 else None
    for j,val in enumerate(row):
        c = ws.cell(r, j+1, val); c.border = BORDER
        num = isinstance(val, (int, float))
        wrap = j in (0,7,8,15)
        c.alignment = Alignment(horizontal="right" if num else ("left" if wrap else "center"),
                                vertical="top", wrap_text=wrap)
        if num and j in (9,10,12,13): c.number_format = "#,##0"
        if j == 11 and num: c.number_format = '0.00"x"'
        # colour baseline (blue) vs timed (red)
        if j in BASE_COLS:
            c.font = Font(color=BLUEF, bold=True, size=10); c.fill = PatternFill("solid", fgColor=BLUEFILL)
        elif j in TIMED_COLS:
            c.font = Font(color=REDF, bold=True, size=10); c.fill = PatternFill("solid", fgColor=REDFILL)
        else:
            c.font = Font(size=10, color="222222")
            if alt: c.fill = alt
    ws.row_dimensions[r].height = 74

# small legend under the table
lr = len(R) + 4
ws.cell(lr, 1, "Legend:").font = Font(bold=True, size=9)
b = ws.cell(lr, 2, "baseline (untimed MUST)"); b.font = Font(color=BLUEF, bold=True, size=9)
t2 = ws.cell(lr, 4, "timed (MUST-tau)"); t2.font = Font(color=REDF, bold=True, size=9)
ws.merge_cells(start_row=lr, start_column=2, end_row=lr, end_column=3)
ws.merge_cells(start_row=lr, start_column=4, end_row=lr, end_column=6)

ws.freeze_panes = "B3"           # keep title, header and File column visible
ws.sheet_view.showGridLines = False
wb.save(OUT)
print("wrote", OUT, " (rows:", len(R), " cols:", ncols, ")")
