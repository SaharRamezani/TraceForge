# v12 results (generated)

## Campaign

- finished runs: 5,044; DNF at the deadline: 14; stopped by the operator (not results): 444; parallel runs (8 or 16 workers): 83
- cells in the table: 1,815; Mustard finished in 1,760; both tools finished in 1,663
- cross-machine controls: 315 identical commands finished on both machines, counts identical in 315

## Verdicts per protocol

| protocol | cells | Mustard finished | both finished | flips (Must violates, Mustard holds) | both violate | both hold | pending |
|---|---|---|---|---|---|---|---|
| abp | 375 | 356 | 333 | 102 | 93 | 138 | 42 |
| brp | 148 | 145 | 134 | 86 | 48 | 0 | 14 |
| lease | 672 | 662 | 656 | 315 | 341 | 0 | 16 |
| lease_f | 68 | 67 | 66 | 0 | 0 | 66 | 2 |
| nisshi | 77 | 74 | 62 | 39 | 12 | 11 | 15 |
| nrp | 43 | 38 | 37 | 0 | 11 | 26 | 6 |
| retail | 49 | 47 | 47 | 0 | 47 | 0 | 2 |
| swim | 298 | 286 | 286 | 0 | 286 | 0 | 12 |
| yarn | 85 | 85 | 42 | 2 | 32 | 8 | 43 |

## Lease: closed-form boundary

A violation is feasible iff PAUSE >= TTL - 2(U - L) - sd. Checked on every finished lease cell: 662 consistent, 0 not.

| regime | cells consistent | inconsistent |
|---|---|---|
| A | 141 | 0 |
| A-sd | 8 | 0 |
| B | 80 | 0 |
| B-sd | 4 | 0 |
| Bp | 80 | 0 |
| Bp-sd | 4 | 0 |
| C | 114 | 0 |
| C-sd | 6 | 0 |
| Cp | 80 | 0 |
| Cp-sd | 4 | 0 |
| D | 119 | 0 |
| D-sd | 6 | 0 |
| E | 16 | 0 |

## PAR (ABP file, variant par): closed-form window

A violation (premature retransmission accepted as a new frame) is feasible iff L <= W <= 2U + sd. Checked on every finished par cell: 208 consistent, 0 not.

## Frontiers per protocol and regime

Both = largest cell where both tools finished; Mustard only = largest Mustard cell with the Must baseline unfinished. Times in seconds on one core unless marked (c16 = 16 workers).

### abp

**A/W1000** (18 cells)
- both: `ABP-file abp K=4 R=2 W=1000 net=A[0,1] sd=0`: Must 6,315,296 / 0 / 366 s; Mustard 8,002 / 0 / 2.3 s; both hold
- largest flip: `ABP-file par K=7 R=1 W=1000 net=A[0,1] sd=0`: Must 3,959,709 / 318,906 / 258 s; Mustard 8,746 / 0 / 3.3 s; FLIP (Must violates, Mustard holds)

**A/W200** (82 cells)
- both: `ABP-file par K=4 R=3 W=200 net=A[0,1] sd=0`: Must 308,419,431 / 6,150,740 / 5.39 h; Mustard 159,106 / 0 / 67 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=4 R=3 W=200 net=A[0,1] sd=0`: Must 308,419,431 / 6,150,740 / 5.39 h; Mustard 159,106 / 0 / 67 s; FLIP (Must violates, Mustard holds)
- Mustard only: `ABP-file abp K=2 R=10 W=200 net=A[0,1] sd=0`: Must - / - / queued/running; Mustard 12,574,722 / 0 / 3.51 h; Must pending

**A/W3000** (10 cells)
- both: `ABP-file par K=5 R=1 W=3000 net=A[0,1] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=5 R=1 W=3000 net=A[0,1] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)

**A/Wb** (6 cells)
- both: `ABP-file par K=5 R=2 W=2 net=A[0,1] sd=0`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 1,955,414 / 60,702 / 840 s; both violate
- largest flip: `ABP-file par K=5 R=2 W=3 net=A[0,1] sd=0`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 56,022 / 0 / 21 s; FLIP (Must violates, Mustard holds)

**A/Wsd** (2 cells)
- both: `ABP-file par K=5 R=2 W=3 net=A[0,1] sd=1`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 1,955,414 / 60,702 / 806 s; both violate
- largest flip: `ABP-file par K=5 R=2 W=4 net=A[0,1] sd=1`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 56,022 / 0 / 21 s; FLIP (Must violates, Mustard holds)

**B/W1000** (10 cells)
- both: `ABP-file par K=5 R=1 W=1000 net=B[0,65] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=5 R=1 W=1000 net=B[0,65] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)

**B/W200** (10 cells)
- both: `ABP-file par K=5 R=1 W=200 net=B[0,65] sd=0`: Must 63,077 / 3,910 / 3.0 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=5 R=1 W=200 net=B[0,65] sd=0`: Must 63,077 / 3,910 / 3.0 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)

**B/W3000** (10 cells)
- both: `ABP-file par K=5 R=1 W=3000 net=B[0,65] sd=0`: Must 63,077 / 3,910 / 3.0 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=5 R=1 W=3000 net=B[0,65] sd=0`: Must 63,077 / 3,910 / 3.0 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)

**B/Wb** (4 cells)
- both: `ABP-file par K=5 R=2 W=130 net=B[0,65] sd=0`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 1,955,414 / 60,702 / 813 s; both violate
- largest flip: `ABP-file par K=5 R=2 W=131 net=B[0,65] sd=0`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 56,022 / 0 / 20 s; FLIP (Must violates, Mustard holds)

**B/Wsd** (2 cells)
- both: `ABP-file par K=5 R=2 W=195 net=B[0,65] sd=65`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 1,955,414 / 60,702 / 817 s; both violate
- largest flip: `ABP-file par K=5 R=2 W=196 net=B[0,65] sd=65`: Must 69,529,579 / 3,109,929 / 1.22 h; Mustard 56,022 / 0 / 21 s; FLIP (Must violates, Mustard holds)

**C/W1000** (38 cells)
- both: `ABP-file par K=4 R=3 W=1000 net=C[0,200] sd=0`: Must 308,419,431 / 6,150,740 / 5.39 h; Mustard 159,106 / 0 / 67 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=4 R=3 W=1000 net=C[0,200] sd=0`: Must 308,419,431 / 6,150,740 / 5.39 h; Mustard 159,106 / 0 / 67 s; FLIP (Must violates, Mustard holds)
- Mustard only: `ABP-file par K=2 R=8 W=1000 net=C[0,200] sd=0`: Must - / - / queued/running; Mustard 784,386 / 0 / 493 s; Must pending

**C/W200** (46 cells)
- both: `ABP-file par K=4 R=3 W=200 net=C[0,200] sd=0`: Must 308,419,431 / 6,150,740 / 5.39 h; Mustard 17,218,356 / 379,944 / 2.46 h; both violate
- Mustard only: `ABP-file par K=2 R=8 W=200 net=C[0,200] sd=0`: Must - / - / queued/running; Mustard 9,208,708 / 2,289 / 1.51 h; Must pending

**C/W3000** (10 cells)
- both: `ABP-file par K=5 R=1 W=3000 net=C[0,200] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)
- largest flip: `ABP-file par K=5 R=1 W=3000 net=C[0,200] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 970 / 0 / 0.2 s; FLIP (Must violates, Mustard holds)

**C/Wb** (4 cells)
- both: `ABP-file par K=6 R=1 W=400 net=C[0,200] sd=0`: Must 496,447 / 35,874 / 27 s; Mustard 230,836 / 16,999 / 93 s; both violate
- largest flip: `ABP-file par K=6 R=1 W=401 net=C[0,200] sd=0`: Must 496,447 / 35,874 / 27 s; Mustard 2,914 / 0 / 0.8 s; FLIP (Must violates, Mustard holds)

**D/W1000** (63 cells)
- both: `ABP-file abp K=3 R=3 W=1000 net=D[0,2500] sd=0`: Must 20,391,072 / 0 / 1109 s; Mustard 20,184,036 / 0 / 2.86 h; both hold

**D/W200** (18 cells)
- both: `ABP-file abp K=4 R=2 W=200 net=D[0,2500] sd=0`: Must 6,315,296 / 0 / 366 s; Mustard 6,315,296 / 0 / 2890 s; both hold

**D/W3000** (10 cells)
- both: `ABP-file abp K=5 R=1 W=3000 net=D[0,2500] sd=0`: Must 53,627 / 0 / 2.7 s; Mustard 42,242 / 0 / 15 s; both hold

**D/Wb** (2 cells)
- both: `ABP-file par K=6 R=1 W=5000 net=D[0,2500] sd=0`: Must 496,447 / 35,874 / 27 s; Mustard 230,836 / 16,999 / 93 s; both violate
- largest flip: `ABP-file par K=6 R=1 W=5001 net=D[0,2500] sd=0`: Must 496,447 / 35,874 / 27 s; Mustard 2,914 / 0 / 0.8 s; FLIP (Must violates, Mustard holds)

**E/W1000** (10 cells)
- both: `ABP-file par K=5 R=1 W=1000 net=E[0,90000] sd=0`: Must 63,077 / 3,910 / 3.0 s; Mustard 63,077 / 3,910 / 20 s; both violate

**E/W200** (10 cells)
- both: `ABP-file par K=5 R=1 W=200 net=E[0,90000] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 63,077 / 3,910 / 20 s; both violate

**E/W3000** (10 cells)
- both: `ABP-file par K=5 R=1 W=3000 net=E[0,90000] sd=0`: Must 63,077 / 3,910 / 3.1 s; Mustard 63,077 / 3,910 / 20 s; both violate

### brp

**A/T1=1000** (10 cells)
- both: `BRP F=1 n=3 MAX=2 TD=1 T1=1000 TR=4003 SYNC=4003 net=A (p4)`: Must 17,761,263 / 6,908,822 / 1271 s; Mustard 12,191 / 2,094 / 8.4 s; both violate
- largest flip: `BRP F=1 n=3 MAX=2 TD=1 T1=1000 TR=4003 SYNC=4003 net=A (p2)`: Must 3,311,730 / 92,574 / 182 s; Mustard 7,968 / 0 / 4.2 s; FLIP (Must violates, Mustard holds)

**A/T1=200** (35 cells)
- both: `BRP F=1 n=6 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A (p2)`: Must 1,260,206,952 / 39,940,668 / 2.27 h (c16); Mustard 398,145 / 0 / 24 s (c16); FLIP (Must violates, Mustard holds)
- largest flip: `BRP F=1 n=6 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A (p2)`: Must 1,260,206,952 / 39,940,668 / 2.27 h (c16); Mustard 398,145 / 0 / 24 s (c16); FLIP (Must violates, Mustard holds)
- Mustard only: `BRP F=1 n=2 MAX=9 TD=1 T1=200 TR=3603 SYNC=3603 net=A (p2)`: Must - / - / queued/running; Mustard 8,899,846 / 0 / 3.98 h; Must pending

**B/T1=1000** (10 cells)
- both: `BRP F=1 n=3 MAX=2 TD=65 T1=1000 TR=4195 SYNC=4195 net=B (p4)`: Must 17,761,263 / 6,908,822 / 1271 s; Mustard 12,191 / 2,094 / 8.5 s; both violate
- largest flip: `BRP F=1 n=3 MAX=2 TD=65 T1=1000 TR=4195 SYNC=4195 net=B (p2)`: Must 3,311,730 / 92,574 / 182 s; Mustard 7,968 / 0 / 4.2 s; FLIP (Must violates, Mustard holds)

**B/T1=200** (14 cells)
- both: `BRP F=1 n=4 MAX=2 TD=65 T1=200 TR=995 SYNC=995 net=B (p2)`: Must 588,910,898 / 14,427,662 / 1.61 h (c8); Mustard 198,164 / 0 / 22 s (c8); FLIP (Must violates, Mustard holds)
- largest flip: `BRP F=1 n=4 MAX=2 TD=65 T1=200 TR=995 SYNC=995 net=B (p2)`: Must 588,910,898 / 14,427,662 / 1.61 h (c8); Mustard 198,164 / 0 / 22 s (c8); FLIP (Must violates, Mustard holds)

**C/T1=1000** (32 cells)
- both: `BRP F=1 n=6 MAX=1 TD=200 T1=1000 TR=2600 SYNC=2600 net=C (p2)`: Must 1,260,206,952 / 39,940,668 / 2.27 h (c16); Mustard 660,121 / 0 / 41 s (c16); FLIP (Must violates, Mustard holds)
- largest flip: `BRP F=1 n=6 MAX=1 TD=200 T1=1000 TR=2600 SYNC=2600 net=C (p2)`: Must 1,260,206,952 / 39,940,668 / 2.27 h (c16); Mustard 660,121 / 0 / 41 s (c16); FLIP (Must violates, Mustard holds)
- Mustard only: `BRP F=3 n=2 MAX=1 TD=200 T1=1000 TR=2600 SYNC=2600 net=C (p2)`: Must - / - / >11285 (DNF); Mustard 3,244,777 / 0 / 3310 s; Must pending

**C/T1=200** (10 cells)
- both: `BRP F=1 n=3 MAX=2 TD=200 T1=200 TR=1400 SYNC=1400 net=C (p4)`: Must 17,761,263 / 6,908,822 / 1271 s; Mustard 1,312,615 / 197,460 / 1154 s; both violate
- largest flip: `BRP F=1 n=3 MAX=2 TD=200 T1=200 TR=1400 SYNC=1400 net=C (p2)`: Must 3,311,730 / 92,574 / 182 s; Mustard 931,860 / 0 / 482 s; FLIP (Must violates, Mustard holds)

**D/T1=1000** (27 cells)
- both: `BRP F=1 n=5 MAX=1 TD=2500 T1=1000 TR=9500 SYNC=9500 net=D (p2)`: Must 29,053,689 / 959,143 / 1966 s; Mustard 4,839,952 / 6 / 1.06 h; both violate
- largest flip: `BRP F=1 n=2 MAX=5 TD=2500 T1=1000 TR=17500 SYNC=17500 net=D (p2)`: Must 206,967,587 / 1,446,477 / 1674 s (c16); Mustard 25,088,242 / 0 / 3224 s (c16); FLIP (Must violates, Mustard holds)

**D/T1=200** (10 cells)
- both: `BRP F=1 n=3 MAX=2 TD=2500 T1=200 TR=8300 SYNC=8300 net=D (p4)`: Must 17,761,263 / 6,908,822 / 1271 s; Mustard 1,624,921 / 517,433 / 1089 s; both violate
- largest flip: `BRP F=1 n=3 MAX=2 TD=2500 T1=200 TR=8300 SYNC=8300 net=D (p2)`: Must 3,311,730 / 92,574 / 182 s; Mustard 942,836 / 0 / 443 s; FLIP (Must violates, Mustard holds)

### lease

**A** (142 cells)
- both: `Lease C=4 R=2 net=A[0,1] sd=0 TTL=12000 PAUSE=11998`: Must 99,628,848 / 37,607,328 / 4.06 h; Mustard 11,787,624 / 634,752 / 4.19 h; both violate
- largest flip: `Lease C=4 R=2 net=A[0,1] sd=0 TTL=12000 PAUSE=11997`: Must 99,628,848 / 37,607,328 / 4.06 h; Mustard 11,122,128 / 0 / 3.96 h; FLIP (Must violates, Mustard holds)
- Mustard only: `Lease C=2 R=11 net=A[0,1] sd=0 TTL=12000 PAUSE=13000`: Must - / - / queued/running; Mustard 224 / 16 / 0.1 s; Must pending

**A-sd** (8 cells)
- both: `Lease C=3 R=3 net=A[0,1] sd=1 TTL=12000 PAUSE=11997`: Must 49,206,872 / 11,603,400 / 2.12 h; Mustard 6,068,016 / 330,744 / 2.37 h; both violate
- largest flip: `Lease C=3 R=3 net=A[0,1] sd=1 TTL=12000 PAUSE=11996`: Must 49,206,872 / 11,603,400 / 2.12 h; Mustard 5,737,272 / 0 / 2.22 h; FLIP (Must violates, Mustard holds)

**B** (80 cells)
- both: `Lease C=2 R=3 net=B[0,65] sd=0 TTL=12000 PAUSE=11870`: Must 34,972 / 3,428 / 3.8 s; Mustard 14,202 / 640 / 9.2 s; both violate
- largest flip: `Lease C=2 R=3 net=B[0,65] sd=0 TTL=60000 PAUSE=200`: Must 34,972 / 3,428 / 3.8 s; Mustard 5,728 / 0 / 3.7 s; FLIP (Must violates, Mustard holds)

**B-sd** (4 cells)
- both: `Lease C=2 R=3 net=B[0,65] sd=65 TTL=12000 PAUSE=11805`: Must 34,972 / 3,428 / 3.8 s; Mustard 14,202 / 640 / 9.1 s; both violate
- largest flip: `Lease C=2 R=3 net=B[0,65] sd=65 TTL=12000 PAUSE=11804`: Must 34,972 / 3,428 / 3.8 s; Mustard 13,562 / 0 / 8.7 s; FLIP (Must violates, Mustard holds)

**Bp** (80 cells)
- both: `Lease C=2 R=3 net=Bp[14,65] sd=0 TTL=60000 PAUSE=59898`: Must 34,972 / 3,428 / 3.8 s; Mustard 14,202 / 640 / 11 s; both violate
- largest flip: `Lease C=2 R=3 net=Bp[14,65] sd=0 TTL=60000 PAUSE=200`: Must 34,972 / 3,428 / 3.8 s; Mustard 5,728 / 0 / 4.5 s; FLIP (Must violates, Mustard holds)

**Bp-sd** (4 cells)
- both: `Lease C=2 R=3 net=Bp[14,65] sd=65 TTL=60000 PAUSE=59833`: Must 34,972 / 3,428 / 3.8 s; Mustard 14,202 / 640 / 11 s; both violate
- largest flip: `Lease C=2 R=3 net=Bp[14,65] sd=65 TTL=12000 PAUSE=11832`: Must 34,972 / 3,428 / 3.8 s; Mustard 13,562 / 0 / 10 s; FLIP (Must violates, Mustard holds)

**C** (118 cells)
- both: `Lease C=6 R=1 net=C[0,200] sd=0 TTL=12000 PAUSE=11600`: Must 85,157,536 / 51,560,640 / 3.13 h; Mustard 14,142,136 / 637,560 / 3.65 h; both violate
- largest flip: `Lease C=6 R=1 net=C[0,200] sd=0 TTL=12000 PAUSE=11599`: Must 85,157,536 / 51,560,640 / 3.13 h; Mustard 13,504,576 / 0 / 3.48 h; FLIP (Must violates, Mustard holds)

**C-sd** (6 cells)
- both: `Lease C=2 R=5 net=C[0,200] sd=200 TTL=12000 PAUSE=11400`: Must 11,337,388 / 1,111,508 / 1763 s; Mustard 2,049,066 / 92,368 / 3106 s; both violate
- largest flip: `Lease C=2 R=5 net=C[0,200] sd=200 TTL=12000 PAUSE=11399`: Must 11,337,388 / 1,111,508 / 1763 s; Mustard 1,956,698 / 0 / 2912 s; FLIP (Must violates, Mustard holds)

**Cp** (80 cells)
- both: `Lease C=2 R=3 net=Cp[42,200] sd=0 TTL=12000 PAUSE=11684`: Must 34,972 / 3,428 / 3.8 s; Mustard 14,202 / 640 / 11 s; both violate
- largest flip: `Lease C=2 R=3 net=Cp[42,200] sd=0 TTL=12000 PAUSE=11683`: Must 34,972 / 3,428 / 3.8 s; Mustard 13,562 / 0 / 10 s; FLIP (Must violates, Mustard holds)

**Cp-sd** (4 cells)
- both: `Lease C=2 R=3 net=Cp[42,200] sd=200 TTL=60000 PAUSE=59484`: Must 34,972 / 3,428 / 3.8 s; Mustard 14,202 / 640 / 11 s; both violate
- largest flip: `Lease C=2 R=3 net=Cp[42,200] sd=200 TTL=12000 PAUSE=11483`: Must 34,972 / 3,428 / 3.8 s; Mustard 13,562 / 0 / 10 s; FLIP (Must violates, Mustard holds)

**D** (124 cells)
- both: `Lease C=6 R=1 net=D[0,2500] sd=0 TTL=12000 PAUSE=7000`: Must 85,157,536 / 51,560,640 / 3.13 h; Mustard 14,142,136 / 637,560 / 3.62 h; both violate
- largest flip: `Lease C=6 R=1 net=D[0,2500] sd=0 TTL=12000 PAUSE=6999`: Must 85,157,536 / 51,560,640 / 3.13 h; Mustard 13,504,576 / 0 / 3.47 h; FLIP (Must violates, Mustard holds)

**D-sd** (6 cells)
- both: `Lease C=2 R=5 net=D[0,2500] sd=2500 TTL=12000 PAUSE=4500`: Must 11,337,388 / 1,111,508 / 1763 s; Mustard 11,047,158 / 413,728 / 4.23 h; both violate
- largest flip: `Lease C=2 R=5 net=D[0,2500] sd=2500 TTL=12000 PAUSE=4499`: Must 11,337,388 / 1,111,508 / 1763 s; Mustard 10,633,430 / 0 / 3.99 h; FLIP (Must violates, Mustard holds)

**E** (16 cells)
- both: `Lease C=2 R=3 net=E[0,90000] sd=90000 TTL=60000 PAUSE=200`: Must 34,972 / 3,428 / 3.8 s; Mustard 37,556 / 3,580 / 22 s; both violate

### lease_f

**A** (28 cells)
- both: `Lease + fencing C=2 R=5 net=A[0,1] sd=0 TTL=12000 PAUSE=11998`: Must 94,406,884 / 0 / 3.79 h; Mustard 6,461,828 / 0 / 2.63 h; both hold
- Mustard only: `Lease + fencing C=6 R=1 net=A[0,1] sd=0 TTL=12000 PAUSE=11998`: Must - / - / >21210 (DNF); Mustard 17,512,456 / 0 / 4.50 h; Must pending

**C** (12 cells)
- both: `Lease + fencing C=2 R=3 net=C[0,200] sd=0 TTL=12000 PAUSE=11600`: Must 104,884 / 0 / 11 s; Mustard 25,220 / 0 / 17 s; both hold

**D** (28 cells)
- both: `Lease + fencing C=2 R=5 net=D[0,2500] sd=0 TTL=12000 PAUSE=7000`: Must 94,406,884 / 0 / 3.79 h; Mustard 6,461,828 / 0 / 1364 s (c7); both hold

### nisshi

**A** (19 cells)
- both: `nisshi T=2 R=5 L=0 U=1 sd=0 W=30000 B=100 net=A (both)`: Must 58,985,415 / 1,977,470 / 3.07 h; Mustard 29,348 / 0 / 28 s; FLIP (Must violates, Mustard holds)
- largest flip: `nisshi T=2 R=5 L=0 U=1 sd=0 W=30000 B=100 net=A (both)`: Must 58,985,415 / 1,977,470 / 3.07 h; Mustard 29,348 / 0 / 28 s; FLIP (Must violates, Mustard holds)
- Mustard only: `nisshi T=2 R=10 L=0 U=1 sd=0 W=30000 B=100 net=A (both)`: Must - / - / queued/running; Mustard 17,537,560 / 0 / 1.35 h (c16); Must pending

**C** (16 cells)
- both: `nisshi T=2 R=5 L=0 U=200 sd=0 W=30000 B=100 net=C (both)`: Must 58,985,415 / 1,977,470 / 3.07 h; Mustard 29,348 / 0 / 28 s; FLIP (Must violates, Mustard holds)
- largest flip: `nisshi T=2 R=5 L=0 U=200 sd=0 W=30000 B=100 net=C (both)`: Must 58,985,415 / 1,977,470 / 3.07 h; Mustard 29,348 / 0 / 28 s; FLIP (Must violates, Mustard holds)
- Mustard only: `nisshi T=8 R=4 L=0 U=200 sd=0 W=30000 B=100 net=C (both)`: Must - / - / >13478 (DNF); Mustard 759,286 / 0 / 1530 s; Must pending

**D** (16 cells)
- both: `nisshi T=2 R=5 L=0 U=2500 sd=0 W=30000 B=100 net=D (both)`: Must 58,985,415 / 1,977,470 / 3.07 h; Mustard 29,348 / 0 / 28 s; FLIP (Must violates, Mustard holds)
- largest flip: `nisshi T=2 R=5 L=0 U=2500 sd=0 W=30000 B=100 net=D (both)`: Must 58,985,415 / 1,977,470 / 3.07 h; Mustard 29,348 / 0 / 28 s; FLIP (Must violates, Mustard holds)
- Mustard only: `nisshi T=8 R=4 L=0 U=2500 sd=0 W=30000 B=100 net=D (both)`: Must - / - / >13478 (DNF); Mustard 759,286 / 0 / 1530 s; Must pending

**E** (14 cells)
- both: `nisshi T=16 R=2 L=0 U=90000 sd=0 W=30000 B=100 net=E (both)`: Must 3,289,304 / 168,880 / 1145 s; Mustard 3,289,304 / 168,880 / 3.75 h; both violate

**E/fix** (12 cells)
- both: `nisshi T=6 R=3 L=0 U=90000 sd=0 W=30000 B=100 fix=epoch-bump net=E (both)`: Must 16,611,387 / 0 / 1.09 h; Mustard 8,777,115 / 0 / 3.99 h; both hold
- Mustard only: `nisshi T=2 R=5 L=0 U=90000 sd=0 W=30000 B=100 fix=epoch-bump net=E (both)`: Must - / - / >21475 (DNF); Mustard 13,288,105 / 0 / 4.04 h; Must pending

### nrp

**A** (13 cells)
- both: `NRP nrp R=4 M=3 H=1000 P=2 net=A[0,1] sd=0`: Must 118,424,493 / 3,538,558 / 2.57 h; Mustard 1,492,721 / 33,704 / 1296 s; both violate
- Mustard only: `NRP nrp R=5 M=3 H=1000 P=2 net=A[0,1] sd=0`: Must - / - / queued/running; Mustard 125,536,980 / 4,896,854 / 2.91 h (c16); Must pending

**B** (6 cells)
- both: `NRP nrp R=4 M=3 H=1000 P=130 net=B[0,65] sd=0`: Must 118,424,493 / 3,538,558 / 2.57 h; Mustard 1,492,721 / 33,704 / 1298 s; both violate

**C** (12 cells)
- both: `NRP nrp R=4 M=3 H=1000 P=400 net=C[0,200] sd=0`: Must 118,424,493 / 3,538,558 / 2.57 h; Mustard 3,918,481 / 64,314 / 3312 s; both violate

**D** (12 cells)
- both: `NRP nrp R=4 M=3 H=10000 P=5000 net=D[0,2500] sd=0`: Must 118,424,493 / 3,538,558 / 2.57 h; Mustard 4,243,153 / 64,314 / 1.03 h; both violate

### retail

**A** (15 cells)
- both: `Shop v2 C=2 N=2 R=5 S=20 D=29000 net=A[0,1] sd=0`: Must 15,012,496 / 14,754,448 / 2263 s; Mustard 10,253,688 / 9,995,640 / 3.02 h; both violate

**B** (6 cells)
- both: `Shop v2 C=3 N=3 R=2 S=20 D=29000 net=B[0,65] sd=0`: Must 1,564,672 / 1,558,912 / 212 s; Mustard 861,328 / 855,568 / 561 s; both violate

**C** (7 cells)
- both: `Shop v2 C=2 N=2 R=4 S=20 D=29000 net=C[0,200] sd=0`: Must 1,038,352 / 1,020,432 / 141 s; Mustard 709,432 / 691,512 / 582 s; both violate

**D** (15 cells)
- both: `Shop v2 C=2 N=2 R=5 S=20 D=29000 net=D[0,2500] sd=0`: Must 15,012,496 / 14,754,448 / 345 s (c7); Mustard 10,253,688 / 9,995,640 / 1515 s (c7); both violate

**E** (6 cells)
- both: `Shop v2 C=3 N=3 R=2 S=20 D=29000 net=E[0,90000] sd=0`: Must 1,564,672 / 1,558,912 / 207 s; Mustard 1,564,672 / 1,558,912 / 858 s; both violate

### swim

**lan/A** (38 cells)
- both: `SWIM N=11 R=1 timers=lan(Wp=500,Ws=4000) net=A[0,1] sd=0`: Must 37,939,862 / 37,290,312 / 1.54 h; Mustard 23,320,437 / 22,769,302 / 3.50 h; both violate

**lan/B** (12 cells)
- both: `SWIM N=4 R=2 timers=lan(Wp=500,Ws=4000) net=B[0,65] sd=65`: Must 27,436 / 24,300 / 2.0 s; Mustard 10,588 / 8,904 / 2.7 s; both violate

**lan/C** (12 cells)
- both: `SWIM N=4 R=2 timers=lan(Wp=500,Ws=4000) net=C[0,200] sd=200`: Must 27,436 / 24,300 / 2.1 s; Mustard 23,437 / 19,779 / 6.7 s; both violate

**lan/D** (12 cells)
- both: `SWIM N=4 R=2 timers=lan(Wp=500,Ws=4000) net=D[0,2500] sd=0`: Must 27,436 / 24,300 / 2.0 s; Mustard 34,873 / 28,875 / 8.3 s; both violate

**lan/E** (12 cells)
- both: `SWIM N=4 R=2 timers=lan(Wp=500,Ws=4000) net=E[0,90000] sd=0`: Must 27,436 / 24,300 / 2.0 s; Mustard 34,873 / 28,875 / 8.3 s; both violate

**local/A** (12 cells)
- both: `SWIM N=4 R=2 timers=local(Wp=200,Ws=3000) net=A[0,1] sd=1`: Must 27,436 / 24,300 / 2.0 s; Mustard 10,588 / 8,904 / 2.7 s; both violate

**local/B** (12 cells)
- both: `SWIM N=4 R=2 timers=local(Wp=200,Ws=3000) net=B[0,65] sd=65`: Must 27,436 / 24,300 / 2.1 s; Mustard 10,588 / 8,904 / 2.8 s; both violate

**local/C** (38 cells)
- both: `SWIM N=6 R=2 timers=local(Wp=200,Ws=3000) net=C[0,200] sd=0`: Must 14,228,178 / 13,870,962 / 1572 s; Mustard 12,234,531 / 11,797,935 / 1.66 h; both violate

**local/D** (38 cells)
- both: `SWIM N=11 R=1 timers=local(Wp=200,Ws=3000) net=D[0,2500] sd=0`: Must 37,939,862 / 37,290,312 / 1.54 h; Mustard 40,840,025 / 40,013,328 / 4.62 h; both violate

**wan/A** (12 cells)
- both: `SWIM N=4 R=2 timers=wan(Wp=3000,Ws=30000) net=A[0,1] sd=0`: Must 27,436 / 24,300 / 2.1 s; Mustard 10,588 / 8,904 / 2.8 s; both violate

**wan/B** (12 cells)
- both: `SWIM N=4 R=2 timers=wan(Wp=3000,Ws=30000) net=B[0,65] sd=65`: Must 27,436 / 24,300 / 2.0 s; Mustard 10,588 / 8,904 / 2.7 s; both violate

**wan/C** (38 cells)
- both: `SWIM N=11 R=1 timers=wan(Wp=3000,Ws=30000) net=C[0,200] sd=0`: Must 37,939,862 / 37,290,312 / 1.54 h; Mustard 23,320,437 / 22,769,302 / 3.67 h; both violate

**wan/D** (38 cells)
- both: `SWIM N=6 R=2 timers=wan(Wp=3000,Ws=30000) net=D[0,2500] sd=0`: Must 14,228,178 / 13,870,962 / 1572 s; Mustard 12,234,531 / 11,797,935 / 1.67 h; both violate

**wan/E** (12 cells)
- both: `SWIM N=4 R=2 timers=wan(Wp=3000,Ws=30000) net=E[0,90000] sd=90000`: Must 27,436 / 24,300 / 2.1 s; Mustard 34,873 / 28,875 / 8.6 s; both violate

### yarn

**A-C** (48 cells)
- both: `YARN K=2 QS=3 DD=8 U=0 H=10 miss net=A-C`: Must 159,156,296 / 223,993 / 4.74 h; Mustard 28,363 / 732 / 23 s; both violate
- largest flip: `YARN K=2 QS=3 DD=8 U=0 H=10 late-report net=A-C`: Must 160,091,241 / 496,450 / 2508 s (c8); Mustard 29,515 / 0 / 3.8 s (c8); FLIP (Must violates, Mustard holds)
- Mustard only: `YARN K=4 QS=5 DD=8 U=0 H=6 miss net=A-C`: Must - / - / queued/running; Mustard 14,731,889 / 0 / 4.87 h; Must pending

**D** (37 cells)
- both: `YARN K=2 QS=3 DD=8 U=3 H=10 miss net=D`: Must 159,156,296 / 223,993 / 4.74 h; Mustard 231,212 / 11,041 / 187 s; both violate
- Mustard only: `YARN K=3 QS=4 DD=8 U=3 H=8 miss net=D`: Must - / - / queued/running; Mustard 14,988,378 / 187,269 / 4.48 h; Must pending

## Largest finished sizes per protocol and dimension

For each size coordinate: the largest value reached by a finished cell, with the full size of that cell. Size coordinates: lease (C, R); SWIM (N, R); nisshi (T, R); ABP/PAR (K, R); BRP (F, n, MAX); NRP (R); shop (C, N, R); YARN (K, H).

| protocol | coordinate | Mustard max (size) | Must max (size) |
|---|---|---|---|
| abp | K | 15 (15x1) | 10 (10x1) |
| abp | R | 11 (2x11) | 7 (2x7) |
| brp | n(F=...) | 8 (8x1x1) | 6 (6x1x1) |
| brp | MAX | 9 (2x9x1) | 5 (2x5x1) |
| brp | F | 3 (2x1x3) | 2 (2x1x2) |
| lease | C | 6 (6x1) | 6 (6x1) |
| lease | R | 6 (2x6) | 6 (2x6) |
| lease_f | C | 6 (6x1) | 5 (5x1) |
| lease_f | R | 5 (2x5) | 5 (2x5) |
| nisshi | T | 32 (32x2) | 32 (32x2) |
| nisshi | R | 10 (2x10) | 5 (2x5) |
| nrp | R | 5 (5) | 4 (4) |
| retail | C | 3 (3x3x2) | 3 (3x3x2) |
| retail | N | 3 (3x3x2) | 3 (3x3x2) |
| retail | R | 5 (2x2x5) | 5 (2x2x5) |
| swim | N | 11 (11x1) | 11 (11x1) |
| swim | R | 7 (3x7) | 7 (3x7) |
| yarn | K | 4 (4x6) | 3 (3x6) |
| yarn | H | 52 (1x52) | 24 (1x24) |

## Reductions where both tools finished (cells with Must time >= 10 s)

Exec. ratio = Must Exec. / Mustard Exec.; time ratio = Must time / Mustard time (> 1: Mustard faster). Parallel runs excluded.

| protocol | regime | cells | Exec. ratio median | Exec. ratio max | time ratio median | time ratio max | Mustard slower in |
|---|---|---|---|---|---|---|---|
| abp | A | 39 | 452.7 | 4726 | 76.6 | 773 | 1 |
| abp | B | 6 | 170.4 | 1241 | 31.8 | 217 | 1 |
| abp | C | 34 | 45.8 | 3299 | 6.4 | 431 | 12 |
| abp | D | 25 | 1.0 | 170 | 0.1 | 32 | 24 |
| brp | A | 17 | 415.6 | 1457 | 44.2 | 152 | 0 |
| brp | B | 11 | 415.6 | 1457 | 43.7 | 149 | 0 |
| brp | C | 17 | 274.7 | 1457 | 32.7 | 148 | 3 |
| brp | D | 16 | 5.2 | 15 | 0.5 | 1 | 12 |
| lease | A | 32 | 6.0 | 50613 | 0.9 | 29271 | 26 |
| lease | C | 18 | 5.5 | 2812 | 0.6 | 1457 | 15 |
| lease | D | 22 | 4.2 | 14 | 0.6 | 2 | 20 |
| lease_f | A | 14 | 8.9 | 107 | 1.4 | 22 | 4 |
| lease_f | C | 3 | 7.7 | 49 | 1.2 | 9 | 1 |
| lease_f | D | 13 | 7.8 | 25 | 1.2 | 4 | 4 |
| nisshi | A | 6 | 102.2 | 2010 | 17.2 | 396 | 0 |
| nisshi | C | 6 | 102.2 | 2010 | 17.6 | 396 | 0 |
| nisshi | D | 6 | 102.2 | 2010 | 17.6 | 390 | 0 |
| nisshi | E | 10 | 1.7 | 7 | 0.3 | 1 | 9 |
| nrp | A | 8 | 15.5 | 79 | 1.8 | 7 | 0 |
| nrp | B | 4 | 15.5 | 79 | 1.8 | 7 | 0 |
| nrp | C | 7 | 8.9 | 30 | 1.0 | 3 | 3 |
| nrp | D | 8 | 8.9 | 28 | 1.0 | 3 | 2 |
| retail | A | 6 | 1.8 | 2 | 0.4 | 0 | 6 |
| retail | B | 2 | 1.9 | 2 | 0.4 | 0 | 2 |
| retail | C | 3 | 1.8 | 2 | 0.4 | 0 | 3 |
| retail | D | 5 | 1.8 | 2 | 0.4 | 0 | 5 |
| retail | E | 2 | 1.0 | 1 | 0.2 | 0 | 2 |
| swim | lan | 13 | 2.6 | 11 | 0.6 | 2 | 9 |
| swim | local | 21 | 0.9 | 1 | 0.3 | 0 | 21 |
| swim | wan | 23 | 1.6 | 11 | 0.5 | 2 | 19 |
| yarn | A-C | 11 | 888.6 | 9083 | 165.1 | 746 | 0 |
| yarn | D | 11 | 185.3 | 688 | 28.2 | 91 | 0 |

## Cost per explored graph (both finished on one core, Must time >= 10 s)

Ratio = (Mustard time / Mustard Exec.) / (Must time / Must Exec.): how much more one graph costs in Mustard. A tool wins on time when its execution reduction exceeds this ratio.

| protocol | cells | per-graph cost ratio, median | min | max |
|---|---|---|---|---|
| abp | 104 | 6.6 | 4.9 | 11.5 |
| brp | 61 | 9.4 | 6.9 | 12.3 |
| lease | 72 | 7.5 | 1.7 | 10.1 |
| lease_f | 30 | 6.7 | 4.8 | 10.1 |
| nisshi | 28 | 5.8 | 4.1 | 11.8 |
| nrp | 27 | 8.8 | 8.5 | 11.2 |
| retail | 18 | 5.1 | 4.1 | 7.0 |
| swim | 57 | 3.9 | 2.7 | 6.5 |
| yarn | 22 | 7.5 | 5.2 | 13.2 |

## Parallel runs (8 or 16 workers; wall time not comparable with one core)

- `v12D-abp-01-t-r1` ABP-file par K=14 R=1 W=200 net=A[0,1] sd=0: 1451 s on 16 workers, brain10
- `v12D-abp-02-t-r1` ABP-file par K=15 R=1 W=200 net=A[0,1] sd=0: 4238 s on 16 workers, brain10
- `v12D-nisshi-04-t-r1` nisshi T=2 R=10 L=0 U=1 sd=0 W=30000 B=100 net=A (both): 4843 s on 16 workers, brain10
- `v12D-yarn-01-t-r1` YARN K=2 QS=3 DD=8 U=0 H=18 miss net=A-C: 839 s on 16 workers, brain10
- `v12D-yarn-02-t-r1` YARN K=2 QS=3 DD=8 U=0 H=19 miss net=A-C: 1742 s on 16 workers, brain10
- `v12D-yarn-03-t-r1` YARN K=1 QS=2 DD=7 U=0 H=52 preempt net=A-C: 1342 s on 16 workers, brain10
- `v12E-01-mus-v12L-nisshi-00194-b-r1` nisshi baseline 16-2-both-epoch-bump: 726 s on 8 workers, brain05
- `v12E-02-mus-v12-nrp-00106-t-r1` NRP leasing R=4 M=3 H=10000 P=5000 net=D[0,2500] sd=0: 680 s on 8 workers, brain10
- `v12E-03-mus-v12L-nrp-00165-t-r1` NRP leasing R=4 M=3 H=10000 P=5000 net=D[0,2500] sd=0: 662 s on 8 workers, brain10
- `v12E-04-mus-v12L-nrp-00145-t-r1` NRP leasing R=4 M=3 H=1000 P=400 net=C[0,200] sd=0: 600 s on 8 workers, brain05
- `v12E-06-mus-v12L-nrp-00105-t-r1` NRP noopt R=4 M=3 H=10000 P=5000 net=D[0,2500] sd=0: 505 s on 14 workers, brain10
- `v12E-07-mus-v12-nrp-00088-t-r1` NRP noopt R=4 M=3 H=10000 P=5000 net=D[0,2500] sd=0: 459 s on 14 workers, brain10
- `v12E-08-mus-v12L-nisshi-00015-t-r1` nisshi T=32 R=2 L=0 U=1 sd=0 W=30000 B=100 net=A (both): 393 s on 16 workers, brain05
- `v12E-09-mus-v12L-nrp-00085-t-r1` NRP noopt R=4 M=3 H=1000 P=400 net=C[0,200] sd=0: 470 s on 14 workers, brain10
- `v12E-10-mus-v12-nrp-00097-t-r1` NRP leasing R=4 M=3 H=1000 P=130 net=B[0,65] sd=0: 226 s on 8 workers, brain05
- `v12E-11-mus-v12L-nrp-00125-t-r1` NRP leasing R=4 M=3 H=1000 P=2 net=A[0,1] sd=0: 236 s on 8 workers, brain05
- `v12E-13-mus-v12L-nrp-00065-t-r1` NRP noopt R=4 M=3 H=1000 P=2 net=A[0,1] sd=0: 158 s on 14 workers, brain10
- `v12E-15-mus-v12-abp-00711-b-r1` abp baseline par-8-1: 605 s on 16 workers, brain10
- `v12E-19-mus-v12-yarn-00085-t-r1` YARN K=2 QS=3 DD=8 U=3 H=10 preempt net=D: 33 s on 7 workers, brain10
- `v12E-21-mus-v12-brp-00353-t-r1` BRP F=1 n=4 MAX=2 TD=65 T1=200 TR=995 SYNC=995 net=B (p2): 22 s on 8 workers, brain10
- `v12E-22-mus-v12-brp-00355-t-r1` BRP F=1 n=4 MAX=2 TD=200 T1=1000 TR=4600 SYNC=4600 net=C (p2): 19 s on 8 workers, brain10
- `v12E-24-mus-v12L-abp-00403-t-r1` ABP-file par K=2 R=7 W=200 net=A[0,1] sd=0: 13 s on 8 workers, brain10
- `v12E-25-mus-v12L-abp-00471-t-r1` ABP-file par K=2 R=7 W=1000 net=C[0,200] sd=0: 13 s on 8 workers, brain10
- `v12E-28-mus-v12L-yarn-00013-t-r1` YARN K=2 QS=3 DD=8 U=0 H=11 miss net=A-C: 4 s on 16 workers, brain10
- `v12E-30-mus-v12L-brp-00145-t-r1` BRP F=1 n=2 MAX=5 TD=200 T1=1000 TR=10600 SYNC=10600 net=C (p2): 9 s on 8 workers, brain10
- `v12E-32-mus-v12-yarn-00073-t-r1` YARN K=2 QS=3 DD=8 U=0 H=10 late-report net=A-C: 4 s on 8 workers, brain10
- `v12E2-101-must` abp baseline abp-8-1: 120 s on 12 workers, brain10
- `v12E2-102-must` ABP-file abp K=10 R=1 W=200 net=A[0,1] sd=0: 9 s on 16 workers, brain10
- `v12E2-103-must` brp baseline 2-5-1-p2: 1674 s on 16 workers, brain10
- `v12E2-105-must` retail baseline 2-2-5: 345 s on 7 workers, brain10
- `v12E2-106-must` YARN K=3 QS=4 DD=8 U=0 H=6 miss net=A-C: 6 s on 16 workers, brain10
- `v12E2-107-must` YARN K=3 QS=4 DD=8 U=3 H=6 miss net=D: 59 s on 16 workers, brain10
- `v12E2-108-must` YARN K=3 QS=4 DD=8 U=0 H=6 miss net=A-C: 6 s on 16 workers, brain10
- `v12E2-109-must` YARN K=3 QS=4 DD=8 U=3 H=6 miss net=D: 56 s on 16 workers, brain10
- `v12F-12-mus-v12-nrp-00079-t-r1` NRP noopt R=4 M=3 H=1000 P=130 net=B[0,65] sd=0: 149 s on 14 workers, brain10
- `v12F-14-mus-v12L-abp-00437-t-r1` ABP-file par K=2 R=7 W=200 net=C[0,200] sd=0: 290 s on 8 workers, brain10
- `v12F-16-mus-v12L-brp-00049-t-r1` BRP F=1 n=6 MAX=1 TD=200 T1=1000 TR=2600 SYNC=2600 net=C (p2): 41 s on 16 workers, brain10
- `v12F-17-mus-v12L-yarn-00055-t-r1` YARN K=2 QS=3 DD=8 U=3 H=11 miss net=D: 41 s on 16 workers, brain10
- `v12F-18-mus-v12-yarn-00088-t-r1` YARN K=2 QS=3 DD=8 U=3 H=10 late-report net=D: 125 s on 8 workers, brain10
- `v12F-20-mus-v12L-brp-00015-t-r1` BRP F=1 n=6 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A (p2): 24 s on 16 workers, brain10
- `v12F-23-mus-v12-brp-00349-t-r1` BRP F=1 n=4 MAX=2 TD=1 T1=200 TR=803 SYNC=803 net=A (p2): 16 s on 8 workers, brain10
- `v12F-26-mus-v12-brp-00365-t-r1` BRP F=1 n=3 MAX=3 TD=200 T1=1000 TR=6600 SYNC=6600 net=C (p2): 7 s on 8 workers, brain10
- `v12F-27-mus-v12-brp-00361-t-r1` BRP F=1 n=3 MAX=3 TD=1 T1=200 TR=1203 SYNC=1203 net=A (p2): 7 s on 8 workers, brain10
- `v12F-29-mus-v12-yarn-00067-t-r1` YARN K=2 QS=3 DD=8 U=0 H=10 preempt net=A-C: 4 s on 7 workers, brain10
- `v12F-31-mus-v12L-brp-00117-t-r1` BRP F=1 n=2 MAX=5 TD=1 T1=200 TR=2003 SYNC=2003 net=A (p2): 3 s on 8 workers, brain10
- `v12F-33-mus-v12L-lease-00377-t-r1` Lease C=2 R=6 net=A[0,1] sd=0 TTL=12000 PAUSE=13000: 0 s on 16 workers, brain10
- `v12P-abp-00083-t-r1` ABP-file par K=8 R=1 W=200 net=C[0,200] sd=0: 2178 s on 16 workers, brain10
- `v12P-abp-00211-t-r1` ABP-file par K=8 R=1 W=1000 net=D[0,2500] sd=0: 5609 s on 16 workers, brain10
- `v12P-brp-00173-t-r1` BRP F=1 n=2 MAX=5 TD=2500 T1=1000 TR=17500 SYNC=17500 net=D (p2): 3224 s on 16 workers, brain05
- `v12P-lease-00017-t-r1` Lease C=2 R=6 net=A[0,1] sd=0 TTL=12000 PAUSE=11997: 3049 s on 16 workers, brain10
- `v12P-swim-00239-t-r1` SWIM N=3 R=7 timers=lan(Wp=500,Ws=4000) net=A[0,1] sd=0: 1850 s on 16 workers, brain05
- `v12P12-W3-abp-00339-t-r1` ABP-file abp K=8 R=1 W=1000 net=D[0,2500] sd=0: 918 s on 12 workers, brain10
- `v12P14-nrp-B-noopt-4-r2` nrp baseline noopt-4: 2827 s on 14 workers, brain05
- `v12P16-W2-brp-00051-t-r1` BRP F=1 n=7 MAX=1 TD=200 T1=1000 TR=2600 SYNC=2600 net=C (p2): 564 s on 16 workers, brain05
- `v12P16-W2-lease-00257-t-r1` Lease C=2 R=6 net=D[0,2500] sd=0 TTL=12000 PAUSE=6999: 5560 s on 16 workers, brain10
- `v12P16-abp-B-abp-10_1-r1` abp baseline abp-10-1: 6093 s on 16 workers, brain10
- `v12P16-abp-par-2-11-r1` ABP-file par K=2 R=11 W=200 net=A[0,1] sd=0: 4466 s on 16 workers, brain05
- `v12P16-brp-00016-b-r1` brp baseline 6-1-1-p2: 8172 s on 16 workers, brain10
- `v12P16-brp-00019-t-r1` BRP F=1 n=8 MAX=1 TD=1 T1=200 TR=403 SYNC=403 net=A (p2): 2704 s on 16 workers, brain10
- `v12P16-lease-00408-b-r1` lease baseline 2-6: 3334 s on 16 workers, brain05
- `v12P16-nisshi-00016-b-r1` nisshi baseline 32-2-both-none: 2747 s on 16 workers, brain05
- `v12P16-nrp-C03-t-r1` NRP nrp R=5 M=3 H=1000 P=2 net=A[0,1] sd=0: 10460 s on 16 workers, brain10
- `v12P16-swim-b-3-7-r1` swim baseline 3-7: 6129 s on 16 workers, brain05
- `v12P16-yarn-B-2_11_8_miss-r1` yarn baseline 2-11-8-miss: 8185 s on 16 workers, brain05
- `v12P16-yarn-B-3_6_8_miss-r1` yarn baseline 3-6-8-miss: 10142 s on 16 workers, brain05
- `v12P16-yarn-C02-t-r1` YARN K=2 QS=3 DD=8 U=3 H=15 miss net=D: 2758 s on 16 workers, brain10
- `v12P7-W3-abp-00287-t-r1` ABP-file abp K=14 R=1 W=200 net=A[0,1] sd=0: 2614 s on 7 workers, brain05
- `v12P7-W3-lease_f-00075-t-r1` Lease + fencing C=2 R=5 net=D[0,2500] sd=0 TTL=12000 PAUSE=7000: 1364 s on 7 workers, brain05
- `v12P7-W3-retail-00037-t-r1` Shop v2 C=2 N=2 R=5 S=20 D=29000 net=D[0,2500] sd=0: 1515 s on 7 workers, brain05
- `v12P7-yarn-B-2_10_8_preempt-r2` yarn baseline 2-10-8-preempt: 3140 s on 7 workers, brain10
- `v12P8-abp-00404-b-r1` abp baseline par-2-7: 11953 s on 8 workers, brain05
- `v12P8-brp-00174-b-r1` brp baseline 2-5-1-p2: 2099 s on 8 workers, brain05
- `v12P8-brp-B-3_3_1_p2-r1` brp baseline 3-3-1-p2: 2692 s on 8 workers, brain05
- `v12P8-brp-B-4_2_1_p2-r1` brp baseline 4-2-1-p2: 5791 s on 8 workers, brain05
- `v12P8-lease-00077-t-r1` Lease C=2 R=6 net=A[0,1] sd=0 TTL=12000 PAUSE=11998: 6650 s on 8 workers, brain10
- `v12P8-nisshi-00193-t-r1` nisshi T=16 R=2 L=0 U=90000 sd=0 W=30000 B=100 fix=epoch-bump net=E (both): 8168 s on 8 workers, brain10
- `v12P8-nrp-B-leasing-4-r2` nrp baseline leasing-4: 3192 s on 8 workers, brain05
- `v12P8-yarn-B-2_10_8_late_report-r2` yarn baseline 2-10-8-late-report: 2508 s on 8 workers, brain10
- `v12T-abp-00568-b-r1` abp baseline abp-2-4: 42 s on 8 workers, brain05
- `v12T-brp-00172-b-r1` brp baseline 2-4-1-p2: 97 s on 8 workers, brain05
- `v12T-lease-00404-b-r1` lease baseline 2-4: 17 s on 8 workers, brain05
- `v12T-nisshi-00094-b-r1` nisshi baseline 2-4-both-none: 51 s on 8 workers, brain05
- `v12T-swim-00585-b-r1` swim baseline 4-3: 63 s on 8 workers, brain05

