# v12 regimes: every timing number comes from a real system (citations checked 2026-10-15)

Unit of time: 1 ms. The checker's verdicts depend only on the order relations between sums of delays and timers, so
absolute magnitudes are free; what is chosen are the RATIOS that real deployments have. All sends are lossy (model rule).
Window of a message: transit in [L, U], storage lifetime sd (how long an arrived message stays readable).

## 1. How L, U and sd are chosen

- **U** is a tail bound of the one-way delay, never a median. It comes from a conservative documented RTT: U = RTT/2.
- **L = 0** is the default. It is the only lower bound valid for every network (the sources give medians, and a median
  is not a minimum). A larger L only removes executions, so a HOLD at L = 0 holds for every real L >= 0. The variant
  `p` (suffix on the network letter) uses L = P50/2 as a SENSITIVITY check, flagged as median-based.
- **sd** is not a number any of the sources give. Two documented levels are used: `0` (a message is readable only at
  its arrival instant, the strict model) and `U` (one extra transit). Larger sd only enlarges the explored space.

| Net | Meaning | one-way [L, U] ms | Source of U (RTT/2) | Source of the typical value |
|---|---|---|---|---|
| A | same region, cross-AZ | [0, 1] | Azure AZ doc: RTT target "less than approximately 2 ms" | Dean's list: RTT within a datacenter 0.5 ms (secondary, gist) |
| B | continental, cross-region | [0, 65] (p: [14, 65]) | etcd tuning guide: "reasonable round-trip time for the continental United States is 130ms" | Azure P50 RTT East US to Central US 28-29 ms |
| C | intercontinental | [0, 200] (p: [42, 200]) | etcd tuning guide: US to Japan "around 350-400ms" | Azure P50 RTT East US to West Europe 83-85 ms, East US to Japan East 162 ms |
| D | degraded global | [0, 2500] (p: [42, 2500]) | etcd: election timeout cap 50 s with "at least 10 times the round-trip time", i.e. RTT up to 5 s (derived) | none |
| E | partition-delayed | [0, 90000] | GitHub 2012 incident, packets delayed "approximately 90 seconds" (Kleppmann 2016, citing Imbriaco) | none |

## 2. Timers and leases (W)

| Quantity | Value | Source |
|---|---|---|
| SWIM probe timeout / suspicion (memberlist) | Local: 200 ms / 3 x 1 s; LAN: 500 ms / 4 x 1 s; WAN: 3 s / 6 x 5 s | hashicorp/memberlist `config.go` (DefaultLocalConfig, DefaultLANConfig, DefaultWANConfig); doc comment: ProbeTimeout "should be set to 99-percentile of RTT" |
| Lease length | 12 s default extension, 45 s grace | Burrows, OSDI 2006, section 2.8 |
| Lease length | 60 s initial timeout | Ghemawat et al., SOSP 2003, section 3.1 |
| Process pause | G1 pause goal 200 ms (not a guarantee, full GCs "might be very slow") | Oracle JDK 17 GC tuning guide |
| Process pause | GC pauses "have sometimes been known to last for several minutes" | Kleppmann 2016, citing Lipcon 2011 |
| Retransmission timer (ABP, PAR variant, BRP T1) | 200 ms (Linux `tcp_rto_min_us` default 200000), 1 s (RFC 6298: initial RTO and minimum "SHOULD be rounded up to 1 second"), 3 s (RFC 1122 legacy initial, RFC 6298 section 7) | RFC 6298; kernel ip-sysctl |
| Retransmission bound | `tcp_retries1` 3, `tcp_syn_retries` 6, `tcp_retries2` 15 | kernel ip-sysctl |
| Failover (NRP) | Advertisement_Interval 100 centiseconds (1 s); Master_Down_Interval = 3 x Adver + skew, so max-missed 3 | RFC 5798 section 6.1 |
| Ping timeout (NRP) | 2U = the p99 round trip, by memberlist's ProbeTimeout rule | memberlist `config.go` |
| Kafka client | request.timeout.ms 30000, retry.backoff.ms 100, transaction.timeout.ms 60000, delivery.timeout.ms 120000, session.timeout.ms 45000, heartbeat.interval.ms 3000 | Apache Kafka producer/consumer configuration |
| HTTP front door | integration timeout range 50 ms to 29 s | AWS API Gateway quotas |
| YARN tick | NodeManager heartbeat 1000 ms; NM and AM liveness expiry 600000 ms | `yarn-default.xml` |

## 3. Pauses scanned for the lease

For a regime with window [L, U] and storage lifetime sd, the lease law is: a violation is feasible iff
PAUSE >= TTL - 2(U - L) - sd (sd enters once). Measured on 2026-10-15 by 27 bisections at C = 2, R = 2 (U/L/sd =
1/0/{0,1,2}; 65/0/{0,16.25,32.5,65,130}; 200/0/{0,50,100,200,400}; 200/42/{0,50,100,200,400}; 2500/0/{0,625,1250,2500,5000};
50/10/{0,12.5,25,50,100}): the threshold sat exactly 2(U - L) + sd below TTL in every probe. An earlier form with
2(U - L + sd) had only been checked at sd = 0, where both agree. The sd = 0 regimes are scanned at 200 ms (G1 goal),
at b-1 and b (one ms either side of their own boundary b), at TTL + 1 s, and at 180 s ("several minutes"). The sd = U
regimes were first scanned at the wrong boundary (they hold there) and are pinned in Phase C at t-1 and t with
t = TTL - 2(U - L) - U.

## 4. Protocols that cannot take citable regimes

- **Chandra-Toueg**: with etcd's election/heartbeat ratio of 10 the receiver needs about 33 steps, so the horizon would
  have to be at least 33 rounds; the 2-hour campaign could not finish H = 19.
- **Spanning tree**: IEEE 802.1D timers (hello 2 s, forward delay 15 s per Cisco; max age 20 s implied) need at least
  18 hellos to be non-vacuous, beyond what Mustard reached (R = 13 in 70 min).
- **Sensor network**: no citable deployment figures.
- **PAR (exact-delay Bosnacki-Dams model)**: replaced by the ABP file's `par` variant (acks without a bit), which takes
  a [L, U] window; its original values (dK = dL = 3, To = 9) are not used.

## 5. Not verified

Pingmesh tail latencies (only a lecture summary was available; not used for any number in the table) and the max-age
default of 802.1D (spanning tree is not mapped). Verified on 2026-10-15 from the RFC text: VRRP (RFC 5798, section
5.2.7): "Maximum Advertisement Interval is a 12-bit field ... in centiseconds. The default is 100 centiseconds (1
second)"; Master_Down_Interval = 3 x Master_Adver_Interval + Skew_Time (so the field's maximum is 4095 cs, and the
10 s interval used where 2U >= 1 s fits).
