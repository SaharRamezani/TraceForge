#!/usr/bin/env python3
"""v12: regime-based campaign. All time values in milliseconds (unit = 1 ms). Every number below is cited in REGIMES.md.

NETWORKS: one-way transit window [L, U] and storage delay sd. U is a conservative tail bound, L = 0 is the only
bound that is valid for every network (the medians quoted in the sources are NOT lower bounds), so L = 0 is the
default and L = P50/2 is only a sensitivity variant (suffix 'p').
"""
NETS = {
    # id: (name, L, U, L_p50_variant)
    "A": ("same region, cross-AZ", 0, 1, 0),             # Dean 0.5 ms RTT; Azure AZ target RTT < ~2 ms
    "B": ("continental, cross-region", 0, 65, 14),        # etcd: continental US RTT 130 ms; Azure P50 East US-Central US 28-29 ms
    "C": ("intercontinental", 0, 200, 42),                # etcd: US-Japan RTT 350-400 ms; Azure P50 East US-West Europe 83-85 ms
    "D": ("degraded global", 0, 2500, 42),                # etcd: very global network RTT up to 5 s
    "E": ("partition-delayed", 0, 90000, 42),             # GitHub 2012 incident, packets delayed ~90 s (via Kleppmann)
}
# SWIM timer profiles: HashiCorp memberlist config.go (ProbeTimeout, SuspicionMult * ProbeInterval for N <= 10)
SWIM_PROFILES = {"local": (200, 3000), "lan": (500, 4000), "wan": (3000, 30000)}
# lease lengths: Chubby default extension 12 s (Burrows OSDI'06 s2.8), GFS initial lease 60 s (Ghemawat SOSP'03 s3.1)
LEASE_TTL = {"chubby12": 12000, "gfs60": 60000}
# process pauses (ms): G1 pause goal 200 ms (Oracle GC tuning guide), several minutes (Kleppmann citing Lipcon 2011)
PAUSE_FIXED = [200, 180000]
# retransmission timers: Linux tcp_rto_min 200 ms, RFC 6298 min/initial 1 s, RFC 1122 legacy initial 3 s
RTO = [200, 1000, 3000]
# NRP / VRRP (RFC 5798): Advertisement_Interval 1 s, Master_Down_Interval = 3 x Adver + skew -> max-missed 3
VRRP_ADV = 1000
# Kafka client defaults: request.timeout.ms 30000, retry.backoff.ms 100 (producer/consumer docs)
KAFKA_REQ = 30000
KAFKA_BACKOFF = 100
# HTTP front door: AWS API Gateway integration timeout 29 s
HTTP_TIMEOUT = 29000
# YARN: NM heartbeat 1000 ms = the model's tick (yarn-default.xml)
YARN_TICK_MS = 1000
