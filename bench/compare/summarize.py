#!/usr/bin/env python3
"""Summarise run.sh output: median throughput and latency per size, Java vs Go."""
import re
import statistics
import sys
from collections import defaultdict

thr = defaultdict(list)  # (lang, size) -> msgs_per_sec
lat = defaultdict(list)  # (lang, size) -> {p50,..}
for line in open(sys.argv[1]):
    if not line.startswith("RESULT"):
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", line))
    key = (kv["lang"], int(kv["size"]))
    if kv["mode"] == "throughput":
        thr[key].append(float(kv["msgs_per_sec"]))
    else:
        lat[key].append({k: int(kv[k]) for k in ("p50_ns", "p90_ns", "p99_ns", "p999_ns")})

sizes = sorted({s for _, s in thr})
print("THROUGHPUT (median msgs/s over all runs; spread = min..max)")
print(f"{'size':>5} {'java':>12} {'go':>12} {'go/java':>8}   java spread          go spread")
for s in sizes:
    j, g = thr[("java", s)], thr[("go", s)]
    mj, mg = statistics.median(j), statistics.median(g)
    print(f"{s:>5} {mj:>12,.0f} {mg:>12,.0f} {mg/mj:>7.2f}x   {min(j)/1e6:5.2f}..{max(j)/1e6:5.2f}M   {min(g)/1e6:5.2f}..{max(g)/1e6:5.2f}M")

print("\nLATENCY round trip, ns (median of the per-round values)")
print(f"{'size':>5} {'pct':>5} {'java':>9} {'go':>9} {'go/java':>8}")
for s in sizes:
    for p in ("p50_ns", "p90_ns", "p99_ns", "p999_ns"):
        mj = statistics.median(x[p] for x in lat[("java", s)])
        mg = statistics.median(x[p] for x in lat[("go", s)])
        print(f"{s:>5} {p[:-3]:>5} {mj:>9,.0f} {mg:>9,.0f} {mg/mj:>7.2f}x")
