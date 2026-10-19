#!/usr/bin/env python3
"""Summarise run.sh output as absolute numbers, one column per client.

Throughput is the median over all runs; latency is the median of the per-round
percentiles, in microseconds.
"""
import re
import statistics
import sys
from collections import defaultdict

thr = defaultdict(list)  # (lang, size) -> msgs_per_sec
mbs = defaultdict(list)  # (lang, size) -> mb_per_sec
lat = defaultdict(list)  # (lang, size) -> [{p50_ns, ...}]
langs = []
for line in open(sys.argv[1]):
    if not line.startswith("RESULT"):
        continue
    kv = dict(re.findall(r"(\w+)=(\S+)", line))
    lang = kv["lang"]
    if lang not in langs:
        langs.append(lang)
    key = (lang, int(kv["size"]))
    if kv["mode"] == "throughput":
        thr[key].append(float(kv["msgs_per_sec"]))
        mbs[key].append(float(kv["mb_per_sec"]))
    else:
        lat[key].append({k: int(kv[k]) for k in ("p50_ns", "p90_ns", "p99_ns", "p999_ns")})

order = [l for l in ("java", "go", "go-main") if l in langs] + [l for l in langs if l not in ("java", "go", "go-main")]
sizes = sorted({s for _, s in thr})
w = 12


def header(title, unit):
    print(f"\n{title} ({unit})")
    print(f"{'size':>6} " + " ".join(f"{l:>{w}}" for l in order))


header("THROUGHPUT, messages per second, median", "millions")
for s in sizes:
    print(f"{s:>5}B " + " ".join(f"{statistics.median(thr[(l, s)]) / 1e6:>{w}.2f}" for l in order))

header("THROUGHPUT, data rate, median", "MB per second")
for s in sizes:
    print(f"{s:>5}B " + " ".join(f"{statistics.median(mbs[(l, s)]):>{w},.0f}" for l in order))

for p, name in (("p50_ns", "p50"), ("p90_ns", "p90"), ("p99_ns", "p99"), ("p999_ns", "p99.9")):
    header(f"LATENCY round trip {name}, median of rounds", "microseconds")
    for s in sizes:
        print(f"{s:>5}B " + " ".join(f"{statistics.median(x[p] for x in lat[(l, s)]) / 1000:>{w}.2f}" for l in order))
