#!/usr/bin/env bash
# Java vs Go Aeron client comparison.
#
# Starts one Java media driver, then runs the Java and Go harnesses against it
# in alternating rounds so machine drift does not favour whichever runs first.
# Both clients are pinned to the same cores, the driver to others, and each mode
# and size runs in a fresh process.
#
#   bench/compare/run.sh                 full run
#   QUICK=1 bench/compare/run.sh         small counts, to check the setup
#
# Environment: ROUNDS (2), AERON_VERSION (1.53.3), AERON_JAR, DRIVER_CPUS,
# CLIENT_CPUS, PIN (producer,consumer CPUs; "none" to leave threads floating),
# IDLE (throughput consumer: spin, or backoff for Agrona's BackoffIdleStrategy),
# SIZES, OUT (bench/compare/results.txt), and EXTRA_GO: a space
# separated list of other Go harness binaries to include (for example one built
# from another branch; each reports its own lang= label).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
version="${AERON_VERSION:-1.53.3}"
rounds="${ROUNDS:-2}"
sizes="${SIZES:-32,256,1024,4000}"
out="${OUT:-$here/results.txt}"
work="$(mktemp -d)"
dir="/dev/shm/aeron-compare-$$"

if [[ -n "${QUICK:-}" ]]; then
  args=(-count=200000 -repeats=1 -latcount=20000 -latwarmup=5000)
  rounds=1
else
  args=()
fi

jar="${AERON_JAR:-$here/.cache/aeron-all-$version.jar}"
if [[ ! -f "$jar" ]]; then
  mkdir -p "$(dirname "$jar")"
  curl -fsSL -o "$jar" "https://repo1.maven.org/maven2/io/aeron/aeron-all/$version/aeron-all-$version.jar"
fi

# Pin the driver and the clients to separate cores when there are enough.
driver_pin=() client_pin=()
if [[ "$(nproc)" -ge 4 ]] && command -v taskset >/dev/null; then
  driver_pin=(taskset -c "${DRIVER_CPUS:-0,1}")
  client_pin=(taskset -c "${CLIENT_CPUS:-2,3}")
fi

jvm=(--add-exports java.base/jdk.internal.misc=ALL-UNNAMED)

args+=(-idle="${IDLE:-spin}")

# Pin the two busy threads (publisher and consumer, pinger and ponger) to one
# CPU each, so neither client is at the mercy of the scheduler putting them on
# the same core. PIN=none leaves them floating.
if [[ "${PIN:-}" != "none" ]]; then
  cpus="${CLIENT_CPUS:-2,3}"
  if [[ "$(nproc)" -ge 4 ]]; then args+=(-pin="${PIN:-$cpus}"); fi
fi

cleanup() {
  [[ -n "${driver_pid:-}" ]] && kill "$driver_pid" 2>/dev/null || true
  rm -rf "$work" "$dir"
}
trap cleanup EXIT

javac -proc:none -cp "$jar" -d "$work/classes" "$here/java/Bench.java" 2>&1 | grep -v "Picked up" || true
(cd "$root" && go build -o "$work/gobench" ./bench/compare/gobench)

"${driver_pin[@]}" java "${jvm[@]}" -Daeron.dir="$dir" -Daeron.threading.mode=DEDICATED \
  -cp "$jar" io.aeron.driver.MediaDriver >"$work/driver.log" 2>&1 &
driver_pid=$!
for _ in $(seq 100); do [[ -f "$dir/cnc.dat" ]] && break; sleep 0.1; done
[[ -f "$dir/cnc.dat" ]] || { echo "driver did not start:"; cat "$work/driver.log"; exit 1; }

{
  echo "# date:    $(date -u +%FT%TZ)"
  echo "# host:    $(uname -sr), $(nproc) cpus, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
  echo "# java:    $(java -version 2>&1 | grep -v 'Picked up' | head -1), aeron-all $version (driver and Java client)"
  echo "# go:      $(go version), aergo $(git -C "$root" rev-parse --short HEAD)"
  echo "# driver:  threading=DEDICATED cpus=${driver_pin[*]:-unpinned}; clients cpus=${client_pin[*]:-unpinned}"
  echo "# args:    ${args[*]} sizes=$sizes rounds=$rounds"
  runners=(java go)
  for extra in ${EXTRA_GO:-}; do runners+=("$extra"); done
  for r in $(seq "$rounds"); do
    # Rotate who goes first each round so nobody always runs on a quiet or a
    # busy machine.
    for k in $(seq 0 $((${#runners[@]} - 1))); do
      who="${runners[$(((k + r - 1) % ${#runners[@]}))]}"
      echo "# round $r: $who"
      # Each mode and size gets a fresh process, so no phase can leave state
      # (mappings, heap, scheduler history) behind for the next one.
      for mode in throughput latency; do
        for size in ${sizes//,/ }; do
          case "$who" in
            java) "${client_pin[@]}" java "${jvm[@]}" -cp "$jar:$work/classes" Bench -dir="$dir" -mode=$mode -sizes=$size "${args[@]}" 2>&1 | grep -v 'Picked up' ;;
            go) "${client_pin[@]}" "$work/gobench" -dir="$dir" -mode=$mode -sizes=$size "${args[@]}" 2>&1 ;;
            *) "${client_pin[@]}" "$who" -dir="$dir" -mode=$mode -sizes=$size "${args[@]}" 2>&1 ;;
          esac
        done
      done | sed "s/^RESULT /RESULT round=$r /"
    done
  done
} | tee "$out"
