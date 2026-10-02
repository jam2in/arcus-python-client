#!/bin/sh
set -eu
repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"
export COMPOSE_PROJECT_NAME=arcus-python-client-benchmark
export ARCUS_BENCH_RESULTS="$repository_root/build/benchmarks/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$ARCUS_BENCH_RESULTS"
compose() { docker compose -f "$repository_root/benchmarks/compose.yaml" "$@"; }
if [ -n "$(compose ps --all --quiet)" ]; then
    printf '%s\n' 'An isolated benchmark stack is already running.' >&2
    exit 1
fi
cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [ -n "${monitor_pid:-}" ]; then kill "$monitor_pid" 2>/dev/null || true; wait "$monitor_pid" 2>/dev/null || true; fi
    compose logs --no-color > "$ARCUS_BENCH_RESULTS/services.log" 2>&1 || true
    compose down --volumes --remove-orphans --timeout 10 || result=1
    printf 'Benchmark results: %s\n' "$ARCUS_BENCH_RESULTS"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# Record both the committed revision and local patch used by the build.
git rev-parse HEAD > "$ARCUS_BENCH_RESULTS/revision.txt"
git diff -- src pyproject.toml benchmarks > "$ARCUS_BENCH_RESULTS/source.patch"
find src benchmarks -type f ! -path '*/__pycache__/*' ! -path '*/target/*' \
    -exec shasum -a 256 {} + > "$ARCUS_BENCH_RESULTS/source-sha256.txt"
compose config > "$ARCUS_BENCH_RESULTS/compose.yaml"
docker version > "$ARCUS_BENCH_RESULTS/docker-version.txt"
compose build benchmark
docker image inspect arcus-python-client-benchmark-benchmark > "$ARCUS_BENCH_RESULTS/image.json"
compose up --detach register
compose up --detach --wait --wait-timeout 120 cache1
container_ids=$(compose ps --quiet)
# Periodic server/container observations are independent of latency measurements.
(
    while :; do
        date -u +%Y-%m-%dT%H:%M:%SZ
        docker stats --no-stream --format '{{json .}}' $container_ids
        sleep 2
    done
) > "$ARCUS_BENCH_RESULTS/container-stats.jsonl" 2>&1 &
monitor_pid=$!
result=0
compose run --rm --no-deps -T benchmark "$@" > "$ARCUS_BENCH_RESULTS/runner.log" 2>&1 || result=$?
cat "$ARCUS_BENCH_RESULTS/runner.log"
exit "$result"
